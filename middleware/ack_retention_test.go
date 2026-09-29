package middleware

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// 可补齐空档 vs 已过期空档：同一订阅上，保留窗口滚动前跨过丢失位点的确认
// 报 ErrAckGap（还在窗口内，重连可补齐，应继续等）；窗口滚过丢失位点后改报
// ErrPositionExpired（怎么重连都补不回来，应尽快重置）。两种原因必须可用
// errors.Is 明确区分、互不相容，且被拒绝后确认位点保持不动。
func TestAckGapRecoverableVsExpired(t *testing.T) {
	b := NewBroker(Config{QueueSize: 2, Retention: 4})
	defer b.Close()

	sub, err := b.Subscribe("t", 0) // 默认 PolicyDropOldest
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	pub := func(n int) {
		for i := 0; i < n; i++ {
			if _, err := b.Publish("t", Version1, nil); err != nil {
				t.Fatal(err)
			}
		}
	}

	// 发布 1..4：队列容量 2 不消费，seq1、2 被挤掉（lost={1,2}），
	// 队列剩 {3,4}；保留窗口 [1..4]，下沿 floor=1，丢失位点仍在窗口内。
	pub(4)
	err = sub.Ack(4)
	if !errors.Is(err, ErrAckGap) {
		t.Fatalf("ack over recoverable gap: got %v, want ErrAckGap", err)
	}
	if errors.Is(err, ErrPositionExpired) {
		t.Fatalf("recoverable gap misclassified as expired: %v", err)
	}
	t.Logf("recoverable gap rejected: %v", err)
	if p := sub.Position(); p != 0 {
		t.Fatalf("position moved after rejected ack: %d", p)
	}

	// 再发 5、6：seq3、4 也被挤掉（lost={1..4}），保留窗口滚到 [3..6]，
	// floor=3，丢失位点 1、2 已滚出窗口，怎么重连都补不回来。
	pub(2)
	err = sub.Ack(6)
	if !errors.Is(err, ErrPositionExpired) {
		t.Fatalf("ack over expired gap: got %v, want ErrPositionExpired", err)
	}
	if errors.Is(err, ErrAckGap) {
		t.Fatalf("expired gap misclassified as recoverable: %v", err)
	}
	t.Logf("expired gap rejected: %v", err)
	if p := sub.Position(); p != 0 {
		t.Fatalf("position moved after rejected ack: %d", p)
	}
}

// 边界判定：丢失位点 == 保留窗口下沿时消息仍在窗口内（可补齐，ErrAckGap）；
// 丢失位点 == 下沿-1 时才算彻底过期（ErrPositionExpired）。两个方向都不能误判。
func TestAckGapRetentionBoundary(t *testing.T) {
	b := NewBroker(Config{QueueSize: 1, Retention: 4})
	defer b.Close()

	sub, err := b.Subscribe("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	// 发布 1..4：队列容量 1，每条挤掉上一条，lost={1,2,3}，队列剩 {4}；
	// 窗口 [1..4]，floor=1，最早丢失位点 1 恰好卡在窗口下沿上：仍可补齐。
	for i := 0; i < 4; i++ {
		if _, err := b.Publish("t", Version1, nil); err != nil {
			t.Fatal(err)
		}
	}
	err = sub.Ack(4)
	if !errors.Is(err, ErrAckGap) || errors.Is(err, ErrPositionExpired) {
		t.Fatalf("gap exactly at retention floor must be recoverable: got %v", err)
	}
	t.Logf("gap at floor (1 == oldest retained 1) still recoverable: %v", err)
	if p := sub.Position(); p != 0 {
		t.Fatalf("position moved after rejected ack: %d", p)
	}

	// 再发 1 条：窗口滚到 [2..5]，floor=2，丢失位点 1 刚滚出窗口一格：彻底过期。
	if _, err := b.Publish("t", Version1, nil); err != nil {
		t.Fatal(err)
	}
	err = sub.Ack(5)
	if !errors.Is(err, ErrPositionExpired) || errors.Is(err, ErrAckGap) {
		t.Fatalf("gap one below retention floor must be expired: got %v", err)
	}
	t.Logf("gap below floor (1 < oldest retained 2) expired: %v", err)
	if p := sub.Position(); p != 0 {
		t.Fatalf("position moved after rejected ack: %d", p)
	}
}

// 续传游标还没追到、消息就被保留窗口裁掉的情况：这些位置对该订阅构成永久
// 空档，确认跨过时必须按 ErrPositionExpired 拒绝，不能静默漏过。
func TestAckGapTrimmedBeforeReplay(t *testing.T) {
	b := NewBroker(Config{QueueSize: 1, Retention: 4})
	defer b.Close()

	// 先发 1..4（暂无订阅），再订阅：续传游标从 seq1 开始懒重放，
	// 队列容量 1，建立订阅时同步补入 msg1 后暂停。
	for i := 0; i < 4; i++ {
		if _, err := b.Publish("t", Version1, nil); err != nil {
			t.Fatal(err)
		}
	}
	sub, err := b.Subscribe("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	// 不消费，继续发 5..8：保留窗口滚动把 msg2..4 裁掉，而续传游标还停在
	// msg2 上（队列满）——2..4 对该订阅永远无法补齐，必须记为丢失位点。
	for i := 0; i < 4; i++ {
		if _, err := b.Publish("t", Version1, nil); err != nil {
			t.Fatal(err)
		}
	}

	// 排空：只能收到 1 与窗口内尚存的 5..8，2..4 已永久缺失。
	want := []uint64{1, 5, 6, 7, 8}
	for _, w := range want {
		if m := recv(t, sub); m.Seq != w {
			t.Fatalf("got seq=%d, want %d", m.Seq, w)
		}
	}
	err = sub.Ack(8)
	if !errors.Is(err, ErrPositionExpired) || errors.Is(err, ErrAckGap) {
		t.Fatalf("ack over trimmed-before-replay gap: got %v, want ErrPositionExpired", err)
	}
	t.Logf("trimmed-before-replay gap rejected: %v", err)
	if p := sub.Position(); p != 0 {
		t.Fatalf("position moved after rejected ack: %d", p)
	}
}

// 可补齐空档的完整闭环：确认被拒（ErrAckGap）后继续消费并重连，
// 续传把空档补齐，最终位点与“一直在线且跟得上”的订阅一模一样，不重不漏。
func TestAckGapRecoverableReconnectFill(t *testing.T) {
	b := NewBroker(Config{QueueSize: 2, Retention: 64})
	defer b.Close()

	control, err := b.Subscribe("t", 0) // 一直在线且跟得上的对照订阅
	if err != nil {
		t.Fatal(err)
	}
	defer control.Unsubscribe()
	slow, err := b.Subscribe("t", 0) // 队列开小、会积压丢消息的订阅
	if err != nil {
		t.Fatal(err)
	}

	const total = 6
	var controlSeqs []uint64
	for i := 0; i < total; i++ {
		if _, err := b.Publish("t", Version1, nil); err != nil {
			t.Fatal(err)
		}
		// 对照订阅随发随收随确认，始终跟得上。
		m := recv(t, control)
		if err := control.Ack(m.Seq); err != nil {
			t.Fatalf("control ack %d: %v", m.Seq, err)
		}
		controlSeqs = append(controlSeqs, m.Seq)
	}

	// slow 队列容量 2 全程未消费：seq1..4 被挤掉（仍在保留窗口内），
	// 跨空档确认被 ErrAckGap 拒绝，位点不动。
	if err := slow.Ack(total); !errors.Is(err, ErrAckGap) || errors.Is(err, ErrPositionExpired) {
		t.Fatalf("slow ack over recoverable gap: got %v, want ErrAckGap", err)
	}
	if p := slow.Position(); p != 0 {
		t.Fatalf("slow position moved after rejected ack: %d", p)
	}
	pos := slow.Position()
	slow.Unsubscribe()

	// 重连：从确认位点续传，空档全部补齐；逐条确认到最终位点。
	gen2, err := b.Subscribe("t", pos)
	if err != nil {
		t.Fatal(err)
	}
	defer gen2.Unsubscribe()
	var got []uint64
	for i := 0; i < total; i++ {
		m := recv(t, gen2)
		if err := gen2.Ack(m.Seq); err != nil {
			t.Fatalf("resumed ack %d: %v", m.Seq, err)
		}
		got = append(got, m.Seq)
	}
	if len(got) != len(controlSeqs) {
		t.Fatalf("resumed got %v, want %v", got, controlSeqs)
	}
	for i := range got {
		if got[i] != controlSeqs[i] {
			t.Fatalf("resumed view differs from always-online: got %v, want %v", got, controlSeqs)
		}
	}
	if gen2.Position() != control.Position() {
		t.Fatalf("final position=%d, always-online position=%d", gen2.Position(), control.Position())
	}
	t.Logf("gap filled by resume: final position=%d matches always-online view %v",
		gen2.Position(), controlSeqs)
}

// 已过期空档的完整闭环：确认被拒（ErrPositionExpired）后，非零过期位点的
// 重连同样被拒；只能重置（从头重新订阅，接受窗口内尚存的消息），
// 之后确认可推进到与对照订阅一致的最终位点。
func TestAckGapExpiredReset(t *testing.T) {
	b := NewBroker(Config{QueueSize: 2, Retention: 4})
	defer b.Close()

	control, err := b.Subscribe("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Unsubscribe()
	slow, err := b.Subscribe("t", 0)
	if err != nil {
		t.Fatal(err)
	}

	const total = 8
	for i := 0; i < total; i++ {
		if _, err := b.Publish("t", Version1, nil); err != nil {
			t.Fatal(err)
		}
		m := recv(t, control)
		if err := control.Ack(m.Seq); err != nil {
			t.Fatalf("control ack %d: %v", m.Seq, err)
		}
	}

	// slow 全程未消费：lost={1..6}，保留窗口滚到 [5..8]，floor=5，
	// 丢失位点 1..4 已无法补齐。
	if err := slow.Ack(total); !errors.Is(err, ErrPositionExpired) || errors.Is(err, ErrAckGap) {
		t.Fatalf("slow ack over expired gap: got %v, want ErrPositionExpired", err)
	}
	if p := slow.Position(); p != 0 {
		t.Fatalf("slow position moved after rejected ack: %d", p)
	}
	slow.Unsubscribe()

	// 以过期的非零位点重连：同样被 ErrPositionExpired 拒绝（位点 3 < floor-1=4）。
	if _, err := b.Subscribe("t", 3); !errors.Is(err, ErrPositionExpired) {
		t.Fatalf("resume from expired position: got %v, want ErrPositionExpired", err)
	}
	// 恰好卡在窗口下沿前一位（4 == floor-1）仍可续传，不能被误判成过期。
	edge, err := b.Subscribe("t", 4)
	if err != nil {
		t.Fatalf("resume at floor-1 must be accepted: %v", err)
	}
	edge.Unsubscribe()

	// 重置：从头重新订阅，只补窗口内尚存的 5..8，随后确认推进到最终位点。
	reset, err := b.Subscribe("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reset.Unsubscribe()
	for w := uint64(5); w <= total; w++ {
		m := recv(t, reset)
		if m.Seq != w {
			t.Fatalf("reset got seq=%d, want %d", m.Seq, w)
		}
		if err := reset.Ack(m.Seq); err != nil {
			t.Fatalf("reset ack %d: %v", m.Seq, err)
		}
	}
	if reset.Position() != control.Position() {
		t.Fatalf("reset final position=%d, always-online position=%d",
			reset.Position(), control.Position())
	}
	t.Logf("after reset: final position=%d matches always-online control", reset.Position())
}

// 并发压力：多发布方 + 多订阅者 + 极小队列 + 有限保留窗口。消费者逐条确认，
// 按拒绝原因分别处理——ErrAckGap 从当前位点重连续传补齐，ErrPositionExpired
// 重置到 0 重新订阅。要求：不卡死、确认位点最终全部推进到发布总数，
// 过期路径（建立后不消费、窗口从续传游标下滚过）确定性地被触发。
func TestConcurrentAckReconnectTinyQueue(t *testing.T) {
	b := NewBroker(Config{QueueSize: 2, Retention: 256})
	defer b.Close()

	const (
		publishers = 3
		perPub     = 200
		total      = publishers * perPub
	)

	type rejections struct{ gap, expired bool }
	// consume 从 first 订阅开始消费并逐条确认，直到位点推进到 total。
	// delay 模拟消费耗时；返回观察到的拒绝类型。
	consume := func(first *Subscription, delay time.Duration, deadline time.Time) (uint64, rejections, error) {
		var pos uint64
		var seen rejections
		sub := first
		for pos < total {
			if time.Now().After(deadline) {
				return pos, seen, errors.New("consumer deadline exceeded")
			}
			if sub == nil {
				var err error
				sub, err = b.Subscribe("t", pos)
				if errors.Is(err, ErrPositionExpired) {
					// 位点本身已滚出窗口：重置到 0，只补窗口内尚存的消息。
					pos = 0
					sub, err = b.Subscribe("t", 0)
				}
				if err != nil {
					return pos, seen, err
				}
			}
			reconnect := false
			for !reconnect {
				select {
				case m, ok := <-sub.C():
					if !ok {
						reconnect = true
						continue
					}
					err := sub.Ack(m.Seq)
					switch {
					case err == nil:
						pos = m.Seq
						if pos >= total {
							reconnect = true
						}
					case errors.Is(err, ErrAckGap):
						seen.gap = true // 还能补齐：从当前位点重连续传
						reconnect = true
					case errors.Is(err, ErrPositionExpired):
						seen.expired = true // 补不回来了：走重置路径
						reconnect = true
					default:
						return pos, seen, err
					}
					if delay > 0 {
						time.Sleep(delay)
					}
				case <-time.After(2 * time.Second):
					return pos, seen, errors.New("consumer receive timeout")
				}
			}
			sub.Unsubscribe()
			sub = nil
		}
		return pos, seen, nil
	}

	// 三个订阅都在发布前建立：快消费者、慢消费者（制造持续背压）、
	// 以及发布期间完全不消费的“迟到”消费者（窗口必然从它头上滚过）。
	fast, err := b.Subscribe("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	slow, err := b.Subscribe("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	late, err := b.Subscribe("t", 0)
	if err != nil {
		t.Fatal(err)
	}

	var pubWg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		pubWg.Add(1)
		go func() {
			defer pubWg.Done()
			for i := 0; i < perPub; i++ {
				if _, err := b.Publish("t", Version1, nil); err != nil {
					t.Errorf("publish: %v", err)
					return
				}
			}
		}()
	}

	deadline := time.Now().Add(15 * time.Second)
	type result struct {
		pos  uint64
		seen rejections
		err  error
	}
	run := func(first *Subscription, delay time.Duration) <-chan result {
		ch := make(chan result, 1)
		go func() {
			pos, seen, err := consume(first, delay, deadline)
			ch <- result{pos, seen, err}
		}()
		return ch
	}
	fastCh := run(fast, 0)
	slowCh := run(slow, time.Millisecond)
	// 迟到消费者等发布全部结束才开始消费。
	pubWg.Wait()
	lateCh := run(late, 0)

	check := func(name string, ch <-chan result) result {
		select {
		case r := <-ch:
			if r.err != nil {
				t.Fatalf("%s consumer: %v (position=%d/%d)", name, r.err, r.pos, total)
			}
			if r.pos != total {
				t.Fatalf("%s consumer final position=%d, want %d", name, r.pos, total)
			}
			t.Logf("%s consumer reached position=%d (gap=%v expired=%v)",
				name, r.pos, r.seen.gap, r.seen.expired)
			return r
		case <-time.After(20 * time.Second):
			t.Fatalf("%s consumer did not finish", name)
			return result{}
		}
	}
	fastRes := check("fast", fastCh)
	slowRes := check("slow", slowCh)
	lateRes := check("late", lateCh)

	// 慢消费者在持续背压下必然丢过消息、遇到过空档拒绝（两种之一）。
	if !slowRes.seen.gap && !slowRes.seen.expired {
		t.Fatal("slow consumer under backpressure must observe gap rejections")
	}
	// 迟到消费者的丢失位点必然已滚出保留窗口：必须观察到过期拒绝。
	if !lateRes.seen.expired {
		t.Fatal("late consumer must observe ErrPositionExpired after window rolled past")
	}
	_ = fastRes
}
