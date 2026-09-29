package middleware

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// 丢失位点仍在保留窗口内：跨空档确认返回可补齐的 ErrAckGap，
// 绝不能误报成不可恢复的 ErrAckGapExpired，且位点保持不动。
func TestAckGapStillRecoverable(t *testing.T) {
	b := NewBroker(Config{QueueSize: 2, Retention: 64})
	defer b.Close()

	sub, err := b.Subscribe("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	// 不消费：seq1..3 被挤掉（lost=1,2,3），队列剩 4、5；保留窗口未滚动。
	for i := 0; i < 5; i++ {
		if _, err := b.Publish("t", Version1, nil); err != nil {
			t.Fatal(err)
		}
	}
	err = sub.Ack(5)
	if !errors.Is(err, ErrAckGap) {
		t.Fatalf("ack over recoverable gap: got %v, want ErrAckGap", err)
	}
	if errors.Is(err, ErrAckGapExpired) {
		t.Fatalf("gap inside retention window must not look expired: %v", err)
	}
	if sub.Position() != 0 {
		t.Fatalf("position moved after rejected ack: %d", sub.Position())
	}
	t.Logf("recoverable gap rejected distinctly: %v", err)

	// 被拒后排空队列（继续消费），空档未补，结论不变、位点仍不动。
	<-sub.C()
	<-sub.C()
	if err := sub.Ack(5); !errors.Is(err, ErrAckGap) {
		t.Fatalf("gap still rejected after draining queue: got %v", err)
	}
	if sub.Position() != 0 {
		t.Fatalf("position moved: %d", sub.Position())
	}
}

// 丢失位点已滚出保留窗口：跨空档确认必须返回 ErrAckGapExpired，
// 且它同时仍属于 ErrAckGap（errors.Is 两者皆命中），位点保持不动；
// 重连也无法把缺口补回（从头订阅少给、从旧位点续传被明确拒绝）。
func TestAckGapExpiredBeyondRetention(t *testing.T) {
	b := NewBroker(Config{QueueSize: 2, Retention: 4})
	defer b.Close()

	sub, err := b.Subscribe("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	// 6 条不消费：lost=1..4，队列剩 5、6；保留窗口裁到 seq3..6（oldest=3），
	// 缺口 seq1、2 已滚出窗口。
	for i := 0; i < 6; i++ {
		if _, err := b.Publish("t", Version1, nil); err != nil {
			t.Fatal(err)
		}
	}
	err = sub.Ack(6)
	if !errors.Is(err, ErrAckGapExpired) {
		t.Fatalf("ack over expired gap: got %v, want ErrAckGapExpired", err)
	}
	if !errors.Is(err, ErrAckGap) {
		t.Fatalf("expired gap must still classify as a gap: %v", err)
	}
	if sub.Position() != 0 {
		t.Fatalf("position moved after expired ack: %d", sub.Position())
	}
	t.Logf("expired gap rejected distinctly: %v", err)

	// 同一空档再确认一次，结论稳定、位点仍不动。
	if err := sub.Ack(4); !errors.Is(err, ErrAckGapExpired) {
		t.Fatalf("re-ack over expired gap: got %v, want ErrAckGapExpired", err)
	}
	if sub.Position() != 0 {
		t.Fatalf("position moved after repeated expired ack: %d", sub.Position())
	}

	// 从旧位点重连：resumeFrom=1 < oldest-1=2，缺口补不回来，明确拒绝。
	if _, err := b.Subscribe("t", 1); !errors.Is(err, ErrPositionExpired) {
		t.Fatalf("resume into expired window: got %v, want ErrPositionExpired", err)
	}
	// 从头订阅（哨兵 0）虽允许建立，但只给窗口内尚存的 seq3..6，seq1、2 永久缺失。
	head, err := b.Subscribe("t", 0)
	if err != nil {
		t.Fatalf("head subscribe: %v", err)
	}
	defer head.Unsubscribe()
	for want := uint64(3); want <= 6; want++ {
		if m := recv(t, head); m.Seq != want {
			t.Fatalf("head replay: got seq=%d want %d", m.Seq, want)
		}
	}
	select {
	case m := <-head.C():
		t.Fatalf("expired message reappeared from head replay: seq=%d", m.Seq)
	default:
	}
	t.Logf("reconnect cannot recover expired gap: head replay starts at seq3")
}

// 卡在保留范围边缘的判定：丢失位点恰好等于窗口最早 Seq 时仍可补齐（ErrAckGap），
// 此时从 oldest-1 重连合法且能补全；窗口再滚一条、同一缺口变成 oldest-1 时，
// 立刻变成不可补齐（ErrAckGapExpired），重连也被拒。两种“只差一点点”不得互判。
func TestAckGapRetentionBoundary(t *testing.T) {
	const R = 4
	b := NewBroker(Config{QueueSize: 1, Retention: R})
	defer b.Close()

	sub, err := b.Subscribe("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	// 发满 R 条且不消费：lost=1..R-1，队列 [R]，窗口 [1..R]，oldest=1。
	// 最早缺口 seq1 恰好卡在窗口边缘（1 == oldest），必须判为可补齐。
	for i := 0; i < R; i++ {
		if _, err := b.Publish("t", Version1, nil); err != nil {
			t.Fatal(err)
		}
	}
	err = sub.Ack(R)
	if !errors.Is(err, ErrAckGap) || errors.Is(err, ErrAckGapExpired) {
		t.Fatalf("gap at window edge (seq==oldest) must be recoverable: %v", err)
	}
	if sub.Position() != 0 {
		t.Fatalf("position moved after edge-gap rejection: %d", sub.Position())
	}
	t.Logf("edge gap seq1 == oldest 1 rejected as recoverable: %v", err)

	// 从位点 0 重连能把 1..R 全部补回（缺口确实还来得及补）。
	gen2, err := b.Subscribe("t", 0)
	if err != nil {
		t.Fatalf("resume while gap still in window: %v", err)
	}
	for want := uint64(1); want <= R; want++ {
		m := recv(t, gen2)
		if m.Seq != want {
			t.Fatalf("edge resume: got seq=%d want %d", m.Seq, want)
		}
		if err := gen2.Ack(want); err != nil {
			t.Fatalf("ack %d after edge resume: %v", want, err)
		}
	}
	gen2.Unsubscribe()
	t.Logf("edge gap filled by resume, new subscription advanced to %d", R)

	// 窗口再滚一条：发 seq R+1，窗口 [2..R+1]，oldest=2，同一缺口 seq1
	// 只比最早存活位点小 1，立刻不可补齐。
	if _, err := b.Publish("t", Version1, nil); err != nil {
		t.Fatal(err)
	}
	err = sub.Ack(R + 1)
	if !errors.Is(err, ErrAckGapExpired) {
		t.Fatalf("gap one hair beyond window edge (seq==oldest-1) must be expired: %v", err)
	}
	if sub.Position() != 0 {
		t.Fatalf("position moved after expired edge ack: %d", sub.Position())
	}
	t.Logf("gap seq1 == oldest-1 (2-1) rejected as expired: %v", err)

	// 此时从“边缘位点” oldest-1=1 重连仍合法（只差 seq2..），从更早的位点则被拒：
	// 再滚一条让 oldest=3，resumeFrom=1 < 3-1=2 即过期。
	if _, err := b.Publish("t", Version1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Subscribe("t", 1); !errors.Is(err, ErrPositionExpired) {
		t.Fatalf("resume past window edge: got %v, want ErrPositionExpired", err)
	}
	// 而卡在边缘的 resumeFrom=2 == oldest-1 合法，从 seq3 开始补。
	edge, err := b.Subscribe("t", 2)
	if err != nil {
		t.Fatalf("resume at exact edge oldest-1 must be allowed: %v", err)
	}
	defer edge.Unsubscribe()
	if m := recv(t, edge); m.Seq != 3 {
		t.Fatalf("edge-legal resume: got seq=%d want 3", m.Seq)
	}
	t.Logf("subscribe boundary matches ack boundary: oldest-1 legal, older expired")
}

// 拒绝跨空档确认之后，继续消费并靠多代重连补齐，最终收到的消息集合与位点
// 必须和“一直在线且始终跟得上”的订阅一模一样：不重不漏。多发布方并发发布。
func TestAckGapRejectThenResumeConverges(t *testing.T) {
	const (
		publishers = 4
		pubEach    = 150
		total      = publishers * pubEach
	)
	b := NewBroker(Config{QueueSize: 1, Retention: 4096})
	defer b.Close()

	// 受害订阅：队列容量 1 且完全不消费，发布期间几乎所有消息都被挤掉。
	victim, err := b.Subscribe("t", 0)
	if err != nil {
		t.Fatal(err)
	}

	var pubWg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		pubWg.Add(1)
		go func() {
			defer pubWg.Done()
			for i := 0; i < pubEach; i++ {
				if _, err := b.Publish("t", Version1, nil); err != nil {
					t.Errorf("publish: %v", err)
					return
				}
			}
		}()
	}
	pubWg.Wait()

	// 积压空档巨大但全在保留窗口内：跨空档确认必须被“可补齐”原因拒绝，位点不动。
	if err := victim.Ack(total); !errors.Is(err, ErrAckGap) {
		t.Fatalf("victim ack over big recoverable gap: got %v, want ErrAckGap", err)
	}
	if errors.Is(err, ErrAckGapExpired) {
		t.Fatalf("retention large, gap must not be expired: %v", err)
	}
	if victim.Position() != 0 {
		t.Fatalf("victim position moved: %d", victim.Position())
	}
	t.Logf("victim ack rejected with recoverable gap, position stays 0: %v", err)
	victim.Unsubscribe()

	// 多代重连补齐：每代消费一批后断线，下一代从确认位点懒续传。
	got := make(map[uint64]bool, total)
	var pos uint64
	generation := 0
	deadline := time.Now().Add(20 * time.Second)
	for pos < total {
		if time.Now().After(deadline) {
			t.Fatalf("resume chain stuck at %d/%d", pos, total)
		}
		s, err := b.Subscribe("t", pos)
		if err != nil {
			t.Fatalf("gen %d subscribe at %d: %v", generation, pos, err)
		}
		var consumed uint64
		const batch = 37
		idle := time.After(2 * time.Second)
	genLoop:
		for {
			select {
			case m := <-s.C():
				if m.Seq <= pos {
					t.Fatalf("gen %d: duplicated/out-of-order seq=%d at pos=%d", generation, m.Seq, pos)
				}
				pos = m.Seq
				got[m.Seq] = true
				consumed++
				idle = time.After(500 * time.Millisecond)
				if consumed >= batch {
					break genLoop
				}
			case <-idle:
				break genLoop
			}
		}
		// 补到哪里确认到哪里：被补过的区间无空档，确认必须成功。
		if err := s.Ack(pos); err != nil {
			t.Fatalf("gen %d ack to %d: %v", generation, pos, err)
		}
		s.Unsubscribe()
		generation++
	}
	if pos != total {
		t.Fatalf("final position=%d want %d", pos, total)
	}

	// 全部消息同主题：一直在线且跟得上的视图就是全集 {1..total}。
	// 逐位对比：重连补齐后的视图必须恰好是全集，不重不漏。
	if len(got) != total {
		var missing []uint64
		for seq := uint64(1); seq <= total; seq++ {
			if !got[seq] {
				missing = append(missing, seq)
			}
		}
		t.Fatalf("resumed view has %d distinct seqs, want %d; missing sample=%v", len(got), total, missing[:min(10, len(missing))])
	}
	for seq := uint64(1); seq <= total; seq++ {
		if !got[seq] {
			t.Fatalf("resumed subscription missed seq %d (view diverges from always-online)", seq)
		}
	}
	t.Logf("resumed across %d generations: %d seqs, identical to always-online view", generation, total)
}

// 并发压力：多发布方并发发布、多个极小队列订阅者同时在线，保留窗口故意开得很小，
// 使空档一边产生一边过期。要求：
//   - 全程不卡死（发布、消费、确认、退订、关闭都在时限内完成）；
//   - 快消费订阅最终位点推进到最后一条，且从未在被拒后发生位点漂移；
//   - 慢消费订阅在空档过期后稳定收到 ErrAckGapExpired（且仍归类为 ErrAckGap），
//     任何跨空档确认都不得把位点推过缺口；
//   - 每个订阅的去向账目与匹配消息总数对得上，重复计数为 0。
func TestConcurrentGapExpiryTinyQueues(t *testing.T) {
	const (
		publishers  = 4
		pubEach     = 250
		total       = publishers * pubEach
		subscribers = 6
	)
	b := NewBroker(Config{QueueSize: 2, Retention: 32})

	// 先把全部订阅建好再发布：保证每条消息都在实时路径上有确定去向
	// （入队 / 挤旧 / 拒收），避免“订阅时日志已滚过窗口、续传只补到尾部”
	// 造成账目缺口——那是另一种生命周期，由其它用例覆盖。
	subs := make([]*Subscription, subscribers)
	var readWg sync.WaitGroup
	readCounts := make([]uint64, subscribers)
	sawRecoverable := make([]bool, subscribers)
	sawExpired := make([]bool, subscribers)
	var countersMu sync.Mutex
	for i := 0; i < subscribers; i++ {
		policy := PolicyDropOldest
		if i%2 == 1 {
			policy = PolicyDropNewest
		}
		s, err := b.SubscribeWith("t", 0, SubscribeOptions{Policy: policy})
		if err != nil {
			t.Fatal(err)
		}
		subs[i] = s
		readWg.Add(1)
		go func(id int, s *Subscription) {
			defer readWg.Done()
			var n uint64
			var last uint64
			for m := range s.C() {
				n++
				// 偶数号快消费：每条都确认；奇数号慢消费且不逐条确认，
				// 制造大量积压与跨空档确认。收到顺序必须严格递增（不重）。
				if m.Seq <= last {
					t.Errorf("sub %d non-monotonic delivery: seq=%d after %d", id, m.Seq, last)
					return
				}
				last = m.Seq
				if id%2 == 0 {
					if err := s.Ack(m.Seq); err != nil {
						// 快消费也可能被“尚未补齐的旧缺口”挡住：原因必须合法、
						// 且位点绝不能漂移到 m.Seq。
						switch {
						case errors.Is(err, ErrAckGapExpired):
							countersMu.Lock()
							sawExpired[id] = true
							countersMu.Unlock()
						case errors.Is(err, ErrAckGap):
							countersMu.Lock()
							sawRecoverable[id] = true
							countersMu.Unlock()
						default:
							t.Errorf("sub %d unexpected ack error: %v", id, err)
						}
						if s.Position() > m.Seq {
							t.Errorf("sub %d position jumped past gap: %d > %d", id, s.Position(), m.Seq)
						}
					}
				} else if n%7 == 0 {
					// 慢消费周期性地试着确认到最新，跨空档必被拒，记录原因分类。
					err := s.Ack(m.Seq)
					if err == nil {
						continue
					}
					countersMu.Lock()
					switch {
					case errors.Is(err, ErrAckGapExpired):
						sawExpired[id] = true
					case errors.Is(err, ErrAckGap):
						sawRecoverable[id] = true
					default:
						t.Errorf("sub %d unexpected slow-ack error: %v", id, err)
					}
					countersMu.Unlock()
					// 位点不得越过未补齐的缺口。
					if p := s.Position(); p > m.Seq {
						t.Errorf("sub %d position %d jumped past attempted seq %d", id, p, m.Seq)
					}
				}
				if id%2 == 1 && n%5 == 0 {
					time.Sleep(time.Microsecond)
				}
			}
			readCounts[id] = n
		}(i, s)
	}

	// 所有订阅就位后再并发发布。
	var pubWg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		pubWg.Add(1)
		go func() {
			defer pubWg.Done()
			for i := 0; i < pubEach; i++ {
				if _, err := b.Publish("t", Version1, nil); err != nil {
					t.Errorf("publish: %v", err)
					return
				}
			}
		}()
	}

	pubDone := make(chan struct{})
	go func() { pubWg.Wait(); close(pubDone) }()
	select {
	case <-pubDone:
	case <-time.After(15 * time.Second):
		t.Fatal("publishers deadlocked under tiny queues and small retention")
	}
	t.Logf("all %d publishes completed (queue=2, retention=32)", total)

	// 等续传泵把积压补完（快消费会持续腾出空位），慢订阅的缺口也已定型。
	dl := time.Now().Add(15 * time.Second)
	for b.hasPendingReplay() {
		if time.Now().After(dl) {
			t.Fatal("replay pump did not finish draining")
		}
		time.Sleep(time.Millisecond)
	}

	// 发布结束后再做一次终局确认判定：此时窗口最早 Seq 已很大。
	for i, s := range subs {
		err := s.Ack(total)
		st := s.Stats()
		if err == nil {
			// 无空档确认到终点：位点必须真的到 total（快订阅的理想结局）。
			if s.Position() != total {
				t.Errorf("sub %d accepted ack but position=%d want %d", i, s.Position(), total)
			}
			continue
		}
		// 只要存在跨空档的拒绝，位点必须严格小于 total、没有越过缺口。
		if s.Position() >= total {
			t.Errorf("sub %d rejected ack but position advanced to %d: %v", i, s.Position(), err)
		}
		switch {
		case errors.Is(err, ErrAckGapExpired):
			countersMu.Lock()
			sawExpired[i] = true
			countersMu.Unlock()
		case errors.Is(err, ErrAckGap):
			countersMu.Lock()
			sawRecoverable[i] = true
			countersMu.Unlock()
		default:
			t.Errorf("sub %d final ack unexpected error: %v (stats=%+v)", i, err, st)
		}
	}

	// 至少要观察到一个订阅在并发中经历过空档过期（否则用例没有真正压到新判定）。
	countersMu.Lock()
	var anyExpired, anyRecoverable bool
	for i := range subs {
		anyExpired = anyExpired || sawExpired[i]
		anyRecoverable = anyRecoverable || sawRecoverable[i]
	}
	countersMu.Unlock()
	if !anyExpired {
		t.Fatal("expected at least one subscriber to observe ErrAckGapExpired under retention=32")
	}
	t.Logf("gap reasons observed concurrently: expired=%v recoverable=%v", anyExpired, anyRecoverable)

	for _, s := range subs {
		s.Unsubscribe()
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	closedDone := make(chan struct{})
	go func() { readWg.Wait(); close(closedDone) }()
	select {
	case <-closedDone:
	case <-time.After(10 * time.Second):
		t.Fatal("consumers deadlocked during shutdown")
	}

	// 账目核对：所有 total 条匹配消息必须在三类去向中有归属，重复为 0，
	// 两种丢法不串账（与既有混合背压用例同一套勾稽关系）。
	for i, s := range subs {
		st := s.Stats()
		accounted := st.Delivered + st.DroppedNewest + st.DroppedLate
		if accounted != total {
			t.Errorf("sub %d accounting: delivered+newest+late=%d != %d (stats=%+v)",
				i, accounted, total, st)
		}
		if got := st.Delivered - st.DroppedOldest; got != readCounts[i] {
			t.Errorf("sub %d read accounting: delivered-oldest=%d but read=%d",
				i, got, readCounts[i])
		}
		if st.Duplicates != 0 {
			t.Errorf("sub %d unexpected duplicates: %d", i, st.Duplicates)
		}
		if s.Policy() == PolicyDropOldest && st.DroppedNewest != 0 {
			t.Errorf("sub %d drop-oldest counted newest: %d", i, st.DroppedNewest)
		}
		if s.Policy() == PolicyDropNewest && st.DroppedOldest != 0 {
			t.Errorf("sub %d drop-newest counted oldest: %d", i, st.DroppedOldest)
		}
		t.Logf("sub %d policy=%v read=%d position=%d expired=%v recoverable=%v stats=%+v",
			i, s.Policy(), readCounts[i], s.Position(), sawExpired[i], sawRecoverable[i], st)
	}
}
