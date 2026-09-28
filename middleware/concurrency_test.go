package middleware

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// 并发收发 + 并发退订 + 并发关闭：不得死锁、不得 panic，
// 关闭后所有发布/订阅必须返回 ErrClosed，消费者看到的序列号严格递增。
func TestConcurrentLifecycle(t *testing.T) {
	b := NewBroker(Config{QueueSize: 8})

	const subscribers = 8
	const publishers = 4
	const perPublisher = 50

	var wg sync.WaitGroup

	// 订阅者：持续消费并确认，记录序列号单调性。
	violations := make(chan string, subscribers)
	subs := make([]*Subscription, 0, subscribers)
	for i := 0; i < subscribers; i++ {
		pattern := "robot/#"
		if i%2 == 0 {
			pattern = "robot/telemetry"
		}
		sub, err := b.Subscribe(pattern, 0)
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
		wg.Add(1)
		go func(id int, s *Subscription) {
			defer wg.Done()
			var last uint64
			for m := range s.C() {
				if m.Seq <= last {
					violations <- "non-monotonic seq"
					return
				}
				last = m.Seq
				_ = s.Ack(m.Seq)
			}
			t.Logf("subscriber %d drained at position=%d stats=%+v", id, s.Position(), s.Stats())
		}(i, sub)
	}

	// 发布者：并发发布；关闭后应得到 ErrClosed 而非 panic。
	var publishErrs sync.Map // 统计关闭后的错误类型
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perPublisher; i++ {
				_, err := b.Publish("robot/telemetry", Version1, []byte{byte(p), byte(i)})
				if err != nil && !errors.Is(err, ErrClosed) {
					publishErrs.Store(p, err)
					return
				}
			}
		}(p)
	}

	// 并发退订一半订阅者。
	for i := 0; i < subscribers/2; i++ {
		wg.Add(1)
		go func(s *Subscription) {
			defer wg.Done()
			s.Unsubscribe()
			s.Unsubscribe() // 幂等性
		}(subs[i])
	}

	// 短暂并发后关闭 Broker（与发布/退订并发进行）。
	time.Sleep(5 * time.Millisecond)
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: concurrent lifecycle did not finish")
	}
	select {
	case v := <-violations:
		t.Fatalf("ordering violation: %s", v)
	default:
	}
	publishErrs.Range(func(k, v any) bool {
		t.Errorf("publisher %v saw unexpected error: %v", k, v)
		return true
	})
	t.Logf("concurrent lifecycle finished cleanly: no deadlock, no panic, monotonic seqs")
}

// 续传积压期间并发退订/关闭：续传泵不得因向已关闭通道投递而 panic
// （泵一旦 panic 死亡，后续所有订阅都会静默停滞），Close 必须正常返回。
func TestConcurrentUnsubscribeDuringReplay(t *testing.T) {
	for iter := 0; iter < 30; iter++ {
		b := NewBroker(Config{QueueSize: 1, Retention: 8192})
		for i := 0; i < 300; i++ {
			if _, err := b.Publish("t", Version1, nil); err != nil {
				t.Fatal(err)
			}
		}
		const nsub = 8
		var wg sync.WaitGroup
		subs := make([]*Subscription, nsub)
		for i := 0; i < nsub; i++ {
			s, err := b.Subscribe("t", 0)
			if err != nil {
				t.Fatal(err)
			}
			subs[i] = s
			wg.Add(1)
			go func(s *Subscription) {
				defer wg.Done()
				for range s.C() {
				}
			}(s)
		}
		var uwg sync.WaitGroup
		for i := 0; i < nsub; i++ {
			uwg.Add(1)
			go func(s *Subscription) {
				defer uwg.Done()
				s.Unsubscribe()
			}(subs[i])
		}
		uwg.Wait()
		// 退订后再发布并关闭：若泵已死或有残留竞态，这里会卡住或 panic。
		for i := 0; i < 50; i++ {
			if _, err := b.Publish("t", Version1, nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Close(); err != nil {
			t.Fatalf("iteration %d: %v", iter, err)
		}
		wg.Wait()
	}
	t.Logf("30 iterations of unsubscribe-during-replay finished without pump panic")
}

// 并发重连续传：多个订阅者并发地断线重连，已确认消息不得重复交付。
func TestConcurrentResume(t *testing.T) {
	// 队列需容纳整段重放窗口，否则最旧消息会按淘汰策略被丢弃（确定性行为，
	// 本测试关注续传不丢不重，故给足队列）。
	b := NewBroker(Config{QueueSize: 256})
	defer b.Close()

	const total = 200
	var pubWg sync.WaitGroup
	pubWg.Add(1)
	go func() {
		defer pubWg.Done()
		for i := 0; i < total; i++ {
			if _, err := b.Publish("robot/telemetry", Version1, []byte{byte(i)}); err != nil {
				t.Errorf("publish: %v", err)
				return
			}
		}
	}()
	pubWg.Wait()

	// 多代订阅：每代消费一部分后“断线”，下一代从确认位点继续。
	var pos uint64
	const generations = 5
	for g := 0; g < generations; g++ {
		sub, err := b.Subscribe("robot/telemetry", pos)
		if err != nil {
			t.Fatal(err)
		}
		want := uint64(total / generations)
		var got uint64
		for m := range sub.C() {
			if m.Seq <= pos {
				t.Fatalf("generation %d: duplicated seq=%d at position=%d", g, m.Seq, pos)
			}
			pos = m.Seq
			got++
			if got >= want {
				break
			}
		}
		t.Logf("generation %d: resumed, consumed %d msgs, new position=%d", g, got, pos)
		sub.Unsubscribe()
	}
	if pos != total {
		t.Fatalf("final position=%d, want %d (loss or duplication across reconnects)", pos, total)
	}
	t.Logf("resume chain complete: position=%d equals total published=%d", pos, total)
}
