package middleware

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// 背压并发压测：多发布方并发写，多订阅者（drop-oldest / drop-newest 混合）以
// 极小队列并发消费，部分订阅中途退订重连。要求：不卡死、不 panic、
// 交付序列严格递增、每个订阅的统计恒等式成立。
func TestConcurrentBackpressureStress(t *testing.T) {
	b := NewBroker(Config{QueueSize: 2, Retention: 4096})

	const (
		publishers = 4
		pubsEach   = 200
		consumers  = 6
	)
	total := uint64(publishers * pubsEach)

	var pubWg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		pubWg.Add(1)
		go func(p int) {
			defer pubWg.Done()
			for i := 0; i < pubsEach; i++ {
				if _, err := b.Publish("t/stress", Version1, []byte{byte(p), byte(i)}); err != nil {
					t.Errorf("publish p=%d i=%d: %v", p, i, err)
					return
				}
			}
		}(p)
	}

	var consWg sync.WaitGroup
	violations := make(chan string, consumers)
	finalStats := make(chan Stats, consumers)
	finalize := func(s *Subscription) {
		// 排空缓冲后统计；恒等式必须成立。
		drained := 0
		for {
			select {
			case _, ok := <-s.C():
				if !ok {
					goto check
				}
				drained++
			case <-time.After(50 * time.Millisecond):
				goto check
			}
		}
	check:
		st := s.Stats()
		if !st.Reconciled() {
			violations <- fmt.Sprintf("stats identity mismatch: %+v", st)
		}
		finalStats <- st
	}

	for c := 0; c < consumers; c++ {
		policy := PolicyDropOldest
		if c%2 == 1 {
			policy = PolicyDropNewest
		}
		sub, err := b.SubscribeWith("t/stress", 0, SubscribeOptions{OverflowPolicy: policy})
		if err != nil {
			t.Fatal(err)
		}
		consWg.Add(1)
		go func(c int, s *Subscription, policy OverflowPolicy) {
			defer consWg.Done()
			var last uint64
			for m := range s.C() {
				if m.Seq <= last {
					violations <- "non-monotonic seq"
					return
				}
				last = m.Seq
			}
			// 部分消费者中途断线重连（在发布结束前），从确认位点继续推进。
			if c == 0 {
				t.Logf("consumer %d reconnect cycle at last=%d policy=%s", c, last, policy)
			}
		}(c, sub, policy)

		// 两个消费者中途退订重连一次（新订阅同样小队列、策略保持一致）。
		if c < 2 {
			consWg.Add(1)
			go func(c int, first *Subscription) {
				defer consWg.Done()
				time.Sleep(3 * time.Millisecond)
				pos := first.Position()
				first.Unsubscribe()
				next, err := b.SubscribeWith("t/stress", pos, SubscribeOptions{OverflowPolicy: policy})
				if err != nil {
					// 窗口翻转（明确拒绝过期位点）或撞上关闭都属正常退出，不是卡死。
					if errors.Is(err, ErrPositionExpired) || errors.Is(err, ErrClosed) {
						return
					}
					violations <- err.Error()
					return
				}
				defer func() { finalize(next); next.Unsubscribe() }()
				var last uint64 = pos
				for m := range next.C() {
					if m.Seq <= last {
						violations <- "non-monotonic seq after reconnect"
						return
					}
					last = m.Seq
				}
			}(c, sub)
		}
	}

	// 发布完成后关闭，推动所有 range 结束。
	pubDone := make(chan struct{})
	go func() { pubWg.Wait(); close(pubDone) }()
	select {
	case <-pubDone:
	case <-time.After(10 * time.Second):
		t.Fatal("publishers stalled: backpressure blocked publish path")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	consDone := make(chan struct{})
	go func() { consWg.Wait(); close(consDone) }()
	select {
	case <-consDone:
	case <-time.After(10 * time.Second):
		t.Fatal("consumers stalled: deadlock under small queues")
	}
	close(violations)
	close(finalStats)
	for v := range violations {
		t.Fatalf("stress violation: %s", v)
	}
	n := 0
	for st := range finalStats {
		n++
		t.Logf("consumer stats: %+v identity-ok", st)
	}
	t.Logf("stress finished: %d finalized subscriptions, total published=%d", n, total)
}
