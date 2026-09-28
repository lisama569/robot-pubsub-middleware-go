package middleware

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// 混合背压并发压力：多发布方并发发布，多订阅方以两种策略、极小队列订阅并持续消费；
// 中途并发退订与关闭。要求：不卡死、不 panic，每个订阅的去向账目与匹配消息总数对得上。
func TestConcurrentMixedBackpressure(t *testing.T) {
	b := NewBroker(Config{QueueSize: 2, Retention: 4096})

	const (
		publishers     = 4
		pubEach        = 300 // 共发布 1200 条到同一主题
		totalPublished = publishers * pubEach
		subscribers    = 6
	)

	var pubWg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		pubWg.Add(1)
		go func(p int) {
			defer pubWg.Done()
			for i := 0; i < pubEach; i++ {
				if _, err := b.Publish("robot/telemetry", Version1, []byte{byte(p), byte(i)}); err != nil {
					if !errors.Is(err, ErrClosed) {
						t.Errorf("publisher %d: %v", p, err)
					}
					return
				}
			}
		}(p)
	}

	subs := make([]*Subscription, subscribers)
	var readWg sync.WaitGroup
	readCounts := make([]uint64, subscribers)
	for i := 0; i < subscribers; i++ {
		policy := PolicyDropOldest
		if i%2 == 1 {
			policy = PolicyDropNewest
		}
		s, err := b.SubscribeWith("robot/telemetry", 0, SubscribeOptions{Policy: policy})
		if err != nil {
			t.Fatal(err)
		}
		subs[i] = s
		readWg.Add(1)
		go func(id int, s *Subscription) {
			defer readWg.Done()
			var n uint64
			// 奇数号订阅消费慢一点，偶数号消费快一些，制造持续背压。
			for range s.C() {
				n++
				if id%2 == 1 && n%3 == 0 {
					time.Sleep(time.Microsecond)
				}
			}
			readCounts[id] = n
		}(i, s)
	}

	// 发布全部结束后再退订一半、关闭，避免关闭后发布数未知导致账目无法核对。
	pubDone := make(chan struct{})
	go func() { pubWg.Wait(); close(pubDone) }()
	select {
	case <-pubDone:
	case <-time.After(15 * time.Second):
		t.Fatal("publishers deadlocked under tiny queues")
	}
	t.Logf("all %d publishes completed with queue size 2", totalPublished)

	// 确定性等待续传泵把所有存活订阅的积压补完（慢订阅消费会腾出空位），
	// 避免“发布刚结束、5ms ticker 尚未补完”导致把在途消息误判成丢失。
	dl := time.Now().Add(10 * time.Second)
	for b.hasPendingReplay() {
		if time.Now().After(dl) {
			t.Fatal("replay pump did not finish draining backlog")
		}
		time.Sleep(time.Millisecond)
	}
	t.Logf("all live subscribers finished replaying backlog")

	for i := 0; i < subscribers; i += 2 {
		subs[i].Unsubscribe()
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

	// 对账：每个订阅关闭前所有 1200 条匹配消息都必须在四类去向中有明确归属。
	// Delivered 含被挤掉的；读到 = Delivered - DroppedOldest。
	for i, s := range subs {
		st := s.Stats()
		accounted := st.Delivered + st.DroppedNewest + st.DroppedLate
		t.Logf("sub %d policy=%d read=%d stats=%+v", i, s.Policy(), readCounts[i], st)
		if accounted != totalPublished {
			t.Errorf("sub %d accounting: delivered+newest+late=%d != %d (stats=%+v)",
				i, accounted, totalPublished, st)
		}
		if got := st.Delivered - st.DroppedOldest; got != readCounts[i] {
			t.Errorf("sub %d read accounting: delivered-oldest=%d but read=%d",
				i, got, readCounts[i])
		}
		if st.Duplicates != 0 {
			t.Errorf("sub %d unexpected duplicates: %d", i, st.Duplicates)
		}
		// 两种丢法不得互相串。
		if s.Policy() == PolicyDropOldest && st.DroppedNewest != 0 {
			t.Errorf("sub %d drop-oldest counted newest drops: %d", i, st.DroppedNewest)
		}
		if s.Policy() == PolicyDropNewest && st.DroppedOldest != 0 {
			t.Errorf("sub %d drop-newest counted oldest drops: %d", i, st.DroppedOldest)
		}
	}
}

// 极小队列下高并发 + 并发重连续传：不卡死、不 panic，
// 续传完成后该订阅的最终位点与发布总数一致（不重不漏，重放窗口足够大）。
func TestConcurrentTinyQueueResume(t *testing.T) {
	b := NewBroker(Config{QueueSize: 1, Retention: 8192})
	defer b.Close()

	const total = 500
	var pubWg sync.WaitGroup
	for p := 0; p < 4; p++ {
		pubWg.Add(1)
		go func() {
			defer pubWg.Done()
			for i := 0; i < total/4; i++ {
				if _, err := b.Publish("t", Version1, nil); err != nil {
					t.Errorf("publish: %v", err)
					return
				}
			}
		}()
	}
	pubWg.Wait()

	// 多代订阅，每代只消费若干条就断线，下一代从确认位点懒续传，直到收齐。
	var pos uint64
	generation := 0
	deadline := time.Now().Add(15 * time.Second)
	for pos < total {
		if time.Now().After(deadline) {
			t.Fatalf("resume stuck at position=%d/%d", pos, total)
		}
		sub, err := b.Subscribe("t", pos)
		if err != nil {
			t.Fatal(err)
		}
		batch := uint64(37)
		var consumed uint64
		// 每代消费至多 batch 条；最后一代可能不足 batch，靠 idle 超时判定“已收齐”。
		idle := time.After(2 * time.Second)
	genLoop:
		for {
			select {
			case m := <-sub.C():
				if m.Seq <= pos {
					t.Fatalf("generation %d: duplicated seq=%d at position=%d", generation, m.Seq, pos)
				}
				pos = m.Seq
				consumed++
				idle = time.After(500 * time.Millisecond)
				if consumed >= batch {
					break genLoop
				}
			case <-idle:
				break genLoop
			}
		}
		if err := sub.Ack(pos); err != nil {
			t.Fatalf("generation %d ack position=%d: %v", generation, pos, err)
		}
		sub.Unsubscribe()
		generation++
	}
	if pos != total {
		t.Fatalf("final position=%d, want %d", pos, total)
	}
	t.Logf("tiny-queue resume chain: %d generations, final position=%d, no dup/loss", generation, pos)
}
