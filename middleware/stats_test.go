package middleware

import (
	"testing"
	"time"
)

// 消息去向汇总：每项语义清楚、能互相对得上。
// 对账关系（每个订阅独立）：
//
//	匹配消息总数 = Delivered + DroppedNewest + DroppedLate
//	被消费读到   = Delivered - DroppedOldest   （挤掉的旧消息确实入过队）
//
// 重复消息（非新的匹配消息）单独计入 Duplicates，不参与上式。
func TestStatsReconciliation(t *testing.T) {
	b := NewBroker(Config{QueueSize: 3, Retention: 64})

	sub, err := b.SubscribeWith("t", 0, SubscribeOptions{Policy: PolicyDropOldest})
	if err != nil {
		t.Fatal(err)
	}

	const n = 10
	for i := 0; i < n; i++ {
		if _, err := b.Publish("t", Version1, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	// 队列容量 3、完全不消费：Delivered=10（每条都入过队），DroppedOldest=7，
	// 实际只能读到最新的 3 条。
	read := drain(t, sub, 50*time.Millisecond)
	if len(read) != 3 {
		t.Fatalf("drained %d, want 3 (newest survived)", len(read))
	}
	if read[0].Seq != 8 || read[2].Seq != 10 {
		t.Fatalf("survivors = %d..%d, want 8..10", read[0].Seq, read[2].Seq)
	}

	// 退订后再发布：计入 DroppedLate，不再交付。
	sub.Unsubscribe()
	const late = 4
	for i := 0; i < late; i++ {
		if _, err := b.Publish("t", Version1, nil); err != nil {
			t.Fatal(err)
		}
	}
	st := sub.Stats()
	t.Logf("post-unsubscribe stats: %+v", st)
	if st.Delivered != n || st.DroppedOldest != n-3 || st.DroppedLate != late {
		t.Fatalf("stats = %+v, want delivered=%d droppedOldest=%d droppedLate=%d",
			st, n, n-3, late)
	}
	// 勾稽：新匹配消息总数 = 入队 + 拒收 + 关闭后丢弃。
	if st.Delivered+st.DroppedNewest+st.DroppedLate != n+late {
		t.Fatalf("accounting: %d+%d+%d != %d", st.Delivered, st.DroppedNewest, st.DroppedLate, n+late)
	}
	// 实际读到 = 入队 - 被挤掉。
	if uint64(len(read)) != st.Delivered-st.DroppedOldest {
		t.Fatalf("drain accounting: read=%d delivered-oldest=%d", len(read), st.Delivered-st.DroppedOldest)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	// Stats 在退订/关闭后仍可查询，且不变化（汇总定格在该订阅生命周期结束时）。
	if got := sub.Stats(); got != st {
		t.Fatalf("stats changed after close: %+v vs %+v", got, st)
	}
	t.Logf("stats remain queryable and frozen after unsubscribe/close: %+v", st)
}

// Broker 整体关闭后才到达的消息计入 DroppedLate；不同策略的订阅各自独立对账。
func TestStatsDroppedLateOnBrokerClose(t *testing.T) {
	b := NewBroker(Config{QueueSize: 2})
	subs := make([]*Subscription, 2)
	policies := []BackpressurePolicy{PolicyDropOldest, PolicyDropNewest}
	for i, p := range policies {
		s, err := b.SubscribeWith("t", 0, SubscribeOptions{Policy: p})
		if err != nil {
			t.Fatal(err)
		}
		subs[i] = s
	}
	if _, err := b.Publish("t", Version1, nil); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	for _, s := range subs {
		st := s.Stats()
		t.Logf("policy=%d stats after broker close: %+v", s.Policy(), st)
		if st.Delivered != 1 || st.DroppedOldest != 0 || st.DroppedNewest != 0 || st.DroppedLate != 0 {
			t.Fatalf("pre-close message must stay delivered: %+v", st)
		}
		// 关闭前入队的 1 条仍可排空，随后通道关闭。
		m, ok := <-s.C()
		if !ok || m.Seq != 1 {
			t.Fatalf("drain after close: ok=%v m=%+v", ok, m)
		}
		if _, ok := <-s.C(); ok {
			t.Fatal("channel should be closed after draining")
		}
	}
}

// 重复挡下与满队丢弃分开计数：重复消息只进 Duplicates，不混入丢弃项。
func TestStatsDuplicatesSeparated(t *testing.T) {
	b := NewBroker(Config{})
	defer b.Close()
	sub, err := b.Subscribe("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	if _, err := b.Publish("t", Version1, []byte("m")); err != nil {
		t.Fatal(err)
	}
	m := recv(t, sub)
	// 同一订阅内重复入队两条：只计 Duplicates，不入队、不算满队/迟到丢弃。
	sub.enqueue(m)
	sub.enqueue(m)
	st := sub.Stats()
	t.Logf("duplicate stats: %+v", st)
	if st.Duplicates != 2 || st.Delivered != 1 {
		t.Fatalf("stats = %+v, want duplicates=2 delivered=1", st)
	}
	if st.DroppedOldest+st.DroppedNewest+st.DroppedLate != 0 {
		t.Fatalf("duplicates leaked into drop counters: %+v", st)
	}
}
