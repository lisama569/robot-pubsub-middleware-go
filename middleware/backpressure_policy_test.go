package middleware

import (
	"testing"
	"time"
)

// drop-newest 策略：队列满后“留先来的、新的先不收”。
// 被拒的是新消息，队列中最早入队的消息原样保留；两类丢弃计数分开。
func TestBackpressureDropNewest(t *testing.T) {
	b := NewBroker(Config{QueueSize: 3})
	defer b.Close()

	sub, err := b.SubscribeWith("t/data", 0, SubscribeOptions{OverflowPolicy: PolicyDropNewest})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	if sub.Policy() != PolicyDropNewest {
		t.Fatalf("policy = %v, want drop-newest", sub.Policy())
	}

	const n = 10
	for i := 0; i < n; i++ {
		if _, err := b.Publish("t/data", Version1, []byte{byte(i)}); err != nil {
			t.Fatalf("publish %d blocked/failed: %v", i, err)
		}
	}

	// 队列里应是最早的 3 条（payload 0,1,2），后 7 条被拒收。
	got := drain(sub, 3)
	for i, m := range got {
		if int(m.Payload[0]) != i {
			t.Fatalf("drop-newest violated at %d: payload=%d want %d", i, m.Payload[0], i)
		}
		t.Logf("kept oldest: payload=%d seq=%d", m.Payload[0], m.Seq)
	}

	st := sub.Stats()
	// 对账恒等式：Matched = Delivered + DroppedOldest + DroppedNewest + DroppedLate + Duplicates
	if !st.Reconciled() {
		t.Fatalf("stats identity violated: %+v", st)
	}
	if st.Matched != n || st.Delivered != 3 || st.DroppedNewest != 7 || st.DroppedOldest != 0 {
		t.Fatalf("stats = %+v, want matched=%d delivered=3 droppedNewest=7 droppedOldest=0", st, n)
	}
	t.Logf("drop-newest stats: %+v (identity holds)", st)
}

// 两种策略的订阅并存：慢订阅者各按各的策略丢弃，互不干扰，发布方不被拖住。
func TestBackpressurePoliciesIsolated(t *testing.T) {
	b := NewBroker(Config{QueueSize: 2})
	defer b.Close()

	oldestSub, err := b.SubscribeWith("t/x", 0, SubscribeOptions{OverflowPolicy: PolicyDropOldest})
	if err != nil {
		t.Fatal(err)
	}
	defer oldestSub.Unsubscribe()
	newestSub, err := b.SubscribeWith("t/x", 0, SubscribeOptions{OverflowPolicy: PolicyDropNewest})
	if err != nil {
		t.Fatal(err)
	}
	defer newestSub.Unsubscribe()

	const n = 6
	for i := 0; i < n; i++ {
		if _, err := b.Publish("t/x", Version1, []byte{byte(i)}); err != nil {
			t.Fatalf("publish blocked at %d: %v", i, err)
		}
	}

	oldest := drain(oldestSub, 2)
	newest := drain(newestSub, 2)
	if oldest[0].Payload[0] != 4 || oldest[1].Payload[0] != 5 {
		t.Fatalf("drop-oldest queue = %d,%d, want 4,5", oldest[0].Payload[0], oldest[1].Payload[0])
	}
	if newest[0].Payload[0] != 0 || newest[1].Payload[0] != 1 {
		t.Fatalf("drop-newest queue = %d,%d, want 0,1", newest[0].Payload[0], newest[1].Payload[0])
	}
	t.Logf("isolated: drop-oldest kept %v, drop-newest kept %v",
		[]byte{oldest[0].Payload[0], oldest[1].Payload[0]},
		[]byte{newest[0].Payload[0], newest[1].Payload[0]})

	os := oldestSub.Stats()
	ns := newestSub.Stats()
	if os.DroppedOldest != 4 || os.DroppedNewest != 0 {
		t.Fatalf("oldest-sub stats = %+v, want droppedOldest=4 droppedNewest=0", os)
	}
	if ns.DroppedNewest != 4 || ns.DroppedOldest != 0 {
		t.Fatalf("newest-sub stats = %+v, want droppedNewest=4 droppedOldest=0", ns)
	}
	// 两个慢订阅者都不影响彼此，且所有消息都计入了各自的命中总数。
	if os.Matched != n || ns.Matched != n {
		t.Fatalf("matched counters: oldest=%d newest=%d, want %d", os.Matched, ns.Matched, n)
	}
	t.Logf("policy isolation confirmed: oldest stats %+v; newest stats %+v", os, ns)
}

// drop-newest 下队列腾出空间后，后续新消息必须恢复入队（拒收不是永久关闭）。
func TestBackpressureDropNewestRecovers(t *testing.T) {
	b := NewBroker(Config{QueueSize: 2})
	defer b.Close()

	sub, _ := b.SubscribeWith("t/y", 0, SubscribeOptions{OverflowPolicy: PolicyDropNewest})
	defer sub.Unsubscribe()

	pub := func(i int) {
		if _, err := b.Publish("t/y", Version1, []byte{byte(i)}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	pub(0)
	pub(1)
	pub(2)       // 被拒收
	recv(t, sub) // 腾出一个位置：消费 0
	pub(3)       // 应正常入队
	rest := drain(sub, 2)
	if rest[0].Payload[0] != 1 || rest[1].Payload[0] != 3 {
		t.Fatalf("recovery order = %d,%d, want 1,3", rest[0].Payload[0], rest[1].Payload[0])
	}
	st := sub.Stats()
	if st.Delivered != 3 || st.DroppedNewest != 1 || st.Matched != 4 {
		t.Fatalf("stats = %+v, want delivered=3 droppedNewest=1 matched=4", st)
	}
	t.Logf("drop-newest recovered after drain: %+v", st)
}

// drain 在超时时间内读取至多 n 条消息。
func drain(sub *Subscription, n int) []Message {
	out := make([]Message, 0, n)
	for len(out) < n {
		select {
		case m, ok := <-sub.C():
			if !ok {
				return out
			}
			out = append(out, m)
		case <-time.After(time.Second):
			return out
		}
	}
	return out
}
