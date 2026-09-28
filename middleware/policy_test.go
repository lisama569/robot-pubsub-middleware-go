package middleware

import (
	"errors"
	"testing"
	"time"
)

// drain 接收 sub 上当前所有已入队消息：遇到通道关闭即结束，
// 或空闲超过 idle 即认为队列已排空（适用于订阅仍存活的场景）。
func drain(t *testing.T, sub *Subscription, idle time.Duration) []Message {
	t.Helper()
	var got []Message
	for {
		select {
		case m, ok := <-sub.C():
			if !ok {
				return got
			}
			got = append(got, m)
		case <-time.After(idle):
			return got
		}
	}
}

// PolicyDropNewest：队列满时留着先来的、拒收新到的；
// 发布方不被阻塞，且丢失全部计入 DroppedNewest 而非 DroppedOldest。
func TestBackpressureDropNewest(t *testing.T) {
	b := NewBroker(Config{QueueSize: 4})
	defer b.Close()

	slow, err := b.SubscribeWith("robot/telemetry", 0, SubscribeOptions{Policy: PolicyDropNewest})
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Unsubscribe()
	if got := slow.Policy(); got != PolicyDropNewest {
		t.Fatalf("policy = %d, want PolicyDropNewest", got)
	}

	fast, err := b.SubscribeWith("robot/telemetry", 0, SubscribeOptions{Policy: PolicyDropOldest})
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Unsubscribe()

	const n = 10
	fastDone := make(chan []Message, 1)
	gotOne := make(chan struct{}, n)
	go func() {
		got := make([]Message, 0, n)
		for m := range fast.C() {
			got = append(got, m)
			gotOne <- struct{}{}
		}
		fastDone <- got
	}()

	// 发布方逐条发布，只等“快订阅者”收到；慢订阅者不消费，队列必满。
	for i := 0; i < n; i++ {
		if _, err := b.Publish("robot/telemetry", Version1, []byte{byte(i)}); err != nil {
			t.Fatalf("publish i=%d: %v", i, err)
		}
		select {
		case <-gotOne:
		case <-time.After(2 * time.Second):
			t.Fatalf("publisher blocked at i=%d", i)
		}
	}
	t.Logf("publisher pushed %d without blocking on the drop-newest subscriber", n)

	// 快订阅者使用 DropOldest 且实时消费，应一条不丢。
	fast.Unsubscribe()
	fastGot := <-fastDone
	if len(fastGot) != n {
		t.Fatalf("fast subscriber got %d, want %d", len(fastGot), n)
	}

	// 慢订阅者：前 4 条留在队列里，后 6 条被拒收。
	got := drain(t, slow, 50*time.Millisecond)
	if len(got) != 4 {
		t.Fatalf("slow kept %d messages, want 4", len(got))
	}
	for i, m := range got {
		if int(m.Payload[0]) != i {
			t.Fatalf("drop-newest violated at %d: payload=%d want %d (oldest must be kept)", i, m.Payload[0], i)
		}
	}
	st := slow.Stats()
	t.Logf("slow drop-newest stats: delivered=%d droppedNewest=%d droppedOldest=%d",
		st.Delivered, st.DroppedNewest, st.DroppedOldest)
	if st.Delivered != 4 || st.DroppedNewest != 6 || st.DroppedOldest != 0 {
		t.Fatalf("slow stats = %+v, want delivered=4 droppedNewest=6 droppedOldest=0", st)
	}
	// 对账：4 条留队 + 6 条因满拒收 = 10 条匹配消息。
	if st.Delivered+st.DroppedNewest != n {
		t.Fatalf("accounting mismatch: %+v", st)
	}
}

// 两种丢法分别计数：同一 Broker 上两个不同策略的慢订阅者，各丢各的，互不混淆。
func TestBackpressureCountersSeparated(t *testing.T) {
	b := NewBroker(Config{QueueSize: 3})
	defer b.Close()

	oldest, err := b.SubscribeWith("t", 0, SubscribeOptions{Policy: PolicyDropOldest})
	if err != nil {
		t.Fatal(err)
	}
	defer oldest.Unsubscribe()
	newest, err := b.SubscribeWith("t", 0, SubscribeOptions{Policy: PolicyDropNewest})
	if err != nil {
		t.Fatal(err)
	}
	defer newest.Unsubscribe()

	const n = 8
	for i := 0; i < n; i++ {
		if _, err := b.Publish("t", Version1, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	so := oldest.Stats()
	sn := newest.Stats()
	t.Logf("drop-oldest sub: %+v; drop-newest sub: %+v", so, sn)
	if so.DroppedOldest != n-3 || so.DroppedNewest != 0 || so.Delivered != n {
		t.Fatalf("drop-oldest stats wrong: %+v", so)
	}
	if sn.DroppedNewest != n-3 || sn.DroppedOldest != 0 || sn.Delivered != 3 {
		t.Fatalf("drop-newest stats wrong: %+v", sn)
	}
}

// 默认策略为 DropOldest；未知策略被明确拒绝并返回 ErrInvalidPolicy。
func TestSubscribePolicyValidation(t *testing.T) {
	b := NewBroker(Config{})
	defer b.Close()

	def, err := b.Subscribe("t", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer def.Unsubscribe()
	if def.Policy() != PolicyDropOldest {
		t.Fatalf("default policy = %d, want PolicyDropOldest", def.Policy())
	}
	def2, err := b.SubscribeWith("t", 0, SubscribeOptions{Policy: PolicyDefault})
	if err != nil {
		t.Fatal(err)
	}
	defer def2.Unsubscribe()
	if def2.Policy() != PolicyDropOldest {
		t.Fatalf("PolicyDefault normalized = %d, want PolicyDropOldest", def2.Policy())
	}
	_, err = b.SubscribeWith("t", 0, SubscribeOptions{Policy: BackpressurePolicy(99)})
	if !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("invalid policy: got %v, want ErrInvalidPolicy", err)
	}
	t.Logf("invalid policy rejected with ErrInvalidPolicy: %v", err)
}
