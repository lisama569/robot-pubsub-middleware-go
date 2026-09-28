package middleware

import (
	"testing"
	"time"
)

// 慢订阅者队列超限：按确定策略淘汰最旧消息，统计可区分；
// 同时快订阅者与发布方不得被阻塞。
func TestBackpressureDropOldest(t *testing.T) {
	b := NewBroker(Config{QueueSize: 4})
	defer b.Close()

	slow, err := b.Subscribe("robot/telemetry", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Unsubscribe()
	fast, err := b.Subscribe("robot/telemetry", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Unsubscribe()

	const n = 10
	// 快订阅者逐条同步消费：每收到一条发一个信号，发布方等信号后再发下一条，
	// 从而确定性地保证“跟得上的订阅者一条不丢”。
	fastGot := make([]Message, 0, n)
	acked := make(chan struct{})
	go func() {
		for m := range fast.C() {
			fastGot = append(fastGot, m)
			acked <- struct{}{}
		}
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			if _, err := b.Publish("robot/telemetry", Version1, []byte{byte(i)}); err != nil {
				t.Errorf("publish blocked or failed at i=%d: %v", i, err)
				return
			}
			select {
			case <-acked:
			case <-time.After(2 * time.Second):
				t.Errorf("fast subscriber fell behind at i=%d", i)
				return
			}
		}
	}()
	select {
	case <-done:
		t.Logf("publisher completed %d publishes without blocking on slow subscriber", n)
	case <-time.After(5 * time.Second):
		t.Fatal("publisher blocked by slow subscriber")
	}

	// 快订阅者实时消费，应收到全部 n 条且顺序一致。
	if len(fastGot) != n {
		t.Fatalf("fast subscriber received %d messages, want %d", len(fastGot), n)
	}
	for i, m := range fastGot {
		if int(m.Payload[0]) != i {
			t.Fatalf("fast subscriber: got payload %d at index %d (seq=%d)", m.Payload[0], i, m.Seq)
		}
	}
	fst := fast.Stats()
	t.Logf("fast stats: delivered=%d droppedOldest=%d", fst.Delivered, fst.DroppedOldest)
	if fst.Delivered != n || fst.DroppedOldest != 0 {
		t.Fatalf("fast subscriber affected by slow one: %+v", fst)
	}

	// 慢订阅者队列容量 4：10 条全部入队（Delivered=10），
	// 其中 6 条最旧的在被消费前被淘汰（DroppedOldest=6），队列中只剩最新 4 条。
	sst := slow.Stats()
	t.Logf("slow stats: delivered=%d droppedOldest=%d (queue=4, published=%d)",
		sst.Delivered, sst.DroppedOldest, n)
	if sst.Delivered != 10 || sst.DroppedOldest != 6 {
		t.Fatalf("slow subscriber stats = %+v, want delivered=10 droppedOldest=6", sst)
	}
	for i := 6; i < n; i++ {
		m := recv(t, slow)
		t.Logf("slow recv after overflow: seq=%d payload=%d", m.Seq, m.Payload[0])
		if int(m.Payload[0]) != i {
			t.Fatalf("drop-oldest policy violated: got payload %d, want %d", m.Payload[0], i)
		}
	}
}

// 关闭语义：关闭前已入队的消息可排空；关闭后发布返回 ErrClosed。
func TestCloseSemantics(t *testing.T) {
	b := NewBroker(Config{})
	sub, err := b.Subscribe("a/#", 0)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := b.Publish("a/b", Version1, []byte("queued-before-close"))
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal("Close must be idempotent")
	}
	// 已入队消息仍可读取，随后通道关闭。
	m := recv(t, sub)
	t.Logf("drained after close: seq=%d (published seq=%d)", m.Seq, seq)
	if _, ok := <-sub.C(); ok {
		t.Fatal("channel should be closed after draining")
	}
	if _, err := b.Publish("a/b", Version1, nil); err != ErrClosed {
		t.Fatalf("publish after close: got %v, want ErrClosed", err)
	}
	if _, err := b.Subscribe("a/#", 0); err != ErrClosed {
		t.Fatalf("subscribe after close: got %v, want ErrClosed", err)
	}
	t.Logf("post-close publish/subscribe rejected with ErrClosed; queued message drained, channel closed")
}
