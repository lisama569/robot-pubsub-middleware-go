package pubsub

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// TestConcurrentLifecycle 发布、订退、ACK、关闭高度并发，要求无死锁、无 panic、无 race。
func TestConcurrentLifecycle(t *testing.T) {
	b := NewBroker()
	ctx := context.Background()

	const producers = 4
	const msgsEach = 200
	var wg sync.WaitGroup

	// 多个发布者：不同 producer 名，各自序列号独立。
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			for i := uint64(1); i <= msgsEach; i++ {
				m := newTestMsg("robot.arm.temp", i)
				m.Producer = fmt.Sprintf("p-%d", pid)
				if _, err := b.Publish(ctx, m); err != nil {
					// 关闭后的发布必须得到明确的 broker_closed，而不是 panic/其他错误。
					if re, ok := AsRejectError(err); !ok || re.Reason != ReasonBrokerClosed {
						t.Errorf("unexpected publish error: %v", err)
					}
					return
				}
			}
		}(p)
	}

	// 一批长期订阅者，边收边 ACK。
	const liveConsumers = 6
	for c := 0; c < liveConsumers; c++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			id := fmt.Sprintf("live-%d", cid)
			s, err := b.Subscribe(ctx, id, mustPattern(t, "robot.#"), 32, OverflowDropOldest, 0)
			if err != nil {
				return // 可能在关闭后才订阅
			}
			n := 0
			for d := range s.Deliveries() {
				// 尽力确认：在 drop_oldest 满队列背压下，位点可能被挤出形成缺口，
				// 此时 gap/ahead/closed 都是中间件明确告知的正常结果；
				// 只有 rollback（收到的位点倒退）代表真实的顺序破坏，不允许出现。
				if err := s.Ack(d.Position); err != nil {
					if re, ok := AsRejectError(err); ok {
						if re.Reason == ReasonPositionRollback {
							t.Errorf("position order violated (rollback): %v", err)
						}
					} else {
						t.Errorf("unexpected non-reject ack error: %v", err)
					}
				}
				n++
			}
			t.Logf("[lifecycle] %s drained %d deliveries after close (buffered messages preserved)", id, n)
		}(c)
	}

	// 一批反复订退的短命订阅者，与发布、关闭并发。
	for c := 0; c < 8; c++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			for k := 0; k < 20; k++ {
				id := fmt.Sprintf("flip-%d-%d", cid, k)
				s, err := b.Subscribe(ctx, id, mustPattern(t, "robot.*.temp"), 8, OverflowBlock, 0)
				if err != nil {
					return
				}
				go func() {
					for range s.Deliveries() {
					}
				}()
				if k%2 == 0 {
					_ = b.Unsubscribe(id)
				} else {
					_ = s.Close()
				}
			}
		}(c)
	}

	// 中途并发调用 Close 多次（必须幂等、安全）。
	go func() {
		for i := 0; i < 5; i++ {
			_ = b.Close()
		}
	}()

	wg.Wait()
	if err := b.Close(); err != nil {
		t.Fatalf("final close: %v", err)
	}
	t.Logf("[lifecycle] concurrent publish/subscribe/unsubscribe/close finished; stats=%+v", b.Stats())

	// 关闭后的所有写操作必须得到明确拒绝。
	if _, err := b.Publish(ctx, newTestMsg("a.b", 1)); !isReason(err, ReasonBrokerClosed) {
		t.Fatalf("publish after close: %v", err)
	}
	if _, err := b.Subscribe(ctx, "x", mustPattern(t, "a.b"), 4, OverflowBlock, 0); !isReason(err, ReasonBrokerClosed) {
		t.Fatalf("subscribe after close: %v", err)
	}
}

// TestClosePreservesBufferedMessages 关闭时已进入投递通道的消息确定保留、可读尽，
// 之后通道关闭；关闭后 ACK 被明确拒绝。
func TestClosePreservesBufferedMessages(t *testing.T) {
	b := NewBroker()
	ctx := context.Background()
	s := mustSub(t, b, ctx, "s", "t", 10, OverflowBlock, 0)
	for i := uint64(1); i <= 5; i++ {
		mustPublish(t, b, ctx, "t", i)
	}
	// 等 5 条全部推入投递通道后再读，确保关闭时它们都属于"已缓冲"而非 inbox 待推送。
	waitUntil(t, func() bool { return s.Counters().Delivered == 5 })
	d := drainN(t, s, 3) // 读走 3 条但不 ACK
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// 缓冲中剩余 2 条仍可读尽。
	rest := 0
	for range s.Deliveries() {
		rest++
	}
	if rest != 2 {
		t.Fatalf("expected 2 buffered messages preserved, got %d", rest)
	}
	t.Logf("[close] pre-close buffered messages readable: consumed=%d, rest=%d, channel then closed", len(d), rest)

	if err := s.Ack(3); !isReason(err, ReasonSubscriptionClosed) {
		t.Fatalf("ack after close should be subscription_closed, got %v", err)
	}
	t.Logf("[close] ack after close REJECTED reason=%s", ReasonSubscriptionClosed)
}

// TestCloseDiscardsUndeliveredInbox 使用 block 策略且队列深度极小：制造一批还压在 inbox、
// 未进入投递通道的消息，关闭后它们必须确定作废——已缓冲的可读尽，inbox 中的不再补发。
func TestCloseDiscardsUndeliveredInbox(t *testing.T) {
	b := NewBroker()
	ctx := context.Background()
	// depth=1：读走首条后 dispatch 会把第 2 条推进通道并停住，第 3 条及之后滞留 inbox。
	s := mustSub(t, b, ctx, "s", "t", 1, OverflowBlock, 0)
	for i := uint64(1); i <= 10; i++ {
		mustPublish(t, b, ctx, "t", i)
	}
	first := recvOne(t, s) // 通道腾出，dispatch 塞入 pos2；pos3..10 仍在 inbox 途中
	waitUntil(t, func() bool { return s.Counters().Delivered >= 2 })
	t.Logf("[close-inbox] consumed pos=%d; channel now holds one more, pos3..10 in-flight in inbox", first.Position)

	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	var kept []uint64
	for d := range s.Deliveries() {
		kept = append(kept, d.Position)
	}
	// 关闭时已进入投递通道缓冲的那一条保留；inbox 中的消息确定作废，不会补发。
	if len(kept) != 1 || kept[0] != 2 {
		t.Fatalf("expected exactly buffered pos=2 kept, got %v", kept)
	}
	t.Logf("[close-inbox] after close: buffered=%v kept, inbox in-flight pos3..10 discarded deterministically", kept)
}

// TestPublishRejectsBadVersionsEndToEnd 通过 Broker 发布未知/不兼容版本消息，必须被拒绝且可解释。
func TestPublishRejectsBadVersionsEndToEnd(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	ctx := context.Background()
	s := mustSub(t, b, ctx, "s", "a.b", 4, OverflowBlock, 0)

	bad := []struct {
		ver    Version
		reason RejectReason
	}{
		{Version{}, ReasonUnknownVersion},
		{Version{2, 0}, ReasonUnknownVersion},
		{Version{0, 1}, ReasonIncompatibleVersion},
	}
	for _, c := range bad {
		m := Message{Topic: mustTopic(t, "a.b"), Seq: 1, Version: c.ver, Producer: "p"}
		_, err := b.Publish(ctx, m)
		assertReason(t, err, c.reason, fmt.Sprintf("publish %s", c.ver))
	}

	// 一条合法消息仍可正常投递，进程不受影响。
	pos := mustPublish(t, b, ctx, "a.b", 1)
	d := recvOne(t, s)
	t.Logf("[e2e-version] valid msg delivered pos=%d seq=%d ver=%s after rejects (no crash)",
		d.Position, d.Message.Seq, d.Message.Version)
	if d.Position != pos {
		t.Fatalf("position mismatch %d vs %d", d.Position, pos)
	}
}

func isReason(err error, r RejectReason) bool {
	re, ok := AsRejectError(err)
	return ok && re.Reason == r
}
