package pubsub

import (
	"context"
	"testing"
)

// TestSlowSubscriberDoesNotBlockOthers 慢订阅者（队列深度 1、不读取、drop_newest）
// 不得阻塞发布方与其他订阅者。
func TestSlowSubscriberDoesNotBlockOthers(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	ctx := context.Background()

	slow := mustSub(t, b, ctx, "slow", "t", 1, OverflowDropNewest, 0)
	fast := mustSub(t, b, ctx, "fast", "t", 16, OverflowBlock, 0)

	done := make(chan uint64, 1)
	go func() {
		var last uint64
		for i := uint64(1); i <= 20; i++ {
			pos, err := b.Publish(ctx, newTestMsg("t", i))
			if err != nil {
				t.Errorf("publish blocked/failed by slow sub: %v", err)
				return
			}
			last = pos
		}
		done <- last
	}()
	select {
	case last := <-done:
		t.Logf("[backpressure] 20 publishes completed despite slow subscriber, last position=%d", last)
	case <-timeout():
		t.Fatal("publisher blocked by slow subscriber")
	}

	// fast 订阅者仍可收全 20 条。
	all := drainN(t, fast, 20)
	t.Logf("[backpressure] fast subscriber received %d/20; slow counters=%+v", len(all), slow.Counters())
	if len(all) != 20 {
		t.Fatalf("fast subscriber lost messages: %d/20", len(all))
	}
	if slow.Counters().DroppedNewest == 0 {
		t.Fatal("slow subscriber should have DroppedNewest > 0")
	}
}

// TestOverflowStrategies 分别验证两种丢弃策略的统计可区分，且被丢弃位点形成 gap，
// ACK 不得静默越过（必须先续传补齐）。
func TestOverflowStrategies(t *testing.T) {
	t.Run("drop_newest", func(t *testing.T) {
		b := NewBroker()
		defer b.Close()
		ctx := context.Background()
		s := mustSub(t, b, ctx, "s", "t", 2, OverflowDropNewest, 0)

		// 发布 5 条但完全不读：队列容量 2。
		var positions []uint64
		for i := uint64(1); i <= 5; i++ {
			positions = append(positions, mustPublish(t, b, ctx, "t", i))
		}
		waitUntil(t, func() bool {
			c := s.Counters()
			return c.Enqueued == 5 && c.DroppedNewest == 3 && c.Delivered == 2
		})
		c := s.Counters()
		t.Logf("[drop_newest] enqueued=%d delivered=%d droppedNewest=%d droppedOldest=%d",
			c.Enqueued, c.Delivered, c.DroppedNewest, c.DroppedOldest)
		if c.DroppedNewest != 3 || c.DroppedOldest != 0 {
			t.Fatalf("drop_newest counters wrong: %+v", c)
		}
		// 队列里是最旧的 pos 1,2。
		d1 := recvOne(t, s)
		d2 := recvOne(t, s)
		t.Logf("[drop_newest] consumer actually received pos=%d,%d (newest dropped)", d1.Position, d2.Position)
		if d1.Position != 1 || d2.Position != 2 {
			t.Fatalf("expected oldest pos 1,2 in queue, got %d,%d", d1.Position, d2.Position)
		}
		if err := s.Ack(2); err != nil {
			t.Fatalf("ack up to 2 should pass: %v", err)
		}
		// drop_newest 保留的是最旧连续段：消费方只见过 pos<=2，pos 3..5 在 highWater 之后，
		// 因而越界确认表现为 position_ahead（可区分；这些消息仍可续传补齐）。
		err := s.Ack(5)
		assertReason(t, err, ReasonPositionAhead, "ack past newest-dropped highwater")
	})

	t.Run("drop_oldest", func(t *testing.T) {
		b := NewBroker()
		defer b.Close()
		ctx := context.Background()
		s := mustSub(t, b, ctx, "s", "t", 2, OverflowDropOldest, 0)

		for i := uint64(1); i <= 5; i++ {
			mustPublish(t, b, ctx, "t", i)
		}
		waitUntil(t, func() bool {
			c := s.Counters()
			return c.Enqueued == 5 && c.DroppedOldest == 3 && c.Delivered == 5
		})
		c := s.Counters()
		t.Logf("[drop_oldest] enqueued=%d attempted-deliveries=%d droppedNewest=%d droppedOldest=%d",
			c.Enqueued, c.Delivered, c.DroppedNewest, c.DroppedOldest)
		if c.DroppedOldest != 3 || c.DroppedNewest != 0 {
			t.Fatalf("drop_oldest counters wrong: %+v", c)
		}
		d1 := recvOne(t, s)
		d2 := recvOne(t, s)
		t.Logf("[drop_oldest] consumer actually received pos=%d,%d (older pos 1,2,3 evicted)", d1.Position, d2.Position)
		if d1.Position != 4 || d2.Position != 5 {
			t.Fatalf("expected newest pos 4,5 in queue, got %d,%d", d1.Position, d2.Position)
		}
		// 消费方见过 pos=5（highWater=5），但 1,2,3 被挤出形成洞：
		// 从 0 确认到 5 必须被 position_gap 明确拒绝。
		err := s.Ack(5)
		assertReason(t, err, ReasonPositionGap, "ack over evicted gap")
	})
}

// TestGapRefillByResume 被背压挤出旧消息产生缺口后，按当前 committed 重连可补齐，随后 ACK 成功。
func TestGapRefillByResume(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	ctx := context.Background()
	// depth=1 + drop_oldest：连发 4 条，队列最终只剩 pos=4，pos 1,2,3 形成缺口。
	s := mustSub(t, b, ctx, "s", "t", 1, OverflowDropOldest, 0)

	for i := uint64(1); i <= 4; i++ {
		mustPublish(t, b, ctx, "t", i)
	}
	waitUntil(t, func() bool { return s.Counters().DroppedOldest == 3 })
	d := recvOne(t, s) // 只有 pos=4
	t.Logf("[gap-refill] consumer only holds pos=%d; pos 1,2,3 were evicted", d.Position)
	err := s.Ack(d.Position) // committed=0 -> 跨越缺口
	assertReason(t, err, ReasonPositionGap, "gap before refill")

	committed := s.CommittedPosition() // 仍为 0，位点不被破坏
	if err := b.Unsubscribe("s"); err != nil {
		t.Fatal(err)
	}
	s2 := mustSub(t, b, ctx, "s", "t", 8, OverflowBlock, committed)
	refilled := drainN(t, s2, 4) // 重放 pos 1,2,3,4
	t.Logf("[gap-refill] reconnected from committed=%d, refilled pos=%d,%d,%d,%d",
		committed, refilled[0].Position, refilled[1].Position,
		refilled[2].Position, refilled[3].Position)
	if err := s2.Ack(4); err != nil {
		t.Fatalf("ack after refill should pass: %v", err)
	}
	t.Logf("[gap-refill] ack pos=4 accepted after refill, committed=%d", s2.CommittedPosition())
}

func recvOne(t *testing.T, s *Subscription) Delivery {
	t.Helper()
	select {
	case d, ok := <-s.Deliveries():
		if !ok {
			t.Fatal("channel closed unexpectedly")
		}
		return d
	case <-timeout():
		t.Fatal("timeout waiting delivery")
	}
	return Delivery{}
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := timeout()
	for {
		if cond() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("condition not met before timeout")
		default:
		}
	}
}
