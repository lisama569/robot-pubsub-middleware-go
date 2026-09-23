package pubsub

import (
	"context"
	"testing"
)

// TestResumeAfterDisconnect 订阅者确认部分消息后"断线"，用同 id 与已确认位点重连：
// 已确认的消息不得重复交付，未确认的消息必须从位点之后补齐。
func TestResumeAfterDisconnect(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	ctx := context.Background()

	s := mustSub(t, b, ctx, "sensor-1", "data.#", 16, OverflowBlock, 0)
	for i := uint64(1); i <= 5; i++ {
		mustPublish(t, b, ctx, "data.temp", i)
	}
	delivered := drainN(t, s, 5)
	// 消费并确认前 3 条（committed=3），第 4、5 条收到但未确认。
	if err := s.Ack(delivered[2].Position); err != nil {
		t.Fatal(err)
	}
	committed := s.CommittedPosition()
	t.Logf("[resume] before disconnect: delivered up to pos=%d, committed=%d (pos 4,5 unacked)",
		delivered[4].Position, committed)

	// 模拟断线：退订。
	if err := b.Unsubscribe("sensor-1"); err != nil {
		t.Fatal(err)
	}
	ch := s.Deliveries()
	if d, ok := <-ch; ok {
		t.Logf("[resume] buffered pos=%d still readable after unsubscribe (drain)", d.Position)
	}

	// 断线期间又有新消息（pos=6,7）。
	mustPublish(t, b, ctx, "data.temp", 6)
	mustPublish(t, b, ctx, "data.humidity", 7)

	// 重连：从 committed=3 续传，应收到 pos 4,5,6,7（按模式重放历史再接新消息）。
	s2 := mustSub(t, b, ctx, "sensor-1", "data.#", 16, OverflowBlock, committed)
	got := drainN(t, s2, 4)
	for i, d := range got {
		t.Logf("[resume] after reconnect #%d received pos=%d seq=%d topic=%s (all must be > committed=%d)",
			i+1, d.Position, d.Message.Seq, d.Message.Topic, committed)
		if d.Position <= committed {
			t.Fatalf("already-acked pos=%d redelivered after resume", d.Position)
		}
	}
	if got[0].Position != 4 || got[3].Position != 7 {
		t.Fatalf("resume window wrong: %d..%d", got[0].Position, got[3].Position)
	}
}

// TestAckRollbackAndAhead 位点回退与越前必须被明确拒绝且原因可区分。
func TestAckRollbackAndAhead(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	ctx := context.Background()
	s := mustSub(t, b, ctx, "s", "a.b", 8, OverflowBlock, 0)

	mustPublish(t, b, ctx, "a.b", 1)
	mustPublish(t, b, ctx, "a.b", 2)
	d := drainN(t, s, 2)
	if err := s.Ack(d[1].Position); err != nil {
		t.Fatal(err)
	}
	t.Logf("[ack] committed advanced to pos=%d seq=%d", d[1].Position, d[1].Message.Seq)

	err := s.Ack(d[0].Position) // 回退
	assertReason(t, err, ReasonPositionRollback, "ack rollback")

	err = s.Ack(d[1].Position) // 幂等重复 ACK
	if err != nil {
		t.Fatalf("idempotent ack should pass: %v", err)
	}
	t.Logf("[ack] idempotent ack pos=%d accepted", d[1].Position)

	err = s.Ack(999) // 越前：从未投递
	assertReason(t, err, ReasonPositionAhead, "ack ahead")
}

// TestResumeRejectsExpiredAndAheadPosition 续传位点过期（超出保留窗口）与越前都要拒绝。
func TestResumeRejectsExpiredAndAheadPosition(t *testing.T) {
	ctx := context.Background()
	b := NewBroker(WithRetention(3))
	for i := uint64(1); i <= 5; i++ {
		mustPublish(t, b, ctx, "a.b", i)
	}
	// 日志只剩 pos 3,4,5；从 pos=0/1 续传属于过期缺口。
	_, err := b.Subscribe(ctx, "late", mustPattern(t, "a.b"), 8, OverflowBlock, 1)
	assertReason(t, err, ReasonPositionExpired, "resume expired")

	_, err = b.Subscribe(ctx, "future", mustPattern(t, "a.b"), 8, OverflowBlock, 100)
	assertReason(t, err, ReasonPositionAhead, "resume ahead")

	// resumeFrom = oldest-1 (=2) 合法：恰好不丢任何现存消息。
	s := mustSub(t, b, ctx, "ok", "a.b", 8, OverflowBlock, 2)
	got := drainN(t, s, 3)
	t.Logf("[resume] resumeFrom=2 refilled positions=%d,%d,%d",
		got[0].Position, got[1].Position, got[2].Position)
}
