package pubsub

import (
	"context"
	"testing"
)

func newTestMsg(topic string, seq uint64) Message {
	t, err := ParseTopic(topic)
	if err != nil {
		panic(err)
	}
	return Message{Topic: t, Seq: seq, Version: CurrentVersion, Producer: "arm-1", Payload: []byte("x")}
}

// TestWildcardFanout 发布到具体主题，验证多个通配订阅者各收各的，位点全局单调。
func TestWildcardFanout(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	ctx := context.Background()

	s1 := mustSub(t, b, ctx, "s1", "robot.*.temp", 8, OverflowBlock, 0)
	s2 := mustSub(t, b, ctx, "s2", "robot.#", 8, OverflowBlock, 0)
	s3 := mustSub(t, b, ctx, "s3", "robot.arm.vib", 8, OverflowBlock, 0)

	for i := uint64(1); i <= 3; i++ {
		pos, err := b.Publish(ctx, newTestMsg("robot.arm.temp", i))
		if err != nil {
			t.Fatalf("publish seq=%d: %v", i, err)
		}
		t.Logf("[fanout] published seq=%d -> global position=%d", i, pos)
	}

	// s1 与 s2 应各收到 3 条；s3 主题不匹配，收不到。
	d1 := drainN(t, s1, 3)
	d2 := drainN(t, s2, 3)
	for i, d := range d1 {
		t.Logf("[fanout] s1 received pos=%d seq=%d pattern=robot.*.temp", d.Position, d.Message.Seq)
		if d.Message.Seq != uint64(i+1) {
			t.Fatalf("s1 seq order broken: %d", d.Message.Seq)
		}
	}
	for _, d := range d2 {
		t.Logf("[fanout] s2 received pos=%d seq=%d pattern=robot.#", d.Position, d.Message.Seq)
	}
	if len(d1) != 3 || len(d2) != 3 {
		t.Fatalf("fanout counts wrong: s1=%d s2=%d", len(d1), len(d2))
	}
	select {
	case d := <-s3.Deliveries():
		t.Fatalf("s3 should not receive mismatched topic, got %v", d)
	default:
	}
}

// TestRejectDuplicateAndRollbackProducerSeq 同一生产者重复/回退序列号必须被明确拒绝。
func TestRejectDuplicateAndRollbackProducerSeq(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	ctx := context.Background()

	pos1, err := b.Publish(ctx, newTestMsg("a.b", 5))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("[dupseq] publish seq=5 accepted position=%d", pos1)

	_, err = b.Publish(ctx, newTestMsg("a.b", 5))
	assertReason(t, err, ReasonInvalidSequence, "duplicate seq=5")
	_, err = b.Publish(ctx, newTestMsg("a.b", 3))
	assertReason(t, err, ReasonInvalidSequence, "rollback seq=3")
	t.Logf("[dupseq] duplicate and rollback seq REJECTED reason=%s", ReasonInvalidSequence)

	// 不同生产者各自独立计数。
	other := newTestMsg("a.b", 1)
	other.Producer = "arm-2"
	if _, err := b.Publish(ctx, other); err != nil {
		t.Fatalf("different producer seq=1 should pass: %v", err)
	}
}

// TestNoDuplicateDelivery 已确认位点之后的迟到重复投递不得重复交付（幂等吸收）。
func TestNoDuplicateDelivery(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	ctx := context.Background()

	s := mustSub(t, b, ctx, "s", "a.b", 8, OverflowBlock, 0)
	pos := mustPublish(t, b, ctx, "a.b", 1)
	d := recvN(t, s, 1)[0]
	if d.Position != pos {
		t.Fatalf("position mismatch: %d vs %d", d.Position, pos)
	}
	if err := s.Ack(pos); err != nil {
		t.Fatal(err)
	}
	t.Logf("[dedup] delivered pos=%d seq=%d, acked committed=%d", d.Position, d.Message.Seq, s.CommittedPosition())

	// 直接向该订阅者再次 enqueue 同位点（模拟并发迟到/重复事件）：必须被吸收。
	if err := s.enqueue(Delivery{Position: pos, Message: d.Message}); err != nil {
		t.Fatalf("duplicate enqueue should be absorbed, not error: %v", err)
	}
	if got := s.Counters().Duplicates; got != 1 {
		t.Fatalf("expected Duplicates=1, got %d", got)
	} else {
		t.Logf("[dedup] duplicate pos=%d absorbed, counters.duplicates=%d (not delivered twice)", pos, got)
	}
}

func mustSub(t *testing.T, b *Broker, ctx context.Context, id, pattern string,
	depth int, st OverflowStrategy, resume uint64) *Subscription {
	t.Helper()
	p := mustPattern(t, pattern)
	s, err := b.Subscribe(ctx, id, p, depth, st, resume)
	if err != nil {
		t.Fatalf("subscribe %s: %v", id, err)
	}
	return s
}

func mustPublish(t *testing.T, b *Broker, ctx context.Context, topic string, seq uint64) uint64 {
	t.Helper()
	pos, err := b.Publish(ctx, newTestMsg(topic, seq))
	if err != nil {
		t.Fatalf("publish %s seq=%d: %v", topic, seq, err)
	}
	return pos
}

func recvN(t *testing.T, s *Subscription, n int) []Delivery {
	t.Helper()
	return drainN(t, s, n)
}

func drainN(t *testing.T, s *Subscription, n int) []Delivery {
	t.Helper()
	out := make([]Delivery, 0, n)
	for len(out) < n {
		select {
		case d, ok := <-s.Deliveries():
			if !ok {
				return out
			}
			out = append(out, d)
		case <-timeout():
			t.Fatalf("subscriber=%s expected %d deliveries, got %d", s.ID(), n, len(out))
		}
	}
	return out
}

func assertReason(t *testing.T, err error, want RejectReason, note string) {
	t.Helper()
	re, ok := AsRejectError(err)
	if !ok || re.Reason != want {
		t.Fatalf("%s: expected reason=%s, got %v", note, want, err)
	}
	t.Logf("[reject] %s -> reason=%s detail=%q", note, re.Reason, re.Detail)
}
