package pubsub

import (
	"context"
	"testing"
)

func TestSubscribeInputValidation(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	ctx := context.Background()

	if _, err := b.Subscribe(ctx, "", mustPattern(t, "a"), 4, OverflowBlock, 0); !isReason(err, ReasonInvalidTopic) {
		t.Fatalf("empty id: %v", err)
	}
	if _, err := b.Subscribe(ctx, "x", Pattern{raw: "a.#.b"}, 4, OverflowBlock, 0); !isReason(err, ReasonInvalidTopic) {
		t.Fatalf("bad pattern: %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := b.Subscribe(cctx, "y", mustPattern(t, "a"), 4, OverflowBlock, 0); err == nil {
		t.Fatal("canceled ctx subscribe should fail")
	}
	if _, err := b.Publish(cctx, newTestMsg("a.b", 1)); err == nil {
		t.Fatal("canceled ctx publish should fail")
	}
	t.Log("[edge] empty id / bad pattern / canceled ctx all rejected or failed")
}

func TestUnsubscribeIdempotent(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	ctx := context.Background()
	s := mustSub(t, b, ctx, "s", "a", 4, OverflowBlock, 0)

	if err := b.Unsubscribe("missing"); err != nil {
		t.Fatalf("unsubscribe missing id should be nil, got %v", err)
	}
	if err := b.Unsubscribe("s"); err != nil {
		t.Fatalf("first unsubscribe: %v", err)
	}
	if err := b.Unsubscribe("s"); err != nil {
		t.Fatalf("second unsubscribe should be idempotent nil, got %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("sub close after broker-side close should be nil, got %v", err)
	}
	// 关闭后 enqueue 明确拒绝。
	err := s.enqueue(Delivery{Position: 1})
	assertReason(t, err, ReasonSubscriptionClosed, "enqueue after close")
	<-s.Done()
	if s.ID() != "s" {
		t.Fatalf("id wrong: %s", s.ID())
	}
	t.Log("[edge] unsubscribe/close idempotent, post-close enqueue rejected, Done closed")
}

func TestDuplicateSubscriberRejected(t *testing.T) {
	b := NewBroker()
	defer b.Close()
	ctx := context.Background()
	mustSub(t, b, ctx, "dup", "a", 4, OverflowBlock, 0)
	_, err := b.Subscribe(ctx, "dup", mustPattern(t, "a"), 4, OverflowBlock, 0)
	assertReason(t, err, ReasonDuplicateSubscriber, "same id live subscribe")
}

func TestClosedAccessor(t *testing.T) {
	b := NewBroker()
	if b.Closed() {
		t.Fatal("should be open")
	}
	_ = b.Close()
	if !b.Closed() {
		t.Fatal("should report closed")
	}
	if err := b.Close(); err != nil {
		t.Fatalf("double close must be nil: %v", err)
	}
	t.Log("[edge] Closed() flips once; Close idempotent")
}

func TestAsRejectErrorUnwrap(t *testing.T) {
	if _, ok := AsRejectError(nil); ok {
		t.Fatal("nil should not match")
	}
	if _, ok := AsRejectError(context.Canceled); ok {
		t.Fatal("plain error should not match")
	}
	re, ok := AsRejectError(&wrappedErr{inner: rejectf(ReasonBrokerClosed, "x")})
	if !ok || re.Reason != ReasonBrokerClosed {
		t.Fatalf("wrapped reject should be unwrapped, got %v", re)
	}
	t.Log("[edge] AsRejectError handles nil/plain/wrapped errors")
}

type wrappedErr struct{ inner error }

func (e *wrappedErr) Error() string { return e.inner.Error() }
func (e *wrappedErr) Unwrap() error { return e.inner }
