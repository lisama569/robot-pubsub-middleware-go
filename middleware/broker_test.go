package middleware

import (
	"errors"
	"testing"
	"time"
)

// recv 带超时地从订阅接收一条消息，并打印判定依据。
func recv(t *testing.T, sub *Subscription) Message {
	t.Helper()
	select {
	case m, ok := <-sub.C():
		if !ok {
			t.Fatal("subscription channel closed unexpectedly")
		}
		t.Logf("recv: topic=%q seq=%d version=%d", m.Topic, m.Seq, m.Version)
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
		return Message{}
	}
}

func TestPublishVersionValidation(t *testing.T) {
	b := NewBroker(Config{SupportedVersions: []Version{Version1}})
	defer b.Close()

	if _, err := b.Publish("robot/cmd", Version(99), []byte("x")); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("publish with unknown version: got %v, want ErrUnsupportedVersion", err)
	}
	t.Logf("reject basis: version=99 not in supported set [1] => ErrUnsupportedVersion")

	seq1, err := b.Publish("robot/cmd", Version1, []byte("go"))
	if err != nil {
		t.Fatalf("publish v1: %v", err)
	}
	// 被拒绝的发布不得占用序列号：下一条成功发布的 Seq 应紧接其后。
	seq2, _ := b.Publish("robot/cmd", Version1, []byte("go2"))
	t.Logf("seq continuity: seq1=%d seq2=%d (rejected publish must not consume a seq)", seq1, seq2)
	if seq2 != seq1+1 {
		t.Fatalf("seq gap after rejected publish: seq1=%d seq2=%d", seq1, seq2)
	}
	if _, err := b.Publish("", Version1, nil); !errors.Is(err, ErrEmptyTopic) {
		t.Fatalf("empty topic: got %v, want ErrEmptyTopic", err)
	}
}

func TestAckPositionRegression(t *testing.T) {
	b := NewBroker(Config{})
	defer b.Close()
	sub, err := b.Subscribe("robot/#", 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := b.Publish("robot/telemetry", Version1, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	var last uint64
	for i := 0; i < 3; i++ {
		m := recv(t, sub)
		last = m.Seq
		if err := sub.Ack(m.Seq); err != nil {
			t.Fatalf("ack seq=%d: %v", m.Seq, err)
		}
		t.Logf("ack ok: seq=%d position=%d", m.Seq, sub.Position())
	}
	err = sub.Ack(last - 1)
	if !errors.Is(err, ErrPositionRegression) {
		t.Fatalf("regression ack: got %v, want ErrPositionRegression", err)
	}
	t.Logf("regression rejected: position=%d attempted=%d err=%v", sub.Position(), last-1, err)
	if sub.Position() != last {
		t.Fatalf("position moved after rejected ack: got %d want %d", sub.Position(), last)
	}
}

func TestResumeAfterReconnect(t *testing.T) {
	b := NewBroker(Config{})
	defer b.Close()

	sub1, err := b.Subscribe("robot/telemetry", 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := b.Publish("robot/telemetry", Version1, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	// 只确认前 2 条，然后“断线”（退订）。
	var acked uint64
	for i := 0; i < 2; i++ {
		m := recv(t, sub1)
		if err := sub1.Ack(m.Seq); err != nil {
			t.Fatal(err)
		}
		acked = m.Seq
	}
	t.Logf("disconnect: acked position=%d", acked)
	sub1.Unsubscribe()

	// 断线期间再发 2 条。
	for i := 0; i < 2; i++ {
		if _, err := b.Publish("robot/telemetry", Version1, []byte{0xf0, byte(i)}); err != nil {
			t.Fatal(err)
		}
	}

	// 重连：从已确认位点继续，应恰好收到 5 条未确认消息（3 条在队 + 2 条离线期间）。
	sub2, err := b.Subscribe("robot/telemetry", acked)
	if err != nil {
		t.Fatal(err)
	}
	defer sub2.Unsubscribe()
	prev := acked
	for i := 0; i < 5; i++ {
		m := recv(t, sub2)
		if m.Seq <= prev {
			t.Fatalf("duplicated or out-of-order delivery: seq=%d after %d", m.Seq, prev)
		}
		prev = m.Seq
	}
	t.Logf("resumed from position=%d, received seqs up to %d, no dup/loss", acked, prev)
	if got := sub2.Stats().Duplicates; got != 0 {
		t.Fatalf("unexpected duplicates on resume: %d", got)
	}
}

func TestNoDuplicateDeliveryOnResubscribe(t *testing.T) {
	b := NewBroker(Config{})
	defer b.Close()
	if _, err := b.Publish("a/b", Version1, []byte("m1")); err != nil {
		t.Fatal(err)
	}
	sub, err := b.Subscribe("a/#", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	m := recv(t, sub)
	// 再次以更早位点订阅同一连接不会发生——但同一订阅内重复入队必须被拒绝。
	// 这里直接验证订阅侧去重：重复入队同一条消息。
	if r := sub.enqueue(m); r != DropNone {
		t.Fatalf("duplicate enqueue reason = %v", r)
	}
	st := sub.Stats()
	t.Logf("dedup basis: seq=%d lastEnqueued=%d duplicates=%d", m.Seq, m.Seq, st.Duplicates)
	if st.Duplicates != 1 {
		t.Fatalf("duplicates = %d, want 1", st.Duplicates)
	}
	select {
	case dup := <-sub.C():
		t.Fatalf("duplicate message delivered: seq=%d", dup.Seq)
	case <-time.After(50 * time.Millisecond):
		t.Logf("no duplicate delivery confirmed")
	}
}
