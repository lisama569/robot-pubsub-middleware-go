package middleware

import (
	"errors"
	"testing"
)

// 四类非法确认必须各自可区分，且被拒绝后位点不动。
func TestAckValidationDistinctions(t *testing.T) {
	b := NewBroker(Config{QueueSize: 4})
	defer b.Close()

	// gapSub 用小队列制造 drop-oldest 空档：6 条发布中 seq=1,2 被挤出。
	gapSub, err := b.SubscribeWith("t/a", 0, SubscribeOptions{OverflowPolicy: PolicyDropOldest})
	if err != nil {
		t.Fatal(err)
	}
	defer gapSub.Unsubscribe()
	for i := 0; i < 6; i++ {
		if _, err := b.Publish("t/a", Version1, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 4; i++ { // 排空队列中现存的 seq 3..6
		recv(t, gapSub)
	}

	// 1) 还没交到手上：确认位点超过交付前沿。
	err = gapSub.Ack(100)
	if !errors.Is(err, ErrAckNotDelivered) {
		t.Fatalf("ack ahead: got %v, want ErrAckNotDelivered", err)
	}
	t.Logf("not-delivered rejected as expected: %v", err)
	if gapSub.Position() != 0 {
		t.Fatalf("position moved after rejected ack: %d", gapSub.Position())
	}

	// 2) 中间有空档：seq 3..6 已交付，但 seq 1,2 被挤出；确认 3 或 6 都必须拒绝。
	for _, seq := range []uint64{3, 6} {
		err = gapSub.Ack(seq)
		if !errors.Is(err, ErrAckGap) {
			t.Fatalf("ack %d over gap: got %v, want ErrAckGap", seq, err)
		}
		t.Logf("gap rejected as expected (seq=%d): %v", seq, err)
	}
	if gapSub.Position() != 0 {
		t.Fatalf("position moved after gap ack: %d", gapSub.Position())
	}

	// 合法确认与回退用另一个没有空档的订阅验证（同 Broker、不同主题）。
	okSub, err := b.Subscribe("t/ok", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer okSub.Unsubscribe()
	for i := 0; i < 3; i++ {
		if _, err := b.Publish("t/ok", Version1, nil); err != nil {
			t.Fatal(err)
		}
	}
	recv(t, okSub)
	if err := okSub.Ack(1); err != nil {
		t.Fatalf("legal ack 1: %v", err)
	}
	// 3) 回退：再确认更小的位点。
	err = okSub.Ack(0)
	if !errors.Is(err, ErrPositionRegression) {
		t.Fatalf("regression: got %v, want ErrPositionRegression", err)
	}
	t.Logf("regression rejected as expected: %v", err)
	if okSub.Position() != 1 {
		t.Fatalf("position moved after regression: %d", okSub.Position())
	}

	// 四种错误哨兵互不混淆。
	errs := []error{ErrPositionRegression, ErrAckNotDelivered, ErrAckGap, ErrPositionExpired}
	for i, e1 := range errs {
		for j, e2 := range errs {
			if i != j && errors.Is(e1, e2) {
				t.Fatalf("error sentinels %d and %d must be distinct", i, j)
			}
		}
	}
}

// drop-newest 同样形成空档：被拒收的新消息会阻断对其后已交付消息的整段确认。
func TestAckGapWithDropNewest(t *testing.T) {
	b := NewBroker(Config{QueueSize: 2})
	defer b.Close()
	sub, _ := b.SubscribeWith("t/b", 0, SubscribeOptions{OverflowPolicy: PolicyDropNewest})
	defer sub.Unsubscribe()

	// seq1,seq2 入队；seq3,seq4,seq5 拒收（队列没腾过）。
	for i := 0; i < 5; i++ {
		if _, err := b.Publish("t/b", Version1, nil); err != nil {
			t.Fatal(err)
		}
	}
	// 确认 seq2 合法（已交付）；试图确认 seq5 时最早空档是 seq3。
	if err := sub.Ack(2); err != nil {
		t.Fatalf("ack seq2: %v", err)
	}
	err := sub.Ack(5)
	if !errors.Is(err, ErrAckGap) {
		t.Fatalf("ack over drop-newest gaps: got %v, want ErrAckGap", err)
	}
	t.Logf("drop-newest gap rejected: %v", err)
	// 腾队列不会凭空补齐 seq3：只有重连重放能补，Ack 仍应拒绝。
	recv(t, sub)
	recv(t, sub)
	err = sub.Ack(5)
	if !errors.Is(err, ErrAckGap) {
		t.Fatalf("ack gap after drain without replay: got %v, want ErrAckGap", err)
	}
}

// 空档对应的消息滚出保留窗口后，Ack 返回 ErrPositionExpired，与“可补齐的空档”区分。
func TestAckGapExpiredByRetention(t *testing.T) {
	b := NewBroker(Config{QueueSize: 1, Retention: 2})
	defer b.Close()
	sub, _ := b.SubscribeWith("t/c", 0, SubscribeOptions{OverflowPolicy: PolicyDropOldest})
	defer sub.Unsubscribe()

	// seq1 入队；seq2 挤掉 seq1（空档 seq1）；seq3 挤掉 seq2（空档 seq2）。
	for i := 0; i < 3; i++ {
		if _, err := b.Publish("t/c", Version1, nil); err != nil {
			t.Fatal(err)
		}
	}
	// 保留窗口只剩 seq2,seq3（retention=2），seq1 已不可恢复。
	err := sub.Ack(3)
	if !errors.Is(err, ErrPositionExpired) {
		t.Fatalf("ack over expired gap: got %v, want ErrPositionExpired", err)
	}
	t.Logf("expired gap rejected as expected: %v", err)
	// 窗口内的空档（seq2）也不能放过：尝试只确认到 seq2 时仍是过期空档。
	err = sub.Ack(2)
	if !errors.Is(err, ErrPositionExpired) {
		t.Fatalf("ack seq2 over expired gap seq1: got %v, want ErrPositionExpired", err)
	}
}

// 续传位点早于保留窗口起点时，Subscribe 必须拒绝，而不是静默丢段重放。
func TestSubscribeResumeExpired(t *testing.T) {
	b := NewBroker(Config{Retention: 3})
	defer b.Close()
	var seqs []uint64
	for i := 0; i < 5; i++ {
		seq, err := b.Publish("t/d", Version1, nil)
		if err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, seq)
	}
	// 窗口起点为 seq3；从 seq1 或 seq2 续传，中间缺段不可恢复。
	_, err := b.Subscribe("t/d", seqs[0])
	if !errors.Is(err, ErrPositionExpired) {
		t.Fatalf("resume before window: got %v, want ErrPositionExpired", err)
	}
	t.Logf("expired resume rejected as expected: %v", err)
	if _, err := b.Subscribe("t/d", seqs[1]); !errors.Is(err, ErrPositionExpired) {
		t.Fatalf("resume at seq2: got %v, want ErrPositionExpired", err)
	}
	sub, err := b.Subscribe("t/d", seqs[2])
	if err != nil {
		t.Fatalf("resume at window start seq3: %v", err)
	}
	defer sub.Unsubscribe()
	m := recv(t, sub)
	if m.Seq != seqs[3] {
		t.Fatalf("first replay msg seq=%d, want %d", m.Seq, seqs[3])
	}
	t.Logf("resume at window boundary works: first msg seq=%d", m.Seq)
}
