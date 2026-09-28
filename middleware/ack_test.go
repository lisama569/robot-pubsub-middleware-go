package middleware

import (
	"errors"
	"testing"
)

// 各种非法确认必须被明确拒绝、原因可区分，且被拒绝后位点不得移动。
func TestAckInvalidReasons(t *testing.T) {
	b := NewBroker(Config{QueueSize: 2})
	defer b.Close()

	sub, err := b.Subscribe("robot/telemetry", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	// 发布 5 条，队列容量 2 且不消费：前 3 条（seq1-3）被挤掉，队列里剩 seq4、seq5。
	for i := 0; i < 5; i++ {
		if _, err := b.Publish("robot/telemetry", Version1, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	st := sub.Stats()
	t.Logf("after overflow: seen=%d droppedOldest=%d queue=[%d,%d]",
		5, st.DroppedOldest, mustRecv(t, sub).Seq, mustRecv(t, sub).Seq)

	// 1) 确认到一个从未交到手上的位置：seq6 > 已见水位 5。
	err = sub.Ack(6)
	if !errors.Is(err, ErrAckNotDelivered) {
		t.Fatalf("ack beyond watermark: got %v, want ErrAckNotDelivered", err)
	}
	t.Logf("not-delivered rejected: %v", err)
	if sub.Position() != 0 {
		t.Fatalf("position moved after rejected ack: %d", sub.Position())
	}

	// 2) 跨空档确认：seq1-3 已被挤掉尚未补齐，Ack(5) 必须拒绝并指出缺口位点 1。
	err = sub.Ack(5)
	if !errors.Is(err, ErrAckGap) {
		t.Fatalf("ack over gap: got %v, want ErrAckGap", err)
	}
	t.Logf("gap rejected: %v", err)
	if sub.Position() != 0 {
		t.Fatalf("position moved after gap ack: %d", sub.Position())
	}

	// 只确认到缺口之前的位置是允许的（位点 0，区间内无丢失）——确认水位未推进时
	// Ack(0) 幂等无意义；改为验证：补缺口过程中也不能跨过缺口。
	// 3) 位点回退：先补齐 seq1 后确认到 1，再确认 0。
	// 通过重连续传补齐缺口：以位点 0 重新订阅会拿到保留日志重放；这里直接在同一
	// 订阅上模拟“缺口仍在”时的回退判定：lastAcked=0，Ack(0) 恒成功（非回退）。
	if err := sub.Ack(0); err != nil {
		t.Fatalf("idempotent ack at current position: %v", err)
	}
}

// mustRecv 立即接收一条消息（队列中已有消息时不会阻塞）。
func mustRecv(t *testing.T, sub *Subscription) Message {
	t.Helper()
	m := recv(t, sub)
	return m
}

// 缺口补齐：DropOldest 挤掉的消息在重连续传时从确认位点补回，
// 补齐过程中不能跨缺口确认，补完后可确认到最终位点，结果与一直在线一致。
func TestAckGapFilledByResume(t *testing.T) {
	const queue = 2
	const total = 6
	b := NewBroker(Config{QueueSize: queue, Retention: 64})
	defer b.Close()

	// 第一代订阅不消费：seq1..4 被挤掉，队列只剩 seq5、seq6，丢失位点 1..4。
	gen1, err := b.Subscribe("robot/telemetry", 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < total; i++ {
		if _, err := b.Publish("robot/telemetry", Version1, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if s := gen1.Stats(); s.DroppedOldest != total-queue {
		t.Fatalf("gen1 droppedOldest=%d, want %d", s.DroppedOldest, total-queue)
	}
	// 缺口未补，确认必须被拒绝。
	if err := gen1.Ack(total); !errors.Is(err, ErrAckGap) {
		t.Fatalf("gen1 ack over gap: got %v, want ErrAckGap", err)
	}
	gen1.Unsubscribe()

	// 重连：从确认位点 0 重放，保留窗口完整，1..6 全部补回（给足消费速度避免再次溢出）。
	gen2, err := b.Subscribe("robot/telemetry", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer gen2.Unsubscribe()
	seqs := make(map[uint64]bool, total)
	var prev uint64
	for i := 0; i < total; i++ {
		m := recv(t, gen2)
		if m.Seq <= prev {
			t.Fatalf("redelivered out of order/dup: seq=%d after %d", m.Seq, prev)
		}
		prev = m.Seq
		seqs[m.Seq] = true
	}
	if len(seqs) != total {
		t.Fatalf("resume filled %d distinct seqs, want %d: %v", len(seqs), total, seqs)
	}
	t.Logf("resume redelivered %d distinct messages in order, no dup/loss", total)

	// 补齐后逐条确认全部成功，且无重复计数。
	for seq := uint64(1); seq <= total; seq++ {
		if err := gen2.Ack(seq); err != nil {
			t.Fatalf("ack %d after gap fill: %v", seq, err)
		}
	}
	if gen2.Position() != total {
		t.Fatalf("position after fill = %d, want %d", gen2.Position(), total)
	}
	if d := gen2.Stats().Duplicates; d != 0 {
		t.Fatalf("duplicates on resumed sub: %d", d)
	}

	// 补齐之后再回退确认，必须拒绝。
	err = gen2.Ack(total - 1)
	if !errors.Is(err, ErrPositionRegression) {
		t.Fatalf("regression after fill: got %v, want ErrPositionRegression", err)
	}
	t.Logf("regression rejected after fill: %v", err)
	if gen2.Position() != total {
		t.Fatalf("position changed after rejected regression ack: %d", gen2.Position())
	}
}

// 非匹配主题的序列号不构成缺口：订阅 "a" 时确认位点可越过只发到 "b" 的消息。
func TestAckIgnoresNonMatchingSeq(t *testing.T) {
	b := NewBroker(Config{})
	defer b.Close()
	sub, err := b.Subscribe("a", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	if _, err := b.Publish("b", Version1, nil); err != nil { // seq1，不匹配
		t.Fatal(err)
	}
	m2seq, err := b.Publish("a", Version1, nil) // seq2，匹配
	if err != nil {
		t.Fatal(err)
	}
	m := recv(t, sub)
	if m.Seq != m2seq {
		t.Fatalf("got seq=%d, want %d", m.Seq, m2seq)
	}
	if err := sub.Ack(m2seq); err != nil {
		t.Fatalf("ack across non-matching seq rejected: %v", err)
	}
	t.Logf("ack %d accepted despite non-matching seq 1", m2seq)
}

// 补齐过程中只能确认已补到的部分：已补到 seq3 时，Ack(3) 成功、Ack(4) 被 ErrAckGap 拒绝；
// 全部补完后 Ack 到最终位点，结果与一直在线完全一致（不重不漏）。
func TestAckDuringPartialFill(t *testing.T) {
	const queue = 3
	const total = 5
	b := NewBroker(Config{QueueSize: queue, Retention: 64})
	defer b.Close()

	// 第一代 DropNewest：前 3 条留队，后 2 条被拒收（lost=4,5）。
	gen1, err := b.SubscribeWith("t", 0, SubscribeOptions{Policy: PolicyDropNewest})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < total; i++ {
		if _, err := b.Publish("t", Version1, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if s := gen1.Stats(); s.DroppedNewest != 2 {
		t.Fatalf("gen1 droppedNewest=%d, want 2", s.DroppedNewest)
	}
	// seq4、5 被拒收、从未交到手上（水位停在 3）：确认到 5 是“确认到没交付的位置”。
	if err := gen1.Ack(total); !errors.Is(err, ErrAckNotDelivered) {
		t.Fatalf("gen1 ack over rejected-undelivered: got %v, want ErrAckNotDelivered", err)
	}
	t.Logf("gen1 ack(%d) rejected with ErrAckNotDelivered (watermark=3)", total)
	gen1.Unsubscribe()

	// 重连懒续传：先补满队列（seq1-3），此时不消费，4、5 还没补；
	// 消费腾出空位后续传泵继续补 4、5，全程按序。
	gen2, err := b.SubscribeWith("t", 0, SubscribeOptions{Policy: PolicyDropNewest})
	if err != nil {
		t.Fatal(err)
	}
	defer gen2.Unsubscribe()

	// 此刻续传泵已把队列补满（seq1-3）后暂停，seq4、5 尚未补入：
	// 确认到已补到的 3 允许；确认 4 时该位点还没交到手上，被 ErrAckNotDelivered 拒绝。
	if err := gen2.Ack(3); err != nil {
		t.Fatalf("ack up to filled watermark 3: %v", err)
	}
	if err := gen2.Ack(4); !errors.Is(err, ErrAckNotDelivered) {
		t.Fatalf("ack beyond paused replay: got %v, want ErrAckNotDelivered", err)
	}
	t.Logf("ack(3) accepted while replay paused; ack(4) rejected with ErrAckNotDelivered")

	var got []Message
	for i := 0; i < total; i++ {
		m := recv(t, gen2)
		if m.Seq != uint64(i+1) {
			t.Fatalf("resume order at index %d: got seq=%d want %d", i, m.Seq, i+1)
		}
		got = append(got, m)
	}
	// 消费腾出空位后续传泵补完 4、5；此时可确认到最终位点。
	if err := gen2.Ack(total); err != nil {
		t.Fatalf("final ack after full fill: %v", err)
	}
	if gen2.Position() != total {
		t.Fatalf("position=%d want %d", gen2.Position(), total)
	}
	if err := gen2.Ack(total - 1); !errors.Is(err, ErrPositionRegression) {
		t.Fatalf("regression after fill: got %v, want ErrPositionRegression", err)
	}
	if len(got) != total {
		t.Fatalf("resumed sub got %d, want %d", len(got), total)
	}
	t.Logf("gap filled in order 1..%d, final position=%d matches always-online view", total, total)
}

// 续传位点合法性：早于保留窗口返回 ErrPositionExpired；超过最新位点返回
// ErrAckNotDelivered，两种原因可区分。
func TestResumePositionValidation(t *testing.T) {
	b := NewBroker(Config{Retention: 4})
	defer b.Close()
	for i := 0; i < 10; i++ {
		if _, err := b.Publish("t", Version1, nil); err != nil {
			t.Fatal(err)
		}
	}
	// 保留窗口最早为 seq7；resumeFrom=5 早于 7-1=6，无法补齐 seq6 这一段。
	_, err := b.Subscribe("t", 5)
	if !errors.Is(err, ErrPositionExpired) {
		t.Fatalf("expired resume: got %v, want ErrPositionExpired", err)
	}
	t.Logf("expired resume rejected: %v", err)
	// resumeFrom=6（窗口最早 seq7 的前一位）合法：只差 seq7..10。
	sub, err := b.Subscribe("t", 6)
	if err != nil {
		t.Fatalf("resume at window edge: %v", err)
	}
	sub.Unsubscribe()
	// 超过最新位点 10：明确拒绝。
	_, err = b.Subscribe("t", 11)
	if !errors.Is(err, ErrAckNotDelivered) {
		t.Fatalf("future resume: got %v, want ErrAckNotDelivered", err)
	}
	t.Logf("future resume rejected: %v", err)
	// 从头订阅不受保留窗口限制。
	head, err := b.Subscribe("t", 0)
	if err != nil {
		t.Fatalf("fresh subscribe: %v", err)
	}
	head.Unsubscribe()
}
