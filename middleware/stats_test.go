package middleware

import (
	"testing"
)

// 退订后仍可查询本次生命周期的消息去向汇总，且各项能互相对账：
// Matched = Delivered + DroppedOldest + DroppedNewest + DroppedLate + Duplicates。
func TestStatsReconciliation(t *testing.T) {
	b := NewBroker(Config{QueueSize: 2, Retention: 128})
	defer b.Close()

	sub, _ := b.SubscribeWith("t/s", 0, SubscribeOptions{OverflowPolicy: PolicyDropOldest})
	// seq1..5 依次进入容量 2 的队列：每条入队都计入 Delivered（含后来被挤的），
	// 被挤出的是 seq1,seq2,seq3 三条（DroppedOldest=3），队列中留 seq4,seq5。
	for i := 0; i < 5; i++ {
		if _, err := b.Publish("t/s", Version1, nil); err != nil {
			t.Fatal(err)
		}
	}
	// 重复消息：以同 seq 再入队一次，应计入 Duplicates。
	sub.enqueue(Message{Topic: "t/s", Seq: 5, Version: Version1})

	// 读走现存 seq4,seq5 后退订。退订后订阅已从 Broker 移除，
	// 新发布不再触达它（不属于本订阅的去向），发布本身对其他订阅照常成功。
	recv(t, sub)
	recv(t, sub)
	sub.Unsubscribe()
	if _, err := b.Publish("t/s", Version1, nil); err != nil {
		t.Fatalf("publish after one sub unsubscribed must still work: %v", err)
	}

	st := sub.Stats() // 退订后仍可查询
	if !st.Reconciled() {
		t.Fatalf("identity: %+v", st)
	}
	if st.Matched != 6 || st.Delivered != 5 || st.DroppedOldest != 3 ||
		st.DroppedNewest != 0 || st.DroppedLate != 0 || st.Duplicates != 1 {
		t.Fatalf("stats = %+v, want {Matched:6 Delivered:5 DroppedOldest:3 DroppedNewest:0 DroppedLate:0 Duplicates:1}", st)
	}
	t.Logf("reconciliation ok: %+v", st)

	// 幂等退订不影响统计。
	sub.Unsubscribe()
	if sub.Stats() != st {
		t.Fatalf("stats changed after idempotent unsubscribe: %+v vs %+v", sub.Stats(), st)
	}
}

// Broker 关闭后，尚未排空的“关闭后到达”消息计入 DroppedLate；
// 关闭前已入队消息可排空，且其计数保持可对账。
func TestStatsAfterBrokerClose(t *testing.T) {
	b := NewBroker(Config{QueueSize: 4})
	sub, _ := b.SubscribeWith("t/z", 0, SubscribeOptions{OverflowPolicy: PolicyDropNewest})
	for i := 0; i < 4; i++ {
		if _, err := b.Publish("t/z", Version1, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	// 关闭后发布直接被 Broker 拒绝（ErrClosed），不进入订阅统计；订阅通道可排空后关闭。
	drained := 0
	for range sub.C() {
		drained++
	}
	if drained != 4 {
		t.Fatalf("drained %d, want 4 queued-before-close messages", drained)
	}
	st := sub.Stats()
	if st.Delivered != 4 || st.DroppedLate != 0 || st.Matched != 4 {
		t.Fatalf("post-close stats = %+v, want delivered=4 matched=4", st)
	}
	t.Logf("post-close drain stats: %+v", st)

	// 已关闭订阅上再走 enqueue 应计入 late（防御性，内部调用路径）。
	r := sub.enqueue(Message{Topic: "t/z", Seq: 99, Version: Version1})
	if r != DropClosed {
		t.Fatalf("enqueue after close reason = %v, want DropClosed", r)
	}
	st2 := sub.Stats()
	if st2.DroppedLate != 1 || st2.Matched != 5 {
		t.Fatalf("late stats = %+v, want droppedLate=1 matched=5", st2)
	}
}

// 空队列订阅无任何消息时，恒等式平凡成立。
func TestStatsEmpty(t *testing.T) {
	b := NewBroker(Config{})
	defer b.Close()
	sub, _ := b.Subscribe("t/none", 0)
	sub.Unsubscribe()
	if st := sub.Stats(); st != (Stats{}) {
		t.Fatalf("empty subscription stats = %+v, want zero", st)
	}
}
