package middleware

import (
	"testing"
	"time"
)

// waitSeq 等待收到 seq == want 的消息（期间消费其他消息），断言序列严格递增。
func waitSeq(t *testing.T, sub *Subscription, want uint64, within time.Duration) {
	t.Helper()
	deadline := time.After(within)
	var last uint64
	for {
		select {
		case m, ok := <-sub.C():
			if !ok {
				t.Fatalf("channel closed before seq=%d (last=%d)", want, last)
			}
			if m.Seq <= last {
				t.Fatalf("non-monotonic replay delivery: seq=%d after %d", m.Seq, last)
			}
			last = m.Seq
			if m.Seq == want {
				return
			}
		case <-deadline:
			t.Fatalf("timeout waiting for seq=%d (last received=%d)", want, last)
		}
	}
}

// drop-oldest 造成空档后，从已确认位点重连：缺口被按序补齐，
// 补完后 Ack 整段成功，结果与一直在线等价（不重不漏）。
func TestResumeFillsDropOldestGap(t *testing.T) {
	b := NewBroker(Config{QueueSize: 2, Retention: 128})
	defer b.Close()

	sub, _ := b.SubscribeWith("t/e", 0, SubscribeOptions{OverflowPolicy: PolicyDropOldest})
	// 队列 2：seq3,seq4 挤掉 seq1,seq2，形成空档。
	for i := 0; i < 4; i++ {
		if _, err := b.Publish("t/e", Version1, nil); err != nil {
			t.Fatal(err)
		}
	}
	// 现存 seq3,seq4 能正常消费；但 Ack(4) 必须被空档挡下。
	recv(t, sub)
	recv(t, sub)
	if err := sub.Ack(4); err == nil {
		t.Fatal("Ack over gap unexpectedly accepted")
	} else {
		t.Logf("gap blocks ack before refill: %v", err)
	}
	sub.Unsubscribe()
	if pos := sub.Position(); pos != 0 {
		t.Fatalf("position = %d, want 0 (nothing ackable before gap)", pos)
	}

	// 重连：从位点 0 补齐。小队列（2）下消费者必须边补边读，不能卡住也不能再丢。
	sub2, err := b.SubscribeWith("t/e", sub.Position(), SubscribeOptions{OverflowPolicy: PolicyDropOldest})
	if err != nil {
		t.Fatal(err)
	}
	defer sub2.Unsubscribe()
	waitSeq(t, sub2, 4, 3*time.Second)
	// 补齐后整段确认必须成功，且不重：重放从头开始，收到 seq1..4 各一次。
	if err := sub2.Ack(4); err != nil {
		t.Fatalf("ack after refill: %v", err)
	}
	if sub2.Position() != 4 {
		t.Fatalf("position after refill = %d, want 4", sub2.Position())
	}
	st := sub2.Stats()
	if st.Delivered != 4 || st.DroppedOldest != 0 || st.Duplicates != 0 {
		t.Fatalf("refill stats = %+v, want exactly 4 delivered with no drop/dup", st)
	}
	t.Logf("gap refilled: position=%d stats=%+v", sub2.Position(), st)
}

// 重放期间实时消息接续：补齐结果必须与“一直在线”完全一致——
// seq 单调连续、不重不漏，且补齐后可一路确认无空档。
func TestResumeEquivalentToAlwaysOnline(t *testing.T) {
	// 队列给足：本测试关注重放与实时消息的接续顺序，不引入队列溢出；
	// 小队列下的推进能力由并发压测单独覆盖。
	const queue = 64
	const before = 20
	const during = 20
	b := NewBroker(Config{QueueSize: queue, Retention: 256})
	defer b.Close()

	// 参照组：一条一直在线、全速消费的订阅（仅收集序列，末尾统一确认）。
	ref, _ := b.SubscribeWith("t/f", 0, SubscribeOptions{OverflowPolicy: PolicyDropOldest})
	defer ref.Unsubscribe()
	refSeqs := make(chan uint64, before+during)
	refDone := make(chan struct{})
	go func() {
		defer close(refDone)
		var last uint64
		for m := range ref.C() {
			if m.Seq <= last {
				t.Errorf("ref non-monotonic: seq=%d after %d", m.Seq, last)
				return
			}
			last = m.Seq
			refSeqs <- m.Seq
		}
	}()

	// 断线组：先发 before 条（该连接不存在），再从 0 重连；重放期间继续发布 during 条。
	for i := 0; i < before; i++ {
		if _, err := b.Publish("t/f", Version1, nil); err != nil {
			t.Fatal(err)
		}
	}
	rec, err := b.SubscribeWith("t/f", 0, SubscribeOptions{OverflowPolicy: PolicyDropOldest})
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Unsubscribe()

	recSeqs := make(chan uint64, before+during)
	recDone := make(chan struct{})
	go func() {
		defer close(recDone)
		var last uint64
		for m := range rec.C() {
			if m.Seq <= last {
				t.Errorf("resumed non-monotonic/dup: seq=%d after %d", m.Seq, last)
				return
			}
			last = m.Seq
			recSeqs <- m.Seq
		}
	}()

	for i := 0; i < during; i++ {
		if _, err := b.Publish("t/f", Version1, nil); err != nil {
			t.Fatal(err)
		}
	}

	collect := func(ch <-chan uint64, done <-chan struct{}) []uint64 {
		out := make([]uint64, 0, before+during)
		for len(out) < before+during {
			select {
			case v := <-ch:
				out = append(out, v)
			case <-time.After(3 * time.Second):
				t.Fatalf("collected %d of %d seqs, replay stalled", len(out), before+during)
			case <-done:
				t.Fatalf("collector exited early after %d seqs", len(out))
			}
		}
		return out
	}
	got := collect(recSeqs, recDone)
	want := collect(refSeqs, refDone)
	if len(got) != len(want) {
		t.Fatalf("resumed got %d msgs, always-online got %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("divergence at %d: resumed=%d online=%d", i, got[i], want[i])
		}
	}
	if got[0] != 1 || got[len(got)-1] != uint64(before+during) {
		t.Fatalf("boundary wrong: first=%d last=%d", got[0], got[len(got)-1])
	}
	seen := make(map[uint64]bool, len(got))
	for _, v := range got {
		if seen[v] {
			t.Fatalf("dup seq=%d in resumed stream", v)
		}
		seen[v] = true
	}
	// 两边都无空档：可一次性确认到末位点（严格 Ack 校验）。
	if err := ref.Ack(uint64(before + during)); err != nil {
		t.Fatalf("always-online ack: %v", err)
	}
	if err := rec.Ack(uint64(before + during)); err != nil {
		t.Fatalf("resumed ack: %v", err)
	}
	t.Logf("resume equivalence confirmed: %d seqs identical to always-online, acks clean", len(got))
}

// drop-newest 造成的空档也能通过重连续传补齐。
func TestResumeFillsDropNewestGap(t *testing.T) {
	b := NewBroker(Config{QueueSize: 2, Retention: 128})
	defer b.Close()
	sub, _ := b.SubscribeWith("t/g", 0, SubscribeOptions{OverflowPolicy: PolicyDropNewest})
	for i := 0; i < 5; i++ { // seq1,2 入队；seq3,4,5 被拒收
		if _, err := b.Publish("t/g", Version1, nil); err != nil {
			t.Fatal(err)
		}
	}
	recv(t, sub)
	recv(t, sub)
	if err := sub.Ack(2); err != nil {
		t.Fatalf("ack seq2: %v", err)
	}
	sub.Unsubscribe()

	sub2, err := b.SubscribeWith("t/g", 2, SubscribeOptions{OverflowPolicy: PolicyDropNewest})
	if err != nil {
		t.Fatal(err)
	}
	defer sub2.Unsubscribe()
	waitSeq(t, sub2, 5, 3*time.Second)
	if err := sub2.Ack(5); err != nil {
		t.Fatalf("ack after drop-newest refill: %v", err)
	}
	st := sub2.Stats()
	if st.Delivered != 3 || st.DroppedNewest != 0 || st.Duplicates != 0 {
		t.Fatalf("refill stats = %+v, want 3 delivered no drop/dup", st)
	}
	t.Logf("drop-newest gap refilled from position=2: stats=%+v", st)
}

// 极小队列 + 持续负载：断线期间积压 50 条（队列仅 2），重连后边重放边继续发布。
// 消费者持续排空，要求：不卡死、严格递增、恰好收到全部 100 条、最终可确认到位、
// 统计恒等式成立（重放消息不丢、不重复计 Matched）。
func TestResumeCatchUpTinyQueueUnderLoad(t *testing.T) {
	b := NewBroker(Config{QueueSize: 2, Retention: 512})
	defer b.Close()

	const offline = 50
	const live = 50
	for i := 0; i < offline; i++ {
		if _, err := b.Publish("t/h", Version1, nil); err != nil {
			t.Fatal(err)
		}
	}
	sub, err := b.SubscribeWith("t/h", 0, SubscribeOptions{OverflowPolicy: PolicyDropOldest})
	if err != nil {
		t.Fatal(err)
	}

	got := make(chan uint64, offline+live)
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		var last uint64
		for m := range sub.C() {
			if m.Seq <= last {
				t.Errorf("non-monotonic catch-up delivery: seq=%d after %d", m.Seq, last)
				return
			}
			last = m.Seq
			got <- m.Seq
		}
	}()
	for i := 0; i < live; i++ {
		if _, err := b.Publish("t/h", Version1, nil); err != nil {
			t.Fatalf("publish during catch-up: %v", err)
		}
	}

	seqs := make([]uint64, 0, offline+live)
	for len(seqs) < offline+live {
		select {
		case s := <-got:
			seqs = append(seqs, s)
		case <-time.After(5 * time.Second):
			t.Fatalf("catch-up stalled after %d of %d messages", len(seqs), offline+live)
		}
	}
	if seqs[0] != 1 || seqs[len(seqs)-1] != offline+live {
		t.Fatalf("catch-up boundaries: first=%d last=%d", seqs[0], seqs[len(seqs)-1])
	}
	if err := sub.Ack(offline + live); err != nil {
		t.Fatalf("ack after full catch-up: %v", err)
	}
	st := sub.Stats()
	if !st.Reconciled() {
		t.Fatalf("identity mismatch: %+v", st)
	}
	if st.Delivered != offline+live || st.Matched != offline+live {
		t.Fatalf("catch-up stats = %+v, want exactly %d delivered", st, offline+live)
	}
	sub.Unsubscribe()
	<-consumerDone
	t.Logf("tiny-queue catch-up under load complete: %d msgs exactly once, stats=%+v", len(seqs), st)
}
