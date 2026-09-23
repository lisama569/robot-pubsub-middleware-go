package middleware

import (
	"fmt"
	"sync"
)

// Subscription 表示一个订阅。消息经 C() 交付；消费后调用 Ack 推进确认位点。
//
// 并发约定：enqueue 由 Broker 在发布路径上调用；C/Ack/Unsubscribe 可由订阅方
// 任意并发调用。锁序固定为 broker.mu -> sub.mu，反向不成立，因此不会死锁。
type Subscription struct {
	pattern string

	ch chan Message

	mu           sync.Mutex
	closed       bool
	lastEnqueued uint64 // 已入队的最大序列号，用于订阅侧去重
	lastAcked    uint64 // 已确认位点
	stats        Stats

	onClose func(*Subscription) // 退订时通知 Broker 移除，至多执行一次
}

// C 返回消息交付通道。退订或 Broker 关闭后，已入队消息仍可被排空读取，
// 排空后通道关闭（range 可正常结束）。
func (s *Subscription) C() <-chan Message {
	return s.ch
}

// enqueue 由 Broker 调用：去重后入队；队列满时按确定策略淘汰最旧消息。
// 返回本次是否真正入队，以及（若未入队）可区分的原因。
func (s *Subscription) enqueue(m Message) DropReason {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		s.stats.DroppedLate++
		return DropClosed
	}
	if m.Seq <= s.lastEnqueued {
		// 重复消息：明确拒绝交付并计数，不静默。
		s.stats.Duplicates++
		return DropNone
	}
	if len(s.ch) == cap(s.ch) {
		<-s.ch // 淘汰最旧，慢订阅者不阻塞发布方
		s.stats.DroppedFull++
	}
	s.ch <- m
	s.lastEnqueued = m.Seq
	s.stats.Delivered++
	return DropNone
}

// close 标记关闭并关闭交付通道，幂等。已入队消息保留在缓冲中供排空。
func (s *Subscription) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
}

// Ack 确认序列号 seq（含）之前的消息，推进确认位点。
// 位点回退（seq 小于当前位点）返回包装了 ErrPositionRegression 的错误，
// 错误信息中携带可区分的位点细节。
func (s *Subscription) Ack(seq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq < s.lastAcked {
		return fmt.Errorf("%w: current position %d, attempted ack %d",
			ErrPositionRegression, s.lastAcked, seq)
	}
	s.lastAcked = seq
	return nil
}

// Position 返回当前已确认位点。
func (s *Subscription) Position() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastAcked
}

// Stats 返回该订阅的可区分统计快照。
func (s *Subscription) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

// Unsubscribe 退订：关闭交付通道（已入队消息可排空），并从 Broker 注销。幂等。
func (s *Subscription) Unsubscribe() {
	s.close()
	if s.onClose != nil {
		s.onClose(s)
	}
}
