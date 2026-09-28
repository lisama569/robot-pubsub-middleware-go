package middleware

import (
	"fmt"
	"sync"
)

// Subscription 表示一个订阅。消息经 C() 交付；消费后调用 Ack 推进确认位点。
//
// 并发约定：enqueue 由 Broker 在发布路径上调用；C/Ack/Unsubscribe 可由订阅方
// 任意并发调用。锁序固定为 broker.mu -> sub.mu，反向不成立，因此不会死锁。
// 读取 Broker 保留窗口位点走原子读，不需要在 sub.mu 内反向获取 broker.mu。
type Subscription struct {
	pattern string
	policy  OverflowPolicy

	ch   chan Message
	done chan struct{} // close() 时关闭，用于唤醒重放等待

	mu           sync.Mutex
	closed       bool
	replaying    bool      // 仅由 Broker 在 broker.mu 内访问：建连快照重放进行中
	pending      []Message // 仅由 Broker 在 broker.mu 内访问：重放期间到达的实时消息
	lastEnqueued uint64    // 已命中入队判定的最大序列号，用于订阅侧去重
	lastAcked    uint64    // 已确认位点
	// gaps 记录“命中过本订阅、但未真正进入队列”的序列号（两类队列满丢弃）。
	// 这些位置形成确认空档：Ack 跨越时必须拒绝，直到重连续传补齐。
	// 成功确认后删除其之前的空档记录。
	gaps  map[uint64]struct{}
	stats Stats

	broker  *Broker
	onClose func(*Subscription) // 退订时通知 Broker 移除，至多执行一次
}

// C 返回消息交付通道。退订或 Broker 关闭后，已入队消息仍可被排空读取，
// 排空后通道关闭（range 可正常结束）。
func (s *Subscription) C() <-chan Message {
	return s.ch
}

// enqueue 由 Broker 在持有 broker.mu 时调用（因此同一订阅的投递天然串行）：
// 按“去重 -> 背压策略”的顺序处理一条命中消息。全程非阻塞——所有 channel 操作
// 都用带 default 的 select 瞬时完成，故可在持有 s.mu 时执行：慢订阅者既不会拖住
// 发布方或其他订阅者，也不会与 close()（同样需要 s.mu）形成死锁。
// 返回值区分本次去向（成功入队/挤旧/拒新/关闭），重复消息返回 DropNone。
func (s *Subscription) enqueue(m Message) DropReason {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.Matched++
	if s.closed {
		s.stats.DroppedLate++
		return DropClosed
	}
	if m.Seq <= s.lastEnqueued {
		// 重复消息：明确拒绝交付并计数，不静默。
		s.stats.Duplicates++
		return DropNone
	}
	switch s.policy {
	case PolicyDropNewest:
		// 留先来的：放得下就入队；放不下（队列仍满）直接拒收新消息。
		select {
		case s.ch <- m:
			s.lastEnqueued = m.Seq
			s.stats.Delivered++
			return DropNone
		default:
			s.stats.DroppedNewest++
			s.addGapLocked(m.Seq)
			s.lastEnqueued = m.Seq
			return DropNewest
		}
	default: // PolicyDropOldest
		// 留最新的：先尝试直接入队；只有确认队列满时才挤出一条最旧消息，
		// 挤出后重试。两层 select 均带 default，全程不阻塞；
		// 若挤出时消费者恰好排空了队列（default 分支），则重试直接入队，
		// 不会在其实有空位时误删消息。
		for {
			select {
			case s.ch <- m:
				s.lastEnqueued = m.Seq
				s.stats.Delivered++
				return DropNone
			default:
			}
			select {
			case oldest := <-s.ch:
				s.stats.DroppedOldest++
				s.addGapLocked(oldest.Seq)
			default:
				// 队列已被消费者腾出空位：回到外层重试直接入队。
			}
		}
	}
}

// addGapLocked 记录一个未成功交付的命中位置（调用方持有 mu）。
func (s *Subscription) addGapLocked(seq uint64) {
	if s.gaps == nil {
		s.gaps = make(map[uint64]struct{})
	}
	s.gaps[seq] = struct{}{}
}

// deliverReplay 由 Broker 在持有 broker.mu 时调用，向一条重放路径上的消息入队。
// 与实时 enqueue 的区别仅在于队列满时不丢不挤：返回 DropNewest 表示“此刻没有
// 空位”，且本条不记账；调用方（重放 goroutine）稍后重试同一条。
// 全程非阻塞 select，可安全持有 s.mu。
func (s *Subscription) deliverReplay(m Message) DropReason {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		s.stats.Matched++
		s.stats.DroppedLate++
		return DropClosed
	}
	if m.Seq <= s.lastEnqueued {
		s.stats.Matched++
		s.stats.Duplicates++
		return DropNone
	}
	select {
	case s.ch <- m:
		s.stats.Matched++
		s.lastEnqueued = m.Seq
		s.stats.Delivered++
		return DropNone
	default:
		// 队列暂满：不记账，调用方稍后重试。
		return DropNewest
	}
}

// close 标记关闭并关闭交付通道，幂等。已入队消息保留在缓冲中供排空。
func (s *Subscription) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.done)
	close(s.ch)
}

// isClosed 返回订阅是否已关闭（退订或 Broker 关闭）。
func (s *Subscription) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// markLate 由 Broker 在持有 broker.mu 时调用：把 n 条未投递消息计入
// Matched 与 DroppedLate（重放因关闭中止时的收尾统计）。
func (s *Subscription) markLate(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.Matched += uint64(n)
	s.stats.DroppedLate += uint64(n)
}

// Ack 确认序列号 seq（含）之前的消息，推进确认位点。下列情况被明确拒绝，
// 且错误可通过 errors.Is 区分，错误信息中携带位点细节便于排查：
//   - ErrPositionRegression：seq 小于当前位点（回退）；
//   - ErrAckNotDelivered：seq 超过本订阅已交付的前沿位点（还没交到手上）；
//   - ErrPositionExpired：确认区间内最早的空档已超出 Broker 保留窗口，不可恢复；
//   - ErrAckGap：确认区间内存在未交付空档（消息曾命中但被队列满丢弃），
//     需以当前 Position() 重连续传补齐后再确认。
//
// 已实际交付（入队或已被读走）的位置不受保留窗口滚动影响：只要区间内没有空档，
// 确认照常推进。
func (s *Subscription) Ack(seq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq < s.lastAcked {
		return fmt.Errorf("%w: current position %d, attempted ack %d",
			ErrPositionRegression, s.lastAcked, seq)
	}
	if seq == s.lastAcked {
		return nil
	}
	if seq > s.lastEnqueued {
		return fmt.Errorf("%w: attempted ack %d beyond delivered frontier %d",
			ErrAckNotDelivered, seq, s.lastEnqueued)
	}
	// 找出确认区间内最早的未交付空档。空档落在保留窗口之外即不可恢复。
	oldest := s.broker.oldestRetained()
	var hole uint64
	for g := range s.gaps {
		if g > s.lastAcked && g <= seq {
			if oldest > 0 && g < oldest {
				return fmt.Errorf("%w: missing seq %d predates retention window start %d",
					ErrPositionExpired, g, oldest)
			}
			if hole == 0 || g < hole {
				hole = g
			}
		}
	}
	if hole != 0 {
		return fmt.Errorf("%w: first missing seq %d between position %d and attempted ack %d",
			ErrAckGap, hole, s.lastAcked, seq)
	}
	s.lastAcked = seq
	// 已跨过的空档记录不再需要。
	for g := range s.gaps {
		if g <= seq {
			delete(s.gaps, g)
		}
	}
	return nil
}

// Position 返回当前已确认位点。
func (s *Subscription) Position() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastAcked
}

// Policy 返回建订阅时声明的背压策略。
func (s *Subscription) Policy() OverflowPolicy {
	return s.policy
}

// Stats 返回该订阅本次生命周期的去向汇总快照。退订或关闭后仍可查询。
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
