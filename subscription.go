package pubsub

import (
	"container/list"
	"sync"
)

// OverflowStrategy 是订阅者投递队列超限时的确定策略。
type OverflowStrategy string

const (
	// OverflowDropNewest：丢弃最新到达的消息（旧消息保留，保证旧位点优先可续传）。
	OverflowDropNewest OverflowStrategy = "drop_newest"
	// OverflowDropOldest：丢弃队列中最旧的未确认消息，为新消息腾位（新值优先）。
	OverflowDropOldest OverflowStrategy = "drop_oldest"
	// OverflowBlock：反压，仅阻塞该订阅者自己的推送协程等待空位；
	// 不阻塞发布方与其他订阅者（发布只写入无界 inbox）。Broker 关闭时立即解除。
	OverflowBlock OverflowStrategy = "block"
)

// OverflowCounters 记录一个订阅者生命周期内的投递与背压统计，各字段含义可区分。
type OverflowCounters struct {
	Enqueued       uint64 // 已进入 inbox 的消息总数（含重放）
	Delivered      uint64 // 已成功推入投递通道、交给消费方的消息数
	DroppedNewest  uint64 // 因队列满且策略为 drop_newest 而丢弃的新消息数
	DroppedOldest  uint64 // 因队列满且策略为 drop_oldest 而丢弃的旧消息数
	Duplicates     uint64 // 位点 <= 已确认位点、被判定重复而拒绝交付的消息数
	RejectedClosed uint64 // 订阅已关闭后投递/确认被拒绝的次数
}

// Subscription 表示一个订阅读者。
//
// 并发模型：
//   - Publish 路径只做 O(1) 的 inbox 追加（mu 保护的链表），不按订阅者慢度阻塞；
//   - 每个订阅者有且仅有一个 dispatch goroutine，把 inbox 按 FIFO 转入有界 queue；
//   - 消费方从 Deliveries() 读取，用 Ack(position) 确认位点；
//   - 位点严格单调：回退拒绝（ReasonPositionRollback），越前拒绝（ReasonPositionAhead）。
type Subscription struct {
	id       string
	pattern  Pattern
	depth    int
	strategy OverflowStrategy

	mu        sync.Mutex
	pending   *list.List          // 待 dispatch 的 Delivery，FIFO
	committed uint64              // 已确认位点（所有 <= committed 的消息视为完成）
	highWater uint64              // 已真正交付给消费方的最大位点（ACK 上界）
	gaps      map[uint64]struct{} // 因背压被丢弃、消费方从未见过的位点
	closed    bool
	counters  OverflowCounters

	queue  chan Delivery // 有界投递通道，消费方读取；只由 dispatch goroutine 发送/关闭
	notify chan struct{} // inbox 非空信号，容量 1
	done   chan struct{} // 关闭信号
}

func newSubscription(id string, p Pattern, depth int, strategy OverflowStrategy, resumeFrom uint64) *Subscription {
	if depth < 1 {
		depth = 1
	}
	s := &Subscription{
		id:        id,
		pattern:   p,
		depth:     depth,
		strategy:  strategy,
		pending:   list.New(),
		committed: resumeFrom,
		gaps:      make(map[uint64]struct{}),
		queue:     make(chan Delivery, depth),
		notify:    make(chan struct{}, 1),
		done:      make(chan struct{}),
	}
	return s
}

// start 启动唯一的 dispatch goroutine。
func (s *Subscription) start() { go s.dispatchLoop() }

// enqueue 由 Broker 在发布或续传重放时调用：把一条消息放入 inbox。
// 永不按消费速度阻塞调用方；订阅已关闭则明确拒绝。
// 位点 <= committed 的消息在此被判定为重复，绝不进入投递通道。
func (s *Subscription) enqueue(d Delivery) error {
	s.mu.Lock()
	if s.closed {
		s.counters.RejectedClosed++
		s.mu.Unlock()
		return rejectf(ReasonSubscriptionClosed, "subscriber=%s is closed, drop pos=%d", s.id, d.Position)
	}
	if d.Position <= s.committed {
		s.counters.Duplicates++
		s.mu.Unlock()
		return nil // 已确认过的重复消息：幂等吸收，不报错也不交付
	}
	s.counters.Enqueued++
	s.pending.PushBack(d)
	s.mu.Unlock()

	select {
	case s.notify <- struct{}{}:
	default:
	}
	return nil
}

// pop 取出 inbox 队首；调用发生在 dispatch goroutine。
func (s *Subscription) popHead() (Delivery, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 入队后可能被提前到达的 ACK 覆盖（防御性去重）。
	for s.pending.Front() != nil {
		front := s.pending.Front()
		d := s.pending.Remove(front).(Delivery)
		if d.Position <= s.committed {
			s.counters.Duplicates++
			continue
		}
		return d, true
	}
	return Delivery{}, false
}

// dispatchLoop 是每个订阅者唯一的推送协程。
func (s *Subscription) dispatchLoop() {
	for {
		select {
		case <-s.notify:
		case <-s.done:
			// 关闭语义：inbox 中尚未进入投递通道的消息确定作废；
			// 已经进入 queue 缓冲的消息保留，消费方仍可读尽，读尽后得到通道关闭。
			close(s.queue)
			return
		}

		for {
			d, ok := s.popHead()
			if !ok {
				break
			}
			switch s.strategy {
			case OverflowBlock:
				select {
				case s.queue <- d:
					s.markDelivered(d.Position)
				case <-s.done:
					close(s.queue)
					return
				}
			case OverflowDropNewest:
				select {
				case s.queue <- d:
					s.markDelivered(d.Position)
				default:
					// 队列满：放弃本条新消息。消息仍在 Broker 续传日志中，重连可重获；
					// 位点记入 gaps，消费方无法在不重连的情况下确认越过它（见 Ack）。
					s.mu.Lock()
					s.counters.DroppedNewest++
					s.gaps[d.Position] = struct{}{}
					s.mu.Unlock()
				}
			case OverflowDropOldest:
				select {
				case s.queue <- d:
					s.markDelivered(d.Position)
				default:
					// 队列满：挤出最旧的一条缓冲消息，再放入新消息。
					select {
					case old := <-s.queue:
						s.mu.Lock()
						s.counters.DroppedOldest++
						s.gaps[old.Position] = struct{}{}
						s.mu.Unlock()
					default:
					}
					s.queue <- d // 取出一条后必有空位；关闭时不会走到这里（队列只由本协程关闭）
					s.markDelivered(d.Position)
				}
			}
		}
	}
}

func (s *Subscription) markDelivered(pos uint64) {
	s.mu.Lock()
	s.counters.Delivered++
	if pos > s.highWater {
		s.highWater = pos
	}
	s.mu.Unlock()
}

// Deliveries 返回消费方读取的投递通道。
// Broker/Subscription 关闭后，通道中已缓冲的消息仍可读尽，随后通道关闭。
func (s *Subscription) Deliveries() <-chan Delivery { return s.queue }

// Ack 确认所有 position <= pos 的消息已处理，位点单调推进。
//   - pos < 已确认位点：ReasonPositionRollback（明确拒绝回退）；
//   - pos == 已确认位点：幂等成功；
//   - pos > 见过的最大位点：ReasonPositionAhead（拒绝确认从未投递的位点）；
//   - 订阅已关闭：ReasonSubscriptionClosed。
func (s *Subscription) Ack(pos uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		s.counters.RejectedClosed++
		return rejectf(ReasonSubscriptionClosed, "subscriber=%s ack pos=%d after close", s.id, pos)
	}
	if pos < s.committed {
		return rejectf(ReasonPositionRollback,
			"subscriber=%s ack rollback: committed=%d requested=%d", s.id, s.committed, pos)
	}
	if pos > s.highWater {
		return rejectf(ReasonPositionAhead,
			"subscriber=%s ack ahead: highwater(acknowledgeable)=%d requested=%d",
			s.id, s.highWater, pos)
	}
	if pos == s.committed {
		return nil
	}
	// 区间 (committed, pos] 内若存在背压丢弃位点，则不允许跳过确认：
	// 消费方从未见过该消息，静默推进会造成确定的消息丢失。
	// 明确拒绝并给出最小缺口位点；调用方应以当前 committed 为 resumeFrom 重连续传补齐。
	var firstGap uint64
	for g := range s.gaps {
		if g > s.committed && g <= pos {
			if firstGap == 0 || g < firstGap {
				firstGap = g
			}
		}
	}
	if firstGap != 0 {
		return rejectf(ReasonPositionGap,
			"subscriber=%s cannot ack pos=%d over dropped position=%d (committed=%d); "+
				"resume from %d after reconnect to refill",
			s.id, pos, firstGap, s.committed, s.committed)
	}
	s.committed = pos
	// 已确认覆盖的 gap 记录可清除。
	for g := range s.gaps {
		if g <= pos {
			delete(s.gaps, g)
		}
	}
	// 清除 inbox 中已被确认覆盖的迟到条目。
	removed := 0
	for e := s.pending.Front(); e != nil; {
		d := e.Value.(Delivery)
		next := e.Next()
		if d.Position <= pos {
			s.pending.Remove(e)
			removed++
		}
		e = next
	}
	if removed > 0 {
		s.counters.Duplicates += uint64(removed)
	}
	return nil
}

// Close 退订，幂等且并发安全。可由订阅者自己或 Broker.Close 调用。
func (s *Subscription) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	close(s.done)
	return nil
}

// Done 在订阅终止（退订或 broker 关闭）后关闭。
func (s *Subscription) Done() <-chan struct{} { return s.done }

// ID 返回订阅者标识。
func (s *Subscription) ID() string { return s.id }

// Counters 返回投递与背压统计快照。
func (s *Subscription) Counters() OverflowCounters {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counters
}

// CommittedPosition 返回当前已确认位点，供断线重连时作为 resumeFrom。
func (s *Subscription) CommittedPosition() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.committed
}
