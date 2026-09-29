package middleware

import (
	"fmt"
	"sort"
	"sync"
)

// Subscription 表示一个订阅。消息经 C() 交付；消费后调用 Ack 推进确认位点。
//
// 并发约定：所有向交付通道的写入/淘汰都在 s.mu 内以“非阻塞 select”原子完成，
// 不做 len(ch)==cap 这类先查后做的判断（消费者无锁读通道，check 与 act 之间
// 状态会变，会造成收发错配）。offer 由 Broker 在发布/续传路径调用，锁序固定为
// broker.mu -> sub.mu；C/Ack/Unsubscribe 可由订阅方任意并发调用。
type Subscription struct {
	pattern string
	policy  BackpressurePolicy

	ch chan Message

	mu        sync.Mutex
	closed    bool
	lastSeen  uint64   // 已真正交到本订阅手上的最大匹配序列号（入队或被挤掉），用于去重与确认水位
	lastAcked uint64   // 已确认位点
	lost      []uint64 // 已判定丢失、尚未靠续传补齐的匹配位点，严格升序
	// oldestRetained 是 Broker 最近一次推送的保留日志最早 Seq（0 表示日志为空）。
	// lost 中 seq >= oldestRetained 的空档仍可重连补齐，< oldestRetained 则已
	// 滚出保留窗口、不可补齐。由 Broker 持有 broker.mu 时调用 setRetentionWatermark
	// 更新（锁序 broker.mu -> sub.mu），订阅侧只在 sub.mu 内读取。
	oldestRetained uint64
	stats          Stats

	// 续传游标，仅由 Broker 持有 broker.mu 时访问：
	// replayIdx 是下一条待考虑的保留日志下标；replayPending 表示尚有积压未续完。
	replayIdx     int
	replayPending bool

	onClose func(*Subscription) // 退订时通知 Broker，至多执行一次
}

// C 返回消息交付通道。退订或 Broker 关闭后，已入队消息仍可被排空读取，
// 排空后通道关闭（range 可正常结束）。
func (s *Subscription) C() <-chan Message {
	return s.ch
}

// Policy 返回建立订阅时声明的背压策略。
func (s *Subscription) Policy() BackpressurePolicy {
	return s.policy
}

// isActive 报告订阅是否仍处于可交付状态（未退订、Broker 未关闭）。
// 由 Broker 持有 broker.mu 时调用。
func (s *Subscription) isActive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.closed
}

// isClosed 报告订阅是否已关闭。由 Broker 持有 broker.mu 时调用。
func (s *Subscription) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// setRetentionWatermark 由 Broker 持有 broker.mu 时调用，推送保留日志最早 Seq。
// oldest==0 表示日志为空（尚无消息可补）。水位单调不降，调用方需保证这一点。
func (s *Subscription) setRetentionWatermark(oldest uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.oldestRetained = oldest
}

// offer 由 Broker 调用，把一条匹配消息交给本订阅。replay=false 为实时投递，
// 队列满时按订阅策略处理（DropOldest 挤旧、DropNewest 拒收），任何分支都不阻塞，
// 因此慢订阅者拖不住发布方与其他订阅者；replay=true 为续传补入，队列满时返回
// enqueued=false 暂停（绝不淘汰、不丢），由续传泵稍后重试同一条。
//
// 返回 (enqueued, reason)：enqueued 表示消息本次是否真正进入队列（重复消息视为
// 已处理返回 true）；reason 给出未入队时的可区分原因。
func (s *Subscription) offer(m Message, replay bool) (bool, DropReason) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		s.stats.DroppedLate++
		return false, DropClosed
	}
	if m.Seq <= s.lastSeen {
		// 续传时 lastSeen 只对真正交付过的消息推进，故 seq<=lastSeen 只会是重复入队。
		s.stats.Duplicates++
		return true, DropNone
	}
	if replay {
		// 续传补入：满则暂停。非阻塞写，绝不淘汰。
		select {
		case s.ch <- m:
			s.lastSeen = m.Seq
			s.stats.Delivered++
			return true, DropNone
		default:
			return false, DropNone
		}
	}
	// 实时投递：先尝试非阻塞写。
	select {
	case s.ch <- m:
		s.lastSeen = m.Seq
		s.stats.Delivered++
		return true, DropNone
	default:
	}
	switch s.policy {
	case PolicyDropNewest:
		// 留着先来的：拒收这条新来的，已排队的消息原封不动。
		// 注意 lastSeen 不推进——这条消息没交到手上，重连续传时仍需补到它。
		s.addLostLocked(m.Seq)
		s.stats.DroppedNewest++
		return false, DropNewestRejected
	default: // PolicyDropOldest
		// 留着最新的：原子地尝试挤掉一条最旧的。
		select {
		case dropped := <-s.ch:
			s.addLostLocked(dropped.Seq)
			s.stats.DroppedOldest++
			// 腾出一个槽，立即放入新消息（槽位刚由上面的接收产生，写必成功）。
			s.ch <- m
			s.lastSeen = m.Seq
			s.stats.Delivered++
			return true, DropNone
		default:
			// 队列在“写失败”与“此刻”之间被消费者读空了：现在必有空位，重试一次写。
			select {
			case s.ch <- m:
				s.lastSeen = m.Seq
				s.stats.Delivered++
				return true, DropNone
			default:
				// 极端情况下又被填满（并发发布受 broker.mu 串行化，这里实际不会发生），
				// 退化为再挤一次，保证不阻塞且消息不凭空消失。
				dropped := <-s.ch
				s.addLostLocked(dropped.Seq)
				s.stats.DroppedOldest++
				s.ch <- m
				s.lastSeen = m.Seq
				s.stats.Delivered++
				return true, DropNone
			}
		}
	}
}

// enqueue 实时投递的便捷封装。
func (s *Subscription) enqueue(m Message) DropReason {
	_, r := s.offer(m, false)
	return r
}

// addLostLocked 记录一个判定丢失的位点，保持严格升序。
func (s *Subscription) addLostLocked(seq uint64) {
	i := sort.Search(len(s.lost), func(i int) bool { return s.lost[i] >= seq })
	if i < len(s.lost) && s.lost[i] == seq {
		return
	}
	s.lost = append(s.lost, 0)
	copy(s.lost[i+1:], s.lost[i:])
	s.lost[i] = seq
}

// pruneLostLocked 清除已被确认位点覆盖的丢失记录。
func (s *Subscription) pruneLostLocked() {
	i := sort.Search(len(s.lost), func(i int) bool { return s.lost[i] > s.lastAcked })
	s.lost = append(s.lost[:0], s.lost[i:]...)
}

// close 标记关闭并关闭交付通道，幂等。已入队消息保留在缓冲中供排空。
// 所有通道写入都在 s.mu 内，close(ch) 与之互斥，不会向已关闭通道投递。
func (s *Subscription) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
}

// Ack 确认序列号 seq（含）之前的消息，推进确认位点。以下情况被明确拒绝，
// 且原因可通过 errors.Is 区分，位点保持不变：
//   - ErrPositionRegression：seq 小于当前已确认位点（回退）；
//   - ErrAckNotDelivered：seq 超过该订阅已交到手上的水位（确认到一个没交到手的位置）；
//   - ErrAckGap：(当前位点, seq] 区间内存在已判定丢失、尚未补齐、且仍在保留窗口内
//     的位点（中间留了空档，但还来得及靠重连续传补齐，继续等待即可）；
//   - ErrAckGapExpired：区间内的空档位点已滚出保留窗口，怎么重连都补不回来，
//     应尽快重置订阅。它同时属于 ErrAckGap（errors.Is 两者皆命中）。
//
// 判定边界：保留窗口最早尚存活的位点为 oldestRetained 时，丢失位点
// g >= oldestRetained 仍可补齐（卡在窗口边缘、只差一点点也算可补，绝不误判过期）；
// g < oldestRetained 才是彻底不可补齐。只检查区间内最早的一个丢失位点即可：
// 它若不可补，后面的更旧缺口同样不可补，错误信息明确指出该位点。
func (s *Subscription) Ack(seq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq < s.lastAcked {
		return fmt.Errorf("%w: current position %d, attempted ack %d",
			ErrPositionRegression, s.lastAcked, seq)
	}
	// 区间内最早一个还没补齐的丢失位点，决定拒绝原因。
	i := sort.Search(len(s.lost), func(i int) bool { return s.lost[i] > s.lastAcked })
	if i < len(s.lost) && s.lost[i] <= seq {
		g := s.lost[i]
		// 从未发布到保留日志为空（oldestRetained==0）之间不存在“滚出窗口”：
		// 水位为 0 时一律按仍可补齐处理。
		if s.oldestRetained != 0 && g < s.oldestRetained {
			return fmt.Errorf("%w: attempted ack %d skips unrecoverable position %d (position %d, oldest retained %d)",
				ErrAckGapExpired, seq, g, s.lastAcked, s.oldestRetained)
		}
		return fmt.Errorf("%w: attempted ack %d skips undelivered position %d (position %d)",
			ErrAckGap, seq, g, s.lastAcked)
	}
	if seq > s.lastSeen {
		return fmt.Errorf("%w: attempted ack %d beyond delivered watermark %d",
			ErrAckNotDelivered, seq, s.lastSeen)
	}
	s.lastAcked = seq
	s.pruneLostLocked()
	return nil
}

// Position 返回当前已确认位点。
func (s *Subscription) Position() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastAcked
}

// Stats 返回该订阅本次生命周期内的消息去向统计快照。
func (s *Subscription) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

// Unsubscribe 退订：关闭交付通道（已入队消息可排空），并通知 Broker。幂等。
func (s *Subscription) Unsubscribe() {
	s.close()
	if s.onClose != nil {
		s.onClose(s)
	}
}
