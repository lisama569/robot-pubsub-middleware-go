package middleware

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
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
	stats     Stats

	// retentionFloor 是 Broker 保留日志当前最早消息的序列号（0 表示尚无日志）。
	// 由 Broker 在建立订阅与裁剪保留窗口时更新（atomic，免锁读取）；
	// Ack 据此区分丢失位点是“仍在窗口内、重连可补齐”还是“已滚出窗口、无法补齐”。
	retentionFloor atomic.Uint64

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

// noteTrimmed 由 Broker 在裁剪保留窗口时调用（持有 broker.mu，锁序 broker.mu -> sub.mu）。
// 续传积压中的订阅还没来得及补到的匹配消息一旦被裁掉，就永远无法补齐：
// 把它们记为丢失位点，Ack 跨过时才能按 ErrPositionExpired 明确拒绝，
// 而不是让确认位点静默漏过这段空档。
// 过滤条件 m.Seq > s.lastSeen 恰好选中“尚未被续传游标考虑过”的匹配消息：
// 续传路径上已被考虑且匹配的消息要么已入队（lastSeen 随之推进），
// 要么因队列满停在游标上（下标不小于游标，不会被本次裁剪波及）。
func (s *Subscription) noteTrimmed(trimmed []Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range trimmed {
		if m.Seq > s.lastSeen && MatchTopic(s.pattern, m.Topic) {
			s.addLostLocked(m.Seq)
		}
	}
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
//   - ErrAckGap：(当前位点, seq] 区间内存在已判定丢失、但仍在保留窗口内、
//     重连续传即可补齐的位点——继续等补齐即可，不要重置；
//   - ErrPositionExpired：(当前位点, seq] 区间内存在已判定丢失、且已滚出保留
//     窗口的位点——该位点怎么重连都补不回来，确认位点永远无法跨过它，
//     应尽快重置（按新位点重新订阅）而不是继续等。
//
// 两种空档的边界按丢失位点与保留窗口下沿比较判定：丢失位点 == 窗口最早
// 序列号时消息仍在窗口内（可补齐，报 ErrAckGap）；严格小于下沿才算彻底过期。
func (s *Subscription) Ack(seq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq < s.lastAcked {
		return fmt.Errorf("%w: current position %d, attempted ack %d",
			ErrPositionRegression, s.lastAcked, seq)
	}
	if seq > s.lastSeen {
		return fmt.Errorf("%w: attempted ack %d beyond delivered watermark %d",
			ErrAckNotDelivered, seq, s.lastSeen)
	}
	i := sort.Search(len(s.lost), func(i int) bool { return s.lost[i] > s.lastAcked })
	if i < len(s.lost) && s.lost[i] <= seq {
		gap := s.lost[i]
		if floor := s.retentionFloor.Load(); floor > 0 && gap < floor {
			return fmt.Errorf("%w: attempted ack %d skips position %d which is older than oldest retained %d (position %d); reset the subscription",
				ErrPositionExpired, seq, gap, floor, s.lastAcked)
		}
		return fmt.Errorf("%w: attempted ack %d skips undelivered position %d (position %d); still recoverable via resume",
			ErrAckGap, seq, gap, s.lastAcked)
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
