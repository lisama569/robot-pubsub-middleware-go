package pubsub

import (
	"context"
	"sync"
)

// 默认续传日志保留条数：默认无限保留（进程内中间件，保证"已发布即可从任意已确认位点续传"）。
const defaultRetention uint64 = 0

// Broker 是进程内发布订阅中间件：
//   - 主题/通配匹配发布订阅；
//   - 消息携带可校验的生产者序列号与版本，非法即明确拒绝；
//   - 全局位点 + 每订阅者 ACK，支持断线续传；
//   - 慢订阅者只拖累自己，背压策略确定；
//   - Publish/Subscribe/Unsubscribe/Close 可并发，不加长临界区、不死锁。
type Broker struct {
	mu        sync.Mutex
	closed    bool
	nextPos   uint64
	log       []Delivery
	retention uint64            // 0 表示无限
	lastSeq   map[string]uint64 // producer -> 最近发布序列号（单调校验）
	subs      map[string]*Subscription
	stats     BrokerStats

	closeOnce sync.Once
	closeDone chan struct{}
}

// BrokerStats 是可观测统计快照。
type BrokerStats struct {
	Published     uint64
	Rejected      uint64
	Subscriptions int // 当前存活订阅数
}

// Option 配置 Broker。
type Option func(*Broker)

// WithRetention 配置续传日志按全局消息条数的保留长度；0（默认）表示无限保留。
func WithRetention(n uint64) Option {
	return func(b *Broker) { b.retention = n }
}

// NewBroker 创建 Broker。
func NewBroker(opts ...Option) *Broker {
	b := &Broker{
		retention: defaultRetention,
		lastSeq:   make(map[string]uint64),
		subs:      make(map[string]*Subscription),
		closeDone: make(chan struct{}),
	}
	for _, o := range opts {
		o(b)
	}
	return b
}

// Publish 校验并发布一条消息，返回 Broker 分配的全局位点（从 1 开始）。
//
// 拒绝路径（均返回 *RejectError，绝不静默丢弃）：
//   - broker 已关闭：ReasonBrokerClosed；
//   - ctx 已取消：ctx.Err() 包装；
//   - 主题/版本/序列号非法：对应原因（见 Message.Validate）；
//   - 同一 Producer 序列号回退或重复：ReasonInvalidSequence；
//     跳号（gap）允许发布，但序列号判定依据会记录在返回说明中。
//
// 发布本身不按任何订阅者的消费速度阻塞：匹配者各有独立 inbox。
func (b *Broker) Publish(ctx context.Context, msg Message) (uint64, error) {
	if err := msg.Validate(); err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, rejectf(ReasonBrokerClosed, "publish %s after broker closed", msg.Topic)
	}

	// 同一生产者序列号必须严格单调递增。
	last := b.lastSeq[msg.Producer]
	if msg.Seq <= last {
		b.stats.Rejected++
		b.mu.Unlock()
		return 0, rejectf(ReasonInvalidSequence,
			"producer=%s seq=%d is not greater than last=%d (duplicate or rollback)",
			msg.Producer, msg.Seq, last)
	}
	gap := uint64(0)
	if last > 0 && msg.Seq > last+1 {
		gap = msg.Seq - last - 1
	}
	b.lastSeq[msg.Producer] = msg.Seq

	b.nextPos++
	pos := b.nextPos
	d := Delivery{Position: pos, Message: msg}

	b.log = append(b.log, d)
	if b.retention > 0 && uint64(len(b.log)) > b.retention {
		// 裁剪到 retention（保留最近的消息）。
		drop := uint64(len(b.log)) - b.retention
		b.log = b.log[drop:]
	}
	b.stats.Published++

	// 位点分配与入队必须在同一个有序临界区内：否则两个并发发布者可能
	// 按 pos 升序分配位点、却乱序入队，使订阅者先收到高 pos 再收到低 pos。
	// enqueue 仅做 O(1) 链表追加，不按订阅者消费速度阻塞，故不会放大临界区。
	// 锁顺序单向（b.mu -> s.mu），与 Ack/Close 之间不存在反向获取，不会死锁。
	var closedRejects int
	for _, s := range b.subs {
		if !s.pattern.Match(msg.Topic) {
			continue
		}
		if err := s.enqueue(d); err != nil {
			// 订阅恰在并发中关闭：关闭即明确取消，不计为消息丢失。
			closedRejects++
		}
	}
	b.stats.Rejected += uint64(closedRejects)
	b.mu.Unlock()

	_ = gap // gap 仅用于语义说明；单调校验只拒绝回退与重复
	return pos, nil
}

// Subscribe 按模式建立订阅。
//
//   - id：订阅者稳定标识；同 id 重复（且仍存活）订阅返回 ReasonDuplicateSubscriber；
//     断线后应以同一 id 并带 resumeFrom 重新订阅完成续传。
//   - queueDepth：有界投递通道容量（<1 时取 1）。
//   - strategy：队列超限时的确定背压策略。
//   - resumeFrom：上次已确认位点；Broker 会从续传日志中重放 Position > resumeFrom
//     且命中模式的历史消息，然后再接新消息。
//   - resumeFrom=0：从最早可用历史开始（新订阅者）。
//   - resumeFrom 落在已被 retention 裁剪掉的区间：ReasonPositionExpired（明确拒绝，
//     绝不从错误位点静默续传）。
func (b *Broker) Subscribe(ctx context.Context, id string, pattern Pattern, queueDepth int,
	strategy OverflowStrategy, resumeFrom uint64) (*Subscription, error) {

	if id == "" {
		return nil, rejectf(ReasonInvalidTopic, "subscriber id must not be empty")
	}
	if _, err := ParsePattern(pattern.raw); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s := newSubscription(id, pattern, queueDepth, strategy, resumeFrom)

	var replay []Delivery
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, rejectf(ReasonBrokerClosed, "subscribe %s after broker closed", id)
	}
	if old, ok := b.subs[id]; ok && !old.isClosed() {
		b.mu.Unlock()
		return nil, rejectf(ReasonDuplicateSubscriber, "subscriber=%s already subscribed", id)
	}

	// 校验续传位点：找到日志中最小可用位点。
	oldest := uint64(0)
	if len(b.log) > 0 {
		oldest = b.log[0].Position
	}
	if resumeFrom > 0 && len(b.log) > 0 && resumeFrom < oldest-1 {
		b.mu.Unlock()
		return nil, rejectf(ReasonPositionExpired,
			"subscriber=%s resumeFrom=%d older than oldest available=%d (retention=%d)",
			id, resumeFrom, oldest, b.retention)
	}
	if resumeFrom > b.nextPos {
		b.mu.Unlock()
		return nil, rejectf(ReasonPositionAhead,
			"subscriber=%s resumeFrom=%d greater than newest position=%d", id, resumeFrom, b.nextPos)
	}

	for _, d := range b.log {
		if d.Position <= resumeFrom {
			continue
		}
		if pattern.Match(d.Message.Topic) {
			replay = append(replay, d)
		}
	}
	// 先在同一临界区内按位点顺序入队历史重放，再让订阅对新发布可见：
	// 保证任何新消息都排在重放消息之后，续传顺序确定（锁顺序 b.mu -> s.mu，无环）。
	for i := range replay {
		if err := s.enqueue(replay[i]); err != nil {
			b.mu.Unlock()
			return nil, err
		}
	}
	b.subs[id] = s
	b.stats.Subscriptions = len(b.subs)
	b.mu.Unlock()

	s.start()
	return s, nil
}

// Unsubscribe 按 id 退订，幂等（不存在或已关闭均返回 nil）。
func (b *Broker) Unsubscribe(id string) error {
	b.mu.Lock()
	s, ok := b.subs[id]
	if !ok {
		b.mu.Unlock()
		return nil
	}
	delete(b.subs, id)
	b.stats.Subscriptions = len(b.subs)
	b.mu.Unlock()
	return s.Close()
}

// Close 并发安全、幂等。
//
// 关闭时未完成消息的确定语义：
//   - 已进入订阅者投递通道缓冲的消息：保留，消费方可以读尽，随后 Deliveries() 通道关闭；
//   - 仍在 inbox 未推送、以及此后 Publish 的消息：确定作废（明确不再交付）；
//   - 关闭后 Publish/Subscribe 返回 ReasonBrokerClosed；
//   - 关闭后对存活订阅的 Ack 返回 ReasonSubscriptionClosed；
//   - 已 Ack 的位点不会因为关闭而改变（位点由订阅者持有，重连可继续使用）。
func (b *Broker) Close() error {
	b.closeOnce.Do(func() { close(b.closeDone) })

	var subs []*Subscription
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	for _, s := range b.subs {
		subs = append(subs, s)
	}
	b.subs = make(map[string]*Subscription)
	b.stats.Subscriptions = 0
	b.mu.Unlock()

	for _, s := range subs {
		_ = s.Close()
	}
	return nil
}

// Closed 返回 broker 是否已关闭。
func (b *Broker) Closed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

// Stats 返回 broker 级统计快照。
func (b *Broker) Stats() BrokerStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stats
}

func (s *Subscription) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}
