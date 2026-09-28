package middleware

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Config 配置 Broker 行为。
type Config struct {
	// SupportedVersions 是可接受的消息版本集合，为空时默认仅 Version1。
	SupportedVersions []Version
	// QueueSize 是每个订阅的缓冲队列长度，<=0 时使用默认值 64。
	QueueSize int
	// Retention 是 Broker 保留用于断线续传的消息条数（全局日志），<=0 时使用默认值 1024。
	Retention int
}

const (
	defaultQueueSize = 64
	defaultRetention = 1024
	// replayPace 是重放期间队列暂满、等待订阅方排空的轮询节奏。
	replayPace = 200 * time.Microsecond
)

// SubscribeOptions 是建订阅时的可选项。零值即可用（默认 drop-oldest，兼容历史行为）。
type SubscribeOptions struct {
	// OverflowPolicy 声明队列满后的处理办法：PolicyDropOldest（默认，挤最旧）
	// 或 PolicyDropNewest（拒收新消息）。
	OverflowPolicy OverflowPolicy
}

// Broker 是进程内发布/订阅中间件，支持主题通配、可靠续传与背压控制。
//
// 序列号语义：Broker 为每条成功发布的消息分配全局单调递增的 Seq（从 1 开始），
// 校验失败（空主题、版本不兼容、已关闭）的发布不占用序列号，
// 因此订阅侧可将 Seq 作为严格递增的位点进行确认与续传。
type Broker struct {
	mu     sync.Mutex
	cfg    Config
	closed bool

	seq uint64
	log []Message // 全局保留日志，按 Seq 升序，超出 Retention 时裁掉最旧
	// logBase 是保留窗口内最早可用消息的 Seq（即 log[0].Seq），日志为空时为 0。
	// 发布路径在 mu 内更新，订阅 Ack 路径通过原子读访问，避免锁反转。
	logBase atomic.Uint64

	subs     map[*Subscription]struct{}
	replayWg sync.WaitGroup // 进行中的建连快照重放；Close 等其收尾以保证统计完整
}

// NewBroker 创建一个 Broker，规范化配置中的缺省值。
func NewBroker(cfg Config) *Broker {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultQueueSize
	}
	if cfg.Retention <= 0 {
		cfg.Retention = defaultRetention
	}
	return &Broker{
		cfg:  cfg,
		subs: make(map[*Subscription]struct{}),
	}
}

// oldestRetained 返回保留窗口内最早可用消息的 Seq；无保留消息时返回 0。
func (b *Broker) oldestRetained() uint64 {
	return b.logBase.Load()
}

// Publish 校验并发布一条消息：空主题返回 ErrEmptyTopic，
// 版本不兼容返回 ErrUnsupportedVersion，已关闭返回 ErrClosed。
// 成功时返回 Broker 分配的序列号。
func (b *Broker) Publish(topic string, version Version, payload []byte) (uint64, error) {
	if topic == "" {
		return 0, ErrEmptyTopic
	}
	m := Message{Topic: topic, Version: version, Payload: append([]byte(nil), payload...)}
	if err := Validate(m, b.cfg.SupportedVersions); err != nil {
		return 0, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, ErrClosed
	}
	b.seq++
	m.Seq = b.seq
	b.log = append(b.log, m)
	if len(b.log) > b.cfg.Retention {
		b.log = append([]Message(nil), b.log[len(b.log)-b.cfg.Retention:]...)
	}
	b.logBase.Store(b.log[0].Seq)
	for s := range b.subs {
		if !MatchTopic(s.pattern, topic) {
			continue
		}
		if s.replaying {
			// 建连快照重放进行中：实时消息先挂起，重放结束后在同一临界区内
			// 按序接续投递，保证该订阅看到的顺序与一直在线完全一致。
			s.pending = append(s.pending, m)
			continue
		}
		s.enqueue(m)
	}
	return m.Seq, nil
}

// Subscribe 按模式订阅，使用默认选项（drop-oldest）。等价于 SubscribeWith(pattern, resumeFrom, SubscribeOptions{})。
func (b *Broker) Subscribe(pattern string, resumeFrom uint64) (*Subscription, error) {
	return b.SubscribeWith(pattern, resumeFrom, SubscribeOptions{})
}

// SubscribeWith 按模式与背压选项订阅。resumeFrom 为已确认位点（全局序列号），
// 0 表示从头开始；重连时传入上次确认的位点，Broker 会从保留日志中重放该位点
// 之后、且匹配模式的消息（含曾因队列满被丢弃的空档），补完结果与一直在线一致：
// 不重不漏（前提为重放窗口内订阅方能排空队列，实时路径同样按策略背压）。
//
// 重放受 Retention 限制：resumeFrom 早于保留窗口起点时返回包装
// ErrPositionExpired 的错误（中间缺口已不可恢复），订阅不会建立。
func (b *Broker) SubscribeWith(pattern string, resumeFrom uint64, opts SubscribeOptions) (*Subscription, error) {
	if pattern == "" {
		return nil, ErrEmptyTopic
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	if base := b.logBase.Load(); base > 0 && resumeFrom != 0 && resumeFrom < base {
		return nil, fmt.Errorf("%w: resume position %d predates retention window start %d",
			ErrPositionExpired, resumeFrom, base)
	}
	sub := &Subscription{
		pattern:   pattern,
		policy:    opts.OverflowPolicy,
		ch:        make(chan Message, b.cfg.QueueSize),
		done:      make(chan struct{}),
		lastAcked: resumeFrom,
		gaps:      make(map[uint64]struct{}),
		broker:    b,
		onClose:   b.removeSub,
	}
	// 快照需要重放的日志段（Seq 连续、按序）。
	snapshot := make([]Message, 0)
	for _, m := range b.log {
		if m.Seq > resumeFrom && MatchTopic(pattern, m.Topic) {
			snapshot = append(snapshot, m)
		}
	}
	if len(snapshot) == 0 {
		// 无历史缺口：直接进入实时投递。
		b.subs[sub] = struct{}{}
		return sub, nil
	}
	sub.replaying = true
	b.subs[sub] = struct{}{}
	b.replayWg.Add(1)
	go b.replay(sub, snapshot)
	return sub, nil
}

// replay 异步完成建连补齐，分两阶段但共用同一套“逐条、非阻塞、按消费者排空节奏
// 重试”的投递循环：
//  1. 快照阶段：严格按 Seq 升序投递 resumeFrom 之后的保留日志；
//  2. 挂起阶段：投递重放期间到达、缓存在 pending 的实时消息。
//
// 两阶段都不丢消息（队列满就等一下重试同一条），且不长期持有 broker.mu
// 阻塞发布路径。重放期间发布路径把实时消息追加到 pending；第二阶段循环到
// “pending 已全部投递”的同一临界区内才把 replaying 翻为 false，此后实时消息
// 走正常 enqueue。因此该订阅补齐后看到的序列与“一直在线”完全一致：不重不漏、
// 顺序一致（前提：消费者最终排空队列；补齐结束后真正的持续过载仍按订阅策略背压）。
//
// 退出时保证：replaying 复位、pending 全部记账（入队或 DroppedLate）、
// 若关闭已发生则关闭交付通道——这样 Close 等待本函数后得到的统计恒等式完整。
func (b *Broker) replay(sub *Subscription, snapshot []Message) {
	defer b.replayWg.Done()

	// feed 在 broker.mu 内取下一条待投递消息：先快照、后 pending。
	j := 0
	feed := func() (Message, bool) {
		if j < len(snapshot) {
			return snapshot[j], true
		}
		k := j - len(snapshot)
		if k < len(sub.pending) {
			return sub.pending[k], true
		}
		return Message{}, false
	}

	for {
		b.mu.Lock()
		if b.closed {
			// 尚未投递的快照与挂起消息全部计 late（这些消息此前没记过 Matched）。
			if remaining := len(snapshot) + len(sub.pending) - j; remaining > 0 {
				sub.markLate(remaining)
			}
			sub.pending = nil
			sub.replaying = false
			sub.close()
			b.mu.Unlock()
			return
		}
		m, more := feed()
		if !more {
			// 同一临界区内完成切换：此后实时消息走正常 enqueue，顺序不乱。
			sub.pending = nil
			sub.replaying = false
			b.mu.Unlock()
			return
		}
		r := sub.deliverReplay(m)
		if r == DropClosed || sub.isClosed() {
			// 退订/关闭发生在投递窗口：当前条若已由 deliverReplay 计了 late，
			// 计算剩余时扣除它（它在“待投递总数”里），避免重复计数。
			remaining := len(snapshot) + len(sub.pending) - j
			if r == DropClosed && remaining > 0 {
				remaining--
			}
			if remaining > 0 {
				sub.markLate(remaining)
			}
			sub.pending = nil
			sub.replaying = false
			b.mu.Unlock()
			return
		}
		done := sub.done
		b.mu.Unlock()
		if r == DropNewest {
			// 队列暂满：等消费者排空后重试同一条（j 不前进、不记账、不丢弃）。
			select {
			case <-done:
				// 关闭：下一轮在 b.closed / isClosed 分支统一收尾记账。
			case <-time.After(replayPace):
			}
			continue
		}
		j++
	}
}

func (b *Broker) removeSub(s *Subscription) {
	b.mu.Lock()
	if s.replaying {
		// 重放 goroutine 仍在引用本订阅：只标记关闭、解除发布路径可见性，
		// 由重放收尾统一复位（重放中途检测到关闭即退出，不再触碰 b.subs）。
		s.close()
		delete(b.subs, s)
		b.mu.Unlock()
		return
	}
	delete(b.subs, s)
	b.mu.Unlock()
}

// Close 关闭 Broker，幂等且与并发 Publish/Subscribe/Unsubscribe 安全：
//   - 之后的 Publish/Subscribe 返回 ErrClosed；
//   - 关闭前已入队到各订阅缓冲的消息保留，订阅方可排空后看到通道关闭；
//   - 关闭后到达订阅的消息按 DropClosed 计入统计（确定语义：不再交付）。
//
// Close 会等待建连重放收尾，确保关闭后查询到的 Stats 恒等式完整。
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	for s := range b.subs {
		s.close()
	}
	b.subs = make(map[*Subscription]struct{})
	b.mu.Unlock()

	b.replayWg.Wait()
	return nil
}
