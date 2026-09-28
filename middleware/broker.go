package middleware

import (
	"fmt"
	"sync"
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

// SubscribeOptions 是建立订阅时的可选项。
type SubscribeOptions struct {
	// Policy 声明队列满时的背压处理方式。零值 PolicyDefault 规范化为 PolicyDropOldest。
	Policy BackpressurePolicy
}

const (
	defaultQueueSize = 64
	defaultRetention = 1024
	// replayFillInterval 是续传泵的兜底唤醒周期：即使没有新发布，
	// 消费腾出空位后最迟在此周期内续上下一条，避免忙等。
	replayFillInterval = 5 * time.Millisecond
)

// Broker 是进程内发布/订阅中间件，支持主题通配、可靠续传与背压控制。
//
// 序列号语义：Broker 为每条成功发布的消息分配全局单调递增的 Seq（从 1 开始），
// 校验失败（空主题、版本不兼容、已关闭）的发布不占用序列号，
// 因此订阅侧可将 Seq 作为严格递增的位点进行确认与续传。
type Broker struct {
	mu     sync.Mutex
	cfg    Config
	closed bool

	seq  uint64    // 已分配的最大全局序列号（保留窗口裁剪不影响该水位）
	log  []Message // 全局保留日志，按 Seq 升序，超出 Retention 时裁掉最旧
	subs map[*Subscription]struct{}

	// 续传泵：把各订阅积压的历史/拒收消息按全局顺序、在队列有空位时补进去。
	wakeCh chan struct{}
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewBroker 创建一个 Broker，规范化配置中的缺省值并启动续传泵。
func NewBroker(cfg Config) *Broker {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultQueueSize
	}
	if cfg.Retention <= 0 {
		cfg.Retention = defaultRetention
	}
	b := &Broker{
		cfg:    cfg,
		subs:   make(map[*Subscription]struct{}),
		wakeCh: make(chan struct{}, 1),
		stopCh: make(chan struct{}),
	}
	b.wg.Add(1)
	go b.replayPump()
	return b
}

// replayPump 在发布事件与兜底定时两种触发下推进所有订阅的续传。
// 每次唤醒只跑一轮（一轮内把每个订阅的空槽尽量补满），随后回到等待，
// 不在“有进展”时自我唤醒——否则消费者持续读取会让泵 100% 自旋、饿死锁与 Close。
func (b *Broker) replayPump() {
	defer b.wg.Done()
	ticker := time.NewTicker(replayFillInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.stopCh:
			return
		case <-b.wakeCh:
		case <-ticker.C:
		}
		b.fillReplays()
	}
}

// notify 非阻塞地唤醒续传泵。
func (b *Broker) notify() {
	select {
	case b.wakeCh <- struct{}{}:
	default:
	}
}

// fillReplays 推进所有订阅的续传游标；返回本轮是否至少补入了一条消息。
func (b *Broker) fillReplays() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	progress := false
	for s := range b.subs {
		if b.fillOneReplayLocked(s) {
			progress = true
		}
	}
	return progress
}

// fillOneReplayLocked 按全局顺序为单个订阅补消息：非匹配消息跳过；
// 队列满即暂停（续传期间不淘汰、不丢消息），等下一轮空位出现再续。
func (b *Broker) fillOneReplayLocked(s *Subscription) bool {
	if !s.replayPending {
		return false
	}
	progress := false
	idx := s.replayIdx
	for idx < len(b.log) {
		m := b.log[idx]
		if !MatchTopic(s.pattern, m.Topic) {
			idx++
			continue
		}
		if s.isClosed() {
			// 已退订：续传无需再补，游标快速越过。
			idx++
			progress = true
			continue
		}
		enqueued, _ := s.offer(m, true)
		if !enqueued {
			break // 队列仍满，游标停在这条消息上，等空位
		}
		idx++
		progress = true
	}
	s.replayIdx = idx
	s.replayPending = idx < len(b.log)
	return progress
}

// hasPendingReplay 报告是否还有任何存活订阅的历史积压未补完。
// 供需要"等续传补完再对账"的场景确定性等待使用。
func (b *Broker) hasPendingReplay() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for s := range b.subs {
		if s.replayPending && s.isActive() {
			return true
		}
	}
	return false
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
	if b.closed {
		b.mu.Unlock()
		return 0, ErrClosed
	}
	b.seq++
	m.Seq = b.seq
	b.log = append(b.log, m)
	if cut := len(b.log) - b.cfg.Retention; cut > 0 {
		b.log = append([]Message(nil), b.log[cut:]...)
		for s := range b.subs {
			// 只调整仍在续传积压中的游标；已追上实时的订阅游标本就等于日志长度，
			// 裁剪后应继续指向新的日志末尾，绝不能回移到保留区内部
			// （否则会被误判为“仍有积压”，实时投递错误地转入续传而停滞）。
			if s.replayPending && s.replayIdx > 0 {
				s.replayIdx -= cut
				if s.replayIdx < 0 {
					s.replayIdx = 0
				}
			}
		}
	}
	end := len(b.log)
	for s := range b.subs {
		if !MatchTopic(s.pattern, topic) {
			continue
		}
		if !s.isActive() {
			// 已退订但仍保留登记的订阅：只记录“关闭后到达”，不再交付也不参与续传。
			s.offer(m, false)
			continue
		}
		if s.replayPending {
			// 该订阅的历史积压尚未补完：实时消息先不直接入队，由续传泵按顺序补到它，
			// 保证补入顺序与全局发布顺序一致。
			continue
		}
		// 实时投递：DropOldest 挤旧、DropNewest 拒收，均为本次生命周期内的最终去向；
		// 丢失位点可在之后重连续传时从保留日志补齐。游标一律越过本条实时消息。
		s.offer(m, false)
		s.replayIdx = end
	}
	b.mu.Unlock()

	b.notify()
	return m.Seq, nil
}

// Subscribe 按模式订阅，使用默认背压策略 PolicyDropOldest。
// 等价于 SubscribeWith(pattern, resumeFrom, SubscribeOptions{})。
func (b *Broker) Subscribe(pattern string, resumeFrom uint64) (*Subscription, error) {
	return b.SubscribeWith(pattern, resumeFrom, SubscribeOptions{})
}

// SubscribeWith 按模式与背压策略订阅。resumeFrom 为已确认位点（全局序列号），
// 0 表示从头开始；重连时传入上次确认的位点。
//
// 续传语义：建立订阅时从保留日志中按全局顺序、在订阅队列有空位时懒重放
// Seq > resumeFrom 且匹配模式的消息——包括该订阅上一代因背压丢过、但仍在
// 保留窗口内的缺口消息；补完后订阅所见与一直在线且始终跟得上时一模一样，不重不漏。
// 续传期间队列满只会暂停补入，不会再次淘汰。
//
// 以下情况返回可区分错误且不会建立订阅：
//   - ErrEmptyTopic：空模式；
//   - ErrInvalidPolicy：未知背压策略；
//   - ErrClosed：Broker 已关闭；
//   - ErrPositionExpired：resumeFrom 早于保留窗口最早消息，缺口已无法补齐；
//   - ErrAckNotDelivered：resumeFrom 超过 Broker 已发布的最大位点。
func (b *Broker) SubscribeWith(pattern string, resumeFrom uint64, opts SubscribeOptions) (*Subscription, error) {
	if pattern == "" {
		return nil, ErrEmptyTopic
	}
	policy := opts.Policy
	if policy == PolicyDefault {
		policy = PolicyDropOldest
	}
	if policy != PolicyDropOldest && policy != PolicyDropNewest {
		return nil, fmt.Errorf("%w: %d", ErrInvalidPolicy, opts.Policy)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	if resumeFrom > b.seq {
		return nil, fmt.Errorf("%w: resume position %d beyond newest %d",
			ErrAckNotDelivered, resumeFrom, b.seq)
	}
	// resumeFrom==0 是“从头开始”的哨兵，即使窗口起点不是 seq1 也合法
	//（全新订阅只重放窗口内尚存的消息，不构成需要补齐的缺口）。
	if resumeFrom != 0 && len(b.log) > 0 && resumeFrom < b.log[0].Seq-1 {
		return nil, fmt.Errorf("%w: resume position %d, oldest retained %d",
			ErrPositionExpired, resumeFrom, b.log[0].Seq)
	}

	sub := &Subscription{
		pattern:   pattern,
		policy:    policy,
		ch:        make(chan Message, b.cfg.QueueSize),
		lastSeen:  resumeFrom,
		lastAcked: resumeFrom,
		onClose:   b.removeSub,
	}
	// 游标定位到第一条严格新于 resumeFrom 的日志。
	sub.replayIdx = len(b.log)
	for i, m := range b.log {
		if m.Seq > resumeFrom {
			sub.replayIdx = i
			break
		}
	}
	b.subs[sub] = struct{}{}
	sub.replayPending = true
	b.fillOneReplayLocked(sub)
	b.notify()
	return sub, nil
}

// removeSub 是订阅退订时的回调。退订的订阅仍保留在登记表中：
// 之后匹配到它的消息只计入 DroppedLate（isActive 为 false，不再入队），
// 使订阅方退订后仍能查到完整的去向汇总；登记条目在 Broker 关闭时统一清理。
// 重复调用（退订幂等）无副作用。
//
// 已知权衡：退订条目不主动驱逐，以便持续累计 DroppedLate 到 Broker 关闭。
// 这是进程内中间件、订阅规模有限下的有意选择；若未来需要超大规模短连接，
// 可改为按保留窗口给退订条目设上限。
func (b *Broker) removeSub(s *Subscription) {
	// 订阅已在 close() 中标记关闭；此处无需再改表。保留方法以固定 onClose 约定。
}

// Close 关闭 Broker，幂等且与并发 Publish/Subscribe/Unsubscribe 安全：
//   - 之后的 Publish/Subscribe 返回 ErrClosed；
//   - 关闭前已入队到各订阅缓冲的消息保留，订阅方可排空后看到通道关闭；
//   - 关闭后到达订阅的消息按 DropClosed 计入统计（确定语义：不再交付）。
func (b *Broker) Close() error {
	// 先停续传泵，再在锁内关闭订阅通道，避免向已关闭通道投递。
	select {
	case <-b.stopCh:
	default:
		close(b.stopCh)
	}
	b.wg.Wait()

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	for s := range b.subs {
		s.close()
	}
	b.subs = make(map[*Subscription]struct{})
	return nil
}
