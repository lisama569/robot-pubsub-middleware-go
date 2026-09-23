package middleware

import "sync"

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

	seq  uint64
	log  []Message // 全局保留日志，按 Seq 升序，超出 Retention 时裁掉最旧
	subs map[*Subscription]struct{}
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
	for s := range b.subs {
		if MatchTopic(s.pattern, topic) {
			s.enqueue(m)
		}
	}
	return m.Seq, nil
}

// Subscribe 按模式订阅。resumeFrom 为已确认位点（全局序列号），0 表示从头开始；
// 重连时传入上次确认的位点，Broker 会从保留日志中重放该位点之后、
// 且匹配模式的消息（重放受 Retention 限制，早于保留窗口的位点之前的消息已不可恢复）。
func (b *Broker) Subscribe(pattern string, resumeFrom uint64) (*Subscription, error) {
	if pattern == "" {
		return nil, ErrEmptyTopic
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	sub := &Subscription{
		pattern:   pattern,
		ch:        make(chan Message, b.cfg.QueueSize),
		lastAcked: resumeFrom,
		onClose:   b.removeSub,
	}
	b.subs[sub] = struct{}{}
	for _, m := range b.log {
		if m.Seq > resumeFrom && MatchTopic(pattern, m.Topic) {
			sub.enqueue(m)
		}
	}
	return sub, nil
}

func (b *Broker) removeSub(s *Subscription) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.subs, s)
}

// Close 关闭 Broker，幂等且与并发 Publish/Subscribe/Unsubscribe 安全：
//   - 之后的 Publish/Subscribe 返回 ErrClosed；
//   - 关闭前已入队到各订阅缓冲的消息保留，订阅方可排空后看到通道关闭；
//   - 关闭后到达订阅的消息按 DropClosed 计入统计（确定语义：不再交付）。
func (b *Broker) Close() error {
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
