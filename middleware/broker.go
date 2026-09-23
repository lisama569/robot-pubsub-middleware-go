package middleware

import "sync"

// Config 配置 Broker 行为。
type Config struct {
	// SupportedVersions 是可接受的消息版本集合，为空时默认仅 Version1。
	SupportedVersions []Version
	// QueueSize 是每个订阅的缓冲队列长度，<=0 时使用默认值 64。
	QueueSize int
	// Retention 是每个主题保留用于断线续传的消息条数，<=0 时使用默认值 1024。
	Retention int
}

// Broker 是进程内发布/订阅中间件，支持主题通配、可靠续传与背压控制。
type Broker struct {
	mu     sync.Mutex
	cfg    Config
	closed bool
	// TODO: 主题日志、序列号、订阅表
}

// NewBroker 创建一个 Broker。
func NewBroker(cfg Config) *Broker {
	return &Broker{cfg: cfg} // TODO: 规范化配置并初始化内部状态
}

// Publish 校验并发布一条消息：版本不兼容返回 ErrUnsupportedVersion；
// 已关闭返回 ErrClosed。成功时返回 Broker 分配的序列号。
func (b *Broker) Publish(topic string, version Version, payload []byte) (uint64, error) {
	return 0, nil // TODO: 实现
}

// Subscribe 按模式订阅。resumeFrom 为已确认位点（序列号），0 表示从头开始；
// 重连时传入上次确认的位点即可从该位点之后继续接收。
func (b *Broker) Subscribe(pattern string, resumeFrom uint64) (*Subscription, error) {
	return nil, nil // TODO: 实现
}

// Close 关闭 Broker：停止接受发布，所有订阅队列中已入队消息按确定语义处理，
// 之后订阅通道被关闭。并发调用安全且幂等。
func (b *Broker) Close() error {
	return nil // TODO: 实现
}
