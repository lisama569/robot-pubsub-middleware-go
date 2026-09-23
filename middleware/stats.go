package middleware

// DropReason 区分消息未能进入订阅队列的原因。
type DropReason int

const (
	DropNone DropReason = iota
	// DropQueueFull 队列超限，按策略丢弃最旧消息。
	DropQueueFull
	// DropClosed 订阅或 Broker 关闭后到达的消息被丢弃。
	DropClosed
)

func (r DropReason) String() string {
	switch r {
	case DropNone:
		return "none"
	case DropQueueFull:
		return "queue-full"
	case DropClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// Stats 记录单个订阅的可区分投递统计。
type Stats struct {
	Delivered   uint64 // 成功进入订阅队列的消息数
	DroppedFull uint64 // 因队列超限被丢弃（淘汰最旧）的消息数
	DroppedLate uint64 // 关闭后到达被丢弃的消息数
	Duplicates  uint64 // 因序列号重复被拒绝交付的消息数
}
