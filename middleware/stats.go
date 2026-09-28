package middleware

// BackpressurePolicy 决定订阅队列满时如何处理新到达的消息。
// 策略在建立订阅时声明，订阅生命周期内不变。
type BackpressurePolicy int

const (
	// PolicyDefault 是零值，仅用于"未指定"场景，建立订阅时会规范化为 PolicyDropOldest。
	PolicyDefault BackpressurePolicy = iota
	// PolicyDropOldest 留着最新的：队列满时淘汰一条最旧消息为新消息腾位，
	// 新消息仍然入队，发布方不阻塞。
	PolicyDropOldest
	// PolicyDropNewest 留着先来的：队列满时直接拒收新消息，
	// 已在队列中的消息（先到的）全部保留，发布方不阻塞。
	PolicyDropNewest
)

// DropReason 区分一条匹配消息未能正常进入订阅队列的具体原因。
type DropReason int

const (
	DropNone DropReason = iota
	// DropOldest 队列满，按 PolicyDropOldest 挤掉了一条最旧消息；
	// 被挤掉的是队列里的旧消息，新消息本身已成功入队。
	DropOldest
	// DropNewestRejected 队列满，按 PolicyDropNewest 拒收了这条新来的消息。
	DropNewestRejected
	// DropClosed 订阅或 Broker 已关闭，消息到达后不再交付。
	DropClosed
)

func (r DropReason) String() string {
	switch r {
	case DropNone:
		return "none"
	case DropOldest:
		return "drop-oldest"
	case DropNewestRejected:
		return "drop-newest-rejected"
	case DropClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// Stats 记录单个订阅一次生命周期内的消息去向汇总，各项语义互不重叠，
// 可与发布侧逐条对账（见各字段注释与 README 的对账关系）。
type Stats struct {
	// Delivered 成功进入订阅队列的匹配消息数（含之后可能被挤掉的）。
	Delivered uint64
	// DroppedOldest PolicyDropOldest 下、队列满时被挤掉的最旧消息数。
	DroppedOldest uint64
	// DroppedNewest PolicyDropNewest 下、队列满时被拒收的新消息数。
	DroppedNewest uint64
	// DroppedLate 订阅退订或 Broker 关闭之后才到达、因此被丢弃的匹配消息数。
	DroppedLate uint64
	// Duplicates 序列号不大于该订阅已见位点、因重复而被挡下的消息数。
	Duplicates uint64
}
