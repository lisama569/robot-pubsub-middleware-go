package middleware

// OverflowPolicy 是订阅队列满时的背压策略，在 Subscribe 时按订阅声明。
type OverflowPolicy int

const (
	// PolicyDropOldest 留最新、挤最旧：队列满时淘汰尚未消费的最旧消息，
	// 新消息照常入队。零值策略，兼容未显式声明时的历史行为。
	PolicyDropOldest OverflowPolicy = iota
	// PolicyDropNewest 留先来的、新的先不收：队列满时直接拒收新消息，
	// 已在队列中的消息不受影响。
	PolicyDropNewest
)

func (p OverflowPolicy) String() string {
	switch p {
	case PolicyDropOldest:
		return "drop-oldest"
	case PolicyDropNewest:
		return "drop-newest"
	default:
		return "unknown"
	}
}

// DropReason 区分消息未能成功进入订阅队列（或被重复挡下）的原因。
type DropReason int

const (
	// DropNone 表示消息成功入队，或为重复消息被去重挡下（未真正进入队列）。
	DropNone DropReason = iota
	// DropOldest 队列满（drop-oldest 策略）：最旧消息被淘汰，新消息入队。
	DropOldest
	// DropNewest 队列满（drop-newest 策略）：新消息被拒收。
	DropNewest
	// DropClosed 订阅或 Broker 关闭后到达的消息被丢弃。
	DropClosed
)

func (r DropReason) String() string {
	switch r {
	case DropNone:
		return "none"
	case DropOldest:
		return "dropped-oldest"
	case DropNewest:
		return "dropped-newest"
	case DropClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// Stats 记录单个订阅一次生命周期内的消息去向汇总，各项语义清晰且可对账：
//
//	Matched = Delivered + DroppedNewest + DroppedLate + Duplicates
//
// DroppedOldest 是 Delivered 的子项（被挤掉的消息“先成功入队、后在消费前被淘汰”，
// 已计入 Delivered），因此不参与右侧求和；Delivered 自身再细分：
//
//	Delivered = 实际读到 + DroppedOldest + 关闭后未排空的残留
//
// 对账时可先校验第一条恒等式，再用第二条核对淘汰数量。
type Stats struct {
	// Matched 命中本订阅模式、进入投递判定的消息总数（含重复与各类丢弃）。
	Matched uint64
	// Delivered 成功进入订阅队列的消息数（含之后被 drop-oldest 淘汰、
	// 以及关闭后来不及排空的残留）。
	Delivered uint64
	// DroppedOldest drop-oldest 策略下，入队后在被消费前挤出的最旧消息数。
	// 语义上是 Delivered 的子项，不额外占 Matched 份额。
	DroppedOldest uint64
	// DroppedNewest drop-newest 策略下，队列满时被直接拒收、从未入队的新消息数。
	DroppedNewest uint64
	// DroppedLate 关闭（退订/Broker 关闭）后到达、被丢弃的消息数（从未入队）。
	DroppedLate uint64
	// Duplicates 因序列号不大于已入队位点而被去重挡下的消息数。
	Duplicates uint64
}

// Reconciled 校验统计恒等式是否成立：
//
//	Matched == Delivered + DroppedNewest + DroppedLate + Duplicates
//
// DroppedOldest 是 Delivered 的子项，不单独占份额（见 Stats 注释）。
func (s Stats) Reconciled() bool {
	return s.Matched == s.Delivered+s.DroppedNewest+s.DroppedLate+s.Duplicates
}
