package middleware

import "errors"

// Version 是消息协议版本。Broker 只接受其支持版本集合内的消息。
type Version uint16

const (
	// Version1 是当前唯一的稳定协议版本。
	Version1 Version = 1
)

// 消息校验与位点相关的可区分错误。
var (
	// ErrUnsupportedVersion 表示消息版本未知或与 Broker 不兼容。
	ErrUnsupportedVersion = errors.New("middleware: unsupported message version")
	// ErrEmptyTopic 表示发布或订阅使用了空主题。
	ErrEmptyTopic = errors.New("middleware: empty topic")
	// ErrPositionRegression 表示确认位点回退（确认的序列号小于已确认位点）。
	ErrPositionRegression = errors.New("middleware: ack position regression")
	// ErrAckNotDelivered 表示确认的位点超过了该订阅实际已见到的最大位点，
	// 即确认到一个从未交到该订阅手上的位置。
	ErrAckNotDelivered = errors.New("middleware: ack position not delivered")
	// ErrAckGap 表示确认区间内存在已判定丢失、尚未补齐的位点
	// （典型：DropOldest 挤掉的消息还没靠重连续传补回），不允许跨过空档确认。
	ErrAckGap = errors.New("middleware: ack position has a gap")
	// ErrPositionExpired 表示位点早于 Broker 保留窗口的最早消息，
	// 中间缺口已无法从保留日志补齐：续传起始位点过期（SubscribeWith），
	// 或确认区间内有已滚出保留窗口、怎么重连都补不回来的丢失位点（Ack）。
	// 与 ErrAckGap（缺口仍在窗口内、重连可补齐）严格区分。
	ErrPositionExpired = errors.New("middleware: resume position older than retention window")
	// ErrInvalidPolicy 表示建立订阅时声明了未知的背压策略。
	ErrInvalidPolicy = errors.New("middleware: invalid backpressure policy")
	// ErrClosed 表示对已关闭的 Broker 或 Subscription 进行操作。
	ErrClosed = errors.New("middleware: closed")
)

// Message 是在 Broker 中流转的消息。Seq 由 Broker 在发布时按主题单调递增分配，
// 订阅侧可据此校验顺序、去重并作为确认位点。
type Message struct {
	Topic   string
	Seq     uint64
	Version Version
	Payload []byte
}

// Validate 校验消息版本是否被支持。supported 为空时默认仅接受 Version1。
// 不支持时返回 ErrUnsupportedVersion（明确拒绝，而非静默丢弃）。
func Validate(m Message, supported []Version) error {
	if len(supported) == 0 {
		supported = []Version{Version1}
	}
	for _, v := range supported {
		if m.Version == v {
			return nil
		}
	}
	return ErrUnsupportedVersion
}
