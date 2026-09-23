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

// Validate 校验消息版本是否被支持。不支持时返回 ErrUnsupportedVersion。
func Validate(m Message, supported []Version) error {
	return nil // TODO: 实现
}
