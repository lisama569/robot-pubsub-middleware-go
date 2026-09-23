package pubsub

import "fmt"

// RejectReason 是消息被拒绝或无法投递的可区分原因。
// 所有拒绝路径都必须使用这些常量之一，禁止静默丢弃。
type RejectReason string

const (
	ReasonNone                RejectReason = ""
	ReasonInvalidTopic        RejectReason = "invalid_topic"
	ReasonUnknownVersion      RejectReason = "unknown_version"
	ReasonIncompatibleVersion RejectReason = "incompatible_version"
	ReasonInvalidSequence     RejectReason = "invalid_sequence"
	ReasonQueueFullDropNew    RejectReason = "queue_full_drop_new"
	ReasonQueueFullDropOld    RejectReason = "queue_full_drop_old"
	ReasonPositionRollback    RejectReason = "position_rollback"
	ReasonPositionAhead       RejectReason = "position_ahead"
	ReasonPositionExpired     RejectReason = "position_expired"
	ReasonPositionGap         RejectReason = "position_gap"
	ReasonBrokerClosed        RejectReason = "broker_closed"
	ReasonSubscriptionClosed  RejectReason = "subscription_closed"
	ReasonDuplicateSubscriber RejectReason = "duplicate_subscriber"
)

// RejectError 携带可机读的拒绝原因与可读说明。
type RejectError struct {
	Reason RejectReason
	Detail string
}

func (e *RejectError) Error() string {
	if e.Detail == "" {
		return string(e.Reason)
	}
	return fmt.Sprintf("%s: %s", e.Reason, e.Detail)
}

func rejectf(reason RejectReason, format string, args ...any) error {
	return &RejectError{Reason: reason, Detail: fmt.Sprintf(format, args...)}
}

// AsRejectError 从任意 error 中提取 *RejectError（包装时也可识别）。
func AsRejectError(err error) (*RejectError, bool) {
	for err != nil {
		if re, ok := err.(*RejectError); ok {
			return re, true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return nil, false
		}
		err = u.Unwrap()
	}
	return nil, false
}
