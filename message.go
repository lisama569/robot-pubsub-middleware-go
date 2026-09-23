package pubsub

import "fmt"

// Version 表示消息载荷的线路格式版本。
// 兼容规则：主版本（Major）相同即兼容（v1 系列向后兼容，高 Minor 消息可被低 Minor 接收端读取）。
type Version struct {
	Major int
	Minor int
}

// CurrentVersion 是本进程生产与接收消息时使用的当前版本。
var CurrentVersion = Version{Major: 1, Minor: 0}

// MinSupportedVersion 是本进程能够理解的最低版本；更低主版本一律拒绝。
var MinSupportedVersion = Version{Major: 1, Minor: 0}

func (v Version) String() string { return fmt.Sprintf("v%d.%d", v.Major, v.Minor) }

// IsZero 判断版本是否从未设置（零值），零值属于未知版本。
func (v Version) IsZero() bool { return v.Major == 0 && v.Minor == 0 }

// CompatibleTo 判断接收端版本能否理解该消息。
//   - 主版本相同：兼容（Minor 差异向后/向前兼容，未知字段忽略）；
//   - 主版本不同：不兼容。
func (v Version) CompatibleTo(receiver Version) bool {
	return v.Major == receiver.Major
}

// Message 是发布到某个主题的单条消息。
type Message struct {
	Topic    Topic
	Seq      uint64 // 同一 Producer 内从 1 开始严格单调递增
	Version  Version
	Producer string
	Payload  []byte
}

// String 便于日志打印关键字段（序列号、版本、主题）。
func (m Message) String() string {
	return fmt.Sprintf("msg{producer=%s topic=%s seq=%d ver=%s bytes=%d}",
		m.Producer, m.Topic, m.Seq, m.Version, len(m.Payload))
}

// Validate 校验消息自身是否合法且兼容当前进程：
//   - 主题必须已成功解析（非空层级）；
//   - 序列号必须 >= 1（0 不允许，避免与“位点起点”混淆）；
//   - 版本为零值或高于当前主版本：ReasonUnknownVersion；
//   - 版本低于最低支持主版本：ReasonIncompatibleVersion。
//
// 生产者级别的序列号单调性（重复/回退/跳号）由 Broker 在发布时单独校验。
func (m *Message) Validate() error {
	if len(m.Topic.segments) == 0 {
		return rejectf(ReasonInvalidTopic, "message has no parsed topic")
	}
	if m.Seq == 0 {
		return rejectf(ReasonInvalidSequence, "%s sequence must start from 1, got 0", m.Topic)
	}
	if m.Version.IsZero() {
		return rejectf(ReasonUnknownVersion, "%s seq=%d has zero/unset version", m.Topic, m.Seq)
	}
	switch {
	case m.Version.Major > CurrentVersion.Major:
		return rejectf(ReasonUnknownVersion,
			"%s seq=%d version=%s is newer than current=%s",
			m.Topic, m.Seq, m.Version, CurrentVersion)
	case m.Version.Major < MinSupportedVersion.Major:
		return rejectf(ReasonIncompatibleVersion,
			"%s seq=%d version=%s is below minimum=%s",
			m.Topic, m.Seq, m.Version, MinSupportedVersion)
	}
	if !m.Version.CompatibleTo(CurrentVersion) {
		return rejectf(ReasonIncompatibleVersion,
			"%s seq=%d version=%s is incompatible with current=%s",
			m.Topic, m.Seq, m.Version, CurrentVersion)
	}
	return nil
}

// Delivery 是投递到订阅者的一条记录。
//   - Position 是 Broker 分配的全局单调位点（从 1 开始），与生产者序列号 Seq 相互独立；
//     订阅者按 Position 进行 ACK 与断线续传。
type Delivery struct {
	Position uint64
	Message  Message
}

func (d Delivery) String() string {
	return fmt.Sprintf("delivery{pos=%d %s}", d.Position, d.Message)
}
