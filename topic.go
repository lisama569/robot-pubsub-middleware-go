package pubsub

import "strings"

// 主题层级分隔符：
//
//	'.'   层级分隔，例如 robot.arm.joint1.temp
//	'*'   通配符，匹配恰好一个层级，只能出现在 Pattern 中
//	'#'   通配符，匹配零到多个层级，只能位于 Pattern 的末尾
//
// 空层级（出现连续点、前导/末尾点）一律视为非法主题。
const (
	segmentSeparator = "."
	singleWildcard   = "*"
	multiWildcard    = "#"
)

// Topic 是点分层级的具体主题（不含通配符）。
type Topic struct {
	raw      string
	segments []string
}

// ParseTopic 解析具体主题。主题不能为空、不能含空层级、不能含通配符。
func ParseTopic(raw string) (Topic, error) {
	if raw == "" {
		return Topic{}, rejectf(ReasonInvalidTopic, "topic must not be empty")
	}
	segs := strings.Split(raw, segmentSeparator)
	for _, s := range segs {
		if s == "" {
			return Topic{}, rejectf(ReasonInvalidTopic, "topic %q contains an empty segment", raw)
		}
		if s == singleWildcard || s == multiWildcard {
			return Topic{}, rejectf(ReasonInvalidTopic, "publish topic %q must not contain wildcard %q", raw, s)
		}
	}
	return Topic{raw: raw, segments: segs}, nil
}

// String 返回主题原文。
func (t Topic) String() string { return t.raw }

// MustParseTopic 解析主题，失败时 panic；适用于测试与编译期常量模式。
func MustParseTopic(raw string) Topic {
	t, err := ParseTopic(raw)
	if err != nil {
		panic(err)
	}
	return t
}

// Segments 返回层级切片的副本。
func (t Topic) Segments() []string {
	out := make([]string, len(t.segments))
	copy(out, t.segments)
	return out
}

// Pattern 是订阅时使用的通配模式。
type Pattern struct {
	raw      string
	segments []string
}

// ParsePattern 解析通配订阅模式。
// 规则：非空、无空层级；'#' 只允许作为最后一个层级。
func ParsePattern(raw string) (Pattern, error) {
	if raw == "" {
		return Pattern{}, rejectf(ReasonInvalidTopic, "pattern must not be empty")
	}
	segs := strings.Split(raw, segmentSeparator)
	for i, s := range segs {
		if s == "" {
			return Pattern{}, rejectf(ReasonInvalidTopic, "pattern %q contains an empty segment", raw)
		}
		if s == multiWildcard && i != len(segs)-1 {
			return Pattern{}, rejectf(ReasonInvalidTopic, "pattern %q: '#' is only allowed as the last segment", raw)
		}
	}
	return Pattern{raw: raw, segments: segs}, nil
}

// String 返回模式原文。
func (p Pattern) String() string { return p.raw }

// MustParsePattern 解析模式，失败时 panic；适用于测试与编译期常量模式。
func MustParsePattern(raw string) Pattern {
	p, err := ParsePattern(raw)
	if err != nil {
		panic(err)
	}
	return p
}

// Match 判断具体主题是否命中该模式。
//
// 判定依据：
//   - 逐段比较；模式段为 '*' 时匹配任意一个主题段；
//   - 模式段为 '#'（必为末段）时匹配零到多个剩余主题段；
//   - 其余段要求逐字相等；
//   - 模式段比主题段多（且没有 '#' 收尾）则不匹配。
func (p Pattern) Match(t Topic) bool {
	pi := 0
	for ti := 0; ti < len(t.segments); ti++ {
		if pi >= len(p.segments) {
			return false
		}
		ps := p.segments[pi]
		switch ps {
		case multiWildcard:
			// '#' 吞掉所有剩余层级。
			return true
		case singleWildcard:
			pi++ // 恰好消费一个主题段
			continue
		default:
			if ps != t.segments[ti] {
				return false
			}
			pi++
		}
	}
	// 主题段已耗尽：模式剩余部分只能是单独一个 '#'（匹配零层）。
	if pi == len(p.segments) {
		return true
	}
	return pi == len(p.segments)-1 && p.segments[pi] == multiWildcard
}
