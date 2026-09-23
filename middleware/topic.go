package middleware

import "strings"

// MatchTopic 判断订阅模式 pattern 是否匹配主题 topic。
// 通配规则（MQTT 风格，以 '/' 分段）：
//   - "*" 匹配恰好一段；
//   - "#" 仅作为最后一段时有效，匹配其自身及任意多级后缀（"a/#" 匹配 "a"、"a/b"、"a/b/c"）；
//   - 其余段需字面相等。
//
// 空 pattern 或空 topic 均不匹配（空主题由 Broker 在入口处拒绝）。
func MatchTopic(pattern, topic string) bool {
	if pattern == "" || topic == "" {
		return false
	}
	pp := strings.Split(pattern, "/")
	tp := strings.Split(topic, "/")
	for i, seg := range pp {
		if seg == "#" && i == len(pp)-1 {
			// "#" 匹配零段或多段：要求 topic 至少已对齐到 "#" 的前一段。
			return len(tp) >= i
		}
		if i >= len(tp) {
			return false
		}
		if seg != "*" && seg != tp[i] {
			return false
		}
	}
	return len(pp) == len(tp)
}
