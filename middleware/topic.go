package middleware

// MatchTopic 判断订阅模式 pattern 是否匹配主题 topic。
// 通配规则（MQTT 风格，以 '/' 分段）：
//   - "*" 匹配恰好一段；
//   - "#" 作为最后一段时匹配其自身及任意多级后缀；
//   - 其余段需字面相等。
func MatchTopic(pattern, topic string) bool {
	return false // TODO: 实现
}
