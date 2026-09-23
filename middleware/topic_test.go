package middleware

import "testing"

func TestMatchTopic(t *testing.T) {
	cases := []struct {
		pattern, topic string
		want           bool
	}{
		{"a/b/c", "a/b/c", true},
		{"a/b/c", "a/b/d", false},
		{"a/*/c", "a/b/c", true},
		{"a/*/c", "a/b/d", false},
		{"a/*", "a/b", true},
		{"a/*", "a/b/c", false}, // "*" 只匹配一段
		{"a/#", "a", true},      // "#" 可匹配零段
		{"a/#", "a/b", true},
		{"a/#", "a/b/c", true},
		{"#", "a/b/c", true},
		{"a/b/#/c", "a/b/c", false}, // "#" 仅在末尾有效，此处按字面段处理
		{"a/b", "a/b/c", false},
		{"", "a", false},
		{"a", "", false},
	}
	for _, c := range cases {
		got := MatchTopic(c.pattern, c.topic)
		t.Logf("match pattern=%q topic=%q => %v (want %v)", c.pattern, c.topic, got, c.want)
		if got != c.want {
			t.Errorf("MatchTopic(%q, %q) = %v, want %v", c.pattern, c.topic, got, c.want)
		}
	}
}
