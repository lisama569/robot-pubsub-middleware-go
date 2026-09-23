package pubsub

import "testing"

func TestParseTopicRejectsInvalid(t *testing.T) {
	bad := []string{"", "robot..arm", ".robot", "robot.", "robot.*", "robot.#"}
	for _, raw := range bad {
		if _, err := ParseTopic(raw); err == nil {
			t.Fatalf("ParseTopic(%q) expected error, got nil", raw)
		} else if re, ok := AsRejectError(err); !ok || re.Reason != ReasonInvalidTopic {
			t.Fatalf("ParseTopic(%q) expected invalid_topic, got %v", raw, err)
		}
	}
}

func TestParsePatternRejectsMisplacedHash(t *testing.T) {
	if _, err := ParsePattern("a.#.b"); err == nil {
		t.Fatal("expected error for '#' not at tail")
	}
	if _, err := ParsePattern(""); err == nil {
		t.Fatal("expected error for empty pattern")
	}
	if _, err := ParsePattern("a..b"); err == nil {
		t.Fatal("expected error for empty segment")
	}
}

func TestPatternMatch(t *testing.T) {
	cases := []struct {
		pattern string
		topic   string
		want    bool
	}{
		{"robot.arm.temp", "robot.arm.temp", true},
		{"robot.arm.temp", "robot.arm.vib", false},
		{"robot.*.temp", "robot.arm.temp", true},
		{"robot.*.temp", "robot.leg.temp", true},
		{"robot.*.temp", "robot.arm.vib", false},
		{"robot.*.temp", "robot.arm.joint.temp", false}, // '*' 只匹配一层
		{"robot.#", "robot.arm.temp", true},
		{"robot.#", "robot", true}, // '#' 匹配零层
		{"robot.#", "robot.arm", true},
		{"#", "anything.at.all", true},
		{"#", "x", true},
		{"*.temp", "arm.temp", true},
		{"*.temp", "a.b.temp", false},
		{"robot.arm.#", "robot.arm", true},
		{"robot.arm.#", "robot.arm.j1.temp", true},
		{"robot.arm.#", "robot.leg.j1", false},
		{"a.b.c", "a.b", false},
		{"a.b", "a.b.c", false},
	}
	for _, c := range cases {
		p := mustPattern(t, c.pattern)
		topic := mustTopic(t, c.topic)
		if got := p.Match(topic); got != c.want {
			t.Errorf("Match(pattern=%q, topic=%q) = %v, want %v", c.pattern, c.topic, got, c.want)
		}
	}
}

func mustTopic(t *testing.T, raw string) Topic {
	t.Helper()
	topic, err := ParseTopic(raw)
	if err != nil {
		t.Fatalf("ParseTopic(%q): %v", raw, err)
	}
	return topic
}

func mustPattern(t *testing.T, raw string) Pattern {
	t.Helper()
	p, err := ParsePattern(raw)
	if err != nil {
		t.Fatalf("ParsePattern(%q): %v", raw, err)
	}
	return p
}
