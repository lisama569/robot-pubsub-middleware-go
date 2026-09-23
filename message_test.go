package pubsub

import "testing"

func TestVersionCompatibility(t *testing.T) {
	cases := []struct {
		v      Version
		reason RejectReason
	}{
		{Version{1, 0}, ReasonNone},
		{Version{1, 5}, ReasonNone}, // 同主版本高 Minor：可兼容
		{Version{0, 9}, ReasonIncompatibleVersion},
		{Version{2, 0}, ReasonUnknownVersion}, // 更新的主版本：未知
		{Version{}, ReasonUnknownVersion},     // 零值：未知
	}
	topic := mustTopic(t, "robot.arm.temp")
	for _, c := range cases {
		m := Message{Topic: topic, Seq: 1, Version: c.v, Producer: "p"}
		err := m.Validate()
		if c.reason == ReasonNone {
			if err != nil {
				t.Fatalf("version=%s expected accepted, got %v", c.v, err)
			}
			t.Logf("[version] ver=%s => accepted (same major=%d is compatible)", c.v, CurrentVersion.Major)
			continue
		}
		re, ok := AsRejectError(err)
		if !ok || re.Reason != c.reason {
			t.Fatalf("version=%s expected reason=%s, got %v", c.v, c.reason, err)
		}
		t.Logf("[version] ver=%s => REJECTED reason=%s detail=%q", c.v, re.Reason, re.Detail)
	}
}

func TestValidateRejectsZeroSequence(t *testing.T) {
	m := Message{Topic: mustTopic(t, "a.b"), Seq: 0, Version: CurrentVersion, Producer: "p"}
	err := m.Validate()
	re, ok := AsRejectError(err)
	if !ok || re.Reason != ReasonInvalidSequence {
		t.Fatalf("expected invalid_sequence, got %v", err)
	}
	t.Logf("[sequence] seq=0 => REJECTED reason=%s", re.Reason)
}
