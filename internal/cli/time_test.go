package cli

import (
	"testing"
	"time"
)

func TestParseTime(t *testing.T) {
	now := time.Date(2026, 10, 12, 15, 30, 0, 0, time.Local)
	at := func(h, m, s, ns int) int64 { return time.Date(2026, 10, 12, h, m, s, ns, time.Local).UnixNano() }
	for _, tc := range []struct {
		in     string
		lo, hi int64
	}{
		{"now", now.UnixNano(), now.UnixNano()},
		{"-90s", at(15, 28, 30, 0), at(15, 28, 30, 0)},
		{"15:04:05", at(15, 4, 5, 0), at(15, 4, 5, 999_999_999)},
		{"15:04", at(15, 4, 0, 0), at(15, 4, 59, 999_999_999)},
		{"15:04:05.123", at(15, 4, 5, 123_000_000), at(15, 4, 5, 123_999_999)},
		{"2026-10-12T15:04:05.123", at(15, 4, 5, 123_000_000), at(15, 4, 5, 123_999_999)},
		{fmtTS(at(15, 4, 5, 123_456_789)), at(15, 4, 5, 123_000_000), at(15, 4, 5, 123_999_999)},
	} {
		lo, hi, err := parseTime(tc.in, now)
		if err != nil {
			t.Errorf("%q: %v", tc.in, err)
			continue
		}
		if lo != tc.lo || hi != tc.hi {
			t.Errorf("%q = [%d, %d], want [%d, %d]", tc.in, lo, hi, tc.lo, tc.hi)
		}
	}
	utc := time.Date(2026, 10, 12, 22, 4, 5, 0, time.UTC).UnixNano()
	if lo, hi, err := parseTime("2026-10-12T22:04:05Z", now); err != nil || lo != utc || hi != utc+999_999_999 {
		t.Errorf("RFC3339 = [%d, %d] %v", lo, hi, err)
	}
	if _, _, err := parseTime("yesterday", now); err == nil {
		t.Error("expected error")
	}
}
