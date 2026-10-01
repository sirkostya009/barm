package barm

import "testing"

func TestNeedsParens(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		sql  string
		want bool
	}{
		{"a = 1 OR b = 2", true},
		{"a = 1 or b = 2", true},
		{"a BETWEEN 1 aNd 2", true},
		{"a  OR  b", true},
		{"a = 1\nOR b = 2", true},
		{"a = 1 OR\n\tb = 2", true},
		{"a = 1\tAND b = 2", true},
		{"(a = 1)OR(b = 2)", true},
		{"x = 'a'OR y = 'b'", true},
		{"a = 1\r\nor b = 2", true},
		{"a = 1", false},
		{"orders > 1", false},
		{"color = 1 AND", false},
		{"x = 1 ORDER BY y", false},
		{"brand = 1", false},
		{"anderson = 1", false},
		{"x = sand", false},
		{"x\nORDER BY y", false},
		{"", false},
	} {
		if got := needsParens(tc.sql); got != tc.want {
			t.Errorf("needsParens(%q) = %v, want %v", tc.sql, got, tc.want)
		}
	}
}
