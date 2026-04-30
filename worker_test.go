package main

import "testing"

func TestDeadLetterList(t *testing.T) {
	tests := []struct {
		redisList string
		want      string
	}{
		{"my-list", "my-list:failed"},
		{"orders", "orders:failed"},
		{"", ":failed"},
	}

	for _, tc := range tests {
		got := deadLetterList(tc.redisList)
		if got != tc.want {
			t.Errorf("deadLetterList(%q) = %q, want %q", tc.redisList, got, tc.want)
		}
	}
}
