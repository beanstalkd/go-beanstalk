package beanstalk

import (
	"testing"
	"time"
)

func TestFormatDuration(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{100e9, "100"},
		{0, "0"},
		{-1, "0"},
		{-1 * time.Second, "0"},
		{-10 * time.Second, "0"},
	}
	for _, c := range cases {
		if got := dur2sec(c.d); got != c.want {
			t.Fatalf("got %s, expected: %s", got, c.want)
		}
	}
}
