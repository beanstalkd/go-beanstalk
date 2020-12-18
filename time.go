package beanstalk

import (
	"strconv"
	"time"
)

func dur2sec(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	return strconv.FormatInt(int64(d/time.Second), 10)
}
