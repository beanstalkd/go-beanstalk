package beanstalk_test

import (
	"fmt"
	"github.com/kr/beanstalk"
	"time"
)

func ExampleTubeSet_Reserve() {
	c, err := beanstalk.Dial("tcp", "127.0.0.1:11300")
	if err != nil {
		panic(err)
	}
	id, body, err := c.Reserve(10 * time.Hour)
	if cerr, ok := err.(beanstalk.ConnError); ok && cerr.Err == beanstalk.ErrTimeout {
		fmt.Println("timed out")
		return
	} else if err != nil {
		panic(err)
	}
	fmt.Println("job", id)
	fmt.Println(string(body))
}
