package beanstalk_test

import (
	"fmt"
	"github.com/kr/beanstalk"
	"time"
)

func Example() {
	c, err := beanstalk.Dial("tcp", "127.0.0.1:11300")
	if err != nil {
		panic(err)
	}
	c.Put([]byte("hello"), 1, 0, 120*time.Second)
	id, body, err := c.Reserve(5 * time.Second)
	if err != nil {
		panic(err)
	}
	fmt.Println("job", id)
	fmt.Println(string(body))
}

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
