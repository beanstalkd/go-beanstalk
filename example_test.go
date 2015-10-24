package beanstalk_test

import (
	"fmt"
	"github.com/kr/beanstalk"
	"time"
	"net"
	"testing"
)

var conn, _ = beanstalk.Dial("tcp", "127.0.0.1:11300")

func Example_reserve() {
	id, body, err := conn.Reserve(5 * time.Second)
	if err != nil {
		panic(err)
	}
	fmt.Println("job", id)
	fmt.Println(string(body))
}

func Example_reserveOtherTubeSet() {
	tubeSet := beanstalk.NewTubeSet(conn, "mytube1", "mytube2")
	id, body, err := tubeSet.Reserve(10 * time.Hour)
	if err != nil {
		panic(err)
	}
	fmt.Println("job", id)
	fmt.Println(string(body))
}

func Example_put() {
	id, err := conn.Put([]byte("myjob"), 1, 0, time.Minute)
	if err != nil {
		panic(err)
	}
	fmt.Println("job", id)
}

func Example_putOtherTube() {
	tube := &beanstalk.Tube{conn, "mytube"}
	id, err := tube.Put([]byte("myjob"), 1, 0, time.Minute)
	if err != nil {
		panic(err)
	}
	fmt.Println("job", id)
}

func Example_reuse_socket_connection() {

	master, err := beanstalk.Dial("tcp", "127.0.0.1:11300")
	if err != nil {
		panic(err)
	}
	master.Tube.Name = "mytube1"
	_, err = master.Put([]byte("data1"), 1, 0, 5 * time.Minute)
	if err != nil {
		panic(err)
	}
	_, err = master.Put([]byte("data2"), 1, 0, 5 * time.Minute)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Put two data into mytube1\n")

	reusedConn, _ := net.Dial("tcp", "127.0.0.1:11300")

	worker1Bq := beanstalk.NewConn(reusedConn)
	worker1Bq.Tube.Name = "mytube1"
	worker1Bq.TubeSet = *beanstalk.NewTubeSet(worker1Bq, "mytube1")
	id, data, err := worker1Bq.Reserve(5 * time.Second)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Consume mytube1 will get 'data1', real got is %s\n", string(data))
	err = worker1Bq.Delete(id)
	if err != nil {
		panic(err)
	}

	worker2Bq := beanstalk.NewConn(reusedConn)
	worker2Bq.Tube.Name = "mytube2"
	worker2Bq.TubeSet = *beanstalk.NewTubeSet(worker2Bq, "mytube2")
	id, data, err = worker2Bq.Reserve(5 * time.Second)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Consume mytube2 should take nothing(mytube2 is empty), but we got %s", string(data))
	err = worker1Bq.Delete(id)
	if err != nil {
		panic(err)
	}

}

func TestIt(t *testing.T) {
	Example_reuse_socket_connection()
}
