// Driver for the beanstalk protocol. See http://kr.github.com/beanstalkd/
//
// Example use:
//   c, err := beanstalk.Dial("tcp", "127.0.0.1:11300")
//   c.Put([]byte("hello"), 1, 0, 120*time.Second)
//   id, body, err := c.Reserve(5 * time.Second)
package beanstalk
