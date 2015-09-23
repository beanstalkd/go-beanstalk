# Beanstalk

Go client for [beanstalkd](http://kr.github.com/beanstalkd/).

## Install

    $ go get github.com/kr/beanstalk

## Use

Produce jobs:

    c, err := beanstalk.Dial("tcp", "127.0.0.1:11300")
    id, err := c.Put([]byte("hello"), 1, 0, 120*time.Second)

Consume jobs:

    c, err := beanstalk.Dial("tcp", "127.0.0.1:11300")
    id, body, err := c.Reserve(5 * time.Second)

Work with tubes

    c, err := beanstalk.Dial("tcp", "127.0.0.1:11300")
    err = c.UseTube("newTube") // Put jobs to tube "newTube"
    err = c.WatchTube("newTube") // Consume jobs also from "newTube"
    err = c.ApplyTube("newTube") // UseTube plus WatchTube combination
    err = c.UnwatchTube("newTube") // Do not consume jobs from "newTube"






