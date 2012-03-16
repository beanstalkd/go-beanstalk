package beanstalk

import (
	"net/textproto"
)

type reader struct {
	p  textproto.Pipeline
	id uint
}
