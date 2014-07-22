package fqueue

import (
	"errors"
)

var (
	NoSpace         = errors.New("no space error")
	QueueEmpty      = errors.New("queue is empty")
	InvaildMeta     = errors.New("invaild meta")
	MustBeFile      = errors.New("must be file")
	MustBeDirectory = errors.New("must be directory")
	InvaildReadn    = errors.New("invaild readn")
)
