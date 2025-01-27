package inerrors

import "errors"

var (
	ErrJobExecutionCancelled = errors.New("no need to execute job right now")
)
