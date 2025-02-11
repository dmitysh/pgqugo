package inerrors

import "errors"

var (
	// ErrJobExecutionCancelled occurs to cancel the job execution at the current moment
	ErrJobExecutionCancelled = errors.New("no need to execute job right now")
)
