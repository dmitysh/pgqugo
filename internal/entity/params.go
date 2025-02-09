package entity

import "time"

type GetPendingTasksParams struct {
	KindID       int16
	BatchSize    int
	AttemptDelay time.Duration
}

type DeleteTerminalTasksParams struct {
	KindID int16
	Limit  int
	After  time.Duration
}

type SoftFailTasksParams struct {
	TaskID int64
	Delay  time.Duration
}

type FailTaskParams struct {
	TaskID int64
}

type ExecuteJobParams struct {
	Job       string
	JobPeriod time.Duration
}
