package entity

import "time"

// GetPendingTasksParams parameters for GetPendingTasks query
type GetPendingTasksParams struct {
	KindID       int16
	BatchSize    int
	AttemptDelay time.Duration
}

// DeleteTerminalTasksParams parameters for DeleteTerminalTasks query
type DeleteTerminalTasksParams struct {
	KindID int16
	Limit  int
	After  time.Duration
}

// SoftFailTasksParams parameters for SoftFailTasks query
type SoftFailTasksParams struct {
	TaskID int64
	Delay  time.Duration
}

// ExecuteJobParams parameters for ExecuteJob query
type ExecuteJobParams struct {
	Job       string
	JobPeriod time.Duration
}
