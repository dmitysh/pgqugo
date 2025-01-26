package pgqugo

import "time"

type Task struct {
	Kind    int16
	Key     *string
	Payload string
}

type ProcessingTask struct {
	Task
	AttemptsLeft    int16
	AttemptsElapsed int16
}

type FullTaskInfo struct {
	ID              int64
	Kind            int16
	Key             *string
	Payload         string
	Status          string
	AttemptsLeft    int16
	AttemptsElapsed int16
	NextAttemptTime *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type GetWaitingTasksParams struct {
	KindID           int16
	BatchSize        int
	AttemptsInterval time.Duration
}

type DeleteTerminalTasksParams struct {
	KindID int16
	Limit  int
	After  time.Duration
}
