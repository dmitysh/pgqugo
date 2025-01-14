package pgqugo

import (
	"context"
	"time"
)

type TaskHandler interface {
	HandleTask(ctx context.Context, task ProcessingTask) error
}

type TaskKinds []taskKind

type taskKind struct {
	id                   int16
	handler              TaskHandler
	fetchPeriod          time.Duration
	maxAttempts          int16
	delayBetweenAttempts time.Duration
}

func NewTaskKind(id int16, handler TaskHandler, maxAttempts int16, fetchPeriod time.Duration, delayBetweenAttempts time.Duration) taskKind {
	return taskKind{
		id:                   id,
		handler:              handler,
		maxAttempts:          maxAttempts,
		fetchPeriod:          fetchPeriod,
		delayBetweenAttempts: delayBetweenAttempts,
	}
}
