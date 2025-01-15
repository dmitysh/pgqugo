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
	id               int16
	handler          TaskHandler
	fetchPeriod      time.Duration
	attemptsInterval time.Duration
	maxAttempts      int16
	batchSize        int32
}

// TODO: options for default values

func NewTaskKind(id int16, handler TaskHandler, maxAttempts int16, batchSize int32, fetchPeriod time.Duration, attemptsInterval time.Duration) taskKind {
	return taskKind{
		id:               id,
		handler:          handler,
		maxAttempts:      maxAttempts,
		fetchPeriod:      fetchPeriod,
		attemptsInterval: attemptsInterval,
		batchSize:        batchSize,
	}
}
