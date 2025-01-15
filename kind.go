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

type TaskKindOption func(tk *taskKind)

func WithMaxAttempts(n int16) TaskKindOption {
	return func(tk *taskKind) {
		tk.maxAttempts = n
	}
}

func WithBatchSize(n int32) TaskKindOption {
	return func(tk *taskKind) {
		tk.batchSize = n
	}
}

func WithFetchPeriod(period time.Duration) TaskKindOption {
	return func(tk *taskKind) {
		tk.fetchPeriod = period
	}
}

func WitAttemptsInterval(interval time.Duration) TaskKindOption {
	return func(tk *taskKind) {
		tk.attemptsInterval = interval
	}
}

func NewTaskKind(id int16, handler TaskHandler, opts ...TaskKindOption) taskKind {
	tk := taskKind{
		id:               id,
		handler:          handler,
		maxAttempts:      3,
		fetchPeriod:      time.Millisecond * 300,
		attemptsInterval: time.Second * 10,
		batchSize:        10,
	}

	for _, opt := range opts {
		opt(&tk)
	}

	return tk
}
