package pgqugo

import (
	"context"
	"time"
)

const (
	year = time.Hour * 24 * 365
)

type TaskHandler interface {
	HandleTask(ctx context.Context, task ProcessingTask) error
}

type TaskKinds []taskKind

type taskKind struct {
	id      int16
	handler TaskHandler

	fetchPeriod      time.Duration
	attemptsInterval time.Duration
	maxAttempts      int16
	batchSize        int
	workerCount      int
	attemptTimeout   time.Duration
}

func NewTaskKind(id int16, handler TaskHandler, opts ...TaskKindOption) taskKind {
	tk := taskKind{
		id:      id,
		handler: handler,

		maxAttempts:      3,
		fetchPeriod:      time.Millisecond * 300,
		attemptsInterval: time.Second * 10,
		batchSize:        10,
		workerCount:      3,
		attemptTimeout:   year,
	}

	for _, opt := range opts {
		opt(&tk)
	}

	return tk
}

type TaskKindOption func(tk *taskKind)

func WithMaxAttempts(n int16) TaskKindOption {
	if n <= 0 {
		panic("number of attempts must be positive")
	}

	return func(tk *taskKind) {
		tk.maxAttempts = n
	}
}

func WithBatchSize(n int) TaskKindOption {
	if n <= 0 {
		panic("batch size must be positive")
	}

	return func(tk *taskKind) {
		tk.batchSize = n
	}
}

func WithFetchPeriod(period time.Duration) TaskKindOption {
	if period <= 0 {
		panic("fetch period must be positive")
	}

	return func(tk *taskKind) {
		tk.fetchPeriod = period
	}
}

func WithAttemptsInterval(interval time.Duration) TaskKindOption {
	if interval <= 0 {
		panic("attempts interval must be positive")
	}

	return func(tk *taskKind) {
		tk.attemptsInterval = interval
	}
}

func WithWorkerCount(n int) TaskKindOption {
	if n <= 0 {
		panic("number of workers must be positive")
	}

	return func(tk *taskKind) {
		tk.workerCount = n
	}
}

func WithAttemptTimeout(timeout time.Duration) TaskKindOption {
	return func(tk *taskKind) {
		tk.attemptTimeout = timeout
	}
}
