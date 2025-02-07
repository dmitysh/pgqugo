package pgqugo

import (
	"context"
	"fmt"
	"time"

	"github.com/DmitySH/pgqugo/pkg/log"
	"github.com/DmitySH/pgqugo/pkg/stats"
)

const (
	maxAttemptTimeout = time.Hour * 24 * 1000
)

type TaskHandler interface {
	HandleTask(ctx context.Context, task ProcessingTask) error
}

type Logger interface {
	Warnf(ctx context.Context, format string, args ...any)
	Errorf(ctx context.Context, format string, args ...any)
}

type StatsCollector interface {
	IncNewTasks()
	AddInProgressTasks(count int)
	IncSuccessTasks()
	IncSoftFailedTasks()
	IncFailedTasks()
}

type AttemptDelayer func(attempt int16) time.Duration

type TaskKinds []taskKind

type taskKind struct {
	id      int16
	handler TaskHandler

	fetchDelayer   func() time.Duration
	attemptDelayer AttemptDelayer
	maxAttempts    int16
	batchSize      int
	workerCount    int
	attemptTimeout time.Duration

	cleanerCfg     cleanerConfig
	logger         Logger
	statsCollector StatsCollector
}

type cleanerConfig struct {
	terminalTasksTTL time.Duration
	period           time.Duration
	limit            int
}

func NewTaskKind(id int16, handler TaskHandler, opts ...TaskKindOption) taskKind {
	tk := defaultTaskKind(id, handler)

	for _, opt := range opts {
		opt(&tk)
	}

	if err := validateTaskKind(tk); err != nil {
		panic(err)
	}

	return tk
}

func defaultTaskKind(id int16, handler TaskHandler) taskKind {
	return taskKind{
		id:      id,
		handler: handler,

		maxAttempts:    3,
		fetchDelayer:   nil,
		attemptDelayer: nil,
		batchSize:      10,
		workerCount:    1,
		attemptTimeout: maxAttemptTimeout,
		cleanerCfg: cleanerConfig{
			terminalTasksTTL: time.Hour * 24,
			period:           time.Minute * 10,
			limit:            10_000,
		},
		logger:         log.NewSTDLogger(),
		statsCollector: stats.NewLocalCollector(),
	}
}

func validateTaskKind(kind taskKind) error {
	if kind.attemptDelayer == nil {
		return fmt.Errorf("attempt delayer must be set via WithAttemptDelayer")
	}

	if kind.fetchDelayer == nil {
		return fmt.Errorf("fetch delayer must be set via WithFetchPeriod")
	}

	return nil
}
