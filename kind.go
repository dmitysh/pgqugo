package pgqugo

import (
	"context"
	"fmt"
	"time"

	"github.com/dmitysh/pgqugo/pkg/delayer"
	"github.com/dmitysh/pgqugo/pkg/log"
	"github.com/dmitysh/pgqugo/pkg/stats"
)

const (
	maxAttemptTimeout = time.Hour * 24 * 1000
)

// TaskHandler receives the ProcessingTask from the queue. Implements the logic of working with the task
type TaskHandler interface {
	HandleTask(ctx context.Context, task ProcessingTask) error
}

// Logger used in queue to log warnings and errors
type Logger interface {
	Warnf(ctx context.Context, format string, args ...any)
	Errorf(ctx context.Context, format string, args ...any)
}

// StatsCollector used in queue to work with TaskStatus change events, to collect statistics information
type StatsCollector interface {
	IncNewTasks()
	AddInProgressTasks(count int)
	IncSuccessTasks()
	IncSoftFailedTasks()
	IncFailedTasks()
}

// AttemptDelayer calculates the delay time between task execution from the number of attempts
type AttemptDelayer func(attempt int16) time.Duration

// TaskKinds used when registering kinds in a queue
type TaskKinds []taskKind

type taskKind struct {
	id      int16
	handler TaskHandler

	fetchPeriod    func() time.Duration
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

// NewTaskKind taskKind constructor. Parameterized via PoolOption:
//
// WithMaxAttempts sets maximum number of attempts used to handle queue's task. Default is 3
//
// WithBatchSize sets number of tasks taken from queue by one fetch. Default is 100
//
// WithFetchPeriod sets duration between requests to the task queue. Default is mean=1.5s, deviation=0.25
//
// WithAttemptDelayer sets delay function between attempts of task. Default is delayer.Linear, mean=10s, deviation=0.2
//
// WithWorkerCount sets number of workers (goroutines from pool) used to handle fetched tasks. Panics if n <= 0
//
// WithAttemptTimeout sets timeout duration added to TaskHandler.HandleTask context. Default is maxAttemptTimeout
//
// WithTerminalTasksTTL sets the minimum time until the tasks in the terminal status are deleted from queue by cleaner job. Default is 24h
//
// WithCleaningPeriod sets period between cleaner job executions. Default is 10m
//
// WithCleaningLimit sets the maximum number of tasks deleted by one cleaner job execution. Default is 10_000
//
// WithLogger sets Logger  Can be used with log package. Default is log.STDLogger
//
// WithStatsCollector sets StatsCollector. Default is stats.LocalCollector
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

		maxAttempts: 3,
		batchSize:   100,
		fetchPeriod: func() time.Duration {
			return calculateDeviationPeriod(time.Millisecond*1500, 0.25)
		},
		attemptDelayer: delayer.Linear(time.Second*10, 0.2),
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

	if kind.fetchPeriod == nil {
		return fmt.Errorf("fetch delayer must be set via WithFetchPeriod")
	}

	return nil
}
