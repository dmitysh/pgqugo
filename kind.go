package pgqugo

import (
	"context"
	"fmt"
	"math/rand/v2"
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

type Logger interface {
	Warnf(ctx context.Context, format string, args ...any)
	Errorf(ctx context.Context, format string, args ...any)
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

func WithFetchPeriod(mean time.Duration, deviation float64) TaskKindOption {
	if mean <= 0 {
		panic("mean must be positive")
	}

	if deviation < 0 || deviation > 1 {
		panic("deviation must be in [0;1]")
	}

	return func(tk *taskKind) {
		tk.fetchDelayer = func() time.Duration {
			return calculateDeviationPeriod(mean, deviation)
		}
	}
}

func calculateDeviationPeriod(mean time.Duration, deviation float64) time.Duration {
	return time.Duration(float64(mean) + deviation*(rand.Float64()*float64(2)-float64(1))*float64(mean))
}

func WithAttemptDelayer(attemptDelayer AttemptDelayer) TaskKindOption {
	return func(tk *taskKind) {
		tk.attemptDelayer = attemptDelayer
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
	if timeout <= 0 {
		panic("timeout must be positive")
	}

	if timeout > maxAttemptTimeout {
		panic(fmt.Sprintf("timeout must be less than %f hours", maxAttemptTimeout.Hours()))
	}

	return func(tk *taskKind) {
		tk.attemptTimeout = timeout
	}
}

func WithTerminalTasksTTL(ttl time.Duration) TaskKindOption {
	if ttl <= 0 {
		panic("terminal task ttl must be positive")
	}

	return func(tk *taskKind) {
		tk.cleanerCfg.terminalTasksTTL = ttl
	}
}

func WithCleaningPeriod(period time.Duration) TaskKindOption {
	if period <= 0 {
		panic("cleaner period must be positive")
	}

	return func(tk *taskKind) {
		tk.cleanerCfg.period = period
	}
}

func WithCleaningLimit(limit int) TaskKindOption {
	if limit <= 0 {
		panic("cleaner limit must be positive")
	}

	return func(tk *taskKind) {
		tk.cleanerCfg.limit = limit
	}
}

func WithLogger(logger Logger) TaskKindOption {
	if logger == nil {
		panic("logger must not be nil")
	}

	return func(tk *taskKind) {
		tk.logger = logger
	}
}

func WithStatsCollector(collector StatsCollector) TaskKindOption {
	if collector == nil {
		panic("collector must not be nil")
	}

	return func(tk *taskKind) {
		tk.statsCollector = collector
	}
}
