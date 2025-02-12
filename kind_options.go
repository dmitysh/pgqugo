package pgqugo

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// TaskKindOption option for New
type TaskKindOption func(tk *taskKind)

// WithMaxAttempts sets maximum number of attempts used to handle queue's task. Panics if n <= 0
func WithMaxAttempts(n int16) TaskKindOption {
	if n <= 0 {
		panic("number of attempts must be positive")
	}

	return func(tk *taskKind) {
		tk.maxAttempts = n
	}
}

// WithBatchSize sets number of tasks taken from queue by one fetch. Panics if n <= 0
func WithBatchSize(n int) TaskKindOption {
	if n <= 0 {
		panic("batch size must be positive")
	}

	return func(tk *taskKind) {
		tk.batchSize = n
	}
}

// WithFetchPeriod sets duration between requests to the task queue. Small values load the database more
//
// Calculates as: mean + mean * random(-deviation;deviation)
//
// Panics if mean <= 0 or deviation not in [0;1]
func WithFetchPeriod(mean time.Duration, deviation float64) TaskKindOption {
	if mean <= 0 {
		panic("mean must be positive")
	}

	if deviation < 0 || deviation > 1 {
		panic("deviation must be in [0;1]")
	}

	return func(tk *taskKind) {
		tk.fetchPeriod = func() time.Duration {
			return calculateDeviationPeriod(mean, deviation)
		}
	}
}

func calculateDeviationPeriod(mean time.Duration, deviation float64) time.Duration {
	return time.Duration(float64(mean) + deviation*(rand.Float64()*float64(2)-float64(1))*float64(mean))
}

// WithAttemptDelayer sets delay function between attempts of task. Can be used with delayer package. Panics if delayer is nil
func WithAttemptDelayer(attemptDelayer AttemptDelayer) TaskKindOption {
	if attemptDelayer == nil {
		panic("delayer must not be nil")
	}

	return func(tk *taskKind) {
		tk.attemptDelayer = attemptDelayer
	}
}

// WithWorkerCount sets number of workers (goroutines from pool) used to handle fetched tasks. Panics if n <= 0
func WithWorkerCount(n int) TaskKindOption {
	if n <= 0 {
		panic("number of workers must be positive")
	}

	return func(tk *taskKind) {
		tk.workerCount = n
	}
}

// WithAttemptTimeout sets timeout duration added to TaskHandler.HandleTask context. Panics if timeout <= 0 or timeout > maxAttemptTimeout
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

// WithTerminalTasksTTL sets the minimum time until the tasks in the terminal status are deleted from queue by cleaner job. Panics if ttl <= 0
func WithTerminalTasksTTL(ttl time.Duration) TaskKindOption {
	if ttl <= 0 {
		panic("terminal task ttl must be positive")
	}

	return func(tk *taskKind) {
		tk.cleanerCfg.terminalTasksTTL = ttl
	}
}

// WithCleaningPeriod sets period between cleaner job executions. Panics if period <= 0
func WithCleaningPeriod(period time.Duration) TaskKindOption {
	if period <= 0 {
		panic("cleaner period must be positive")
	}

	return func(tk *taskKind) {
		tk.cleanerCfg.period = period
	}
}

// WithCleaningLimit sets the maximum number of tasks deleted by one cleaner job execution. Panics if limit <= 0
func WithCleaningLimit(limit int) TaskKindOption {
	if limit <= 0 {
		panic("cleaner limit must be positive")
	}

	return func(tk *taskKind) {
		tk.cleanerCfg.limit = limit
	}
}

// WithLogger sets Logger  Can be used with log package. Panics if logger is nil
func WithLogger(logger Logger) TaskKindOption {
	if logger == nil {
		panic("logger must not be nil")
	}

	return func(tk *taskKind) {
		tk.logger = logger
	}
}

// WithStatsCollector sets StatsCollector. Panics if collector is nil
func WithStatsCollector(collector StatsCollector) TaskKindOption {
	if collector == nil {
		panic("collector must not be nil")
	}

	return func(tk *taskKind) {
		tk.statsCollector = collector
	}
}
