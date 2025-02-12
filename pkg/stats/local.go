package stats

import (
	"sync/atomic"
)

// LocalCollector stats collector based on atomics
type LocalCollector struct {
	m map[string]*atomic.Int64
}

// NewLocalCollector LocalCollector constructor
func NewLocalCollector() *LocalCollector {
	return &LocalCollector{
		m: map[string]*atomic.Int64{
			"new":         {},
			"in_progress": {},
			"succeeded":   {},
			"retry":       {},
			"failed":      {},
		},
	}
}

// IncNewTasks increments status new counter
func (l *LocalCollector) IncNewTasks() {
	l.m["new"].Add(1)
}

// AddInProgressTasks add count to status in_progress counter
func (l *LocalCollector) AddInProgressTasks(count int) {
	l.m["in_progress"].Add(int64(count))
}

// IncSuccessTasks increments status succeeded counter
func (l *LocalCollector) IncSuccessTasks() {
	l.m["succeeded"].Add(1)
}

// IncSoftFailedTasks increments status retry counter
func (l *LocalCollector) IncSoftFailedTasks() {
	l.m["retry"].Add(1)
}

// IncFailedTasks increments status failed counter
func (l *LocalCollector) IncFailedTasks() {
	l.m["failed"].Add(1)
}

// GetTasksByStatus returns the counter value by status
func (l *LocalCollector) GetTasksByStatus(status string) int64 {
	return l.m[status].Load()
}
