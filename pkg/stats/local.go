package stats

import (
	"sync/atomic"
)

type LocalCollector struct {
	m map[string]*atomic.Int64
}

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

func (l *LocalCollector) IncNewTasks() {
	l.m["new"].Add(1)
}
func (l *LocalCollector) AddInProgressTasks(count int) {
	l.m["in_progress"].Add(int64(count))
}
func (l *LocalCollector) IncSuccessTasks() {
	l.m["succeeded"].Add(1)
}
func (l *LocalCollector) IncSoftFailedTasks() {
	l.m["retry"].Add(1)
}
func (l *LocalCollector) IncFailedTasks() {
	l.m["failed"].Add(1)
}

func (l *LocalCollector) GetTasksByStatus(status string) int64 {
	return l.m[status].Load()
}
