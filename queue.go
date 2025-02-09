package pgqugo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dmitysh/pgqugo/internal/entity"
)

type TaskStatus string

const (
	TaskStatusNew        = "new"
	TaskStatusSuccess    = "succeeded"
	TaskStatusInProgress = "in_progress"
	TaskStatusRetry      = "retry"
	TaskStatusFailed     = "failed"
)

const (
	defaultDBTimeout = time.Second * 3
)

type empty = struct{}

type DB interface {
	CreateTask(ctx context.Context, task entity.FullTaskInfo) error
	GetPendingTasks(ctx context.Context, params entity.GetPendingTasksParams) ([]entity.FullTaskInfo, error)
	SoftFailTask(ctx context.Context, params entity.SoftFailTasksParams) error
	FailTask(ctx context.Context, taskID int64) error
	SucceedTask(ctx context.Context, taskID int64) error
	DeleteTerminalTasks(ctx context.Context, params entity.DeleteTerminalTasksParams) error
	RegisterJob(ctx context.Context, job string) error
	ExecuteJob(ctx context.Context, params entity.ExecuteJobParams) error
}

type Queue struct {
	db    DB
	kinds map[int16]taskKind

	stopWg sync.WaitGroup
	stopCh chan empty
}

func New(db DB, kinds TaskKinds) *Queue {
	err := validateKinds(kinds)
	if err != nil {
		panic(err)
	}

	kindsMap := make(map[int16]taskKind, len(kinds))
	for _, kind := range kinds {
		kindsMap[kind.id] = kind
	}

	return &Queue{
		db:     db,
		kinds:  kindsMap,
		stopWg: sync.WaitGroup{},
		stopCh: make(chan empty),
	}
}

func validateKinds(kinds TaskKinds) error {
	uniqueKinds := map[int16]empty{}

	for _, kind := range kinds {
		if _, exists := uniqueKinds[kind.id]; exists {
			return fmt.Errorf("task kinds keys must be unique, error kind: %d", kind.id)
		}

		uniqueKinds[kind.id] = empty{}
	}

	return nil
}

func (q *Queue) Start() {
	err := q.registerJobs()
	if err != nil {
		panic(err)
	}

	for _, kind := range q.kinds {
		f := newFetcher(kind, q.db)
		c := newCleaner(kind, q.db)

		q.stopWg.Add(2)
		go q.runWithDone(f)
		go q.runWithDone(c)
	}
}

func (q *Queue) registerJobs() error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultDBTimeout)
	defer cancel()

	for _, kind := range q.kinds {
		err := q.db.RegisterJob(ctx, cleanerJob(kind.id))
		if err != nil {
			return fmt.Errorf("can't register cleaner job for kind %d: %w", kind.id, err)
		}
	}

	return nil
}

type runner interface {
	run(stopCh <-chan empty)
}

func (q *Queue) runWithDone(r runner) {
	defer q.stopWg.Done()

	r.run(q.stopCh)
}

func (q *Queue) Stop() {
	close(q.stopCh)
	q.stopWg.Wait()
}

func (q *Queue) CreateTask(ctx context.Context, task Task) error {
	kind, exists := q.kinds[task.Kind]
	if !exists {
		return fmt.Errorf("kind %d does not exist", task.Kind)
	}

	ti := entity.FullTaskInfo{
		Kind:         task.Kind,
		Key:          task.Key,
		Payload:      task.Payload,
		AttemptsLeft: kind.maxAttempts,
	}

	err := q.db.CreateTask(ctx, ti)
	if err != nil {
		return err
	}
	kind.statsCollector.IncNewTasks()

	return nil
}

type Task struct {
	Kind    int16
	Key     *string
	Payload string
}

type ProcessingTask struct {
	Task
	AttemptsLeft    int16
	AttemptsElapsed int16
}
