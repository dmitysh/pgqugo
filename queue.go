package pgqugo

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/DmitySH/wopo"
)

// TODO:
// * log package to zap
// * метрики ?

// TODO: benchmarks

type empty = struct{}

type DB interface {
	CreateTask(ctx context.Context, task FullTaskInfo) error
	GetWaitingTasks(ctx context.Context, fetchParams FetchParams) ([]FullTaskInfo, error)
	SoftFailTask(ctx context.Context, taskID int64) error
	FailTask(ctx context.Context, taskID int64) error
	SucceedTask(ctx context.Context, taskID int64) error
}

type FetchParams struct {
	KindID           int16
	BatchSize        int
	AttemptsInterval time.Duration
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
	for _, kind := range q.kinds {
		go q.workLoop(kind)
	}
}

func (q *Queue) workLoop(kind taskKind) {
	q.stopWg.Add(1)
	defer q.stopWg.Done()

	t := time.NewTicker(kind.fetchPeriod)
	defer t.Stop()

	e := executor{
		tk: kind,
		db: q.db,
	}
	wp := wopo.NewPool(
		e.execute,
		wopo.WithWorkerCount[FullTaskInfo, empty](kind.workerCount),
		wopo.WithTaskBufferSize[FullTaskInfo, empty](kind.workerCount/2),
		wopo.WithResultBufferSize[FullTaskInfo, empty](-1),
	)
	wp.Start()

	for {
		select {
		case <-q.stopCh:
			wp.Stop()
			return
		case <-t.C:
			err := q.fetchAndPushTasks(kind, wp)
			if err != nil {
				log.Println(err)
			}
		}
	}
}

func (q *Queue) fetchAndPushTasks(kind taskKind, wp *wopo.Pool[FullTaskInfo, empty]) error {
	const defaultFetchTimeout = time.Second * 3

	ctx, cancel := context.WithTimeout(context.Background(), defaultFetchTimeout)
	defer cancel()

	tasks, err := q.db.GetWaitingTasks(ctx, FetchParams{
		KindID:           kind.id,
		BatchSize:        kind.batchSize,
		AttemptsInterval: kind.attemptsInterval,
	})
	if err != nil {
		return fmt.Errorf("can't fetch tasks: %w", err)
	}

	// TODO: add context with timeout
	for i := 0; i < len(tasks); i++ {
		taskCtx := context.Background()
		wp.PushTask(taskCtx, tasks[i])
	}

	return nil
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

	ti := FullTaskInfo{
		Kind:         task.Kind,
		Key:          task.Key,
		Payload:      task.Payload,
		AttemptsLeft: kind.maxAttempts,
	}

	return q.db.CreateTask(ctx, ti)
}
