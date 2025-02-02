package pgqugo

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/DmitySH/pgqugo/internal/entity"
	"github.com/DmitySH/wopo"
)

const (
	wpBufferSizeToNumOfWorkersFactor = 2
	defaultDBTimeout                 = time.Second * 3
)

// TODO:
// logger
// metrics
// transactions

// TODO: benchmarks, refactor tests (suites)

type empty = struct{}

type DB interface {
	CreateTask(ctx context.Context, task entity.FullTaskInfo) error
	GetWaitingTasks(ctx context.Context, params entity.GetWaitingTasksParams) ([]entity.FullTaskInfo, error)
	SoftFailTask(ctx context.Context, taskID int64) error
	FailTask(ctx context.Context, taskID int64) error
	SucceedTask(ctx context.Context, taskID int64) error
	DeleteTerminalTasks(ctx context.Context, params entity.DeleteTerminalTasksParams) error
	RegisterJob(ctx context.Context, job string) error
	ExecuteJob(ctx context.Context, jobName string, jobPeriod time.Duration) error
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
		q.stopWg.Add(1)
		go q.workLoop(kind)
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

func (q *Queue) workLoop(kind taskKind) {
	defer q.stopWg.Done()

	ctx, cancelWorkLoop := context.WithCancel(context.Background())
	defer cancelWorkLoop()

	t := time.NewTimer(kind.fetchPeriod())
	defer t.Stop()

	c := cleaner{
		tk: kind,
		db: q.db,
	}
	go c.run(ctx)

	e := executor{
		tk: kind,
		db: q.db,
	}
	wp := wopo.NewPool(
		e.execute,
		wopo.WithWorkerCount[entity.FullTaskInfo, empty](kind.workerCount),
		wopo.WithTaskBufferSize[entity.FullTaskInfo, empty](kind.workerCount*wpBufferSizeToNumOfWorkersFactor),
		wopo.WithResultBufferSize[entity.FullTaskInfo, empty](-1),
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
			t.Reset(kind.fetchPeriod())
		}
	}
}

func (q *Queue) fetchAndPushTasks(kind taskKind, wp *wopo.Pool[entity.FullTaskInfo, empty]) error {
	fetchCtx, fetchCancel := context.WithTimeout(context.Background(), defaultDBTimeout)
	defer fetchCancel()

	tasks, err := q.db.GetWaitingTasks(fetchCtx, entity.GetWaitingTasksParams{
		KindID:           kind.id,
		BatchSize:        kind.batchSize,
		AttemptsInterval: kind.attemptsInterval,
	})
	if err != nil {
		return fmt.Errorf("can't fetch tasks: %w", err)
	}

	for i := 0; i < len(tasks); i++ {
		wp.PushTask(context.Background(), tasks[i])
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

	ti := entity.FullTaskInfo{
		Kind:         task.Kind,
		Key:          task.Key,
		Payload:      task.Payload,
		AttemptsLeft: kind.maxAttempts,
	}

	return q.db.CreateTask(ctx, ti)
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
