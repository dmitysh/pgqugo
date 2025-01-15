package pgqugo

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// TODO:
// * log package to zap
// * сущность для каждого kind - fetcher ?
// * батчи - ok
// * пул воркеров
// * метрики ?

type DB interface {
	CreateTask(ctx context.Context, task FullTaskInfo) error
	GetWaitingTasks(ctx context.Context, fetchParams FetchParams) ([]FullTaskInfo, error)
	SoftFailTasks(ctx context.Context, taskIDs []int64) error
	FailTasks(ctx context.Context, taskIDs []int64) error
	SucceedTasks(ctx context.Context, taskIDs []int64) error
}

type FetchParams struct {
	KindID           int16
	BatchSize        int32
	AttemptsInterval time.Duration
}

type Queue struct {
	db    DB
	kinds map[int16]taskKind

	stopWg sync.WaitGroup
	stopCh chan struct{}
}

func New(db DB, kinds TaskKinds) (*Queue, error) {
	err := validateKinds(kinds)
	if err != nil {
		return nil, err
	}

	kindsMap := make(map[int16]taskKind, len(kinds))
	for _, kind := range kinds {
		kindsMap[kind.id] = kind
	}

	return &Queue{
		db:     db,
		kinds:  kindsMap,
		stopWg: sync.WaitGroup{},
		stopCh: make(chan struct{}),
	}, nil
}

func validateKinds(kinds TaskKinds) error {
	uniqueKinds := map[int16]struct{}{}

	for _, kind := range kinds {
		if _, exists := uniqueKinds[kind.id]; exists {
			return fmt.Errorf("task kinds keys must be unique, error kind: %d", kind.id)
		}

		uniqueKinds[kind.id] = struct{}{}

		if kind.maxAttempts <= 0 {
			return fmt.Errorf("number of attempts must be positive, error kind: %d", kind.id)
		}
	}

	return nil
}

func (q *Queue) Start(ctx context.Context) {
	for _, kind := range q.kinds {
		go q.workLoop(ctx, kind)
	}
}

func (q *Queue) workLoop(ctx context.Context, kind taskKind) {
	q.stopWg.Add(1)
	defer q.stopWg.Done()

	t := time.NewTicker(kind.fetchPeriod)
	defer t.Stop()

	for {
		select {
		case <-q.stopCh:
			return
		case <-t.C:
			err := q.fetchAndHandle(ctx, kind)
			if err != nil {
				log.Println(err)
			}
		}
	}
}

func (q *Queue) fetchAndHandle(ctx context.Context, kind taskKind) error {
	const defaultFetchTimeout = time.Second * 3

	ctx, cancel := context.WithTimeout(ctx, defaultFetchTimeout)
	defer cancel()

	tasks, err := q.db.GetWaitingTasks(ctx, FetchParams{
		KindID:           kind.id,
		BatchSize:        kind.batchSize,
		AttemptsInterval: kind.attemptsInterval,
	})
	if err != nil {
		return fmt.Errorf("can't fetch tasks: %w", err)
	}

	succeededTaskIDs := make([]int64, 0)
	softFailedTaskIDs := make([]int64, 0)
	failedTaskIDs := make([]int64, 0)

	for i := 0; i < len(tasks); i++ {
		pt := ProcessingTask{
			Task: Task{
				Kind:    tasks[i].Kind,
				Key:     tasks[i].Key,
				Payload: tasks[i].Payload,
			},
			AttemptsElapsed: tasks[i].AttemptsElapsed,
		}

		handlerErr := kind.handler.HandleTask(ctx, pt)
		if handlerErr != nil {
			log.Println(handlerErr)

			if tasks[i].AttemptsLeft == 0 {
				failedTaskIDs = append(failedTaskIDs, tasks[i].ID)
			} else {
				softFailedTaskIDs = append(softFailedTaskIDs, tasks[i].ID)
			}
		} else {
			succeededTaskIDs = append(succeededTaskIDs, tasks[i].ID)
		}
	}

	err = q.db.SucceedTasks(ctx, succeededTaskIDs)
	if err != nil {
		return fmt.Errorf("can't succeed tasks: %w", err)
	}

	err = q.db.SoftFailTasks(ctx, softFailedTaskIDs)
	if err != nil {
		return fmt.Errorf("can't soft fail tasks: %w", err)
	}

	err = q.db.FailTasks(ctx, failedTaskIDs)
	if err != nil {
		return fmt.Errorf("can't fail tasks: %w", err)
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
