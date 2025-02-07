package pgqugo_test

import (
	"context"
	"fmt"
	"time"

	"github.com/DmitySH/pgqugo"
	"github.com/DmitySH/pgqugo/pkg/adapter"
	"github.com/DmitySH/pgqugo/pkg/delayer"
)

func (s *pgxV5Suite) TestWithBatchSize() {
	ctx := context.Background()

	const (
		taskCount   = 30
		fetchPeriod = time.Millisecond * 300
	)

	h := newSuccessHandler(0)
	q := pgqugo.New(
		adapter.NewPGXv5(s.pool),
		pgqugo.TaskKinds{
			pgqugo.NewTaskKind(
				testTaskKind,
				h,
				pgqugo.WithFetchPeriod(fetchPeriod, 0),
				pgqugo.WithMaxAttempts(1),
				pgqugo.WithAttemptDelayer(delayer.Linear(time.Millisecond*10, 0)),

				pgqugo.WithBatchSize(taskCount),
			),
		},
	)
	q.Start()
	defer q.Stop()

	for i := 0; i < taskCount; i++ {
		key := fmt.Sprintf("key_%d", i)
		task := pgqugo.Task{
			Kind:    1,
			Key:     &key,
			Payload: "{}",
		}
		s.Require().NoError(q.CreateTask(ctx, task))
	}

	s.Require().Eventually(func() bool {
		return s.getTasksByStatus(testTaskKind, pgqugo.TaskStatusSuccess) == taskCount
	}, eventuallyTimeout, eventuallyPollingPeriod)

	now := time.Now()
	for i := 0; i < taskCount; i++ {
		key := fmt.Sprintf("key_%d", i)
		taskInfo := s.getTaskByKey(testTaskKind, key)

		s.Require().WithinDuration(now, taskInfo.UpdatedAt, fetchPeriod/2)
	}
}

func (s *pgxV5Suite) TestWithWorkerCount() {
	ctx := context.Background()

	const taskCount = 5
	handlerDur := time.Millisecond * 100
	h := newSuccessHandler(handlerDur)
	q := pgqugo.New(
		adapter.NewPGXv5(s.pool),
		pgqugo.TaskKinds{
			pgqugo.NewTaskKind(
				testTaskKind,
				h,
				pgqugo.WithFetchPeriod(time.Millisecond*10, 0),
				pgqugo.WithMaxAttempts(1),
				pgqugo.WithBatchSize(taskCount),
				pgqugo.WithAttemptDelayer(delayer.Linear(time.Millisecond*10, 0)),

				pgqugo.WithWorkerCount(5),
			),
		},
	)
	q.Start()
	defer q.Stop()

	for i := 0; i < taskCount; i++ {
		key := fmt.Sprintf("key_%d", i)
		task := pgqugo.Task{
			Kind:    1,
			Key:     &key,
			Payload: "{}",
		}
		s.Require().NoError(q.CreateTask(ctx, task))
	}

	s.Require().Eventually(func() bool {
		return s.getTasksByStatus(testTaskKind, pgqugo.TaskStatusSuccess) == taskCount
	}, eventuallyTimeout, eventuallyPollingPeriod)

	for i := 0; i < taskCount; i++ {
		key := fmt.Sprintf("key_%d", i)
		taskInfo := s.getTaskByKey(testTaskKind, key)

		s.Require().WithinDuration(taskInfo.CreatedAt, taskInfo.UpdatedAt, handlerDur*150/100)
	}
}

func (s *pgxV5Suite) TestWithAttemptTimeout() {
	ctx := context.Background()

	const taskCount = 3
	h := newSuccessHandler(time.Millisecond * 100)
	q := pgqugo.New(
		adapter.NewPGXv5(s.pool),
		pgqugo.TaskKinds{
			pgqugo.NewTaskKind(
				testTaskKind,
				h,
				pgqugo.WithFetchPeriod(time.Millisecond*300, 0),
				pgqugo.WithMaxAttempts(1),
				pgqugo.WithBatchSize(taskCount),
				pgqugo.WithAttemptDelayer(delayer.Linear(time.Millisecond*10, 0)),

				pgqugo.WithAttemptTimeout(time.Millisecond*30),
			),
		},
	)
	q.Start()
	defer q.Stop()

	for i := 0; i < taskCount; i++ {
		key := fmt.Sprintf("key_%d", i)
		task := pgqugo.Task{
			Kind:    1,
			Key:     &key,
			Payload: "{}",
		}
		s.Require().NoError(q.CreateTask(ctx, task))
	}

	s.Require().Eventually(func() bool {
		return s.getTasksByStatus(testTaskKind, pgqugo.TaskStatusFailed) == taskCount
	}, eventuallyTimeout, eventuallyPollingPeriod)
}

func (s *pgxV5Suite) TestCleaner() {
	ctx := context.Background()

	const taskCount = 30
	h := newSuccessHandler(0)
	q := pgqugo.New(
		adapter.NewPGXv5(s.pool),
		pgqugo.TaskKinds{
			pgqugo.NewTaskKind(
				testTaskKind,
				h,
				pgqugo.WithFetchPeriod(time.Millisecond*300, 0),
				pgqugo.WithMaxAttempts(1),
				pgqugo.WithBatchSize(taskCount),
				pgqugo.WithAttemptDelayer(delayer.Linear(time.Millisecond*10, 0)),
				pgqugo.WithTerminalTasksTTL(time.Millisecond*10),

				pgqugo.WithCleaningLimit(taskCount),
				pgqugo.WithCleaningPeriod(time.Millisecond*100),
			),
		},
	)
	q.Start()
	defer q.Stop()

	for i := 0; i < taskCount; i++ {
		key := fmt.Sprintf("key_%d", i)
		task := pgqugo.Task{
			Kind:    1,
			Key:     &key,
			Payload: "{}",
		}
		s.Require().NoError(q.CreateTask(ctx, task))
	}

	s.Require().Eventually(func() bool {
		return s.getTotalTasks(testTaskKind) > 0
	}, eventuallyTimeout, eventuallyPollingPeriod)

	s.Require().Eventually(func() bool {
		return s.getTotalTasks(testTaskKind) == 0
	}, eventuallyTimeout, eventuallyPollingPeriod)
}
