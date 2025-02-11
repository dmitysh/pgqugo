package pgqugo_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dmitysh/pgqugo"
	"github.com/dmitysh/pgqugo/internal/entity"
	"github.com/dmitysh/pgqugo/pkg/adapter"
	"github.com/dmitysh/pgqugo/pkg/delayer"
	"github.com/dmitysh/pgqugo/pkg/log"
	"github.com/dmitysh/pgqugo/pkg/stats"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

const (
	dbTimeout               = time.Second * 2
	eventuallyPollingPeriod = time.Millisecond * 100
	eventuallyTimeout       = eventuallyPollingPeriod * 20
)

const (
	testTaskKind = int16(1)
)

var (
	errTest = errors.New("some error")
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestSuiteQueuePgxV5(t *testing.T) {
	t.Parallel()

	s := newQueuePgxV5Suite()
	suite.Run(t, s)
}

type pgxV5Suite struct {
	suite.Suite

	pool *pgxpool.Pool
}

func newQueuePgxV5Suite() *pgxV5Suite {
	return &pgxV5Suite{}
}

func (s *pgxV5Suite) SetupSuite() {
	s.pool = s.newTestPgPool()
}

func (s *pgxV5Suite) newTestPgPool() *pgxpool.Pool {
	ctx, cancel := s.newContextWithDBTimeout()
	defer cancel()

	p, err := pgxpool.New(ctx, os.Getenv("TEST_PG_DSN"))
	s.Require().NoError(err)

	err = p.Ping(ctx)
	s.Require().NoError(err)

	return p
}

func (s *pgxV5Suite) newContextWithDBTimeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), dbTimeout)
}

func (s *pgxV5Suite) TearDownSuite() {
	s.pool.Close()
}

func (s *pgxV5Suite) SetupTest() {
	s.clearQueueTable()
}

func (s *pgxV5Suite) clearQueueTable() {
	ctx, cancel := s.newContextWithDBTimeout()
	defer cancel()

	_, err := s.pool.Exec(ctx, "TRUNCATE TABLE pgqueue")
	s.Require().NoError(err)
}

func (s *pgxV5Suite) TestCreateTaskSuccess() {
	ctx := context.Background()

	sc := stats.NewLocalCollector()
	h := newSuccessHandler(0)
	q := pgqugo.New(
		adapter.NewPGXv5(s.pool),
		pgqugo.TaskKinds{
			pgqugo.NewTaskKind(
				testTaskKind,
				h,
				pgqugo.WithFetchPeriod(time.Millisecond*300, 0),
				pgqugo.WithMaxAttempts(3),
				pgqugo.WithAttemptDelayer(delayer.Linear(time.Millisecond*10, 0)),
				pgqugo.WithStatsCollector(sc),
			),
		},
	)
	q.Start()
	defer q.Stop()

	const taskCount = 3
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
		return s.getTasksByStatus(testTaskKind, pgqugo.TaskStatusSuccess) == 3
	}, eventuallyTimeout, eventuallyPollingPeriod)

	for i := 0; i < taskCount; i++ {
		key := fmt.Sprintf("key_%d", i)
		taskInfo := s.getTaskByKey(testTaskKind, key)

		s.Require().Equal(testTaskKind, taskInfo.Kind)
		s.Require().Equal(key, *taskInfo.Key)
		s.Require().WithinDuration(time.Now(), taskInfo.CreatedAt, eventuallyTimeout)
		s.Require().WithinDuration(time.Now(), taskInfo.UpdatedAt, eventuallyTimeout)
		s.Require().Nil(taskInfo.NextAttemptTime)
		s.Require().Equal(pgqugo.TaskStatusSuccess, taskInfo.Status)
		s.Require().Equal(int16(2), taskInfo.AttemptsLeft)
		s.Require().Equal(int16(1), taskInfo.AttemptsElapsed)
		s.Require().Equal("{}", taskInfo.Payload)
	}

	s.Require().Equal(3, h.callsCount())

	s.Require().Equal(int64(3), sc.GetTasksByStatus(pgqugo.TaskStatusNew))
	s.Require().Equal(int64(3), sc.GetTasksByStatus(pgqugo.TaskStatusInProgress))
	s.Require().Equal(int64(3), sc.GetTasksByStatus(pgqugo.TaskStatusSuccess))
	s.Require().Equal(int64(0), sc.GetTasksByStatus(pgqugo.TaskStatusRetry))
	s.Require().Equal(int64(0), sc.GetTasksByStatus(pgqugo.TaskStatusFailed))
}

func (s *pgxV5Suite) TestCreateTaskTx() {
	ctx := context.Background()

	sc := stats.NewLocalCollector()
	h := newSuccessHandler(0)
	q := pgqugo.New(
		adapter.NewPGXv5(s.pool),
		pgqugo.TaskKinds{
			pgqugo.NewTaskKind(
				testTaskKind,
				h,
				pgqugo.WithFetchPeriod(time.Millisecond*300, 0),
				pgqugo.WithMaxAttempts(3),
				pgqugo.WithAttemptDelayer(delayer.Linear(time.Millisecond*10, 0)),
				pgqugo.WithStatsCollector(sc),
			),
		},
	)
	q.Start()
	defer q.Stop()

	tx, err := s.pool.Begin(ctx)
	s.Require().NoError(err)
	defer tx.Rollback(ctx) //nolint: errcheck

	const taskCount = 3
	for i := 0; i < taskCount; i++ {
		key := fmt.Sprintf("key_%d", i)
		task := pgqugo.Task{
			Kind:    1,
			Key:     &key,
			Payload: "{}",
		}
		if i > 0 {
			s.Require().NoError(q.CreateTask(adapter.InjectPGXv5Tx(ctx, tx), task))
		} else {
			s.Require().NoError(q.CreateTask(ctx, task))
		}
	}
	s.Require().NoError(tx.Rollback(ctx))

	s.Require().Eventually(func() bool {
		return s.getTasksByStatus(testTaskKind, pgqugo.TaskStatusSuccess) == 1
	}, eventuallyTimeout, eventuallyPollingPeriod)

	s.Require().Equal(1, s.getTotalTasks(testTaskKind))
}

func (s *pgxV5Suite) getTasksByStatus(kind int16, status pgqugo.TaskStatus) int {
	ctx, cancel := s.newContextWithDBTimeout()
	defer cancel()

	const q = `SELECT count(1) FROM pgqueue 
                WHERE kind = $1
                  AND status = $2`

	var n int
	s.Require().NoError(s.pool.QueryRow(ctx, q, kind, status).Scan(&n))

	return n
}

func (s *pgxV5Suite) TestCreateTaskUnknownKind() {
	ctx := context.Background()

	q := pgqugo.New(
		adapter.NewPGXv5(s.pool),
		pgqugo.TaskKinds{
			pgqugo.NewTaskKind(
				testTaskKind,
				nil,
				pgqugo.WithFetchPeriod(time.Millisecond*300, 0),
				pgqugo.WithAttemptDelayer(delayer.Linear(time.Millisecond*10, 0)),
			),
		},
	)
	q.Start()
	defer q.Stop()

	key := fmt.Sprintf("key_%d", 0)
	task := pgqugo.Task{
		Kind:    testTaskKind + 1,
		Key:     &key,
		Payload: "{}",
	}
	s.Require().ErrorContains(q.CreateTask(ctx, task), "kind 2 does not exist")
}

func (s *pgxV5Suite) TestCreateTaskIdempotencyConflict() {
	ctx := context.Background()

	q := pgqugo.New(
		adapter.NewPGXv5(s.pool),
		pgqugo.TaskKinds{
			pgqugo.NewTaskKind(
				testTaskKind,
				nil,
				pgqugo.WithFetchPeriod(time.Millisecond*300, 0),
				pgqugo.WithAttemptDelayer(delayer.Linear(time.Millisecond*10, 0)),
			),
		},
	)
	q.Start()
	defer q.Stop()

	key := "conflict"
	task := pgqugo.Task{
		Kind:    testTaskKind,
		Key:     &key,
		Payload: "{}",
	}
	s.Require().NoError(q.CreateTask(ctx, task))
	s.Require().NoError(q.CreateTask(ctx, task))
}
func (s *pgxV5Suite) TestHandlerError() {
	ctx := context.Background()

	sc := stats.NewLocalCollector()
	h := newErrorHandler()
	q := pgqugo.New(
		adapter.NewPGXv5(s.pool),
		pgqugo.TaskKinds{
			pgqugo.NewTaskKind(
				testTaskKind,
				h,
				pgqugo.WithMaxAttempts(3),
				pgqugo.WithFetchPeriod(time.Millisecond*300, 0.5),
				pgqugo.WithAttemptDelayer(delayer.Linear(time.Millisecond*10, 0)),
				pgqugo.WithStatsCollector(sc),
			),
		},
	)
	q.Start()
	defer q.Stop()

	const taskCount = 3
	for i := 0; i < taskCount; i++ {
		key := fmt.Sprintf("key_%d", i)
		task := pgqugo.Task{
			Kind:    testTaskKind,
			Key:     &key,
			Payload: "{}",
		}
		s.Require().NoError(q.CreateTask(ctx, task))
	}

	s.Require().Eventually(func() bool {
		return s.getTasksByStatus(testTaskKind, pgqugo.TaskStatusFailed) == 3
	}, eventuallyTimeout, eventuallyPollingPeriod)

	for i := 0; i < taskCount; i++ {
		key := fmt.Sprintf("key_%d", i)
		taskInfo := s.getTaskByKey(testTaskKind, key)

		s.Require().Equal(testTaskKind, taskInfo.Kind)
		s.Require().Equal(key, *taskInfo.Key)
		s.Require().WithinDuration(time.Now(), taskInfo.CreatedAt, eventuallyTimeout)
		s.Require().WithinDuration(time.Now(), taskInfo.UpdatedAt, eventuallyTimeout)
		s.Require().Nil(taskInfo.NextAttemptTime)
		s.Require().Equal(pgqugo.TaskStatusFailed, taskInfo.Status)
		s.Require().Equal(int16(0), taskInfo.AttemptsLeft)
		s.Require().Equal(int16(3), taskInfo.AttemptsElapsed)
		s.Require().Equal("{}", taskInfo.Payload)
	}

	s.Require().Equal(9, h.callsCount())

	s.Require().Equal(int64(3), sc.GetTasksByStatus(pgqugo.TaskStatusNew))
	s.Require().Equal(int64(9), sc.GetTasksByStatus(pgqugo.TaskStatusInProgress))
	s.Require().Equal(int64(0), sc.GetTasksByStatus(pgqugo.TaskStatusSuccess))
	s.Require().Equal(int64(6), sc.GetTasksByStatus(pgqugo.TaskStatusRetry))
	s.Require().Equal(int64(3), sc.GetTasksByStatus(pgqugo.TaskStatusFailed))
}

func (s *pgxV5Suite) TestHandlerPanic() {
	ctx := context.Background()

	sc := stats.NewLocalCollector()
	h := newPanicHandler()
	q := pgqugo.New(
		adapter.NewPGXv5(s.pool),
		pgqugo.TaskKinds{
			pgqugo.NewTaskKind(
				testTaskKind,
				h,
				pgqugo.WithMaxAttempts(3),
				pgqugo.WithFetchPeriod(time.Millisecond*300, 0.5),
				pgqugo.WithAttemptDelayer(delayer.Linear(time.Millisecond*10, 0)),
				pgqugo.WithLogger(log.NewNoOp()),
				pgqugo.WithStatsCollector(sc),
			),
		},
	)
	q.Start()
	defer q.Stop()

	const taskCount = 3
	for i := 0; i < taskCount; i++ {
		key := fmt.Sprintf("key_%d", i)
		task := pgqugo.Task{
			Kind:    testTaskKind,
			Key:     &key,
			Payload: "{}",
		}
		s.Require().NoError(q.CreateTask(ctx, task))
	}

	s.Require().Eventually(func() bool {
		return s.getTasksByStatus(testTaskKind, pgqugo.TaskStatusFailed) == 3
	}, eventuallyTimeout, eventuallyPollingPeriod)

	for i := 0; i < taskCount; i++ {
		key := fmt.Sprintf("key_%d", i)
		taskInfo := s.getTaskByKey(testTaskKind, key)

		s.Require().Equal(testTaskKind, taskInfo.Kind)
		s.Require().Equal(key, *taskInfo.Key)
		s.Require().WithinDuration(time.Now(), taskInfo.CreatedAt, eventuallyTimeout)
		s.Require().WithinDuration(time.Now(), taskInfo.UpdatedAt, eventuallyTimeout)
		s.Require().Nil(taskInfo.NextAttemptTime)
		s.Require().Equal(pgqugo.TaskStatusFailed, taskInfo.Status)
		s.Require().Equal(int16(0), taskInfo.AttemptsLeft)
		s.Require().Equal(int16(3), taskInfo.AttemptsElapsed)
		s.Require().Equal("{}", taskInfo.Payload)
	}

	s.Require().Equal(9, h.callsCount())

	s.Require().Equal(int64(3), sc.GetTasksByStatus(pgqugo.TaskStatusNew))
	s.Require().Equal(int64(9), sc.GetTasksByStatus(pgqugo.TaskStatusInProgress))
	s.Require().Equal(int64(0), sc.GetTasksByStatus(pgqugo.TaskStatusSuccess))
	s.Require().Equal(int64(6), sc.GetTasksByStatus(pgqugo.TaskStatusRetry))
	s.Require().Equal(int64(3), sc.GetTasksByStatus(pgqugo.TaskStatusFailed))
}

func (s *pgxV5Suite) getTaskByKey(kind int16, key string) entity.FullTaskInfo {
	ctx, cancel := s.newContextWithDBTimeout()
	defer cancel()

	const q = `SELECT id,               
					  kind,             
					  key,              
					  payload,          
					  status,           
					  attempts_left,    
					  attempts_elapsed, 
					  next_attempt_time,
					  created_at,       
					  updated_at        
				 FROM pgqueue
			    WHERE kind = $1
			      AND key = $2`

	var task entity.FullTaskInfo
	s.Require().NoError(s.pool.QueryRow(ctx, q, kind, key).Scan(&task.ID, &task.Kind, &task.Key, &task.Payload,
		&task.Status, &task.AttemptsLeft, &task.AttemptsElapsed, &task.NextAttemptTime, &task.CreatedAt, &task.UpdatedAt))

	return task
}

func (s *pgxV5Suite) getTotalTasks(kind int16) int {
	ctx, cancel := s.newContextWithDBTimeout()
	defer cancel()

	const q = `SELECT count(1) FROM pgqueue WHERE kind = $1`

	var n int
	s.Require().NoError(s.pool.QueryRow(ctx, q, kind).Scan(&n))

	return n
}

type successHandler struct {
	baseHandler
	dur time.Duration
}

func newSuccessHandler(dur time.Duration) *successHandler {
	return &successHandler{
		dur: dur,
	}
}

func (h *successHandler) HandleTask(ctx context.Context, _ pgqugo.ProcessingTask) error {
	h.callCounter.Add(1)

	select {
	case <-time.After(h.dur):
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

type errorHandler struct {
	baseHandler
}

func newErrorHandler() *errorHandler {
	return &errorHandler{}
}

func (h *errorHandler) HandleTask(_ context.Context, _ pgqugo.ProcessingTask) error {
	h.callCounter.Add(1)
	return errTest
}

type panicHandler struct {
	baseHandler
}

func newPanicHandler() *panicHandler {
	return &panicHandler{}
}

func (h *panicHandler) HandleTask(_ context.Context, _ pgqugo.ProcessingTask) error {
	h.callCounter.Add(1)
	panic("some panic")
}

type baseHandler struct {
	callCounter atomic.Int32
}

func (h *baseHandler) callsCount() int {
	return int(h.callCounter.Load())
}
