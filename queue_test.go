package pgqugo_test

import (
	"context"
	"errors"
	"math/rand/v2"
	"strconv"
	"testing"
	"time"

	"github.com/DmitySH/pgqugo"
	"github.com/DmitySH/pgqugo/pkg/adapter"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	errFailed = errors.New("task failed")
)

const (
	testPostgresDSN = "postgresql://postgres:postgres@localhost:5490/postgres"
)

const (
	successHandleTaskKind = int16(iota) + 1
	failHandleTaskKind
)

type successHandler struct{}

func (h successHandler) HandleTask(_ context.Context, _ pgqugo.ProcessingTask) error {
	return nil
}

type failHandler struct{}

func (h failHandler) HandleTask(_ context.Context, _ pgqugo.ProcessingTask) error {
	return errFailed
}

func newTestPGXPool(ctx context.Context, t *testing.T) *pgxpool.Pool {
	t.Helper()

	p, err := pgxpool.New(ctx, testPostgresDSN)
	require.NoError(t, err)

	err = p.Ping(ctx)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	return p
}

func TestQueue_SuccessHandleTask_PGX(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	pool := newTestPGXPool(ctx, t)
	_, err := pool.Exec(ctx, `DELETE FROM pgqueue WHERE kind = $1`, successHandleTaskKind)
	require.NoError(t, err)

	h := successHandler{}
	kinds := pgqugo.TaskKinds{
		pgqugo.NewTaskKind(successHandleTaskKind, h, 3, 2, time.Millisecond*100, time.Minute),
	}

	q, err := pgqugo.New(adapter.NewPGX(pool), kinds)
	require.NoError(t, err)

	task := pgqugo.Task{
		Kind:    successHandleTaskKind,
		Payload: "{}",
	}

	for i := 0; i < 3; i++ {
		key := strconv.Itoa(int(rand.Int64()))
		task.Key = &key

		err = q.CreateTask(ctx, task)
		require.NoError(t, err)
	}

	q.Start(ctx)
	time.Sleep(time.Second)
	q.Stop()

	rows, err := pool.Query(ctx,
		`SELECT id, kind, key, payload, status, attempts_left, attempts_elapsed,
					next_attempt_time, created_at, updated_at
			   FROM pgqueue 
			  WHERE kind = $1`,
		successHandleTaskKind)

	taskInfos, err := pgx.CollectRows(rows, pgx.RowToStructByPos[pgqugo.FullTaskInfo])
	require.NoError(t, err)

	for _, ti := range taskInfos {
		assert.Equal(t, successHandleTaskKind, ti.Kind)
		assert.NotNil(t, ti.Key)
		assert.Equal(t, "{}", ti.Payload)
		assert.Equal(t, "succeeded", ti.Status)
		assert.Equal(t, int16(2), ti.AttemptsLeft)
		assert.Equal(t, int16(1), ti.AttemptsElapsed)
		assert.Nil(t, ti.NextAttemptTime)
	}
}

func TestQueue_FailHandleTask_PGX(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	pool := newTestPGXPool(ctx, t)
	_, err := pool.Exec(ctx, `DELETE FROM pgqueue WHERE kind = $1`, failHandleTaskKind)
	require.NoError(t, err)

	h := failHandler{}
	kinds := pgqugo.TaskKinds{
		pgqugo.NewTaskKind(failHandleTaskKind, h, 3, 2, time.Millisecond*60, time.Millisecond*270),
	}

	q, err := pgqugo.New(adapter.NewPGX(pool), kinds)
	require.NoError(t, err)

	task := pgqugo.Task{
		Kind:    failHandleTaskKind,
		Payload: "{}",
	}

	for i := 0; i < 3; i++ {
		key := strconv.Itoa(int(rand.Int64()))
		task.Key = &key

		err = q.CreateTask(ctx, task)
		require.NoError(t, err)
	}
	q.Start(ctx)
	// check retrying

	time.Sleep(time.Millisecond * 500)
	rows, err := pool.Query(ctx,
		`SELECT id, kind, key, payload, status, attempts_left, attempts_elapsed,
					next_attempt_time, created_at, updated_at
			   FROM pgqueue 
			  WHERE kind = $1`,
		failHandleTaskKind)
	require.NoError(t, err)
	taskInfos, err := pgx.CollectRows(rows, pgx.RowToStructByPos[pgqugo.FullTaskInfo])
	require.NoError(t, err)

	for _, ti := range taskInfos {
		assert.Equal(t, failHandleTaskKind, ti.Kind)
		assert.NotNil(t, ti.Key)
		assert.Equal(t, "{}", ti.Payload)
		assert.Equal(t, "retry", ti.Status)
		assert.Equal(t, int16(1), ti.AttemptsLeft)
		assert.Equal(t, int16(2), ti.AttemptsElapsed)
		assert.NotNil(t, ti.NextAttemptTime)
	}

	time.Sleep(time.Millisecond * 500)
	q.Stop()

	// check final failed status
	rows, err = pool.Query(ctx,
		`SELECT id, kind, key, payload, status, attempts_left, attempts_elapsed,
					next_attempt_time, created_at, updated_at
			   FROM pgqueue 
			  WHERE kind = $1`,
		failHandleTaskKind)
	require.NoError(t, err)

	taskInfos, err = pgx.CollectRows(rows, pgx.RowToStructByPos[pgqugo.FullTaskInfo])
	require.NoError(t, err)

	for _, ti := range taskInfos {
		assert.Equal(t, failHandleTaskKind, ti.Kind)
		assert.NotNil(t, ti.Key)
		assert.Equal(t, "{}", ti.Payload)
		assert.Equal(t, "failed", ti.Status)
		assert.Equal(t, int16(0), ti.AttemptsLeft)
		assert.Equal(t, int16(3), ti.AttemptsElapsed)
		assert.Nil(t, ti.NextAttemptTime)
	}
}
