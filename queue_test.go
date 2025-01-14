package pgqugo_test

import (
	"context"
	"log"
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

const (
	testPostgresDSN = "postgresql://postgres:postgres@localhost:5490/postgres"
)

const (
	successHandleTasksKind int16 = 1
)

type printHandler struct{}

func (h printHandler) HandleTask(_ context.Context, task pgqugo.ProcessingTask) error {
	log.Printf("%+v", task)
	return nil
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
	_, err := pool.Exec(ctx, `DELETE FROM pgqueue WHERE kind = $1`, successHandleTasksKind)
	require.NoError(t, err)

	h := printHandler{}
	kinds := pgqugo.TaskKinds{
		pgqugo.NewTaskKind(successHandleTasksKind, h, 3, time.Millisecond*100, time.Minute),
	}

	q, err := pgqugo.New(adapter.NewPGX(pool), kinds)
	require.NoError(t, err)

	task := pgqugo.Task{
		Kind:    successHandleTasksKind,
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
		successHandleTasksKind)

	taskInfos, err := pgx.CollectRows(rows, pgx.RowToStructByPos[pgqugo.FullTaskInfo])
	require.NoError(t, err)

	for _, ti := range taskInfos {
		assert.Equal(t, successHandleTasksKind, ti.Kind)
		assert.NotNil(t, ti.Key)
		assert.Equal(t, "{}", ti.Payload)
		assert.Equal(t, "succeeded", ti.Status)
		assert.Equal(t, int16(2), ti.AttemptsLeft)
		assert.Equal(t, int16(1), ti.AttemptsElapsed)
		assert.Nil(t, ti.NextAttemptTime)
	}
}
