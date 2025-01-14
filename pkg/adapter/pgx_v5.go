package adapter

import (
	"context"
	"fmt"
	"github.com/DmitySH/pgqugo"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

type PGXv5 struct {
	pool *pgxpool.Pool
}

func NewPGX(ctx context.Context, dsn string) (*PGXv5, error) {
	p, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("can't create pgx pool: %w", err)
	}

	err = p.Ping(ctx)
	if err != nil {
		return nil, fmt.Errorf("can't ping db: %w", err)
	}

	return &PGXv5{
		pool: p,
	}, nil
}

func (p *PGXv5) CreateTask(ctx context.Context, task pgqugo.FullTaskInfo) error {
	q := `INSERT INTO pgqueue (kind, key, payload, attempts_left) VALUES ($1, $2, $3, $4)`

	_, err := p.pool.Exec(ctx, q, task.Kind, task.Key, task.Payload, task.AttemptsLeft)
	if err != nil {
		return err
	}

	return nil
}

func (p *PGXv5) GetWaitingTasks(ctx context.Context, kind int16, delay time.Duration) ([]pgqugo.FullTaskInfo, error) {
	q := `WITH selected AS (
			   SELECT id
			     FROM pgqueue
			    WHERE (status = 'new' OR (status IN ('retry', 'in_progress') AND next_attempt_time < now()))
				  AND kind = $1
			    ORDER BY created_at
			    LIMIT 1
			   FOR UPDATE SKIP LOCKED
			  )
		UPDATE pgqueue
		   SET status = 'in_progress', next_attempt_time = now()+$2::interval, updated_at = now()
		 WHERE id IN (SELECT id FROM selected)
	 RETURNING id, kind, key, payload, status, attempts_left-1, attempts_elapsed+1,
			   next_attempt_time, created_at, updated_at`

	rows, err := p.pool.Query(ctx, q, kind, delay)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	taskInfos, err := pgx.CollectRows(rows, pgx.RowToStructByPos[pgqugo.FullTaskInfo])
	if err != nil {
		return nil, err
	}

	return taskInfos, nil
}

func (p *PGXv5) SucceedTasks(ctx context.Context, taskIDs []int64) error {
	q := `UPDATE pgqueue
		     SET status = 'succeeded', attempts_left = attempts_left-1, 
		         attempts_elapsed = attempts_elapsed+1, next_attempt_time = null,
		         updated_at = now()
		   WHERE id = ANY($1)
		     AND status = 'in_progress'`

	_, err := p.pool.Exec(ctx, q, taskIDs)
	if err != nil {
		return err
	}

	return nil
}

func (p *PGXv5) SoftFailTasks(ctx context.Context, taskIDs []int64) error {
	q := `UPDATE pgqueue
		     SET status = 'retry', attempts_left = attempts_left-1, 
		         attempts_elapsed = attempts_elapsed+1, updated_at = now()
		   WHERE id = ANY($1)
		     AND status = 'in_progress'`

	_, err := p.pool.Exec(ctx, q, taskIDs)
	if err != nil {
		return err
	}

	return nil
}

func (p *PGXv5) FailTasks(ctx context.Context, taskIDs []int64) error {
	q := `UPDATE pgqueue
		     SET status = 'failed', attempts_left = 0, 
		         attempts_elapsed = attempts_elapsed+1, next_attempt_time = null,
		         updated_at = now()
		   WHERE id = ANY($1)
		     AND status = 'in_progress'`

	_, err := p.pool.Exec(ctx, q, taskIDs)
	if err != nil {
		return err
	}

	return nil
}
