package adapter

import (
	"context"
	"errors"
	"time"

	"github.com/DmitySH/pgqugo"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PGXv5 struct {
	pool *pgxpool.Pool
}

func NewPGX(pool *pgxpool.Pool) *PGXv5 {
	return &PGXv5{
		pool: pool,
	}
}

func (p *PGXv5) CreateTask(ctx context.Context, task pgqugo.FullTaskInfo) error {
	_, err := p.pool.Exec(ctx, createTaskQuery, task.Kind, task.Key, task.Payload, task.AttemptsLeft)
	if err != nil {
		return err
	}

	return nil
}

func (p *PGXv5) GetWaitingTasks(ctx context.Context, params pgqugo.GetWaitingTasksParams) ([]pgqugo.FullTaskInfo, error) {
	rows, err := p.pool.Query(ctx, getWaitingTasksQuery, params.KindID, params.BatchSize, params.AttemptsInterval)
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

func (p *PGXv5) SucceedTask(ctx context.Context, taskID int64) error {
	_, err := p.pool.Exec(ctx, succeedTaskQuery, taskID)
	if err != nil {
		return err
	}

	return nil
}

func (p *PGXv5) SoftFailTask(ctx context.Context, taskID int64) error {
	_, err := p.pool.Exec(ctx, softFailTaskQuery, taskID)
	if err != nil {
		return err
	}

	return nil
}

func (p *PGXv5) FailTask(ctx context.Context, taskID int64) error {
	_, err := p.pool.Exec(ctx, failTaskQuery, taskID)
	if err != nil {
		return err
	}

	return nil
}

func (p *PGXv5) GetTask(ctx context.Context, kind int16, delay time.Duration) ([]pgqugo.FullTaskInfo, error) {
	rows, err := p.pool.Query(ctx, getWaitingTasksQuery, kind, delay)
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

func (p *PGXv5) DeleteTerminalTasks(ctx context.Context, params pgqugo.DeleteTerminalTasksParams) error {
	_, err := p.pool.Exec(ctx, cleanTerminalTasksQuery, params.KindID, params.After, params.Limit)
	if err != nil {
		return err
	}

	return nil
}

func (p *PGXv5) RegisterJob(ctx context.Context, job string) error {
	_, err := p.pool.Exec(ctx, registerJobsQuery, job)
	if err != nil {
		return err
	}

	return nil
}

func (p *PGXv5) ExecuteJob(ctx context.Context, jobName string, jobPeriod time.Duration) error {
	var ok bool
	err := p.pool.QueryRow(ctx, executeJobQuery, jobName, jobPeriod).Scan(&ok)
	if errors.Is(err, pgx.ErrNoRows) {
		return pgqugo.ErrJobExecutionCancelled
	}
	if err != nil {
		return err
	}

	return nil
}
