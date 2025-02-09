package adapter

import (
	"context"
	"errors"
	"github.com/dmitysh/pgqugo/internal/entity"
	"github.com/dmitysh/pgqugo/internal/inerrors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type executor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

type txKey struct{}

type PGXv5 struct {
	pool *pgxpool.Pool
}

func NewPGXv5(pool *pgxpool.Pool) *PGXv5 {
	return &PGXv5{
		pool: pool,
	}
}

func (p *PGXv5) CreateTask(ctx context.Context, task entity.FullTaskInfo) error {
	var ex executor

	if tx := extractPGXv5Tx(ctx); tx != nil {
		ex = tx
	} else {
		ex = p.pool
	}

	_, err := ex.Exec(ctx, createTaskQuery, task.Kind, task.Key, task.Payload, task.AttemptsLeft)
	if err != nil {
		return err
	}

	return nil
}

func (p *PGXv5) GetPendingTasks(ctx context.Context, params entity.GetPendingTasksParams) ([]entity.FullTaskInfo, error) {
	rows, err := p.pool.Query(ctx, getPendingTasksQuery, params.KindID, params.BatchSize, params.AttemptDelay)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	taskInfos, err := pgx.CollectRows(rows, pgx.RowToStructByPos[entity.FullTaskInfo])
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

func (p *PGXv5) SoftFailTask(ctx context.Context, params entity.SoftFailTasksParams) error {
	_, err := p.pool.Exec(ctx, softFailTaskQuery, params.TaskID, params.Delay)
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

func (p *PGXv5) DeleteTerminalTasks(ctx context.Context, params entity.DeleteTerminalTasksParams) error {
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

func (p *PGXv5) ExecuteJob(ctx context.Context, params entity.ExecuteJobParams) error {
	var ok bool
	err := p.pool.QueryRow(ctx, executeJobQuery, params.Job, params.JobPeriod).Scan(&ok)
	if errors.Is(err, pgx.ErrNoRows) {
		return inerrors.ErrJobExecutionCancelled
	}
	if err != nil {
		return err
	}

	return nil
}

func InjectPGXv5Tx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

func extractPGXv5Tx(ctx context.Context) pgx.Tx {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return tx
	}

	return nil
}
