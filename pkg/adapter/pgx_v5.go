package adapter

import (
	"context"
	"errors"
	"fmt"

	"github.com/dmitysh/pgqugo"
	"github.com/dmitysh/pgqugo/internal/entity"
	"github.com/dmitysh/pgqugo/internal/inerrors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGXv5 database adapter for queue usage with the jackc/pgx/v5 package
type PGXv5 struct {
	pool *pgxpool.Pool
}

// NewPGXv5 constructor of PGXv5
func NewPGXv5(pool *pgxpool.Pool) *PGXv5 {
	return &PGXv5{
		pool: pool,
	}
}

type pgxV5Executor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// CreateTask creates a task in the queue with the status pgqugo.TaskStatusNew
//
// Task creation can be performed in a transaction using InjectPGXv5Tx
func (p *PGXv5) CreateTask(ctx context.Context, task entity.FullTaskInfo) error {
	_, err := p.executor(ctx).Exec(ctx, createTaskQuery, task.Kind, task.Key, task.Payload, task.AttemptsLeft)
	if err != nil {
		return err
	}

	return nil
}

// GetPendingTasks returns tasks that are ready for execution and switches it to the status pgqugo.TaskStatusInProgress
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

// SucceedTask marks the task as successfully completed and switches it to the status pgqugo.TaskStatusSuccess.
//
// Injects the transaction into ctx.
// This way, you can add database operations to the same transaction, which marks the task as completed successfully.
// In case of an error inside the cb, it is assumed that the entire task has been completed unsuccessfully with the appropriate queue response to this
func (p *PGXv5) SucceedTask(ctx context.Context, taskID int64, cb pgqugo.Callback) (err error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("can't begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
				err = fmt.Errorf("can't rollback tx: %w, initial error: %w", rollbackErr, err)
			}
		} else {
			if commitErr := tx.Commit(ctx); commitErr != nil {
				err = fmt.Errorf("can't commit tx: %w", commitErr)
			}
		}
	}()

	ctx = InjectPGXv5Tx(ctx, tx)

	if cb != nil {
		err = cb(ctx)
		if err != nil {
			return fmt.Errorf("%w: %w", inerrors.ErrCallbackFailed, err)
		}
	}

	_, err = p.executor(ctx).Exec(ctx, succeedTaskQuery, taskID)
	if err != nil {
		return err
	}

	return nil
}

// SoftFailTask marks the task as unsuccessfully completed and puts it in the status pgqugo.TaskStatusRetry
//
// It is called only for tasks that still have attempts to complete
func (p *PGXv5) SoftFailTask(ctx context.Context, params entity.SoftFailTasksParams) error {
	_, err := p.pool.Exec(ctx, softFailTaskQuery, params.TaskID, params.Delay)
	if err != nil {
		return err
	}

	return nil
}

// FailTask marks the task as unsuccessfully completed and puts it in the status pgqugo.TaskStatusFailed
//
// It is called only for tasks that does not have any attempts left
func (p *PGXv5) FailTask(ctx context.Context, taskID int64) error {
	_, err := p.pool.Exec(ctx, failTaskQuery, taskID)
	if err != nil {
		return err
	}

	return nil
}

// DeleteTerminalTasks deletes tasks in terminal statuses
func (p *PGXv5) DeleteTerminalTasks(ctx context.Context, params entity.DeleteTerminalTasksParams) error {
	_, err := p.pool.Exec(ctx, cleanTerminalTasksQuery, params.KindID, params.After, params.Limit)
	if err != nil {
		return err
	}

	return nil
}

// RegisterJob registers an internal queue job
func (p *PGXv5) RegisterJob(ctx context.Context, job string) error {
	_, err := p.pool.Exec(ctx, registerJobsQuery, job)
	if err != nil {
		return err
	}

	return nil
}

// ExecuteJob performs an internal queue task
//
// In the case when the task was completed not so long ago, returns inerrors.ErrJobExecutionCancelled
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

func (p *PGXv5) executor(ctx context.Context) pgxV5Executor {
	if tx := ExtractPGXv5Tx(ctx); tx != nil {
		return tx
	}
	return p.pool
}

// InjectPGXv5Tx adds a transaction to ctx
//
// Can be used for PGXv5.CreateTask and callbacks
func InjectPGXv5Tx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, entity.TxKey{}, tx)
}

// ExtractPGXv5Tx allows to get a transaction from inside the callback
func ExtractPGXv5Tx(ctx context.Context) pgx.Tx {
	if ctx == nil {
		return nil
	}
	if tx, ok := ctx.Value(entity.TxKey{}).(pgx.Tx); ok {
		return tx
	}

	return nil
}
