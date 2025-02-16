package pgqugo

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/dmitysh/pgqugo/internal/entity"
	"github.com/dmitysh/pgqugo/internal/inerrors"
)

type executor struct {
	tk taskKind

	db DB
}

type callbacks struct {
	OnSuccess Callback
}

// Callback function being executed on task status change
type Callback func(ctx context.Context) error

// ProcessingTask used to operate with task in queue handlers
type ProcessingTask struct {
	Task
	AttemptsLeft    int16
	AttemptsElapsed int16

	callbacks *callbacks
}

// OnSuccess sets a callback running in a transaction that moves the task to the pgqugo.TaskStatusSuccess
func (p ProcessingTask) OnSuccess(cb Callback) {
	p.callbacks.OnSuccess = cb
}

func (e executor) execute(ctx context.Context, task entity.FullTaskInfo) (empty, error) {
	pt := ProcessingTask{
		Task: Task{
			Kind:    task.Kind,
			Key:     task.Key,
			Payload: task.Payload,
		},
		AttemptsElapsed: task.AttemptsElapsed,
		AttemptsLeft:    task.AttemptsLeft,
		callbacks:       &callbacks{},
	}

	taskCtx, taskCancel := context.WithTimeout(ctx, e.tk.attemptTimeout)
	defer taskCancel()
	handlerErr := e.safeHandle(taskCtx, pt)

	dbCtx, cancelDbCtx := context.WithTimeout(ctx, defaultDBTimeout)
	defer cancelDbCtx()

	if handlerErr == nil {
		err := dbRetry(ctx, "SucceedTask", func() error {
			succeedTaskErr := e.db.SucceedTask(dbCtx, task.ID, pt.callbacks.OnSuccess)
			if errors.Is(succeedTaskErr, inerrors.ErrCallbackFailed) {
				// OnSuccess callback error is considered the same as if the task's handler had failed
				handlerErr = succeedTaskErr
				return nil
			}
			return succeedTaskErr
		}, e.tk.logger)
		if err != nil {
			return empty{}, fmt.Errorf("can't succeed task: %w", err)
		}

		// Double check because here can be callback's error now
		if handlerErr == nil {
			e.tk.statsCollector.IncSuccessTasks()
		}
	}

	if handlerErr != nil {
		if task.AttemptsLeft == 0 {
			err := dbRetry(ctx, "FailTask", func() error { return e.db.FailTask(dbCtx, task.ID) }, e.tk.logger)
			if err != nil {
				return empty{}, fmt.Errorf("can't fail task: %w", err)
			}

			e.tk.logger.Errorf(ctx, "[%d] task (%d) has no attempts left, last error: %v", e.tk.id, task.ID, handlerErr)
			e.tk.statsCollector.IncFailedTasks()
		} else {
			err := dbRetry(ctx, "SoftFailTask", func() error {
				return e.db.SoftFailTask(dbCtx, entity.SoftFailTasksParams{
					TaskID: task.ID,
					Delay:  e.tk.attemptDelayer(task.AttemptsElapsed),
				})
			}, e.tk.logger)
			if err != nil {
				return empty{}, fmt.Errorf("can't soft fail task: %w", err)
			}

			e.tk.logger.Warnf(ctx, "[%d] task (%d) failed, error: %v", e.tk.id, task.ID, handlerErr)
			e.tk.statsCollector.IncSoftFailedTasks()
		}
	}

	return empty{}, nil
}

func (e executor) safeHandle(ctx context.Context, pt ProcessingTask) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in handler: %s", string(debug.Stack()))
		}
	}()

	return e.tk.handler.HandleTask(ctx, pt)
}
