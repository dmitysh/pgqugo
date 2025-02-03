package pgqugo

import (
	"context"
	"fmt"
	"github.com/DmitySH/pgqugo/internal/entity"
	"log"
	"runtime/debug"
)

type executor struct {
	tk taskKind

	db DB
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
	}

	taskCtx, taskCancel := context.WithTimeout(ctx, e.tk.attemptTimeout)
	defer taskCancel()
	handlerErr := e.safeHandle(taskCtx, pt)

	dbCtx, cancelDbCtx := context.WithTimeout(ctx, defaultDBTimeout)
	defer cancelDbCtx()

	if handlerErr != nil {
		log.Println(handlerErr)

		if task.AttemptsLeft == 0 {
			err := dbRetry(ctx, "FailTask", func() error { return e.db.FailTask(dbCtx, task.ID) })
			if err != nil {
				return empty{}, fmt.Errorf("can't fail task: %w", err)
			}
		} else {
			err := dbRetry(ctx, "SoftFailTask", func() error { return e.db.SoftFailTask(dbCtx, task.ID, e.tk.attemptDelayer(task.AttemptsElapsed)) })
			if err != nil {
				return empty{}, fmt.Errorf("can't soft fail task: %w", err)
			}
		}
	} else {
		err := dbRetry(ctx, "SucceedTask", func() error { return e.db.SucceedTask(dbCtx, task.ID) })
		if err != nil {
			return empty{}, fmt.Errorf("can't succeed task: %w", err)
		}
	}

	return empty{}, nil
}

func (e executor) safeHandle(ctx context.Context, pt ProcessingTask) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in handler: %s", string(debug.Stack()))
		}
		return
	}()

	return e.tk.handler.HandleTask(ctx, pt)
}
