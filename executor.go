package pgqugo

import (
	"context"
	"fmt"
	"log"
)

type executor struct {
	tk taskKind

	db DB
}

func (e executor) execute(ctx context.Context, task FullTaskInfo) (empty, error) {
	pt := ProcessingTask{
		Task: Task{
			Kind:    task.Kind,
			Key:     task.Key,
			Payload: task.Payload,
		},
		AttemptsElapsed: task.AttemptsElapsed,
		AttemptsLeft:    task.AttemptsLeft,
	}

	handlerErr := e.tk.handler.HandleTask(ctx, pt)
	if handlerErr != nil {
		log.Println(handlerErr)

		if task.AttemptsLeft == 0 {
			err := e.db.FailTask(ctx, task.ID)
			if err != nil {
				return empty{}, fmt.Errorf("can't fail task: %w", err)
			}
		} else {
			err := e.db.SoftFailTask(ctx, task.ID)
			if err != nil {
				return empty{}, fmt.Errorf("can't soft fail task: %w", err)
			}
		}
	} else {
		err := e.db.SucceedTask(ctx, task.ID)
		if err != nil {
			return empty{}, fmt.Errorf("can't succeed task: %w", err)
		}
	}

	return empty{}, nil
}
