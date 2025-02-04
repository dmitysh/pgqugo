package pgqugo

import (
	"context"
	"fmt"
	"github.com/DmitySH/pgqugo/internal/entity"
	"github.com/DmitySH/wopo"
	"log"
	"time"
)

type fetcher struct {
	tk taskKind

	wp *wopo.Pool[entity.FullTaskInfo, empty]
	db DB
}

func newFetcher(tk taskKind, db DB) fetcher {
	const wpBufferSizeToNumOfWorkersFactor = 2
	return fetcher{
		tk: tk,
		wp: wopo.NewPool(
			executor{tk: tk, db: db}.execute,
			wopo.WithWorkerCount[entity.FullTaskInfo, empty](tk.workerCount),
			wopo.WithTaskBufferSize[entity.FullTaskInfo, empty](tk.workerCount*wpBufferSizeToNumOfWorkersFactor),
			wopo.WithResultBufferSize[entity.FullTaskInfo, empty](-1),
		),
		db: db,
	}
}

func (f fetcher) run(stopCh <-chan empty) {
	t := time.NewTimer(f.tk.fetchDelayer())
	defer t.Stop()

	f.wp.Start()
	for {
		select {
		case <-stopCh:
			f.wp.Stop()
			return
		case <-t.C:
			err := f.fetchAndPushTasks(context.Background())
			if err != nil {
				log.Println(err)
			}
			t.Reset(f.tk.fetchDelayer())
		}
	}
}

func (f fetcher) fetchAndPushTasks(ctx context.Context) error {
	fetchCtx, fetchCancel := context.WithTimeout(ctx, defaultDBTimeout)
	defer fetchCancel()

	const overAttemptTimeoutTime = time.Minute
	tasks, err := f.db.GetWaitingTasks(fetchCtx, entity.GetWaitingTasksParams{
		KindID:       f.tk.id,
		BatchSize:    f.tk.batchSize,
		AttemptDelay: f.tk.attemptTimeout + overAttemptTimeoutTime,
	})
	if err != nil {
		return fmt.Errorf("can't fetch tasks: %w", err)
	}

	for i := 0; i < len(tasks); i++ {
		f.wp.PushTask(ctx, tasks[i])
	}

	return nil
}
