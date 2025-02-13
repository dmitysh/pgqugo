package pgqugo

import (
	"context"
	"fmt"
	"time"

	"github.com/dmitysh/pgqugo/internal/entity"
	"github.com/dmitysh/wopo"
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
	ctx := context.Background()

	t := time.NewTimer(f.tk.fetchPeriod())
	defer t.Stop()

	f.wp.Start()
	for {
		select {
		case <-t.C:
			err := f.fetchAndPushTasks(ctx)
			if err != nil {
				f.tk.logger.Errorf(ctx, "[%d] failed to fetch and push tasks: %v", f.tk.id, err)
			}
			t.Reset(f.tk.fetchPeriod())
		case <-stopCh:
			f.wp.Stop()
			return
		}
	}
}

func (f fetcher) fetchAndPushTasks(ctx context.Context) error {
	fetchCtx, fetchCancel := context.WithTimeout(ctx, defaultDBTimeout)
	defer fetchCancel()

	const overAttemptTimeoutDuration = time.Minute
	tasks, err := f.db.GetPendingTasks(fetchCtx, entity.GetPendingTasksParams{
		KindID:       f.tk.id,
		BatchSize:    f.tk.batchSize,
		AttemptDelay: f.tk.attemptTimeout + overAttemptTimeoutDuration,
	})
	if err != nil {
		return fmt.Errorf("can't fetch tasks: %w", err)
	}
	f.tk.statsCollector.AddInProgressTasks(len(tasks))

	for i := 0; i < len(tasks); i++ {
		f.wp.PushTask(ctx, tasks[i])
	}

	return nil
}
