package pgqugo

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/DmitySH/pgqugo/internal/entity"
	"github.com/DmitySH/pgqugo/internal/inerrors"
)

type cleaner struct {
	tk taskKind

	db DB
}

func (c cleaner) run(ctx context.Context) {
	t := time.NewTimer(time.Nanosecond)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			err := c.cleanTerminalTasks(ctx)
			if err != nil {
				log.Println(err)
			}

			t.Reset(c.tk.cleanerCfg.period)
		case <-ctx.Done():
			return
		}
	}
}

func (c cleaner) cleanTerminalTasks(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultDBTimeout)
	defer cancel()

	err := c.db.ExecuteJob(ctx, cleanerJob(c.tk.id), c.tk.cleanerCfg.period)
	if errors.Is(err, inerrors.ErrJobExecutionCancelled) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("can't execute job: %w", err)
	}

	err = c.db.DeleteTerminalTasks(ctx, entity.DeleteTerminalTasksParams{
		KindID: c.tk.id,
		Limit:  c.tk.cleanerCfg.limit,
		After:  c.tk.cleanerCfg.terminalTasksTTL,
	})
	if err != nil {
		return fmt.Errorf("can't delete terminal tasks: %w", err)
	}

	return nil
}

func cleanerJob(kindID int16) string {
	return fmt.Sprintf("cleaner_%d", kindID)
}
