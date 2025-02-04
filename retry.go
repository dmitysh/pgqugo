package pgqugo

import (
	"context"
	"time"

	"github.com/cenkalti/backoff"
)

func dbRetry(ctx context.Context, opName string, op func() error, logger Logger) error {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = time.Second
	b.MaxInterval = time.Second * 5
	b.MaxElapsedTime = time.Second * 30

	notifier := func(err error, _ time.Duration) {
		logger.Errorf(ctx, "[db] retrying %s: %v", opName, err)
	}

	return backoff.RetryNotify(op, backoff.WithContext(b, ctx), notifier)
}
