package pgqugo

import (
	"context"
	"log"
	"time"

	"github.com/cenkalti/backoff"
)

func dbRetry(ctx context.Context, opName string, op func() error) error {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = time.Second
	b.MaxInterval = time.Second * 5
	b.MaxElapsedTime = time.Second * 30

	notifier := func(err error, t time.Duration) {
		log.Printf("retrying %s in %v: %v\n", opName, t, err)
	}

	return backoff.RetryNotify(op, backoff.WithContext(b, ctx), notifier)
}
