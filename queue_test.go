package pgqugo_test

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/DmitySH/pgqugo"
	"github.com/DmitySH/pgqugo/pkg/adapter"
)

type printHandler struct {
}

func (h printHandler) HandleTask(_ context.Context, task pgqugo.ProcessingTask) error {
	log.Printf("%+v", task)
	return nil
}

func newTestPGX(t *testing.T) *adapter.PGXv5 {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	dsn := "postgresql://postgres:postgres@localhost:5490/postgres"
	p, err := adapter.NewPGX(ctx, dsn)

	if err != nil {
		t.Fatalf("can't create pgx adapter: %v", err)
	}

	return p
}

func TestQueue_SuccessHandleTask_PGX(t *testing.T) {
	h := printHandler{}
	kinds := pgqugo.TaskKinds{
		pgqugo.NewTaskKind(2, h, 3, time.Millisecond*100, time.Minute),
	}
	q, err := pgqugo.New(newTestPGX(t), kinds)
	if err != nil {
		t.Errorf("can't create queue: %v", err)
	}

	ctx := context.Background()

	task := pgqugo.Task{
		Kind:    2,
		Key:     nil,
		Payload: "{}",
	}

	err = q.CreateTask(ctx, task)
	if err != nil {
		t.Fatalf("can't create task: %v", err)
	}
	err = q.CreateTask(ctx, task)
	if err != nil {
		t.Fatalf("can't create task: %v", err)
	}
	err = q.CreateTask(ctx, task)
	if err != nil {
		t.Fatalf("can't create task: %v", err)
	}

	q.Start(ctx)
	time.Sleep(time.Second)
	q.Stop()
}
