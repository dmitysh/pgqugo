package main

import (
	"context"
	"flag"
	"log"
	"math/rand/v2"

	"github.com/dmitysh/pgqugo"
	"github.com/dmitysh/pgqugo/pkg/adapter"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pgDSN := flag.String("pg-dsn", "", "PostgreSQL connection string")
	numberOfTasks := flag.Int("tasks", 0, "Number of tasks to create")
	numberOfKinds := flag.Int("kinds", 1, "Number of kinds to create")

	flag.Parse()

	cfg, err := pgxpool.ParseConfig(*pgDSN)
	if err != nil {
		log.Fatal("can't init pgxpool:", err)
	}

	cfg.MaxConns = 2
	cfg.MinConns = 2

	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		log.Fatal("can't init pgxpool:", err)
	}

	err = p.Ping(ctx)
	if err != nil {
		log.Fatal("can't init ping postgres:", err)
	}

	kinds := pgqugo.TaskKinds{}
	for i := 1; i <= *numberOfKinds; i++ {
		kinds = append(kinds, pgqugo.NewTaskKind(int16(i), nil))
	}
	pgq := pgqugo.New(adapter.NewPGXv5(p), kinds)

	generateTasks(ctx, pgq, *numberOfTasks, *numberOfKinds)
}

func generateTasks(ctx context.Context, pgq *pgqugo.Queue, numberOfTasks int, numberOfKinds int) {
	errCount := 0
	for i := 0; i < numberOfTasks; i++ {
		key := generateRandomString(rand.IntN(100))
		pl := generateRandomString(rand.IntN(1000))

		err := pgq.CreateTask(ctx, pgqugo.Task{
			Kind:    int16(rand.IntN(numberOfKinds) + 1),
			Key:     &key,
			Payload: pl,
		})
		if err != nil {
			errCount++
		}
	}

	if errCount > 0 {
		log.Printf("ERRORS: %d\n", errCount)
	}
}

func generateRandomString(length int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	b := make([]byte, length)
	for i := range b {
		b[i] = letterBytes[rand.IntN(len(letterBytes))]
	}

	return string(b)
}
