package entity

import "time"

type FullTaskInfo struct {
	ID              int64
	Kind            int16
	Key             *string
	Payload         string
	Status          string
	AttemptsLeft    int16
	AttemptsElapsed int16
	NextAttemptTime *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}
