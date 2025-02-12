package entity

import "time"

// FullTaskInfo queue task model
type FullTaskInfo struct {
	ID  int64
	Key *string

	CreatedAt       time.Time
	UpdatedAt       time.Time
	NextAttemptTime *time.Time

	Payload         string
	Status          string
	Kind            int16
	AttemptsLeft    int16
	AttemptsElapsed int16
}
