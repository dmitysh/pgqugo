package log

import (
	"context"
)

// NoOp no-op logger
type NoOp struct{}

// NewNoOp NoOp constructor
func NewNoOp() NoOp {
	return NoOp{}
}

// Warnf no-op
func (s NoOp) Warnf(_ context.Context, _ string, _ ...any) {}

// Errorf no-op
func (s NoOp) Errorf(_ context.Context, _ string, _ ...any) {}
