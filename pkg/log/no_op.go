package log

import (
	"context"
)

type NoOp struct{}

func NewNoOp() NoOp {
	return NoOp{}
}

func (s NoOp) Warnf(_ context.Context, _ string, _ ...any)  {}
func (s NoOp) Errorf(_ context.Context, _ string, _ ...any) {}
