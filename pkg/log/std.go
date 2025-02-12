package log

import (
	"context"
	"fmt"
	"log/slog"
)

// STDLogger logger based on standard log/slog package
type STDLogger struct {
	l *slog.Logger
}

// NewSTDLogger STDLogger constructor
func NewSTDLogger() STDLogger {
	return STDLogger{
		l: slog.Default(),
	}
}

// Warnf slog's formatted WarnContext
func (s STDLogger) Warnf(ctx context.Context, format string, args ...any) {
	s.l.WarnContext(ctx, fmt.Sprintf(format, args...))
}

// Errorf slog's formatted ErrorContext
func (s STDLogger) Errorf(ctx context.Context, format string, args ...any) {
	s.l.ErrorContext(ctx, fmt.Sprintf(format, args...))
}
