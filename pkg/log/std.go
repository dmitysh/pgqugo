package log

import (
	"context"
	"fmt"
	"log/slog"
)

type STDLogger struct {
	l *slog.Logger
}

func NewSTDLogger() STDLogger {
	return STDLogger{
		l: slog.Default(),
	}
}

func (s STDLogger) Warnf(ctx context.Context, format string, args ...any) {
	s.l.WarnContext(ctx, fmt.Sprintf(format, args...))
}
func (s STDLogger) Errorf(ctx context.Context, format string, args ...any) {
	s.l.ErrorContext(ctx, fmt.Sprintf(format, args...))

}
