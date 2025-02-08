package pgqugo

import (
	"context"

	"github.com/dmitysh/pgqugo/internal/entity"
)

type Transactor interface {
	CreateTaskTx(ctx context.Context, task entity.FullTaskInfo) error
}
