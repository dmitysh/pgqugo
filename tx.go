package pgqugo

import (
	"context"

	"github.com/DmitySH/pgqugo/internal/entity"
)

type Transactor interface {
	CreateTaskTx(ctx context.Context, task entity.FullTaskInfo) error
}
