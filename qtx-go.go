package qtx

import (
	"errors"
	"fmt"
)

var (
	ErrTxError          = errors.New("#TX_ILLEGAL_STATE")
	ErrTxAborted        = fmt.Errorf("#TX_ABORTED: %w", ErrTxError)
	ErrInvalidOperation = errors.New("#TX_INVALID_OPERATION")
)

type contextKey[T any] struct{}
