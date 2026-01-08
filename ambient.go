package qtx

import (
	"context"
)

func WithTransaction(ctx context.Context, tx Transaction) context.Context {
	return context.WithValue(ctx, contextKey[Transaction]{}, tx)
}

func CurrentTransaction(ctx context.Context) Transaction {
	tx, ok := ctx.Value(contextKey[Transaction]{}).(Transaction)
	if !ok {
		return nil
	}
	return tx
}
