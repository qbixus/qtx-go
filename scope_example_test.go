package qtx

import (
	"context"
	"fmt"
)

func ExampleWithTransactionScope() {
	ctx, complete, dispose := WithTransactionScope(context.Background())
	defer dispose()

	tx := CurrentTransaction(ctx)
	if tx != nil {
		fmt.Printf("tx is not nil")
	}

	if err := complete(); err != nil {
		return
	}

	// Output:
	// tx is not nil
}
