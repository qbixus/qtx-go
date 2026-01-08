package qtx

import (
	"context"
	"github.com/qbixus/qtx-go/internal"
)

// WithTransactionScope возвращает производный по отношению к ctx контекст с новой транзакционной зоной.
// Если не указано иное, то зона создается с опцией WithTxRequired.
//
// Возвращает результирующий контекст и complete- и dispose- функции для зоны.
func WithTransactionScope(ctx context.Context, opts ...ScopeOption) (
	newCtx context.Context, complete func() error, dispose func() error,
) {
	internal.Assert(ctx != nil, "#args: ctx")
	options := scopeOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	if options.createScope == nil {
		options.createScope = createRequiresScope
	}
	return options.createScope(ctx, &options)
}

func createTransactionScope(ctx context.Context, options *scopeOptions) (context.Context, func() error, func() error) {
	scope := transactionScope{tx: options.tx}
	ctx = WithTransaction(ctx, scope.tx)
	return ctx, scope.complete, scope.dispose
}

func createRequiresScope(ctx context.Context, options *scopeOptions) (context.Context, func() error, func() error) {
	if tx := CurrentTransaction(ctx); tx != nil {
		scope := transactionScope{tx: tx}
		return ctx, scope.complete, scope.dispose
	}

	scope := committableScope{}
	ctx = WithTransaction(ctx, &scope.tx)
	return ctx, scope.complete, scope.dispose
}

func createRequiresNewScope(ctx context.Context, options *scopeOptions) (context.Context, func() error, func() error) {
	scope := committableScope{}
	ctx = WithTransaction(ctx, &scope.tx)
	return ctx, scope.complete, scope.dispose
}

func createSuppressScope(ctx context.Context, options *scopeOptions) (context.Context, func() error, func() error) {
	scope := emptyScope{}
	ctx = WithTransaction(ctx, nil)
	return ctx, scope.complete, scope.dispose
}

// ---

type committableScope struct {
	tx         CommittableTransaction
	terminated bool
}

func (s *committableScope) dispose() error {
	if s.terminated {
		return nil
	}
	err := s.tx.Rollback(context.Background())
	s.terminated = true
	return err
}

func (s *committableScope) complete() error {
	if s.terminated {
		return ErrInvalidOperation
	}
	err := s.tx.Commit(context.Background())
	s.terminated = true
	return err
}

// ---

type transactionScope struct {
	tx         Transaction
	terminated bool
}

func (s *transactionScope) dispose() error {
	if s.terminated {
		return nil
	}
	err := s.tx.Rollback(context.Background())
	s.terminated = true
	return err
}

func (s *transactionScope) complete() error {
	if s.terminated {
		return ErrInvalidOperation
	}
	s.terminated = true
	return nil
}

// ---

type emptyScope struct {
	terminated bool
}

func (s *emptyScope) dispose() error {
	return nil
}

func (s *emptyScope) complete() error {
	if s.terminated {
		return ErrInvalidOperation
	}
	s.terminated = true
	return nil
}

// ---

type ScopeOption func(*scopeOptions)

// WithScopeTransaction создает зону с указанной транзакцией.
func WithScopeTransaction(tx Transaction) ScopeOption {
	internal.Assert(tx != nil, "#args")
	return func(options *scopeOptions) {
		options.tx = tx
		options.createScope = createTransactionScope
	}
}

// WithTxRequired создает зону либо с текущей транзакцией, либо с новой.
func WithTxRequired() ScopeOption {
	return func(options *scopeOptions) { options.createScope = createRequiresScope }
}

// WithRequiresNewTx создает зону с новой транзакцией.
func WithRequiresNewTx() ScopeOption {
	return func(options *scopeOptions) { options.createScope = createRequiresNewScope }
}

// WithSuppressTx создает зону без транзакции.
func WithSuppressTx() ScopeOption {
	return func(options *scopeOptions) { options.createScope = createSuppressScope }
}

type scopeOptions struct {
	tx          Transaction
	createScope func(context.Context, *scopeOptions) (context.Context, func() error, func() error)
}
