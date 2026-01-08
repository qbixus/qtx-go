package qtx

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestCommittableTransaction_Rollback(t *testing.T) {
	t.Run("Возвращает ошибку если транзакция уже зафиксирована", func(t *testing.T) {
		assert_ := assert.New(t)
		target := CommittableTransaction{}
		if err := target.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}

		// Act
		actErr := target.Rollback(t.Context())

		assert_.ErrorIs(actErr, ErrTxError)
	})

	t.Run("Возвращает ошибку если транзакция уже отменена", func(t *testing.T) {
		assert_ := assert.New(t)
		target := CommittableTransaction{}
		if err := target.Rollback(t.Context()); err != nil {
			t.Fatal(err)
		}

		// Act
		actErr := target.Rollback(t.Context())

		assert_.ErrorIs(actErr, ErrTxAborted)
	})

	t.Run("Отменяет все", func(t *testing.T) {
		assert_ := assert.New(t)
		var wg sync.WaitGroup
		vrm1 := NewMockEnlistmentNotification(t)
		vrm2 := NewMockEnlistmentNotification(t)
		drm := NewMockSinglePhaseNotification(t)

		target := CommittableTransaction{}
		if err := target.EnlistVolatile(vrm1); err != nil {
			t.Fatal(err)
		}
		if err := target.EnlistVolatile(vrm2); err != nil {
			t.Fatal(err)
		}
		if err := target.EnlistTheOnlyDurable(drm); err != nil {
			t.Fatal(err)
		}

		wg.Add(3)
		mock.InOrder(
			drm.EXPECT().Rollback(mock.Anything, mock.Anything).
				Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
				Once(),
			vrm1.EXPECT().Rollback(mock.Anything, mock.Anything).
				Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
				Once(),
			vrm2.EXPECT().Rollback(mock.Anything, mock.Anything).
				Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
				Once(),
		)

		// Act
		actErr := target.Rollback(t.Context())

		assert_.NoError(actErr)
		wg.Wait()
	})

	t.Run("Может применяться на фазе подготовки", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			assert_ := assert.New(t)
			var wg sync.WaitGroup
			vrm := NewMockEnlistmentNotification(t)

			target := CommittableTransaction{}
			if err := target.EnlistVolatile(vrm); err != nil {
				t.Fatal(err)
			}

			// Имея target в состоянии подготовки...
			var commErr error
			prep := vrm.EXPECT().Prepare(mock.Anything, mock.Anything).
				Run(func(ctx context.Context, enl PreparingEnlistment) { time.Sleep(1); enl.Prepared() }).
				Once()
			wg.Go(func() { commErr = target.Commit(t.Context()) })
			synctest.Wait()
			if target.status != txStatusPreparing {
				t.Fatal(target.status)
			}

			wg.Add(1)
			vrm.EXPECT().Rollback(mock.Anything, mock.Anything).
				Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
				NotBefore(prep).
				Once()

			// Act
			actErr := target.Rollback(t.Context())

			assert_.NoError(actErr)

			time.Sleep(1)
			wg.Wait()
			assert_.ErrorIs(commErr, ErrTxAborted)
		})
	})
}

func TestCommittableTransaction_Commit(t *testing.T) {
	t.Run("Возвращает ошибку если транзакция уже зафиксирована", func(t *testing.T) {
		assert_ := assert.New(t)
		target := CommittableTransaction{}
		if err := target.Commit(t.Context()); err != nil {
			t.Fatal(err)
		}

		// Act
		actErr := target.Commit(t.Context())

		assert_.ErrorIs(actErr, ErrTxError)
	})

	t.Run("Возвращает ошибку если транзакция уже отменена", func(t *testing.T) {
		assert_ := assert.New(t)
		target := CommittableTransaction{}
		if err := target.Rollback(t.Context()); err != nil {
			t.Fatal(err)
		}

		// Act
		actErr := target.Commit(t.Context())

		assert_.ErrorIs(actErr, ErrTxAborted)
	})

	t.Run("Фиксирует все", func(t *testing.T) {
		t.Run("Получая синхронные ответы", func(t *testing.T) {
			assert_ := assert.New(t)
			var wg sync.WaitGroup
			vrm1 := NewMockEnlistmentNotification(t)
			vrm2 := NewMockEnlistmentNotification(t)
			drm := NewMockSinglePhaseNotification(t)

			target := CommittableTransaction{}
			if err := target.EnlistVolatile(vrm1); err != nil {
				t.Fatal(err)
			}
			if err := target.EnlistVolatile(vrm2); err != nil {
				t.Fatal(err)
			}
			if err := target.EnlistTheOnlyDurable(drm); err != nil {
				t.Fatal(err)
			}

			wg.Add(2)
			mock.InOrder(
				vrm1.EXPECT().Prepare(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl PreparingEnlistment) { enl.Prepared() }).
					Once(),
				vrm2.EXPECT().Prepare(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl PreparingEnlistment) { enl.Prepared() }).
					Once(),
				drm.EXPECT().SinglePhaseCommit(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl SinglePhaseEnlistment) { enl.Committed() }).
					Once(),
				vrm1.EXPECT().Commit(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
					Once(),
				vrm2.EXPECT().Commit(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
					Once(),
			)

			// Act
			actErr := target.Commit(t.Context())

			assert_.NoError(actErr)
			wg.Wait()
		})

		t.Run("Получая асинхронные ответы", func(t *testing.T) {
			assert_ := assert.New(t)
			var wg sync.WaitGroup
			vrm1 := NewMockEnlistmentNotification(t)
			vrm2 := NewMockEnlistmentNotification(t)
			drm := NewMockSinglePhaseNotification(t)

			target := CommittableTransaction{}
			if err := target.EnlistVolatile(vrm1); err != nil {
				t.Fatal(err)
			}
			if err := target.EnlistVolatile(vrm2); err != nil {
				t.Fatal(err)
			}
			if err := target.EnlistTheOnlyDurable(drm); err != nil {
				t.Fatal(err)
			}

			wg.Add(2)
			mock.InOrder(
				vrm1.EXPECT().Prepare(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl PreparingEnlistment) { go enl.Prepared() }).
					Once(),
				vrm2.EXPECT().Prepare(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl PreparingEnlistment) { go enl.Prepared() }).
					Once(),
				drm.EXPECT().SinglePhaseCommit(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl SinglePhaseEnlistment) { go enl.Committed() }).
					Once(),
				vrm1.EXPECT().Commit(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { go func() { defer wg.Done(); enl.Done() }() }).
					Once(),
				vrm2.EXPECT().Commit(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { go func() { defer wg.Done(); enl.Done() }() }).
					Once(),
			)

			// Act
			actErr := target.Commit(t.Context())

			assert_.NoError(actErr)
			wg.Wait()
		})
	})

	t.Run("Отменяет все", func(t *testing.T) {
		t.Run("Получая асинхронную ошибку SPC TOD", func(t *testing.T) {
			assert_ := assert.New(t)
			var wg sync.WaitGroup
			vrm1 := NewMockEnlistmentNotification(t)
			vrm2 := NewMockEnlistmentNotification(t)
			drm := NewMockSinglePhaseNotification(t)
			theErr := errors.New("#THE_ERR")

			target := CommittableTransaction{}
			if err := target.EnlistVolatile(vrm1); err != nil {
				t.Fatal(err)
			}
			if err := target.EnlistVolatile(vrm2); err != nil {
				t.Fatal(err)
			}
			if err := target.EnlistTheOnlyDurable(drm); err != nil {
				t.Fatal(err)
			}

			wg.Add(3)
			mock.InOrder(
				vrm1.EXPECT().Prepare(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl PreparingEnlistment) { enl.Prepared() }).
					Once(),
				vrm2.EXPECT().Prepare(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl PreparingEnlistment) { enl.Prepared() }).
					Once(),
				drm.EXPECT().SinglePhaseCommit(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl SinglePhaseEnlistment) { go enl.Aborted(theErr) }).
					Once(),
				drm.EXPECT().Rollback(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
					Once(),
				vrm1.EXPECT().Rollback(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
					Once(),
				vrm2.EXPECT().Rollback(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
					Once(),
			)

			// Act
			actErr := target.Commit(t.Context())

			assert_.ErrorIs(actErr, ErrTxAborted)
			wg.Wait()
		})

		t.Run("Получая синхронную ошибку подготовки Volatile", func(t *testing.T) {
			assert_ := assert.New(t)
			var wg sync.WaitGroup
			vrm1 := NewMockEnlistmentNotification(t)
			vrm2 := NewMockEnlistmentNotification(t)
			drm := NewMockSinglePhaseNotification(t)
			theErr := errors.New("#THE_ERR")

			target := CommittableTransaction{}
			if err := target.EnlistVolatile(vrm1); err != nil {
				t.Fatal(err)
			}
			if err := target.EnlistVolatile(vrm2); err != nil {
				t.Fatal(err)
			}
			if err := target.EnlistTheOnlyDurable(drm); err != nil {
				t.Fatal(err)
			}

			wg.Add(3)
			mock.InOrder(
				vrm1.EXPECT().Prepare(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl PreparingEnlistment) { enl.Prepared() }).
					Once(),
				vrm2.EXPECT().Prepare(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl PreparingEnlistment) { enl.ForceRollback(theErr) }).
					Once(),
				drm.EXPECT().Rollback(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
					Once(),
				vrm1.EXPECT().Rollback(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
					Once(),
				vrm2.EXPECT().Rollback(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
					Once(),
			)

			// Act
			actErr := target.Commit(t.Context())

			assert_.ErrorIs(actErr, ErrTxAborted)
			wg.Wait()
		})
	})

	t.Run("Допускает вложенные Rollback на первой фазе", func(t *testing.T) {
		t.Run("Volatile, синхронно", func(t *testing.T) {
			assert_ := assert.New(t)
			var wg sync.WaitGroup
			vrm1 := NewMockEnlistmentNotification(t)
			vrm2 := NewMockEnlistmentNotification(t)
			drm := NewMockSinglePhaseNotification(t)

			target := CommittableTransaction{}
			if err := target.EnlistVolatile(vrm1); err != nil {
				t.Fatal(err)
			}
			if err := target.EnlistVolatile(vrm2); err != nil {
				t.Fatal(err)
			}
			if err := target.EnlistTheOnlyDurable(drm); err != nil {
				t.Fatal(err)
			}

			var rbErr error
			wg.Add(3)
			mock.InOrder(
				vrm1.EXPECT().Prepare(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl PreparingEnlistment) { enl.Prepared() }).
					Once(),
				vrm2.EXPECT().Prepare(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl PreparingEnlistment) {
						rbErr = target.Rollback(ctx)
						enl.Prepared()
					}).
					Once(),
				drm.EXPECT().Rollback(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
					Once(),
				vrm1.EXPECT().Rollback(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
					Once(),
				vrm2.EXPECT().Rollback(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
					Once(),
			)

			// Act
			actErr := target.Commit(t.Context())

			assert_.ErrorIs(actErr, ErrTxAborted)
			assert_.NoError(rbErr)
			wg.Wait()
		})

		t.Run("Volatile, асинхронно", func(t *testing.T) {
			assert_ := assert.New(t)
			var wg sync.WaitGroup
			vrm1 := NewMockEnlistmentNotification(t)
			vrm2 := NewMockEnlistmentNotification(t)
			drm := NewMockSinglePhaseNotification(t)

			target := CommittableTransaction{}
			if err := target.EnlistVolatile(vrm1); err != nil {
				t.Fatal(err)
			}
			if err := target.EnlistVolatile(vrm2); err != nil {
				t.Fatal(err)
			}
			if err := target.EnlistTheOnlyDurable(drm); err != nil {
				t.Fatal(err)
			}

			var rbErr error
			wg.Add(3)
			mock.InOrder(
				vrm1.EXPECT().Prepare(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl PreparingEnlistment) { enl.Prepared() }).
					Once(),
				vrm2.EXPECT().Prepare(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl PreparingEnlistment) {
						go func() {
							rbErr = target.Rollback(ctx)
							enl.Prepared()
						}()
					}).
					Once(),
				drm.EXPECT().Rollback(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
					Once(),
				vrm1.EXPECT().Rollback(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
					Once(),
				vrm2.EXPECT().Rollback(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
					Once(),
			)

			// Act
			actErr := target.Commit(t.Context())

			assert_.ErrorIs(actErr, ErrTxAborted)
			assert_.NoError(rbErr)
			wg.Wait()
		})
	})

	t.Run("Допускает вложенные присоединения на первой фазе", func(t *testing.T) {
		t.Run("Синхронное присоединение TOD", func(t *testing.T) {
			assert_ := assert.New(t)
			var wg sync.WaitGroup
			vrm1 := NewMockEnlistmentNotification(t)
			vrm2 := NewMockEnlistmentNotification(t)
			drm := NewMockSinglePhaseNotification(t)

			target := CommittableTransaction{}
			if err := target.EnlistVolatile(vrm1); err != nil {
				t.Fatal(err)
			}
			if err := target.EnlistVolatile(vrm2); err != nil {
				t.Fatal(err)
			}

			var enlErr error
			wg.Add(2)
			mock.InOrder(
				vrm1.EXPECT().Prepare(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl PreparingEnlistment) { enl.Prepared() }).
					Once(),
				vrm2.EXPECT().Prepare(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl PreparingEnlistment) {
						enlErr = target.EnlistTheOnlyDurable(drm)
						enl.Prepared()
					}).
					Once(),
				drm.EXPECT().SinglePhaseCommit(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl SinglePhaseEnlistment) { enl.Committed() }).
					Once(),
				vrm1.EXPECT().Commit(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
					Once(),
				vrm2.EXPECT().Commit(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
					Once(),
			)

			// Act
			actErr := target.Commit(t.Context())

			assert_.NoError(actErr)
			assert_.NoError(enlErr)
			wg.Wait()
		})

		t.Run("Асинхронное присоединение TOD", func(t *testing.T) {
			assert_ := assert.New(t)
			var wg sync.WaitGroup
			vrm1 := NewMockEnlistmentNotification(t)
			vrm2 := NewMockEnlistmentNotification(t)
			drm := NewMockSinglePhaseNotification(t)

			target := CommittableTransaction{}
			if err := target.EnlistVolatile(vrm1); err != nil {
				t.Fatal(err)
			}
			if err := target.EnlistVolatile(vrm2); err != nil {
				t.Fatal(err)
			}

			var enlErr error
			wg.Add(2)
			mock.InOrder(
				vrm1.EXPECT().Prepare(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl PreparingEnlistment) { enl.Prepared() }).
					Once(),
				vrm2.EXPECT().Prepare(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl PreparingEnlistment) {
						go func() {
							enlErr = target.EnlistTheOnlyDurable(drm)
							enl.Prepared()
						}()
					}).
					Once(),
				drm.EXPECT().SinglePhaseCommit(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl SinglePhaseEnlistment) { enl.Committed() }).
					Once(),
				vrm1.EXPECT().Commit(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
					Once(),
				vrm2.EXPECT().Commit(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
					Once(),
			)

			// Act
			actErr := target.Commit(t.Context())

			assert_.NoError(actErr)
			assert_.NoError(enlErr)
			wg.Wait()
		})

		t.Run("Синхронное присоединение Volatile", func(t *testing.T) {
			assert_ := assert.New(t)
			var wg sync.WaitGroup
			vrm1 := NewMockEnlistmentNotification(t)
			vrm2 := NewMockEnlistmentNotification(t)
			drm := NewMockSinglePhaseNotification(t)

			target := CommittableTransaction{}
			if err := target.EnlistVolatile(vrm1); err != nil {
				t.Fatal(err)
			}
			if err := target.EnlistTheOnlyDurable(drm); err != nil {
				t.Fatal(err)
			}

			var enlErr error
			wg.Add(2)
			mock.InOrder(
				vrm1.EXPECT().Prepare(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl PreparingEnlistment) {
						enlErr = target.EnlistVolatile(vrm2)
						enl.Prepared()
					}).
					Once(),
				vrm2.EXPECT().Prepare(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl PreparingEnlistment) { enl.Prepared() }).
					Once(),
				drm.EXPECT().SinglePhaseCommit(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl SinglePhaseEnlistment) { enl.Committed() }).
					Once(),
				vrm1.EXPECT().Commit(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
					Once(),
				vrm2.EXPECT().Commit(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, enl Enlistment) { defer wg.Done(); enl.Done() }).
					Once(),
			)

			// Act
			actErr := target.Commit(t.Context())

			assert_.NoError(actErr)
			assert_.NoError(enlErr)
			wg.Wait()
		})
	})
}
