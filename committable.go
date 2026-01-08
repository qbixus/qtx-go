package qtx

import (
	"context"
	"github.com/qbixus/qtx-go/internal"
	"sync"
)

// CommittableTransaction - локальная транзакция [Transaction], изменения в которой могут быть зафиксированы.
type CommittableTransaction struct {
	mu     sync.Mutex
	status txStatus
	tod    SinglePhaseNotification  // The Only Durable TRM.
	vrms   []EnlistmentNotification // Volatile TRM-s.

	// Для исключения конкурирующих друг с другом Commit и Rollback, в дополнение к mu
	ctlMu sync.Mutex
}

// EnlistTheOnlyDurable реализует [Transaction.EnlistTheOnlyDurable].
func (tx *CommittableTransaction) EnlistTheOnlyDurable(drm SinglePhaseNotification) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.tod != nil {
		return ErrTxError
	}

	if !(tx.status == txStatusActive || tx.isPreparing()) {
		return ErrTxError
	}
	tx.tod = drm
	return nil
}

// EnlistVolatile реализует [Transaction.EnlistVolatile].
func (tx *CommittableTransaction) EnlistVolatile(vrm EnlistmentNotification) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if !(tx.status == txStatusActive || tx.isPreparing()) {
		return ErrTxError
	}
	tx.vrms = append(tx.vrms, vrm)
	return nil
}

// Commit фиксирует изменения в транзакции.
// Фиксация изменений выполняется поэтапно: 1) фаза подготовки 2PC; 2) фиксация SPC; 3) фаза фиксации или отмены
// 2PC, включая отмену SPC.
// Блокируется на все время выполнения фиксации изменений за исключением обработки ответов на последнем этапе - она
// всегда выполняется конкурентно и может завершиться уже после завершения вызова Commit.
// Может использоваться конкурентно.
// Допускает вложенное использование Rollback, EnlistTheOnlyDurable и EnlistVolatile на фазе подготовки 2PC.
//
// Возвращает nil если изменения зафиксированы, ErrTxAborted если изменения отменены или были отменены ранее, и
// ErrTxError если изменения были зафиксированы ранее.
func (tx *CommittableTransaction) Commit(ctx context.Context) error {
	tx.ctlMu.Lock()
	defer tx.ctlMu.Unlock()

	tx.mu.Lock()

	// ... т.к. tx.ctlMu исключает конкурирующие вызовы Commit и Rollback
	internal.Assert(tx.isTerminated() || tx.status == txStatusActive)

	// Проверяем текущее состояние
	if tx.status == txStatusAborted {
		tx.mu.Unlock()
		return ErrTxAborted
	}
	if tx.isTerminated() {
		tx.mu.Unlock()
		return ErrTxError
	}
	// ... и возможность быстрого завершения
	if tx.tod == nil && len(tx.vrms) == 0 {
		tx.status = txStatusCommitted
		tx.clear()
		tx.mu.Unlock()
		return nil
	}

	internal.Assert(tx.status == txStatusActive)

	// Формируем рабочий набор данных
	var (
		tod         = tx.tod
		vrms        = append(make([]EnlistmentNotification, 0, len(tx.vrms)+len(tx.vrms)/2+1), tx.vrms...)
		responses   = make(chan trmResponse, len(vrms)+1)
		shouldAbort bool
	)

	// Шаг 1: 2PC Prepare

	tx.status = txStatusPreparing

	// Выполняем подготовку не долгосрочных ресурсов
	for processed := 0; !shouldAbort && processed < len(vrms); {
		tx.mu.Unlock()

		for i := processed; i < len(vrms); i++ {
			vrms[i].Prepare(ctx, enlistment{id: i, resp: responses})
		}

		for ; processed < len(vrms); processed++ {
			resp, ok := <-responses
			internal.Assert(ok)
			switch resp.code {
			case trmResponseCodeDone:
				vrms[resp.enlId] = nil
			case trmResponseCodeAbort:
				shouldAbort = true
			case trmResponseCodeCommit:
			}
		}

		tx.mu.Lock()

		// Учитываем возможные вложенные присоединения...
		if len(tx.vrms) > len(vrms) {
			vrms = append(vrms, tx.vrms[len(vrms):]...)
			close(responses)
			responses = make(chan trmResponse, len(vrms)+1)
		}
		tod = tx.tod

		// Учитываем возможные вложенные Rollback...
		if tx.status == txStatusPrepareAborted {
			shouldAbort = true
		}
	}

	// Шаг : SPC Commit

	tx.status = txStatusFinalizing

	// Выполняем SPC Commit для TOD, если применимо
	if tod != nil && !shouldAbort {
		tx.mu.Unlock()

		tod.SinglePhaseCommit(ctx, enlistment{id: -1, resp: responses})

		resp, ok := <-responses
		internal.Assert(ok)
		if resp.code != trmResponseCodeCommit {
			shouldAbort = true
		}

		tx.mu.Lock()
	}

	//	Шаг 3: 2PC Rollback/Commit + SPC Rollback

	// Фиксируем результирующий статус транзакции
	if shouldAbort {
		tx.status = txStatusAborted
	} else {
		tx.status = txStatusCommitted
	}

	// Высвобождаем накопленные ресурсы - все необходимое есть в рабочем наборе данных
	tx.clear()

	tx.mu.Unlock()

	// Инициируем необходимые Commit/Rollback
	pendingRespsNo := 0
	if tod != nil && shouldAbort {
		tod.Rollback(ctx, enlistment{id: -1, resp: responses})
		pendingRespsNo++
	}
	for i, vrm := range vrms {
		if vrm == nil {
			//	"Done" присоединения игнорируем
			continue
		}
		if shouldAbort {
			vrm.Rollback(ctx, enlistment{id: i, resp: responses})
		} else {
			vrm.Commit(ctx, enlistment{id: i, resp: responses})
		}
		pendingRespsNo++
	}

	// Запускаем конкурентную фоновую обработку ответов
	go func() {
		for range pendingRespsNo {
			_, ok := <-responses
			internal.Assert(ok)
		}
		close(responses)
	}()

	// Завершаем вызов

	if shouldAbort {
		return ErrTxAborted
	}
	return nil
}

// Rollback реализует [Transaction.Rollback].
func (tx *CommittableTransaction) Rollback(ctx context.Context) error {
	// Отрабатываем случай вложенного (и неотличимого конкурентного) вызова во время 2PC Prepare
	tx.mu.Lock()
	if tx.isPreparing() {
		tx.status = txStatusPrepareAborted
		tx.mu.Unlock()
		return nil
	}
	tx.mu.Unlock()

	tx.ctlMu.Lock()
	defer tx.ctlMu.Unlock()

	tx.mu.Lock()

	// ... т.к. tx.ctlMu исключает конкурирующие вызовы Commit и Rollback
	internal.Assert(tx.isTerminated() || tx.status == txStatusActive)

	// Проверяем текущее состояние
	if tx.status == txStatusAborted {
		tx.mu.Unlock()
		return ErrTxAborted
	}
	if tx.isTerminated() {
		tx.mu.Unlock()
		return ErrTxError
	}
	// ... и возможность быстрого завершения
	if tx.tod == nil && len(tx.vrms) == 0 {
		tx.status = txStatusAborted
		tx.mu.Unlock()
		return nil
	}

	// Формируем рабочий набор данных
	var (
		tod  = tx.tod
		vrms = tx.vrms
	)

	// Единственный шаг: 2PC/SPC Rollback

	// Фиксируем результирующий статус транзакции
	tx.status = txStatusAborted

	// Высвобождаем накопленные ресурсы - все необходимое есть в рабочем наборе данных
	tx.clear()

	tx.mu.Unlock()

	// Инициируем необходимые Rollback
	responses := make(chan trmResponse, len(vrms)+1)
	pendingRespsNo := 0
	if tod != nil {
		tod.Rollback(ctx, enlistment{id: -1, resp: responses})
		pendingRespsNo++
	}
	for i, vrm := range vrms {
		vrm.Rollback(ctx, enlistment{id: i, resp: responses})
	}
	pendingRespsNo += len(vrms)

	// Запускаем конкурентную фоновую обработку ответов
	go func() {
		for range pendingRespsNo {
			_, ok := <-responses
			internal.Assert(ok)
		}
		close(responses)
	}()

	// Завершаем вызов

	return nil
}

func (tx *CommittableTransaction) isTerminated() bool {
	return tx.status == txStatusCommitted || tx.status == txStatusAborted
}

func (tx *CommittableTransaction) isPreparing() bool {
	return tx.status == txStatusPreparing || tx.status == txStatusPrepareAborted
}

func (tx *CommittableTransaction) clear() {
	tx.tod = nil
	tx.vrms = nil
}

// ---

type txStatus int

const (
	txStatusActive txStatus = iota
	txStatusPreparing
	txStatusPrepareAborted
	txStatusFinalizing
	txStatusCommitted
	txStatusAborted
)
