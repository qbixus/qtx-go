package qtx

import "context"

type Enlistment interface {
	// Done indicates that the transaction participant has completed its work.
	Done()
}

type SinglePhaseEnlistment interface {
	Aborted(cause error)
	Committed()
	//	InDoubt(cause error)
}

type PreparingEnlistment interface {
	Enlistment
	// ForceRollback indicates that the transaction should be rolled back.
	ForceRollback(cause error)
	// Prepared indicates that the transaction can be commited.
	Prepared()
}

type EnlistmentNotification interface {
	Prepare(ctx context.Context, enl PreparingEnlistment)
	Commit(ctx context.Context, enl Enlistment)
	Rollback(ctx context.Context, enl Enlistment)
	//	InDoubt(enl Enlistment)
}

type SinglePhaseNotification interface {
	EnlistmentNotification
	SinglePhaseCommit(ctx context.Context, enl SinglePhaseEnlistment)
}

// Transaction - локальная транзакция с множественными участниками-диспетчерами долговременных (durable) и не
// долговременных (volatile) ресурсов, взаимодействие с которыми производится по протоколам Two Phase Commit (2PC) и
// Single Phase Commit (SPC).
type Transaction interface {

	// EnlistTheOnlyDurable присоединяет диспетчер долгосрочных ресурсов в режиме один-и-только-один. В этом режиме
	// присоединение других диспетчеров долгосрочных ресурсов не допускается, а взаимодействие с присоединенным
	// диспетчером всегда производится только по протоколу SPC.
	// Может использоваться конкурентно. На фазе подготовки 2PC также может использоваться вложенно.
	//
	// Возвращает nil если диспетчер был присоединен и ErrTxError если статус транзакции не допускает новые
	// присоединения или если присоединенный диспетчер долговременных ресурсов уже есть.
	EnlistTheOnlyDurable(trm SinglePhaseNotification) error

	// EnlistVolatile присоединяет диспетчер не долговременных ресурсов.
	// Может использоваться конкурентно. На фазе подготовки 2PC также может использоваться вложенно.
	//
	// Возвращает nil если диспетчер был присоединен и ErrTxError если статус транзакции не допускает новые
	// присоединения.
	EnlistVolatile(trm EnlistmentNotification) error

	// Rollback отменяет все изменения в транзакции.
	// Блокируется на все время выполнения отмены изменений за исключением заключительной обработки ответов - она
	// всегда выполняется конкурентно и может завершиться уже после завершения вызова Rollback.
	// Может использоваться конкурентно. На фазе подготовки 2PC также может использоваться вложенно.
	//
	// Возвращает nil если изменения отменены, ErrTxAborted если изменения были отменены ранее, и ErrTxError если
	// изменения были зафиксированы ранее.
	Rollback(context.Context) error

	//	RollbackErr(error) error
}
