package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/transaction"
	"github.com/oarkflow/transaction/logs"
	"github.com/oarkflow/transaction/metrics"
	"github.com/oarkflow/transaction/storage"
)

type DummyResource struct {
	Name         string
	FailCommit   bool
	FailRollback bool
}

func (dr *DummyResource) Commit(ctx context.Context) error {
	if dr.FailCommit {
		return fmt.Errorf("resource %s commit failed", dr.Name)
	}
	log.Printf("DummyResource %s committed", dr.Name)
	return nil
}

func (dr *DummyResource) Rollback(ctx context.Context) error {
	if dr.FailRollback {
		return fmt.Errorf("resource %s rollback failed", dr.Name)
	}
	log.Printf("DummyResource %s rolled back", dr.Name)
	return nil
}

type DummyPreparableResource struct {
	Name        string
	FailPrepare bool
}

func (dpr *DummyPreparableResource) Prepare(ctx context.Context) error {
	if dpr.FailPrepare {
		return fmt.Errorf("resource %s prepare failed", dpr.Name)
	}
	log.Printf("DummyPreparableResource %s prepared", dpr.Name)
	return nil
}

func (dpr *DummyPreparableResource) Commit(ctx context.Context) error {
	log.Printf("DummyPreparableResource %s committed", dpr.Name)
	return nil
}

func (dpr *DummyPreparableResource) Rollback(ctx context.Context) error {
	log.Printf("DummyPreparableResource %s rolled back", dpr.Name)
	return nil
}

type DummyTracer struct{}

type DummySpan struct {
	name string
}

func (dt *DummyTracer) StartSpan(ctx context.Context, name string) (context.Context, metrics.Span) {
	log.Printf("Starting span: %s", name)
	return ctx, &DummySpan{name: name}
}

func (ds *DummySpan) End() {
	log.Printf("Ending span: %s", ds.name)
}

func (ds *DummySpan) SetAttributes(attrs map[string]interface{}) {
	log.Printf("Span %s attributes: %v", ds.name, attrs)
}

type DummyDistributedCoordinator struct{}

func (ddc *DummyDistributedCoordinator) BeginDistributed(tx *transaction.Transaction) error {
	log.Printf("DistributedCoordinator: Begin transaction %d", tx.GetID())
	return nil
}

func (ddc *DummyDistributedCoordinator) CommitDistributed(tx *transaction.Transaction) error {
	log.Printf("DistributedCoordinator: Commit transaction %d", tx.GetID())
	return nil
}

func (ddc *DummyDistributedCoordinator) RollbackDistributed(tx *transaction.Transaction) error {
	log.Printf("DistributedCoordinator: Rollback transaction %d", tx.GetID())
	return nil
}

var lifecycleHooks = &transaction.LifecycleHooks{
	OnBegin: func(txID int64, ctx context.Context) {
		log.Printf("Lifecycle Hook: Transaction %d begun", txID)
	},
	OnBeforeCommit: func(txID int64, ctx context.Context) {
		log.Printf("Lifecycle Hook: Transaction %d before commit", txID)
	},
	OnAfterCommit: func(txID int64, ctx context.Context) {
		log.Printf("Lifecycle Hook: Transaction %d after commit", txID)
	},
	OnBeforeRollback: func(txID int64, ctx context.Context) {
		log.Printf("Lifecycle Hook: Transaction %d before rollback", txID)
	},
	OnAfterRollback: func(txID int64, ctx context.Context) {
		log.Printf("Lifecycle Hook: Transaction %d after rollback", txID)
	},
	OnClose: func(txID int64, ctx context.Context) {
		log.Printf("Lifecycle Hook: Transaction %d closed", txID)
	},
}

var customRetryPolicy = transaction.RetryPolicy{
	MaxRetries: 2,
	Delay:      200 * time.Millisecond,
	ShouldRetry: func(err error) bool {
		return true
	},
	BackoffStrategy: func(attempt int) time.Duration {
		return time.Duration(100*(1<<attempt)) * time.Millisecond
	},
}

func exampleBasicTransaction() {
	log.Println("=== Example 1: Basic Successful Transaction ===")
	tx := transaction.NewTransaction()
	ctx := context.Background()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Basic commit action executed")
		return nil
	})
	res := &DummyResource{Name: "BasicResource"}
	tx.RegisterResource(res)
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Commit error: %v", err)
	} else {
		log.Printf("Transaction %d state: %s", tx.GetID(), tx.GetState().String())
	}
}

func exampleSimulatedCommitFailure() {
	log.Println("=== Example 2: Simulated Commit Failure ===")
	tx := transaction.NewTransaction()
	ctx := context.Background()
	tx.SetTestHooks(&transaction.TestHooks{SimulateCommitFailure: true})
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Commit action (will be ignored due to simulated failure)")
		return nil
	})
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Expected commit failure: %v", err)
		log.Printf("Transaction state: %s", tx.GetState().String())
	}
}

func exampleSimulatedPrepareFailure() {
	log.Println("=== Example 3: Simulated Prepare Failure ===")
	tx := transaction.NewTransaction()
	ctx := context.Background()
	tx.SetTestHooks(&transaction.TestHooks{SimulatePrepareFailure: true})
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Commit action (ignored due to prepare failure)")
		return nil
	})
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Expected prepare failure: %v", err)
	}
}

func exampleSimulatedRollbackFailure() {
	log.Println("=== Example 4: Simulated Rollback Failure ===")
	tx := transaction.NewTransaction()
	ctx := context.Background()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}

	tx.RegisterCommit(func(ctx context.Context) error {
		return errors.New("simulated commit error to trigger rollback")
	})
	tx.SetTestHooks(&transaction.TestHooks{SimulateRollbackFailure: true})
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Expected rollback failure: %v", err)
	}
}

func exampleNestedTransactions() {
	log.Println("=== Example 5: Nested Transactions and Savepoints ===")
	tx := transaction.NewTransaction()
	ctx := context.Background()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}

	sp, err := tx.CreateSavepoint(ctx)
	if err != nil {
		log.Printf("Error creating savepoint: %v", err)
		return
	}
	log.Printf("Created unnamed savepoint at index %d", sp)

	if err := tx.CreateNamedSavepoint("namedSP"); err != nil {
		log.Printf("Error creating named savepoint: %v", err)
	}

	nestedTx, err := tx.BeginNested(ctx)
	if err != nil {
		log.Printf("Error beginning nested transaction: %v", err)
		return
	}
	tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Nested commit action executed")
		return nil
	})
	if err := nestedTx.Commit(ctx); err != nil {
		log.Printf("Nested transaction commit error: %v", err)
	} else {
		log.Println("Nested transaction committed")
	}

	if err := tx.RollbackToNamedSavepoint(ctx, "namedSP"); err != nil {
		log.Printf("Error rolling back to named savepoint: %v", err)
	} else {
		log.Println("Rolled back to named savepoint 'namedSP'")
	}
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Final commit error after nested transactions: %v", err)
	} else {
		log.Printf("Final transaction committed, state: %s", tx.GetState().String())
	}
}

func exampleAsyncOperations() {
	log.Println("=== Example 6: Asynchronous Operations ===")

	tx := transaction.NewTransaction()
	ctx := context.Background()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Async commit action executed")
		return nil
	})
	res := &DummyResource{Name: "AsyncResource"}
	tx.RegisterResource(res)
	asyncCommit := tx.AsyncCommitWithResult(ctx)
	result := <-asyncCommit
	if result.Err != nil {
		log.Printf("Async commit error: %v", result.Err)
	} else {
		log.Printf("Async commit succeeded, state: %s", result.State.String())
	}

	tx2 := transaction.NewTransaction()
	if err := tx2.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	tx2.RegisterCommit(func(ctx context.Context) error {
		return errors.New("simulated error to trigger async rollback")
	})
	asyncRollback := tx2.AsyncRollbackWithResult(ctx)
	rbResult := <-asyncRollback
	if rbResult.Err != nil {
		log.Printf("Async rollback error: %v", rbResult.Err)
	} else {
		log.Printf("Async rollback succeeded, state: %s", rbResult.State.String())
	}
}

func exampleRunInTransaction() {
	log.Println("=== Example 7: RunInTransaction Helper ===")
	ctx := context.Background()
	err := transaction.RunInTransaction(ctx, func(tx *transaction.Transaction) error {
		tx.RegisterCommit(func(ctx context.Context) error {
			log.Println("RunInTransaction commit action executed")
			return nil
		})
		res := &DummyResource{Name: "RunInTransResource"}
		tx.RegisterResource(res)
		return nil
	})
	if err != nil {
		log.Printf("RunInTransaction error: %v", err)
	} else {
		log.Println("RunInTransaction succeeded")
	}
}

func exampleCustomLoggerWithCorrelation() {
	log.Println("=== Example 8: Custom Logger with Correlation ID ===")

	ctx := context.WithValue(context.Background(), transaction.ContextKeyCorrelationID, "corr-12345")
	tx := transaction.NewTransaction()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Commit action executed with custom logger and correlation id")
		return nil
	})
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Commit error: %v", err)
	} else {
		log.Println("Transaction committed successfully with correlation id")
	}
}

func exampleDistributedCoordinator() {
	log.Println("=== Example 9: Distributed Coordinator and Tracing ===")
	opts := transaction.TransactionOptions{
		IsolationLevel:         "distributed",
		Timeout:                5 * time.Second,
		RetryPolicy:            transaction.RetryPolicy{MaxRetries: 2, Delay: 100 * time.Millisecond, ShouldRetry: func(err error) bool { return true }, BackoffStrategy: func(attempt int) time.Duration { return time.Duration(100*(1<<attempt)) * time.Millisecond }},
		Logger:                 &logs.DefaultLogger{Level: logs.InfoLevel},
		Metrics:                &metrics.NoopMetricsCollector{},
		DistributedCoordinator: &DummyDistributedCoordinator{},
		CaptureStackTrace:      true,
		Tracer:                 &DummyTracer{},
		LifecycleHooks:         lifecycleHooks,
	}
	tx := transaction.NewTransactionWithOptions(opts)
	ctx := context.Background()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}
	tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Distributed commit action executed")
		return nil
	})
	res := &DummyResource{Name: "DistributedResource"}
	tx.RegisterResource(res)
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Distributed transaction commit error: %v", err)
	} else {
		log.Printf("Distributed transaction committed, state: %s", tx.GetState().String())
	}
}

func examplePerActionRetryPolicies() {
	log.Println("=== Example 10: Per-Action Retry Policies ===")
	tx := transaction.NewTransaction()
	ctx := context.Background()
	if err := tx.Begin(ctx); err != nil {
		log.Printf("Begin error: %v", err)
		return
	}

	err := tx.RegisterCommitWithRetryPolicy(func(ctx context.Context) error {
		log.Println("Custom retry commit action executed")
		return nil
	}, customRetryPolicy)
	if err != nil {
		log.Printf("Error registering commit action with custom policy: %v", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		log.Printf("Transaction commit error with custom retry policy: %v", err)
	} else {
		log.Printf("Transaction with custom retry policies committed, state: %s", tx.GetState().String())
	}
}

func transactionBuilder() {
	tb := transaction.NewTransactionBuilder().
		SetIsolationLevel("SERIALIZABLE").
		SetTimeout(10 * time.Second).
		SetParallelCommit(true).
		SetParallelRollback(true).
		SetRetryPolicy(transaction.RetryPolicy{
			MaxRetries:      3,
			Delay:           200 * time.Millisecond,
			ShouldRetry:     func(err error) bool { return true },
			BackoffStrategy: func(attempt int) time.Duration { return time.Duration(100*(1<<attempt)) * time.Millisecond },
		}).
		SetLifecycleHooks(&transaction.LifecycleHooks{
			OnBegin: func(txID int64, ctx context.Context) {
				log.Printf("Transaction %d started", txID)
			},
			OnBeforeCommit: func(txID int64, ctx context.Context) {
				log.Printf("Transaction %d before commit", txID)
			},
			OnAfterCommit: func(txID int64, ctx context.Context) {
				log.Printf("Transaction %d committed", txID)
			},
			OnBeforeRollback: func(txID int64, ctx context.Context) {
				log.Printf("Transaction %d before rollback", txID)
			},
			OnAfterRollback: func(txID int64, ctx context.Context) {
				log.Printf("Transaction %d rolled back", txID)
			},
			OnClose: func(txID int64, ctx context.Context) {
				log.Printf("Transaction %d closed", txID)
			},
		}).
		SetDistributedCoordinator(&transaction.TwoPhaseCoordinator{}).
		SetLogger(&logs.DefaultLogger{Fields: map[string]interface{}{"app": "transactionBuilderExample"}, Level: logs.InfoLevel}).
		SetMetrics(&metrics.NoopMetricsCollector{}).
		SetTracer(&metrics.NoopTracer{}).
		SetAuditLogger(&logs.SimpleAuditLogger{})

	tx := tb.Build()
	ctx := context.Background()
	if err := tx.Begin(ctx); err != nil {
		log.Fatalf("Begin error: %v", err)
	}
	tx.RegisterCommit(func(ctx context.Context) error {
		log.Println("Commit action executed")
		return nil
	})
	if err := tx.Commit(ctx); err != nil {
		log.Fatalf("Commit error: %v", err)
	}
}

func main() {
	// initialize storage and run recovery on startup
	fstore, err := storage.NewFileStorage("./txlogs")
	if err != nil {
		log.Printf("failed to initialize storage: %v", err)
	} else {
		// run conservative recovery; log but don't fail startup
		_ = transaction.RecoverInProgress(fstore, nil)
	}
	exampleBasicTransaction()
	time.Sleep(500 * time.Millisecond)

	exampleSimulatedCommitFailure()
	time.Sleep(500 * time.Millisecond)

	exampleSimulatedPrepareFailure()
	time.Sleep(500 * time.Millisecond)

	exampleSimulatedRollbackFailure()
	time.Sleep(500 * time.Millisecond)

	exampleNestedTransactions()
	time.Sleep(500 * time.Millisecond)

	exampleAsyncOperations()
	time.Sleep(500 * time.Millisecond)

	exampleRunInTransaction()
	time.Sleep(500 * time.Millisecond)

	exampleCustomLoggerWithCorrelation()
	time.Sleep(500 * time.Millisecond)

	exampleDistributedCoordinator()
	time.Sleep(500 * time.Millisecond)

	examplePerActionRetryPolicies()
	time.Sleep(500 * time.Millisecond)

	transactionBuilder()
}
