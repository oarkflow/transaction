package transaction

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/transaction/logs"
	"github.com/oarkflow/transaction/metrics"
)

type contextKey string

const (
	contextKeyBatch         contextKey = "batch"
	ContextKeyCorrelationID contextKey = "correlation_id"
)

type TransactionError struct {
	TxID       int64
	Err        error
	Code       string
	Category   string
	Action     string
	StackTrace string
}

func (te TransactionError) Error() string {
	return fmt.Sprintf("transaction %d [%s, code %s, category %s]: %v\nStackTrace:\n%s", te.TxID, te.Action, te.Code, te.Category, te.Err, te.StackTrace)
}

type TransactionalResource interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type PreparableResource interface {
	TransactionalResource
	Prepare(ctx context.Context) error
}

type RetryPolicy struct {
	MaxRetries      int
	Delay           time.Duration
	ShouldRetry     func(err error) bool
	BackoffStrategy func(attempt int) time.Duration
}

type TestHooks struct {
	SimulateCommitFailure   bool
	SimulateRollbackFailure bool
	SimulatePrepareFailure  bool
}

type DistributedCoordinator interface {
	BeginDistributed(tx *Transaction) error
	CommitDistributed(tx *Transaction) error
	RollbackDistributed(tx *Transaction) error
}

type TwoPhaseCoordinator struct{}

func (c *TwoPhaseCoordinator) BeginDistributed(tx *Transaction) error {
	log.Printf("TwoPhaseCoordinator: Begin distributed transaction %d", tx.GetID())
	return nil
}

func (c *TwoPhaseCoordinator) CommitDistributed(tx *Transaction) error {
	log.Printf("TwoPhaseCoordinator: Commit distributed transaction %d", tx.GetID())
	return nil
}

func (c *TwoPhaseCoordinator) RollbackDistributed(tx *Transaction) error {
	log.Printf("TwoPhaseCoordinator: Rollback distributed transaction %d", tx.GetID())
	return nil
}

type LifecycleHooks struct {
	OnBegin          func(txID int64, ctx context.Context)
	OnBeforeCommit   func(txID int64, ctx context.Context)
	OnAfterCommit    func(txID int64, ctx context.Context)
	OnBeforeRollback func(txID int64, ctx context.Context)
	OnAfterRollback  func(txID int64, ctx context.Context)
	OnClose          func(txID int64, ctx context.Context)

	OnBeforeCommitAction   func(txID int64, actionDesc string, ctx context.Context)
	OnAfterCommitAction    func(txID int64, actionDesc string, ctx context.Context)
	OnBeforeRollbackAction func(txID int64, actionDesc string, ctx context.Context)
	OnAfterRollbackAction  func(txID int64, actionDesc string, ctx context.Context)
}

type TransactionOptions struct {
	IsolationLevel         string
	Timeout                time.Duration
	PrepareTimeout         time.Duration
	CommitTimeout          time.Duration
	RollbackTimeout        time.Duration
	ParallelCommit         bool
	ParallelRollback       bool
	RetryPolicy            RetryPolicy
	LifecycleHooks         *LifecycleHooks
	DistributedCoordinator DistributedCoordinator
	Logger                 logs.Logger
	Metrics                metrics.Collector
	CaptureStackTrace      bool
	Tracer                 metrics.Tracer
	AuditLogger            logs.AuditLogger
}

type TxState int

const (
	StateInitialized TxState = iota
	StateInProgress
	StateCommitted
	StateRolledBack
	StateFailed
)

func (s TxState) String() string {
	switch s {
	case StateInitialized:
		return "Initialized"
	case StateInProgress:
		return "InProgress"
	case StateCommitted:
		return "Committed"
	case StateRolledBack:
		return "RolledBack"
	case StateFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

type AsyncResult struct {
	TxID  int64
	Err   error
	State TxState
}

type ActionWithPolicy struct {
	fn func(ctx context.Context) error
	rp *RetryPolicy
}

type Transaction struct {
	mu                        sync.RWMutex
	id                        int64
	state                     TxState
	rollbackActions           []func(ctx context.Context) error
	commitActions             []func(ctx context.Context) error
	commitActionsWithPolicy   []ActionWithPolicy
	rollbackActionsWithPolicy []ActionWithPolicy
	cleanupActions            []func(ctx context.Context) error
	resources                 []TransactionalResource
	cleanupCalled             bool
	retryPolicy               RetryPolicy
	parallelCommit            bool
	parallelRollback          bool
	onError                   func(txID int64, err error)
	isolationLevel            string
	timeout                   time.Duration
	prepareTimeout            time.Duration
	commitTimeout             time.Duration
	rollbackTimeout           time.Duration
	lifecycleHooks            *LifecycleHooks
	distributedCoordinator    DistributedCoordinator
	logger                    logs.Logger
	metrics                   metrics.Collector
	testHooks                 *TestHooks
	savepoints                map[string]int
	abortCalled               bool
	captureStackTrace         bool
	tracer                    metrics.Tracer
	auditLogger               logs.AuditLogger
}

var txCounter int64

var txPool = sync.Pool{
	New: func() any { return &Transaction{} },
}

func LoadTransactionConfig() TransactionOptions {
	var opts TransactionOptions
	opts.IsolationLevel = os.Getenv("TX_ISOLATION_LEVEL")
	opts.IsolationLevel = validateIsolationLevel(opts.IsolationLevel)
	timeoutStr := os.Getenv("TX_TIMEOUT")
	if t, err := strconv.Atoi(timeoutStr); err == nil {
		opts.Timeout = time.Duration(t) * time.Millisecond
	} else {
		opts.Timeout = 5000 * time.Millisecond
	}
	opts.PrepareTimeout = opts.Timeout / 2
	opts.CommitTimeout = opts.Timeout / 2
	opts.RollbackTimeout = opts.Timeout / 2
	opts.ParallelCommit = os.Getenv("TX_PARALLEL_COMMIT") == "true"
	opts.ParallelRollback = os.Getenv("TX_PARALLEL_ROLLBACK") == "true"
	opts.RetryPolicy = RetryPolicy{
		MaxRetries:  3,
		Delay:       200 * time.Millisecond,
		ShouldRetry: func(err error) bool { return true },
		BackoffStrategy: func(attempt int) time.Duration {
			return time.Duration(100*(1<<attempt)) * time.Millisecond
		},
	}
	opts.Logger = &logs.DefaultLogger{Fields: make(map[string]any), Level: logs.InfoLevel}
	opts.Metrics = &metrics.PrometheusMetricsCollector{}
	opts.CaptureStackTrace = true
	opts.Tracer = &metrics.NoopTracer{}
	opts.AuditLogger = &logs.SimpleAuditLogger{}
	return opts
}

func validateIsolationLevel(level string) string {
	switch level {
	case "READ_UNCOMMITTED", "READ_COMMITTED", "REPEATABLE_READ", "SERIALIZABLE":
		return level
	default:
		return "default"
	}
}

type TransactionBuilder struct {
	opts TransactionOptions
}

func NewTransactionBuilder() *TransactionBuilder {
	return &TransactionBuilder{opts: LoadTransactionConfig()}
}

func (tb *TransactionBuilder) SetIsolationLevel(level string) *TransactionBuilder {
	tb.opts.IsolationLevel = validateIsolationLevel(level)
	return tb
}

func (tb *TransactionBuilder) SetTimeout(d time.Duration) *TransactionBuilder {
	tb.opts.Timeout = d
	tb.opts.PrepareTimeout = d / 2
	tb.opts.CommitTimeout = d / 2
	tb.opts.RollbackTimeout = d / 2
	return tb
}

func (tb *TransactionBuilder) SetParallelCommit(parallel bool) *TransactionBuilder {
	tb.opts.ParallelCommit = parallel
	return tb
}

func (tb *TransactionBuilder) SetParallelRollback(parallel bool) *TransactionBuilder {
	tb.opts.ParallelRollback = parallel
	return tb
}

func (tb *TransactionBuilder) SetRetryPolicy(rp RetryPolicy) *TransactionBuilder {
	tb.opts.RetryPolicy = rp
	return tb
}

func (tb *TransactionBuilder) SetLifecycleHooks(hooks *LifecycleHooks) *TransactionBuilder {
	tb.opts.LifecycleHooks = hooks
	return tb
}

func (tb *TransactionBuilder) SetDistributedCoordinator(dc DistributedCoordinator) *TransactionBuilder {
	tb.opts.DistributedCoordinator = dc
	return tb
}

func (tb *TransactionBuilder) SetLogger(l logs.Logger) *TransactionBuilder {
	tb.opts.Logger = l
	return tb
}

func (tb *TransactionBuilder) SetMetrics(m metrics.Collector) *TransactionBuilder {
	tb.opts.Metrics = m
	return tb
}

func (tb *TransactionBuilder) SetTracer(tr metrics.Tracer) *TransactionBuilder {
	tb.opts.Tracer = tr
	return tb
}

func (tb *TransactionBuilder) SetAuditLogger(a logs.AuditLogger) *TransactionBuilder {
	tb.opts.AuditLogger = a
	return tb
}

func (tb *TransactionBuilder) Build() *Transaction {
	return NewTransactionWithOptions(tb.opts)
}

func NewTransaction() *Transaction {
	return NewTransactionWithOptions(TransactionOptions{
		IsolationLevel:    "default",
		Timeout:           0,
		RetryPolicy:       RetryPolicy{MaxRetries: 0, Delay: 0, ShouldRetry: func(err error) bool { return false }},
		Metrics:           &metrics.NoopMetricsCollector{},
		CaptureStackTrace: true,
		Tracer:            &metrics.NoopTracer{},
		AuditLogger:       &logs.SimpleAuditLogger{},
	})
}

func NewTransactionWithOptions(opts TransactionOptions) *Transaction {
	tx := txPool.Get().(*Transaction)
	tx.id = atomic.AddInt64(&txCounter, 1)
	tx.state = StateInitialized
	tx.rollbackActions = make([]func(ctx context.Context) error, 0)
	tx.commitActions = make([]func(ctx context.Context) error, 0)
	tx.commitActionsWithPolicy = make([]ActionWithPolicy, 0)
	tx.rollbackActionsWithPolicy = make([]ActionWithPolicy, 0)
	tx.cleanupActions = make([]func(ctx context.Context) error, 0)
	tx.resources = make([]TransactionalResource, 0)
	tx.savepoints = make(map[string]int)
	tx.abortCalled = false

	tx.retryPolicy = opts.RetryPolicy
	tx.isolationLevel = validateIsolationLevel(opts.IsolationLevel)
	tx.timeout = opts.Timeout
	tx.prepareTimeout = opts.PrepareTimeout
	tx.commitTimeout = opts.CommitTimeout
	tx.rollbackTimeout = opts.RollbackTimeout
	tx.parallelCommit = opts.ParallelCommit
	tx.parallelRollback = opts.ParallelRollback
	tx.logger = opts.Logger
	tx.metrics = opts.Metrics
	tx.lifecycleHooks = opts.LifecycleHooks
	tx.distributedCoordinator = opts.DistributedCoordinator
	tx.captureStackTrace = opts.CaptureStackTrace
	tx.tracer = opts.Tracer
	// tx.auditLogger = opts.AuditLogger
	tx.testHooks = nil

	return tx
}

func (t *Transaction) SetLogger(l logs.Logger) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.logger = l
}

func (t *Transaction) SetMetricsCollector(m metrics.Collector) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.metrics = m
}

func (t *Transaction) SetTestHooks(th *TestHooks) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.testHooks = th
}

func (t *Transaction) SetRetryPolicy(rp RetryPolicy) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.retryPolicy = rp
}

func (t *Transaction) SetParallelExecution(parallelCommit, parallelRollback bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.parallelCommit = parallelCommit
	t.parallelRollback = parallelRollback
}

func (t *Transaction) SetOnError(f func(txID int64, err error)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.onError = f
}

func (t *Transaction) SetIsolationLevel(level string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.isolationLevel = validateIsolationLevel(level)
}

func (t *Transaction) SetTimeout(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.timeout = d
}

func (t *Transaction) Log(level logs.LogLevel, msg string, args ...any) {
	if t.logger == nil {
		return
	}
	switch level {
	case logs.ErrorLevel:
		t.logger.Error(msg, args...)
	default:
		t.logger.Info(msg, args...)
	}
}

func (t *Transaction) SetPrepareTimeout(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.prepareTimeout = d
}

func (t *Transaction) SetCommitTimeout(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.commitTimeout = d
}

func (t *Transaction) SetRollbackTimeout(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rollbackTimeout = d
}

func (t *Transaction) SetLifecycleHooks(hooks *LifecycleHooks) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lifecycleHooks = hooks
}

func (t *Transaction) SetDistributedCoordinator(dc DistributedCoordinator) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.distributedCoordinator = dc
}

func (t *Transaction) SetTracer(tr metrics.Tracer) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tracer = tr
}

func (t *Transaction) RegisterCommitWithRetryPolicy(fn func(ctx context.Context) error, rp RetryPolicy) error {
	if fn == nil {
		return errors.New("commit action cannot be nil")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return fmt.Errorf("cannot register commit action in transaction %d state %s", t.id, t.state)
	}
	t.commitActionsWithPolicy = append(t.commitActionsWithPolicy, ActionWithPolicy{fn: fn, rp: &rp})
	t.Log(logs.InfoLevel, "Transaction %d: Registered commit action with custom retry policy", t.id)
	return nil
}

func (t *Transaction) RegisterRollbackWithRetryPolicy(fn func(ctx context.Context) error, rp RetryPolicy) error {
	if fn == nil {
		return errors.New("rollback action cannot be nil")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return fmt.Errorf("cannot register rollback action in transaction %d state %s", t.id, t.state)
	}
	t.rollbackActionsWithPolicy = append(t.rollbackActionsWithPolicy, ActionWithPolicy{fn: fn, rp: &rp})
	t.Log(logs.InfoLevel, "Transaction %d: Registered rollback action with custom retry policy", t.id)
	return nil
}

func (t *Transaction) Begin(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	batch := ctx.Value(contextKeyBatch)

	if cid, ok := ctx.Value(ContextKeyCorrelationID).(string); ok {
		if t.logger != nil {
			t.logger = t.logger.WithFields(map[string]any{"correlation_id": cid})
		}
	}
	if t.state != StateInitialized {
		return fmt.Errorf("cannot begin transaction %d in state %s", t.id, t.state)
	}
	t.state = StateInProgress
	t.rollbackActions = make([]func(ctx context.Context) error, 0)
	t.commitActions = make([]func(ctx context.Context) error, 0)
	t.commitActionsWithPolicy = make([]ActionWithPolicy, 0)
	t.rollbackActionsWithPolicy = make([]ActionWithPolicy, 0)
	t.cleanupActions = make([]func(ctx context.Context) error, 0)
	t.resources = make([]TransactionalResource, 0)
	t.savepoints = make(map[string]int)
	t.abortCalled = false

	if t.lifecycleHooks != nil && t.lifecycleHooks.OnBegin != nil {
		t.lifecycleHooks.OnBegin(t.id, ctx)
	}

	t.Log(logs.InfoLevel, "Transaction %d begun with isolation level '%s'", t.id, t.isolationLevel)
	if batch != nil {
		t.Log(logs.InfoLevel, "Transaction %d begun for batch %v", t.id, batch)
	}
	if t.distributedCoordinator != nil {
		if err := t.distributedCoordinator.BeginDistributed(t); err != nil {
			return fmt.Errorf("begin distributed failed: %w", err)
		}
	}

	if t.tracer != nil {
		var span metrics.Span
		ctx, span = t.tracer.StartSpan(ctx, fmt.Sprintf("Transaction-%d-Begin", t.id))
		span.End()
	}
	if t.auditLogger != nil {
		t.auditLogger.LogEvent("Begin", map[string]any{"tx_id": t.id})
	}
	return nil
}

func (t *Transaction) RegisterRollback(fn func(ctx context.Context) error) error {
	if fn == nil {
		return errors.New("rollback action cannot be nil")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return fmt.Errorf("cannot register rollback action in transaction %d state %s", t.id, t.state)
	}
	t.rollbackActions = append(t.rollbackActions, fn)
	t.Log(logs.InfoLevel, "Transaction %d: Registered rollback action", t.id)
	return nil
}

func (t *Transaction) RegisterCommit(fn func(ctx context.Context) error) error {
	if fn == nil {
		return errors.New("commit action cannot be nil")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return fmt.Errorf("cannot register commit action in transaction %d state %s", t.id, t.state)
	}
	t.commitActions = append(t.commitActions, fn)
	t.Log(logs.InfoLevel, "Transaction %d: Registered commit action", t.id)
	return nil
}

func (t *Transaction) RegisterCleanup(fn func(ctx context.Context) error) error {
	if fn == nil {
		return errors.New("cleanup action cannot be nil")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return fmt.Errorf("cannot register cleanup action in transaction %d state %s", t.id, t.state)
	}
	t.cleanupActions = append(t.cleanupActions, fn)
	t.Log(logs.InfoLevel, "Transaction %d: Registered cleanup action", t.id)
	return nil
}

func (t *Transaction) RegisterResource(res TransactionalResource) error {
	if res == nil {
		return errors.New("transactional resource cannot be nil")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return fmt.Errorf("cannot register resource in transaction %d state %s", t.id, t.state)
	}
	t.resources = append(t.resources, res)
	t.Log(logs.InfoLevel, "Transaction %d: Registered transactional resource", t.id)
	return nil
}

func (t *Transaction) DeregisterCommitAction(index int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if index < 0 || index >= len(t.commitActions) {
		return fmt.Errorf("commit action index %d out of bounds", index)
	}
	t.commitActions = append(t.commitActions[:index], t.commitActions[index+1:]...)
	t.Log(logs.InfoLevel, "Transaction %d: Deregistered commit action at index %d", t.id, index)
	return nil
}

func (t *Transaction) DeregisterRollbackAction(index int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if index < 0 || index >= len(t.rollbackActions) {
		return fmt.Errorf("rollback action index %d out of bounds", index)
	}
	t.rollbackActions = append(t.rollbackActions[:index], t.rollbackActions[index+1:]...)
	t.Log(logs.InfoLevel, "Transaction %d: Deregistered rollback action at index %d", t.id, index)
	return nil
}

func (t *Transaction) prepareResources(ctx context.Context) error {
	var prepareCtx context.Context
	var cancel context.CancelFunc
	t.mu.RLock()
	pt := t.prepareTimeout
	t.mu.RUnlock()
	if pt > 0 {
		prepareCtx, cancel = context.WithTimeout(ctx, pt)
		defer cancel()
	} else {
		prepareCtx = ctx
	}

	t.mu.RLock()
	if t.testHooks != nil && t.testHooks.SimulatePrepareFailure {
		t.mu.RUnlock()
		return errors.New("simulated prepare failure")
	}
	t.mu.RUnlock()

	for i, res := range t.resources {
		if pr, ok := res.(PreparableResource); ok {
			desc := fmt.Sprintf("resource prepare %d for transaction %d", i, t.id)
			if err := retryAction(prepareCtx, func(ctx context.Context) error {
				return safeAction(ctx, pr.Prepare, desc)
			}, desc, t.retryPolicy); err != nil {
				t.Log(logs.ErrorLevel, "Transaction %d: Resource prepare failed: %v", t.id, err)
				return fmt.Errorf("prepare resource error: %w", err)
			}
		}
	}
	return nil
}

func (t *Transaction) Commit(ctx context.Context) error {
	startTime := time.Now()

	var commitCtx context.Context
	var cancel context.CancelFunc
	t.mu.RLock()
	ct := t.commitTimeout
	t.mu.RUnlock()
	if ct > 0 {
		commitCtx, cancel = context.WithTimeout(ctx, ct)
		defer cancel()
	} else {
		commitCtx = ctx
	}

	var span metrics.Span
	if t.tracer != nil {
		commitCtx, span = t.tracer.StartSpan(commitCtx, fmt.Sprintf("Transaction-%d-Commit", t.id))
		defer span.End()
	}

	if t.lifecycleHooks != nil && t.lifecycleHooks.OnBeforeCommit != nil {
		t.lifecycleHooks.OnBeforeCommit(t.id, commitCtx)
	}

	t.mu.Lock()
	if t.state != StateInProgress {
		if t.state == StateCommitted || t.state == StateRolledBack {
			t.mu.Unlock()
			return nil
		}
		t.mu.Unlock()
		return fmt.Errorf("cannot commit transaction %d in state %s", t.id, t.state)
	}
	batch := commitCtx.Value(contextKeyBatch)
	if err := commitCtx.Err(); err != nil {
		t.mu.Unlock()
		t.Log(logs.ErrorLevel, "Transaction %d: Context error during commit: %v", t.id, err)
		return fmt.Errorf("commit context error: %w", err)
	}
	t.mu.Unlock()

	if t.testHooks != nil && t.testHooks.SimulateCommitFailure {
		err := fmt.Errorf("simulated commit failure")
		t.Log(logs.ErrorLevel, "Transaction %d: %v", t.id, err)
		t.metrics.IncErrorCount()
		_ = t.Rollback(commitCtx)
		stack := ""
		if t.captureStackTrace {
			stack = string(debug.Stack())
		}
		return TransactionError{TxID: t.id, Err: err, Code: "SIM_COMMIT_FAIL", Category: "FATAL", Action: "commit", StackTrace: stack}
	}

	if err := t.prepareResources(commitCtx); err != nil {
		t.mu.Lock()
		t.state = StateFailed
		t.mu.Unlock()
		t.Log(logs.ErrorLevel, "Transaction %d: Prepare phase failed: %v", t.id, err)
		t.metrics.IncErrorCount()
		_ = t.Rollback(commitCtx)
		stack := ""
		if t.captureStackTrace {
			stack = string(debug.Stack())
		}
		return TransactionError{TxID: t.id, Err: err, Code: "PREP_FAIL", Category: "TRANSIENT", Action: "prepare", StackTrace: stack}
	}

	handleError := func(err error) {
		if t.onError != nil {
			t.onError(t.id, err)
		}
	}

	if t.parallelCommit {
		var wg sync.WaitGroup
		errCh := make(chan error, len(t.commitActions))
		for i, action := range t.commitActions {
			wg.Add(1)
			go func(i int, action func(ctx context.Context) error) {
				defer wg.Done()
				desc := fmt.Sprintf("commit action %d in transaction %d", i, t.id)

				if t.lifecycleHooks != nil && t.lifecycleHooks.OnBeforeCommitAction != nil {
					t.lifecycleHooks.OnBeforeCommitAction(t.id, desc, commitCtx)
				}
				start := time.Now()
				if err := retryAction(commitCtx, func(ctx context.Context) error {
					return safeAction(ctx, action, desc)
				}, desc, t.retryPolicy); err != nil {
					errCh <- err
					t.metrics.IncRetryCount()
				} else {
					t.metrics.RecordActionDuration("commit_action", time.Since(start))
				}

				if t.lifecycleHooks != nil && t.lifecycleHooks.OnAfterCommitAction != nil {
					t.lifecycleHooks.OnAfterCommitAction(t.id, desc, commitCtx)
				}
			}(i, action)
		}
		wg.Wait()
		close(errCh)
		if len(errCh) > 0 {
			var errList []error
			for err := range errCh {
				errList = append(errList, err)
			}
			t.mu.Lock()
			t.state = StateFailed
			t.mu.Unlock()
			rbErr := t.Rollback(commitCtx)
			if rbErr != nil {
				handleError(rbErr)
				stack := ""
				if t.captureStackTrace {
					stack = string(debug.Stack())
				}
				return TransactionError{TxID: t.id, Err: fmt.Errorf("commit errors: %v; rollback failed: %v", errList, rbErr), Code: "COMMIT_ERR", Category: "FATAL", Action: "commit", StackTrace: stack}
			}
			handleError(fmt.Errorf("commit errors: %v", errList))
			t.metrics.IncErrorCount()
			stack := ""
			if t.captureStackTrace {
				stack = string(debug.Stack())
			}
			return TransactionError{TxID: t.id, Err: fmt.Errorf("commit errors: %v", errList), Code: "COMMIT_ERR", Category: "TRANSIENT", Action: "commit", StackTrace: stack}
		}
	} else {
		for i, action := range t.commitActions {
			desc := fmt.Sprintf("commit action %d in transaction %d", i, t.id)
			if t.lifecycleHooks != nil && t.lifecycleHooks.OnBeforeCommitAction != nil {
				t.lifecycleHooks.OnBeforeCommitAction(t.id, desc, commitCtx)
			}
			start := time.Now()
			if err := retryAction(commitCtx, func(ctx context.Context) error {
				return safeAction(ctx, action, desc)
			}, desc, t.retryPolicy); err != nil {
				t.mu.Lock()
				t.state = StateFailed
				t.mu.Unlock()
				t.Log(logs.ErrorLevel, "Transaction %d: Commit action failed: %v", t.id, err)
				handleError(err)
				rbErr := t.Rollback(commitCtx)
				if rbErr != nil {
					stack := ""
					if t.captureStackTrace {
						stack = string(debug.Stack())
					}
					return TransactionError{TxID: t.id, Err: fmt.Errorf("commit failed: %v; rollback failed: %v", err, rbErr), Code: "COMMIT_ERR", Category: "FATAL", Action: "commit", StackTrace: stack}
				}
				t.metrics.IncErrorCount()
				stack := ""
				if t.captureStackTrace {
					stack = string(debug.Stack())
				}
				return TransactionError{TxID: t.id, Err: fmt.Errorf("commit action failed: %v", err), Code: "COMMIT_ERR", Category: "TRANSIENT", Action: "commit", StackTrace: stack}
			} else {
				t.metrics.RecordActionDuration("commit_action", time.Since(start))
			}
			if t.lifecycleHooks != nil && t.lifecycleHooks.OnAfterCommitAction != nil {
				t.lifecycleHooks.OnAfterCommitAction(t.id, desc, commitCtx)
			}
		}
	}

	for i, actionWP := range t.commitActionsWithPolicy {
		desc := fmt.Sprintf("commit action (custom policy) %d in transaction %d", i, t.id)
		if t.lifecycleHooks != nil && t.lifecycleHooks.OnBeforeCommitAction != nil {
			t.lifecycleHooks.OnBeforeCommitAction(t.id, desc, commitCtx)
		}
		start := time.Now()
		if err := retryAction(commitCtx, func(ctx context.Context) error {
			return safeAction(ctx, actionWP.fn, desc)
		}, desc, *actionWP.rp); err != nil {
			t.mu.Lock()
			t.state = StateFailed
			t.mu.Unlock()
			t.Log(logs.ErrorLevel, "Transaction %d: Commit action (custom policy) failed: %v", t.id, err)
			handleError(err)
			rbErr := t.Rollback(commitCtx)
			if rbErr != nil {
				stack := ""
				if t.captureStackTrace {
					stack = string(debug.Stack())
				}
				return TransactionError{TxID: t.id, Err: fmt.Errorf("commit custom policy error: %v; rollback failed: %v", err, rbErr), Code: "COMMIT_ERR", Category: "FATAL", Action: "commit", StackTrace: stack}
			}
			t.metrics.IncErrorCount()
			stack := ""
			if t.captureStackTrace {
				stack = string(debug.Stack())
			}
			return TransactionError{TxID: t.id, Err: fmt.Errorf("commit custom policy error: %v", err), Code: "COMMIT_ERR", Category: "TRANSIENT", Action: "commit", StackTrace: stack}
		} else {
			t.metrics.RecordActionDuration("commit_action_custom", time.Since(start))
		}
		if t.lifecycleHooks != nil && t.lifecycleHooks.OnAfterCommitAction != nil {
			t.lifecycleHooks.OnAfterCommitAction(t.id, desc, commitCtx)
		}
	}

	if t.parallelCommit {
		var wg sync.WaitGroup
		errCh := make(chan error, len(t.resources))
		for i, res := range t.resources {
			wg.Add(1)
			go func(i int, res TransactionalResource) {
				defer wg.Done()
				desc := fmt.Sprintf("resource commit %d for transaction %d", i, t.id)
				if t.lifecycleHooks != nil && t.lifecycleHooks.OnBeforeCommitAction != nil {
					t.lifecycleHooks.OnBeforeCommitAction(t.id, desc, commitCtx)
				}
				start := time.Now()
				if err := retryAction(commitCtx, func(ctx context.Context) error {
					return safeResourceCommit(ctx, res, i, t.id)
				}, desc, t.retryPolicy); err != nil {
					errCh <- err
					t.metrics.IncRetryCount()
				} else {
					t.metrics.RecordActionDuration("resource_commit", time.Since(start))
				}
				if t.lifecycleHooks != nil && t.lifecycleHooks.OnAfterCommitAction != nil {
					t.lifecycleHooks.OnAfterCommitAction(t.id, desc, commitCtx)
				}
			}(i, res)
		}
		wg.Wait()
		close(errCh)
		if len(errCh) > 0 {
			var errList []error
			for err := range errCh {
				errList = append(errList, err)
			}
			t.mu.Lock()
			t.state = StateFailed
			t.mu.Unlock()
			rbErr := t.Rollback(commitCtx)
			if rbErr != nil {
				handleError(rbErr)
				stack := ""
				if t.captureStackTrace {
					stack = string(debug.Stack())
				}
				return TransactionError{TxID: t.id, Err: fmt.Errorf("resource commit errors: %v; rollback failed: %v", errList, rbErr), Code: "RES_COMMIT_ERR", Category: "FATAL", Action: "commit", StackTrace: stack}
			}
			handleError(fmt.Errorf("resource commit errors: %v", errList))
			t.metrics.IncErrorCount()
			stack := ""
			if t.captureStackTrace {
				stack = string(debug.Stack())
			}
			return TransactionError{TxID: t.id, Err: fmt.Errorf("resource commit errors: %v", errList), Code: "RES_COMMIT_ERR", Category: "TRANSIENT", Action: "commit", StackTrace: stack}
		}
	} else {
		for i, res := range t.resources {
			desc := fmt.Sprintf("resource commit %d for transaction %d", i, t.id)
			if t.lifecycleHooks != nil && t.lifecycleHooks.OnBeforeCommitAction != nil {
				t.lifecycleHooks.OnBeforeCommitAction(t.id, desc, commitCtx)
			}
			start := time.Now()
			if err := retryAction(commitCtx, func(ctx context.Context) error {
				return safeResourceCommit(ctx, res, i, t.id)
			}, desc, t.retryPolicy); err != nil {
				t.mu.Lock()
				t.state = StateFailed
				t.mu.Unlock()
				t.Log(logs.ErrorLevel, "Transaction %d: Resource commit failed: %v", t.id, err)
				handleError(err)
				rbErr := t.Rollback(commitCtx)
				if rbErr != nil {
					stack := ""
					if t.captureStackTrace {
						stack = string(debug.Stack())
					}
					return TransactionError{TxID: t.id, Err: fmt.Errorf("resource commit failed: %v; rollback failed: %v", err, rbErr), Code: "RES_COMMIT_ERR", Category: "FATAL", Action: "commit", StackTrace: stack}
				}
				t.metrics.IncErrorCount()
				stack := ""
				if t.captureStackTrace {
					stack = string(debug.Stack())
				}
				return TransactionError{TxID: t.id, Err: fmt.Errorf("resource commit failed: %v", err), Code: "RES_COMMIT_ERR", Category: "TRANSIENT", Action: "commit", StackTrace: stack}
			} else {
				t.metrics.RecordActionDuration("resource_commit", time.Since(start))
			}
			if t.lifecycleHooks != nil && t.lifecycleHooks.OnAfterCommitAction != nil {
				t.lifecycleHooks.OnAfterCommitAction(t.id, desc, commitCtx)
			}
		}
	}

	t.mu.Lock()
	t.state = StateCommitted
	t.rollbackActions = nil
	t.mu.Unlock()
	if batch != nil {
		t.Log(logs.InfoLevel, "Transaction %d committed successfully for batch %v", t.id, batch)
	} else {
		t.Log(logs.InfoLevel, "Transaction %d committed successfully", t.id)
	}
	if t.distributedCoordinator != nil {
		if err := t.distributedCoordinator.CommitDistributed(t); err != nil {
			return fmt.Errorf("distributed commit error: %w", err)
		}
	}
	if err := t.runCleanup(commitCtx); err != nil {
		handleError(err)
		t.metrics.IncErrorCount()
		stack := ""
		if t.captureStackTrace {
			stack = string(debug.Stack())
		}
		return TransactionError{TxID: t.id, Err: fmt.Errorf("commit cleanup error: %v", err), Code: "CLEANUP_ERR", Category: "TRANSIENT", Action: "cleanup", StackTrace: stack}
	}
	t.metrics.RecordCommitDuration(time.Since(startTime))
	t.metrics.IncCommitCount()
	if t.lifecycleHooks != nil && t.lifecycleHooks.OnAfterCommit != nil {
		t.lifecycleHooks.OnAfterCommit(t.id, commitCtx)
	}
	if t.auditLogger != nil {
		t.auditLogger.LogEvent("Commit", map[string]any{"tx_id": t.id, "state": t.state.String()})
	}
	return nil
}

func (t *Transaction) AsyncCommitWithResult(ctx context.Context) <-chan AsyncResult {
	resultCh := make(chan AsyncResult, 1)
	go func() {
		err := t.Commit(ctx)
		t.mu.RLock()
		state := t.state
		t.mu.RUnlock()
		resultCh <- AsyncResult{TxID: t.id, Err: err, State: state}
	}()
	return resultCh
}

func (t *Transaction) AsyncCommit(ctx context.Context) <-chan error {
	resultCh := make(chan error, 1)
	go func() {
		resultCh <- t.Commit(ctx)
	}()
	return resultCh
}

func (t *Transaction) Rollback(ctx context.Context) error {
	startTime := time.Now()

	var rbCtx context.Context
	var cancel context.CancelFunc
	t.mu.RLock()
	rt := t.rollbackTimeout
	t.mu.RUnlock()
	if rt > 0 {
		rbCtx, cancel = context.WithTimeout(ctx, rt)
		defer cancel()
	} else {
		rbCtx = ctx
	}

	var span metrics.Span
	if t.tracer != nil {
		rbCtx, span = t.tracer.StartSpan(rbCtx, fmt.Sprintf("Transaction-%d-Rollback", t.id))
		defer span.End()
	}

	if t.lifecycleHooks != nil && t.lifecycleHooks.OnBeforeRollback != nil {
		t.lifecycleHooks.OnBeforeRollback(t.id, rbCtx)
	}

	t.mu.Lock()
	if t.state != StateInProgress && t.state != StateFailed && !t.abortCalled {
		t.mu.Unlock()
		return fmt.Errorf("cannot rollback transaction %d in state %s", t.id, t.state)
	}
	if err := rbCtx.Err(); err != nil {
		t.mu.Unlock()
		t.Log(logs.ErrorLevel, "Transaction %d: Context error during rollback: %v", t.id, err)
		return fmt.Errorf("rollback context error: %w", err)
	}
	actions := t.rollbackActions
	resources := t.resources
	t.rollbackActions = nil
	t.resources = nil
	t.state = StateRolledBack
	t.mu.Unlock()

	if t.testHooks != nil && t.testHooks.SimulateRollbackFailure {
		err := fmt.Errorf("simulated rollback failure")
		t.Log(logs.ErrorLevel, "Transaction %d: %v", t.id, err)
		t.metrics.IncErrorCount()
		stack := ""
		if t.captureStackTrace {
			stack = string(debug.Stack())
		}
		return TransactionError{TxID: t.id, Err: err, Code: "SIM_RB_FAIL", Category: "FATAL", Action: "rollback", StackTrace: stack}
	}

	var errs []error
	t.Log(logs.InfoLevel, "Transaction %d: Rolling back", t.id)

	if t.parallelRollback {
		var wg sync.WaitGroup
		errCh := make(chan error, len(actions))
		for i := len(actions) - 1; i >= 0; i-- {
			wg.Add(1)
			go func(i int, action func(ctx context.Context) error) {
				defer wg.Done()
				desc := fmt.Sprintf("rollback action %d in transaction %d", i, t.id)
				if t.lifecycleHooks != nil && t.lifecycleHooks.OnBeforeRollbackAction != nil {
					t.lifecycleHooks.OnBeforeRollbackAction(t.id, desc, rbCtx)
				}
				if err := retryAction(rbCtx, func(ctx context.Context) error {
					return safeAction(ctx, action, desc)
				}, desc, t.retryPolicy); err != nil {
					errCh <- err
				}
				if t.lifecycleHooks != nil && t.lifecycleHooks.OnAfterRollbackAction != nil {
					t.lifecycleHooks.OnAfterRollbackAction(t.id, desc, rbCtx)
				}
			}(i, actions[i])
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			errs = append(errs, err)
			t.Log(logs.ErrorLevel, "Transaction %d: Error during parallel rollback action: %v", t.id, err)
		}
	} else {
		for i := len(actions) - 1; i >= 0; i-- {
			desc := fmt.Sprintf("rollback action %d in transaction %d", i, t.id)
			if t.lifecycleHooks != nil && t.lifecycleHooks.OnBeforeRollbackAction != nil {
				t.lifecycleHooks.OnBeforeRollbackAction(t.id, desc, rbCtx)
			}
			if err := retryAction(rbCtx, func(ctx context.Context) error {
				return safeAction(ctx, actions[i], desc)
			}, desc, t.retryPolicy); err != nil {
				errs = append(errs, err)
				t.Log(logs.ErrorLevel, "Transaction %d: Error during rollback action %d: %v", t.id, i, err)
			}
			if t.lifecycleHooks != nil && t.lifecycleHooks.OnAfterRollbackAction != nil {
				t.lifecycleHooks.OnAfterRollbackAction(t.id, desc, rbCtx)
			}
		}
	}

	if t.parallelRollback {
		var wg sync.WaitGroup
		errCh := make(chan error, len(resources))
		for i := len(resources) - 1; i >= 0; i-- {
			wg.Add(1)
			go func(i int, res TransactionalResource) {
				defer wg.Done()
				desc := fmt.Sprintf("resource rollback %d for transaction %d", i, t.id)
				if t.lifecycleHooks != nil && t.lifecycleHooks.OnBeforeRollbackAction != nil {
					t.lifecycleHooks.OnBeforeRollbackAction(t.id, desc, rbCtx)
				}
				if err := retryAction(rbCtx, func(ctx context.Context) error {
					return safeResourceRollback(ctx, res, i, t.id)
				}, desc, t.retryPolicy); err != nil {
					errCh <- err
				}
				if t.lifecycleHooks != nil && t.lifecycleHooks.OnAfterRollbackAction != nil {
					t.lifecycleHooks.OnAfterRollbackAction(t.id, desc, rbCtx)
				}
			}(i, resources[i])
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			errs = append(errs, err)
			t.Log(logs.ErrorLevel, "Transaction %d: Error during parallel resource rollback: %v", t.id, err)
		}
	} else {
		for i := len(resources) - 1; i >= 0; i-- {
			desc := fmt.Sprintf("resource rollback %d for transaction %d", i, t.id)
			if t.lifecycleHooks != nil && t.lifecycleHooks.OnBeforeRollbackAction != nil {
				t.lifecycleHooks.OnBeforeRollbackAction(t.id, desc, rbCtx)
			}
			if err := retryAction(rbCtx, func(ctx context.Context) error {
				return safeResourceRollback(ctx, resources[i], i, t.id)
			}, desc, t.retryPolicy); err != nil {
				errs = append(errs, err)
				t.Log(logs.ErrorLevel, "Transaction %d: Error during resource rollback %d: %v", t.id, i, err)
			}
			if t.lifecycleHooks != nil && t.lifecycleHooks.OnAfterRollbackAction != nil {
				t.lifecycleHooks.OnAfterRollbackAction(t.id, desc, rbCtx)
			}
		}
	}

	if cleanupErr := t.runCleanup(rbCtx); cleanupErr != nil {
		errs = append(errs, cleanupErr)
	}

	if len(errs) > 0 {
		t.metrics.IncErrorCount()
		stack := ""
		if t.captureStackTrace {
			stack = string(debug.Stack())
		}
		return TransactionError{TxID: t.id, Err: fmt.Errorf("rollback encountered errors: %v", errs), Code: "RB_ERR", Category: "TRANSIENT", Action: "rollback", StackTrace: stack}
	}
	t.metrics.RecordRollbackDuration(time.Since(startTime))
	t.metrics.IncRollbackCount()
	if t.distributedCoordinator != nil {
		if err := t.distributedCoordinator.RollbackDistributed(t); err != nil {
			return fmt.Errorf("distributed rollback error: %w", err)
		}
	}
	if t.lifecycleHooks != nil && t.lifecycleHooks.OnAfterRollback != nil {
		t.lifecycleHooks.OnAfterRollback(t.id, rbCtx)
	}
	if t.auditLogger != nil {
		t.auditLogger.LogEvent("Rollback", map[string]any{"tx_id": t.id, "state": t.state.String()})
	}
	return nil
}

func (t *Transaction) AsyncRollbackWithResult(ctx context.Context) <-chan AsyncResult {
	resultCh := make(chan AsyncResult, 1)
	go func() {
		err := t.Rollback(ctx)
		t.mu.RLock()
		state := t.state
		t.mu.RUnlock()
		resultCh <- AsyncResult{TxID: t.id, Err: err, State: state}
	}()
	return resultCh
}

func (t *Transaction) AsyncRollback(ctx context.Context) <-chan error {
	resultCh := make(chan error, 1)
	go func() {
		resultCh <- t.Rollback(ctx)
	}()
	return resultCh
}

func (t *Transaction) Abort(ctx context.Context) error {
	t.mu.Lock()
	t.abortCalled = true
	t.mu.Unlock()
	t.Log(logs.InfoLevel, "Transaction %d: Aborting", t.id)
	return t.Rollback(ctx)
}

func (t *Transaction) Close() error {
	t.mu.RLock()
	currentState := t.state
	t.mu.RUnlock()
	if t.lifecycleHooks != nil && t.lifecycleHooks.OnClose != nil {
		t.lifecycleHooks.OnClose(t.id, context.Background())
	}
	if currentState == StateCommitted || currentState == StateRolledBack {
		txPool.Put(t)
		return nil
	}
	t.Log(logs.InfoLevel, "Transaction %d: Closing - performing rollback", t.id)
	err := t.Rollback(context.Background())
	txPool.Put(t)
	return err
}

func (t *Transaction) CreateSavepoint(_ context.Context) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return 0, fmt.Errorf("cannot create savepoint in transaction %d state %s", t.id, t.state)
	}
	sp := len(t.rollbackActions)
	t.Log(logs.InfoLevel, "Transaction %d: Created savepoint at index %d", t.id, sp)
	return sp, nil
}

func (t *Transaction) CreateNamedSavepoint(name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return fmt.Errorf("cannot create named savepoint in transaction %d state %s", t.id, t.state)
	}
	if _, exists := t.savepoints[name]; exists {
		return fmt.Errorf("savepoint '%s' already exists in transaction %d", name, t.id)
	}
	t.savepoints[name] = len(t.rollbackActions)
	t.Log(logs.InfoLevel, "Transaction %d: Created named savepoint '%s' at index %d", t.id, name, t.savepoints[name])
	return nil
}

func (t *Transaction) RollbackToNamedSavepoint(ctx context.Context, name string) error {
	t.mu.RLock()
	sp, exists := t.savepoints[name]
	t.mu.RUnlock()
	if !exists {
		return fmt.Errorf("savepoint '%s' does not exist in transaction %d", name, t.id)
	}
	return t.RollbackToSavepoint(ctx, sp)
}

func (t *Transaction) ReleaseNamedSavepoint(name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	sp, exists := t.savepoints[name]
	if !exists {
		return fmt.Errorf("savepoint '%s' does not exist in transaction %d", name, t.id)
	}
	if sp < 0 || sp > len(t.rollbackActions) {
		return fmt.Errorf("invalid savepoint index %d for transaction %d", sp, t.id)
	}
	t.rollbackActions = t.rollbackActions[:sp]
	delete(t.savepoints, name)
	t.Log(logs.InfoLevel, "Transaction %d: Released named savepoint '%s'", t.id, name)
	return nil
}

func (t *Transaction) RollbackToSavepoint(ctx context.Context, sp int) error {
	t.mu.Lock()
	if t.state != StateInProgress {
		t.mu.Unlock()
		return fmt.Errorf("cannot rollback to savepoint in transaction %d state %s", t.id, t.state)
	}
	if sp < 0 || sp > len(t.rollbackActions) {
		t.mu.Unlock()
		return fmt.Errorf("invalid savepoint index %d for transaction %d", sp, t.id)
	}
	actionsToRollback := t.rollbackActions[sp:]
	t.rollbackActions = t.rollbackActions[:sp]
	t.mu.Unlock()

	var errs []error
	t.Log(logs.InfoLevel, "Transaction %d: Rolling back to savepoint at index %d", t.id, sp)
	for i := len(actionsToRollback) - 1; i >= 0; i-- {
		desc := fmt.Sprintf("rollback savepoint action %d in transaction %d", i, t.id)
		if err := retryAction(ctx, func(ctx context.Context) error {
			return safeAction(ctx, actionsToRollback[i], desc)
		}, desc, t.retryPolicy); err != nil {
			errs = append(errs, err)
			t.Log(logs.ErrorLevel, "Transaction %d: Error during rollback action at savepoint: %v", t.id, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("transaction %d rollback to savepoint encountered errors: %v", t.id, errs)
	}
	return nil
}

func (t *Transaction) GetState() TxState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state
}

func (t *Transaction) GetID() int64 {
	return t.id
}

type NestedTransaction struct {
	parent    *Transaction
	savepoint int
	active    bool
}

func (t *Transaction) BeginNested(ctx context.Context) (*NestedTransaction, error) {
	sp, err := t.CreateSavepoint(ctx)
	if err != nil {
		return nil, err
	}
	nt := &NestedTransaction{
		parent:    t,
		savepoint: sp,
		active:    true,
	}
	t.Log(logs.InfoLevel, "Transaction %d: Nested transaction started at savepoint %d", t.id, sp)
	return nt, nil
}

func (nt *NestedTransaction) BeginNested(ctx context.Context) (*NestedTransaction, error) {
	return nt.parent.BeginNested(ctx)
}

func (t *Transaction) ReleaseSavepoint(_ context.Context, sp int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != StateInProgress {
		return fmt.Errorf("cannot release savepoint in transaction %d state %s", t.id, t.state)
	}
	if sp < 0 || sp > len(t.rollbackActions) {
		return fmt.Errorf("invalid savepoint index %d for transaction %d", sp, t.id)
	}
	t.rollbackActions = t.rollbackActions[:sp]
	t.Log(logs.InfoLevel, "Transaction %d: Released savepoint at index %d", t.id, sp)
	return nil
}

func (nt *NestedTransaction) Commit(ctx context.Context) error {
	if !nt.active {
		return fmt.Errorf("nested transaction already closed")
	}
	if err := nt.parent.ReleaseSavepoint(ctx, nt.savepoint); err != nil {
		return err
	}
	nt.active = false
	nt.parent.Log(logs.InfoLevel, "Transaction %d: Nested transaction committed (released savepoint %d)", nt.parent.id, nt.savepoint)
	return nil
}

func (nt *NestedTransaction) Rollback(ctx context.Context) error {
	if !nt.active {
		return fmt.Errorf("nested transaction already closed")
	}
	if err := nt.parent.RollbackToSavepoint(ctx, nt.savepoint); err != nil {
		return err
	}
	nt.active = false
	nt.parent.Log(logs.InfoLevel, "Transaction %d: Nested transaction rolled back to savepoint %d", nt.parent.id, nt.savepoint)
	return nil
}

func (t *Transaction) runCleanup(ctx context.Context) error {
	t.mu.Lock()
	if t.cleanupCalled {
		t.mu.Unlock()
		return nil
	}
	actions := t.cleanupActions
	t.cleanupCalled = true
	t.mu.Unlock()

	var errs []error
	for i, action := range actions {
		desc := fmt.Sprintf("cleanup action %d in transaction %d", i, t.id)
		if err := safeAction(ctx, action, desc); err != nil {
			errs = append(errs, err)
			t.Log(logs.ErrorLevel, "Transaction %d: Error during cleanup action %d: %v", t.id, i, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("cleanup encountered errors: %v", errs)
	}
	return nil
}

func RunInTransaction(ctx context.Context, fn func(tx *Transaction) error) error {
	tx := NewTransaction()
	if err := tx.Begin(ctx); err != nil {
		return err
	}
	defer func() {
		_ = tx.Close()
	}()
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			stack := ""
			if tx.captureStackTrace {
				stack = string(debug.Stack())
			}
			return TransactionError{TxID: tx.id, Err: fmt.Errorf("rollback error: %v (original error: %v)", rbErr, err), Code: "RUNINTRANS_RB_ERR", Category: "FATAL", Action: "rollback", StackTrace: stack}
		}
		return err
	}
	return tx.Commit(ctx)
}
