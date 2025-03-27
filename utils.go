package transaction

import (
	"context"
	"fmt"
	"log"
	"time"
)

func retryAction(ctx context.Context, action func(ctx context.Context) error, desc string, rp RetryPolicy) error {
	var err error
	for attempt := 0; attempt <= rp.MaxRetries; attempt++ {
		if attempt > 0 {
			var delay time.Duration
			if rp.BackoffStrategy != nil {
				delay = rp.BackoffStrategy(attempt)
			} else {
				delay = rp.Delay
			}
			// Log the retry attempt.
			log.Printf("Retrying (%d/%d) for %s", attempt, rp.MaxRetries, desc)
			// Wait for the backoff duration.
			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during retry of %s: %w", desc, ctx.Err())
			case <-time.After(delay):
			}
		}
		err = action(ctx)
		if err == nil {
			return nil
		}
		if rp.ShouldRetry != nil && !rp.ShouldRetry(err) {
			return fmt.Errorf("non-retryable error in %s: %w", desc, err)
		}
	}
	return fmt.Errorf("action %s failed after %d attempts: %w", desc, rp.MaxRetries+1, err)
}

// safeAction wraps the execution of an action, recovering from panics.
func safeAction(ctx context.Context, action func(ctx context.Context) error, desc string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic recovered during %s: %v", desc, r)
		}
	}()
	err = action(ctx)
	return err
}

// safeResourceCommit wraps the commit call for a resource.
func safeResourceCommit(ctx context.Context, res TransactionalResource, index int, txID int64) error {
	desc := fmt.Sprintf("resource commit %d for transaction %d", index, txID)
	return safeAction(ctx, res.Commit, desc)
}

// safeResourceRollback wraps the rollback call for a resource.
func safeResourceRollback(ctx context.Context, res TransactionalResource, index int, txID int64) error {
	desc := fmt.Sprintf("resource rollback %d for transaction %d", index, txID)
	return safeAction(ctx, res.Rollback, desc)
}
