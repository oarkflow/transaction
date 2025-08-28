package transaction

import (
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/transaction/storage"
)

// RecoverInProgress scans storage for in-progress transactions and attempts
// to resolve them using the provided resolver callback. If resolver is nil,
// a default conservative resolver will mark InProgress/Preparing transactions
// as RolledBack and will append a recovery log entry for Prepared/Committing
// transactions for manual review.
func RecoverInProgress(st storage.Storage, resolver func(txID int64, state storage.TxState, logs []storage.LogEntry) error) error {
	if st == nil {
		return fmt.Errorf("storage is nil; cannot recover")
	}
	ids, err := st.ListInProgress()
	if err != nil {
		return fmt.Errorf("listing in-progress txs: %w", err)
	}
	if len(ids) == 0 {
		return nil
	}
	var aggErr error
	for _, txID := range ids {
		state, meta, err := st.LoadTxState(txID)
		if err != nil {
			log.Printf("recovery: error loading state for tx %d: %v", txID, err)
			aggErr = err
			continue
		}
		_ = meta
		logs, _ := st.GetLog(txID)
		if resolver != nil {
			if err := resolver(txID, state, logs); err != nil {
				log.Printf("recovery: resolver error for tx %d: %v", txID, err)
				aggErr = err
			}
			continue
		}

		// default conservative resolver
		switch state {
		case storage.TxStatePrepared, storage.TxStateCommitting:
			// Unable to safely decide; append a recovery-needed log entry.
			_ = st.AppendLog(txID, storage.LogEntry{Key: "recovery_needed", Type: "recovery", Applied: false, Data: map[string]interface{}{"note": "prepared_or_committing"}})
			log.Printf("recovery: tx %d in state %s - manual review required (marked in log)", txID, state)
		default:
			// For other states, attempt to roll back conservatively.
			if err := st.SaveTxState(txID, storage.TxStateRolledBack, map[string]any{"recovered_at": time.Now().UTC().String()}); err != nil {
				log.Printf("recovery: failed to mark tx %d rolled back: %v", txID, err)
				aggErr = err
				continue
			}
			_ = st.AppendLog(txID, storage.LogEntry{Key: "recovery_rollback", Type: "recovery", Applied: true, Data: map[string]interface{}{"note": "auto-rolled-back"}})
			log.Printf("recovery: tx %d marked RolledBack", txID)
		}
	}
	return aggErr
}
