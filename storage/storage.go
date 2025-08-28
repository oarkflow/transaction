package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type TxState string

const (
	TxStateInitialized TxState = "Initialized"
	TxStateInProgress  TxState = "InProgress"
	TxStatePreparing   TxState = "Preparing"
	TxStatePrepared    TxState = "Prepared"
	TxStateCommitting  TxState = "Committing"
	TxStateCommitted   TxState = "Committed"
	TxStateRolledBack  TxState = "RolledBack"
	TxStateFailed      TxState = "Failed"
)

type LogEntry struct {
	Key       string                 `json:"key"`
	Type      string                 `json:"type"`
	Applied   bool                   `json:"applied"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

type Storage interface {
	SaveTxState(txID int64, state TxState, metadata map[string]any) error
	LoadTxState(txID int64) (TxState, map[string]any, error)
	AppendLog(txID int64, entry LogEntry) error
	GetLog(txID int64) ([]LogEntry, error)
	ListInProgress() ([]int64, error)
}

// FileStorage is a very small, simple file-backed storage for demo and
// development. It is NOT meant for production use beyond basic durability.
type FileStorage struct {
	dir string
	mu  sync.Mutex
}

func NewFileStorage(dir string) (*FileStorage, error) {
	if dir == "" {
		dir = "./txlogs"
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStorage{dir: dir}, nil
}

func (f *FileStorage) txFilePath(txID int64) string {
	return filepath.Join(f.dir, fmt.Sprintf("tx-%d.json", txID))
}

type persistedTx struct {
	TxID  int64          `json:"tx_id"`
	State TxState        `json:"state"`
	Meta  map[string]any `json:"meta,omitempty"`
	Log   []LogEntry     `json:"log,omitempty"`
}

func (f *FileStorage) read(txID int64) (*persistedTx, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := &persistedTx{TxID: txID, State: TxStateInitialized, Meta: map[string]any{}, Log: []LogEntry{}}
	path := f.txFilePath(txID)
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return p, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(b, p); err != nil {
		return nil, err
	}
	return p, nil
}

func (f *FileStorage) write(p *persistedTx) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := f.txFilePath(p.TxID)
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (f *FileStorage) SaveTxState(txID int64, state TxState, metadata map[string]any) error {
	p, err := f.read(txID)
	if err != nil {
		return err
	}
	p.State = state
	if metadata != nil {
		if p.Meta == nil {
			p.Meta = map[string]any{}
		}
		for k, v := range metadata {
			p.Meta[k] = v
		}
	}
	return f.write(p)
}

func (f *FileStorage) LoadTxState(txID int64) (TxState, map[string]any, error) {
	p, err := f.read(txID)
	if err != nil {
		return "", nil, err
	}
	return p.State, p.Meta, nil
}

func (f *FileStorage) AppendLog(txID int64, entry LogEntry) error {
	p, err := f.read(txID)
	if err != nil {
		return err
	}
	if p.Log == nil {
		p.Log = []LogEntry{}
	}
	entry.Timestamp = time.Now().UTC()
	p.Log = append(p.Log, entry)
	return f.write(p)
}

func (f *FileStorage) GetLog(txID int64) ([]LogEntry, error) {
	p, err := f.read(txID)
	if err != nil {
		return nil, err
	}
	return p.Log, nil
}

func (f *FileStorage) ListInProgress() ([]int64, error) {
	// naive implementation: list files under dir and read states
	files, err := os.ReadDir(f.dir)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for _, fi := range files {
		if fi.IsDir() {
			continue
		}
		var txID int64
		n, err := fmt.Sscanf(fi.Name(), "tx-%d.json", &txID)
		if err != nil || n != 1 {
			continue
		}
		p, err := f.read(txID)
		if err != nil {
			continue
		}
		if p.State == TxStateInProgress || p.State == TxStatePreparing || p.State == TxStatePrepared || p.State == TxStateCommitting {
			ids = append(ids, txID)
		}
	}
	return ids, nil
}
