package store

import (
	"database/sql"
	"fmt"
	"time"
	"sync"

	_ "modernc.org/sqlite"
)

type PendingChunk struct {
	FileID      string
	ChunkIndex  int
	RecipientID string
	SenderID    string
	Data        []byte
	Hash        string
	Signature   string
	CreatedAt   time.Time
	Placed      bool
	TTLSeconds  int
}

type SQLiteStore struct {
	db *sql.DB
	mu   sync.Mutex
}

func New(path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		return nil, err
	}
	if _, err := db.Exec(`PRAGMA busy_timeout=5000`); err != nil {
		return nil, err
	}

	if err := runMigrations(db); err != nil {
		return nil, fmt.Errorf("run migrations: %w", err)
	}

	return &SQLiteStore{db: db}, nil
}

func (s *SQLiteStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}
