package store

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

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
	return retry(func() error {
		return s.db.Close()
	})
}

func isLocked(err error) bool {
	return strings.Contains(err.Error(), "database is locked")
}

func retry(fn func() error) error {
	var err error
	for i := 0; i < 10; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		if !isLocked(err) {
			return err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("database locked after retries: %w", err)
}

func retryWith[T any](fn func() (T, error)) (T, error) {
	var result T
	var err error
	for i := 0; i < 10; i++ {
		result, err = fn()
		if err == nil {
			return result, nil
		}
		if !isLocked(err) {
			return result, err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return result, fmt.Errorf("database locked after retries: %w", err)
}
