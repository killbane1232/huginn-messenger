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
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS chunks (
			file_id TEXT NOT NULL,
			chunk_index INTEGER NOT NULL,
			data BLOB NOT NULL,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			ttl_seconds INTEGER NOT NULL DEFAULT 604800,
			PRIMARY KEY (file_id, chunk_index)
		);
		CREATE TABLE IF NOT EXISTS stored_peers (
			login TEXT NOT NULL,
			peer_id TEXT NOT NULL,
			encryption_key TEXT NOT NULL DEFAULT '',
			signature_key TEXT NOT NULL DEFAULT '',
			last_seen DATETIME NOT NULL,
			PRIMARY KEY (login, peer_id)
		);
		CREATE TABLE IF NOT EXISTS messages (
			message_uid TEXT PRIMARY KEY,
			login TEXT NOT NULL,
			sender_login TEXT NOT NULL,
			chat_id TEXT NOT NULL,
			data BLOB NOT NULL,
			created_at DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS pending_chunks (
			file_id TEXT NOT NULL,
			chunk_index INTEGER NOT NULL,
			recipient_id TEXT NOT NULL,
			sender_id TEXT NOT NULL,
			data BLOB NOT NULL,
			hash TEXT NOT NULL,
			signature TEXT NOT NULL,
			created_at DATETIME NOT NULL,
			placed BOOLEAN NOT NULL DEFAULT 0,
			ttl_seconds INTEGER NOT NULL DEFAULT 604800,
			PRIMARY KEY (file_id, chunk_index)
		);
	`); err != nil {
		return nil, err
	}

	s := &SQLiteStore{db: db}
	s.migrate()
	return s, nil
}

func (s *SQLiteStore) migrate() {
	migrations := []string{
		"ALTER TABLE chunks ADD COLUMN created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
		"ALTER TABLE chunks ADD COLUMN ttl_seconds INTEGER NOT NULL DEFAULT 604800",
		"ALTER TABLE pending_chunks ADD COLUMN ttl_seconds INTEGER NOT NULL DEFAULT 604800",
	}
	for _, m := range migrations {
		s.db.Exec(m)
	}
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
