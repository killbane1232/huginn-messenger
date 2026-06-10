package store

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type StoredPeer struct {
	PeerID        string
	EncryptionKey string
	SignatureKey  string
	LastSeen      time.Time
}

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
			PRIMARY KEY (file_id, chunk_index)
		);
	`); err != nil {
		return nil, err
	}
	return &SQLiteStore{db: db}, nil
}

func (s *SQLiteStore) Close() error {
	return retry(func() error {
		return s.db.Close()
	})
}

func (s *SQLiteStore) StoreChunk(fileID string, chunkIndex int, data []byte) error {
	return retry(func() error {
		_, err := s.db.Exec("INSERT OR REPLACE INTO chunks (file_id, chunk_index, data) VALUES (?, ?, ?)",
			fileID, chunkIndex, data)
		return err
	})
}

func (s *SQLiteStore) GetChunk(fileID string, chunkIndex int) ([]byte, error) {
	return retryWith(func() ([]byte, error) {
		var data []byte
		err := s.db.QueryRow("SELECT data FROM chunks WHERE file_id = ? AND chunk_index = ?",
			fileID, chunkIndex).Scan(&data)
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return data, err
	})
}

func (s *SQLiteStore) DeleteChunks(fileID string) error {
	return retry(func() error {
		_, err := s.db.Exec("DELETE FROM chunks WHERE file_id = ?", fileID)
		return err
	})
}

func (s *SQLiteStore) ListChunkFiles() ([]string, error) {
	return retryWith(func() ([]string, error) {
		rows, err := s.db.Query("SELECT DISTINCT file_id FROM chunks")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var files []string
		for rows.Next() {
			var f string
			if err := rows.Scan(&f); err != nil {
				return nil, err
			}
			files = append(files, f)
		}
		return files, rows.Err()
	})
}

func (s *SQLiteStore) ListChunks(fileID string) (map[int][]byte, error) {
	return retryWith(func() (map[int][]byte, error) {
		rows, err := s.db.Query("SELECT chunk_index, data FROM chunks WHERE file_id = ?", fileID)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		result := make(map[int][]byte)
		for rows.Next() {
			var idx int
			var data []byte
			if err := rows.Scan(&idx, &data); err != nil {
				return nil, err
			}
			result[idx] = data
		}
		return result, rows.Err()
	})
}

func (s *SQLiteStore) SaveMessage(msg_uid string, login string, senderLogin string, chatID string, data []byte, created_at time.Time) error {
	return retry(func() error {
		_, err := s.db.Exec("INSERT INTO messages (message_uid, login, sender_login, chat_id, data, created_at) VALUES (?, ?, ?, ?, ?, ?)",
			msg_uid, login, senderLogin, chatID, data, created_at)
		return err
	})
}

func (s *SQLiteStore) GetMessages(peerID string) ([][]byte, error) {
	return retryWith(func() ([][]byte, error) {
		rows, err := s.db.Query("SELECT data FROM messages WHERE login = ?", peerID)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var result [][]byte
		for rows.Next() {
			var data []byte
			if err := rows.Scan(&data); err != nil {
				return nil, err
			}
			result = append(result, data)
		}
		return result, rows.Err()
	})
}

func (s *SQLiteStore) StorePeer(login string, peerID string, encryptionKey, signatureKey string, lastSeen time.Time) error {
	return retry(func() error {
		_, err := s.db.Exec(
			"INSERT OR REPLACE INTO stored_peers (login, peer_id, encryption_key, signature_key, last_seen) VALUES (?, ?, ?, ?, ?)",
			login, peerID, encryptionKey, signatureKey, lastSeen)
		return err
	})
}

func (s *SQLiteStore) GetStoredPeer(login, peerID string) (*StoredPeer, error) {
	return retryWith(func() (*StoredPeer, error) {
		row := s.db.QueryRow(
			"SELECT peer_id, encryption_key, signature_key, last_seen FROM stored_peers WHERE login = ? AND peer_id = ?",
			login, peerID)
		var p StoredPeer
		err := row.Scan(&p.PeerID, &p.EncryptionKey, &p.SignatureKey, &p.LastSeen)
		if err == sql.ErrNoRows {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return &p, nil
	})
}

func (s *SQLiteStore) SearchStoredPeers(query string) ([]StoredPeer, error) {
	return retryWith(func() ([]StoredPeer, error) {
		rows, err := s.db.Query(
			"SELECT peer_id, encryption_key, signature_key, last_seen FROM stored_peers WHERE peer_id LIKE ? ORDER BY last_seen DESC",
			"%"+query+"%")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var peers []StoredPeer
		for rows.Next() {
			var p StoredPeer
			if err := rows.Scan(&p.PeerID, &p.EncryptionKey, &p.SignatureKey, &p.LastSeen); err != nil {
				return nil, err
			}
			peers = append(peers, p)
		}
		return peers, rows.Err()
	})
}

func (s *SQLiteStore) GetStoredPeers() ([]StoredPeer, error) {
	return retryWith(func() ([]StoredPeer, error) {
		rows, err := s.db.Query("SELECT peer_id, encryption_key, signature_key, last_seen FROM stored_peers ORDER BY last_seen DESC")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var peers []StoredPeer
		for rows.Next() {
			var p StoredPeer
			if err := rows.Scan(&p.PeerID, &p.EncryptionKey, &p.SignatureKey, &p.LastSeen); err != nil {
				return nil, err
			}
			peers = append(peers, p)
		}
		return peers, rows.Err()
	})
}

func (s *SQLiteStore) StorePendingChunk(pc *PendingChunk) error {
	return retry(func() error {
		_, err := s.db.Exec(
			`INSERT OR REPLACE INTO pending_chunks (file_id, chunk_index, recipient_id, sender_id, data, hash, signature, created_at, placed) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			pc.FileID, pc.ChunkIndex, pc.RecipientID, pc.SenderID, pc.Data, pc.Hash, pc.Signature, pc.CreatedAt, pc.Placed)
		return err
	})
}

func (s *SQLiteStore) GetUnplacedChunks() ([]PendingChunk, error) {
	return retryWith(func() ([]PendingChunk, error) {
		rows, err := s.db.Query(
			`SELECT file_id, chunk_index, recipient_id, sender_id, data, hash, signature, created_at, placed FROM pending_chunks WHERE placed = 0 ORDER BY created_at ASC`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var chunks []PendingChunk
		for rows.Next() {
			var c PendingChunk
			if err := rows.Scan(&c.FileID, &c.ChunkIndex, &c.RecipientID, &c.SenderID, &c.Data, &c.Hash, &c.Signature, &c.CreatedAt, &c.Placed); err != nil {
				return nil, err
			}
			chunks = append(chunks, c)
		}
		return chunks, rows.Err()
	})
}

func (s *SQLiteStore) MarkChunkPlaced(fileID string, chunkIndex int) error {
	return retry(func() error {
		_, err := s.db.Exec(`UPDATE pending_chunks SET placed = 1 WHERE file_id = ? AND chunk_index = ?`, fileID, chunkIndex)
		return err
	})
}

func (s *SQLiteStore) GetPendingChunksByMessage(fileID string) ([]PendingChunk, error) {
	return retryWith(func() ([]PendingChunk, error) {
		rows, err := s.db.Query(
			`SELECT file_id, chunk_index, recipient_id, sender_id, data, hash, signature, created_at, placed FROM pending_chunks WHERE file_id = ? ORDER BY chunk_index ASC`, fileID)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var chunks []PendingChunk
		for rows.Next() {
			var c PendingChunk
			if err := rows.Scan(&c.FileID, &c.ChunkIndex, &c.RecipientID, &c.SenderID, &c.Data, &c.Hash, &c.Signature, &c.CreatedAt, &c.Placed); err != nil {
				return nil, err
			}
			chunks = append(chunks, c)
		}
		return chunks, rows.Err()
	})
}

func (s *SQLiteStore) DeletePendingChunks(fileID string) error {
	return retry(func() error {
		_, err := s.db.Exec(`DELETE FROM pending_chunks WHERE file_id = ?`, fileID)
		return err
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
