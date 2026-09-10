package store

import (
	"database/sql"
	"time"
)

func (s *SQLiteStore) StoreChunk(fileID string, chunkIndex int, data []byte, ttlSeconds int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		"INSERT OR REPLACE INTO chunks (file_id, chunk_index, data, created_at, ttl_seconds) VALUES (?, ?, ?, ?, ?)",
		fileID, chunkIndex, data, time.Now(), ttlSeconds)
	return err
}

func (s *SQLiteStore) GetChunk(fileID string, chunkIndex int) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var data []byte
	err := s.db.QueryRow("SELECT data FROM chunks WHERE file_id = ? AND chunk_index = ?",
		fileID, chunkIndex).Scan(&data)
	if err == sql.ErrNoRows {
		// Recover outgoing payloads removed by older housekeeping code.
		var created time.Time
		var ttl int
		err = s.db.QueryRow("SELECT data, created_at, ttl_seconds FROM pending_chunks WHERE file_id = ? AND chunk_index = ?", fileID, chunkIndex).Scan(&data, &created, &ttl)
		if err == sql.ErrNoRows || (err == nil && !time.Now().Before(created.Add(time.Duration(ttl)*time.Second))) {
			return nil, nil
		}
	}
	return data, err
}

func (s *SQLiteStore) DeleteChunks(fileID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec("DELETE FROM chunks WHERE file_id = ?", fileID)
	return err
}

func (s *SQLiteStore) ListChunkFiles() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
}

func (s *SQLiteStore) ListChunks(fileID string) (map[int][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
}

func (s *SQLiteStore) DeleteExpiredChunks(now time.Time) error {
	return s.deleteExpiredPayloads("chunks", now)
}

func (s *SQLiteStore) DeleteChunksWithMessage() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`DELETE FROM chunks WHERE file_id IN (SELECT message_uid FROM messages)
		 AND file_id NOT IN (SELECT file_id FROM pending_chunks)`)
	return err
}
func (s *SQLiteStore) DeleteChunk(fileID string, index int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec("DELETE FROM chunks WHERE file_id = ? AND chunk_index = ?", fileID, index)
	return err
}
