package store

import "time"

type FailedChunk struct {
	FileID      string
	ChunkIndex  int
	RecipientID string
	CreatedAt   int64
	TTLSeconds  int
}

func (s *SQLiteStore) GetLastChunkCheck(recipientID string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var val int64
	err := s.db.QueryRow(`SELECT last_check FROM last_check WHERE recipient_id = ?`, recipientID).Scan(&val)
	if err != nil {
		return 0
	}
	return val
}

func (s *SQLiteStore) SetLastChunkCheck(recipientID string, unix int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO last_check (recipient_id, last_check) VALUES (?, ?)`,
		recipientID, unix,
	)
	return err
}

func (s *SQLiteStore) StoreFailedChunk(fileID string, chunkIndex int, recipientID string, ttlSeconds int) error {
	if ttlSeconds <= 0 {
		ttlSeconds = 604800
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO failed_chunks (file_id, chunk_index, recipient_id, created_at, ttl_seconds) VALUES (?, ?, ?, ?, ?)`,
		fileID, chunkIndex, recipientID, time.Now().Unix(), ttlSeconds,
	)
	return err
}

func (s *SQLiteStore) DeleteFailedChunk(fileID string, chunkIndex int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`DELETE FROM failed_chunks WHERE file_id = ? AND chunk_index = ?`, fileID, chunkIndex)
	return err
}

func (s *SQLiteStore) IsChunkFailed(fileID string, chunkIndex int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM failed_chunks WHERE file_id = ? AND chunk_index = ?`, fileID, chunkIndex).Scan(&count)
	if err != nil {
		return false
	}
	return count > 0
}

func (s *SQLiteStore) ListFailedChunks(recipientID string) ([]FailedChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT file_id, chunk_index, recipient_id, created_at, ttl_seconds FROM failed_chunks WHERE recipient_id = ?`, recipientID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []FailedChunk
	for rows.Next() {
		var fc FailedChunk
		if err := rows.Scan(&fc.FileID, &fc.ChunkIndex, &fc.RecipientID, &fc.CreatedAt, &fc.TTLSeconds); err != nil {
			return nil, err
		}
		result = append(result, fc)
	}
	return result, rows.Err()
}

func (s *SQLiteStore) DeleteExpiredFailedChunks(now int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`DELETE FROM failed_chunks WHERE created_at + ttl_seconds <= ?`, now)
	return err
}
