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
	row, err := retryWith(func() (int64, error) {
		var val int64
		err := s.db.QueryRow(`SELECT last_check FROM last_check WHERE recipient_id = ?`, recipientID).Scan(&val)
		return val, err
	})
	if err != nil {
		return 0
	}
	return row
}

func (s *SQLiteStore) SetLastChunkCheck(recipientID string, unix int64) error {
	_, err := retryWith(func() (struct{}, error) {
		var empty struct{}
		_, err := s.db.Exec(
			`INSERT OR REPLACE INTO last_check (recipient_id, last_check) VALUES (?, ?)`,
			recipientID, unix,
		)
		return empty, err
	})
	return err
}

func (s *SQLiteStore) StoreFailedChunk(fileID string, chunkIndex int, recipientID string, ttlSeconds int) error {
	if ttlSeconds <= 0 {
		ttlSeconds = 604800
	}
	_, err := retryWith(func() (struct{}, error) {
		var empty struct{}
		_, err := s.db.Exec(
			`INSERT OR IGNORE INTO failed_chunks (file_id, chunk_index, recipient_id, created_at, ttl_seconds) VALUES (?, ?, ?, ?, ?)`,
			fileID, chunkIndex, recipientID, time.Now().Unix(), ttlSeconds,
		)
		return empty, err
	})
	return err
}

func (s *SQLiteStore) DeleteFailedChunk(fileID string, chunkIndex int) error {
	_, err := retryWith(func() (struct{}, error) {
		var empty struct{}
		_, err := s.db.Exec(`DELETE FROM failed_chunks WHERE file_id = ? AND chunk_index = ?`, fileID, chunkIndex)
		return empty, err
	})
	return err
}

func (s *SQLiteStore) IsChunkFailed(fileID string, chunkIndex int) bool {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM failed_chunks WHERE file_id = ? AND chunk_index = ?`, fileID, chunkIndex).Scan(&count)
	if err != nil {
		return false
	}
	return count > 0
}

func (s *SQLiteStore) ListFailedChunks(recipientID string) ([]FailedChunk, error) {
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
	_, err := retryWith(func() (struct{}, error) {
		var empty struct{}
		_, err := s.db.Exec(`DELETE FROM failed_chunks WHERE created_at + ttl_seconds <= ?`, now)
		return empty, err
	})
	return err
}
