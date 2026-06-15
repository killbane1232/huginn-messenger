package store

import (
	"database/sql"
	"time"
)

func (s *SQLiteStore) StoreChunk(fileID string, chunkIndex int, data []byte, ttlSeconds int) error {
	return retry(func() error {
		_, err := s.db.Exec(
			"INSERT OR REPLACE INTO chunks (file_id, chunk_index, data, created_at, ttl_seconds) VALUES (?, ?, ?, ?, ?)",
			fileID, chunkIndex, data, time.Now(), ttlSeconds)
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

func (s *SQLiteStore) DeleteExpiredChunks(now time.Time) error {
	return retry(func() error {
		_, err := s.db.Exec(`
			DELETE FROM chunks 
			WHERE created_at is not null
				and ttl_seconds is not null
				and ttl_seconds <> 0
				and CAST((julianday(?) - julianday(created_at)) * 86400 AS INTEGER) > ttl_seconds
			`,
			now)
		return err
	})
}

func (s *SQLiteStore) DeleteChunksWithMessage() error {
	return retry(func() error {
		_, err := s.db.Exec(
			"DELETE FROM chunks WHERE file_id IN (SELECT message_uid FROM messages)")
		return err
	})
}
