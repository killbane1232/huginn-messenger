package store

import "time"

func (s *SQLiteStore) StorePendingChunk(pc *PendingChunk) error {
	return retry(func() error {
		_, err := s.db.Exec(
			`INSERT OR REPLACE INTO pending_chunks (file_id, chunk_index, recipient_id, sender_id, data, hash, signature, created_at, placed, ttl_seconds) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			pc.FileID, pc.ChunkIndex, pc.RecipientID, pc.SenderID, pc.Data, pc.Hash, pc.Signature, pc.CreatedAt, pc.Placed, pc.TTLSeconds)
		return err
	})
}

func (s *SQLiteStore) GetUnplacedChunks() ([]PendingChunk, error) {
	return retryWith(func() ([]PendingChunk, error) {
		rows, err := s.db.Query(
			`SELECT file_id, chunk_index, recipient_id, sender_id, data, hash, signature, created_at, placed, ttl_seconds FROM pending_chunks WHERE placed = 0 ORDER BY created_at ASC`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var chunks []PendingChunk
		for rows.Next() {
			var c PendingChunk
			if err := rows.Scan(&c.FileID, &c.ChunkIndex, &c.RecipientID, &c.SenderID, &c.Data, &c.Hash, &c.Signature, &c.CreatedAt, &c.Placed, &c.TTLSeconds); err != nil {
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
			`SELECT file_id, chunk_index, recipient_id, sender_id, data, hash, signature, created_at, placed, ttl_seconds FROM pending_chunks WHERE file_id = ? ORDER BY chunk_index ASC`, fileID)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var chunks []PendingChunk
		for rows.Next() {
			var c PendingChunk
			if err := rows.Scan(&c.FileID, &c.ChunkIndex, &c.RecipientID, &c.SenderID, &c.Data, &c.Hash, &c.Signature, &c.CreatedAt, &c.Placed, &c.TTLSeconds); err != nil {
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

func (s *SQLiteStore) DeleteExpiredPendingChunks(now time.Time) error {
	return retry(func() error {
		_, err := s.db.Exec(
			"DELETE FROM pending_chunks WHERE CAST((julianday(?) - julianday(created_at)) * 86400 AS INTEGER) > ttl_seconds",
			now)
		return err
	})
}
