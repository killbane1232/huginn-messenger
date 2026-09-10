package store

import (
	"fmt"
	"time"
)

// QueueOutgoing commits the visible message and every retry payload together.
// A successful send response must never precede this transaction.
func (s *SQLiteStore) QueueOutgoing(msgID, login, sender, chatID string, data []byte, sentAt time.Time, chunks []PendingChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO messages (message_uid, login, sender_login, chat_id, data, created_at, state_updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, msgID, login, sender, chatID, data, sentAt.UnixMicro(), time.Now().UTC().UnixMicro()); err != nil {
		return err
	}
	for _, c := range chunks {
		if len(c.Data) == 0 || c.TTLSeconds <= 0 {
			return fmt.Errorf("invalid outgoing chunk")
		}
		if _, err := tx.Exec(`INSERT INTO chunks (file_id, chunk_index, data, created_at, ttl_seconds)
			VALUES (?, ?, ?, ?, ?)`, c.FileID, c.ChunkIndex, c.Data, c.CreatedAt, c.TTLSeconds); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO pending_chunks
			(file_id, chunk_index, recipient_id, sender_id, data, hash, signature, created_at, placed, ttl_seconds, persist)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)`, c.FileID, c.ChunkIndex, c.RecipientID, c.SenderID, c.Data, c.Hash, c.Signature, c.CreatedAt, c.TTLSeconds, c.Persist); err != nil {
			return err
		}
	}
	return tx.Commit()
}
