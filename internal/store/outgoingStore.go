package store

import (
	"fmt"
	"time"
)

// QueueOutgoing commits the visible message and every retry payload together.
// A successful send response must never precede this transaction.
func (s *SQLiteStore) QueueOutgoing(msgID, login, sender, chatID string, data []byte, sentAt time.Time, chunks []PendingChunk) error {
	return s.queueOutgoing(msgID, login, sender, chatID, data, sentAt, chunks, false)
}

func (s *SQLiteStore) RetryOutgoing(msgID, login, sender, chatID string, data []byte, sentAt time.Time, chunks []PendingChunk) error {
	return s.queueOutgoing(msgID, login, sender, chatID, data, sentAt, chunks, true)
}

func (s *SQLiteStore) queueOutgoing(msgID, login, sender, chatID string, data []byte, sentAt time.Time, chunks []PendingChunk, retry bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	query := `INSERT INTO messages (message_uid, login, sender_login, chat_id, data, created_at, state_updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`
	args := []any{msgID, login, sender, chatID, data, sentAt.UnixMicro(), time.Now().UTC().UnixMicro()}
	if retry {
		query = `UPDATE messages SET login = ?, sender_login = ?, chat_id = ?, data = ?, created_at = ?, state_updated_at = ?
			WHERE message_uid = ? AND json_valid(data) AND json_extract(data, '$.send_error') != ''`
		args = []any{login, sender, chatID, data, sentAt.UnixMicro(), time.Now().UTC().UnixMicro(), msgID}
	}
	result, err := tx.Exec(query, args...)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		if retry {
			return fmt.Errorf("failed message no longer exists")
		}
		return ErrDeletedMessage
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
