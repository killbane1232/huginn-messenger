package store

import (
	"database/sql"
	"fmt"
	"time"
)

// MessageReplicationDelta contains the messages that existed locally during a
// bounded interval. Checkpoint is the inclusive upper bound of that interval.
type MessageReplicationDelta struct {
	Version    int                 `json:"version"`
	Checkpoint int64               `json:"checkpoint"`
	Messages   []ReplicatedMessage `json:"messages"`
}

func (s *SQLiteStore) GetLastStateCheck(peerID string) (time.Time, error) {
	if peerID == "" {
		return time.Time{}, fmt.Errorf("peer id is empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var unixMicro int64
	err := s.db.QueryRow(`
		SELECT last_check_datetime
		FROM last_state_check
		WHERE peer_id = ?`, peerID).Scan(&unixMicro)
	if err == sql.ErrNoRows {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("get last state check for %s: %w", peerID, err)
	}
	return time.UnixMicro(unixMicro).UTC(), nil
}

func (s *SQLiteStore) SetLastStateCheck(peerID string, checkedAt time.Time) error {
	if peerID == "" {
		return fmt.Errorf("peer id is empty")
	}
	if checkedAt.IsZero() {
		return fmt.Errorf("last state check is zero")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec(`
		INSERT INTO last_state_check (peer_id, last_check_datetime)
		VALUES (?, ?)
		ON CONFLICT(peer_id) DO UPDATE SET
			last_check_datetime = MAX(last_state_check.last_check_datetime, excluded.last_check_datetime)`,
		peerID, checkedAt.UTC().UnixMicro())
	if err != nil {
		return fmt.Errorf("set last state check for %s: %w", peerID, err)
	}
	return nil
}

func (s *SQLiteStore) ExportMessagesForStateSync(since, checkpoint time.Time) (MessageReplicationDelta, error) {
	if checkpoint.IsZero() {
		return MessageReplicationDelta{}, fmt.Errorf("state sync checkpoint is zero")
	}

	sinceUnixMicro := int64(0)
	if !since.IsZero() {
		sinceUnixMicro = since.UTC().UnixMicro()
	}
	checkpointUnixMicro := checkpoint.UTC().UnixMicro()
	if checkpointUnixMicro < sinceUnixMicro {
		return MessageReplicationDelta{}, fmt.Errorf("state sync checkpoint precedes last check")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Keep the lower bound inclusive. A message inserted in the same clock tick
	// as the previous checkpoint may be resent, but cannot be skipped; imports
	// are idempotent by message_uid.
	rows, err := s.db.Query(`
		SELECT message_uid, login, sender_login, chat_id, data, CAST(created_at AS INTEGER)
		FROM messages
		WHERE state_updated_at >= ? AND state_updated_at <= ?
		ORDER BY state_updated_at, message_uid`, sinceUnixMicro, checkpointUnixMicro)
	if err != nil {
		return MessageReplicationDelta{}, fmt.Errorf("export messages for state sync: %w", err)
	}
	defer rows.Close()

	delta := MessageReplicationDelta{Version: 1, Checkpoint: checkpointUnixMicro}
	for rows.Next() {
		var message ReplicatedMessage
		if err := rows.Scan(
			&message.MessageUID,
			&message.Login,
			&message.SenderLogin,
			&message.ChatID,
			&message.Data,
			&message.CreatedAt,
		); err != nil {
			return MessageReplicationDelta{}, fmt.Errorf("scan state sync message: %w", err)
		}
		delta.Messages = append(delta.Messages, message)
	}
	if err := rows.Err(); err != nil {
		return MessageReplicationDelta{}, fmt.Errorf("iterate state sync messages: %w", err)
	}
	return delta, nil
}

// ImportStateSyncMessages inserts messages that are absent locally. Existing
// rows are deliberately preserved because they may contain device-local file
// paths that are stripped from replicated data.
func (s *SQLiteStore) ImportStateSyncMessages(messages []ReplicatedMessage) ([]ReplicatedMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin state sync import: %w", err)
	}
	defer tx.Rollback()
	importedAt := time.Now().UTC().UnixMicro()

	inserted := make([]ReplicatedMessage, 0, len(messages))
	for _, message := range messages {
		result, err := tx.Exec(`
			INSERT INTO messages (message_uid, login, sender_login, chat_id, data, created_at, state_updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(message_uid) DO NOTHING`,
			message.MessageUID,
			message.Login,
			message.SenderLogin,
			message.ChatID,
			message.Data,
			message.CreatedAt,
			importedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("import state sync message %s: %w", message.MessageUID, err)
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("inspect state sync message %s: %w", message.MessageUID, err)
		}
		if rowsAffected > 0 {
			inserted = append(inserted, message)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit state sync import: %w", err)
	}
	return inserted, nil
}
