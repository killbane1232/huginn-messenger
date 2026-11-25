package store

import (
	"errors"
	"strings"
)

var ErrDeletedMessage = errors.New("message was deleted locally")

// DeleteChat clears local history but preserves peer/group keys and pending
// deliveries. Tombstones prevent another device or the server replaying history.
func (s *SQLiteStore) DeleteChat(chatID string) error {
	if strings.TrimSpace(chatID) == "" {
		return errors.New("chat id is empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT OR IGNORE INTO deleted_messages (message_uid)
		SELECT message_uid FROM messages WHERE chat_id = ?`, chatID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM messages WHERE chat_id = ?`, chatID); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT OR IGNORE INTO hidden_chats (chat_id) VALUES (?)`, chatID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteStore) GetHiddenChatIDs() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT chat_id FROM hidden_chats ORDER BY chat_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
