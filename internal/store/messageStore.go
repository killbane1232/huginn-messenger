package store

import (
	"time"
)

const messageHistorySelect = `
		SELECT data
		FROM (
			SELECT message_uid, data, created_at
			FROM messages
			WHERE chat_id = ?
		)`

func messageHistoryArgs(peerID string) []any {
	return []any{
		peerID,
	}
}

func (s *SQLiteStore) SaveMessage(msg_uid string, login string, senderLogin string, chatID string, data []byte, created_at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`
		INSERT INTO messages (message_uid, login, sender_login, chat_id, data, created_at, state_updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		msg_uid, login, senderLogin, chatID, data, created_at.UnixMicro(), time.Now().UTC().UnixMicro())
	return err
}

func (s *SQLiteStore) GetMessages(peerID string) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(messageHistorySelect+`
		ORDER BY created_at ASC`,
		messageHistoryArgs(peerID)...)
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
}

func (s *SQLiteStore) GetMessagesDesc(peerID string, limit, offset int) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	args := append(messageHistoryArgs(peerID), limit, offset)
	rows, err := s.db.Query(messageHistorySelect+`
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?`,
		args...)
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
}

func (s *SQLiteStore) FindMessageById(msgID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query("SELECT '1' FROM messages WHERE message_uid = ?", msgID)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return false, nil
		}
		if len(data) > 0 {
			return true, nil
		}
		result = append(result, data)
	}
	return len(result) > 0, nil
}
