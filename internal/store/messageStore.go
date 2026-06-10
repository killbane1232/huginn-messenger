package store

import (
	"time"
)

func (s *SQLiteStore) SaveMessage(msg_uid string, login string, senderLogin string, chatID string, data []byte, created_at time.Time) error {
	return retry(func() error {
		_, err := s.db.Exec("INSERT INTO messages (message_uid, login, sender_login, chat_id, data, created_at) VALUES (?, ?, ?, ?, ?, ?)",
			msg_uid, login, senderLogin, chatID, data, created_at)
		return err
	})
}

func (s *SQLiteStore) GetMessages(peerID string) ([][]byte, error) {
	return retryWith(func() ([][]byte, error) {
		rows, err := s.db.Query("SELECT data FROM messages WHERE login = ?", peerID)
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
	})
}