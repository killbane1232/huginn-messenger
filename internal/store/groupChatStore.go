package store

import (
	"time"
)

type GroupChat struct {
	UID         string    `json:"uid"`
	Name        string    `json:"name"`
	EncPrivate  string    `json:"enc_private,omitempty"`
	EncPublic   string    `json:"enc_public"`
	SignPrivate string    `json:"sign_private,omitempty"`
	SignPublic  string    `json:"sign_public"`
	CreatedAt   time.Time `json:"created_at"`
}

func (s *SQLiteStore) CreateGroupChat(gc *GroupChat) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`INSERT INTO group_chats (uid, name, enc_private, enc_public, sign_private, sign_public, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		gc.UID, gc.Name, gc.EncPrivate, gc.EncPublic, gc.SignPrivate, gc.SignPublic, gc.CreatedAt,
	)
	return err
}

func (s *SQLiteStore) GetGroupChats() ([]GroupChat, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT uid, name, enc_private, enc_public, sign_private, sign_public, created_at FROM group_chats ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []GroupChat
	for rows.Next() {
		var gc GroupChat
		if err := rows.Scan(&gc.UID, &gc.Name, &gc.EncPrivate, &gc.EncPublic, &gc.SignPrivate, &gc.SignPublic, &gc.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, gc)
	}
	return result, rows.Err()
}

func (s *SQLiteStore) GetGroupChatByName(name string) (*GroupChat, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row := s.db.QueryRow(`SELECT uid, name, enc_private, enc_public, sign_private, sign_public, created_at FROM group_chats WHERE name = $1`, name)
	var gc GroupChat
	if err := row.Scan(&gc.UID, &gc.Name, &gc.EncPrivate, &gc.EncPublic, &gc.SignPrivate, &gc.SignPublic, &gc.CreatedAt); err != nil {
		return nil, err
	}
	return &gc, nil
}

func (s *SQLiteStore) GetGroupChat(uid string) (*GroupChat, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row := s.db.QueryRow(`SELECT uid, name, enc_private, enc_public, sign_private, sign_public, created_at FROM group_chats WHERE uid = $1`, uid)
	var gc GroupChat
	if err := row.Scan(&gc.UID, &gc.Name, &gc.EncPrivate, &gc.EncPublic, &gc.SignPrivate, &gc.SignPublic, &gc.CreatedAt); err != nil {
		return nil, err
	}
	return &gc, nil
}

func (s *SQLiteStore) DeleteGroupChat(uid string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`DELETE FROM group_chats WHERE uid = $1`, uid)
	return err
}
