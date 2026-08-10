package store

import (
	"database/sql"
	"fmt"
	"time"
)

type ReplicatedMessage struct {
	MessageUID  string `json:"message_uid"`
	Login       string `json:"login"`
	SenderLogin string `json:"sender_login"`
	ChatID      string `json:"chat_id"`
	Data        []byte `json:"data"`
	CreatedAt   int64  `json:"created_at"`
}

type ReplicatedPeer struct {
	Login         string    `json:"login"`
	PeerID        string    `json:"peer_id"`
	EncryptionKey string    `json:"encryption_key"`
	SignatureKey  string    `json:"signature_key"`
	LastSeen      time.Time `json:"last_seen"`
	IsFake        bool      `json:"is_fake"`
}

type ReplicationSnapshot struct {
	Version  int                 `json:"version"`
	Messages []ReplicatedMessage `json:"messages"`
	Peers    []ReplicatedPeer    `json:"peers"`
	Groups   []GroupChat         `json:"groups"`
}

func (s *SQLiteStore) ExportReplicationSnapshot() (ReplicationSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return ReplicationSnapshot{}, fmt.Errorf("begin replication snapshot: %w", err)
	}
	defer tx.Rollback()

	snapshot := ReplicationSnapshot{Version: 1}
	if snapshot.Messages, err = readReplicatedMessages(tx); err != nil {
		return ReplicationSnapshot{}, err
	}
	if snapshot.Peers, err = readReplicatedPeers(tx); err != nil {
		return ReplicationSnapshot{}, err
	}
	if snapshot.Groups, err = readReplicatedGroups(tx); err != nil {
		return ReplicationSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return ReplicationSnapshot{}, fmt.Errorf("commit replication snapshot: %w", err)
	}
	return snapshot, nil
}

func readReplicatedMessages(tx *sql.Tx) ([]ReplicatedMessage, error) {
	rows, err := tx.Query(`
		SELECT message_uid, login, sender_login, chat_id, data, CAST(created_at AS INTEGER)
		FROM messages
		ORDER BY created_at, message_uid`)
	if err != nil {
		return nil, fmt.Errorf("read replicated messages: %w", err)
	}
	defer rows.Close()

	var result []ReplicatedMessage
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
			return nil, fmt.Errorf("scan replicated message: %w", err)
		}
		result = append(result, message)
	}
	return result, rows.Err()
}

func readReplicatedPeers(tx *sql.Tx) ([]ReplicatedPeer, error) {
	rows, err := tx.Query(`
		SELECT login, peer_id, encryption_key, signature_key, last_seen, is_fake
		FROM stored_peers
		ORDER BY last_seen, login, signature_key`)
	if err != nil {
		return nil, fmt.Errorf("read replicated peers: %w", err)
	}
	defer rows.Close()

	var result []ReplicatedPeer
	for rows.Next() {
		var peer ReplicatedPeer
		if err := rows.Scan(
			&peer.Login,
			&peer.PeerID,
			&peer.EncryptionKey,
			&peer.SignatureKey,
			&peer.LastSeen,
			&peer.IsFake,
		); err != nil {
			return nil, fmt.Errorf("scan replicated peer: %w", err)
		}
		result = append(result, peer)
	}
	return result, rows.Err()
}

func readReplicatedGroups(tx *sql.Tx) ([]GroupChat, error) {
	rows, err := tx.Query(`
		SELECT uid, name, enc_private, enc_public, sign_private, sign_public, created_at
		FROM group_chats
		ORDER BY created_at, uid`)
	if err != nil {
		return nil, fmt.Errorf("read replicated groups: %w", err)
	}
	defer rows.Close()

	var result []GroupChat
	for rows.Next() {
		var group GroupChat
		if err := rows.Scan(
			&group.UID,
			&group.Name,
			&group.EncPrivate,
			&group.EncPublic,
			&group.SignPrivate,
			&group.SignPublic,
			&group.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan replicated group: %w", err)
		}
		result = append(result, group)
	}
	return result, rows.Err()
}

func (s *SQLiteStore) ImportReplicationSnapshot(snapshot ReplicationSnapshot) error {
	if snapshot.Version != 1 {
		return fmt.Errorf("unsupported replication snapshot version %d", snapshot.Version)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin replication import: %w", err)
	}
	defer tx.Rollback()

	for _, message := range snapshot.Messages {
		if _, err := tx.Exec(`
			INSERT INTO messages (message_uid, login, sender_login, chat_id, data, created_at)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(message_uid) DO UPDATE SET
				login = excluded.login,
				sender_login = excluded.sender_login,
				chat_id = excluded.chat_id,
				data = excluded.data,
				created_at = excluded.created_at`,
			message.MessageUID,
			message.Login,
			message.SenderLogin,
			message.ChatID,
			message.Data,
			message.CreatedAt,
		); err != nil {
			return fmt.Errorf("import message %s: %w", message.MessageUID, err)
		}
	}

	for _, peer := range snapshot.Peers {
		if _, err := tx.Exec(`
			INSERT INTO stored_peers (login, peer_id, encryption_key, signature_key, last_seen, is_fake)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(login, signature_key) DO UPDATE SET
				peer_id = excluded.peer_id,
				encryption_key = excluded.encryption_key,
				last_seen = excluded.last_seen,
				is_fake = excluded.is_fake`,
			peer.Login,
			peer.PeerID,
			peer.EncryptionKey,
			peer.SignatureKey,
			peer.LastSeen,
			peer.IsFake,
		); err != nil {
			return fmt.Errorf("import peer %s: %w", peer.PeerID, err)
		}
	}

	for _, group := range snapshot.Groups {
		if _, err := tx.Exec(`
			INSERT INTO group_chats (uid, name, enc_private, enc_public, sign_private, sign_public, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(uid) DO UPDATE SET
				name = excluded.name,
				enc_private = excluded.enc_private,
				enc_public = excluded.enc_public,
				sign_private = excluded.sign_private,
				sign_public = excluded.sign_public,
				created_at = excluded.created_at`,
			group.UID,
			group.Name,
			group.EncPrivate,
			group.EncPublic,
			group.SignPrivate,
			group.SignPublic,
			group.CreatedAt,
		); err != nil {
			return fmt.Errorf("import group %s: %w", group.UID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit replication import: %w", err)
	}
	return nil
}
