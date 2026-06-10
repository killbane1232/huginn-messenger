package store

import (
	"database/sql"
	"time"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
)

type StoredPeer struct {
	PeerID        string
	EncryptionKey string
	SignatureKey  string
	LastSeen      time.Time
}

func (s *StoredPeer) ToMuninnPeer() muninn.Peer {
	return muninn.Peer{
		ID:            s.PeerID,
		Addresses:     nil,
		EncryptionKey: s.EncryptionKey,
		SignatureKey:  s.SignatureKey,
		Metadata:      nil,
		LastSeen:      time.Now(),
		QualityScore:  100,
	}
}

func (s *SQLiteStore) StorePeer(login string, peerID string, encryptionKey, signatureKey string, lastSeen time.Time) error {
	return retry(func() error {
		_, err := s.db.Exec(
			"INSERT OR REPLACE INTO stored_peers (login, peer_id, encryption_key, signature_key, last_seen) VALUES (?, ?, ?, ?, ?)",
			login, peerID, encryptionKey, signatureKey, lastSeen)
		return err
	})
}

func (s *SQLiteStore) GetStoredPeer(login, peerID string) (*StoredPeer, error) {
	return retryWith(func() (*StoredPeer, error) {
		row := s.db.QueryRow(
			"SELECT peer_id, encryption_key, signature_key, last_seen FROM stored_peers WHERE login = ? AND peer_id = ?",
			login, peerID)
		var p StoredPeer
		err := row.Scan(&p.PeerID, &p.EncryptionKey, &p.SignatureKey, &p.LastSeen)
		if err == sql.ErrNoRows {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return &p, nil
	})
}

func (s *SQLiteStore) SearchStoredPeers(query string) ([]StoredPeer, error) {
	return retryWith(func() ([]StoredPeer, error) {
		rows, err := s.db.Query(
			"SELECT peer_id, encryption_key, signature_key, last_seen FROM stored_peers WHERE peer_id LIKE ? ORDER BY last_seen DESC",
			"%"+query+"%")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var peers []StoredPeer
		for rows.Next() {
			var p StoredPeer
			if err := rows.Scan(&p.PeerID, &p.EncryptionKey, &p.SignatureKey, &p.LastSeen); err != nil {
				return nil, err
			}
			peers = append(peers, p)
		}
		return peers, rows.Err()
	})
}

func (s *SQLiteStore) GetStoredPeers() ([]StoredPeer, error) {
	return retryWith(func() ([]StoredPeer, error) {
		rows, err := s.db.Query("SELECT peer_id, encryption_key, signature_key, last_seen FROM stored_peers ORDER BY last_seen DESC")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var peers []StoredPeer
		for rows.Next() {
			var p StoredPeer
			if err := rows.Scan(&p.PeerID, &p.EncryptionKey, &p.SignatureKey, &p.LastSeen); err != nil {
				return nil, err
			}
			peers = append(peers, p)
		}
		return peers, rows.Err()
	})
}