package store

import (
	_ "database/sql"
	"time"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
)

type StoredPeer struct {
	PeerID        string
	EncryptionKey string
	SignatureKey  string
	LastSeen      time.Time
	IsFake        bool
}

func (s *StoredPeer) ToMuninnPeer() muninn.Peer {
	return muninn.Peer{
		Key:           s.PeerID,
		Addresses:     nil,
		EncryptionKey: s.EncryptionKey,
		SignatureKey:  s.SignatureKey,
		Metadata:      nil,
		LastSeen:      time.Now(),
		QualityScore:  100,
		IsFake:        s.IsFake,
	}
}

func (s *SQLiteStore) StorePeer(peerID string, login string, encryptionKey, signatureKey string, lastSeen time.Time, isFake bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		"INSERT OR REPLACE INTO stored_peers (login, peer_id, encryption_key, signature_key, last_seen, is_fake) VALUES (?, ?, ?, ?, ?, ?)",
		login, peerID, encryptionKey, signatureKey, lastSeen, isFake)
	return err
}

func (s *SQLiteStore) SearchStoredPeers(query string) ([]StoredPeer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(
		"SELECT peer_id, encryption_key, signature_key, last_seen, is_fake FROM stored_peers WHERE peer_id LIKE ? and is_fake = FALSE ORDER BY last_seen DESC",
		"%"+query+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var peers []StoredPeer
	for rows.Next() {
		var p StoredPeer
		if err := rows.Scan(&p.PeerID, &p.EncryptionKey, &p.SignatureKey, &p.LastSeen, &p.IsFake); err != nil {
			return nil, err
		}
		peers = append(peers, p)
	}
	return peers, rows.Err()
}

func (s *SQLiteStore) GetStoredPeers() ([]StoredPeer, error) {
	rows, err := s.db.Query("SELECT peer_id, encryption_key, signature_key, last_seen, is_fake FROM stored_peers where is_fake = FALSE ORDER BY last_seen DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var peers []StoredPeer
	for rows.Next() {
		var p StoredPeer
		if err := rows.Scan(&p.PeerID, &p.EncryptionKey, &p.SignatureKey, &p.LastSeen, &p.IsFake); err != nil {
			return nil, err
		}
		peers = append(peers, p)
	}
	return peers, rows.Err()
}