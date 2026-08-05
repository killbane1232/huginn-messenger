package messenger

import (
	"encoding/json"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/muninn"
	"github.com/killbane1232/huginn-messenger/internal/store"
	//"runtime/debug"
)

func (m *Messenger) peerRefreshLoop() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.replicatePendingChunks()
			m.checkPendingMessages()
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Messenger) SearchPeers(query string) []muninn.Peer {
	seen := make(map[string]*muninn.Peer)
	q := strings.ToLower(query)

	stored, err := m.store.SearchStoredPeers(query)
	if err == nil {
		for _, s := range stored {
			if s.Key() == m.Key {
				continue
			}
			seen[s.Key()] = &s
		}
	}

	muninnPeers, err := m.muninnClient.List(m.ctx)
	if err == nil {
		for _, p := range muninnPeers {
			if p.Key() == m.Key {
				continue
			}
			if strings.Contains(strings.ToLower(p.Key()), q) {
				m.upsertPeer(p.ID, p.Key(), p.Login, p.EncryptionKey, p.SignatureKey, p.LastSeen, false)
				if existing, ok := seen[p.Key()]; ok {
					if p.LastSeen.After(existing.LastSeen) {
						existing.LastSeen = p.LastSeen
					}
					if p.EncryptionKey != "" {
						existing.EncryptionKey = p.EncryptionKey
					}
					if p.SignatureKey != "" {
						existing.SignatureKey = p.SignatureKey
					}
					existing.TTLSeconds = p.TTLSeconds
				} else {
					cp := p
					seen[p.Key()] = &cp
				}
			}
		}
	}

	result := make([]muninn.Peer, 0, len(seen))
	for _, p := range seen {
		result = append(result, *p)
	}
	return result
}

func (m *Messenger) upsertPeer(peerID, peerKey, login, encryptionKey, signatureKey string, lastSeen time.Time, isFake bool) {
	login = (muninn.Peer{
		ID:           peerID,
		Login:        login,
		SignatureKey: signatureKey,
		IsFake:       isFake,
	}).DisplayLogin()
	m.mu.Lock()
	found := false
	peer, found := m.peersMap[peerKey]
	if found {
		if !slices.Contains(peer.IDS, peerID) {
			peer.IDS = append(peer.IDS, peerID)
		}
		m.peersMap[peerKey] = peer
	}
	if !found {
		m.peersMap[peerKey] = muninn.Peer{
			ID:            peerID,
			Login:         login,
			EncryptionKey: encryptionKey,
			SignatureKey:  signatureKey,
			LastSeen:      lastSeen,
			IsFake:        isFake,
			IDS:           []string{peerID},
		}
		if err := m.store.StorePeer(login, peerKey, encryptionKey, signatureKey, lastSeen, isFake); err != nil {
			log.Printf("store peer %s: %v", peerID, err)
		}
	}
	m.peers = m.PeerSlice()
	m.mu.Unlock()

	m.subsMu.Lock()
	for _, ch := range m.peerSubs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	m.subsMu.Unlock()
}

func (m *Messenger) GetPeers() []muninn.Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]muninn.Peer, 0, len(m.peers))
	for _, p := range m.peers {
		if p.Key() != m.Key {
			result = append(result, p)
		}
	}
	return result
}

func (m *Messenger) IsPeerConnected(peerID string) bool {
	return m.rtcManager.IsConnected(peerID)
}

func (m *Messenger) getConnectedPeers() []muninn.Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var online []muninn.Peer
	for _, p := range m.peers {
		for _, pid := range p.IDS {
			if pid != m.ID && m.IsPeerConnected(pid) {
				online = append(online, p)
				break
			}
		}
	}
	return online
}

func (m *Messenger) IsPeerOnline(peerID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, p := range m.peers {
		for _, pid := range p.IDS {
			if pid == peerID && p.IsFake {
				return p.LastSeen.After(time.Now().Add(time.Duration(-p.TTLSeconds/2) * time.Second))
				break
			}
		}
	}
	return false
}

func (m *Messenger) IsPeerOnlineByKey(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, p := range m.peers {
		if p.Key() == key && p.IsFake {
			return p.LastSeen.After(time.Now().Add(time.Duration(-p.TTLSeconds/2) * time.Second))
		}
	}
	return false
}

func (m *Messenger) getOnlinePeers() []muninn.Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var online []muninn.Peer
	for _, p := range m.peers {
		for _, pid := range p.IDS {
			if pid != m.ID && p.LastSeen.After(time.Now().Add(time.Duration(-p.TTLSeconds/2)*time.Second)) {
				online = append(online, p)
				break
			}
		}
	}
	return online
}

func (m *Messenger) findPeerByID(id string) *muninn.Peer {
	m.mu.RLock()
	for _, p := range m.peers {
		if slices.Contains(p.IDS, id) {
			m.mu.RUnlock()
			return &p
		}
	}
	m.mu.RUnlock()

	stored, err := m.muninnClient.Get(m.ctx, id)
	if err != nil || stored == nil {
		return nil
	}

	m.upsertPeer(stored.ID, stored.Key(), stored.Login, stored.EncryptionKey, stored.SignatureKey, time.Now(), stored.IsFake)

	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, p := range m.peers {
		if slices.Contains(p.IDS, id) {
			return &p
		}
	}
	return nil
}

func (m *Messenger) findPeerByKey(key string) *muninn.Peer {
	keySplit := strings.Split(key, ":")
	if len(keySplit) < 2 {
		return nil
	}

	login := keySplit[0]
	signature := keySplit[1]
	m.mu.RLock()
	peer, exists := m.peersMap[key]
	m.mu.RUnlock()
	if exists && peer.ID != "" {
		return &peer
	}

	stored, err := m.muninnClient.GetAllByKey(m.ctx, login, signature)
	if err != nil || stored == nil {
		return nil
	}

	for _, p := range stored {
		m.upsertPeer(p.ID, p.Key(), p.Login, p.EncryptionKey, p.SignatureKey, time.Now(), p.IsFake)
	}
	m.mu.RLock()
	peer, exists = m.peersMap[key]
	m.mu.RUnlock()
	if exists {
		return &peer
	}
	return nil
}

func (m *Messenger) ConnectPeer(toPeerID string) error {
	m.mu.Lock()
	if _, ok := m.peersConnecting[toPeerID]; ok {
		m.mu.Unlock()
		return nil
	}
	m.peersConnecting[toPeerID] = struct{}{}
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.peersConnecting, toPeerID)
		m.mu.Unlock()
	}()

	offer, err := m.rtcManager.CreateOffer(toPeerID)
	if err != nil {
		return fmt.Errorf("create offer: %w", err)
	}
	offerData, _ := json.Marshal(offer)

	if !m.wsClient.IsConnected() {
		m.wsClient.Connect(m.ctx)
	}
	if m.wsClient != nil && m.wsClient.IsConnected() {
		if err := m.wsClient.ConnectToPeer(m.ctx, toPeerID, string(offerData)); err == nil {
			log.Printf("webrtc offer sent to %s via rtc relay", toPeerID)
			return nil
		}
		log.Printf("rtc connect peer failed, fallback to http: %v", err)
	}

	sig := muninn.Signal{From: m.ID, Type: "offer", Data: string(offerData)}
	if err := m.muninnClient.SendSignal(m.ctx, toPeerID, sig); err != nil {
		return fmt.Errorf("send signal: %w", err)
	}
	log.Printf("webrtc offer sent to %s via http signal", toPeerID)
	return nil
}

func (m *Messenger) DisconnectPeer(toPeerID string) {
	m.rtcManager.Close(toPeerID)
	log.Printf("disconnected peer %s", toPeerID)
}

func (m *Messenger) PeerSlice() []muninn.Peer {
	s := make([]muninn.Peer, 0, len(m.peersMap))
	for _, v := range m.peersMap {
		s = append(s, v)
	}
	return s
}

func (m *Messenger) GetContacts() ([]store.StoredPeer, error) {
	return m.store.GetStoredPeers()
}

func (m *Messenger) SubscribePeers() chan struct{} {
	ch := make(chan struct{}, 1)
	m.subsMu.Lock()
	m.peerSubs[fmt.Sprintf("%p", ch)] = ch
	m.subsMu.Unlock()
	return ch
}

func (m *Messenger) UnsubscribePeers(ch chan struct{}) {
	m.subsMu.Lock()
	for id, c := range m.peerSubs {
		if c == ch {
			delete(m.peerSubs, id)
			close(ch)
			break
		}
	}
	m.subsMu.Unlock()
}
