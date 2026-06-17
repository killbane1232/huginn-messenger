package messenger

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"strings"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/chunk"
	"github.com/killbane1232/huginn-messenger/internal/config"
	"github.com/killbane1232/huginn-messenger/internal/crypto"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
	"github.com/killbane1232/huginn-messenger/internal/store"
	"github.com/killbane1232/huginn-messenger/internal/webrtc"
	"github.com/google/uuid"
	pion "github.com/pion/webrtc/v3"
)

type ChatMessage struct {
	From      string     `json:"from"`
	Text      string     `json:"text"`
	Timestamp time.Time  `json:"timestamp"`
	MsgID     string     `json:"msg_id,omitempty"`
	Files     []FileMeta `json:"files,omitempty"`
}

type FileMeta struct {
	FileID        string `json:"file_id"`
	FileHash      string `json:"file_hash"`
	DecryptionKey string `json:"decryption_key"`
	TotalChunks   int    `json:"total_chunks"`
	Filename      string `json:"filename,omitempty"`
}

type pendingFileDownload struct {
	fileMeta  FileMeta
	senderID  string
}

type MessagePayload struct {
	Text      string     `json:"text"`
	Timestamp time.Time  `json:"timestamp"`
	Files     []FileMeta `json:"files,omitempty"`
}

type MessengerOption func(*messengerOpts)

type messengerOpts struct {
	iceServers []pion.ICEServer
	iceSet     bool
	peerFlag   muninn.PeerFlag
	turnAddr   string
	turnUser   string
	turnPass   string
}

func WithICEServers(servers []pion.ICEServer) MessengerOption {
	return func(o *messengerOpts) {
		o.iceServers = servers
		o.iceSet = true
	}
}

func WithPeerFlag(flag muninn.PeerFlag) MessengerOption {
	return func(o *messengerOpts) {
		o.peerFlag = flag
	}
}

func WithTURN(addr, user, pass string) MessengerOption {
	return func(o *messengerOpts) {
		o.turnAddr = addr
		o.turnUser = user
		o.turnPass = pass
	}
}

type Messenger struct {
	ID       string
	Username string

	signPublic  ed25519.PublicKey
	signPrivate ed25519.PrivateKey
	encPrivate  []byte
	encPublic   []byte

	muninnClient *muninn.Client
	rtcClient    *muninn.RTCClient
	rtcManager   *webrtc.Manager
	rtcMsgChan   chan webrtc.ChatMessage
	signalChan   chan muninn.Signal

	store *store.SQLiteStore

	peers    []muninn.Peer
	mu       sync.RWMutex

	peerSubs   map[string]chan struct{}
	subsMu     sync.Mutex
	msgSubs    []chan ChatMessage
	msgSubsMu  sync.Mutex

	ctx          context.Context
	cancel       context.CancelFunc
	peerFlag     muninn.PeerFlag
	downloadsDir string

	pendingFileDownloads map[string]*pendingFileDownload
	pendingMu            sync.Mutex

	processingMsg map[string]bool
	processingMu  sync.Mutex
}

func New(username string, muninnClient *muninn.Client, dbPath string, opts ...MessengerOption) (*Messenger, error) {
	var o messengerOpts
	for _, opt := range opts {
		opt(&o)
	}

	keysPath := filepath.Join(filepath.Dir(dbPath), "keys.conf")
	signPub, signPriv, encPriv, encPub, err := crypto.LoadKeys(keysPath)
	if err != nil {
		signPub, signPriv, err = crypto.GenerateSigningKey()
		if err != nil {
			return nil, fmt.Errorf("generate signing key: %w", err)
		}
		encPriv, encPub, err = crypto.GenerateEncryptionKey()
		if err != nil {
			return nil, fmt.Errorf("generate encryption key: %w", err)
		}
		if err := crypto.SaveKeys(keysPath, signPub, signPriv, encPriv, encPub); err != nil {
			log.Printf("save keys to %s: %v", keysPath, err)
		}
	}

	st, err := store.New(dbPath)
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}

	downloadsDir := filepath.Join(filepath.Dir(dbPath), "downloads", "huginn")
	if err := os.MkdirAll(downloadsDir, 0755); err != nil {
		return nil, fmt.Errorf("create downloads dir: %w", err)
	}

	rtcMsgChan := make(chan webrtc.ChatMessage, 100)
	signalChan := make(chan muninn.Signal, 100)
	ctx, cancel := context.WithCancel(context.Background())

	m := &Messenger{
		ID:       username,
		Username: username,

		signPublic:  signPub,
		signPrivate: signPriv,
		encPrivate:  encPriv,
		encPublic:   encPub,

		muninnClient: muninnClient,
		rtcMsgChan:   rtcMsgChan,
		signalChan:   signalChan,
		store:        st,
		peerSubs:     make(map[string]chan struct{}),
		ctx:          ctx,
		cancel:       cancel,
		peerFlag:     o.peerFlag,
		downloadsDir: downloadsDir,

		pendingFileDownloads: make(map[string]*pendingFileDownload),
		processingMsg:       make(map[string]bool),
	}

	if !o.iceSet {
		o.iceServers = []pion.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		}
	}
	if o.turnAddr != "" {
		o.iceServers = append(o.iceServers,
			pion.ICEServer{
				URLs:       []string{"turn:" + o.turnAddr},
				Username:   o.turnUser,
				Credential: o.turnPass,
			},
			pion.ICEServer{
				URLs: []string{"stun:" + o.turnAddr},
			},
		)
	}
	m.rtcManager = webrtc.NewManager(username, rtcMsgChan, m.handleChunkStore, m.handleChunkGet, o.iceServers)

	m.rtcClient = muninn.NewRTCClient(muninnClient.BaseURL(), username, o.iceServers)
	m.rtcClient.SetOnSignal(func(sig muninn.Signal) {
		select {
		case m.signalChan <- sig:
		default:
			log.Printf("dropping rtc signal from %s (channel full)", sig.From)
		}
	})
	m.rtcClient.SetOnDisconnect(func() {
		log.Printf("[rtc] connection to muninn lost, will reconnect")
	})
	go m.rtcReconnectLoop()
	storedPeers, _ := st.GetStoredPeers()
	for _, peer := range storedPeers {
		m.peers = append(m.peers, peer.ToMuninnPeer())
	}

	go m.heartbeatLoop()   // TODO: переделать на более низкое энергопотребление
	go m.peerRefreshLoop() // т.к. текущее решение занимает все потоки на всё время
	go m.signalPollLoop()
	go m.processRTCMessages()
	go m.pendingChunkLoop()
	go m.fileDownloadLoop()
	go m.chunkCleanupLoop()

	return m, nil
}

func (m *Messenger) handleChunkStore(peerID string, req webrtc.ChunkStoreRequest) {
	// Если мы не являемся конечным получателем — подтверждаем получение на сервере
	if req.RecipientID != "" && req.RecipientID != m.ID && req.Hash != "" && req.Signature != "" {
		confirmReq := muninn.ConfirmChunkRequest{
			RecipientID: m.ID,
			FileID:      req.FileID,
			ChunkIndex:  req.ChunkIndex,
			Hash:        req.Hash,
			Signature:   req.Signature,
		}
		if _, err := m.muninnClient.ConfirmChunk(m.ctx, confirmReq); err != nil {
			log.Printf("confirm chunk %s/%d failed, not saving: %v", req.FileID, req.ChunkIndex, err)
			return
		}
		log.Printf("confirmed chunk %s/%d as storage peer", req.FileID, req.ChunkIndex)
	}

	ttl := req.TTLSeconds
	if ttl <= 0 {
		ttl = 604800
	}
	if err := m.store.StoreChunk(req.FileID, req.ChunkIndex, req.Data, ttl); err != nil {
		log.Printf("store chunk %s/%d: %v", req.FileID, req.ChunkIndex, err)
		return
	}
		log.Printf("stored chunk %s/%d from %s", req.FileID, req.ChunkIndex, peerID)
	go m.checkPendingMessages()
	go m.checkPendingFileDownloads()
}

func (m *Messenger) handleChunkGet(peerID string, req webrtc.ChunkGetRequest) ([]byte, bool) {
	data, err := m.store.GetChunk(req.FileID, req.ChunkIndex)
	if err != nil || data == nil {
		log.Printf("chunk get err: %v %s %d", err, req.FileID, req.ChunkIndex)
		return nil, false
	}
	log.Printf("sent chunk: %s %d", req.FileID, req.ChunkIndex)
	return data, true
}

func (m *Messenger) processRTCMessages() {
	for {
		select {
		case msg := <-m.rtcMsgChan:
			cm := ChatMessage{
				From:      msg.From,
				Text:      msg.Text,
				Timestamp: msg.Timestamp,
				MsgID:     msg.MsgID,
			}
			if cm.Timestamp.IsZero() {
				cm.Timestamp = time.Now()
			}
			jsonData, _ := json.Marshal(cm)
			if err := m.store.SaveMessage(msg.MsgID, msg.From, cm.From, cm.From, jsonData, cm.Timestamp); err != nil {
				log.Printf("save message: %v", err)
			}
			encKey, signKey := "", ""
			if p := m.findPeerByID(msg.From); p != nil {
				encKey, signKey = p.EncryptionKey, p.SignatureKey
			}
			m.upsertPeer(msg.From, encKey, signKey, cm.Timestamp)
			m.msgSubsMu.Lock()
			for _, sub := range m.msgSubs {
				select {
				case sub <- cm:
				default:
				}
			}
			m.msgSubsMu.Unlock()
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Messenger) processPendingSignals() {
	sigs, err := m.muninnClient.PollSignals(m.ctx, m.ID)
	if err != nil {
		return
	}
	for _, sig := range sigs {
		m.handleSignal(sig)
	}

	for {
		select {
		case sig := <-m.signalChan:
			m.handleSignal(sig)
		default:
			return
		}
	}
}

func (m *Messenger) handleSignal(sig muninn.Signal) {
	switch sig.Type {
	case "offer":
		var offer pion.SessionDescription
		if err := json.Unmarshal([]byte(sig.Data), &offer); err != nil {
			return
		}
		answer, err := m.rtcManager.HandleOffer(sig.From, offer)
		if err != nil {
			log.Printf("handle offer from %s: %v", sig.From, err)
			return
		}
		ansData, _ := json.Marshal(answer)

		if m.rtcClient != nil && m.rtcClient.IsConnected() {
			if err := m.rtcClient.RelaySignal(m.ctx, sig.From, "answer", string(ansData)); err != nil {
				log.Printf("rtc relay answer to %s: %v, fallback to http", sig.From, err)
				m.muninnClient.SendSignal(m.ctx, sig.From, muninn.Signal{From: m.ID, Type: "answer", Data: string(ansData)})
			}
		} else {
			m.muninnClient.SendSignal(m.ctx, sig.From, muninn.Signal{From: m.ID, Type: "answer", Data: string(ansData)})
		}

	case "answer":
		var answer pion.SessionDescription
		if err := json.Unmarshal([]byte(sig.Data), &answer); err != nil {
			return
		}
		if err := m.rtcManager.SetRemoteDescription(sig.From, answer); err != nil {
			log.Printf("set remote desc from %s: %v", sig.From, err)
		}
	}
}

func (m *Messenger) rtcReconnectLoop() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}

		if m.rtcClient.IsConnected() {
			continue
		}

		log.Printf("[rtc] attempting to reconnect to muninn...")
		if err := m.rtcClient.Connect(m.ctx); err != nil {
			log.Printf("[rtc] reconnect failed: %v", err)
			continue
		}
		log.Printf("[rtc] reconnected to muninn")
	}
}

func (m *Messenger) signalPollLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.processPendingSignals()
		case sig := <-m.signalChan:
			m.handleSignal(sig)
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Messenger) heartbeatLoop() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := m.muninnClient.Heartbeat(m.ctx, m.ID, 15); err != nil {
				if (strings.Contains(err.Error(), "peer not found")) { 
					log.Printf("heartbeat error: %v, registering peer", err)
					if err := m.Register(); err != nil {
						log.Printf("register peer error: %v", err)
					}
				} else {
					log.Printf("heartbeat error: %v", err)
				}
			}
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Messenger) peerRefreshLoop() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.processPendingSignals()
			m.replicatePendingChunks()
			m.checkPendingMessages()
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Messenger) pendingChunkLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.distributePendingChunks()
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Messenger) chunkCleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			now := time.Now()
			if err := m.store.DeleteExpiredChunks(now); err != nil {
				log.Printf("cleanup expired chunks: %v", err)
			}
			if err := m.store.DeleteExpiredPendingChunks(now); err != nil {
				log.Printf("cleanup expired pending chunks: %v", err)
			}
			if err := m.store.DeleteChunksWithMessage(); err != nil {
				log.Printf("cleanup message chunks: %v", err)
			}
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Messenger) Register() error {
	req := &muninn.RegisterRequest{
		ID:       m.ID,
		Keys:     []muninn.Key{{Login: m.ID, Signature: "huginn-v1"}},
		Addresses: []string{""},
		EncryptionKey: crypto.EncodeKey(m.encPublic),
		SignatureKey:  crypto.EncodeKey(m.signPublic),
		Metadata: map[string]string{
			"username": m.ID,
			"type":     "huginn-messenger",
		},
		TTLSeconds: 120,
		PeerFlag:   m.peerFlag,
	}
	return m.muninnClient.Register(m.ctx, req)
}

func (m *Messenger) SearchPeers(query string) []muninn.Peer {
	seen := make(map[string]*muninn.Peer)
	q := strings.ToLower(query)

	stored, err := m.store.SearchStoredPeers(query)
	if err == nil {
		for _, s := range stored {
			if s.PeerID == m.ID {
				continue
			}
			seen[s.PeerID] = &muninn.Peer{
				ID:            s.PeerID,
				EncryptionKey: s.EncryptionKey,
				SignatureKey:  s.SignatureKey,
				LastSeen:      s.LastSeen,
				Metadata:      map[string]string{"username": s.PeerID},
			}
		}
	}

	muninnPeers, err := m.muninnClient.List(m.ctx)
	if err == nil {
		for _, p := range muninnPeers {
			if p.ID == m.ID {
				continue
			}
			if strings.Contains(strings.ToLower(p.ID), q) {
				m.upsertPeer(p.ID, p.EncryptionKey, p.SignatureKey, p.LastSeen)
				if existing, ok := seen[p.ID]; ok {
					if p.LastSeen.After(existing.LastSeen) {
						existing.LastSeen = p.LastSeen
					}
					if p.EncryptionKey != "" {
						existing.EncryptionKey = p.EncryptionKey
					}
					if p.SignatureKey != "" {
						existing.SignatureKey = p.SignatureKey
					}
					if p.Metadata != nil {
						existing.Metadata = p.Metadata
					}
					existing.Addresses = p.Addresses
					existing.Keys = p.Keys
					existing.TTLSeconds = p.TTLSeconds
					existing.QualityScore = p.QualityScore
				} else {
					cp := p
					seen[p.ID] = &cp
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

func (m *Messenger) upsertPeer(peerID, encryptionKey, signatureKey string, lastSeen time.Time) {
	m.mu.Lock()
	found := false
	for i := range m.peers {
		if m.peers[i].ID == peerID {
			if encryptionKey != "" {
				m.peers[i].EncryptionKey = encryptionKey
			}
			if signatureKey != "" {
				m.peers[i].SignatureKey = signatureKey
			}
			if lastSeen.After(m.peers[i].LastSeen) {
				m.peers[i].LastSeen = lastSeen
			}
			found = true
			break
		}
	}
	if !found {
		m.peers = append(m.peers, muninn.Peer{
			ID:            peerID,
			EncryptionKey: encryptionKey,
			SignatureKey:  signatureKey,
			LastSeen:      lastSeen,
			QualityScore:  100,
		})
	}
	m.mu.Unlock()

	if err := m.store.StorePeer(m.ID, peerID, encryptionKey, signatureKey, lastSeen); err != nil {
		log.Printf("store peer %s: %v", peerID, err)
	}

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
		if p.ID != m.ID {
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
		if p.ID != m.ID && m.IsPeerConnected(p.ID) {
			online = append(online, p)
		}
	}
	return online
}

func (m *Messenger) IsPeerOnline(peerID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, p := range m.peers {
		if p.ID == peerID {
			return p.LastSeen.After(time.Now().Add(time.Duration(- p.TTLSeconds / 2) * time.Second))
		}
	}
	return false
}

func (m *Messenger) getOnlinePeers() []muninn.Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var online []muninn.Peer
	for _, p := range m.peers {
		if p.ID != m.ID && p.LastSeen.After(time.Now().Add(time.Duration(- p.TTLSeconds / 2) * time.Second)) {
			online = append(online, p)
		}
	}
	return online
}

func (m *Messenger) distributePendingChunks() {
	chunks, err := m.store.GetUnplacedChunks()
	if err != nil {
		log.Printf("get unplaced chunks: %v", err)
		return
	}
	if len(chunks) == 0 {
		return
	}

	byRecipient := make(map[string][]store.PendingChunk)
	for _, c := range chunks {
		if (byRecipient[c.RecipientID] == nil) {
			byRecipient[c.RecipientID] = []store.PendingChunk{}
		}
		byRecipient[c.RecipientID] = append(byRecipient[c.RecipientID], c)
	}

	for recipientID, recipientChunks := range byRecipient {
		m.distributeChunksForRecipient(recipientID, recipientChunks)
	}
}

func (m *Messenger) distributeChunksForRecipient(recipientID string, chunks []store.PendingChunk) {
	onlinePeers, err := m.muninnClient.GetBestPeers(m.ctx, 10)
	if err != nil {
		onlinePeers = m.getOnlinePeers()
	}

	// Подключаем пиры
	var storagePeers []string
	for _, p := range onlinePeers {
		if p.ID == m.ID || p.ID == recipientID {
			continue
		}
		if !m.IsPeerConnected(p.ID) {
			m.ConnectPeer(p.ID)
		}
		storagePeers = append(storagePeers, p.ID)
	}

	
	if len(storagePeers) == 0 {
		return
	}

	for i := 0; i < 30 && len(storagePeers) > 0; i++ {
		time.Sleep(100 * time.Millisecond)
		allConnected := true
		for _, pid := range storagePeers {
			if !m.IsPeerConnected(pid) {
				allConnected = false
				break
			}
		}
		if allConnected {
			break
		}
	}

	byPeer := make(map[string][]store.PendingChunk)
	for i, c := range chunks {
		pid := storagePeers[i%len(storagePeers)]
		byPeer[pid] = append(byPeer[pid], c)
	}

	for pid, peerChunks := range byPeer {
		if !m.IsPeerConnected(pid) {
			continue
		}

		byFile := make(map[string][]store.PendingChunk)
		for _, c := range peerChunks {
			byFile[c.FileID] = append(byFile[c.FileID], c)
		}

		for fileID, fileChunks := range byFile {
			ttlSeconds := fileChunks[0].TTLSeconds
			batch := make([]webrtc.ChunkStoreRequest, len(fileChunks))
			regBatch := make([]muninn.RegisterChunkBatchEntry, len(fileChunks))
			for i, c := range fileChunks {
				batch[i] = webrtc.ChunkStoreRequest{
					FileID: c.FileID, ChunkIndex: c.ChunkIndex, Data: c.Data,
					SenderID: c.SenderID, RecipientID: c.RecipientID, Hash: c.Hash,
					Signature: c.Signature, TTLSeconds: ttlSeconds,
				}
				regBatch[i] = muninn.RegisterChunkBatchEntry{
					ChunkIndex: c.ChunkIndex, SenderID: c.SenderID, RecipientID: c.RecipientID,
					Hash: c.Hash, Signature: c.Signature, PeerID: pid,
				}
			}

			if err := m.rtcManager.SendChunkStoreBatch(pid, webrtc.ChunkStoreBatchRequest{Chunks: batch}); err != nil {
				log.Printf("distribute batch %s to %s: %v", fileID, pid, err)
				continue
			}

			if err := m.muninnClient.RegisterChunks(m.ctx, fileID, muninn.RegisterChunkBatchRequest{Chunks: regBatch}); err != nil {
				log.Printf("register batch %s on %s: %v", fileID, pid, err)
				/*
				for _, c := range fileChunks {
					if err := m.muninnClient.RegisterChunk(m.ctx, c.FileID, c.ChunkIndex, muninn.RegisterChunkRequest{
						SenderID: c.SenderID, RecipientID: c.RecipientID,
						Hash: c.Hash, Signature: c.Signature, PeerID: pid,
					}); err != nil {
						log.Printf("register chunk %s/%d on %s fallback warning: %v", c.FileID, c.ChunkIndex, pid, err)
					}
				}*/
			}

			for _, c := range fileChunks {
				if err := m.store.MarkChunkPlaced(c.FileID, c.ChunkIndex); err != nil {
					log.Printf("mark chunk placed %s/%d: %v", c.FileID, c.ChunkIndex, err)
				}
			}
		}
	}
}

func (m *Messenger) findPeerByID(id string) *muninn.Peer {
	m.mu.RLock()
	for _, p := range m.peers {
		if p.ID == id {
			m.mu.RUnlock()
			return &p
		}
	}
	m.mu.RUnlock()

	stored, err := m.muninnClient.Get(m.ctx, id)
	if err != nil || stored == nil {
		return nil
	}

	m.upsertPeer(stored.ID, stored.EncryptionKey, stored.SignatureKey, time.Now())

	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, p := range m.peers {
		if p.ID == id {
			return &p
		}
	}
	return nil
}

func (m *Messenger) SendMessageWithFiles(toPeerID, text string, filePaths []string, ttlSeconds int) error {
	peer := m.findPeerByID(toPeerID)
	if peer == nil {
		return fmt.Errorf("peer %s not found", toPeerID)
	}

	var files []FileMeta
	for _, fp := range filePaths {
		meta, err := m.sendFileChunks(toPeerID, fp, ttlSeconds)
		if err != nil {
			return fmt.Errorf("send file %s: %w", fp, err)
		}
		files = append(files, *meta)
	}

	msgID := uuid.New().String()

	if ttlSeconds <= 0 {
		ttlSeconds = config.ChunkTTLSeconds("1w")
	}

	if m.IsPeerOnline(toPeerID) {
		if !m.IsPeerConnected(toPeerID) {
			m.ConnectPeer(toPeerID)
		}
	}

	if m.IsPeerConnected(toPeerID) {
		now := time.Now()
		if err := m.rtcManager.SendMessage(toPeerID, text, now, msgID); err != nil {
			return m.sendOffline(msgID, text, peer, ttlSeconds, files)
		}
		cm := ChatMessage{From: m.ID, Text: text, Timestamp: now, MsgID: msgID, Files: files}
		jsonData, _ := json.Marshal(cm)
		if err := m.store.SaveMessage(msgID, toPeerID, m.ID, toPeerID, jsonData, cm.Timestamp); err != nil {
			log.Printf("save message: %v", err)
		}
		m.upsertPeer(toPeerID, peer.EncryptionKey, peer.SignatureKey, now)
		return m.sendOffline(msgID, text, peer, ttlSeconds, files)
	}

	return m.sendOffline(msgID, text, peer, ttlSeconds, files)
}

func (m *Messenger) replicatePendingChunks() {
	fileIDs, err := m.store.ListChunkFiles()
	if err != nil {
		log.Printf("list chunk files: %v", err)
		return
	}
	if len(fileIDs) == 0 {
		return
	}

	peers := m.getConnectedPeers()
	if len(peers) == 0 {
		return
	}

	for _, fileID := range fileIDs {
		chunkMap, err := m.store.ListChunks(fileID)
		if err != nil {
			continue
		}
		for _, peer := range peers {
			batch := make([]webrtc.ChunkStoreRequest, 0, len(chunkMap))
			for idx, data := range chunkMap {
				batch = append(batch, webrtc.ChunkStoreRequest{
					FileID: fileID, ChunkIndex: idx, Data: data, TTLSeconds: 604800,
				})
			}
			if len(batch) == 0 {
				continue
			}
			if err := m.rtcManager.SendChunkStoreBatch(peer.ID, webrtc.ChunkStoreBatchRequest{Chunks: batch}); err != nil {
				log.Printf("replicate chunks %s to %s: %v", fileID, peer.ID, err)
				m.DisconnectPeer(peer.ID)
			}
		}
	}
}

func (m *Messenger) sendFileChunks(recipientID, filePath string, ttlSeconds int) (*FileMeta, error) {
	filedata, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}

	fileID := uuid.New().String()
	filename := filepath.Base(filePath)

	aesKey := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, aesKey); err != nil {
		return nil, fmt.Errorf("generate file key: %w", err)
	}
	fileHash := sha256.Sum256(filedata)
	fileHashB64 := base64.StdEncoding.EncodeToString(fileHash[:])

	envelopes, err := chunk.SplitAndEncryptFile(fileID, m.ID, filedata, aesKey, m.signPrivate)
	if err != nil {
		return nil, fmt.Errorf("split encrypt file: %w", err)
	}

	if ttlSeconds <= 0 {
		ttlSeconds = config.ChunkTTLSeconds("1w")
	}

	type fileChunkData struct {
		envData []byte
		hash    string
		sig     string
	}
	chunks := make([]fileChunkData, len(envelopes))
	for i, env := range envelopes {
		envData, err := chunk.MarshalEnvelope(env)
		if err != nil {
			return nil, fmt.Errorf("marshal file env %d: %w", i, err)
		}
		if err := m.store.StoreChunk(fileID, i, envData, ttlSeconds); err != nil {
			return nil, fmt.Errorf("store file chunk %d: %w", i, err)
		}
		chunkHash := chunk.RegisteredHash(envData)
		expectedPayload := fmt.Sprintf("muninn/expected/v1\n%s\n%d\n%s", fileID, i, chunkHash)
		sig := crypto.Sign(m.signPrivate, []byte(expectedPayload))
		chunks[i] = fileChunkData{envData, chunkHash, crypto.EncodeKey(sig)}
	}

	thickPeers, err := m.muninnClient.GetBestThickPeers(m.ctx, 5)
	if err != nil {
		log.Printf("get best thick peers: %v, fallback to best peers", err)
		allPeers, err2 := m.muninnClient.GetBestPeers(m.ctx, 5)
		if err2 != nil {
			thickPeers = m.getOnlinePeers()
		} else {
			thickPeers = allPeers
		}
	}

	storagePeers := []string{}
	for _, p := range thickPeers {
		if p.ID == m.ID {
			continue
		}
		if !m.IsPeerConnected(p.ID) {
			m.ConnectPeer(p.ID)
		}
		storagePeers = append(storagePeers, p.ID)
	}

	for i := 0; i < 30 && len(storagePeers) > 0; i++ {
		time.Sleep(100 * time.Millisecond)
		allConnected := true
		for _, pid := range storagePeers {
			if !m.IsPeerConnected(pid) {
				allConnected = false
				break
			}
		}
		if allConnected {
			break
		}
	}

		for _, pid := range storagePeers {
		if !m.IsPeerConnected(pid) {
			continue
		}

		batch := make([]webrtc.ChunkStoreRequest, len(chunks))
		regBatch := make([]muninn.RegisterChunkBatchEntry, len(chunks))
		for i, c := range chunks {
			batch[i] = webrtc.ChunkStoreRequest{
				FileID: fileID, ChunkIndex: i, Data: c.envData,
				SenderID: m.ID, Hash: c.hash, Signature: c.sig,
				TTLSeconds: ttlSeconds,
			}
			regBatch[i] = muninn.RegisterChunkBatchEntry{
				ChunkIndex: i, SenderID: m.ID, Hash: c.hash,
				Signature: c.sig, PeerID: pid, Persist: true,
			}
		}

		if err := m.rtcManager.SendChunkStoreBatch(pid, webrtc.ChunkStoreBatchRequest{Chunks: batch}); err != nil {
			log.Printf("distribute file chunks %s to %s: %v", fileID, pid, err)
		}

		if err := m.muninnClient.RegisterChunks(m.ctx, fileID, muninn.RegisterChunkBatchRequest{Chunks: regBatch}); err != nil {
			log.Printf("register file chunks %s on %s: %v", fileID, pid, err)
		}
	}

	localRegBatch := make([]muninn.RegisterChunkBatchEntry, len(chunks))
	for i, c := range chunks {
		localRegBatch[i] = muninn.RegisterChunkBatchEntry{
			ChunkIndex: i, SenderID: m.ID,
			Hash: c.hash, Signature: c.sig, PeerID: m.ID, Persist: true,
		}
	}
	if err := m.muninnClient.RegisterChunks(m.ctx, fileID, muninn.RegisterChunkBatchRequest{Chunks: localRegBatch}); err != nil {
		log.Printf("register file chunks %s on self: %v", fileID, err)
	}

	log.Printf("file %s sent as %s (%d chunks)", filename, fileID, len(chunks))
	return &FileMeta{FileID: fileID, FileHash: fileHashB64, DecryptionKey: crypto.EncodeKey(aesKey), TotalChunks: len(chunks), Filename: filename}, nil
}

func (m *Messenger) SendMessage(toPeerID, text string, ttlSeconds int) error {
	msgID := uuid.New().String()
	peer := m.findPeerByID(toPeerID)
	if peer == nil {
		return fmt.Errorf("peer %s not found", toPeerID)
	}

	if ttlSeconds <= 0 {
		ttlSeconds = config.ChunkTTLSeconds("1w")
	}

	if m.IsPeerOnline(toPeerID) {
		if !m.IsPeerConnected(toPeerID) {
			m.ConnectPeer(toPeerID)
		}
	}

	if m.IsPeerConnected(toPeerID) {
		now := time.Now()
		if err := m.rtcManager.SendMessage(toPeerID, text, now, msgID); err != nil {
			return m.sendOffline(msgID, text, peer, ttlSeconds, nil)
		}
		cm := ChatMessage{From: m.ID, Text: text, Timestamp: now, MsgID: msgID}
		jsonData, _ := json.Marshal(cm)
		if err := m.store.SaveMessage(msgID, toPeerID, m.ID, toPeerID, jsonData, cm.Timestamp); err != nil {
			log.Printf("save message: %v", err)
		}
		m.upsertPeer(toPeerID, peer.EncryptionKey, peer.SignatureKey, now)
		return m.sendOffline(msgID, text, peer, ttlSeconds, nil)
	}

	return m.sendOffline(msgID, text, peer, ttlSeconds, nil)
}

func (m *Messenger) ConnectPeer(toPeerID string) error {
	offer, err := m.rtcManager.CreateOffer(toPeerID)
	if err != nil {
		return fmt.Errorf("create offer: %w", err)
	}
	offerData, _ := json.Marshal(offer)

	if m.rtcClient != nil && m.rtcClient.IsConnected() {
		if err := m.rtcClient.ConnectToPeer(m.ctx, toPeerID, string(offerData)); err == nil {
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

func (m *Messenger) sendOffline(msgID, text string, peer *muninn.Peer, ttlSeconds int, files []FileMeta) error {
	log.Printf("sending offline message %s to %s via chunks", msgID, peer.ID)

	now := time.Now()

	payload := MessagePayload{Text: text, Timestamp: now, Files: files}
	payloadData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	recipientPubKey, err := crypto.DecodeKey(peer.EncryptionKey)
	if err != nil {
		return fmt.Errorf("decode recipient enc key: %w", err)
	}

	envelopes, err := chunk.SplitAndEncrypt(msgID, m.ID, peer.ID, payloadData, recipientPubKey, m.signPrivate)
	if err != nil {
		return fmt.Errorf("split encrypt: %w", err)
	}

	if ttlSeconds <= 0 {
		ttlSeconds = config.ChunkTTLSeconds("1w")
	}

	type chunkData struct {
		envData []byte
		hash    string
		sig     string
	}
	chunks := make([]chunkData, len(envelopes))
	for i, env := range envelopes {
		envData, err := chunk.MarshalEnvelope(env)
		if err != nil {
			return fmt.Errorf("marshal env %d: %w", i, err)
		}
		if err := m.store.StoreChunk(msgID, i, envData, ttlSeconds); err != nil {
			return fmt.Errorf("store chunk %d: %w", i, err)
		}
		chunkHash := chunk.RegisteredHash(envData)
		expectedPayload := fmt.Sprintf("muninn/expected/v1\n%s\n%d\n%s", msgID, i, chunkHash)
		sig := crypto.Sign(m.signPrivate, []byte(expectedPayload))
		chunks[i] = chunkData{envData, chunkHash, crypto.EncodeKey(sig)}
	}

	onlinePeers, err := m.muninnClient.GetBestPeers(m.ctx, 10)
	if err != nil {
		onlinePeers = m.getOnlinePeers()
	}

	storagePeers := []string{}
	for _, p := range onlinePeers {
		if p.ID == m.ID || p.ID == peer.ID {
			continue
		}
		if !m.IsPeerConnected(p.ID) {
			m.ConnectPeer(p.ID)
		}
		storagePeers = append(storagePeers, p.ID)
	}

	for i := 0; i < 30 && len(storagePeers) > 0; i++ {
		time.Sleep(100 * time.Millisecond)
		allConnected := true
		for _, pid := range storagePeers {
			if !m.IsPeerConnected(pid) {
				allConnected = false
				break
			}
		}
		if allConnected {
			break
		}
	}

	storedOnPeers := make(map[int][]string, len(chunks))
	for i := range chunks {
		storedOnPeers[i] = []string{m.ID}
	}

	localRegBatch := make([]muninn.RegisterChunkBatchEntry, len(chunks))
	for i, c := range chunks {
		localRegBatch[i] = muninn.RegisterChunkBatchEntry{
			ChunkIndex: i, SenderID: m.ID, RecipientID: peer.ID,
			Hash: c.hash, Signature: c.sig, PeerID: m.ID,
		}
	}
	if err := m.muninnClient.RegisterChunks(m.ctx, msgID, muninn.RegisterChunkBatchRequest{Chunks: localRegBatch}); err != nil {
		log.Printf("register batch %s on %s: %v", msgID,  peer.ID, err)
		/*
		for i, c := range chunks {
			if err := m.muninnClient.RegisterChunk(m.ctx, msgID, i, muninn.RegisterChunkRequest{
				SenderID: m.ID, RecipientID: peer.ID, Hash: c.hash, Signature: c.sig, PeerID: m.ID,
			}); err != nil {
				log.Printf("register local chunk %d warning: %v", i, err)
			}
		}*/
	}

		for _, pid := range storagePeers {
		if !m.IsPeerConnected(pid) {
			continue
		}

		batch := make([]webrtc.ChunkStoreRequest, len(chunks))
		for i, c := range chunks {
			batch[i] = webrtc.ChunkStoreRequest{
				FileID: msgID, ChunkIndex: i, Data: c.envData,
				SenderID: m.ID, RecipientID: peer.ID, Hash: c.hash,
				Signature: c.sig, TTLSeconds: ttlSeconds,
			}
		}
		if err := m.rtcManager.SendChunkStoreBatch(pid, webrtc.ChunkStoreBatchRequest{Chunks: batch}); err != nil {
			continue
		}
		for i := range chunks {
			storedOnPeers[i] = append(storedOnPeers[i], pid)
		}

		regBatch := make([]muninn.RegisterChunkBatchEntry, len(chunks))
		for i, c := range chunks {
			regBatch[i] = muninn.RegisterChunkBatchEntry{
				ChunkIndex: i, SenderID: m.ID, RecipientID: peer.ID,
				Hash: c.hash, Signature: c.sig, PeerID: pid,
			}
		}
		if err := m.muninnClient.RegisterChunks(m.ctx, msgID, muninn.RegisterChunkBatchRequest{Chunks: regBatch}); err != nil {
			log.Printf("register batch %s on %s: %v", msgID, pid, err)
			/*
			for i, c := range chunks {
				if err := m.muninnClient.RegisterChunk(m.ctx, msgID, i, muninn.RegisterChunkRequest{
					SenderID: m.ID, RecipientID: peer.ID, Hash: c.hash, Signature: c.sig, PeerID: pid,
				}); err != nil {
					log.Printf("register chunk %d on %s fallback warning: %v", i, pid, err)
				}
			}*/
		}
	}

	for i, c := range chunks {
		if err := m.store.StorePendingChunk(&store.PendingChunk{
			FileID:      msgID,
			ChunkIndex:  i,
			RecipientID: peer.ID,
			SenderID:    m.ID,
			Data:        c.envData,
			Hash:        c.hash,
			Signature:   c.sig,
			CreatedAt:   time.Now(),
			Placed:      len(storedOnPeers[i]) > 1,
			TTLSeconds:  ttlSeconds,
		}); err != nil {
			log.Printf("store pending chunk %s/%d: %v", msgID, i, err)
		}
	}

	cm := ChatMessage{From: m.ID, Text: text, Timestamp: now, MsgID: msgID, Files: files}
	jsonData, _ := json.Marshal(cm)
	if err := m.store.SaveMessage(msgID, peer.ID, m.ID, peer.ID, jsonData, cm.Timestamp); err != nil {
		log.Printf("save message: %v", err)
	}
	m.upsertPeer(peer.ID, peer.EncryptionKey, peer.SignatureKey, now)
	return nil
}

func (m *Messenger) checkPendingMessages() {
	chunks, err := m.muninnClient.GetChunksByRecipient(m.ctx, m.ID)
	if err != nil {
		return
	}
	if len(chunks) == 0 {
		return
	}

	byMsg := make(map[string][]muninn.ChunkRecord)
	for _, c := range chunks {
		byMsg[c.FileID] = append(byMsg[c.FileID], c)
	}
	for msgID, msgChunks := range byMsg {
		m.collectAndProcessMessage(msgID, msgChunks)
	}
}

func (m *Messenger) tryProcessMsg(msgID string) bool {
	m.processingMu.Lock()
	if m.processingMsg[msgID] {
		m.processingMu.Unlock()
		return false
	}
	m.processingMsg[msgID] = true
	m.processingMu.Unlock()
	return true
}

func (m *Messenger) releaseProcessMsg(msgID string) {
	m.processingMu.Lock()
	delete(m.processingMsg, msgID)
	m.processingMu.Unlock()
}

func (m *Messenger) collectAndProcessMessage(msgID string, records []muninn.ChunkRecord) {
	if !m.tryProcessMsg(msgID) {
		return
	}
	defer m.releaseProcessMsg(msgID)
	log.Printf("collecting %s (%d chunk records, persist=%v)", msgID, len(records), len(records) > 0 && records[0].Persist)

	seen := make(map[int]bool)
	var chunkData [][]byte

	for _, rec := range records {
		if seen[rec.ChunkIndex] {
			continue
		}

		data, ok := m.getChunkData(rec)
		if !ok {
			log.Printf("not collected any data: %s/%d", rec.FileID, rec.ChunkIndex)
			continue
		}
		if rec.Hash != "" && chunk.RegisteredHash(data) != rec.Hash {
			log.Printf("hash mismatch for chunk %s/%d: got %s, expected %s",
				rec.FileID, rec.ChunkIndex, chunk.RegisteredHash(data), rec.Hash)
			continue
		}
		chunkData = append(chunkData, data)
		seen[rec.ChunkIndex] = true
	}

	if len(chunkData) == 0 {
		log.Printf("not collected any data: %s", msgID)
		return
	}

	var envelopes []chunk.Envelope
	for _, data := range chunkData {
		env, err := chunk.UnmarshalEnvelope(data)
		if err != nil {
			log.Printf("invalid envelope for chunk: %v", err)
			continue
		}
		envelopes = append(envelopes, env)
	}

	if len(envelopes) != len(chunkData) {
		log.Printf("incomplete %s: got %d envelopes", msgID, len(envelopes))
		return
	}

	totalChunks := envelopes[0].TotalChunks
	if len(envelopes) < totalChunks {
		log.Printf("%s: got %d/%d chunks, waiting for more", msgID, len(envelopes), totalChunks)
		return
	}

	senderPeer := m.findPeerByID(records[0].SenderID)
	if senderPeer == nil {
		log.Printf("sender %s not found for %s", records[0].SenderID, msgID)
		return
	}

	senderSignKey, err := crypto.DecodeKey(senderPeer.SignatureKey)
	if err != nil {
		log.Printf("decode sender sign key: %v", err)
		return
	}

	if records[0].Persist {
		log.Printf("file chunks %s ready in store (%d envelopes), waiting for message with decryption key", msgID, len(envelopes))
		return
	}

	plaintext, err := chunk.AssembleAndDecrypt(envelopes, m.encPrivate, m.encPublic, senderSignKey)
	if err != nil {
		log.Printf("assemble/decrypt message %s: %v", msgID, err)
		return
	}

	var payload MessagePayload
	if err := json.Unmarshal(plaintext, &payload); err != nil {
		payload = MessagePayload{Text: string(plaintext)}
	}
	if payload.Timestamp.IsZero() {
		payload.Timestamp = time.Now()
	}

	for _, f := range payload.Files {
		m.processReceivedFile(f, records[0].SenderID)
	}

	decryptedMsg := ChatMessage{
		From:      records[0].SenderID,
		Text:      payload.Text,
		Timestamp: payload.Timestamp,
		MsgID:     msgID,
		Files:     payload.Files,
	}					

	jsonData, _ := json.Marshal(decryptedMsg)
	if err := m.store.SaveMessage(msgID, records[0].SenderID, decryptedMsg.From, decryptedMsg.From, jsonData, decryptedMsg.Timestamp); err != nil {
		log.Printf("save message: %v", err)
	}
	m.upsertPeer(decryptedMsg.From, senderPeer.EncryptionKey, senderPeer.SignatureKey, decryptedMsg.Timestamp)

	m.msgSubsMu.Lock()
	for _, sub := range m.msgSubs {
		select {
		case sub <- decryptedMsg:
		default:
		}
	}
	m.msgSubsMu.Unlock()

	if err := m.store.DeleteChunks(msgID); err != nil {
		log.Printf("delete chunks for %s: %v", msgID, err)
	}

	log.Printf("message %s delivered from %s", msgID, records[0].SenderID)
}

func (m *Messenger) processReceivedFile(f FileMeta, senderID string) {
	if !m.tryProcessMsg("file:" + f.FileID) {
		return
	}
	defer m.releaseProcessMsg("file:" + f.FileID)
	chunkMap, err := m.store.ListChunks(f.FileID)
	if err != nil {
		log.Printf("list chunks for file %s: %v", f.FileID, err)
		return
	}

	if len(chunkMap) < f.TotalChunks {
		log.Printf("file %s: have %d/%d chunks, requesting missing from peers", f.FileID, len(chunkMap), f.TotalChunks)
		for i := 0; i < f.TotalChunks; i++ {
			if _, ok := chunkMap[i]; !ok {
				m.requestMissingChunk(f.FileID, i, senderID)
			}
		}
		m.pendingMu.Lock()
		m.pendingFileDownloads[f.FileID] = &pendingFileDownload{fileMeta: f, senderID: senderID}
		m.pendingMu.Unlock()
		return
	}

	env0data, ok := chunkMap[0]
	if !ok {
		log.Printf("missing chunk 0 for file %s", f.FileID)
		return
	}
	env0, err := chunk.UnmarshalEnvelope(env0data)
	if err != nil {
		log.Printf("unmarshal envelope 0 for file %s: %v", f.FileID, err)
		return
	}

	envelopes := make([]chunk.Envelope, f.TotalChunks)
	envelopes[0] = env0
	for i := 1; i < f.TotalChunks; i++ {
		data, ok := chunkMap[i]
		if !ok {
			log.Printf("missing chunk %d/%d for file %s", i, f.TotalChunks, f.FileID)
			return
		}
		env, err := chunk.UnmarshalEnvelope(data)
		if err != nil {
			log.Printf("unmarshal envelope %d for file %s: %v", i, f.FileID, err)
			return
		}
		envelopes[i] = env
	}

	senderPeer := m.findPeerByID(senderID)
	if senderPeer == nil {
		log.Printf("sender %s not found for file %s", senderID, f.FileID)
		return
	}
	senderSignKey, err := crypto.DecodeKey(senderPeer.SignatureKey)
	if err != nil {
		log.Printf("decode sender sign key for file %s: %v", f.FileID, err)
		return
	}

	aesKey, err := crypto.DecodeKey(f.DecryptionKey)
	if err != nil {
		log.Printf("decode decryption key for file %s: %v", f.FileID, err)
		return
	}

	plaintext, err := chunk.AssembleAndDecryptFile(envelopes, aesKey, senderSignKey)
	if err != nil {
		log.Printf("assemble/decrypt file %s: %v", f.FileID, err)
		return
	}

	actualHash := sha256.Sum256(plaintext)
	actualHashB64 := base64.StdEncoding.EncodeToString(actualHash[:])
	if actualHashB64 != f.FileHash {
		log.Printf("file %s hash mismatch: got %s, expected %s", f.FileID, actualHashB64, f.FileHash)
		return
	}

	outputName := f.Filename
	if outputName == "" {
		outputName = f.FileID
	}
	fp := filepath.Join(m.downloadsDir, outputName)
	if _, err := os.Stat(fp); err == nil {
		log.Printf("file %s already exists at %s, skipping", f.FileID, fp)
		m.pendingMu.Lock()
		delete(m.pendingFileDownloads, f.FileID)
		m.pendingMu.Unlock()
		return
	}
	if err := os.WriteFile(fp, plaintext, 0644); err != nil {
		log.Printf("save file %s: %v", fp, err)
		return
	}
	log.Printf("file saved: %s (%d bytes)", fp, len(plaintext))

	m.pendingMu.Lock()
	delete(m.pendingFileDownloads, f.FileID)
	m.pendingMu.Unlock()
}

func (m *Messenger) requestMissingChunk(fileID string, chunkIndex int, senderID string) {
	targets := []string{senderID}
	for _, p := range m.getConnectedPeers() {
		if p.ID != senderID {
			targets = append(targets, p.ID)
		}
	}
	for _, pid := range targets {
		if m.IsPeerConnected(pid) {
			m.rtcManager.SendChunkGet(pid, webrtc.ChunkGetRequest{
				FileID: fileID, ChunkIndex: chunkIndex,
			})
		} else if pid == senderID {
			go m.ConnectPeer(pid)
		}
	}
}

func (m *Messenger) checkPendingFileDownloads() {
	m.pendingMu.Lock()
	fileIDs := make([]string, 0, len(m.pendingFileDownloads))
	for fileID := range m.pendingFileDownloads {
		fileIDs = append(fileIDs, fileID)
	}
	m.pendingMu.Unlock()

	for _, fileID := range fileIDs {
		m.pendingMu.Lock()
		pd, ok := m.pendingFileDownloads[fileID]
		m.pendingMu.Unlock()
		if !ok {
			continue
		}
		m.processReceivedFile(pd.fileMeta, pd.senderID)
	}
}

func (m *Messenger) fileDownloadLoop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.checkPendingFileDownloads()
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Messenger) getChunkData(rec muninn.ChunkRecord) ([]byte, bool) {
	data, err := m.store.GetChunk(rec.FileID, rec.ChunkIndex)
	if err == nil && data != nil {
		return data, true
	}

	if rec.PeerID == m.ID {
		return nil, false
	}

	if m.IsPeerConnected(rec.PeerID) {
		m.rtcManager.SendChunkGet(rec.PeerID, webrtc.ChunkGetRequest{
			FileID:     rec.FileID,
			ChunkIndex: rec.ChunkIndex,
		})
	} else {
		m.ConnectPeer(rec.PeerID)
		go func() {
			for i := 0; i < 50; i++ {
				select {
				case <-m.ctx.Done():
					return
				case <-time.After(100 * time.Millisecond):
				}
				if m.IsPeerConnected(rec.PeerID) {
					m.rtcManager.SendChunkGet(rec.PeerID, webrtc.ChunkGetRequest{
						FileID:     rec.FileID,
						ChunkIndex: rec.ChunkIndex,
					})
					return
				}
			}
			log.Printf("getChunkData: failed to connect to %s within 5s", rec.PeerID)
		}()
	}

	return nil, false
}

func (m *Messenger) GetMessages(peerID string) []ChatMessage {
	dataList, err := m.store.GetMessages(peerID)
	if err != nil {
		return nil
	}
	result := make([]ChatMessage, 0, len(dataList))
	for _, data := range dataList {
		var msg ChatMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		result = append(result, msg)
	}
	return result
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

func (m *Messenger) SubscribeMessages() chan ChatMessage {
	ch := make(chan ChatMessage, 50)
	m.msgSubsMu.Lock()
	m.msgSubs = append(m.msgSubs, ch)
	m.msgSubsMu.Unlock()
	return ch
}

func (m *Messenger) UnsubscribeMessages(ch chan ChatMessage) {
	m.msgSubsMu.Lock()
	for i, c := range m.msgSubs {
		if c == ch {
			m.msgSubs = append(m.msgSubs[:i], m.msgSubs[i+1:]...)
			close(ch)
			break
		}
	}
	m.msgSubsMu.Unlock()
}

func (m *Messenger) DownloadsDir() string {
	return m.downloadsDir
}

func (m *Messenger) StoredChunkData(fileID string, chunkIndex int) ([]byte, bool) {
	data, err := m.store.GetChunk(fileID, chunkIndex)
	if err != nil || data == nil {
		return nil, false
	}
	return data, true
}

func (m *Messenger) InjectChunk(fileID string, chunkIndex int, data []byte) {
	if err := m.store.StoreChunk(fileID, chunkIndex, data, 604800); err != nil {
		log.Printf("inject chunk: %v", err)
	}
	go m.checkPendingMessages()
	go m.checkPendingFileDownloads()
}

func (m *Messenger) Shutdown() {
	m.cancel()
	delCtx, delCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer delCancel()
	m.muninnClient.Delete(delCtx, m.ID)
	m.rtcClient.Close()
	m.rtcManager.CloseAll()
	m.store.Close()
}
