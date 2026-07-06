package messenger

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"strings"
	"slices"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/chunk"
	"github.com/killbane1232/huginn-messenger/internal/config"
	"github.com/killbane1232/huginn-messenger/internal/crypto"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
	"github.com/killbane1232/huginn-messenger/internal/store"
	"github.com/killbane1232/huginn-messenger/internal/webrtc"
	"github.com/google/uuid"
    //"runtime/debug"
	pion "github.com/pion/webrtc/v4"
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

type FileReadyEvent struct {
	FileID   string `json:"file_id"`
	FilePath string `json:"file_path"`
	Filename string `json:"filename"`
	SenderID string `json:"sender_id"`
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
	peerID     string
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

func WithPeerID(id string) MessengerOption {
	return func(o *messengerOpts) {
		o.peerID = id
	}
}

type Messenger struct {
	ID       string
	Key       string
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

	peersMap            map[string]muninn.Peer
	peers            []muninn.Peer
	peersConnecting  map[string]struct{}
	mu               sync.RWMutex

	peerSubs   map[string]chan struct{}
	subsMu     sync.Mutex
	msgSubs    []chan ChatMessage
	msgSubsMu  sync.Mutex
	fileReadySubs   []chan FileReadyEvent
	fileReadySubsMu sync.Mutex

	ctx          context.Context
	cancel       context.CancelFunc
	peerFlag     muninn.PeerFlag
	downloadsDir string

	pendingFileDownloads map[string]*pendingFileDownload
	pendingMu            sync.Mutex

	processingMsg map[string]bool
	processingMu  sync.Mutex

	appConfig *config.Config
	reloginMu   sync.Mutex
	reloginKeys string
}

func New(username string, muninnClient *muninn.Client, dbPath string, opts ...MessengerOption) (*Messenger, error) {
	var o messengerOpts
	for _, opt := range opts {
		opt(&o)
	}

	st, err := store.New(dbPath)
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}

	dbDir := filepath.Dir(dbPath)
	if err := st.ImportLegacyFiles(dbDir); err != nil {
		log.Printf("import legacy config files: %v", err)
	}

	var signPub ed25519.PublicKey
	var signPriv ed25519.PrivateKey
	var encPriv, encPub []byte

	keysJSON, keysErr := st.GetKeysJSON()
	if keysErr == nil {
		signPub, signPriv, encPriv, encPub, err = crypto.ParseKeyFile([]byte(keysJSON))
		if err != nil {
			return nil, fmt.Errorf("parse stored keys: %w", err)
		}
	} else if !errors.Is(keysErr, store.ErrConfigNotFound) {
		return nil, fmt.Errorf("load keys: %w", keysErr)
	} else {
		signPub, signPriv, err = crypto.GenerateSigningKey()
		if err != nil {
			return nil, fmt.Errorf("generate signing key: %w", err)
		}
		encPriv, encPub, err = crypto.GenerateEncryptionKey()
		if err != nil {
			return nil, fmt.Errorf("generate encryption key: %w", err)
		}
		keyData, err := crypto.FormatKeyFile(signPub, signPriv, encPriv, encPub)
		if err != nil {
			return nil, fmt.Errorf("format keys: %w", err)
		}
		if err := st.SaveKeysJSON(string(keyData)); err != nil {
			log.Printf("save keys to db: %v", err)
		}
	}

	appCfg := &config.Config{DBPath: dbPath}
	storedCfg, cfgErr := st.LoadAppConfig()
	switch {
	case cfgErr == nil:
		*appCfg = *storedCfg
		appCfg.DBPath = dbPath
		if appCfg.Username == "" && username != "" {
			appCfg.Username = username
			log.Printf("messenger: using passed username (stored had empty)")
		}
	case errors.Is(cfgErr, store.ErrConfigNotFound) && username != "":
		appCfg.Username = username
		log.Printf("messenger: new config with username=%q", username)
	default:
		log.Printf("messenger: cfgErr=%v, username=%q", cfgErr, username)
	}
	if appCfg.ChunkTTL == "" {
		appCfg.ChunkTTL = "1w"
	}
	if appCfg.PeerFlag == "" {
		appCfg.PeerFlag = "thin"
	}
	if err := st.SaveAppConfig(appCfg); err != nil {
		log.Printf("save initial config to db: %v", err)
	}
	log.Printf("messenger: final username=%q", appCfg.Username)

	downloadsDir := filepath.Join(filepath.Dir(dbPath), "downloads", "huginn")
	if err := os.MkdirAll(downloadsDir, 0755); err != nil {
		return nil, fmt.Errorf("create downloads dir: %w", err)
	}

	rtcMsgChan := make(chan webrtc.ChatMessage, 100)
	signalChan := make(chan muninn.Signal, 100)
	ctx, cancel := context.WithCancel(context.Background())

	peerID := appCfg.PeerID
	if peerID == "" {
		peerID = uuid.New().String()
	}
	appCfg.PeerID = peerID
	oldUsername := appCfg.Username
	if oldUsername == "" {
		oldUsername = peerID
	}
	key := username + ":" + crypto.EncodeKey(signPub)
	m := &Messenger{
		ID:       peerID,
		Key:      key,
		Username: oldUsername,

		peersMap: make(map[string]muninn.Peer),
		signPublic:  signPub,
		signPrivate: signPriv,
		encPrivate:  encPriv,
		encPublic:   encPub,

		muninnClient: muninnClient,
		rtcMsgChan:   rtcMsgChan,
		signalChan:   signalChan,
		store:            st,
		peersConnecting:  make(map[string]struct{}),
		peerSubs:         make(map[string]chan struct{}),
		ctx:          ctx,
		cancel:       cancel,
		peerFlag:     o.peerFlag,
		downloadsDir: downloadsDir,

		pendingFileDownloads: make(map[string]*pendingFileDownload),
		processingMsg:       make(map[string]bool),
		appConfig:           appCfg,
	}

	o.iceServers = []pion.ICEServer{
		{
			URLs:       []string{
				"turn:stun.evil-bread.ru:3478",
				"turn:turn.evil-bread.ru:3478?transport=udp",
				"turn:turn.evil-bread.ru:3478?transport=tcp",
				"turns:turn.evil-bread.ru:5349?transport=tcp",
			},
			Username:   "turnuser",
			Credential: "turnpass",
		},
	}
	m.rtcManager = webrtc.NewManager(peerID, rtcMsgChan, m.handleChunkStore, m.handleChunkGet,
		m.handleReloginRequest, m.handleReloginResponse, o.iceServers)

	m.rtcClient = muninn.NewRTCClient(muninnClient.BaseURL(), peerID, o.iceServers)
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
		munPeer := peer.ToMuninnPeer()
		m.peersMap[peer.PeerID] = munPeer
		m.peers = append(m.peers, munPeer)
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
	// Если мы не являемся конечным получателем — сообщаем серверу, что сохранили чанк
	if req.RecipientID != "" && req.RecipientID != m.Key && req.Hash != "" && req.SenderID != "" {
		reportedPayload := fmt.Sprintf("muninn/reported/v1\n%s\n%d\n%s\n%s",
			req.FileID, req.ChunkIndex, req.Hash, req.SenderID)
		sig := crypto.Sign(m.signPrivate, []byte(reportedPayload))
		reportReq := muninn.ChunkReportRequest{
			ReporterID: m.ID,
			FileID:     req.FileID,
			ChunkIndex: req.ChunkIndex,
			Hash:       req.Hash,
			Signature:  crypto.EncodeKey(sig),
		}
		if err := m.muninnClient.ReportChunk(m.ctx, req.SenderID, reportReq); err != nil {
			log.Printf("report chunk %s/%d failed, not saving: %v", req.FileID, req.ChunkIndex, err)
			return
		}
		log.Printf("reported chunk %s/%d as storage peer", req.FileID, req.ChunkIndex)
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
			displayText := m.checkInviteText(msg.Text)
			cm := ChatMessage{
				From:      msg.From,
				Text:      displayText,
				Timestamp: msg.Timestamp,
				MsgID:     msg.MsgID,
			}
			if cm.Timestamp.IsZero() {
				cm.Timestamp = time.Now()
			}
			jsonData, _ := json.Marshal(cm)
			fromKey := msg.From
			if p := m.findPeerByID(msg.From); p != nil {
				fromKey = p.Key
			}
			if err := m.store.SaveMessage(msg.MsgID, fromKey, fromKey, fromKey, jsonData, cm.Timestamp); err != nil {
				log.Printf("save message: %v", err)
			}
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
			//log.Printf("[rtc] reconnect stack:\n%s", debug.Stack())
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
					groups, err := m.store.GetGroupChats()
					if err != nil {
						continue
					}
					for _, g := range groups {
						m.registerGroupPeer(g)
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
			if err := m.store.DeleteExpiredFailedChunks(time.Now().Unix()); err != nil {
				log.Printf("cleanup expired failed chunks: %v", err)
			}
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Messenger) Register() error {
	req := &muninn.RegisterRequest{
		ID:       m.ID,
		Login:     m.Username,
		Addresses: []string{""},
		EncryptionKey: crypto.EncodeKey(m.encPublic),
		SignatureKey:  crypto.EncodeKey(m.signPublic),
		Metadata: map[string]string{
			"username": m.Username,
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
			if s.PeerID == m.Key {
				continue
			}
			seen[s.PeerID] = &muninn.Peer{
				Key:           s.PeerID,
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
			if p.Key == m.Key {
				continue
			}
			if strings.Contains(strings.ToLower(p.Key), q) {
				m.upsertPeer(p.ID, p.Key, getLogin(p.Key), p.EncryptionKey, p.SignatureKey, p.LastSeen, false)
				if existing, ok := seen[p.Key]; ok {
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
					existing.TTLSeconds = p.TTLSeconds
					existing.QualityScore = p.QualityScore
				} else {
					cp := p
					seen[p.Key] = &cp
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
			Key:           peerKey,
			EncryptionKey: encryptionKey,
			SignatureKey:  signatureKey,
			LastSeen:      lastSeen,
			QualityScore:  100,
			IsFake:        isFake,
			IDS:		   []string{peerID},
		}
		if err := m.store.StorePeer(peerKey, login, encryptionKey, signatureKey, lastSeen, isFake); err != nil {
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
		if p.Key != m.Key {
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
				return p.LastSeen.After(time.Now().Add(time.Duration(- p.TTLSeconds / 2) * time.Second))
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
		if p.Key == key && p.IsFake {
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
		for _, pid := range p.IDS {
			if pid != m.ID && p.LastSeen.After(time.Now().Add(time.Duration(- p.TTLSeconds / 2) * time.Second)) {
				online = append(online, p)
				break
			}
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
		if p.ID == m.ID || p.Key == recipientID {
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

			if err := m.muninnClient.RegisterChunks(m.ctx, fileID, muninn.RegisterChunkBatchRequest{Chunks: regBatch}); err != nil {
				log.Printf("register batch %s on %s: %v", fileID, pid, err)
				continue
			}

			if err := m.rtcManager.SendChunkStoreBatch(pid, webrtc.ChunkStoreBatchRequest{Chunks: batch}); err != nil {
				log.Printf("distribute batch %s to %s: %v", fileID, pid, err)
				continue
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

	m.upsertPeer(stored.ID, stored.Key, getLogin(stored.Key), stored.EncryptionKey, stored.SignatureKey, time.Now(), stored.IsFake)

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
		m.upsertPeer(p.ID, p.Key, getLogin(p.Key), p.EncryptionKey, p.SignatureKey, time.Now(), p.IsFake)
	}
	m.mu.RLock()
	peer, exists = m.peersMap[key]
	m.mu.RUnlock()
	if exists {
		return &peer
	}
	return nil
}

func (m *Messenger) SendMessage(to, text string, filePaths []string, ttlSeconds int) error {
	go m.sendMessageAsync(to, text, filePaths, ttlSeconds)
	return nil
}

func (m *Messenger) sendMessageAsync(to, text string, filePaths []string, ttlSeconds int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in sendMessageAsync: %v", r)
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	if ttlSeconds <= 0 {
		ttlSeconds = config.ChunkTTLSeconds("1w")
	}

	peer := m.findPeerByKey(to)

	if peer == nil {
		if gc, err := m.store.GetGroupChat(to); err == nil {
			peer = &muninn.Peer{
				ID:            gc.UID,
				EncryptionKey: gc.EncPublic,
				SignatureKey:  gc.SignPublic,
				IsFake:        true,
			}
		}
	}

	if peer == nil {
		return fmt.Errorf("peer %s not found", to)
	}

	var files []FileMeta
	for _, fp := range filePaths {
		meta, err := m.sendFileChunks(to, fp, ttlSeconds)
		if err != nil {
			return fmt.Errorf("send file %s: %w", fp, err)
		}
		files = append(files, *meta)
	}

	msgID := uuid.New().String()

	onlinePeerID := peer.ID
	if onlinePeerID == "" {
		if p := m.findPeerByKey(to); p != nil {
			onlinePeerID = p.ID
		}
	}

	if onlinePeerID != "" && m.IsPeerOnline(onlinePeerID) {
		if !m.IsPeerConnected(onlinePeerID) {
			m.ConnectPeer(onlinePeerID)
		}
	}

	if onlinePeerID != "" && m.IsPeerConnected(onlinePeerID) {
		now := time.Now()
		if err := m.rtcManager.SendMessage(onlinePeerID, text, now, msgID); err != nil {
			return m.sendOffline(msgID, text, peer, ttlSeconds, files)
		}
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
				SenderID: m.Key, Hash: c.hash, Signature: c.sig,
				TTLSeconds: ttlSeconds,
			}
			regBatch[i] = muninn.RegisterChunkBatchEntry{
				ChunkIndex: i, SenderID: m.Key, Hash: c.hash,
				Signature: c.sig, PeerID: pid, Persist: true,
			}
		}

		if err := m.muninnClient.RegisterChunks(m.ctx, fileID, muninn.RegisterChunkBatchRequest{Chunks: regBatch}); err != nil {
			log.Printf("register file chunks %s on %s: %v", fileID, pid, err)
			continue
		}

		if err := m.rtcManager.SendChunkStoreBatch(pid, webrtc.ChunkStoreBatchRequest{Chunks: batch}); err != nil {
			log.Printf("distribute file chunks %s to %s: %v", fileID, pid, err)
		}
	}

	localRegBatch := make([]muninn.RegisterChunkBatchEntry, len(chunks))
	for i, c := range chunks {
		localRegBatch[i] = muninn.RegisterChunkBatchEntry{
			ChunkIndex: i, SenderID: m.Key,
			Hash: c.hash, Signature: c.sig, PeerID: m.ID, Persist: true,
		}
	}
	if err := m.muninnClient.RegisterChunks(m.ctx, fileID, muninn.RegisterChunkBatchRequest{Chunks: localRegBatch}); err != nil {
		log.Printf("register file chunks %s on self: %v", fileID, err)
	}

	log.Printf("file %s sent as %s (%d chunks)", filename, fileID, len(chunks))
	return &FileMeta{FileID: fileID, FileHash: fileHashB64, DecryptionKey: crypto.EncodeKey(aesKey), TotalChunks: len(chunks), Filename: filename}, nil
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

func (m *Messenger) GenerateReloginSignature() (string, error) {
	challenge := make([]byte, 32)
	if _, err := rand.Read(challenge); err != nil {
		return "", fmt.Errorf("generate challenge: %w", err)
	}
	sig := crypto.Sign(m.signPrivate, challenge)
	challengeB64 := base64.StdEncoding.EncodeToString(challenge)
	sigB64 := base64.StdEncoding.EncodeToString(sig)
	return fmt.Sprintf("%s:%s.%s", m.ID, challengeB64, sigB64), nil
}

func (m *Messenger) ApplyReloginSignature(signature string) error {
	parts := strings.SplitN(signature, ":", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid signature: missing peer ID")
	}
	peerID := parts[0]

	dataParts := strings.SplitN(parts[1], ".", 2)
	if len(dataParts) != 2 {
		return fmt.Errorf("invalid signature: missing challenge or signature")
	}
	challengeB64, sigB64 := dataParts[0], dataParts[1]

	challenge, err := base64.StdEncoding.DecodeString(challengeB64)
	if err != nil {
		return fmt.Errorf("decode challenge: %w", err)
	}
	sig, err := base64.StdEncoding.DecodeString(sigB64)
	if err != nil {
		return fmt.Errorf("decode signature: %w", err)
	}

	peer := m.findPeerByID(peerID)
	if peer == nil {
		return fmt.Errorf("peer %s not found", peerID)
	}
	peerSignKey, err := crypto.DecodeKey(peer.SignatureKey)
	if err != nil {
		return fmt.Errorf("decode peer sign key: %w", err)
	}
	if !crypto.Verify(ed25519.PublicKey(peerSignKey), challenge, sig) {
		return fmt.Errorf("invalid signature: not authorized")
	}

	if !m.rtcManager.IsConnected(peerID) {
		if err := m.ConnectPeer(peerID); err != nil {
			return fmt.Errorf("connect to %s: %w", peerID, err)
		}
	}

	for i := 0; i < 50; i++ {
		if m.rtcManager.IsConnected(peerID) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !m.rtcManager.IsConnected(peerID) {
		return fmt.Errorf("timed out waiting for WebRTC connection to %s", peerID)
	}

	m.reloginMu.Lock()
	m.reloginKeys = ""
	m.reloginMu.Unlock()

	m.rtcManager.SendReloginRequest(peerID, webrtc.ReloginRequest{Signature: signature})

	for i := 0; i < 300; i++ {
		m.reloginMu.Lock()
		data := m.reloginKeys
		m.reloginMu.Unlock()
		if data != "" {
			if err := m.store.SaveKeysJSON(data); err != nil {
				return fmt.Errorf("save keys: %w", err)
			}
			m.signPublic, m.signPrivate, m.encPrivate, m.encPublic, err = crypto.ParseKeyFile([]byte(data))
			if err != nil {
				return fmt.Errorf("parse relogin keys: %w", err)
			}
			m.Key = peer.Key
			peerUsername := getLogin(peer.Key)
			m.Username = peerUsername

			if m.appConfig == nil {
				m.appConfig = &config.Config{}
			}
			m.appConfig.Username = peerUsername
			m.appConfig.PeerID = peer.ID
			if err := m.store.SaveAppConfig(m.appConfig); err != nil {
				return fmt.Errorf("save config: %w", err)
			}
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("relogin: timed out waiting for response from %s", peerID)
}

func (m *Messenger) handleReloginRequest(peerID string, req webrtc.ReloginRequest) {
	parts := strings.SplitN(req.Signature, ":", 2)
	log.Printf("relogin: handle")
	if len(parts) != 2 {
		return
	}
	dataParts := strings.SplitN(parts[1], ".", 2)
	if len(dataParts) != 2 {
		return
	}
	challenge, err := base64.StdEncoding.DecodeString(dataParts[0])
	if err != nil {
		return
	}
	sig, err := base64.StdEncoding.DecodeString(dataParts[1])
	if err != nil {
		return
	}
	if !crypto.Verify(m.signPublic, challenge, sig) {
		log.Printf("relogin: invalid signature from %s", peerID)
		return
	}
	keysData, err := m.store.GetKeysJSON()
	if err != nil {
		log.Printf("relogin: read keys from db: %v", err)
		return
	}
	m.rtcManager.SendReloginResponse(peerID, webrtc.ReloginResponse{KeysData: keysData})
}

func (m *Messenger) handleReloginResponse(peerID string, resp webrtc.ReloginResponse) {
	m.reloginMu.Lock()
	m.reloginKeys = resp.KeysData
	m.reloginMu.Unlock()
}

func (m *Messenger) sendOffline(msgID, text string, peer *muninn.Peer, ttlSeconds int, files []FileMeta) error {
	log.Printf("sendOffline[%s]: start peer.ID=%q peer.Key=%q", msgID, peer.ID, peer.Key)

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
	log.Printf("sendOffline[%s]: split into %d chunks", msgID, len(envelopes))

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
	log.Printf("sendOffline[%s]: stored %d chunks locally", msgID, len(chunks))

	cm := ChatMessage{From: m.Username, Text: text, Timestamp: now, MsgID: msgID, Files: files}
	jsonData, _ := json.Marshal(cm)
	if err := m.store.SaveMessage(msgID, peer.Key, m.Key, peer.Key, jsonData, cm.Timestamp); err != nil {
		log.Printf("save message: %v", err)
	}

	for i := range chunks {
		if err := m.store.StorePendingChunk(&store.PendingChunk{
			FileID:      msgID,
			ChunkIndex:  i,
			RecipientID: peer.Key,
			SenderID:    m.Key,
			Data:        chunks[i].envData,
			Hash:        chunks[i].hash,
			Signature:   chunks[i].sig,
			CreatedAt:   time.Now(),
			Placed:      false,
			TTLSeconds:  ttlSeconds,
		}); err != nil {
			log.Printf("store pending chunk %s/%d: %v", msgID, i, err)
		}
	}
	log.Printf("sendOffline[%s]: stored pending chunks", msgID)

	localRegBatch := make([]muninn.RegisterChunkBatchEntry, len(chunks))
	for i, c := range chunks {
		localRegBatch[i] = muninn.RegisterChunkBatchEntry{
			ChunkIndex: i, SenderID: m.Key, RecipientID: peer.Key,
			Hash: c.hash, Signature: c.sig, PeerID: m.ID,
		}
	}
	log.Printf("sendOffline[%s]: registering chunks locally...", msgID)
	if err := m.muninnClient.RegisterChunks(m.ctx, msgID, muninn.RegisterChunkBatchRequest{Chunks: localRegBatch}); err != nil {
		log.Printf("register batch %s on self: %v", msgID, err)
	} else {
		log.Printf("sendOffline[%s]: registered chunks locally", msgID)
	}

	log.Printf("sendOffline[%s]: getting best peers...", msgID)
	onlinePeers, err := m.muninnClient.GetBestPeers(m.ctx, 10)
	if err != nil {
		log.Printf("sendOffline[%s]: GetBestPeers failed: %v, fallback to local", msgID, err)
		onlinePeers = m.getOnlinePeers()
	}
	log.Printf("sendOffline[%s]: got %d best peers", msgID, len(onlinePeers))

	storagePeers := []string{}
	for _, p := range onlinePeers {
		if p.ID == m.ID || (peer.ID != "" && p.ID == peer.ID) {
			continue
		}
		if !m.IsPeerConnected(p.ID) {
			m.ConnectPeer(p.ID)
		}
		storagePeers = append(storagePeers, p.ID)
	}
	log.Printf("sendOffline[%s]: connecting to %d storage peers", msgID, len(storagePeers))

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

	connected := 0
	for _, pid := range storagePeers {
		if !m.IsPeerConnected(pid) {
			continue
		}
		connected++

		batch := make([]webrtc.ChunkStoreRequest, len(chunks))
		regBatch := make([]muninn.RegisterChunkBatchEntry, len(chunks))
		for i, c := range chunks {
			batch[i] = webrtc.ChunkStoreRequest{
				FileID: msgID, ChunkIndex: i, Data: c.envData,
				SenderID: m.Key, RecipientID: peer.Key, Hash: c.hash,
				Signature: c.sig, TTLSeconds: ttlSeconds,
			}
			regBatch[i] = muninn.RegisterChunkBatchEntry{
				ChunkIndex: i, SenderID: m.Key, RecipientID: peer.Key,
				Hash: c.hash, Signature: c.sig, PeerID: pid,
			}
		}
		log.Printf("sendOffline[%s]: registering chunks on peer %s...", msgID, pid)
		if err := m.muninnClient.RegisterChunks(m.ctx, msgID, muninn.RegisterChunkBatchRequest{Chunks: regBatch}); err != nil {
			log.Printf("register batch %s on %s: %v", msgID, pid, err)
			continue
		}
		log.Printf("sendOffline[%s]: sending chunk batch to %s...", msgID, pid)
		if err := m.rtcManager.SendChunkStoreBatch(pid, webrtc.ChunkStoreBatchRequest{Chunks: batch}); err != nil {
			log.Printf("send chunk batch %s to %s: %v", msgID, pid, err)
			continue
		}
		for i := range chunks {
			m.store.MarkChunkPlaced(msgID, i)
		}
		log.Printf("sendOffline[%s]: chunks sent to %s", msgID, pid)
	}
	log.Printf("sendOffline[%s]: done, connected=%d/%d", msgID, connected, len(storagePeers))
	return nil
}

func (m *Messenger) checkPendingMessages() {
	log.Printf("check pending messages for %s", m.Key)
	m.checkRecipientMessages(m.Key)

	groups, err := m.store.GetGroupChats()
	if err != nil {
		return
	}
	for _, g := range groups {
		m.checkRecipientMessages(g.UID)
	}
}

func (m *Messenger) checkRecipientMessages(recipientID string) {
	lastCheck := m.store.GetLastChunkCheck(recipientID)
	chunks, err := m.muninnClient.GetChunksByRecipient(m.ctx, recipientID, lastCheck-1)
	if err != nil {
		log.Printf("check %s: GetChunksByRecipient err: %v", recipientID, err)
		return
	}
	if len(chunks) == 0 {
		m.retryFailedChunks(recipientID)
		return
	}
	log.Printf("check %s: got %d chunk records", recipientID, len(chunks))
	newLastCheck := int64(0)
	if len(chunks) > 0 {
		byMsg := make(map[string][]muninn.ChunkRecord)
		for _, c := range chunks {
			if m.store.IsChunkFailed(c.FileID, c.ChunkIndex) {
				log.Printf("check %s: skip failed chunk %s/%d", recipientID, c.FileID, c.ChunkIndex)
				continue
			}
			if c.CreatedAt > newLastCheck {
				newLastCheck = c.CreatedAt
			}
			hasMsg, _ := m.store.FindMessageById(c.FileID)
			if hasMsg {
				log.Printf("collecting %s skipped", c.FileID)
				continue
			}
			log.Printf("check %s: chunk %s/%d confirmed=%v peer=%s", recipientID, c.FileID, c.ChunkIndex, c.Confirmed, c.PeerID)
			byMsg[c.FileID] = append(byMsg[c.FileID], c)
		}
		log.Printf("check %s: %d unique messages", recipientID, len(byMsg))
		for msgID, msgChunks := range byMsg {
			m.collectAndProcessMessage(msgID, msgChunks)
		}
	}

	if newLastCheck > lastCheck {
		m.store.SetLastChunkCheck(recipientID, newLastCheck)
	}
	m.retryFailedChunks(recipientID)
}

func (m *Messenger) retryFailedChunks(recipientID string) {
	failed, err := m.store.ListFailedChunks(recipientID)
	if err != nil || len(failed) == 0 {
		return
	}

	now := time.Now().Unix()
	seenFile := make(map[string]bool)

	for _, fc := range failed {
		if fc.CreatedAt+int64(fc.TTLSeconds) <= now {
			m.store.DeleteFailedChunk(fc.FileID, fc.ChunkIndex)
			continue
		}
		if seenFile[fc.FileID] {
			continue
		}
		seenFile[fc.FileID] = true

		records, err := m.muninnClient.GetChunksByFileID(m.ctx, fc.FileID)
		if err != nil || len(records) == 0 {
			continue
		}

		allOk := true
		for _, rec := range records {
			if !m.store.IsChunkFailed(rec.FileID, rec.ChunkIndex) {
				continue
			}
			data, ok := m.getChunkData(rec)
			if !ok {
				allOk = false
				continue
			}
			if rec.Hash != "" && chunk.RegisteredHash(data) != rec.Hash {
				allOk = false
				continue
			}
			m.store.DeleteFailedChunk(rec.FileID, rec.ChunkIndex)
		}
		if allOk {
			m.collectAndProcessMessage(fc.FileID, records)
		}
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
	hasMsg, _ := m.store.FindMessageById(msgID)
	if hasMsg {
		log.Printf("collecting %s skipped", msgID)
		return
	}
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
			ttl := rec.TTL
			if ttl <= 0 {
				ttl = 604800
			}
			m.store.StoreFailedChunk(rec.FileID, rec.ChunkIndex, rec.RecipientID, ttl)
			continue
		}
		m.store.DeleteFailedChunk(rec.FileID, rec.ChunkIndex)
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

	senderPeer := m.findPeerByKey(records[0].SenderID)
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

	encPrivate := m.encPrivate
	encPublic := m.encPublic
	recipientID := records[0].RecipientID
	if recipientID != "" && recipientID != m.Key {
		if gc, err := m.store.GetGroupChat(recipientID); err == nil {
			if priv, err := crypto.DecodeKey(gc.EncPrivate); err == nil {
				encPrivate = priv
			}
			if pub, err := crypto.DecodeKey(gc.EncPublic); err == nil {
				encPublic = pub
			}
		}
	}

	plaintext, err := chunk.AssembleAndDecrypt(envelopes, encPrivate, encPublic, senderSignKey)
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

	chatID := senderPeer.Key
	if recipientID != "" && recipientID != m.Key {
		chatID = recipientID
	}

	displayText := m.checkInviteText(payload.Text)

	for _, f := range payload.Files {
		m.processReceivedFile(f, records[0].SenderID)
	}

	decryptedMsg := ChatMessage{
		From:      getLogin(records[0].SenderID),
		Text:      displayText,
		Timestamp: payload.Timestamp,
		MsgID:     msgID,
		Files:     payload.Files,
	}

	jsonData, _ := json.Marshal(decryptedMsg)
	if err := m.store.SaveMessage(msgID, chatID, senderPeer.Key, chatID, jsonData, decryptedMsg.Timestamp); err != nil {
		log.Printf("save message: %v", err)
	}

	m.msgSubsMu.Lock()
	for _, sub := range m.msgSubs {
		select {
		case sub <- decryptedMsg:
		default:
		}
	}
	m.msgSubsMu.Unlock()

	log.Printf("message %s delivered from %s", msgID, records[0].SenderID)
}

func (m *Messenger) deleteChunksAndReturn(msgID string) {
	if err := m.store.DeleteChunks(msgID); err != nil {
		log.Printf("delete chunks for %s: %v", msgID, err)
	}
}

func (m *Messenger) MarkMessageRead(msgID string) error {
	payload := fmt.Sprintf("muninn/read/v1\n%s", msgID)
	sig := crypto.Sign(m.signPrivate, []byte(payload))
	req := muninn.ReadChunkRequest{
		RecipientID: m.Key,
		FileID:      msgID,
		Signature:    base64.StdEncoding.EncodeToString(sig),
	}
	return m.muninnClient.ReadChunk(m.ctx, req)
}

const invitePrefix = "__group_invite__:"

type groupInvitePayload struct {
	UID         string `json:"uid"`
	Name        string `json:"name"`
	EncPrivate  string `json:"enc_private"`
	EncPublic   string `json:"enc_public"`
	SignPrivate string `json:"sign_private"`
	SignPublic  string `json:"sign_public"`
}

func parseInvitePayload(text string) (*groupInvitePayload, error) {
	raw := strings.TrimPrefix(text, invitePrefix)
	var inv groupInvitePayload
	if err := json.Unmarshal([]byte(raw), &inv); err != nil {
		return nil, err
	}
	return &inv, nil
}

func (m *Messenger) checkInviteText(text string) string {
	if !strings.HasPrefix(text, invitePrefix) {
		return text
	}
	inv, err := parseInvitePayload(text)
	if err != nil {
		log.Printf("parse invite: %v", err)
		return text
	}
	existing, _ := m.store.GetGroupChat(inv.UID)
	if existing == nil {
		gc := &store.GroupChat{
			UID:         inv.UID,
			Name:        inv.Name,
			EncPrivate:  inv.EncPrivate,
			EncPublic:   inv.EncPublic,
			SignPrivate: inv.SignPrivate,
			SignPublic:  inv.SignPublic,
			CreatedAt:   time.Now(),
		}
		if err := m.store.CreateGroupChat(gc); err != nil {
			log.Printf("save group invite: %v", err)
		} else {
			log.Printf("joined group %s (%s) via invite", inv.Name, inv.UID)
		}
	}
	return fmt.Sprintf("You were invited to group chat: %s", inv.Name)
}

func (m *Messenger) CreateGroupChat(name string) (*store.GroupChat, error) {
	if existing, _ := m.store.GetGroupChatByName(name); existing != nil {
		return nil, fmt.Errorf("group chat %q already exists", name)
	}

	signPub, signPriv, err := crypto.GenerateSigningKey()
	if err != nil {
		return nil, fmt.Errorf("generate group signing key: %w", err)
	}
	encPriv, encPub, err := crypto.GenerateEncryptionKey()
	if err != nil {
		return nil, fmt.Errorf("generate group enc key: %w", err)
	}

	uid := uuid.New().String()
	gc := &store.GroupChat{
		UID:         uid,
		Name:        name,
		EncPrivate:  crypto.EncodeKey(encPriv),
		EncPublic:   crypto.EncodeKey(encPub),
		SignPrivate: crypto.EncodeKey(signPriv),
		SignPublic:  crypto.EncodeKey(signPub),
		CreatedAt:   time.Now(),
	}

	if err := m.store.CreateGroupChat(gc); err != nil {
		return nil, fmt.Errorf("save group chat: %w", err)
	}

	fake := true
	req := &muninn.RegisterRequest{
		ID:            uid,
		Login:         name,
		Addresses:     []string{""},
		EncryptionKey: gc.EncPublic,
		SignatureKey:  gc.SignPublic,
		Metadata: map[string]string{
			"username": name,
			"type":     "huginn-group",
		},
		TTLSeconds: 86400,
		PeerFlag:   muninn.PeerFlag("very_thick"),
		Fake:       &fake,
	}
	if err := m.muninnClient.Register(m.ctx, req); err != nil {
		log.Printf("register group peer %s (%s): %v", name, uid, err)
	}

	m.upsertPeer(uid, name + ":" + gc.SignPublic, name, gc.EncPublic, gc.SignPublic, time.Now(), true)

	log.Printf("group chat %s created with uid %s", name, uid)
	return gc, nil
}

func (m *Messenger) GetGroupChats() ([]store.GroupChat, error) {
	return m.store.GetGroupChats()
}

func (m *Messenger) registerGroupPeer(gc store.GroupChat) {
	fake := true
	req := &muninn.RegisterRequest{
		ID:            gc.UID,
		Login:         gc.Name,
		Addresses:     []string{""},
		EncryptionKey: gc.EncPublic,
		SignatureKey:  gc.SignPublic,
		Metadata: map[string]string{
			"username": gc.Name,
			"type":     "huginn-group",
		},
		TTLSeconds: 120,
		PeerFlag:   muninn.PeerFlag("very_thick"),
		Fake:       &fake,
	}
	if err := m.muninnClient.Register(m.ctx, req); err != nil {
		log.Printf("register group peer %s (%s): %v", gc.Name, gc.UID, err)
	}
}

func (m *Messenger) InviteToGroupChat(groupUID, memberID string) error {
	gc, err := m.store.GetGroupChat(groupUID)
	if err != nil {
		return fmt.Errorf("group not found: %w", err)
	}

	inv := groupInvitePayload{
		UID:         gc.UID,
		Name:        gc.Name,
		EncPrivate:  gc.EncPrivate,
		EncPublic:   gc.EncPublic,
		SignPrivate: gc.SignPrivate,
		SignPublic:  gc.SignPublic,
	}
	invData, _ := json.Marshal(inv)
	inviteText := invitePrefix + string(invData)

	return m.SendMessage(memberID, inviteText, nil, 604800)
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

	senderPeer := m.findPeerByKey(senderID)
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

	m.notifyFileReady(FileReadyEvent{
		FileID:   f.FileID,
		FilePath: fp,
		Filename: f.Filename,
		SenderID: senderID,
	})
}

func (m *Messenger) requestMissingChunk(fileID string, chunkIndex int, senderID string) {
	targets := []string{}	
	p := m.findPeerByKey(senderID)
	if p == nil {
		return
	}
	targets = p.IDS
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

	log.Printf("send chunk get to %s", rec.PeerID)
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

func (m *Messenger) GetMessagesDesc(peerID string, limit, offset int) []ChatMessage {
	dataList, err := m.store.GetMessagesDesc(peerID, limit, offset)
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

func (m *Messenger) SubscribeFileReady() chan FileReadyEvent {
	ch := make(chan FileReadyEvent, 50)
	m.fileReadySubsMu.Lock()
	m.fileReadySubs = append(m.fileReadySubs, ch)
	m.fileReadySubsMu.Unlock()
	return ch
}

func (m *Messenger) UnsubscribeFileReady(ch chan FileReadyEvent) {
	m.fileReadySubsMu.Lock()
	for i, c := range m.fileReadySubs {
		if c == ch {
			m.fileReadySubs = append(m.fileReadySubs[:i], m.fileReadySubs[i+1:]...)
			close(ch)
			break
		}
	}
	m.fileReadySubsMu.Unlock()
}

func (m *Messenger) notifyFileReady(evt FileReadyEvent) {
	m.fileReadySubsMu.Lock()
	for _, sub := range m.fileReadySubs {
		select {
		case sub <- evt:
		default:
		}
	}
	m.fileReadySubsMu.Unlock()
}

func (m *Messenger) DownloadsDir() string {
	return m.downloadsDir
}

func (m *Messenger) SetDownloadsDir(dir string) {
	m.downloadsDir = dir
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

func (m *Messenger) ListFailedChunks() ([]store.FailedChunk, error) {
	return m.store.ListFailedChunks(m.Key)
}

func (m *Messenger) IsChunkFailed(fileID string, chunkIndex int) bool {
	return m.store.IsChunkFailed(fileID, chunkIndex)
}

func (m *Messenger) Config() *config.Config {
	if m.appConfig == nil {
		return &config.Config{}
	}
	return m.appConfig
}

func (m *Messenger) SaveConfig(cfg *config.Config) error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	m.appConfig = cfg
	return m.store.SaveAppConfig(cfg)
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

func getLogin(key string) string {
	return strings.Split(key, ":")[0]
}

func (m *Messenger) PeerSlice() []muninn.Peer {
	s := make([]muninn.Peer, 0, len(m.peersMap))
	for _, v := range m.peersMap {
		s = append(s, v)
	}
	return s
}