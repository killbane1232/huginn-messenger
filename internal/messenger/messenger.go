package messenger

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/killbane1232/huginn-messenger/internal/chunk"
	"github.com/killbane1232/huginn-messenger/internal/config"
	"github.com/killbane1232/huginn-messenger/internal/crypto"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
	"github.com/killbane1232/huginn-messenger/internal/store"
	"github.com/killbane1232/huginn-messenger/internal/webrtc"

	//"runtime/debug"
	pion "github.com/pion/webrtc/v4"
)

type ChatMessage struct {
	From      string     `json:"from"`
	ChatID    string     `json:"chat_id,omitempty"`
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
	FilePath      string `json:"file_path,omitempty"`
	SourcePeerID  string `json:"source_peer_id,omitempty"`
}

type pendingFileDownload struct {
	fileMeta        FileMeta
	senderID        string
	preferredPeerID string
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

const (
	peerHeartbeatInterval = 15 * time.Second
	peerTTLSeconds        = 120
	groupPeerTTLSeconds   = 24 * 60 * 60
	groupRefreshInterval  = 12 * time.Hour
	fileDownloadTTL       = 7 * 24 * time.Hour
)

type messengerOpts struct {
	iceServers []pion.ICEServer
	iceSet     bool
	peerFlag   muninn.PeerFlag
	turnAddr   string
	turnUser   string
	turnPass   string
	peerID     string
	pollSignal bool
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

func WithPoll() MessengerOption {
	return func(o *messengerOpts) {
		o.pollSignal = true
	}
}

type Messenger struct {
	ID       string
	Key      string
	Username string

	signPublic  ed25519.PublicKey
	signPrivate ed25519.PrivateKey
	encPrivate  []byte
	encPublic   []byte

	muninnClient *muninn.Client
	wsClient     *muninn.WSClient
	rtcManager   *webrtc.Manager
	rtcMsgChan   chan webrtc.ChatMessage
	signalChan   chan muninn.Signal

	store *store.SQLiteStore

	peersMap        map[string]muninn.Peer
	peers           []muninn.Peer
	peersConnecting map[string]struct{}
	mu              sync.RWMutex

	peerSubs        map[string]chan struct{}
	subsMu          sync.Mutex
	msgSubs         []chan ChatMessage
	msgSubsMu       sync.Mutex
	fileReadySubs   []chan FileReadyEvent
	fileReadySubsMu sync.Mutex
	registeredMap   map[string]bool
	registeredMu    sync.Mutex

	ctx          context.Context
	cancel       context.CancelFunc
	async        *asyncPool
	backgroundWG sync.WaitGroup
	shutdownOnce sync.Once
	peerFlag     muninn.PeerFlag
	downloadsDir string

	pendingFileDownloads map[string]*pendingFileDownload
	pendingMu            sync.Mutex

	processingMsg map[string]bool
	processingMu  sync.Mutex

	appConfig          *config.Config
	reloginMu          sync.Mutex
	reloginTransfer    *reloginTransferState
	stateSyncMu        sync.Mutex
	stateSyncRequested map[string]bool
	stateSyncIncoming  map[string]*stateSyncIncomingTransfer
	stateSyncOutgoing  map[string]*stateSyncOutgoingTransfer
	replicaPeers       map[string]bool
	pollSignal         bool
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
	key := oldUsername + ":" + crypto.EncodeKey(signPub)
	log.Printf("messenger: final key=%q", key)
	m := &Messenger{
		ID:       peerID,
		Key:      key,
		Username: oldUsername,

		peersMap:    make(map[string]muninn.Peer),
		signPublic:  signPub,
		signPrivate: signPriv,
		encPrivate:  encPriv,
		encPublic:   encPub,

		muninnClient:    muninnClient,
		rtcMsgChan:      rtcMsgChan,
		signalChan:      signalChan,
		store:           st,
		peersConnecting: make(map[string]struct{}),
		peerSubs:        make(map[string]chan struct{}),
		ctx:             ctx,
		cancel:          cancel,
		peerFlag:        o.peerFlag,
		downloadsDir:    downloadsDir,
		registeredMap:   make(map[string]bool),

		pendingFileDownloads: make(map[string]*pendingFileDownload),
		processingMsg:        make(map[string]bool),
		appConfig:            appCfg,
		stateSyncRequested:   make(map[string]bool),
		stateSyncIncoming:    make(map[string]*stateSyncIncomingTransfer),
		stateSyncOutgoing:    make(map[string]*stateSyncOutgoingTransfer),
		replicaPeers:         make(map[string]bool),
		pollSignal:           o.pollSignal,
	}
	m.async = newAsyncPool(ctx, asyncWorkerCount, asyncQueueSize)

	if len(o.iceServers) < 1 {
		o.iceServers = []pion.ICEServer{
			{
				URLs: []string{
					"turn:stun.evil-bread.ru:3478",
					"turn:turn.evil-bread.ru:3478?transport=udp",
					"turn:turn.evil-bread.ru:3478?transport=tcp",
					"turns:turn.evil-bread.ru:5349?transport=tcp",
				},
				Username:   "turnuser",
				Credential: "turnpass",
			},
		}
	}
	m.rtcManager = webrtc.NewManager(peerID, rtcMsgChan, m.handleChunkStore, m.handleChunkGet,
		m.handleReloginRequest, m.handleReloginResponse, m.handleReloginChunk,
		m.handleStateSyncRequest, m.handleStateSyncManifest, m.handleStateSyncChunk, m.handleStateSyncAck,
		m.handlePeerConnected, m.handlePeerDisconnected, o.iceServers, m.async.submit)

	m.wsClient = muninn.NewWSClient(muninnClient.BaseURL(), peerID)
	m.wsClient.SetOnSignal(func(sig muninn.Signal) {
		select {
		case m.signalChan <- sig:
		default:
			log.Printf("dropping ws signal from %s (channel full)", sig.From)
		}
	})
	m.wsClient.SetOnDisconnect(func() {
		log.Printf("[ws] connection to muninn lost, reconnect scheduled")
	})
	m.startBackground(m.rtcReconnectLoop)
	storedPeers, _ := st.GetStoredPeers()
	for _, sp := range storedPeers {
		munPeer := sp.ToMuninnPeer()
		m.peersMap[munPeer.Key()] = munPeer
		m.peers = append(m.peers, munPeer)
	}

	m.startBackground(m.heartbeatLoop)
	m.startBackground(m.groupRefreshLoop)
	m.startBackground(m.peerRefreshLoop)
	m.startBackground(m.signalPollLoop)
	m.startBackground(m.processRTCMessages)
	m.startBackground(m.pendingChunkLoop)
	m.startBackground(m.fileDownloadLoop)
	m.startBackground(m.chunkCleanupLoop)
	m.async.trySubmit(m.resumeReplicatedFileDownloads)

	return m, nil
}

func (m *Messenger) startBackground(fn func()) {
	m.backgroundWG.Add(1)
	go func() {
		defer m.backgroundWG.Done()
		fn()
	}()
}

func (m *Messenger) heartbeatLoop() {
	ticker := time.NewTicker(peerHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if !m.isCurrentPeerRegistered() {
				if err := m.Register(); err != nil {
					log.Printf("register peer error: %v", err)
				}
				continue
			}
			if err := m.muninnClient.Heartbeat(m.ctx, m.ID, peerTTLSeconds); err != nil {
				if errors.Is(err, muninn.ErrPeerNotFound) {
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

func (m *Messenger) isCurrentPeerRegistered() bool {
	key := m.Username + ":" + crypto.EncodeKey(m.signPublic)
	m.registeredMu.Lock()
	defer m.registeredMu.Unlock()
	return m.registeredMap[key]
}

func (m *Messenger) Register() error {
	sign := crypto.EncodeKey(m.signPublic)
	key := m.Username + ":" + sign
	m.registeredMu.Lock()
	var err error
	if m.registeredMap[key] == true {
		req := &muninn.RefreshRequest{
			ID:           m.ID,
			Login:        m.Username,
			SignatureKey: sign,
		}
		err = m.muninnClient.Refresh(m.ctx, req)
		if err == nil {
			m.registeredMu.Unlock()
			m.registerStoredGroupPeers()
			m.scheduleReplicaConnections()
			return nil
		}
		if !errors.Is(err, muninn.ErrPeerNotFound) {
			m.registeredMu.Unlock()
			return err
		}
		delete(m.registeredMap, key)
	}
	req := &muninn.RegisterRequest{
		ID:            m.ID,
		Login:         m.Username,
		EncryptionKey: crypto.EncodeKey(m.encPublic),
		SignatureKey:  sign,
		TTLSeconds:    peerTTLSeconds,
		PeerFlag:      m.peerFlag,
	}
	err = m.muninnClient.Register(m.ctx, req)
	if err == nil {
		m.registeredMap[key] = true
	}
	m.registeredMu.Unlock()
	if err != nil {
		return err
	}
	m.registerStoredGroupPeers()
	m.scheduleReplicaConnections()
	return nil
}

func (m *Messenger) scheduleReplicaConnections() {
	if m.async != nil {
		m.async.trySubmit(m.refreshReplicaConnections)
	}
}

func (m *Messenger) processReceivedFile(f FileMeta, senderID string) {
	m.processReceivedFileFromPeer(f, senderID, "")
}

func (m *Messenger) processReceivedFileFromPeer(f FileMeta, senderID, preferredPeerID string) {
	if !m.tryProcessMsg("file:" + f.FileID) {
		return
	}
	defer m.releaseProcessMsg("file:" + f.FileID)
	state, proceed := m.prepareFileDownload(f, senderID)
	if !proceed {
		return
	}
	chunkMap, err := m.store.ListChunks(f.FileID)
	if err != nil {
		log.Printf("list chunks for file %s: %v", f.FileID, err)
		return
	}

	if len(chunkMap) < f.TotalChunks {
		if state.StoppedAt != nil || !time.Now().Before(state.ExpiresAt) {
			if state.StoppedAt == nil {
				if err := m.store.MarkFileDownloadStopped(f.FileID, time.Now()); err != nil {
					log.Printf("stop expired file download %s: %v", f.FileID, err)
				}
				log.Printf("file %s download expired after %s; stopping with %d/%d chunks", f.FileID, fileDownloadTTL, len(chunkMap), f.TotalChunks)
			}
			m.removePendingFileDownload(f.FileID)
			return
		}
		log.Printf("file %s: have %d/%d chunks, requesting missing from peers", f.FileID, len(chunkMap), f.TotalChunks)
		m.pendingMu.Lock()
		m.pendingFileDownloads[f.FileID] = &pendingFileDownload{
			fileMeta:        f,
			senderID:        senderID,
			preferredPeerID: preferredPeerID,
		}
		m.pendingMu.Unlock()
		for i := 0; i < f.TotalChunks; i++ {
			if _, ok := chunkMap[i]; !ok {
				m.requestMissingChunkFromPeer(f.FileID, i, senderID, preferredPeerID)
			}
		}
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

	senderSignKey := m.signPublic
	if senderID != m.Key {
		senderPeer := m.findPeerByKey(senderID)
		if senderPeer == nil {
			log.Printf("sender %s not found for file %s", senderID, f.FileID)
			return
		}

		var err error
		senderSignKey, err = crypto.DecodeKey(senderPeer.SignatureKey)
		if err != nil {
			log.Printf("decode sender sign key for file %s: %v", f.FileID, err)
			return
		}
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
	if err := os.WriteFile(fp, plaintext, 0644); err != nil {
		log.Printf("save file %s: %v", fp, err)
		return
	}
	log.Printf("file saved: %s (%d bytes)", fp, len(plaintext))

	if err := m.store.MarkFileDownloadCompleted(f.FileID, fp, time.Now()); err != nil {
		log.Printf("persist completed file %s: %v", f.FileID, err)
	}
	m.removePendingFileDownload(f.FileID)

	m.notifyFileReady(FileReadyEvent{
		FileID:   f.FileID,
		FilePath: fp,
		Filename: f.Filename,
		SenderID: senderID,
	})
}

func (m *Messenger) prepareFileDownload(f FileMeta, senderID string) (store.FileDownloadState, bool) {
	state, err := m.store.EnsureFileDownload(f.FileID, time.Now(), fileDownloadTTL)
	if err != nil {
		log.Printf("track file download %s: %v", f.FileID, err)
		return store.FileDownloadState{}, false
	}

	if state.CompletedAt != nil {
		if fileMatchesMeta(state.LocalPath, f.FileHash, false) {
			m.removePendingFileDownload(f.FileID)
			m.notifyFileReady(FileReadyEvent{
				FileID:   f.FileID,
				FilePath: state.LocalPath,
				Filename: f.Filename,
				SenderID: senderID,
			})
			return state, false
		}
		if err := m.store.ResetFileDownloadCompletion(f.FileID); err != nil {
			log.Printf("reset missing completed file %s: %v", f.FileID, err)
			return state, false
		}
		state.CompletedAt = nil
		state.LocalPath = ""
	}

	outputName := f.Filename
	if outputName == "" {
		outputName = f.FileID
	}
	candidate := filepath.Join(m.downloadsDir, outputName)
	if fileMatchesMeta(candidate, f.FileHash, true) {
		if err := m.store.MarkFileDownloadCompleted(f.FileID, candidate, time.Now()); err != nil {
			log.Printf("persist existing completed file %s: %v", f.FileID, err)
			return state, false
		}
		state.CompletedAt = new(time.Time)
		*state.CompletedAt = time.Now()
		state.LocalPath = candidate
		m.removePendingFileDownload(f.FileID)
		m.notifyFileReady(FileReadyEvent{
			FileID:   f.FileID,
			FilePath: candidate,
			Filename: f.Filename,
			SenderID: senderID,
		})
		return state, false
	}

	if state.StoppedAt != nil {
		m.removePendingFileDownload(f.FileID)
		return state, false
	}
	return state, true
}

func fileMatchesMeta(path, expectedHash string, requireHash bool) bool {
	if path == "" || (requireHash && expectedHash == "") {
		return false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	if expectedHash == "" {
		return true
	}
	digest := sha256.Sum256(data)
	return base64.StdEncoding.EncodeToString(digest[:]) == expectedHash
}

func (m *Messenger) removePendingFileDownload(fileID string) {
	m.pendingMu.Lock()
	delete(m.pendingFileDownloads, fileID)
	m.pendingMu.Unlock()
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
		m.processReceivedFileFromPeer(pd.fileMeta, pd.senderID, pd.preferredPeerID)
	}
}

func (m *Messenger) fileDownloadLoop() {
	ticker := time.NewTicker(15 * time.Second)
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
	m.shutdownOnce.Do(func() {
		m.cancel()
		delCtx, delCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer delCancel()
		m.muninnClient.Delete(delCtx, m.ID)
		m.wsClient.Close()
		m.rtcManager.CloseAll()
		m.backgroundWG.Wait()
		m.async.wait()
		m.store.Close()
	})
}
