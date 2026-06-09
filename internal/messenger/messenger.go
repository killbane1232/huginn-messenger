package messenger

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/chunk"
	"github.com/killbane1232/huginn-messenger/internal/crypto"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
	"github.com/killbane1232/huginn-messenger/internal/p2p"
	"github.com/killbane1232/huginn-messenger/internal/webrtc"
	"github.com/google/uuid"
	pion "github.com/pion/webrtc/v3"
)

type ChatMessage struct {
	From      string    `json:"from"`
	Text      string    `json:"text"`
	Timestamp time.Time `json:"timestamp"`
	MsgID     string    `json:"msg_id,omitempty"`
}

type Messenger struct {
	ID       string
	Username string
	MsgAddr  string

	signPublic  ed25519.PublicKey
	signPrivate ed25519.PrivateKey
	encPrivate  []byte
	encPublic   []byte

	muninnClient *muninn.Client
	p2pSrv       *p2p.Server
	rtcManager   *webrtc.Manager
	rtcMsgChan   chan webrtc.ChatMessage

	peers    []muninn.Peer
	messages map[string][]ChatMessage
	mu       sync.RWMutex

	peerSubs   map[string]chan struct{}
	subsMu     sync.Mutex
	msgSubs    []chan ChatMessage
	msgSubsMu  sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
}

func New(username string, msgPort int, muninnClient *muninn.Client) (*Messenger, error) {
	signPub, signPriv, err := crypto.GenerateSigningKey()
	if err != nil {
		return nil, fmt.Errorf("generate signing key: %w", err)
	}
	encPriv, encPub, err := crypto.GenerateEncryptionKey()
	if err != nil {
		return nil, fmt.Errorf("generate encryption key: %w", err)
	}

	msgAddr := fmt.Sprintf("%s:%d", getLocalIP(), msgPort)
	rtcMsgChan := make(chan webrtc.ChatMessage, 100)

	p2pSrv := p2p.NewServer(msgPort)
	rtcMgr := webrtc.NewManager(username, rtcMsgChan)

	ctx, cancel := context.WithCancel(context.Background())

	m := &Messenger{
		ID:       username,
		Username: username,
		MsgAddr:  msgAddr,

		signPublic:  signPub,
		signPrivate: signPriv,
		encPrivate:  encPriv,
		encPublic:   encPub,

		muninnClient: muninnClient,
		p2pSrv:       p2pSrv,
		rtcManager:   rtcMgr,
		rtcMsgChan:   rtcMsgChan,

		messages: make(map[string][]ChatMessage),
		peerSubs: make(map[string]chan struct{}),
		ctx:      ctx,
		cancel:   cancel,
	}

	go p2pSrv.Start()
	go m.heartbeatLoop()
	go m.peerRefreshLoop()
	go m.processRTCMessages()

	return m, nil
}

func getLocalIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}

func (m *Messenger) processRTCMessages() {
	for {
		select {
		case msg := <-m.rtcMsgChan:
			cm := ChatMessage{
				From:      msg.From,
				Text:      msg.Text,
				Timestamp: time.Now(),
			}
			m.mu.Lock()
			m.messages[msg.From] = append(m.messages[msg.From], cm)
			m.mu.Unlock()
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
	for _, peer := range m.peers {
		if peer.ID == m.ID || len(peer.Addresses) == 0 {
			continue
		}
		sigs, err := p2p.PollSignals(m.ctx, peer.Addresses[0], m.ID)
		if err != nil {
			continue
		}
		for _, sig := range sigs {
			switch sig.Type {
			case "offer":
				var offer pion.SessionDescription
				if err := jsonUnmarshal(sig.Data, &offer); err != nil {
					continue
				}
				answer, err := m.rtcManager.HandleOffer(peer.ID, offer)
				if err != nil {
					log.Printf("handle offer from %s: %v", peer.ID, err)
					continue
				}
				ansData := jsonMarshal(answer)
				sigMsg := p2p.SignalMsg{From: m.ID, Type: "answer", Data: ansData}
				p2p.SendSignal(m.ctx, peer.Addresses[0], sigMsg)
			case "answer":
				var answer pion.SessionDescription
				if err := jsonUnmarshal(sig.Data, &answer); err != nil {
					continue
				}
				if err := m.rtcManager.SetRemoteDescription(peer.ID, answer); err != nil {
					log.Printf("set remote desc from %s: %v", peer.ID, err)
				}
			}
		}
	}
}

func jsonMarshal(v any) string {
	data, _ := jsonMarshalRaw(v)
	return string(data)
}

func jsonMarshalRaw(v any) ([]byte, error) {
	return jsonMarshalImpl(v)
}

func jsonUnmarshal(data string, v any) error {
	return jsonUnmarshalImpl([]byte(data), v)
}

func jsonMarshalImpl(v any) ([]byte, error) {
	return json.Marshal(v)
}

func jsonUnmarshalImpl(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func (m *Messenger) heartbeatLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := m.muninnClient.Heartbeat(m.ctx, m.ID, 120); err != nil {
				log.Printf("heartbeat error: %v", err)
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
			if err := m.RefreshPeers(); err != nil {
				log.Printf("refresh peers error: %v", err)
			}
			m.processPendingSignals()
			m.checkPendingMessages()
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Messenger) Register() error {
	req := &muninn.RegisterRequest{
		ID:        m.ID,
		Keys:      []muninn.Key{{Login: m.Username, Signature: "huginn-v1"}},
		Addresses: []string{m.MsgAddr},
		EncryptionKey: crypto.EncodeKey(m.encPublic),
		SignatureKey:  crypto.EncodeKey(m.signPublic),
		Metadata: map[string]string{
			"username": m.Username,
			"type":     "huginn-messenger",
		},
		TTLSeconds: 120,
	}
	return m.muninnClient.Register(m.ctx, req)
}

func (m *Messenger) RefreshPeers() error {
	peers, err := m.muninnClient.List(m.ctx)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.peers = peers
	m.mu.Unlock()
	m.subsMu.Lock()
	for _, ch := range m.peerSubs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	m.subsMu.Unlock()
	return nil
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

func (m *Messenger) findPeerByID(id string) *muninn.Peer {
	for _, p := range m.peers {
		if p.ID == id {
			return &p
		}
	}
	return nil
}

func (m *Messenger) SendMessage(toPeerID, text string) error {
	msgID := uuid.New().String()
	peer := m.findPeerByID(toPeerID)
	if peer == nil {
		return fmt.Errorf("peer %s not found", toPeerID)
	}

	if m.rtcManager.IsConnected(toPeerID) {
		if err := m.rtcManager.SendMessage(toPeerID, text); err != nil {
			return fmt.Errorf("webrtc send: %w", err)
		}
		cm := ChatMessage{From: m.Username, Text: text, Timestamp: time.Now(), MsgID: msgID}
		m.mu.Lock()
		m.messages[toPeerID] = append(m.messages[toPeerID], cm)
		m.mu.Unlock()
		return nil
	}

	if len(peer.Addresses) > 0 {
		offer, err := m.rtcManager.CreateOffer(toPeerID)
		if err != nil {
			return fmt.Errorf("create offer: %w", err)
		}
		offerData, _ := jsonMarshalRaw(offer)
		sig := p2p.SignalMsg{From: m.ID, Type: "offer", Data: string(offerData)}
		if err := p2p.SendSignal(m.ctx, peer.Addresses[0], sig); err == nil {
			log.Printf("webrtc offer sent to %s, will retry send when connected", toPeerID)
		}
	}

	return m.sendOffline(msgID, text, toPeerID, peer)
}

func (m *Messenger) sendOffline(msgID, text, toPeerID string, peer *muninn.Peer) error {
	log.Printf("sending offline message %s to %s via chunks", msgID, toPeerID)

	bestPeers, err := m.muninnClient.GetBestPeers(m.ctx, 5)
	if err != nil {
		return fmt.Errorf("get best peers: %w", err)
	}

	var storagePeers []muninn.Peer
	for _, p := range bestPeers {
		if p.ID != m.ID && p.ID != toPeerID && len(p.Addresses) > 0 {
			storagePeers = append(storagePeers, p)
		}
	}
	if len(storagePeers) == 0 {
		storagePeers = []muninn.Peer{*m.findPeerByID(m.ID)}
	}

	recipientPubKey, err := crypto.DecodeKey(peer.EncryptionKey)
	if err != nil {
		return fmt.Errorf("decode recipient enc key: %w", err)
	}

	aesKey, err := crypto.DeriveSharedKey(m.encPrivate, recipientPubKey)
	if err != nil {
		return fmt.Errorf("derive key: %w", err)
	}

	envelopes, err := chunk.SplitAndEncrypt(msgID, m.ID, toPeerID, []byte(text), aesKey, m.signPrivate)
	if err != nil {
		return fmt.Errorf("split encrypt: %w", err)
	}

	for i, env := range envelopes {
		sp := storagePeers[i%len(storagePeers)]
		envData, err := chunk.MarshalEnvelope(env)
		if err != nil {
			return fmt.Errorf("marshal env %d: %w", i, err)
		}
		ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
		if err := p2p.StoreChunk(ctx, sp.Addresses[0], msgID, i, envData); err != nil {
			cancel()
			return fmt.Errorf("store chunk %d on %s: %w", i, sp.ID, err)
		}
		cancel()

		chunkHash := chunk.RegisteredHash(envData)
		expectedPayload := fmt.Sprintf("muninn/expected/v1\n%s\n%d\n%s", msgID, i, chunkHash)
		sig := crypto.Sign(m.signPrivate, []byte(expectedPayload))
		chunkReq := muninn.RegisterChunkRequest{
			SenderID:    m.ID,
			RecipientID: toPeerID,
			Hash:        chunkHash,
			Signature:   crypto.EncodeKey(sig),
			PeerID:      sp.ID,
		}
		if err := m.muninnClient.RegisterChunk(m.ctx, msgID, i, chunkReq); err != nil {
			log.Printf("register chunk %d warning: %v", i, err)
		}
	}

	cm := ChatMessage{From: m.Username, Text: text, Timestamp: time.Now(), MsgID: msgID}
	m.mu.Lock()
	m.messages[toPeerID] = append(m.messages[toPeerID], cm)
	m.mu.Unlock()
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

func (m *Messenger) collectAndProcessMessage(msgID string, records []muninn.ChunkRecord) {
	log.Printf("collecting message %s (%d chunks)", msgID, len(records))

	var envelopes []chunk.Envelope
	for _, rec := range records {
		holderAddr := m.findPeerAddress(rec.PeerID)
		if holderAddr == "" {
			log.Printf("unknown holder %s for chunk %s/%d", rec.PeerID, rec.FileID, rec.ChunkIndex)
			continue
		}
		ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
		data, err := p2p.GetChunk(ctx, holderAddr, rec.FileID, rec.ChunkIndex)
		cancel()
		if err != nil {
			log.Printf("failed to get chunk %s/%d from %s: %v", rec.FileID, rec.ChunkIndex, rec.PeerID, err)
			continue
		}
		env, err := chunk.UnmarshalEnvelope(data)
		if err != nil {
			log.Printf("invalid envelope for chunk %s/%d: %v", rec.FileID, rec.ChunkIndex, err)
			continue
		}
		envelopes = append(envelopes, env)
	}

	if len(envelopes) != len(records) {
		log.Printf("incomplete message %s: got %d/%d chunks", msgID, len(envelopes), len(records))
		return
	}

	senderPeer := m.findPeerByID(records[0].SenderID)
	if senderPeer == nil {
		log.Printf("sender %s not found for message %s", records[0].SenderID, msgID)
		return
	}

	senderEncKey, err := crypto.DecodeKey(senderPeer.EncryptionKey)
	if err != nil {
		log.Printf("decode sender enc key: %v", err)
		return
	}
	aesKey, err := crypto.DeriveSharedKey(m.encPrivate, senderEncKey)
	if err != nil {
		log.Printf("derive key: %v", err)
		return
	}
	senderSignKey, err := crypto.DecodeKey(senderPeer.SignatureKey)
	if err != nil {
		log.Printf("decode sender sign key: %v", err)
		return
	}

	plaintext, err := chunk.AssembleAndDecrypt(envelopes, aesKey, senderSignKey)
	if err != nil {
		log.Printf("assemble/decrypt message %s: %v", msgID, err)
		return
	}

	decryptedMsg := ChatMessage{
		From:      records[0].SenderID,
		Text:      string(plaintext),
		Timestamp: time.Now(),
		MsgID:     msgID,
	}

	m.mu.Lock()
	m.messages[records[0].SenderID] = append(m.messages[records[0].SenderID], decryptedMsg)
	m.mu.Unlock()

	m.msgSubsMu.Lock()
	for _, sub := range m.msgSubs {
		select {
		case sub <- decryptedMsg:
		default:
		}
	}
	m.msgSubsMu.Unlock()

	for _, rec := range records {
		holderAddr := m.findPeerAddress(rec.PeerID)
		if holderAddr == "" {
			continue
		}
		ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
		data, err := p2p.GetChunk(ctx, holderAddr, rec.FileID, rec.ChunkIndex)
		cancel()
		if err != nil {
			continue
		}
		actualHash := chunk.RegisteredHash(data)
		reportedPayload := fmt.Sprintf("muninn/reported/v1\n%s\n%d\n%s\n%s",
			rec.FileID, rec.ChunkIndex, actualHash, rec.PeerID)
		reportReq := muninn.ChunkReportRequest{
			ReporterID: m.ID,
			FileID:     rec.FileID,
			ChunkIndex: rec.ChunkIndex,
			Hash:       actualHash,
			Signature:  crypto.EncodeKey(crypto.Sign(m.signPrivate, []byte(reportedPayload))),
		}
		if err := m.muninnClient.ReportChunk(m.ctx, rec.PeerID, reportReq); err != nil {
			log.Printf("report chunk %s/%d: %v", rec.FileID, rec.ChunkIndex, err)
		}
		if actualHash == rec.Hash {
			ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
			p2p.DeleteChunk(ctx, holderAddr, rec.FileID, rec.ChunkIndex)
			cancel()
		}
	}

	log.Printf("message %s delivered from %s", msgID, records[0].SenderID)
}

func (m *Messenger) findPeerAddress(peerID string) string {
	for _, p := range m.peers {
		if p.ID == peerID && len(p.Addresses) > 0 {
			return p.Addresses[0]
		}
	}
	peer, err := m.muninnClient.Get(m.ctx, peerID)
	if err == nil && len(peer.Addresses) > 0 {
		return peer.Addresses[0]
	}
	return ""
}

func (m *Messenger) GetMessages(peerID string) []ChatMessage {
	m.mu.RLock()
	defer m.mu.RUnlock()
	msgs := m.messages[peerID]
	result := make([]ChatMessage, len(msgs))
	copy(result, msgs)
	return result
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

func (m *Messenger) Shutdown() {
	m.cancel()
	delCtx, delCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer delCancel()
	m.muninnClient.Delete(delCtx, m.ID)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	m.p2pSrv.Shutdown(ctx)
	m.rtcManager.CloseAll()
}
