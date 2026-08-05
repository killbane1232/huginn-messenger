package webrtc

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	pion "github.com/pion/webrtc/v4"
)

type ChatMessage struct {
	From      string    `json:"from"`
	Text      string    `json:"text"`
	Timestamp time.Time `json:"timestamp,omitempty"`
	MsgID     string    `json:"msg_id,omitempty"`
}

type ChunkStoreRequest struct {
	FileID      string `json:"file_id"`
	ChunkIndex  int    `json:"chunk_index"`
	Data        []byte `json:"data"`
	SenderID    string `json:"sender_id,omitempty"`
	RecipientID string `json:"recipient_id,omitempty"`
	Hash        string `json:"hash,omitempty"`
	Signature   string `json:"signature,omitempty"`
	TTLSeconds  int    `json:"ttl_seconds,omitempty"`
}

type ChunkStoreBatchRequest struct {
	Chunks []ChunkStoreRequest `json:"chunks"`
}

type ChunkGetRequest struct {
	FileID     string `json:"file_id"`
	ChunkIndex int    `json:"chunk_index"`
}

type ReloginRequest struct {
	Signature string `json:"signature"`
}

type ReloginResponse struct {
	KeysData string `json:"keys_data"`
}

type envelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

const (
	MsgTypeChat            = "chat"
	MsgTypeChunkStore      = "chunk_store"
	MsgTypeChunkStoreBatch = "chunk_store_batch"
	MsgTypeChunkGet        = "chunk_get"
	MsgTypeChunkData       = "chunk_data"
	MsgTypeReloginRequest  = "relogin_request"
	MsgTypeReloginResponse = "relogin_response"
)

type Manager struct {
	mu          sync.RWMutex
	connections map[string]*pion.PeerConnection
	dataChans   map[string]*pion.DataChannel
	chatMsgChan chan ChatMessage
	chunkStore  func(peerID string, req ChunkStoreRequest)
	chunkGet    func(peerID string, req ChunkGetRequest) ([]byte, bool)
	reloginReq  func(peerID string, req ReloginRequest)
	reloginResp func(peerID string, resp ReloginResponse)
	submit      func(func()) bool
	localID     string

	config pion.Configuration
}

func NewManager(localID string, chatMsgChan chan ChatMessage,
	chunkStore func(peerID string, req ChunkStoreRequest),
	chunkGet func(peerID string, req ChunkGetRequest) ([]byte, bool),
	reloginReq func(peerID string, req ReloginRequest),
	reloginResp func(peerID string, resp ReloginResponse),
	iceServers []pion.ICEServer,
	submit func(func()) bool) *Manager {

	return &Manager{
		connections: make(map[string]*pion.PeerConnection),
		dataChans:   make(map[string]*pion.DataChannel),
		chatMsgChan: chatMsgChan,
		chunkStore:  chunkStore,
		chunkGet:    chunkGet,
		reloginReq:  reloginReq,
		reloginResp: reloginResp,
		submit:      submit,
		localID:     localID,
		config: pion.Configuration{
			ICEServers: iceServers,
		},
	}
}

func (m *Manager) submitAsync(job func()) {
	if m.submit != nil {
		m.submit(job)
		return
	}
	job()
}

func (m *Manager) onMessage(remoteID string, msg pion.DataChannelMessage) {
	var env envelope
	if err := json.Unmarshal(msg.Data, &env); err != nil {
		return
	}
	switch env.Type {
	case MsgTypeChat:
		var chat ChatMessage
		if json.Unmarshal(env.Data, &chat) == nil {
			chat.From = remoteID
			select {
			case m.chatMsgChan <- chat:
			default:
			}
		}
	case MsgTypeChunkStore:
		if m.chunkStore == nil {
			return
		}
		var req ChunkStoreRequest
		if json.Unmarshal(env.Data, &req) == nil {
			m.submitAsync(func() { m.chunkStore(remoteID, req) })
		}
	case MsgTypeChunkStoreBatch:
		if m.chunkStore == nil {
			return
		}
		var batch ChunkStoreBatchRequest
		if json.Unmarshal(env.Data, &batch) == nil {
			for _, req := range batch.Chunks {
				req := req
				m.submitAsync(func() { m.chunkStore(remoteID, req) })
			}
		}
	case MsgTypeChunkGet:
		if m.chunkGet == nil {
			return
		}
		var req ChunkGetRequest
		if json.Unmarshal(env.Data, &req) == nil {
			data, ok := m.chunkGet(remoteID, req)
			if ok {
				m.sendEnvelope(remoteID, MsgTypeChunkData, ChunkStoreRequest{
					FileID:     req.FileID,
					ChunkIndex: req.ChunkIndex,
					Data:       data,
				})
			}
		}
	case MsgTypeChunkData:
		if m.chunkStore == nil {
			return
		}
		var msg ChunkStoreRequest
		if json.Unmarshal(env.Data, &msg) == nil {
			m.submitAsync(func() { m.chunkStore(remoteID, msg) })
		}
	case MsgTypeReloginRequest:
		if m.reloginReq == nil {
			return
		}
		var req ReloginRequest
		if json.Unmarshal(env.Data, &req) == nil {
			m.reloginReq(remoteID, req)
		}
	case MsgTypeReloginResponse:
		if m.reloginResp == nil {
			return
		}
		var resp ReloginResponse
		if json.Unmarshal(env.Data, &resp) == nil {
			m.reloginResp(remoteID, resp)
		}
	}
}

func (m *Manager) sendEnvelope(remoteID, msgType string, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	env := envelope{Type: msgType, Data: data}
	raw, err := json.Marshal(env)
	if err != nil {
		return
	}
	m.mu.RLock()
	dc, ok := m.dataChans[remoteID]
	m.mu.RUnlock()
	if !ok || dc == nil {
		return
	}
	dc.Send(raw)
}

func (m *Manager) NewPeerConnection(remoteID string) (*pion.PeerConnection, error) {
	m.mu.Lock()
	existing := m.connections[remoteID]
	delete(m.connections, remoteID)
	delete(m.dataChans, remoteID)
	m.mu.Unlock()
	if existing != nil {
		existing.Close()
	}

	se := pion.SettingEngine{}

	// Полезно на Android: не собирать IPv6-кандидаты.
	se.SetNetworkTypes([]pion.NetworkType{
		pion.NetworkTypeUDP4,
	})

	// Не включайте SetLite(true) на мобильном клиенте.
	// ICE Lite предназначен для публичного/серверного ICE-агента,
	// а не как обход Android.
	api := pion.NewAPI(pion.WithSettingEngine(se))

	pc, err := api.NewPeerConnection(m.config)
	if err != nil {
		return nil, fmt.Errorf("new pc: %w", err)
	}

	pc.OnDataChannel(func(dc *pion.DataChannel) {
		m.mu.Lock()
		if m.connections[remoteID] == pc {
			m.dataChans[remoteID] = dc
		}
		m.mu.Unlock()

		dc.OnMessage(func(msg pion.DataChannelMessage) {
			m.onMessage(remoteID, msg)
		})
	})

	pc.OnConnectionStateChange(func(s pion.PeerConnectionState) {
		if s == pion.PeerConnectionStateDisconnected || s == pion.PeerConnectionStateFailed || s == pion.PeerConnectionStateClosed {
			m.mu.Lock()
			if m.connections[remoteID] == pc {
				delete(m.connections, remoteID)
				delete(m.dataChans, remoteID)
			}
			m.mu.Unlock()
		}
	})

	m.mu.Lock()
	m.connections[remoteID] = pc
	m.mu.Unlock()

	return pc, nil
}

func (m *Manager) CreateOffer(remoteID string) (pion.SessionDescription, error) {
	log.Printf("creating offer: %s", remoteID)
	pc, err := m.NewPeerConnection(remoteID)
	if err != nil {
		return pion.SessionDescription{}, err
	}

	dc, err := pc.CreateDataChannel("chat", nil)
	if err != nil {
		m.Close(remoteID)
		return pion.SessionDescription{}, fmt.Errorf("create dc: %w", err)
	}

	m.mu.Lock()
	m.dataChans[remoteID] = dc
	m.mu.Unlock()

	dc.OnMessage(func(msg pion.DataChannelMessage) {
		m.onMessage(remoteID, msg)
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		m.Close(remoteID)
		return pion.SessionDescription{}, fmt.Errorf("create offer: %w", err)
	}

	if err := pc.SetLocalDescription(offer); err != nil {
		m.Close(remoteID)
		return pion.SessionDescription{}, fmt.Errorf("set local desc: %w", err)
	}

	gatherDone := pion.GatheringCompletePromise(pc)
	select {
	case <-gatherDone:
	case <-time.After(3 * time.Second):
	}
	return *pc.LocalDescription(), nil
}

func (m *Manager) HandleOffer(remoteID string, offer pion.SessionDescription) (pion.SessionDescription, error) {
	log.Printf("handle offer: %s", remoteID)
	pc, err := m.NewPeerConnection(remoteID)
	if err != nil {
		return pion.SessionDescription{}, err
	}

	if err := pc.SetRemoteDescription(offer); err != nil {
		m.Close(remoteID)
		return pion.SessionDescription{}, fmt.Errorf("set remote: %w", err)
	}

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		m.Close(remoteID)
		return pion.SessionDescription{}, fmt.Errorf("create answer: %w", err)
	}

	if err := pc.SetLocalDescription(answer); err != nil {
		m.Close(remoteID)
		return pion.SessionDescription{}, fmt.Errorf("set local answer: %w", err)
	}

	gatherDone := pion.GatheringCompletePromise(pc)
	select {
	case <-gatherDone:
	case <-time.After(3 * time.Second):
	}
	return *pc.LocalDescription(), nil
}

func (m *Manager) SetRemoteDescription(remoteID string, desc pion.SessionDescription) error {
	m.mu.RLock()
	pc, ok := m.connections[remoteID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("no connection for %s", remoteID)
	}
	return pc.SetRemoteDescription(desc)
}

func (m *Manager) AddICECandidate(remoteID string, candidate pion.ICECandidateInit) error {
	m.mu.RLock()
	pc, ok := m.connections[remoteID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("no connection for %s", remoteID)
	}
	return pc.AddICECandidate(candidate)
}

func (m *Manager) SendMessage(remoteID, text string, timestamp time.Time, msgID string) error {
	m.mu.RLock()
	dc, ok := m.dataChans[remoteID]
	m.mu.RUnlock()
	if !ok || dc == nil {
		return fmt.Errorf("no data channel to %s", remoteID)
	}

	chat := ChatMessage{From: m.localID, Text: text, Timestamp: timestamp, MsgID: msgID}
	chatData, _ := json.Marshal(chat)
	env := envelope{Type: MsgTypeChat, Data: chatData}
	raw, _ := json.Marshal(env)
	return dc.Send(raw)
}

func (m *Manager) SendChunkStore(remoteID string, req ChunkStoreRequest) error {
	m.mu.RLock()
	dc, ok := m.dataChans[remoteID]
	m.mu.RUnlock()
	if !ok || dc == nil {
		return fmt.Errorf("no data channel to %s", remoteID)
	}
	reqData, _ := json.Marshal(req)
	env := envelope{Type: MsgTypeChunkStore, Data: reqData}
	raw, _ := json.Marshal(env)
	return dc.Send(raw)
}

func (m *Manager) SendChunkStoreBatch(remoteID string, batch ChunkStoreBatchRequest) error {
	m.mu.RLock()
	dc, ok := m.dataChans[remoteID]
	m.mu.RUnlock()
	if !ok || dc == nil {
		return fmt.Errorf("no data channel to %s", remoteID)
	}
	reqData, _ := json.Marshal(batch)
	env := envelope{Type: MsgTypeChunkStoreBatch, Data: reqData}
	raw, _ := json.Marshal(env)
	return dc.Send(raw)
}

func (m *Manager) SendChunkGet(remoteID string, req ChunkGetRequest) error {
	m.mu.RLock()
	dc, ok := m.dataChans[remoteID]
	m.mu.RUnlock()
	if !ok || dc == nil {
		return fmt.Errorf("no data channel to %s", remoteID)
	}
	reqData, _ := json.Marshal(req)
	env := envelope{Type: MsgTypeChunkGet, Data: reqData}
	raw, _ := json.Marshal(env)
	return dc.Send(raw)
}

func (m *Manager) SendReloginRequest(remoteID string, req ReloginRequest) {
	m.sendEnvelope(remoteID, MsgTypeReloginRequest, req)
}

func (m *Manager) SendReloginResponse(remoteID string, resp ReloginResponse) {
	m.sendEnvelope(remoteID, MsgTypeReloginResponse, resp)
}

func (m *Manager) IsConnected(remoteID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.connections[remoteID]
	return ok
}

func (m *Manager) Close(remoteID string) {
	m.mu.Lock()
	pc := m.connections[remoteID]
	delete(m.connections, remoteID)
	delete(m.dataChans, remoteID)
	m.mu.Unlock()
	if pc != nil {
		pc.Close()
	}
}

func (m *Manager) CloseAll() {
	m.mu.Lock()
	connections := make([]*pion.PeerConnection, 0, len(m.connections))
	for _, pc := range m.connections {
		connections = append(connections, pc)
	}
	m.connections = make(map[string]*pion.PeerConnection)
	m.dataChans = make(map[string]*pion.DataChannel)
	m.mu.Unlock()
	for _, pc := range connections {
		pc.Close()
	}
}
