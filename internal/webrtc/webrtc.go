package webrtc

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
	"log"

	pion "github.com/pion/webrtc/v3"
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
)

type Manager struct {
	mu          sync.RWMutex
	connections map[string]*pion.PeerConnection
	dataChans   map[string]*pion.DataChannel
	chatMsgChan chan ChatMessage
	chunkStore  func(peerID string, req ChunkStoreRequest)
	chunkGet    func(peerID string, req ChunkGetRequest) ([]byte, bool)
	localID     string

	config pion.Configuration
}

func NewManager(localID string, chatMsgChan chan ChatMessage,
	chunkStore func(peerID string, req ChunkStoreRequest),
	chunkGet func(peerID string, req ChunkGetRequest) ([]byte, bool),
	iceServers []pion.ICEServer) *Manager {

	return &Manager{
		connections: make(map[string]*pion.PeerConnection),
		dataChans:   make(map[string]*pion.DataChannel),
		chatMsgChan: chatMsgChan,
		chunkStore:  chunkStore,
		chunkGet:    chunkGet,
		localID:     localID,
		config: pion.Configuration{
			ICEServers: iceServers,
		},
	}
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
			m.chunkStore(remoteID, req)
		}
	case MsgTypeChunkStoreBatch:
		if m.chunkStore == nil {
			return
		}
		var batch ChunkStoreBatchRequest
		if json.Unmarshal(env.Data, &batch) == nil {
			for _, req := range batch.Chunks {
				m.chunkStore(remoteID, req)
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
			m.chunkStore(remoteID, msg)
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
	pc, err := pion.NewPeerConnection(m.config)
	if err != nil {
		return nil, fmt.Errorf("new pc: %w", err)
	}

	pc.OnDataChannel(func(dc *pion.DataChannel) {
		m.mu.Lock()
		m.dataChans[remoteID] = dc
		m.mu.Unlock()

		dc.OnMessage(func(msg pion.DataChannelMessage) {
			m.onMessage(remoteID, msg)
		})
	})

	pc.OnConnectionStateChange(func(s pion.PeerConnectionState) {
		if s == pion.PeerConnectionStateDisconnected || s == pion.PeerConnectionStateFailed || s == pion.PeerConnectionStateClosed {
			m.mu.Lock()
			delete(m.connections, remoteID)
			delete(m.dataChans, remoteID)
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

	<-pion.GatheringCompletePromise(pc)
	return *pc.LocalDescription(), nil
}

func (m *Manager) HandleOffer(remoteID string, offer pion.SessionDescription) (pion.SessionDescription, error) {
	log.Printf("hadke offer: %s", remoteID)
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

	<-pion.GatheringCompletePromise(pc)
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

func (m *Manager) IsConnected(remoteID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.connections[remoteID]
	return ok
}

func (m *Manager) Close(remoteID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if pc, ok := m.connections[remoteID]; ok {
		pc.Close()
	}
	delete(m.connections, remoteID)
	delete(m.dataChans, remoteID)
}

func (m *Manager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, pc := range m.connections {
		pc.Close()
		delete(m.connections, id)
		delete(m.dataChans, id)
	}
}
