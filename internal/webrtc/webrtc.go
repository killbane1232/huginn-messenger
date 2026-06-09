package webrtc

import (
	"encoding/json"
	"fmt"
	"sync"

	pion "github.com/pion/webrtc/v3"
)

type ChatMessage struct {
	From string `json:"from"`
	Text string `json:"text"`
}

type Manager struct {
	mu          sync.RWMutex
	connections map[string]*pion.PeerConnection
	dataChans   map[string]*pion.DataChannel
	msgChan     chan ChatMessage
	localID     string

	config pion.Configuration
}

func NewManager(localID string, msgChan chan ChatMessage) *Manager {
	return &Manager{
		connections: make(map[string]*pion.PeerConnection),
		dataChans:   make(map[string]*pion.DataChannel),
		msgChan:     msgChan,
		localID:     localID,
	}
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
			var chat ChatMessage
			if json.Unmarshal(msg.Data, &chat) == nil {
				chat.From = remoteID
				select {
				case m.msgChan <- chat:
				default:
				}
			}
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
		var chat ChatMessage
		if json.Unmarshal(msg.Data, &chat) == nil {
			chat.From = remoteID
			select {
			case m.msgChan <- chat:
			default:
			}
		}
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

	return offer, nil
}

func (m *Manager) HandleOffer(remoteID string, offer pion.SessionDescription) (pion.SessionDescription, error) {
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

	return answer, nil
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

func (m *Manager) SendMessage(remoteID, text string) error {
	m.mu.RLock()
	dc, ok := m.dataChans[remoteID]
	m.mu.RUnlock()
	if !ok || dc == nil {
		return fmt.Errorf("no data channel to %s", remoteID)
	}

	msg := ChatMessage{From: m.localID, Text: text}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return dc.Send(data)
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
