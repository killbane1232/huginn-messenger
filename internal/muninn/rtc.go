package muninn

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	pion "github.com/pion/webrtc/v3"
)

const (
	rpcMethodSignalRelay     = "signal_relay"
	rpcNotifyIncomingSignal  = "incoming_signal"
	rpcMethodConnectToPeer   = "connect_to_peer"

	rtcRequestTimeout = 10 * time.Second
)

type rpcRequest struct {
	ID     string          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type rpcResponse struct {
	ID     string          `json:"id"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

type rpcNotification struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type SignalRelayRequest struct {
	TargetID string `json:"target_id"`
	From     string `json:"from"`
	Type     string `json:"type"`
	Data     string `json:"data"`
}

type ConnectToPeerRequest struct {
	TargetID string `json:"target_id"`
	Offer    string `json:"offer"`
}

type IncomingSignal struct {
	From string `json:"from"`
	Type string `json:"type"`
	Data string `json:"data"`
}

type OnSignalFunc func(sig Signal)

type OnDisconnectFunc func()

type RTCClient struct {
	mu      sync.RWMutex
	baseURL string
	localID string

	pc         *pion.PeerConnection
	dc         *pion.DataChannel
	connected  bool
	iceServers []pion.ICEServer

	pending   map[string]chan<- rpcResponse
	pendingMu sync.Mutex

	onSignal     OnSignalFunc
	onDisconnect OnDisconnectFunc

	ctx    context.Context
	cancel context.CancelFunc

	httpClient *http.Client
	closeOnce  sync.Once
}

func NewRTCClient(baseURL, localID string, iceServers []pion.ICEServer) *RTCClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &RTCClient{
		baseURL:    baseURL,
		localID:    localID,
		iceServers: iceServers,
		pending:    make(map[string]chan<- rpcResponse),
		ctx:        ctx,
		cancel:     cancel,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *RTCClient) SetOnSignal(fn OnSignalFunc) {
	c.mu.Lock()
	c.onSignal = fn
	c.mu.Unlock()
}

func (c *RTCClient) SetOnDisconnect(fn OnDisconnectFunc) {
	c.mu.Lock()
	c.onDisconnect = fn
	c.mu.Unlock()
}

func (c *RTCClient) Connect(ctx context.Context) error {
	config := pion.Configuration{
		ICEServers: c.iceServers,
	}

	pc, err := pion.NewPeerConnection(config)
	if err != nil {
		return fmt.Errorf("new pc: %w", err)
	}

	dc, err := pc.CreateDataChannel("muninn-rpc", nil)
	if err != nil {
		pc.Close()
		return fmt.Errorf("create dc: %w", err)
	}

	c.dc = dc
	c.pc = pc

	dc.OnMessage(func(msg pion.DataChannelMessage) {
		c.handleMessage(msg.Data)
	})

	dc.OnOpen(func() {
		c.mu.Lock()
		c.connected = true
		c.mu.Unlock()
		log.Printf("[rtc] connected to muninn")
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		pc.Close()
		return fmt.Errorf("create offer: %w", err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		pc.Close()
		return fmt.Errorf("set local desc: %w", err)
	}

	<-pion.GatheringCompletePromise(pc)

	answer, err := c.bootstrap(ctx, *pc.LocalDescription())
	if err != nil {
		pc.Close()
		return fmt.Errorf("bootstrap: %w", err)
	}

	if err := pc.SetRemoteDescription(*answer); err != nil {
		pc.Close()
		return fmt.Errorf("set remote desc: %w", err)
	}

	pc.OnConnectionStateChange(func(s pion.PeerConnectionState) {
		switch s {
		case pion.PeerConnectionStateConnected:
			log.Printf("[rtc] connection state: connected")
		case pion.PeerConnectionStateDisconnected,
			pion.PeerConnectionStateFailed,
			pion.PeerConnectionStateClosed:
			c.mu.Lock()
			c.connected = false
			c.mu.Unlock()
			log.Printf("[rtc] connection state: %s", s)
			if fn := c.onDisconnect; fn != nil {
				fn()
			}
		}
	})

	return nil
}

func (c *RTCClient) bootstrap(ctx context.Context, offer pion.SessionDescription) (*pion.SessionDescription, error) {
	body, err := json.Marshal(offer)
	if err != nil {
		return nil, fmt.Errorf("marshal offer: %w", err)
	}

	url := fmt.Sprintf("%s/api/v1/webrtc/bootstrap", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Peer-ID", c.localID)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("bootstrap failed (status %d): %s", resp.StatusCode, string(b))
	}

	var answer pion.SessionDescription
	if err := json.NewDecoder(resp.Body).Decode(&answer); err != nil {
		return nil, fmt.Errorf("decode answer: %w", err)
	}
	return &answer, nil
}

func (c *RTCClient) handleMessage(data []byte) {
	var notif rpcNotification
	if err := json.Unmarshal(data, &notif); err == nil && notif.Method != "" {
		c.handleNotification(notif)
		return
	}

	var resp rpcResponse
	if err := json.Unmarshal(data, &resp); err != nil || resp.ID == "" {
		return
	}

	c.pendingMu.Lock()
	ch, ok := c.pending[resp.ID]
	delete(c.pending, resp.ID)
	c.pendingMu.Unlock()

	if ok {
		ch <- resp
	}
}

func (c *RTCClient) handleNotification(notif rpcNotification) {
	switch notif.Method {
	case rpcNotifyIncomingSignal:
		var sig IncomingSignal
		if json.Unmarshal(notif.Params, &sig) != nil {
			return
		}
		c.mu.RLock()
		fn := c.onSignal
		c.mu.RUnlock()
		if fn != nil {
			fn(Signal{From: sig.From, Type: sig.Type, Data: sig.Data})
		}
	}
}

func (c *RTCClient) sendRequest(ctx context.Context, method string, params any) (json.RawMessage, error) {
	id := uuid.New().String()
	ch := make(chan rpcResponse, 1)

	c.pendingMu.Lock()
	c.pending[id] = ch
	c.pendingMu.Unlock()

	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
	}()

	p, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("marshal params: %w", err)
	}

	req := rpcRequest{ID: id, Method: method, Params: p}
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	c.mu.RLock()
	dc := c.dc
	connected := c.connected
	c.mu.RUnlock()

	if !connected || dc == nil {
		return nil, fmt.Errorf("not connected to muninn")
	}

	if err := dc.Send(data); err != nil {
		return nil, fmt.Errorf("send: %w", err)
	}

	select {
	case resp := <-ch:
		if resp.Error != "" {
			return nil, fmt.Errorf("rpc error: %s", resp.Error)
		}
		return resp.Result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(rtcRequestTimeout):
		return nil, fmt.Errorf("rpc timeout")
	}
}

func (c *RTCClient) RelaySignal(ctx context.Context, targetID, sigType, data string) error {
	params := SignalRelayRequest{
		TargetID: targetID,
		From:     c.localID,
		Type:     sigType,
		Data:     data,
	}
	_, err := c.sendRequest(ctx, rpcMethodSignalRelay, params)
	return err
}

func (c *RTCClient) ConnectToPeer(ctx context.Context, targetID, offer string) error {
	params := ConnectToPeerRequest{
		TargetID: targetID,
		Offer:    offer,
	}
	_, err := c.sendRequest(ctx, rpcMethodConnectToPeer, params)
	return err
}

func (c *RTCClient) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

func (c *RTCClient) Close() {
	c.closeOnce.Do(func() {
		c.cancel()
		c.mu.Lock()
		if c.dc != nil {
			c.dc.Close()
		}
		if c.pc != nil {
			c.pc.Close()
		}
		c.connected = false
		c.mu.Unlock()
	})
}
