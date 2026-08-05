package muninn

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	rpcMethodSignalRelay    = "signal_relay"
	rpcNotifyIncomingSignal = "incoming_signal"
	rpcMethodConnectToPeer  = "connect_to_peer"

	rtcRequestTimeout = 10 * time.Second
)

type rpcRequest struct {
	ID     string          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type wsResponse struct {
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

type WSClient struct {
	mu        sync.RWMutex
	connectMu sync.Mutex
	baseURL   string
	localID   string

	conn      *websocket.Conn
	connected bool

	pending   map[string]chan<- wsResponse
	pendingMu sync.Mutex

	onSignal     OnSignalFunc
	onDisconnect OnDisconnectFunc

	ctx    context.Context
	cancel context.CancelFunc

	writeMu   sync.Mutex
	closeOnce sync.Once
}

func NewWSClient(baseURL, localID string) *WSClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &WSClient{
		baseURL: baseURL,
		localID: localID,
		pending: make(map[string]chan<- wsResponse),
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (c *WSClient) SetOnSignal(fn OnSignalFunc) {
	c.mu.Lock()
	c.onSignal = fn
	c.mu.Unlock()
}

func (c *WSClient) SetOnDisconnect(fn OnDisconnectFunc) {
	c.mu.Lock()
	c.onDisconnect = fn
	c.mu.Unlock()
}

func (c *WSClient) Connect(ctx context.Context) error {
	c.connectMu.Lock()
	defer c.connectMu.Unlock()

	if c.ctx.Err() != nil {
		return fmt.Errorf("websocket client is closed")
	}
	c.mu.RLock()
	connected := c.connected && c.conn != nil
	c.mu.RUnlock()
	if connected {
		return nil
	}

	u, err := url.Parse(c.baseURL)
	if err != nil {
		return fmt.Errorf("parse base url: %w", err)
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	default:
		u.Scheme = "ws"
	}
	u.Path = "/api/v1/ws"
	q := u.Query()
	q.Set("peer_id", c.localID)
	u.RawQuery = q.Encode()

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	header := http.Header{}
	header.Set("X-Peer-ID", c.localID)

	conn, _, err := dialer.DialContext(ctx, u.String(), header)
	if err != nil {
		return fmt.Errorf("websocket dial: %w", err)
	}

	c.mu.Lock()
	if c.ctx.Err() != nil {
		c.mu.Unlock()
		conn.Close()
		return fmt.Errorf("websocket client is closed")
	}
	c.conn = conn
	c.connected = true
	c.mu.Unlock()

	c.mu.RLock()
	onDisconnect := c.onDisconnect
	c.mu.RUnlock()

	go c.readLoop(conn, onDisconnect)

	log.Printf("[ws] connected to muninn at %s", u.String())
	return nil
}

func (c *WSClient) readLoop(conn *websocket.Conn, onDisconnect OnDisconnectFunc) {
	defer func() {
		conn.Close()
		notifyDisconnect := false
		c.mu.Lock()
		if c.conn == conn {
			c.conn = nil
			c.connected = false
			notifyDisconnect = c.ctx.Err() == nil
		}
		c.mu.Unlock()
		if notifyDisconnect && onDisconnect != nil {
			onDisconnect()
		}
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			if c.ctx.Err() == nil {
				log.Printf("[ws] read error: %v", err)
			}
			return
		}
		c.handleMessage(data)
	}
}

func (c *WSClient) handleMessage(data []byte) {
	var notif rpcNotification
	if err := json.Unmarshal(data, &notif); err == nil && notif.Method != "" {
		c.handleNotification(notif)
		return
	}

	var resp wsResponse
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

func (c *WSClient) handleNotification(notif rpcNotification) {
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

func (c *WSClient) sendRequest(ctx context.Context, method string, params any) (json.RawMessage, error) {
	id := uuid.New().String()
	ch := make(chan wsResponse, 1)

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
	conn := c.conn
	connected := c.connected
	c.mu.RUnlock()

	if !connected || conn == nil {
		return nil, fmt.Errorf("not connected to muninn")
	}

	c.writeMu.Lock()
	err = conn.WriteMessage(websocket.TextMessage, data)
	c.writeMu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("send: %w", err)
	}

	timer := time.NewTimer(rtcRequestTimeout)
	defer timer.Stop()
	select {
	case resp := <-ch:
		if resp.Error != "" {
			return nil, fmt.Errorf("rpc error: %s", resp.Error)
		}
		return resp.Result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("rpc timeout")
	}
}

func (c *WSClient) RelaySignal(ctx context.Context, targetID, sigType, data string) error {
	params := SignalRelayRequest{
		TargetID: targetID,
		From:     c.localID,
		Type:     sigType,
		Data:     data,
	}
	_, err := c.sendRequest(ctx, rpcMethodSignalRelay, params)
	return err
}

func (c *WSClient) ConnectToPeer(ctx context.Context, targetID, offer string) error {
	params := ConnectToPeerRequest{
		TargetID: targetID,
		Offer:    offer,
	}
	_, err := c.sendRequest(ctx, rpcMethodConnectToPeer, params)
	return err
}

func (c *WSClient) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

func (c *WSClient) Close() {
	c.closeOnce.Do(func() {
		c.cancel()
		c.mu.Lock()
		conn := c.conn
		c.conn = nil
		c.connected = false
		c.mu.Unlock()
		if conn != nil {
			conn.Close()
		}
	})
}
