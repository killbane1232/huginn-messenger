package messenger

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
)

type reconnectServer struct {
	server      *httptest.Server
	delay       atomic.Int64
	healthCalls atomic.Int32
	apiCalls    atomic.Int32
	wsCalls     atomic.Int32
	apiFailed   atomic.Bool
	wsFailed    atomic.Bool
	connection  atomic.Pointer[websocket.Conn]
	mu          sync.Mutex
	registered  map[string]bool
}

func newReconnectServer(t *testing.T, delay time.Duration) *reconnectServer {
	t.Helper()
	s := &reconnectServer{registered: make(map[string]bool)}
	s.delay.Store(int64(delay))
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			s.healthCalls.Add(1)
			timer := time.NewTimer(time.Duration(s.delay.Load()))
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-r.Context().Done():
				return
			}
			_, _ = w.Write([]byte(`{"status":"ok"}`))
			return
		case "/api/v1/ws":
			s.wsCalls.Add(1)
			if s.wsFailed.Load() {
				http.Error(w, "signaling unavailable", http.StatusServiceUnavailable)
				return
			}
			conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			s.connection.Store(conn)
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}
		s.apiCalls.Add(1)
		if s.apiFailed.Load() {
			http.Error(w, "API unavailable", http.StatusServiceUnavailable)
			return
		}
		if r.URL.Path == "/api/v1/peers" && r.Method == http.MethodPost {
			var req muninn.RegisterRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
			}
			s.mu.Lock()
			s.registered[req.ID] = true
			s.mu.Unlock()
			w.WriteHeader(http.StatusCreated)
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(func() {
		if conn := s.connection.Load(); conn != nil {
			conn.Close()
		}
		s.server.Close()
	})
	return s
}

func newReconnectMessenger(t *testing.T, a, b *reconnectServer) *Messenger {
	t.Helper()
	m, err := New("alice", muninn.NewClient(a.server.URL), filepath.Join(t.TempDir(), "huginn.db"), WithMuninnServers([]string{b.server.URL}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Shutdown)
	if err := m.Register(); err != nil {
		t.Fatal(err)
	}
	return m
}

func waitForMuninn(t *testing.T, timeout time.Duration, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for Muninn connection")
}

func TestMuninnSelectionWaitsForWebSocketDisconnect(t *testing.T) {
	t.Parallel()
	a := newReconnectServer(t, time.Millisecond)
	b := newReconnectServer(t, 40*time.Millisecond)
	m := newReconnectMessenger(t, a, b)
	waitForMuninn(t, time.Second, func() bool { return m.wsClient.IsConnected() && a.connection.Load() != nil })
	initialA, initialB := a.healthCalls.Load(), b.healthCalls.Load()
	a.delay.Store(int64(300 * time.Millisecond))
	b.delay.Store(int64(time.Millisecond))
	a.apiFailed.Store(true)
	for i := 0; i < 2; i++ {
		if _, err := m.muninnClient.List(m.ctx); err == nil {
			t.Fatal("expected HTTP failure")
		}
	}
	// The old HTTP-failure watcher would probe and switch after five seconds.
	time.Sleep(6 * time.Second)
	if !m.wsClient.IsConnected() || m.muninnClient.BaseURL() != a.server.URL || a.healthCalls.Load() != initialA || b.healthCalls.Load() != initialB || b.apiCalls.Load() != 0 {
		t.Fatal("working WebSocket did not prevent background selection")
	}
	a.apiFailed.Store(false)
	if _, err := m.muninnClient.List(m.ctx); err != nil {
		t.Fatal(err)
	}
	a.connection.Load().Close()
	waitForMuninn(t, 3*time.Second, func() bool {
		return m.muninnClient.BaseURL() == b.server.URL && m.wsClient.IsConnected() && b.connection.Load() != nil
	})
	if a.healthCalls.Load() != initialA+2 || b.healthCalls.Load() != initialB+2 {
		t.Fatal("disconnect did not trigger exactly one selection round")
	}
	saved, err := m.store.LoadAppConfig()
	if err != nil || saved.MuninnAddr != b.server.URL {
		t.Fatalf("selection was not saved: %+v, %v", saved, err)
	}
	b.mu.Lock()
	registered := b.registered[m.ID]
	b.mu.Unlock()
	if !registered {
		t.Fatal("selected server did not receive the endpoint registration")
	}
}

func TestMuninnWebSocketFailureFailsOverDespiteHealthyHTTP(t *testing.T) {
	for _, initialFailure := range []bool{false, true} {
		name := "reconnect"
		if initialFailure {
			name = "initial connect"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			a := newReconnectServer(t, time.Millisecond)
			b := newReconnectServer(t, 40*time.Millisecond)
			a.wsFailed.Store(initialFailure)
			m := newReconnectMessenger(t, a, b)
			if !initialFailure {
				waitForMuninn(t, time.Second, func() bool { return m.wsClient.IsConnected() && a.connection.Load() != nil })
				a.wsFailed.Store(true)
				a.connection.Load().Close()
			}
			// HTTP remains successful and faster on A throughout the outage.
			waitForMuninn(t, 8*time.Second, func() bool {
				return m.muninnClient.BaseURL() == b.server.URL && m.wsClient.IsConnected() && b.connection.Load() != nil
			})
			if a.wsCalls.Load() > 2 || b.wsCalls.Load() != 1 {
				t.Fatal("connection attempts were not bounded")
			}
		})
	}
}

func TestMuninnReconnectCanKeepFastestServer(t *testing.T) {
	t.Parallel()
	a := newReconnectServer(t, time.Millisecond)
	b := newReconnectServer(t, 40*time.Millisecond)
	m := newReconnectMessenger(t, a, b)
	waitForMuninn(t, time.Second, func() bool { return m.wsClient.IsConnected() && a.connection.Load() != nil })
	previous := a.connection.Load()
	previous.Close()
	waitForMuninn(t, 2*time.Second, func() bool { return m.wsClient.IsConnected() && a.connection.Load() != previous })
	if m.muninnClient.BaseURL() != a.server.URL || a.healthCalls.Load() != 4 || b.healthCalls.Load() != 4 || b.wsCalls.Load() != 0 || b.apiCalls.Load() != 0 {
		t.Fatal("reconnect did not check and retain the fastest server")
	}
	// A late notification from a previous socket must not disturb a live one.
	m.wsDisconnected <- struct{}{}
	time.Sleep(100 * time.Millisecond)
	if a.healthCalls.Load() != 4 || b.healthCalls.Load() != 4 || a.wsCalls.Load() != 2 {
		t.Fatal("stale disconnect triggered another selection")
	}
}

func TestMuninnShutdownCancelsReconnectWait(t *testing.T) {
	t.Parallel()
	a := newReconnectServer(t, time.Millisecond)
	b := newReconnectServer(t, 40*time.Millisecond)
	a.wsFailed.Store(true)
	b.wsFailed.Store(true)
	m := newReconnectMessenger(t, a, b)
	waitForMuninn(t, time.Second, func() bool { return a.wsCalls.Load() > 0 })
	done := make(chan struct{})
	go func() { m.Shutdown(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown waited for a reconnect timer")
	}
	if a.healthCalls.Load() != 2 || b.healthCalls.Load() != 2 {
		t.Fatal("shutdown started another selection")
	}
}
