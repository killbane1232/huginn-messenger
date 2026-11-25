package muninn

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type selectionServer struct {
	server             *httptest.Server
	delay              atomic.Int64
	healthStatus       atomic.Int32
	apiStatus          atomic.Int32
	registrationStatus atomic.Int32
	healthCalls        atomic.Int32
	apiCalls           atomic.Int32
	wsCalls            atomic.Int32
	wsSignals          atomic.Int32
	mu                 sync.Mutex
	registrations      map[string]RegisterRequest
	holdList           <-chan struct{}
	listStarted        chan struct{}
}

func newSelectionServer(t *testing.T, latency time.Duration) *selectionServer {
	t.Helper()
	s := &selectionServer{registrations: map[string]RegisterRequest{}}
	s.delay.Store(int64(latency))
	s.healthStatus.Store(200)
	s.apiStatus.Store(200)
	s.registrationStatus.Store(200)
	s.server = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.server.Close)
	return s
}
func (s *selectionServer) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" {
		s.healthCalls.Add(1)
		timer := time.NewTimer(time.Duration(s.delay.Load()))
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
		w.WriteHeader(int(s.healthStatus.Load()))
		_, _ = w.Write([]byte(`{"status":"ok"}`))
		return
	}
	if r.URL.Path == "/api/v1/ws" {
		s.mu.Lock()
		_, registered := s.registrations[r.URL.Query().Get("peer_id")]
		s.mu.Unlock()
		if !registered {
			http.Error(w, "register first", 404)
			return
		}
		conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		s.wsCalls.Add(1)
		for {
			var req rpcRequest
			if conn.ReadJSON(&req) != nil {
				return
			}
			s.wsSignals.Add(1)
			if conn.WriteJSON(wsResponse{ID: req.ID, Result: json.RawMessage(`{}`)}) != nil {
				return
			}
		}
	}
	s.apiCalls.Add(1)
	if code := s.apiStatus.Load(); code != 200 {
		w.WriteHeader(int(code))
		return
	}
	switch {
	case r.Method == "POST" && r.URL.Path == "/api/v1/peers":
		if code := s.registrationStatus.Load(); code != 200 {
			w.WriteHeader(int(code))
			return
		}
		var req RegisterRequest
		if json.NewDecoder(r.Body).Decode(&req) != nil {
			http.Error(w, "bad request", 400)
			return
		}
		s.mu.Lock()
		s.registrations[req.ID] = req
		s.mu.Unlock()
		w.WriteHeader(200)
	case r.Method == "DELETE" && strings.HasPrefix(r.URL.Path, "/api/v1/peers/"):
		s.mu.Lock()
		delete(s.registrations, strings.TrimPrefix(r.URL.Path, "/api/v1/peers/"))
		s.mu.Unlock()
		w.WriteHeader(204)
	case r.Method == "GET":
		if r.URL.Path == "/api/v1/peers" && s.holdList != nil {
			_, _ = w.Write([]byte("["))
			w.(http.Flusher).Flush()
			close(s.listStarted)
			select {
			case <-s.holdList:
			case <-r.Context().Done():
				return
			}
			_, _ = w.Write([]byte("]"))
			return
		}
		_, _ = w.Write([]byte(`[]`))
	case strings.HasSuffix(r.URL.Path, "/signals"):
		w.WriteHeader(204)
	default:
		_, _ = w.Write([]byte(`{}`))
	}
}
func (s *selectionServer) has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.registrations[id]
	return ok
}

func TestFastestServerGetsAllBusinessRequests(t *testing.T) {
	slow := newSelectionServer(t, 40*time.Millisecond)
	fast := newSelectionServer(t, time.Millisecond)
	broken := newSelectionServer(t, 0)
	broken.healthStatus.Store(503)
	c := NewClient(slow.server.URL)
	c.SetServers([]string{fast.server.URL, broken.server.URL})
	changed, err := c.SelectFastest(context.Background())
	if err != nil || !changed || c.BaseURL() != fast.server.URL {
		t.Fatalf("selection: %t %s %v", changed, c.BaseURL(), err)
	}
	if err := c.Register(context.Background(), &RegisterRequest{ID: "alice"}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	failures := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := c.List(context.Background()); failures <- err }()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := c.SendSignal(context.Background(), "bob", Signal{From: "alice", Type: "offer", Data: "SDP"}); err != nil {
		t.Fatal(err)
	}
	if slow.apiCalls.Load() != 0 || broken.apiCalls.Load() != 0 {
		t.Fatal("business requests were sent to an alternative")
	}
	if fast.apiCalls.Load() != 22 {
		t.Fatalf("selected requests = %d", fast.apiCalls.Load())
	}
	if slow.healthCalls.Load() != 2 || fast.healthCalls.Load() != 2 || broken.healthCalls.Load() != 1 {
		t.Fatal("missing bounded health probes")
	}
}
func TestSelectionRetainsStableServerAndBypassesHysteresisOnFailure(t *testing.T) {
	a := newSelectionServer(t, 20*time.Millisecond)
	b := newSelectionServer(t, 25*time.Millisecond)
	c := NewClient(a.server.URL)
	c.SetServers([]string{b.server.URL})
	if changed, err := c.SelectFastest(context.Background()); err != nil || changed {
		t.Fatalf("initial %t %v", changed, err)
	}
	b.delay.Store(int64(18 * time.Millisecond))
	if changed, err := c.SelectFastest(context.Background()); err != nil || changed {
		t.Fatalf("switched for small jitter: %t %v", changed, err)
	}
	a.healthStatus.Store(503)
	if changed, err := c.SelectFastest(context.Background()); err != nil || !changed || c.BaseURL() != b.server.URL {
		t.Fatalf("failover %t %v", changed, err)
	}
}
func TestAPIFailuresDoNotReplayAndTriggerFailover(t *testing.T) {
	a := newSelectionServer(t, 0)
	b := newSelectionServer(t, 5*time.Millisecond)
	c := NewClient(a.server.URL)
	c.SetServers([]string{b.server.URL})
	if _, err := c.SelectFastest(context.Background()); err != nil {
		t.Fatal(err)
	}
	a.apiStatus.Store(503)
	for i := 0; i < 2; i++ {
		if err := c.SendSignal(context.Background(), "bob", Signal{From: "alice", Type: "offer"}); err == nil {
			t.Fatal("missing API error")
		}
	}
	if b.apiCalls.Load() != 0 || a.apiCalls.Load() != 2 {
		t.Fatal("failed write was replayed on another server")
	}
	if !c.NeedsSelection() {
		t.Fatal("API failures were not recorded")
	}
	if changed, err := c.SelectFastest(context.Background()); err != nil || !changed || c.BaseURL() != b.server.URL {
		t.Fatalf("healthy /health hid failed API: %t %v", changed, err)
	}
}
func TestSwitchMovesRegistrationAndWebSocketTogether(t *testing.T) {
	a := newSelectionServer(t, 30*time.Millisecond)
	b := newSelectionServer(t, time.Millisecond)
	c := NewClient(a.server.URL)
	c.SetServers([]string{b.server.URL})
	if err := c.Register(context.Background(), &RegisterRequest{ID: "alice", Login: "alice", SignatureKey: "key", TTLSeconds: 120}); err != nil {
		t.Fatal(err)
	}
	fake := true
	if err := c.Register(context.Background(), &RegisterRequest{ID: "group", Fake: &fake}); err != nil {
		t.Fatal(err)
	}
	ws := NewSelectedWSClient(c, "alice")
	t.Cleanup(ws.Close)
	if err := ws.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := ws.RelaySignal(context.Background(), "bob", "offer", "before"); err != nil {
		t.Fatal(err)
	}
	if changed, err := c.SelectFastest(context.Background()); err != nil || !changed {
		t.Fatalf("switch: %t %v", changed, err)
	}
	if !a.has("alice") || !b.has("alice") || !b.has("group") || !a.has("group") {
		t.Fatal("registration was not migrated correctly")
	}
	if ws.IsConnected() {
		t.Fatal("old WebSocket remained connected")
	}
	if err := ws.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := ws.RelaySignal(context.Background(), "bob", "offer", "after"); err != nil {
		t.Fatal(err)
	}
	if a.wsSignals.Load() != 1 || b.wsSignals.Load() != 1 {
		t.Fatal("WebSocket did not follow selected HTTP server")
	}
	b.mu.Lock()
	hints := b.registrations["alice"].MuninnServers
	b.mu.Unlock()
	if len(hints) != 1 || hints[0] != a.server.URL {
		t.Fatalf("old home was not advertised: %v", hints)
	}
}
func TestFailedRegistrationKeepsPreviousServer(t *testing.T) {
	a := newSelectionServer(t, 25*time.Millisecond)
	b := newSelectionServer(t, 0)
	b.registrationStatus.Store(503)
	c := NewClient(a.server.URL)
	c.SetServers([]string{b.server.URL})
	if err := c.Register(context.Background(), &RegisterRequest{ID: "alice"}); err != nil {
		t.Fatal(err)
	}
	if changed, err := c.SelectFastest(context.Background()); err == nil || changed || c.BaseURL() != a.server.URL {
		t.Fatalf("failed migration switched server: %t %v", changed, err)
	}
	if !a.has("alice") {
		t.Fatal("old registration was lost")
	}
}
func TestSwitchWaitsForExistingResponse(t *testing.T) {
	a := newSelectionServer(t, 0)
	b := newSelectionServer(t, 0)
	release := make(chan struct{})
	a.holdList = release
	a.listStarted = make(chan struct{})
	c := NewClient(a.server.URL)
	c.SetServers([]string{b.server.URL})
	if err := c.Register(context.Background(), &RegisterRequest{ID: "alice"}); err != nil {
		t.Fatal(err)
	}
	listDone := make(chan error, 1)
	go func() { _, err := c.List(context.Background()); listDone <- err }()
	<-a.listStarted
	switchDone := make(chan error, 1)
	go func() { switchDone <- c.switchServer(context.Background(), b.server.URL) }()
	select {
	case err := <-switchDone:
		close(release)
		t.Fatalf("switch overtook pending response: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if b.apiCalls.Load() != 0 {
		t.Fatal("new server received work before old requests drained")
	}
	close(release)
	if err := <-listDone; err != nil {
		t.Fatal(err)
	}
	if err := <-switchDone; err != nil {
		t.Fatal(err)
	}
}
func TestRedirectCannotSendBusinessRequestToAlternative(t *testing.T) {
	target := newSelectionServer(t, 0)
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.server.URL+"/api/v1/peers", http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	if _, err := NewClient(redirect.URL).List(context.Background()); err == nil {
		t.Fatal("redirect was silently followed")
	}
	if target.apiCalls.Load() != 0 {
		t.Fatal("business request escaped the selected origin")
	}
}
func TestCancelledRequestDoesNotPenalizeServer(t *testing.T) {
	server := newSelectionServer(t, 0)
	c := NewClient(server.server.URL)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = c.List(ctx)
	c.selectionMu.Lock()
	failures := c.measurements[c.baseURL].failures
	c.selectionMu.Unlock()
	if failures != 0 {
		t.Fatalf("cancelled request counted as %d failures", failures)
	}
}

func TestDisconnectSelectionBypassesResidenceAndLatencyThresholds(t *testing.T) {
	a := newSelectionServer(t, 30*time.Millisecond)
	b := newSelectionServer(t, 35*time.Millisecond)
	c := NewClient(b.server.URL)
	c.SetServers([]string{a.server.URL})
	c.lastSelection = time.Now()
	c.lastSwitch = time.Now()
	if changed, err := c.SelectAfterDisconnect(context.Background()); err != nil || !changed || c.BaseURL() != a.server.URL {
		t.Fatalf("disconnected client retained slower server: %t %v", changed, err)
	}
}

func TestWebSocketHandshakeFailureSurvivesSuccessfulHTTP(t *testing.T) {
	a := newSelectionServer(t, time.Millisecond)
	b := newSelectionServer(t, 30*time.Millisecond)
	c := NewClient(a.server.URL)
	c.SetServers([]string{b.server.URL})
	ws := NewSelectedWSClient(c, "alice")
	t.Cleanup(ws.Close)
	// This server refuses unregistered WebSockets but serves health and HTTP.
	if err := ws.Connect(context.Background()); err == nil {
		t.Fatal("expected failed WebSocket handshake")
	}
	if _, err := c.List(context.Background()); err != nil {
		t.Fatal(err)
	}
	if changed, err := c.SelectAfterDisconnect(context.Background()); err != nil || !changed || c.BaseURL() != b.server.URL {
		t.Fatalf("healthy HTTP hid failed WebSocket: %t %v", changed, err)
	}
}

func TestSuccessfulWebSocketReconnectClearsHandshakeFailure(t *testing.T) {
	a := newSelectionServer(t, time.Millisecond)
	b := newSelectionServer(t, 30*time.Millisecond)
	c := NewClient(a.server.URL)
	c.SetServers([]string{b.server.URL})
	ws := NewSelectedWSClient(c, "alice")
	t.Cleanup(ws.Close)
	if err := ws.Connect(context.Background()); err == nil {
		t.Fatal("expected failed WebSocket handshake")
	}
	if err := c.Register(context.Background(), &RegisterRequest{ID: "alice"}); err != nil {
		t.Fatal(err)
	}
	if err := ws.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if changed, err := c.SelectFastest(context.Background()); err != nil || changed {
		t.Fatalf("successful reconnect left server excluded: %t %v", changed, err)
	}
}
