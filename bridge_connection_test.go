package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/killbane1232/huginn-messenger/internal/messenger"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
)

func TestConnectionEventsFollowWebSocket(t *testing.T) {
	t.Parallel()
	var allowConnection atomic.Bool
	var connection atomic.Pointer[websocket.Conn]
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/api/v1/ws":
			if !allowConnection.Load() {
				http.Error(w, "offline", http.StatusServiceUnavailable)
				return
			}
			conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			connection.Store(conn)
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		default:
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	t.Cleanup(server.Close)
	m, err := messenger.New("alice", muninn.NewClient(server.URL), filepath.Join(t.TempDir(), "huginn.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Shutdown)
	inst := &instance{m: m, events: make(chan Event, 100), done: make(chan struct{})}
	finished := make(chan struct{})
	go func() { inst.eventLoop(); close(finished) }()
	t.Cleanup(func() { close(inst.done); <-finished })

	waitForState := func(want string) {
		t.Helper()
		timer := time.NewTimer(8 * time.Second)
		defer timer.Stop()
		for {
			select {
			case event := <-inst.events:
				if event.Type != "muninn_connection" {
					continue
				}
				var data struct {
					State string `json:"state"`
				}
				if err := json.Unmarshal(event.Data, &data); err != nil {
					t.Fatal(err)
				}
				if data.State != want {
					t.Fatalf("connection state = %q, want %q", data.State, want)
				}
				return
			case <-timer.C:
				t.Fatalf("no %q connection event", want)
			}
		}
	}

	// Healthy HTTP alone must not show a successful signaling connection.
	waitForState("connecting")
	allowConnection.Store(true)
	waitForState("connected")
	allowConnection.Store(false)
	connection.Load().Close()
	waitForState("connecting")
	allowConnection.Store(true)
	waitForState("connected")

	// Opening a screen after connection still gets the current state.
	last := ""
	inst.pushConnectionState(&last)
	waitForState("connected")
}

func TestConnectionEventRetriesFullQueueAndSuppressesDuplicates(t *testing.T) {
	inst := &instance{m: &messenger.Messenger{}, events: make(chan Event, 1)}
	inst.pushEvent("message", json.RawMessage(`{}`))
	last := "connected"
	inst.pushConnectionState(&last)
	if last != "connected" {
		t.Fatal("full queue lost a state change")
	}
	<-inst.events
	inst.pushConnectionState(&last)
	if event := <-inst.events; event.Type != "muninn_connection" || last != "connecting" {
		t.Fatalf("missing retried connection event: %+v, %s", event, last)
	}
	inst.pushConnectionState(&last)
	if len(inst.events) != 0 {
		t.Fatal("unchanged connection emitted a duplicate event")
	}
}
