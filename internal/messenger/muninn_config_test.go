package messenger

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/config"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
	"github.com/killbane1232/huginn-messenger/internal/store"
)

func TestMuninnAddressUsesSavedConfigOrExplicitClient(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		name := "saved address"
		if explicit {
			name = "explicit address overrides saved"
		}
		t.Run(name, func(t *testing.T) {
			var registrations atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost && r.URL.Path == "/api/v1/peers" {
					registrations.Add(1)
					w.WriteHeader(http.StatusCreated)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte("[]"))
			}))
			defer srv.Close()
			dbPath := filepath.Join(t.TempDir(), "huginn.db")
			st, err := store.New(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			addr := srv.URL
			var client *muninn.Client
			if explicit {
				addr = "http://127.0.0.1:1"
				client = muninn.NewClient(srv.URL)
			}
			if err := st.SaveAppConfig(&config.Config{Username: "alice", MuninnAddr: addr}); err != nil {
				t.Fatal(err)
			}
			st.Close()
			m, err := New("alice", client, dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer m.Shutdown()
			if m.Config().MuninnAddr != srv.URL {
				t.Fatalf("config address = %q, want %q", m.Config().MuninnAddr, srv.URL)
			}
			if err := m.Register(); err != nil {
				t.Fatal(err)
			}
			if registrations.Load() != 1 {
				t.Fatal("registration did not reach the configured server")
			}
			stored, err := m.store.LoadAppConfig()
			if err != nil || stored.MuninnAddr != srv.URL {
				t.Fatalf("effective address was not persisted: %v", err)
			}
		})
	}
}

func TestMissingMuninnAddressDoesNotUsePublicServer(t *testing.T) {
	m, err := New("alice", nil, filepath.Join(t.TempDir(), "huginn.db"))
	if m != nil {
		m.Shutdown()
		t.Fatal("created a messenger without a configured address")
	}
	if !errors.Is(err, ErrMuninnAddressRequired) {
		t.Fatalf("error = %v, want ErrMuninnAddressRequired", err)
	}
}

func TestRegistrationAdvertisesKnownMuninnServers(t *testing.T) {
	registrations := make(chan muninn.RegisterRequest, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/v1/peers" {
			var advertised muninn.RegisterRequest
			if err := json.NewDecoder(r.Body).Decode(&advertised); err != nil {
				t.Error(err)
			}
			registrations <- advertised
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "huginn.db")
	st, err := store.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SaveAppConfig(&config.Config{Username: "alice", MuninnAddr: "https://previous.example", MuninnServers: []string{"https://seed.example", "https://seed.example", srv.URL}}); err != nil {
		t.Fatal(err)
	}
	st.Close()
	m, err := New("alice", muninn.NewClient(srv.URL), dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown()
	if err := m.Register(); err != nil {
		t.Fatal(err)
	}
	advertised := <-registrations
	want := []string{"https://seed.example", "https://previous.example"}
	if !reflect.DeepEqual(advertised.MuninnServers, want) {
		t.Fatalf("hints = %v, want %v", advertised.MuninnServers, want)
	}
	saved, err := m.store.LoadAppConfig()
	if err != nil || !reflect.DeepEqual(saved.MuninnServers, want) {
		t.Fatalf("saved hints = %v, %v", saved, err)
	}
}

func TestStartupSelectsFastestMuninnAndPersistsChoice(t *testing.T) {
	var slowRegistrations, fastRegistrations atomic.Int32
	server := func(delay time.Duration, registrations *atomic.Int32) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/health" {
				time.Sleep(delay)
				_, _ = w.Write([]byte(`{"status":"ok"}`))
				return
			}
			if r.Method == http.MethodPost && r.URL.Path == "/api/v1/peers" {
				registrations.Add(1)
				w.WriteHeader(201)
				return
			}
			_, _ = w.Write([]byte(`[]`))
		}))
	}
	slow := server(40*time.Millisecond, &slowRegistrations)
	defer slow.Close()
	fast := server(time.Millisecond, &fastRegistrations)
	defer fast.Close()
	m, err := New("alice", muninn.NewClient(slow.URL), filepath.Join(t.TempDir(), "huginn.db"), WithMuninnServers([]string{fast.URL}))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown()
	if m.Config().MuninnAddr != fast.URL {
		t.Fatalf("selected = %s", m.Config().MuninnAddr)
	}
	if err := m.Register(); err != nil {
		t.Fatal(err)
	}
	if slowRegistrations.Load() != 0 || fastRegistrations.Load() != 1 {
		t.Fatal("registration did not exclusively use the winner")
	}
	saved, err := m.store.LoadAppConfig()
	if err != nil || saved.MuninnAddr != fast.URL || len(saved.MuninnServers) != 1 || saved.MuninnServers[0] != slow.URL {
		t.Fatalf("saved selection: %+v %v", saved, err)
	}
	cfg := m.Config()
	cfg.MuninnServers[0] = "https://changed.example"
	if m.Config().MuninnServers[0] != slow.URL {
		t.Fatal("configuration alias escaped")
	}
}
