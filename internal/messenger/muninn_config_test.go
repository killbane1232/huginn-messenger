package messenger

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

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
