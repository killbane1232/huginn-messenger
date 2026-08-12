package messenger

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	hcrypto "github.com/killbane1232/huginn-messenger/internal/crypto"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
)

func TestRegisterRecreatesPeerWhenRefreshCannotFindIt(t *testing.T) {
	signPublic, _, err := hcrypto.GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	_, encPublic, err := hcrypto.GenerateEncryptionKey()
	if err != nil {
		t.Fatal(err)
	}

	var methods []string
	var registered muninn.RegisterRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method)
		switch r.Method {
		case http.MethodPut:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"peer not found"}`))
		case http.MethodPost:
			if err := json.NewDecoder(r.Body).Decode(&registered); err != nil {
				t.Errorf("decode registration: %v", err)
			}
			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	const username = "alice"
	key := username + ":" + hcrypto.EncodeKey(signPublic)
	m := &Messenger{
		ID:            "peer-id",
		Username:      username,
		signPublic:    signPublic,
		encPublic:     encPublic,
		muninnClient:  muninn.NewClient(srv.URL),
		registeredMap: map[string]bool{key: true},
		ctx:           context.Background(),
	}

	if err := m.Register(); err != nil {
		t.Fatal(err)
	}
	if want := []string{http.MethodPut, http.MethodPost}; !reflect.DeepEqual(methods, want) {
		t.Fatalf("request methods = %v, want %v", methods, want)
	}
	if registered.ID != m.ID || registered.Login != username {
		t.Fatalf("registration identity = %+v", registered)
	}
	if registered.EncryptionKey == "" || registered.SignatureKey == "" {
		t.Fatalf("full registration keys are missing: %+v", registered)
	}
	if registered.TTLSeconds != peerTTLSeconds {
		t.Fatalf("registration TTL = %d, want %d", registered.TTLSeconds, peerTTLSeconds)
	}
}

func TestRefreshNotFoundHasTypedError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"peer not found"}`))
	}))
	defer srv.Close()

	err := muninn.NewClient(srv.URL).Refresh(context.Background(), &muninn.RefreshRequest{
		ID: "missing", Login: "alice", SignatureKey: "signature",
	})
	if !errors.Is(err, muninn.ErrPeerNotFound) {
		t.Fatalf("refresh error = %v, want ErrPeerNotFound", err)
	}
}

func TestHeartbeatNotFoundHasTypedError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"peer not found"}`))
	}))
	defer srv.Close()

	err := muninn.NewClient(srv.URL).Heartbeat(context.Background(), "missing", peerTTLSeconds)
	if !errors.Is(err, muninn.ErrPeerNotFound) {
		t.Fatalf("heartbeat error = %v, want ErrPeerNotFound", err)
	}
}
