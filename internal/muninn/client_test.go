package muninn

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

func TestGetChunksByRecipientEscapesQueryValue(t *testing.T) {
	const recipientID = "bob:a+b/c="
	const dateFrom = int64(123456)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("recipient_id"); got != recipientID {
			t.Errorf("recipient_id = %q, want %q", got, recipientID)
		}
		if got := r.URL.Query().Get("date_from"); got != strconv.FormatInt(dateFrom, 10) {
			t.Errorf("date_from = %q, want %d", got, dateFrom)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	}))
	defer srv.Close()

	if _, err := NewClient(srv.URL).GetChunksByRecipient(context.Background(), recipientID, dateFrom); err != nil {
		t.Fatal(err)
	}
}

func TestGetAllByKeyEscapesPathAndQueryValues(t *testing.T) {
	const login = "alice/team"
	const signature = "a+b/c="

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/keys/{login}", func(w http.ResponseWriter, r *http.Request) {
		if got := r.PathValue("login"); got != login {
			t.Errorf("login = %q, want %q", got, login)
		}
		if got := r.URL.Query().Get("signature"); got != signature {
			t.Errorf("signature = %q, want %q", got, signature)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	if _, err := NewClient(srv.URL).GetAllByKey(context.Background(), login, signature); err != nil {
		t.Fatal(err)
	}
}

func TestRegisterChunksSendsTTL(t *testing.T) {
	const ttl = 604800

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req RegisterChunkBatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(req.Chunks) != 1 {
			t.Errorf("got %d chunks, want 1", len(req.Chunks))
		} else if req.Chunks[0].TTL != ttl {
			t.Errorf("ttl = %d, want %d", req.Chunks[0].TTL, ttl)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	req := RegisterChunkBatchRequest{Chunks: []RegisterChunkBatchEntry{{
		ChunkIndex:  0,
		SenderID:    "alice:key",
		RecipientID: "bob:key",
		Hash:        "deadbeef",
		Signature:   "signature",
		PeerID:      "holder",
		TTL:         ttl,
	}}}
	if err := NewClient(srv.URL).RegisterChunks(context.Background(), "file-id", req); err != nil {
		t.Fatal(err)
	}
}
