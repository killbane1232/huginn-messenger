package messenger

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/killbane1232/huginn-messenger/internal/crypto"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
	"github.com/killbane1232/huginn-messenger/internal/store"
	"github.com/killbane1232/huginn-messenger/internal/webrtc"
)

func TestHandleChunkStoreReportsActualSourcePeer(t *testing.T) {
	const sourcePeerID = "source-peer-id"
	const fileID = "file-id"
	const chunkHash = "deadbeef01234567"

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/peers/{id}/chunk-reports", func(w http.ResponseWriter, r *http.Request) {
		if got := r.PathValue("id"); got != sourcePeerID {
			t.Errorf("source peer id = %q, want %q", got, sourcePeerID)
		}

		var report muninn.ChunkReportRequest
		if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
			t.Errorf("decode report: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		signature, err := base64.StdEncoding.DecodeString(report.Signature)
		if err != nil {
			t.Errorf("decode signature: %v", err)
		}
		payload := []byte("muninn/reported/v1\n" + fileID + "\n0\n" + chunkHash + "\n" + sourcePeerID)
		if !crypto.Verify(publicKey, payload, signature) {
			t.Error("report signature does not cover the actual source peer id")
		}
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	chunkStore, err := store.New(t.TempDir() + "/chunks.db")
	if err != nil {
		t.Fatal(err)
	}
	defer chunkStore.Close()

	m := &Messenger{
		ID:           "storage-peer-id",
		Key:          "storage:key",
		signPrivate:  privateKey,
		muninnClient: muninn.NewClient(srv.URL),
		store:        chunkStore,
		ctx:          context.Background(),
	}
	m.handleChunkStore(sourcePeerID, webrtc.ChunkStoreRequest{
		FileID:      fileID,
		ChunkIndex:  0,
		Data:        []byte("encrypted chunk"),
		SenderID:    "alice:signature-key",
		RecipientID: "bob:signature-key",
		Hash:        chunkHash,
		TTLSeconds:  604800,
	})

	if _, err := chunkStore.GetChunk(fileID, 0); err != nil {
		t.Fatalf("chunk was not stored after a valid report: %v", err)
	}
}
