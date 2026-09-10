package messenger

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/chunk"
	"github.com/killbane1232/huginn-messenger/internal/config"
	"github.com/killbane1232/huginn-messenger/internal/crypto"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
	"github.com/killbane1232/huginn-messenger/internal/store"
	"github.com/killbane1232/huginn-messenger/internal/webrtc"
)

func newDeliveryTestMessenger(t *testing.T, endpoint string) *Messenger {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := store.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	pub, priv, err := crypto.GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	encPriv, encPub, err := crypto.GenerateEncryptionKey()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Messenger{ID: "receiver-endpoint", Username: "receiver", Key: "receiver:" + crypto.EncodeKey(pub),
		store: st, appConfig: &config.Config{DBPath: dbPath}, ctx: ctx, cancel: cancel, muninnClient: muninn.NewClient(endpoint),
		signPrivate: priv, signPublic: pub, encPrivate: encPriv, encPublic: encPub,
		peersMap: make(map[string]muninn.Peer), processingMsg: make(map[string]bool)}
	t.Cleanup(func() { cancel(); m.store.Close() })
	return m
}

func TestDeliveryCleanupMustRetainUndeliveredPayload(t *testing.T) {
	m := newDeliveryTestMessenger(t, "http://127.0.0.1:1")
	const id = "undelivered-message"
	if err := m.store.StoreChunk(id, 0, []byte("encrypted payload"), 604800); err != nil {
		t.Fatal(err)
	}
	if err := m.store.StorePendingChunk(&store.PendingChunk{FileID: id, Data: []byte("encrypted payload"), CreatedAt: time.Now(), TTLSeconds: 604800}); err != nil {
		t.Fatal(err)
	}
	if err := m.store.SaveMessage(id, "peer", "receiver", "peer", []byte(`{}`), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := m.store.DeleteChunksWithMessage(); err != nil {
		t.Fatal(err)
	}
	pending, err := m.store.GetUnplacedChunks()
	if err != nil || len(pending) != 1 {
		t.Fatal("test requires an unplaced outgoing chunk")
	}
	data, ok := m.handleChunkGet("recipient", webrtc.ChunkGetRequest{FileID: id, ChunkIndex: 0})
	if !ok || len(data) == 0 {
		t.Fatal("housekeeping deleted the sender's undelivered payload while pending_chunks still contains it")
	}
}

func TestDeliveryRecoverMuninnWithoutStoragePeers(t *testing.T) {
	var offline atomic.Bool
	offline.Store(true)
	var registrations atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/chunks") {
			registrations.Add(1)
			if offline.Load() {
				w.WriteHeader(503)
			} else {
				w.WriteHeader(204)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[]`))
	}))
	defer srv.Close()
	m := newDeliveryTestMessenger(t, srv.URL)
	_, recipientKey, err := crypto.GenerateEncryptionKey()
	if err != nil {
		t.Fatal(err)
	}
	recipient := &muninn.Peer{ID: "recipient", Login: "recipient", SignatureKey: "signature", EncryptionKey: crypto.EncodeKey(recipientKey)}
	m.peersMap[recipient.Key()] = *recipient
	outgoing, err := m.prepareOutgoing(recipient.Key(), "payload", nil, 604800)
	if err != nil {
		t.Fatal(err)
	}
	m.distributeChunks(outgoing.chunks)
	if registrations.Load() != 1 {
		t.Fatal("initial registration was not attempted")
	}
	offline.Store(false)
	m.store.Close()
	m.store, err = store.New(m.appConfig.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	m.distributePendingChunks()
	if registrations.Load() == 1 {
		t.Fatal("restored Muninn is never told about pending chunks on the sender when no third-party storage peer exists")
	}
}

func TestDeliveryFileChunksNeedPersistentRetry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer srv.Close()
	m := newDeliveryTestMessenger(t, srv.URL)
	path := filepath.Join(t.TempDir(), "attachment.txt")
	if err := os.WriteFile(path, []byte("file payload"), 0600); err != nil {
		t.Fatal(err)
	}
	_, recipientKey, err := crypto.GenerateEncryptionKey()
	if err != nil {
		t.Fatal(err)
	}
	recipient := muninn.Peer{ID: "recipient", Login: "recipient", SignatureKey: "signature", EncryptionKey: crypto.EncodeKey(recipientKey)}
	m.peersMap[recipient.Key()] = recipient
	outgoing, err := m.prepareOutgoing(recipient.Key(), "attachment", []string{path}, 604800)
	if err != nil {
		t.Fatal(err)
	}
	m.distributeChunks(outgoing.chunks)
	meta := outgoing.message.Files[0]
	chunks, err := m.store.ListChunks(meta.FileID)
	if err != nil || len(chunks) == 0 {
		t.Fatal("file data was not encrypted locally")
	}
	pending, err := m.store.GetUnplacedChunks()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending chunks=%d, want file plus metadata", len(pending))
	}
	fileQueued := false
	for _, c := range pending {
		if c.FileID == meta.FileID {
			fileQueued = c.Persist
		}
	}
	if !fileQueued {
		t.Fatal("file retry lost persist flag")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	m.store.Close()
	m.store, err = store.New(m.appConfig.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := m.store.GetUnplacedChunks()
	if err != nil || len(restored) != 2 {
		t.Fatalf("retry queue lost on restart: %v", err)
	}
}

func TestDeliveryCursorMustRetryTemporarilyUnknownSender(t *testing.T) {
	var records []muninn.ChunkRecord
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(r.URL.Path, "/api/v1/files/") {
			result := []muninn.ChunkRecord{}
			for _, record := range records {
				if record.FileID == "old-message" {
					result = append(result, record)
				}
			}
			json.NewEncoder(w).Encode(result)
			return
		}
		if r.URL.Path == "/api/v1/recipient/chunks" {
			from, _ := strconv.ParseInt(r.URL.Query().Get("date_from"), 10, 64)
			result := []muninn.ChunkRecord{}
			for _, record := range records {
				if record.UpdatedAt >= from {
					result = append(result, record)
				}
			}
			json.NewEncoder(w).Encode(result)
			return
		}
		w.Write([]byte(`[]`))
	}))
	defer srv.Close()
	m := newDeliveryTestMessenger(t, srv.URL)
	senderPub, senderPriv, err := crypto.GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := muninn.Peer{ID: "sender-endpoint", Login: "sender", SignatureKey: crypto.EncodeKey(senderPub)}
	payload, _ := json.Marshal(MessagePayload{Text: "valid offline message", Timestamp: time.Now().UTC()})
	envelopes, err := chunk.SplitAndEncrypt("old-message", sender.ID, m.ID, payload, m.encPublic, senderPriv)
	if err != nil {
		t.Fatal(err)
	}
	for i, envelope := range envelopes {
		data, err := chunk.MarshalEnvelope(envelope)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.store.StoreChunk("old-message", i, data, 604800); err != nil {
			t.Fatal(err)
		}
		records = append(records, muninn.ChunkRecord{FileID: "old-message", ChunkIndex: i, SenderID: sender.Key(), RecipientID: m.Key, PeerID: m.ID, Hash: chunk.RegisteredHash(data), CreatedAt: 100, UpdatedAt: 100, TTL: 604800})
	}

	records = append(records, muninn.ChunkRecord{FileID: "already-received", CreatedAt: 200, UpdatedAt: 300})
	if err := m.store.SaveMessage("already-received", sender.Key(), sender.Login, sender.Key(), []byte(`{}`), time.Now()); err != nil {
		t.Fatal(err)
	}
	m.checkRecipientMessages(m.Key)
	if got := m.store.GetLastChunkCheck(m.Key); got != 300 {
		t.Fatalf("cursor=%d", got)
	}
	failed, _ := m.store.ListFailedChunks(m.Key)
	if len(failed) == 0 {
		t.Fatal("sender lookup failure lost persistent retry state")
	}
	m.store.Close()
	m.store, err = store.New(m.appConfig.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	m.peersMap[sender.Key()] = sender
	m.checkRecipientMessages(m.Key)
	present, _ := m.store.FindMessageById("old-message")
	if !present {
		t.Fatal("complete message was not retried after sender lookup recovered")
	}
	failed, _ = m.store.ListFailedChunks(m.Key)
	if len(failed) != 0 {
		t.Fatal("delivered message still has retry entries")
	}
}

func TestDeliveryStorageMustRetainDataOnReportFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer srv.Close()
	m := newDeliveryTestMessenger(t, srv.URL)
	m.handleChunkStore("sender-endpoint", webrtc.ChunkStoreRequest{FileID: "advertised-message", ChunkIndex: 0, Data: []byte("encrypted payload"), SenderID: "sender:key", RecipientID: "recipient:key", Hash: chunk.RegisteredHash([]byte("encrypted payload")), TTLSeconds: 604800})
	data, err := m.store.GetChunk("advertised-message", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("storage peer drops successfully received bytes on a transient Muninn report failure; there is no negative ACK to the sender")
	}
}

func TestDeliveryInvalidSendNeedsAnObservableFailure(t *testing.T) {
	m := newDeliveryTestMessenger(t, "http://127.0.0.1:1")
	m.async = newAsyncPool(m.ctx, 1, 4)
	t.Cleanup(func() { m.cancel(); m.async.wait() })
	events := m.SubscribeMessages()
	defer m.UnsubscribeMessages(events)
	err := m.SendMessage("unknown-recipient", "message that will vanish", nil, 604800)
	done := make(chan struct{})
	m.async.submit(func() { close(done) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("send worker did not finish")
	}
	pending, pendingErr := m.store.GetUnplacedChunks()
	if pendingErr != nil {
		t.Fatal(pendingErr)
	}
	if err == nil && len(events) == 0 && len(pending) == 0 {
		t.Fatal("SendMessage reports success, but failed worker emits no event and keeps no retry entry")
	}
}
