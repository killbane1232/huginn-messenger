package messenger

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/chunk"
	"github.com/killbane1232/huginn-messenger/internal/crypto"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
)

func TestOfflineRecipientUsesLocalIdentity(t *testing.T) {
	for _, response := range []struct {
		name   string
		status int
		body   string
	}{
		{"expired endpoint", http.StatusNotFound, `{"error":"peer not found"}`},
		{"unavailable directory", http.StatusServiceUnavailable, `{"error":"unavailable"}`},
		{"empty directory", http.StatusOK, `[]`},
		{"null directory", http.StatusOK, `null`},
		{"disconnected directory", 0, ""},
	} {
		for _, cached := range []bool{false, true} {
			source := "database"
			if cached {
				source = "cache"
			}
			t.Run(response.name+"/"+source, func(t *testing.T) {
				var requests atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					requests.Add(1)
					w.WriteHeader(response.status)
					_, _ = w.Write([]byte(response.body))
				}))
				defer srv.Close()
				if response.status == 0 {
					srv.Close()
				}
				sender := newDeliveryTestMessenger(t, srv.URL)
				receiver := newDeliveryTestMessenger(t, srv.URL)
				sender.async = newAsyncPool(sender.ctx, 0, 2)
				lastSeen := time.Now().UTC().Add(-48 * time.Hour)
				if err := sender.store.StorePeer(receiver.Username, receiver.Key, crypto.EncodeKey(receiver.encPublic), crypto.EncodeKey(receiver.signPublic), lastSeen, false); err != nil {
					t.Fatal(err)
				}
				if cached {
					contacts, err := sender.store.GetStoredPeers()
					if err != nil || len(contacts) != 1 {
						t.Fatalf("load contacts: %+v, %v", contacts, err)
					}
					peer := contacts[0].ToMuninnPeer()
					sender.peersMap[peer.Key()] = peer
				}
				file := filepath.Join(t.TempDir(), "offline.txt")
				if err := os.WriteFile(file, []byte("offline attachment"), 0600); err != nil {
					t.Fatal(err)
				}
				for _, paths := range [][]string{nil, {file}} {
					if err := sender.SendMessage(receiver.Key, "offline text", paths, 3600); err != nil {
						t.Fatal(err)
					}
				}
				if requests.Load() != 0 {
					t.Fatal("local acceptance unexpectedly depends on the directory")
				}
				peer := sender.findPeerByKey(receiver.Key)
				if peer == nil || peer.ID != "" || !peer.LastSeen.Equal(lastSeen) || sender.IsPeerOnlineByKey(receiver.Key) {
					t.Fatalf("offline contact was lost or treated as online: %+v", peer)
				}
				reopenFailedMessageStore(t, sender)
				history := sender.GetMessages(receiver.Key)
				if len(history) != 2 {
					t.Fatalf("queued messages did not survive reopen: %+v", history)
				}
				pending, err := sender.store.GetUnplacedChunks()
				if err != nil || len(pending) < 3 {
					t.Fatalf("missing durable text/file payload: %d, %v", len(pending), err)
				}
				for _, message := range history {
					if message.SendError != "" {
						t.Fatalf("offline contact was marked as failed: %+v", message)
					}
					var envelopes []chunk.Envelope
					for _, part := range pending {
						if part.FileID != message.MsgID {
							continue
						}
						if part.RecipientID != receiver.Key {
							t.Fatalf("chunk addressed to endpoint instead of identity: %q", part.RecipientID)
						}
						env, err := chunk.UnmarshalEnvelope(part.Data)
						if err != nil {
							t.Fatal(err)
						}
						envelopes = append(envelopes, env)
					}
					data, err := chunk.AssembleAndDecrypt(envelopes, receiver.encPrivate, receiver.encPublic, sender.signPublic)
					if err != nil {
						t.Fatal(err)
					}
					var payload MessagePayload
					if err := json.Unmarshal(data, &payload); err != nil || payload.Text != "offline text" || len(payload.Files) != len(message.Files) {
						t.Fatalf("invalid offline payload: %+v, %v", payload, err)
					}
					for _, file := range payload.Files {
						var fileEnvelopes []chunk.Envelope
						for _, part := range pending {
							if part.FileID == file.FileID {
								env, err := chunk.UnmarshalEnvelope(part.Data)
								if err != nil {
									t.Fatal(err)
								}
								fileEnvelopes = append(fileEnvelopes, env)
							}
						}
						key, err := crypto.DecodeKey(file.DecryptionKey)
						if err != nil {
							t.Fatal(err)
						}
						contents, err := chunk.AssembleAndDecryptFile(fileEnvelopes, key, sender.signPublic)
						if err != nil || string(contents) != "offline attachment" {
							t.Fatalf("attachment could not be decrypted: %q, %v", contents, err)
						}
					}
				}
			})
		}
	}
}

func TestPeerUpdatesPersistAndRetainKeys(t *testing.T) {
	m := newDeliveryTestMessenger(t, "http://127.0.0.1:1")
	const key = "alice:signature"
	m.upsertPeer("first-endpoint", key, "alice", "first-key", "signature", time.Now(), 120, false)
	m.upsertPeer("second-endpoint", key, "alice", "updated-key", "signature", time.Now(), 120, false)
	m.upsertPeer("", key, "alice", "", "", time.Now(), 0, false)
	peer := m.findPeerByKey(key)
	if peer == nil || peer.ID != "second-endpoint" || peer.EncryptionKey != "updated-key" || peer.SignatureKey != "signature" || len(peer.IDS) != 2 {
		t.Fatalf("partial update discarded contact data: %+v", peer)
	}
	reopenFailedMessageStore(t, m)
	stored, err := m.store.GetStoredPeer(key)
	if err != nil || stored.EncryptionKey != "updated-key" || stored.SignatureKey != "signature" {
		t.Fatalf("updated keys did not persist: %+v, %v", stored, err)
	}
}

func TestOfflineRecipientAfterMessengerRestart(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			_, _ = w.Write([]byte(`{"status":"ok"}`))
			return
		}
		http.Error(w, "peer not found", http.StatusNotFound)
	}))
	defer srv.Close()
	receiver := newDeliveryTestMessenger(t, srv.URL)
	dbPath := filepath.Join(t.TempDir(), "sender.db")
	first, err := New("sender", muninn.NewClient(srv.URL), dbPath, WithICEServers(nil))
	if err != nil {
		t.Fatal(err)
	}
	first.upsertPeer(receiver.ID, receiver.Key, receiver.Username, crypto.EncodeKey(receiver.encPublic), crypto.EncodeKey(receiver.signPublic), time.Now(), 120, false)
	first.Shutdown()
	restarted, err := New("sender", muninn.NewClient(srv.URL), dbPath, WithICEServers(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Shutdown()
	peer := restarted.findPeerByKey(receiver.Key)
	if peer == nil || peer.ID != "" {
		t.Fatalf("test requires a restored contact without endpoint: %+v", peer)
	}
	if err := restarted.SendMessage(receiver.Key, "after restart", nil, 3600); err != nil {
		t.Fatal(err)
	}
	history := restarted.GetMessages(receiver.Key)
	if len(history) != 1 || history[0].SendError != "" {
		t.Fatalf("offline send after restart failed: %+v", history)
	}
}

func TestRetryFailedMessageUsesStoredOfflineRecipient(t *testing.T) {
	m := newDeliveryTestMessenger(t, "http://127.0.0.1:1")
	receiver := newDeliveryTestMessenger(t, "http://127.0.0.1:1")
	m.async = newAsyncPool(m.ctx, 0, 1)
	if err := m.SendMessage(receiver.Key, "retry offline", nil, 3600); err != nil {
		t.Fatal(err)
	}
	failed := m.GetMessages(receiver.Key)[0]
	if failed.SendError == "" {
		t.Fatal("test requires an unknown recipient")
	}
	if err := m.store.StorePeer(receiver.Username, receiver.Key, crypto.EncodeKey(receiver.encPublic), crypto.EncodeKey(receiver.signPublic), time.Now(), false); err != nil {
		t.Fatal(err)
	}
	retried, err := m.RetryMessage(failed.MsgID)
	if err != nil || retried.MsgID != failed.MsgID || retried.SendError != "" {
		t.Fatalf("retry did not resolve the stored contact: %+v, %v", retried, err)
	}
	if history := m.GetMessages(receiver.Key); len(history) != 1 {
		t.Fatalf("retry duplicated message: %+v", history)
	}
}

func TestOfflineSenderCanBeVerifiedFromStoredContact(t *testing.T) {
	receiver := newDeliveryTestMessenger(t, "http://127.0.0.1:1")
	sender := newDeliveryTestMessenger(t, "http://127.0.0.1:1")
	if err := receiver.store.StorePeer(sender.Username, sender.Key, crypto.EncodeKey(sender.encPublic), crypto.EncodeKey(sender.signPublic), time.Now(), false); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(MessagePayload{Text: "sender is offline", Timestamp: time.Now().UTC()})
	envelopes, err := chunk.SplitAndEncrypt("offline-message", sender.ID, "", payload, receiver.encPublic, sender.signPrivate)
	if err != nil {
		t.Fatal(err)
	}
	var records []muninn.ChunkRecord
	for _, env := range envelopes {
		data, err := chunk.MarshalEnvelope(env)
		if err != nil {
			t.Fatal(err)
		}
		if err := receiver.store.StoreChunk(env.MessageID, env.ChunkIndex, data, 3600); err != nil {
			t.Fatal(err)
		}
		records = append(records, muninn.ChunkRecord{FileID: env.MessageID, ChunkIndex: env.ChunkIndex, SenderID: sender.Key, RecipientID: receiver.Key, PeerID: receiver.ID, Hash: chunk.RegisteredHash(data)})
	}
	receiver.collectAndProcessMessage("offline-message", records)
	if history := receiver.GetMessages(sender.Key); len(history) != 1 || history[0].Text != "sender is offline" {
		t.Fatalf("offline sender prevented decryption: %+v", history)
	}
}
