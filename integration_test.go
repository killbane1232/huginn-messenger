package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/chunk"
	"github.com/killbane1232/huginn-messenger/internal/crypto"
	"github.com/killbane1232/huginn-messenger/internal/messenger"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
)

func TestOfflineMessageWithoutWebRTC(t *testing.T) {
	mn := newTestMuninnServer()
	defer mn.Close()

	mc := muninn.NewClient(mn.URL())

	alice, err := messenger.New("alice", mc, t.TempDir()+"/alice.db")
	if err != nil {
		t.Fatal(err)
	}
	defer alice.Shutdown()
	bob, err := messenger.New("bob", mc, t.TempDir()+"/bob.db")
	if err != nil {
		t.Fatal(err)
	}
	defer bob.Shutdown()
	time.Sleep(300 * time.Millisecond)

	for _, m := range []*messenger.Messenger{alice, bob} {
		if err := m.Register(); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(100 * time.Millisecond)

	msgCh := bob.SubscribeMessages()
	defer bob.UnsubscribeMessages(msgCh)

	err = alice.SendMessage("bob", "hello from alice without webrtc", 604800)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)

	records, err := mc.GetChunksByRecipient(context.Background(), "bob")
	if err != nil {
		t.Fatalf("get chunk records: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("no chunk records found — offline message was not registered on Muninn")
	}

	for _, rec := range records {
		data, ok := alice.StoredChunkData(rec.FileID, rec.ChunkIndex)
		if !ok {
			t.Fatalf("alice does not have chunk %s/%d", rec.FileID, rec.ChunkIndex)
		}
		bob.InjectChunk(rec.FileID, rec.ChunkIndex, data)
	}

	deadline := time.Now().Add(10 * time.Second)
	var delivered messenger.ChatMessage
	gotIt := false
	for time.Now().Before(deadline) {
		select {
		case msg := <-msgCh:
			delivered = msg
			gotIt = true
		default:
		}
		if gotIt {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !gotIt {
		t.Fatal("bob did not receive the offline message")
	}

	if delivered.From != "alice" {
		t.Fatalf("message from wrong sender: got %q, want alice", delivered.From)
	}
	if delivered.Text != "hello from alice without webrtc" {
		t.Fatalf("message text mismatch: got %q", delivered.Text)
	}
	if delivered.MsgID == "" {
		t.Fatal("message id is empty")
	}

	msgs := bob.GetMessages("alice")
	if len(msgs) == 0 {
		t.Fatal("bob.GetMessages(\"alice\") returned no messages")
	}
	found := false
	for _, m := range msgs {
		if m.MsgID == delivered.MsgID {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("delivered message not found in stored messages")
	}

	t.Logf("OK: offline message delivered without WebRTC, id=%s", delivered.MsgID)
}

func TestCryptoRoundtrip(t *testing.T) {
	signPub, signPriv, err := crypto.GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	encPrivA, encPubA, err := crypto.GenerateEncryptionKey()
	if err != nil {
		t.Fatal(err)
	}
	encPrivB, encPubB, err := crypto.GenerateEncryptionKey()
	if err != nil {
		t.Fatal(err)
	}

	keyA, err := crypto.DeriveSharedKey(encPrivA, encPubB)
	if err != nil {
		t.Fatal(err)
	}
	keyB, err := crypto.DeriveSharedKey(encPrivB, encPubA)
	if err != nil {
		t.Fatal(err)
	}
	if len(keyA) != 32 || len(keyB) != 32 {
		t.Fatal("key length mismatch")
	}
	for i := range keyA {
		if keyA[i] != keyB[i] {
			t.Fatal("shared keys do not match")
		}
	}

	plaintext := []byte("hello huginn messenger integration test")
	ciphertext, nonce, err := crypto.EncryptAES(plaintext, keyA)
	if err != nil {
		t.Fatal(err)
	}
	decrypted, err := crypto.DecryptAES(ciphertext, nonce, keyA)
	if err != nil {
		t.Fatal(err)
	}
	if string(decrypted) != string(plaintext) {
		t.Fatal("decrypted text mismatch")
	}

	msg := []byte("test message to sign")
	sig := crypto.Sign(signPriv, msg)
	if !crypto.Verify(signPub, msg, sig) {
		t.Fatal("signature verification failed")
	}
	if crypto.Verify(signPub, []byte("wrong msg"), sig) {
		t.Fatal("signature should not verify wrong message")
	}

	encoded := crypto.EncodeKey(signPub)
	decoded, err := crypto.DecodeKey(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != len(signPub) {
		t.Fatal("key encode/decode length mismatch")
	}
}

func TestChunkRoundtrip(t *testing.T) {
	_, signPriv, err := crypto.GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	signPub := signPriv.Public().(ed25519.PublicKey)

	encPrivB, encPubB, err := crypto.GenerateEncryptionKey()
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("Hello, this is a test message for offline chunk delivery in Huginn!")
	envelopes, err := chunk.SplitAndEncrypt("test-msg-001", "alice", "bob", plaintext, encPubB, signPriv)
	if err != nil {
		t.Fatal(err)
	}
	if len(envelopes) == 0 {
		t.Fatal("no envelopes produced")
	}

	for i, env := range envelopes {
		if env.TotalChunks != len(envelopes) {
			t.Fatalf("envelope %d: invalid TotalChunks", i)
		}
		if env.Ciphertext == "" || env.Nonce == "" || env.EphemeralKey == "" || env.Signature == "" {
			t.Fatalf("envelope %d: missing fields", i)
		}
	}

	data, err := chunk.MarshalEnvelope(envelopes[0])
	if err != nil {
		t.Fatal(err)
	}
	_, err = chunk.UnmarshalEnvelope(data)
	if err != nil {
		t.Fatal(err)
	}

	decrypted, err := chunk.AssembleAndDecrypt(envelopes, encPrivB, encPubB, signPub)
	if err != nil {
		t.Fatal(err)
	}
	if string(decrypted) != string(plaintext) {
		t.Fatalf("decrypted text mismatch")
	}
}

func TestChunkLargeMessage(t *testing.T) {
	_, signPriv, err := crypto.GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	signPub := signPriv.Public().(ed25519.PublicKey)

	encPriv, encPub, err := crypto.GenerateEncryptionKey()
	if err != nil {
		t.Fatal(err)
	}

	plaintext := make([]byte, 5000)
	for i := range plaintext {
		plaintext[i] = byte(i % 256)
	}

	envelopes, err := chunk.SplitAndEncrypt("large-msg", "alice", "bob", plaintext, encPub, signPriv)
	if err != nil {
		t.Fatal(err)
	}
	if len(envelopes) < 2 {
		t.Fatalf("expected multiple chunks for 5000 bytes, got %d", len(envelopes))
	}

	decrypted, err := chunk.AssembleAndDecrypt(envelopes, encPriv, encPub, signPub)
	if err != nil {
		t.Fatal(err)
	}
	if len(decrypted) != len(plaintext) {
		t.Fatalf("decrypted length mismatch: got %d, want %d", len(decrypted), len(plaintext))
	}
	for i := range plaintext {
		if decrypted[i] != plaintext[i] {
			t.Fatalf("byte %d mismatch", i)
		}
	}
}

type testMuninnServer struct {
	mu      sync.Mutex
	peers   map[string]*muninn.Peer
	chunks  []muninn.ChunkRecord
	signals map[string][]muninn.Signal
	srv     *httptest.Server
}

func newTestMuninnServer() *testMuninnServer {
	ts := &testMuninnServer{
		peers:   make(map[string]*muninn.Peer),
		chunks:  nil,
		signals: make(map[string][]muninn.Signal),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/peers", ts.handleRegister)
	mux.HandleFunc("GET /api/v1/peers", ts.handleList)
	mux.HandleFunc("GET /api/v1/peers/best", ts.handleBestPeers)
	mux.HandleFunc("GET /api/v1/peers/{id}", ts.handleGet)
	mux.HandleFunc("GET /api/v1/peers/best/thick", ts.handleBestThickPeers)
	mux.HandleFunc("DELETE /api/v1/peers/{id}", ts.handleDelete)
	mux.HandleFunc("POST /api/v1/peers/{id}/heartbeat", ts.handleHeartbeat)
	mux.HandleFunc("PUT /api/v1/files/{fileID}/chunks/{chunkIndex}", ts.handleRegisterChunk)
	mux.HandleFunc("POST /api/v1/files/{fileID}/chunks", ts.handleRegisterChunks)
	mux.HandleFunc("GET /api/v1/files/{fileID}/chunks", ts.handleGetChunksByFileID)
	mux.HandleFunc("GET /api/v1/recipient/{recipientID}/chunks", ts.handleGetChunks)
	mux.HandleFunc("POST /api/v1/peers/{sourcePeerID}/chunk-reports", ts.handleReportChunk)
	mux.HandleFunc("POST /api/v1/peers/{peerID}/signals", ts.handleSendSignal)
	mux.HandleFunc("GET /api/v1/peers/{peerID}/signals", ts.handlePollSignals)
	mux.HandleFunc("DELETE /api/v1/recipient/{recipientID}/chunks/{fileID}", ts.handleDeleteChunksByRecipient)
	ts.srv = httptest.NewServer(mux)
	return ts
}

func (ts *testMuninnServer) URL() string { return ts.srv.URL }
func (ts *testMuninnServer) Close()      { ts.srv.Close() }

func (ts *testMuninnServer) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req muninn.RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ts.mu.Lock()
	ts.peers[req.ID] = &muninn.Peer{
		ID:            req.ID,
		Addresses:     req.Addresses,
		EncryptionKey: req.EncryptionKey,
		SignatureKey:  req.SignatureKey,
		Metadata:      req.Metadata,
		LastSeen:      time.Now(),
		TTLSeconds:    req.TTLSeconds,
		QualityScore:  100,
	}
	ts.mu.Unlock()
	w.WriteHeader(http.StatusCreated)
}

func (ts *testMuninnServer) handleList(w http.ResponseWriter, r *http.Request) {
	ts.mu.Lock()
	peers := make([]muninn.Peer, 0, len(ts.peers))
	for _, p := range ts.peers {
		peers = append(peers, *p)
	}
	ts.mu.Unlock()
	json.NewEncoder(w).Encode(peers)
}

func (ts *testMuninnServer) handleBestPeers(w http.ResponseWriter, r *http.Request) {
	ts.mu.Lock()
	peers := make([]muninn.Peer, 0, len(ts.peers))
	for _, p := range ts.peers {
		peers = append(peers, *p)
	}
	ts.mu.Unlock()
	json.NewEncoder(w).Encode(peers)
}

func (ts *testMuninnServer) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ts.mu.Lock()
	p, ok := ts.peers[id]
	ts.mu.Unlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	json.NewEncoder(w).Encode(p)
}

func (ts *testMuninnServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ts.mu.Lock()
	delete(ts.peers, id)
	ts.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (ts *testMuninnServer) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (ts *testMuninnServer) handleRegisterChunk(w http.ResponseWriter, r *http.Request) {
	fileID := r.PathValue("fileID")
	chunkIndex, _ := strconv.Atoi(r.PathValue("chunkIndex"))
	var req muninn.RegisterChunkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ts.mu.Lock()
	ts.chunks = append(ts.chunks, muninn.ChunkRecord{
		FileID:      fileID,
		ChunkIndex:  chunkIndex,
		SenderID:    req.SenderID,
		RecipientID: req.RecipientID,
		Hash:        req.Hash,
		PeerID:      req.PeerID,
	})
	ts.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (ts *testMuninnServer) handleBestThickPeers(w http.ResponseWriter, r *http.Request) {
	ts.mu.Lock()
	peers := make([]muninn.Peer, 0, len(ts.peers))
	for _, p := range ts.peers {
		peers = append(peers, *p)
	}
	ts.mu.Unlock()
	json.NewEncoder(w).Encode(peers)
}

func (ts *testMuninnServer) handleRegisterChunks(w http.ResponseWriter, r *http.Request) {
	fileID := r.PathValue("fileID")
	var req muninn.RegisterChunkBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ts.mu.Lock()
	for _, entry := range req.Chunks {
		ts.chunks = append(ts.chunks, muninn.ChunkRecord{
			FileID:      fileID,
			ChunkIndex:  entry.ChunkIndex,
			SenderID:    entry.SenderID,
			RecipientID: entry.RecipientID,
			Hash:        entry.Hash,
			PeerID:      entry.PeerID,
			Persist:     entry.Persist,
		})
	}
	ts.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (ts *testMuninnServer) handleGetChunks(w http.ResponseWriter, r *http.Request) {
	recipientID := r.PathValue("recipientID")
	ts.mu.Lock()
	var records []muninn.ChunkRecord
	for _, c := range ts.chunks {
		if c.RecipientID == recipientID {
			records = append(records, c)
		}
	}
	ts.mu.Unlock()
	if records == nil {
		records = []muninn.ChunkRecord{}
	}
	json.NewEncoder(w).Encode(records)
}

func (ts *testMuninnServer) handleGetChunksByFileID(w http.ResponseWriter, r *http.Request) {
	fileID := r.PathValue("fileID")
	ts.mu.Lock()
	var records []muninn.ChunkRecord
	for _, c := range ts.chunks {
		if c.FileID == fileID {
			records = append(records, c)
		}
	}
	ts.mu.Unlock()
	if records == nil {
		records = []muninn.ChunkRecord{}
	}
	json.NewEncoder(w).Encode(records)
}

func (ts *testMuninnServer) handleReportChunk(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (ts *testMuninnServer) handleDeleteChunksByRecipient(w http.ResponseWriter, r *http.Request) {
	recipientID := r.PathValue("recipientID")
	fileID := r.PathValue("fileID")
	ts.mu.Lock()
	var kept []muninn.ChunkRecord
	for _, rec := range ts.chunks {
		if rec.RecipientID == recipientID && rec.FileID == fileID {
			continue
		}
		kept = append(kept, rec)
	}
	ts.chunks = kept
	ts.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (ts *testMuninnServer) handleSendSignal(w http.ResponseWriter, r *http.Request) {
	peerID := r.PathValue("peerID")
	var sig muninn.Signal
	if err := json.NewDecoder(r.Body).Decode(&sig); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ts.mu.Lock()
	ts.signals[peerID] = append(ts.signals[peerID], sig)
	ts.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (ts *testMuninnServer) handlePollSignals(w http.ResponseWriter, r *http.Request) {
	peerID := r.PathValue("peerID")
	ts.mu.Lock()
	sigs := ts.signals[peerID]
	delete(ts.signals, peerID)
	ts.mu.Unlock()
	if sigs == nil {
		sigs = []muninn.Signal{}
	}
	json.NewEncoder(w).Encode(sigs)
}

func TestMessengerOfflineFlow(t *testing.T) {
	mn := newTestMuninnServer()
	defer mn.Close()

	mc := muninn.NewClient(mn.URL())

	alice, err := messenger.New("alice", mc, t.TempDir()+"/alice.db")
	if err != nil {
		t.Fatal(err)
	}
	defer alice.Shutdown()
	bob, err := messenger.New("bob", mc, t.TempDir()+"/bob.db")
	if err != nil {
		t.Fatal(err)
	}
	defer bob.Shutdown()
	time.Sleep(300 * time.Millisecond)

	if err := alice.Register(); err != nil {
		t.Fatal(err)
	}
	if err := bob.Register(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	t.Logf("alice peers: %d, bob peers: %d", len(alice.GetPeers()), len(bob.GetPeers()))

	err = alice.SendMessage("bob", "hello from alice via offline chunks", 604800)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("message sent, injecting chunks into bob...")

	time.Sleep(2 * time.Second)

	records, err := mc.GetChunksByRecipient(context.Background(), "bob")
	if err != nil {
		t.Fatalf("get chunk records: %v", err)
	}
	t.Logf("found %d chunk records", len(records))

	for _, rec := range records {
		data, ok := alice.StoredChunkData(rec.FileID, rec.ChunkIndex)
		if !ok {
			t.Fatalf("alice does not have chunk %s/%d", rec.FileID, rec.ChunkIndex)
		}
		bob.InjectChunk(rec.FileID, rec.ChunkIndex, data)
		t.Logf("injected chunk %s/%d from alice to bob", rec.FileID, rec.ChunkIndex)
	}

	deadline := time.Now().Add(10 * time.Second)
	var lastMsg messenger.ChatMessage
	found := false
	for time.Now().Before(deadline) {
		msgs := bob.GetMessages("alice")
		if len(msgs) > 0 {
			lastMsg = msgs[len(msgs)-1]
			found = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	if !found {
		aliceMsgs := alice.GetMessages("bob")
		t.Logf("alice->bob msgs: %d", len(aliceMsgs))
		for _, m := range aliceMsgs {
			t.Logf("  alice stored: %+v", m)
		}
		t.Fatal("bob did not receive the offline message after chunk injection")
	}

	if lastMsg.Text != "hello from alice via offline chunks" {
		t.Fatalf("message text mismatch: got %q", lastMsg.Text)
	}
	if lastMsg.MsgID == "" {
		t.Fatal("message id is empty")
	}
	t.Logf("OK: offline message delivered, id=%s", lastMsg.MsgID)
}

func TestThreeUserOfflineWithStoragePeer(t *testing.T) {
	mn := newTestMuninnServer()
	defer mn.Close()

	mc := muninn.NewClient(mn.URL())

	alice, err := messenger.New("alice", mc, t.TempDir()+"/alice.db")
	if err != nil {
		t.Fatal(err)
	}
	defer alice.Shutdown()
	bob, err := messenger.New("bob", mc, t.TempDir()+"/bob.db")
	if err != nil {
		t.Fatal(err)
	}
	defer bob.Shutdown()
	charley, err := messenger.New("charley", mc, t.TempDir()+"/charley.db")
	if err != nil {
		t.Fatal(err)
	}
	defer charley.Shutdown()

	time.Sleep(300 * time.Millisecond)

	for _, m := range []*messenger.Messenger{alice, bob, charley} {
		if err := m.Register(); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(100 * time.Millisecond)

	time.Sleep(200 * time.Millisecond)
	t.Logf("alice peers: %d, bob peers: %d, charley peers: %d",
		len(alice.GetPeers()), len(bob.GetPeers()), len(charley.GetPeers()))

	err = alice.SendMessage("bob", "hello from alice via charley storage", 604800)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("message sent, waiting for chunk records...")

	time.Sleep(2 * time.Second)

	records, err := mc.GetChunksByRecipient(context.Background(), "bob")
	if err != nil {
		t.Fatalf("get chunk records: %v", err)
	}
	t.Logf("found %d chunk records (registered by alice)", len(records))

	if len(records) == 0 {
		t.Fatal("no chunk records found")
	}

	for _, rec := range records {
		data, ok := alice.StoredChunkData(rec.FileID, rec.ChunkIndex)
		if !ok {
			t.Fatalf("alice does not have chunk %s/%d", rec.FileID, rec.ChunkIndex)
		}
		charley.InjectChunk(rec.FileID, rec.ChunkIndex, data)
		t.Logf("injected chunk %s/%d from alice -> charley", rec.FileID, rec.ChunkIndex)

		chunkHash := chunk.RegisteredHash(data)
		body := fmt.Sprintf(`{"sender_id":"alice","recipient_id":"bob","hash":"%s","signature":"","peer_id":"charley"}`, chunkHash)
		url := fmt.Sprintf("%s/api/v1/files/%s/chunks/%d", mn.URL(), rec.FileID, rec.ChunkIndex)
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(body)))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		t.Logf("registered charley as storage peer for chunk %s/%d", rec.FileID, rec.ChunkIndex)
	}

	records2, err := mc.GetChunksByRecipient(context.Background(), "bob")
	if err != nil {
		t.Fatalf("get chunk records: %v", err)
	}
	t.Logf("found %d chunk records after charley registration", len(records2))

	charleyRecords := 0
	for _, rec := range records2 {
		if rec.PeerID == "charley" {
			charleyRecords++
			data, ok := charley.StoredChunkData(rec.FileID, rec.ChunkIndex)
			if !ok {
				t.Fatalf("charley does not have chunk %s/%d", rec.FileID, rec.ChunkIndex)
			}
			bob.InjectChunk(rec.FileID, rec.ChunkIndex, data)
			t.Logf("injected chunk %s/%d from charley -> bob", rec.FileID, rec.ChunkIndex)
		}
	}
	if charleyRecords == 0 {
		t.Fatal("no chunk records with peer_id=charley found")
	}

	deadline := time.Now().Add(10 * time.Second)
	var lastMsg messenger.ChatMessage
	found := false
	for time.Now().Before(deadline) {
		msgs := bob.GetMessages("alice")
		if len(msgs) > 0 {
			lastMsg = msgs[len(msgs)-1]
			found = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	if !found {
		aliceMsgs := alice.GetMessages("bob")
		t.Logf("alice->bob msgs: %d", len(aliceMsgs))
		for _, m := range aliceMsgs {
			t.Logf("  alice stored: %+v", m)
		}
		t.Fatal("bob did not receive the offline message via charley")
	}

	if lastMsg.Text != "hello from alice via charley storage" {
		t.Fatalf("message text mismatch: got %q", lastMsg.Text)
	}
	if lastMsg.MsgID == "" {
		t.Fatal("message id is empty")
	}
	t.Logf("OK: offline message delivered via charley, id=%s", lastMsg.MsgID)
}

func TestFileSendAndReceive(t *testing.T) {
	mn := newTestMuninnServer()
	defer mn.Close()

	mc := muninn.NewClient(mn.URL())

	alice, err := messenger.New("alice", mc, t.TempDir()+"/alice.db")
	if err != nil {
		t.Fatal(err)
	}
	defer alice.Shutdown()
	bob, err := messenger.New("bob", mc, t.TempDir()+"/bob.db")
	if err != nil {
		t.Fatal(err)
	}
	defer bob.Shutdown()
	time.Sleep(300 * time.Millisecond)

	for _, m := range []*messenger.Messenger{alice, bob} {
		if err := m.Register(); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(100 * time.Millisecond)

	msgCh := bob.SubscribeMessages()
	defer bob.UnsubscribeMessages(msgCh)

	content := "hello this is a test file from alice"
	tmpFile := filepath.Join(t.TempDir(), "test.txt")
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	err = alice.SendMessageWithFiles("bob", "here is a file", []string{tmpFile}, 604800)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)

	records, err := mc.GetChunksByRecipient(context.Background(), "bob")
	if err != nil {
		t.Fatalf("get chunk records: %v", err)
	}
	t.Logf("found %d message chunk records", len(records))
	if len(records) == 0 {
		t.Fatal("no message chunk records found")
	}

	for _, rec := range records {
		data, ok := alice.StoredChunkData(rec.FileID, rec.ChunkIndex)
		if !ok {
			t.Fatalf("alice does not have chunk %s/%d", rec.FileID, rec.ChunkIndex)
		}
		bob.InjectChunk(rec.FileID, rec.ChunkIndex, data)
		t.Logf("injected message chunk %s/%d", rec.FileID, rec.ChunkIndex)
	}

	mn.mu.Lock()
	var fileRecords []muninn.ChunkRecord
	for _, c := range mn.chunks {
		if c.Persist {
			fileRecords = append(fileRecords, c)
		}
	}
	mn.mu.Unlock()
	t.Logf("found %d file chunk records", len(fileRecords))
	if len(fileRecords) == 0 {
		t.Fatal("no file chunk records found (persist=true)")
	}

	for _, rec := range fileRecords {
		data, ok := alice.StoredChunkData(rec.FileID, rec.ChunkIndex)
		if !ok {
			t.Fatalf("alice does not have file chunk %s/%d", rec.FileID, rec.ChunkIndex)
		}
		bob.InjectChunk(rec.FileID, rec.ChunkIndex, data)
		t.Logf("injected file chunk %s/%d", rec.FileID, rec.ChunkIndex)
	}

	deadline := time.Now().Add(10 * time.Second)
	var delivered messenger.ChatMessage
	gotMsg := false
	for time.Now().Before(deadline) {
		select {
		case msg := <-msgCh:
			delivered = msg
			gotMsg = true
		default:
		}
		if gotMsg {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !gotMsg {
		t.Fatal("bob did not receive the message with file")
	}
	if delivered.Text != "here is a file" {
		t.Fatalf("message text mismatch: got %q", delivered.Text)
	}

	msgs := bob.GetMessages("alice")
	var foundMsg messenger.ChatMessage
	for _, m := range msgs {
		if m.MsgID == delivered.MsgID {
			foundMsg = m
			break
		}
	}
	if foundMsg.MsgID == "" {
		t.Fatal("delivered message not found in stored messages")
	}

	time.Sleep(2 * time.Second)

	entries, err := os.ReadDir(bob.DownloadsDir())
	if err != nil {
		t.Fatalf("read downloads dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("no files in downloads directory")
	}
	downloadedPath := filepath.Join(bob.DownloadsDir(), entries[0].Name())
	downloaded, err := os.ReadFile(downloadedPath)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if string(downloaded) != content {
		t.Fatalf("file content mismatch: got %q, want %q", string(downloaded), content)
	}
	t.Logf("OK: file sent and received, msgID=%s, file=%s, content=%q", delivered.MsgID, entries[0].Name(), content)
}

func TestWebRTCOfflineFallback(t *testing.T) {
	mn := newTestMuninnServer()
	defer mn.Close()

	mc := muninn.NewClient(mn.URL())

	alice, err := messenger.New("alice", mc, t.TempDir()+"/alice.db",
		messenger.WithICEServers(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer alice.Shutdown()
	bob, err := messenger.New("bob", mc, t.TempDir()+"/bob.db",
		messenger.WithICEServers(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer bob.Shutdown()
	time.Sleep(300 * time.Millisecond)

	for _, m := range []*messenger.Messenger{alice, bob} {
		if err := m.Register(); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(100 * time.Millisecond)

	alice.SearchPeers("bob")
	bob.SearchPeers("alice")
	time.Sleep(100 * time.Millisecond)

	msgCh := bob.SubscribeMessages()
	defer bob.UnsubscribeMessages(msgCh)

	alice.ConnectPeer("bob")

	deadline := time.Now().Add(5 * time.Second)
	connected := false
	for time.Now().Before(deadline) {
		if alice.IsPeerConnected("bob") && bob.IsPeerConnected("alice") {
			connected = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !connected {
		t.Fatal("WebRTC connection not established between alice and bob within 5s")
	}
	t.Log("WebRTC connection established, waiting for data channel to open...")
	time.Sleep(2 * time.Second)

	err = alice.SendMessage("bob", "hello via webrtc", 604800)
	if err != nil {
		t.Fatal(err)
	}

	deadline = time.Now().Add(10 * time.Second)
	var delivered messenger.ChatMessage
	gotViaWebRTC := false
	for time.Now().Before(deadline) {
		select {
		case msg := <-msgCh:
			delivered = msg
			gotViaWebRTC = true
		default:
		}
		if gotViaWebRTC {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !gotViaWebRTC {
		t.Log("message not received via WebRTC instantly, checking offline fallback...")
		time.Sleep(2 * time.Second)

		records, err := mc.GetChunksByRecipient(context.Background(), "bob")
		if err == nil && len(records) > 0 {
			t.Logf("found %d chunk records, injecting...", len(records))
			for _, rec := range records {
				data, ok := alice.StoredChunkData(rec.FileID, rec.ChunkIndex)
				if !ok {
					t.Fatalf("alice does not have chunk %s/%d", rec.FileID, rec.ChunkIndex)
				}
				bob.InjectChunk(rec.FileID, rec.ChunkIndex, data)
			}

			deadline = time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				select {
				case msg := <-msgCh:
					delivered = msg
					gotViaWebRTC = true
				default:
				}
				if gotViaWebRTC {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	if !gotViaWebRTC {
		t.Fatal("bob did not receive the message via either WebRTC or offline fallback")
	}

	if delivered.From != "alice" {
		t.Fatalf("message from wrong sender: got %q, want alice", delivered.From)
	}
	if delivered.Text != "hello via webrtc" {
		t.Fatalf("message text mismatch: got %q", delivered.Text)
	}
	if delivered.MsgID == "" {
		t.Fatal("message id is empty")
	}

	msgs := bob.GetMessages("alice")
	found := false
	for _, m := range msgs {
		if m.MsgID == delivered.MsgID {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("delivered message not found in bob.GetMessages(\"alice\")")
	}

	via := "WebRTC"
	if !alice.IsPeerConnected("bob") {
		via = "offline fallback"
	}
	t.Logf("OK: message delivered via %s, id=%s", via, delivered.MsgID)
}
