package main

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/chunk"
	"github.com/killbane1232/huginn-messenger/internal/crypto"
	"github.com/killbane1232/huginn-messenger/internal/messenger"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
	"github.com/killbane1232/huginn-messenger/internal/p2p"
)

func freePort() int {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0
	}
	p := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return p
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

	_, encPubB, err := crypto.GenerateEncryptionKey()
	if err != nil {
		t.Fatal(err)
	}
	encPrivB, _, err := crypto.GenerateEncryptionKey()
	if err != nil {
		t.Fatal(err)
	}

	aesKey, err := crypto.DeriveSharedKey(encPrivB, encPubB)
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("Hello, this is a test message for offline chunk delivery in Huginn!")
	envelopes, err := chunk.SplitAndEncrypt("test-msg-001", "alice", "bob", plaintext, aesKey, signPriv)
	if err != nil {
		t.Fatal(err)
	}
	if len(envelopes) == 0 {
		t.Fatal("no envelopes produced")
	}

	for i, env := range envelopes {
		if env.TotalChunks != len(envelopes) || env.ChunkIndex != i {
			t.Fatalf("envelope %d: invalid TotalChunks or ChunkIndex", i)
		}
		if env.Ciphertext == "" || env.Nonce == "" || env.Signature == "" {
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

	decrypted, err := chunk.AssembleAndDecrypt(envelopes, aesKey, signPub)
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
	aesKey, err := crypto.DeriveSharedKey(encPriv, encPub)
	if err != nil {
		t.Fatal(err)
	}

	plaintext := make([]byte, 5000)
	for i := range plaintext {
		plaintext[i] = byte(i % 256)
	}

	envelopes, err := chunk.SplitAndEncrypt("large-msg", "alice", "bob", plaintext, aesKey, signPriv)
	if err != nil {
		t.Fatal(err)
	}
	if len(envelopes) < 2 {
		t.Fatalf("expected multiple chunks for 5000 bytes, got %d", len(envelopes))
	}

	decrypted, err := chunk.AssembleAndDecrypt(envelopes, aesKey, signPub)
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

func TestP2PChunks(t *testing.T) {
	port := freePort()
	if port == 0 {
		t.Fatal("no free port")
	}
	srv := p2p.NewServer(port)
	go srv.Start()
	time.Sleep(200 * time.Millisecond)

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	ctx := context.Background()
	data := []byte("test chunk data")

	if err := p2p.StoreChunk(ctx, addr, "msg1", 0, data); err != nil {
		t.Fatal(err)
	}
	retrieved, err := p2p.GetChunk(ctx, addr, "msg1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(retrieved) != string(data) {
		t.Fatalf("chunk data mismatch")
	}
	if err := p2p.DeleteChunk(ctx, addr, "msg1", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := p2p.GetChunk(ctx, addr, "msg1", 0); err == nil {
		t.Fatal("expected error getting deleted chunk")
	}
}

func TestP2PSignaling(t *testing.T) {
	port := freePort()
	if port == 0 {
		t.Fatal("no free port")
	}
	srv := p2p.NewServer(port)
	go srv.Start()
	time.Sleep(200 * time.Millisecond)

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	ctx := context.Background()

	sig := p2p.SignalMsg{From: "alice", Type: "offer", Data: `{"sdp":"test"}`}
	if err := p2p.SendSignal(ctx, addr, sig); err != nil {
		t.Fatal(err)
	}

	polled, err := p2p.PollSignals(ctx, addr, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if len(polled) != 1 || polled[0].From != "alice" || polled[0].Type != "offer" {
		t.Fatalf("signal content mismatch: %+v", polled)
	}

	empty, err := p2p.PollSignals(ctx, addr, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("expected 0 signals after poll, got %d", len(empty))
	}
}

type testMuninnServer struct {
	mu     sync.Mutex
	peers  map[string]*muninn.Peer
	chunks map[string][]muninn.ChunkRecord
	srv    *httptest.Server
}

func newTestMuninnServer() *testMuninnServer {
	ts := &testMuninnServer{
		peers:  make(map[string]*muninn.Peer),
		chunks: make(map[string][]muninn.ChunkRecord),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/peers", ts.handleRegister)
	mux.HandleFunc("GET /api/v1/peers", ts.handleList)
	mux.HandleFunc("GET /api/v1/peers/best", ts.handleBestPeers)
	mux.HandleFunc("GET /api/v1/peers/{id}", ts.handleGet)
	mux.HandleFunc("DELETE /api/v1/peers/{id}", ts.handleDelete)
	mux.HandleFunc("POST /api/v1/peers/{id}/heartbeat", ts.handleHeartbeat)
	mux.HandleFunc("PUT /api/v1/files/{fileID}/chunks/{chunkIndex}", ts.handleRegisterChunk)
	mux.HandleFunc("GET /api/v1/recipient/{recipientID}/chunks", ts.handleGetChunks)
	mux.HandleFunc("POST /api/v1/peers/{sourcePeerID}/chunk-reports", ts.handleReportChunk)
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
	var req muninn.RegisterChunkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ts.mu.Lock()
	ts.chunks[req.RecipientID] = append(ts.chunks[req.RecipientID], muninn.ChunkRecord{
		FileID:      fileID,
		ChunkIndex:  0,
		SenderID:    req.SenderID,
		RecipientID: req.RecipientID,
		Hash:        req.Hash,
		PeerID:      req.PeerID,
	})
	ts.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (ts *testMuninnServer) handleGetChunks(w http.ResponseWriter, r *http.Request) {
	recipientID := r.PathValue("recipientID")
	ts.mu.Lock()
	records := ts.chunks[recipientID]
	ts.mu.Unlock()
	if records == nil {
		records = []muninn.ChunkRecord{}
	}
	json.NewEncoder(w).Encode(records)
}

func (ts *testMuninnServer) handleReportChunk(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func TestMessengerOfflineFlow(t *testing.T) {
	mn := newTestMuninnServer()
	defer mn.Close()

	mc := muninn.NewClient(mn.URL())

	alicePort, bobPort := freePort(), freePort()
	if alicePort == 0 || bobPort == 0 {
		t.Fatal("no free ports")
	}

	alice, err := messenger.New("alice", alicePort, mc)
	if err != nil {
		t.Fatal(err)
	}
	defer alice.Shutdown()
	bob, err := messenger.New("bob", bobPort, mc)
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

	if err := alice.RefreshPeers(); err != nil {
		t.Fatal(err)
	}
	if err := bob.RefreshPeers(); err != nil {
		t.Fatal(err)
	}

	t.Logf("alice peers: %d, bob peers: %d", len(alice.GetPeers()), len(bob.GetPeers()))

	err = alice.SendMessage("bob", "hello from alice via offline chunks")
	if err != nil {
		t.Fatal(err)
	}
	t.Log("message sent, waiting for delivery...")

	deadline := time.Now().Add(20 * time.Second)
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
		t.Fatal("bob did not receive the offline message (check messenger.peerRefreshLoop timing)")
	}

	if lastMsg.Text != "hello from alice via offline chunks" {
		t.Fatalf("message text mismatch: got %q", lastMsg.Text)
	}
	if lastMsg.MsgID == "" {
		t.Fatal("message id is empty")
	}
	t.Logf("OK: offline message delivered, id=%s", lastMsg.MsgID)
}
