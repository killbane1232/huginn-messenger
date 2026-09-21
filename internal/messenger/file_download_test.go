package messenger

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/chunk"
	"github.com/killbane1232/huginn-messenger/internal/crypto"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
	"github.com/killbane1232/huginn-messenger/internal/store"
)

func TestDownloadedFileHistorySurvivesRestartWithoutReadyEvent(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "messenger.db")
	db, err := store.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "custom-download-name.txt")
	if err := os.WriteFile(path, []byte("downloaded"), 0600); err != nil {
		t.Fatal(err)
	}
	message := ChatMessage{From: "sender", ChatID: "sender:key", MsgID: "message", Timestamp: time.Now(),
		Files: []FileMeta{{FileID: "file", Filename: "original.txt", TotalChunks: 2}}}
	saveDownloadTestMessage(t, db, message, "sender:key")
	if _, err := db.EnsureFileDownload("file", time.Now(), fileDownloadTTL); err != nil {
		t.Fatal(err)
	}
	if err := db.MarkFileDownloadCompleted("file", path, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := newFileDownloadTestMessenger(db, filepath.Join(dir, "different-downloads"))
	for name, history := range map[string][]ChatMessage{
		"all":  m.GetMessages(message.ChatID),
		"page": m.GetMessagesDesc(message.ChatID, 1, 0),
	} {
		if len(history) != 1 || len(history[0].Files) != 1 || history[0].Files[0].FilePath != path {
			t.Fatalf("%s history lost completed download: %+v", name, history)
		}
	}
	// Local history enrichment must not write the device path into replicated messages.
	rows, err := db.GetMessages(message.ChatID)
	if err != nil {
		t.Fatal(err)
	}
	var stored ChatMessage
	if err := json.Unmarshal(rows[0], &stored); err != nil || stored.Files[0].FilePath != "" {
		t.Fatalf("stored transport metadata changed: %+v, %v", stored, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if got := m.GetMessages(message.ChatID)[0].Files[0].FilePath; got != "" {
		t.Fatalf("removed download is still advertised as available: %q", got)
	}
}

func TestOrdinaryFileDownloadResumesAfterRestart(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "messenger.db")
	db, err := store.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	public, private, err := crypto.GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	sender := muninn.Peer{ID: "sender-device", Login: "sender", SignatureKey: crypto.EncodeKey(public)}
	contents := bytes.Repeat([]byte("x"), chunk.ChunkSize+1)
	aesKey := bytes.Repeat([]byte{1}, 32)
	envelopes, err := chunk.SplitAndEncryptFile("partial", sender.ID, contents, aesKey, private)
	if err != nil {
		t.Fatal(err)
	}
	firstChunk, err := chunk.MarshalEnvelope(envelopes[0])
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(contents)
	firstSeen := time.Now().Add(-24 * time.Hour).Truncate(time.Second)
	for _, id := range []string{"partial", "stopped", "outgoing"} {
		file := FileMeta{FileID: id, Filename: id + ".txt", TotalChunks: 2}
		from, login := "sender", sender.Key()
		if id == "partial" {
			file.FileHash = crypto.EncodeKey(digest[:])
			file.DecryptionKey = crypto.EncodeKey(aesKey)
		}
		if id == "outgoing" {
			file.FilePath = filepath.Join(dir, "source.txt")
			from, login = "receiver", "recipient:key"
		}
		saveDownloadTestMessage(t, db, ChatMessage{From: from, ChatID: login, MsgID: id, Timestamp: firstSeen, Files: []FileMeta{file}}, login)
		if id == "outgoing" {
			continue
		}
		if _, err := db.EnsureFileDownload(id, firstSeen, fileDownloadTTL); err != nil {
			t.Fatal(err)
		}
		if err := db.StoreChunk(id, 0, firstChunk, 604800); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.MarkFileDownloadStopped("stopped", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.New(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := newFileDownloadTestMessenger(db, dir)
	m.Username, m.Key = "receiver", "receiver:key"
	m.ctx = context.Background()
	m.muninnClient = muninn.NewClient(srv.URL)
	m.peersMap = map[string]muninn.Peer{sender.Key(): sender}
	// Execute queued jobs deterministically, without starting unrelated background loops.
	m.async = &asyncPool{ctx: m.ctx, jobs: make(chan func(), 10)}
	m.resumeFileDownloads()
	for len(m.async.jobs) > 0 {
		(<-m.async.jobs)()
	}
	pending := m.pendingFileDownloads["partial"]
	if pending == nil || pending.senderID != sender.Key() || pending.preferredPeerID != "" {
		t.Fatalf("ordinary incoming download was not restored: %+v", pending)
	}
	if len(m.pendingFileDownloads) != 1 {
		t.Fatalf("stopped/outgoing files were queued: %+v", m.pendingFileDownloads)
	}
	state, err := db.GetFileDownload("partial")
	if err != nil || !state.FirstSeenAt.Equal(firstSeen) || !state.ExpiresAt.Equal(firstSeen.Add(fileDownloadTTL)) {
		t.Fatalf("restart reset the download deadline: %+v, %v", state, err)
	}
	chunks, err := db.ListChunks("partial")
	if err != nil || len(chunks) != 1 || !bytes.Equal(chunks[0], firstChunk) {
		t.Fatalf("restart lost partial progress: %v, %v", chunks, err)
	}
	// Receiving the remaining chunk after restart must finish the original file.
	lastChunk, err := chunk.MarshalEnvelope(envelopes[1])
	if err != nil {
		t.Fatal(err)
	}
	if err := db.StoreChunk("partial", 1, lastChunk, 604800); err != nil {
		t.Fatal(err)
	}
	ready := m.SubscribeFileReady()
	defer m.UnsubscribeFileReady(ready)
	m.checkPendingFileDownloads()
	select {
	case event := <-ready:
		data, err := os.ReadFile(event.FilePath)
		if err != nil || !bytes.Equal(data, contents) {
			t.Fatalf("resumed download contents differ: %v", err)
		}
		var restoredPath string
		for _, message := range m.GetMessages(sender.Key()) {
			if message.MsgID == "partial" {
				restoredPath = message.Files[0].FilePath
			}
		}
		if restoredPath != event.FilePath {
			t.Fatalf("completed path = %q, want %q", restoredPath, event.FilePath)
		}
	default:
		t.Fatal("resumed file never completed")
	}
	if len(m.pendingFileDownloads) != 0 {
		t.Fatal("completed download remains pending")
	}
}

func saveDownloadTestMessage(t *testing.T, db *store.SQLiteStore, message ChatMessage, login string) {
	t.Helper()
	data, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SaveMessage(message.MsgID, login, message.From, message.ChatID, data, message.Timestamp); err != nil {
		t.Fatal(err)
	}
}

func TestExistingFileCompletesDownloadBeforeNetworkRequest(t *testing.T) {
	dir := t.TempDir()
	db, err := store.New(filepath.Join(dir, "messenger.db"))
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	contents := []byte("already downloaded")
	digest := sha256.Sum256(contents)
	path := filepath.Join(dir, "ready.txt")
	if err := os.WriteFile(path, contents, 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	m := newFileDownloadTestMessenger(db, dir)
	m.pendingFileDownloads["file-ready"] = &pendingFileDownload{}
	m.processReceivedFileFromPeer(FileMeta{
		FileID:      "file-ready",
		FileHash:    base64.StdEncoding.EncodeToString(digest[:]),
		TotalChunks: 1,
		Filename:    "ready.txt",
	}, "sender:key", "stale-peer")

	state, err := db.GetFileDownload("file-ready")
	if err != nil {
		t.Fatalf("GetFileDownload: %v", err)
	}
	if state.CompletedAt == nil || state.LocalPath != path {
		t.Fatalf("download state = %+v, want completed path %q", state, path)
	}
	if _, pending := m.pendingFileDownloads["file-ready"]; pending {
		t.Fatal("completed file remains pending")
	}
}

func TestExpiredIncompleteFileStopsBeforeNetworkRequest(t *testing.T) {
	dir := t.TempDir()
	db, err := store.New(filepath.Join(dir, "messenger.db"))
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.EnsureFileDownload("file-expired", time.Now().Add(-8*24*time.Hour), fileDownloadTTL); err != nil {
		t.Fatalf("EnsureFileDownload: %v", err)
	}
	m := newFileDownloadTestMessenger(db, dir)
	m.pendingFileDownloads["file-expired"] = &pendingFileDownload{}
	m.processReceivedFileFromPeer(FileMeta{
		FileID:      "file-expired",
		FileHash:    "not-present",
		TotalChunks: 2,
		Filename:    "missing.txt",
	}, "sender:key", "stale-peer")

	state, err := db.GetFileDownload("file-expired")
	if err != nil {
		t.Fatalf("GetFileDownload: %v", err)
	}
	if state.StoppedAt == nil || state.CompletedAt != nil {
		t.Fatalf("download state = %+v, want stopped", state)
	}
	if _, pending := m.pendingFileDownloads["file-expired"]; pending {
		t.Fatal("expired file remains pending")
	}

	// A persisted stopped state must also avoid network access on later runs.
	m.processReceivedFileFromPeer(FileMeta{
		FileID:      "file-expired",
		FileHash:    "not-present",
		TotalChunks: 2,
		Filename:    "missing.txt",
	}, "sender:key", "stale-peer")
}

func newFileDownloadTestMessenger(db *store.SQLiteStore, downloadsDir string) *Messenger {
	return &Messenger{
		store:                db,
		downloadsDir:         downloadsDir,
		pendingFileDownloads: make(map[string]*pendingFileDownload),
		processingMsg:        make(map[string]bool),
	}
}
