package messenger

import (
	"crypto/sha256"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/store"
)

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
