package messenger

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/muninn"
	"github.com/killbane1232/huginn-messenger/internal/store"
)

func TestCancelDownloadInterruptsRequestAndSurvivesRestart(t *testing.T) {
	started := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer srv.Close()
	dir := t.TempDir()
	path := filepath.Join(dir, "downloads.db")
	db, err := store.New(path)
	if err != nil {
		t.Fatal(err)
	}
	m := newFileDownloadTestMessenger(db, dir)
	m.ctx = context.Background()
	m.muninnClient = muninn.NewClient(srv.URL)
	m.peersMap = map[string]muninn.Peer{"sender:key": {ID: "sender", Login: "sender", SignatureKey: "key"}}
	contents := []byte("late download")
	hash := sha256.Sum256(contents)
	file := FileMeta{FileID: "file", Filename: "file.txt", TotalChunks: 2, FileHash: base64.StdEncoding.EncodeToString(hash[:])}
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.processReceivedFile(file, "sender:key")
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("download never requested chunks")
	}
	// Visible even while the message containing the attachment is still being processed.
	items, err := m.GetFileDownloads()
	if err != nil || len(items) != 1 || items[0].Filename != "file.txt" {
		t.Fatalf("active list: %+v, %v", items, err)
	}
	if err := m.CancelFileDownload("file"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cancellation did not interrupt the request")
	}
	if len(m.pendingFileDownloads) != 0 {
		t.Fatal("cancelled download remains pending")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m = newFileDownloadTestMessenger(db, dir)
	ready := m.SubscribeFileReady()
	defer m.UnsubscribeFileReady(ready)
	// Even a matching file received late must not change cancelled to completed.
	if err := os.WriteFile(filepath.Join(dir, file.Filename), contents, 0600); err != nil {
		t.Fatal(err)
	}
	m.processReceivedFile(file, "sender:key")
	state, err := db.GetFileDownload(file.FileID)
	if err != nil || state.CancelledAt == nil || state.CompletedAt != nil {
		t.Fatalf("cancellation lost: %+v, %v", state, err)
	}
	select {
	case <-ready:
		t.Fatal("cancelled download emitted file_ready")
	default:
	}
	items, err = m.GetFileDownloads()
	if err != nil || len(items) != 0 {
		t.Fatalf("cancelled download reappeared: %+v, %v", items, err)
	}
}

func TestCancelledDownloadCannotPublishFile(t *testing.T) {
	dir := t.TempDir()
	db, err := store.New(filepath.Join(dir, "downloads.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m := newFileDownloadTestMessenger(db, dir)
	if _, err := db.TrackFileDownload("file", "file.txt", 2, time.Now(), fileDownloadTTL); err != nil {
		t.Fatal(err)
	}
	ctx, err := m.beginFileDownload("file")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.CancelFileDownload("file"); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() != context.Canceled {
		t.Fatal("scheduled chunk requests were not cancelled")
	}
	path := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(path, []byte("existing file"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := m.completeFileDownload(context.Background(), "file", path, []byte("late data")); err == nil {
		t.Fatal("cancelled download was published")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "existing file" {
		t.Fatalf("cancellation overwrote existing file: %v", err)
	}
	files, err := filepath.Glob(filepath.Join(dir, ".huginn-download-*"))
	if err != nil || len(files) != 0 {
		t.Fatalf("temporary download leaked: %v, %v", files, err)
	}
}
