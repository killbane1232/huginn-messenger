package store

import (
	"path/filepath"
	"testing"
	"time"
)

func TestFileDownloadStatePersistsTerminalStateAndDoesNotExtendDeadline(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "downloads.db")
	db, err := New(dbPath)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	firstSeen := time.Date(2026, time.August, 12, 10, 0, 0, 0, time.UTC)
	state, err := db.EnsureFileDownload("file-1", firstSeen, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("EnsureFileDownload: %v", err)
	}
	if want := firstSeen.Add(7 * 24 * time.Hour); !state.ExpiresAt.Equal(want) {
		t.Fatalf("expires at = %v, want %v", state.ExpiresAt, want)
	}

	state, err = db.EnsureFileDownload("file-1", firstSeen.Add(5*24*time.Hour), 7*24*time.Hour)
	if err != nil {
		t.Fatalf("second EnsureFileDownload: %v", err)
	}
	if !state.FirstSeenAt.Equal(firstSeen) || !state.ExpiresAt.Equal(firstSeen.Add(7*24*time.Hour)) {
		t.Fatalf("repeated discovery extended deadline: %+v", state)
	}

	completedAt := firstSeen.Add(time.Hour)
	if err := db.MarkFileDownloadCompleted("file-1", "/downloads/file.txt", completedAt); err != nil {
		t.Fatalf("MarkFileDownloadCompleted: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err = New(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	state, err = db.GetFileDownload("file-1")
	if err != nil {
		t.Fatalf("GetFileDownload: %v", err)
	}
	if state.CompletedAt == nil || !state.CompletedAt.Equal(completedAt) {
		t.Fatalf("completed at = %v, want %v", state.CompletedAt, completedAt)
	}
	if state.LocalPath != "/downloads/file.txt" || state.StoppedAt != nil {
		t.Fatalf("restored completion = %+v", state)
	}
}
