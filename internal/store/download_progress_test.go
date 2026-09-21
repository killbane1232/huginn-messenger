package store

import (
	"path/filepath"
	"testing"
	"time"
)

func TestActiveDownloadsPersistProgressAndCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "downloads.db")
	db, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Second)
	for _, id := range []string{"active", "cancelled", "completed", "expired", "stopped"} {
		started := now
		if id == "expired" {
			started = now.Add(-8 * 24 * time.Hour)
		}
		if _, err := db.TrackFileDownload(id, id+".txt", 4, started, 7*24*time.Hour); err != nil {
			t.Fatal(err)
		}
		for _, index := range []int{0, 2, -1, 4} {
			if err := db.StoreChunk(id, index, []byte("encrypted"), 604800); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.CancelFileDownload("cancelled", now); err != nil {
		t.Fatal(err)
	}
	if err := db.MarkFileDownloadCompleted("completed", "/saved/file", now); err != nil {
		t.Fatal(err)
	}
	if err := db.MarkFileDownloadStopped("stopped", now); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items, err := db.ListActiveFileDownloads(now)
	if err != nil || len(items) != 1 {
		t.Fatalf("active downloads: %+v, %v", items, err)
	}
	if items[0] != (FileDownloadProgress{FileID: "active", Filename: "active.txt", TotalChunks: 4, ReceivedChunks: 2}) {
		t.Fatalf("incorrect progress: %+v", items[0])
	}
	if err := db.StoreChunk("active", 1, []byte("next"), 604800); err != nil {
		t.Fatal(err)
	}
	items, err = db.ListActiveFileDownloads(now)
	if err != nil || items[0].ReceivedChunks != 3 {
		t.Fatalf("progress did not advance: %+v, %v", items, err)
	}
	state, err := db.TrackFileDownload("cancelled", "again.txt", 4, now.Add(time.Hour), 7*24*time.Hour)
	if err != nil || state.CancelledAt == nil || !state.FirstSeenAt.Equal(now) {
		t.Fatalf("cancel lost: %+v, %v", state, err)
	}
	if err := db.MarkFileDownloadCompleted("cancelled", "/late/file", now); err == nil {
		t.Fatal("late completion undid cancellation")
	}
	if err := db.CancelFileDownload("completed", now); err != ErrDownloadNotActive {
		t.Fatalf("cancel completed: %v", err)
	}
}

func TestDownloadMetadataMigratesFromExistingHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := db.EnsureFileDownload("legacy", now, 7*24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveMessage("message", "sender:key", "sender", "sender:key",
		[]byte(`{"files":[{"file_id":"legacy","filename":"old.txt","total_chunks":3}]}`), now); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveMessage("bad", "sender:key", "sender", "sender:key", []byte("not json"), now); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		"ALTER TABLE file_downloads DROP COLUMN cancelled_at",
		"ALTER TABLE file_downloads DROP COLUMN filename",
		"ALTER TABLE file_downloads DROP COLUMN total_chunks",
		"DELETE FROM schema_version WHERE id = '012'",
	} {
		if _, err := db.db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items, err := db.ListActiveFileDownloads(now)
	if err != nil || len(items) != 1 || items[0].Filename != "old.txt" || items[0].TotalChunks != 3 {
		t.Fatalf("legacy download not migrated: %+v, %v", items, err)
	}
}
