package store

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestReplicationSnapshotRoundTrip(t *testing.T) {
	source, err := New(filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatalf("New(source): %v", err)
	}
	t.Cleanup(func() { _ = source.Close() })

	createdAt := time.Date(2026, time.August, 6, 9, 30, 0, 123456000, time.UTC)
	if err := source.SaveMessage(
		"message-1",
		"alice:signature",
		"alice",
		"alice:signature",
		[]byte(`{"from":"alice","chat_id":"alice:signature","text":"hello","timestamp":"2026-08-06T09:30:00.123456Z","msg_id":"message-1"}`),
		createdAt,
	); err != nil {
		t.Fatalf("SaveMessage: %v", err)
	}
	if err := source.StorePeer("alice", "alice:signature", "enc", "signature", createdAt, false); err != nil {
		t.Fatalf("StorePeer: %v", err)
	}
	group := GroupChat{
		UID:         "group-1",
		Name:        "Group",
		EncPrivate:  "enc-private",
		EncPublic:   "enc-public",
		SignPrivate: "sign-private",
		SignPublic:  "sign-public",
		CreatedAt:   createdAt,
	}
	if err := source.CreateGroupChat(&group); err != nil {
		t.Fatalf("CreateGroupChat: %v", err)
	}

	snapshot, err := source.ExportReplicationSnapshot()
	if err != nil {
		t.Fatalf("ExportReplicationSnapshot: %v", err)
	}

	target, err := New(filepath.Join(t.TempDir(), "target.db"))
	if err != nil {
		t.Fatalf("New(target): %v", err)
	}
	t.Cleanup(func() { _ = target.Close() })
	if err := target.ImportReplicationSnapshot(snapshot); err != nil {
		t.Fatalf("ImportReplicationSnapshot: %v", err)
	}

	restored, err := target.ExportReplicationSnapshot()
	if err != nil {
		t.Fatalf("ExportReplicationSnapshot(target): %v", err)
	}
	if !reflect.DeepEqual(restored, snapshot) {
		t.Fatalf("restored snapshot = %#v, want %#v", restored, snapshot)
	}

	if err := target.ImportReplicationSnapshot(snapshot); err != nil {
		t.Fatalf("second ImportReplicationSnapshot: %v", err)
	}
	second, err := target.ExportReplicationSnapshot()
	if err != nil {
		t.Fatalf("second ExportReplicationSnapshot: %v", err)
	}
	if !reflect.DeepEqual(second, snapshot) {
		t.Fatalf("second snapshot = %#v, want %#v", second, snapshot)
	}
}
