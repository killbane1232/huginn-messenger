package store

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"
)

func TestRetryOutgoingRollbackAndReplication(t *testing.T) {
	st, err := New(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now().UTC()
	failed := []byte(`{"send_error":"recipient not found","text":"keep me"}`)
	if err := st.SaveMessage("message", "peer", "me", "peer", failed, now.Add(-24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := st.ExportReplicationSnapshot()
	if err != nil || len(snapshot.Messages) != 0 {
		t.Fatalf("failed send was replicated: %+v, %v", snapshot, err)
	}
	delta, err := st.ExportMessagesForStateSync(time.Time{}, time.Now().UTC())
	if err != nil || len(delta.Messages) != 0 {
		t.Fatalf("failed send entered state sync: %+v, %v", delta, err)
	}
	if _, err := st.db.Exec(`CREATE TRIGGER reject_retry BEFORE INSERT ON pending_chunks BEGIN SELECT RAISE(ABORT,'disk failure'); END`); err != nil {
		t.Fatal(err)
	}
	parts := []PendingChunk{{FileID: "message", Data: []byte("payload"), CreatedAt: now, TTLSeconds: 3600}}
	if err := st.RetryOutgoing("message", "peer", "me", "peer", []byte(`{}`), now, parts); err == nil {
		t.Fatal("failed retry transaction reported success")
	}
	data, err := st.GetMessage("message")
	if err != nil || !bytes.Equal(data, failed) {
		t.Fatalf("failed retry changed saved message: %s, %v", data, err)
	}
	if chunks, err := st.ListChunks("message"); err != nil || len(chunks) != 0 {
		t.Fatalf("failed retry left partial chunks: %+v, %v", chunks, err)
	}
	if _, err := st.db.Exec(`DROP TRIGGER reject_retry`); err != nil {
		t.Fatal(err)
	}
	if err := st.RetryOutgoing("message", "peer", "me", "peer", []byte(`{}`), now, parts); err != nil {
		t.Fatal(err)
	}
	delta, err = st.ExportMessagesForStateSync(time.Time{}, time.Now().UTC())
	if err != nil || len(delta.Messages) != 1 {
		t.Fatalf("successful retry missing from state sync: %+v, %v", delta, err)
	}
}

func TestOutgoingQueueRollsBackMessageAndChunksTogether(t *testing.T) {
	st, err := New(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.db.Exec(`CREATE TRIGGER reject_retry BEFORE INSERT ON pending_chunks BEGIN SELECT RAISE(ABORT,'disk failure'); END`); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	parts := []PendingChunk{{FileID: "message", Data: []byte("payload"), CreatedAt: now, TTLSeconds: 604800}}
	if err := st.QueueOutgoing("message", "peer", "me", "peer", []byte(`{}`), now, parts); err == nil {
		t.Fatal("failed transaction reported success")
	}
	present, err := st.FindMessageById("message")
	if err != nil || present {
		t.Fatalf("message was committed without outbox: %v", err)
	}
	chunks, err := st.ListChunks("message")
	if err != nil || len(chunks) != 0 {
		t.Fatal("payload was partially committed")
	}
}

func TestOutgoingChunkSurvivesCleanupAndRecoversFromLegacyDeletion(t *testing.T) {
	st, err := New(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now().UTC()
	parts := []PendingChunk{{FileID: "message", Data: []byte("payload"), CreatedAt: now, TTLSeconds: 3600}}
	if err := st.QueueOutgoing("message", "peer", "me", "peer", []byte(`{}`), now, parts); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkChunkPlaced("message", 0); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteChunksWithMessage(); err != nil {
		t.Fatal(err)
	}
	chunks, err := st.ListChunks("message")
	if err != nil || len(chunks) != 1 {
		t.Fatal("advertised source copy was deleted before TTL")
	}
	if err := st.DeleteChunks("message"); err != nil {
		t.Fatal(err)
	}
	data, err := st.GetChunk("message", 0)
	if err != nil || string(data) != "payload" {
		t.Fatal("legacy deletion made outbox bytes unavailable")
	}
	if err := st.DeleteExpiredPendingChunks(now.Add(2 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	data, err = st.GetChunk("message", 0)
	if err != nil || len(data) != 0 {
		t.Fatal("expired outbox payload is still served")
	}
}
