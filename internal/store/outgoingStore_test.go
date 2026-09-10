package store

import (
	"path/filepath"
	"testing"
	"time"
)

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
