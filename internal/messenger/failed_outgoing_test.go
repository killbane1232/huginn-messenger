package messenger

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/crypto"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
	"github.com/killbane1232/huginn-messenger/internal/store"
)

func reopenFailedMessageStore(t *testing.T, m *Messenger) {
	t.Helper()
	if err := m.store.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	m.store, err = store.New(m.appConfig.DBPath)
	if err != nil {
		t.Fatal(err)
	}
}

func TestFailedOutgoingPersistsAndRetriesWithoutDuplicates(t *testing.T) {
	for _, withFile := range []bool{false, true} {
		name := "text"
		if withFile {
			name = "attachment"
		}
		t.Run(name, func(t *testing.T) {
			m := newDeliveryTestMessenger(t, "http://127.0.0.1:1")
			// Keep delivery queued so the test can inspect its durable payload.
			m.async = newAsyncPool(m.ctx, 0, 1)
			events := m.SubscribeMessages()
			defer m.UnsubscribeMessages(events)
			var paths []string
			if withFile {
				path := filepath.Join(t.TempDir(), "attachment.txt")
				if err := os.WriteFile(path, []byte("attachment contents"), 0600); err != nil {
					t.Fatal(err)
				}
				paths = []string{path}
			}
			const recipient = "recipient:signature"
			if err := m.SendMessage(recipient, "keep this message", paths, 1234); err != nil {
				t.Fatal(err)
			}
			failed := <-events
			if failed.SendError != "recipient not found" || failed.ChatID != recipient || failed.From != m.Username {
				t.Fatalf("unexpected failed event: %+v", failed)
			}
			if pending, err := m.store.GetUnplacedChunks(); err != nil || len(pending) != 0 {
				t.Fatalf("failed message entered delivery queue: %v, %v", pending, err)
			}
			reopenFailedMessageStore(t, m)
			saved := m.GetMessages(recipient)
			if len(saved) != 1 || saved[0].SendError == "" || saved[0].MsgID != failed.MsgID || saved[0].Text != failed.Text {
				t.Fatalf("failure did not survive restart: %+v", saved)
			}
			if withFile && (len(saved[0].Files) != 1 || saved[0].Files[0].FilePath != paths[0]) {
				t.Fatalf("attachment did not survive restart: %+v", saved[0].Files)
			}
			if _, err := m.RetryMessage(failed.MsgID); err == nil {
				t.Fatal("retry with missing recipient succeeded")
			}
			if history := m.GetMessages(recipient); len(history) != 1 || history[0].SendError == "" {
				t.Fatalf("unsuccessful retry lost or duplicated message: %+v", history)
			}
			_, public, err := crypto.GenerateEncryptionKey()
			if err != nil {
				t.Fatal(err)
			}
			m.peersMap[recipient] = muninn.Peer{ID: "recipient", Login: "recipient", SignatureKey: "signature", EncryptionKey: crypto.EncodeKey(public)}
			beforeRetry := time.Now()
			retried, err := m.RetryMessage(failed.MsgID)
			if err != nil {
				t.Fatal(err)
			}
			if retried.MsgID != failed.MsgID || retried.SendError != "" || !retried.Timestamp.Equal(failed.Timestamp) {
				t.Fatalf("retry changed identity or retained failure: %+v", retried)
			}
			updated := <-events
			if updated.MsgID != failed.MsgID || updated.SendError != "" {
				t.Fatalf("retry event: %+v", updated)
			}
			pending, err := m.store.GetUnplacedChunks()
			if err != nil || len(pending) == 0 {
				t.Fatalf("retry has no durable delivery payload: %v", err)
			}
			for _, part := range pending {
				if part.TTLSeconds != 1234 || part.CreatedAt.Before(beforeRetry) {
					t.Fatalf("retry did not restart original TTL: %+v", part)
				}
			}
			if _, err := m.RetryMessage(failed.MsgID); err == nil {
				t.Fatal("already queued message retried twice")
			}
			if err := m.DeleteFailedMessage(failed.MsgID); err == nil {
				t.Fatal("failed-send deletion removed a queued message")
			}
			reopenFailedMessageStore(t, m)
			if history := m.GetMessages(recipient); len(history) != 1 || history[0].SendError != "" {
				t.Fatalf("successful retry did not persist: %+v", history)
			}
		})
	}
}

func TestFailedOutgoingDeletionSurvivesRestart(t *testing.T) {
	m := newDeliveryTestMessenger(t, "http://127.0.0.1:1")
	if err := m.SendMessage("missing", "delete me", nil, 0); err != nil {
		t.Fatal(err)
	}
	failed := m.GetMessages("missing")[0]
	if err := m.DeleteFailedMessage(failed.MsgID); err != nil {
		t.Fatal(err)
	}
	reopenFailedMessageStore(t, m)
	if history := m.GetMessages("missing"); len(history) != 0 {
		t.Fatalf("deleted message reappeared: %+v", history)
	}
	if _, err := m.RetryMessage(failed.MsgID); err == nil {
		t.Fatal("deleted message was retried")
	}
	if err := m.store.SaveMessage(failed.MsgID, "missing", m.Username, "missing", []byte(`{}`), failed.Timestamp); err != store.ErrDeletedMessage {
		t.Fatalf("deleted message replay was accepted: %v", err)
	}
}
