package store

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestDeleteChatPersistsAndRejectsReplayedHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chats.db")
	s, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	now := time.Now().UTC()
	if err := s.StorePeer("alice", "alice:key", "enc", "key", now, false); err != nil {
		t.Fatal(err)
	}
	group := &GroupChat{UID: "group", Name: "Friends", EncPrivate: "enc", SignPrivate: "sign", CreatedAt: now}
	if err := s.CreateGroupChat(group); err != nil {
		t.Fatal(err)
	}
	for id, chatID := range map[string]string{"one": "alice:key", "two": "alice:key", "other": "bob:key", "group-message": group.UID} {
		if err := s.SaveMessage(id, "alice:key", "alice", chatID, []byte(id), now); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := s.ExportReplicationSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, chatID := range []string{"alice:key", group.UID, "empty-chat", "alice:key"} {
		if err := s.DeleteChat(chatID); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.DeleteChat(" "); err == nil {
		t.Fatal("empty chat id accepted")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = New(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ImportReplicationSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	inserted, err := s.ImportStateSyncMessages(snapshot.Messages)
	if err != nil || len(inserted) != 0 {
		t.Fatalf("replayed state sync = %v, %v", inserted, err)
	}
	if err := s.SaveMessage("one", "alice:key", "alice", "alice:key", []byte("replay"), now); !errors.Is(err, ErrDeletedMessage) {
		t.Fatalf("live replay = %v", err)
	}
	if err := s.QueueOutgoing("two", "alice:key", "me", "alice:key", []byte("replay"), now, nil); !errors.Is(err, ErrDeletedMessage) {
		t.Fatalf("outbox replay = %v", err)
	}
	for _, id := range []string{"one", "two", "group-message"} {
		found, err := s.FindMessageById(id)
		if err != nil || !found {
			t.Fatalf("deleted message %s was forgotten: %v", id, err)
		}
	}
	for _, chatID := range []string{"alice:key", group.UID} {
		messages, err := s.GetMessagesDesc(chatID, 64, 0)
		if err != nil || len(messages) != 0 {
			t.Fatalf("deleted history %s = %q, %v", chatID, messages, err)
		}
	}
	hidden, err := s.GetHiddenChatIDs()
	if err != nil || !reflect.DeepEqual(hidden, []string{"alice:key", "empty-chat", "group"}) {
		t.Fatalf("hidden chats = %v, %v", hidden, err)
	}
	other, err := s.GetMessages("bob:key")
	if err != nil || len(other) != 1 || string(other[0]) != "other" {
		t.Fatalf("other conversation changed: %q, %v", other, err)
	}
	peers, err := s.GetStoredPeers()
	if err != nil || len(peers) != 1 || peers[0].SignatureKey != "key" {
		t.Fatalf("peer keys changed: %v, %v", peers, err)
	}
	savedGroup, err := s.GetGroupChat(group.UID)
	if err != nil || savedGroup.SignPrivate != group.SignPrivate {
		t.Fatalf("group keys changed: %v, %v", savedGroup, err)
	}
	exported, err := s.ExportReplicationSnapshot()
	if err != nil || len(exported.Messages) != 1 || exported.Messages[0].MessageUID != "other" {
		t.Fatalf("deleted history exported: %v, %v", exported.Messages, err)
	}
}

func TestNewMessagesReopenDeletedChats(t *testing.T) {
	for _, source := range []string{"live", "outbox", "state sync", "snapshot"} {
		t.Run(source, func(t *testing.T) {
			s, err := New(filepath.Join(t.TempDir(), "chats.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if err := s.DeleteChat("chat"); err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			message := ReplicatedMessage{MessageUID: "new", ChatID: "chat", CreatedAt: now.UnixMicro(), Data: []byte("new")}
			switch source {
			case "live":
				err = s.SaveMessage("new", "peer", "peer", "chat", message.Data, now)
			case "outbox":
				err = s.QueueOutgoing("new", "peer", "me", "chat", message.Data, now, nil)
			case "state sync":
				_, err = s.ImportStateSyncMessages([]ReplicatedMessage{message})
			case "snapshot":
				err = s.ImportReplicationSnapshot(ReplicationSnapshot{Version: 1, Messages: []ReplicatedMessage{message}})
			}
			if err != nil {
				t.Fatal(err)
			}
			hidden, err := s.GetHiddenChatIDs()
			if err != nil || len(hidden) != 0 {
				t.Fatalf("new message did not reopen chat: %v, %v", hidden, err)
			}
			history, err := s.GetMessages("chat")
			if err != nil || len(history) != 1 || string(history[0]) != "new" {
				t.Fatalf("new history = %q, %v", history, err)
			}
		})
	}
}
