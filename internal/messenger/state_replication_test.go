package messenger

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/store"
)

func TestStateSyncDeltaRoundTripStripsLocalFilePath(t *testing.T) {
	message := ChatMessage{
		From:      "alice",
		ChatID:    "bob:key",
		Text:      "file",
		Timestamp: time.Date(2026, time.August, 17, 9, 0, 0, 0, time.UTC),
		MsgID:     "message-1",
		Files: []FileMeta{{
			FileID:       "file-1",
			FilePath:     "/device/private/file.txt",
			TotalChunks:  2,
			SourcePeerID: "old-source",
		}},
	}
	data, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	delta := store.MessageReplicationDelta{
		Version:    stateSyncVersion,
		Checkpoint: message.Timestamp.UnixMicro(),
		Messages: []store.ReplicatedMessage{{
			MessageUID: message.MsgID,
			Login:      "bob:key",
			ChatID:     "bob:key",
			Data:       data,
			CreatedAt:  message.Timestamp.UnixMicro(),
		}},
	}

	m := &Messenger{ID: "current-source"}
	if err := m.sanitizeStateSyncMessages(delta.Messages); err != nil {
		t.Fatalf("sanitizeStateSyncMessages: %v", err)
	}
	compressed, digest, err := encodeStateSyncDelta(delta)
	if err != nil {
		t.Fatalf("encodeStateSyncDelta: %v", err)
	}
	if digest == "" {
		t.Fatal("empty state sync digest")
	}
	restored, err := decodeStateSyncDelta(compressed)
	if err != nil {
		t.Fatalf("decodeStateSyncDelta: %v", err)
	}
	if restored.Checkpoint != delta.Checkpoint || len(restored.Messages) != 1 {
		t.Fatalf("restored delta metadata = %#v", restored)
	}
	var restoredMessage ChatMessage
	if err := json.Unmarshal(restored.Messages[0].Data, &restoredMessage); err != nil {
		t.Fatalf("decode restored message: %v", err)
	}
	if got := restoredMessage.Files[0].FilePath; got != "" {
		t.Fatalf("replicated file path = %q, want empty", got)
	}
	if got := restoredMessage.Files[0].SourcePeerID; got != "current-source" {
		t.Fatalf("source peer id = %q, want current-source", got)
	}
}
