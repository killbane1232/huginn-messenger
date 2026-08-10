package messenger

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/store"
)

func TestBuildReloginReplicaOmitsLocalFilePaths(t *testing.T) {
	db, err := store.New(filepath.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	timestamp := time.Date(2026, time.August, 6, 10, 0, 0, 0, time.UTC)
	message := ChatMessage{
		From:      "alice",
		ChatID:    "alice:signature",
		Text:      "file",
		Timestamp: timestamp,
		MsgID:     "message-1",
		Files: []FileMeta{{
			FileID:        "file-1",
			FileHash:      "hash",
			DecryptionKey: "key",
			TotalChunks:   2,
			Filename:      "photo.png",
			FilePath:      "/private/source/photo.png",
		}},
	}
	data, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if err := db.SaveMessage(message.MsgID, message.ChatID, message.From, message.ChatID, data, timestamp); err != nil {
		t.Fatalf("SaveMessage: %v", err)
	}

	messenger := &Messenger{ID: "source-device", store: db}
	compressed, checksum, err := messenger.buildReloginReplica()
	if err != nil {
		t.Fatalf("buildReloginReplica: %v", err)
	}
	if checksum == "" {
		t.Fatal("empty checksum")
	}
	snapshot, err := decodeReloginReplica(compressed)
	if err != nil {
		t.Fatalf("decodeReloginReplica: %v", err)
	}
	if len(snapshot.Messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(snapshot.Messages))
	}
	if strings.Contains(string(snapshot.Messages[0].Data), "/private/source/photo.png") {
		t.Fatalf("replica leaked source path: %s", snapshot.Messages[0].Data)
	}
	var restored ChatMessage
	if err := json.Unmarshal(snapshot.Messages[0].Data, &restored); err != nil {
		t.Fatalf("decode replicated message: %v", err)
	}
	if len(restored.Files) != 1 || restored.Files[0].FileID != "file-1" {
		t.Fatalf("file pointer not preserved: %+v", restored.Files)
	}
	if restored.Files[0].FilePath != "" {
		t.Fatalf("replicated file path = %q, want empty", restored.Files[0].FilePath)
	}
	if restored.Files[0].SourcePeerID != "source-device" {
		t.Fatalf("replicated file source = %q, want source-device", restored.Files[0].SourcePeerID)
	}
}
