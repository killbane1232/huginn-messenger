package messenger

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestOutgoingMessagePersistsLocalFilePath(t *testing.T) {
	wantPath := "/home/user/Pictures/photo.png"
	message := ChatMessage{
		Files: []FileMeta{{
			FileID:   "file-id",
			Filename: "photo.png",
			FilePath: wantPath,
		}},
	}

	data, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("marshal message: %v", err)
	}

	var restored ChatMessage
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("unmarshal message: %v", err)
	}
	if len(restored.Files) != 1 || restored.Files[0].FilePath != wantPath {
		t.Fatalf("restored files = %+v, want local path %q", restored.Files, wantPath)
	}
}

func TestTransportMetadataOmitsLocalFilePath(t *testing.T) {
	files := []FileMeta{{
		FileID:   "file-id",
		Filename: "photo.png",
		FilePath: "/home/user/Pictures/photo.png",
	}}

	transportFiles := withoutLocalFilePaths(files)
	if transportFiles[0].FilePath != "" {
		t.Fatalf("transport file path = %q, want empty", transportFiles[0].FilePath)
	}
	if files[0].FilePath == "" {
		t.Fatal("source metadata was mutated")
	}

	data, err := json.Marshal(MessagePayload{Files: transportFiles})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if strings.Contains(string(data), "file_path") || strings.Contains(string(data), files[0].FilePath) {
		t.Fatalf("transport payload leaks local path: %s", data)
	}
}
