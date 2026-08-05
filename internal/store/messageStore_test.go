package store

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestGetMessagesUsesChatIDAndReadsLegacyGroupRows(t *testing.T) {
	s, err := New(filepath.Join(t.TempDir(), "messages.db"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	base := time.Date(2026, time.August, 4, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		uid, login, chatID, data string
		offset                   time.Duration
	}{
		{"direct", "alice:signature", "alice:signature", "direct", 0},
		{"group-current", "bob:signature", "group-uid", "group-current", time.Second},
		{"group-legacy", "group-uid:group-signature", "group-uid:group-signature", "group-legacy", 2 * time.Second},
		{"other", "other:signature", "other:signature", "other", 3 * time.Second},
	}
	for _, tt := range tests {
		if err := s.SaveMessage(tt.uid, tt.login, tt.login, tt.chatID, []byte(tt.data), base.Add(tt.offset)); err != nil {
			t.Fatalf("SaveMessage(%s): %v", tt.uid, err)
		}
	}

	got, err := s.GetMessages("group-uid")
	if err != nil {
		t.Fatalf("GetMessages: %v", err)
	}
	want := [][]byte{[]byte("group-current"), []byte("group-legacy")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetMessages() = %q, want %q", got, want)
	}

	got, err = s.GetMessagesDesc("group-uid", 1, 0)
	if err != nil {
		t.Fatalf("GetMessagesDesc: %v", err)
	}
	want = [][]byte{[]byte("group-legacy")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetMessagesDesc() = %q, want %q", got, want)
	}
}
