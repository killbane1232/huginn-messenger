package store

import (
	"path/filepath"
	"reflect"
	"strings"
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
		{"similar", "group-uid2:signature", "group-uid2:signature", "similar", 4 * time.Second},
		{"upper-bound", "group-uid;signature", "group-uid;signature", "upper-bound", 5 * time.Second},
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

	got, err = s.GetMessages("alice:signature")
	if err != nil {
		t.Fatalf("GetMessages direct: %v", err)
	}
	want = [][]byte{[]byte("direct")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetMessages(direct) = %q, want %q", got, want)
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

func TestGetMessagesDescUsesHistoryIndexes(t *testing.T) {
	s, err := New(filepath.Join(t.TempDir(), "messages-query-plan.db"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	args := append(messageHistoryArgs("group-uid"), 64, 0)
	rows, err := s.db.Query(`EXPLAIN QUERY PLAN `+messageHistorySelect+`
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?`, args...)
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN: %v", err)
	}
	defer rows.Close()

	var details []string
	usesHistoryIndex := false
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			t.Fatalf("scan query plan: %v", err)
		}
		details = append(details, detail)
		if strings.Contains(detail, "idx_messages_chat_id_created_at") ||
			strings.Contains(detail, "idx_messages_login_created_at") {
			usesHistoryIndex = true
		}
		if strings.Contains(detail, "SCAN messages") {
			t.Fatalf("history query performs a full table scan: %v", details)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("query plan rows: %v", err)
	}
	if !usesHistoryIndex {
		t.Fatalf("history query does not use a history index: %v", details)
	}
}
