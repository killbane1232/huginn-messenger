package store

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestMessageHistoryOrdersTimestampsByInstantAcrossTimeZones(t *testing.T) {
	s, err := New(filepath.Join(t.TempDir(), "message-timezones.db"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	moscow := time.FixedZone("MSK", 3*60*60)
	earlier := time.Date(2026, time.August, 6, 12, 0, 0, 0, moscow)
	later := time.Date(2026, time.August, 6, 10, 0, 0, 0, time.UTC)

	if err := s.SaveMessage("sent", "peer", "me", "peer", []byte("sent"), earlier); err != nil {
		t.Fatalf("SaveMessage(sent): %v", err)
	}
	if err := s.SaveMessage("received", "peer", "peer", "peer", []byte("received"), later); err != nil {
		t.Fatalf("SaveMessage(received): %v", err)
	}

	got, err := s.GetMessages("peer")
	if err != nil {
		t.Fatalf("GetMessages: %v", err)
	}
	want := [][]byte{[]byte("sent"), []byte("received")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetMessages() = %q, want %q", got, want)
	}

	got, err = s.GetMessagesDesc("peer", 2, 0)
	if err != nil {
		t.Fatalf("GetMessagesDesc: %v", err)
	}
	want = [][]byte{[]byte("received"), []byte("sent")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetMessagesDesc() = %q, want %q", got, want)
	}
}

func TestMessageTimestampMigrationNormalizesExistingRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "message-timezone-migration.db")
	s, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	moscow := time.FixedZone("MSK", 3*60*60)
	timestamp := time.Date(2026, time.August, 6, 12, 0, 0, 123456000, moscow)
	data, err := json.Marshal(map[string]any{"timestamp": timestamp})
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if _, err := s.db.Exec(
		"INSERT INTO messages (message_uid, login, sender_login, chat_id, data, created_at) VALUES (?, ?, ?, ?, ?, ?)",
		"legacy", "peer", "me", "peer", data, timestamp,
	); err != nil {
		t.Fatalf("insert legacy message: %v", err)
	}
	if _, err := s.db.Exec("DELETE FROM schema_version WHERE id = '008'"); err != nil {
		t.Fatalf("reset migration: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s, err = New(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	var storageType string
	var createdAt int64
	if err := s.db.QueryRow(
		"SELECT typeof(created_at), created_at FROM messages WHERE message_uid = ?",
		"legacy",
	).Scan(&storageType, &createdAt); err != nil {
		t.Fatalf("read migrated timestamp: %v", err)
	}
	if storageType != "integer" {
		t.Fatalf("typeof(created_at) = %q, want integer", storageType)
	}
	if createdAt != timestamp.UnixMicro() {
		t.Fatalf("created_at = %d, want %d", createdAt, timestamp.UnixMicro())
	}
}

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
		{"group-legacy", "group-uid:group-signature", "group-uid", "group-legacy", 2 * time.Second},
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
