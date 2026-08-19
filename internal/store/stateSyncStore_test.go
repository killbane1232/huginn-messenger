package store

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestLastStateCheckMigrationUpgradesExistingMessages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state-sync-migration.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if _, err := db.Exec(schemaVersionTable()); err != nil {
		t.Fatalf("create schema_version: %v", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE messages (
			message_uid TEXT PRIMARY KEY,
			login TEXT NOT NULL,
			sender_login TEXT NOT NULL,
			chat_id TEXT NOT NULL,
			data BLOB NOT NULL,
			created_at DATETIME NOT NULL
		);
		INSERT INTO messages (message_uid, login, sender_login, chat_id, data, created_at)
		VALUES ('legacy', 'bob:key', 'bob', 'bob:key', '{}', 123456);`); err != nil {
		t.Fatalf("create legacy messages: %v", err)
	}
	for _, migration := range migrations {
		if migration.ID == "010" {
			continue
		}
		if _, err := db.Exec(`
			INSERT INTO schema_version (id, author, description, applied_at, checksum)
			VALUES (?, ?, ?, ?, ?)`, migration.ID, migration.Author, migration.Description, time.Now().Unix(), checksum(migration)); err != nil {
			t.Fatalf("mark migration %s: %v", migration.ID, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy database: %v", err)
	}

	s, err := New(path)
	if err != nil {
		t.Fatalf("New(upgrade): %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	var stateUpdatedAt int64
	if err := s.db.QueryRow(`SELECT state_updated_at FROM messages WHERE message_uid = 'legacy'`).Scan(&stateUpdatedAt); err != nil {
		t.Fatalf("read migrated state_updated_at: %v", err)
	}
	if stateUpdatedAt != 123456 {
		t.Fatalf("state_updated_at = %d, want 123456", stateUpdatedAt)
	}
	if err := s.SetLastStateCheck("replica", time.Now().UTC()); err != nil {
		t.Fatalf("last_state_check table unavailable after upgrade: %v", err)
	}
}

func TestStateSyncCursorPersistsAndNeverMovesBackwards(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state-sync-cursor.db")
	s, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if got, err := s.GetLastStateCheck("replica-1"); err != nil || !got.IsZero() {
		t.Fatalf("initial GetLastStateCheck = %v, %v; want zero, nil", got, err)
	}
	newer := time.Date(2026, time.August, 17, 10, 0, 0, 123456000, time.UTC)
	older := newer.Add(-time.Hour)
	if err := s.SetLastStateCheck("replica-1", newer); err != nil {
		t.Fatalf("SetLastStateCheck(newer): %v", err)
	}
	if err := s.SetLastStateCheck("replica-1", older); err != nil {
		t.Fatalf("SetLastStateCheck(older): %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s, err = New(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	got, err := s.GetLastStateCheck("replica-1")
	if err != nil {
		t.Fatalf("GetLastStateCheck: %v", err)
	}
	if !got.Equal(newer) {
		t.Fatalf("last state check = %s, want %s", got, newer)
	}
}

func TestExportMessagesForStateSyncUsesBoundedIntervalAcrossAllChats(t *testing.T) {
	s, err := New(filepath.Join(t.TempDir(), "state-sync-export.db"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	oldMessageTime := time.Date(2025, time.January, 1, 9, 0, 0, 0, time.UTC)
	if err := s.SaveMessage("already-synced", "bob:key", "bob", "bob:key", []byte("already-synced"), oldMessageTime); err != nil {
		t.Fatalf("SaveMessage(already-synced): %v", err)
	}
	since := time.Now().UTC()
	time.Sleep(time.Millisecond)
	for _, message := range []struct {
		id, login, sender, chat string
	}{
		{"received", "bob:key", "bob", "bob:key"},
		{"sent", "carol:key", "me", "carol:key"},
	} {
		if err := s.SaveMessage(message.id, message.login, message.sender, message.chat, []byte(message.id), oldMessageTime); err != nil {
			t.Fatalf("SaveMessage(%s): %v", message.id, err)
		}
	}
	checkpoint := time.Now().UTC()
	time.Sleep(time.Millisecond)
	if err := s.SaveMessage("after-checkpoint", "dave:key", "dave", "dave:key", []byte("after-checkpoint"), oldMessageTime); err != nil {
		t.Fatalf("SaveMessage(after-checkpoint): %v", err)
	}

	delta, err := s.ExportMessagesForStateSync(since, checkpoint)
	if err != nil {
		t.Fatalf("ExportMessagesForStateSync: %v", err)
	}
	if delta.Version != 1 || delta.Checkpoint != checkpoint.UnixMicro() {
		t.Fatalf("delta metadata = version %d checkpoint %d", delta.Version, delta.Checkpoint)
	}
	got := make([]string, 0, len(delta.Messages))
	for _, message := range delta.Messages {
		got = append(got, message.MessageUID)
	}
	want := []string{"received", "sent"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("exported messages = %v, want %v", got, want)
	}
}

func TestImportStateSyncMessagesIsIdempotentAndPreservesLocalData(t *testing.T) {
	s, err := New(filepath.Join(t.TempDir(), "state-sync-import.db"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	createdAt := time.Date(2026, time.August, 17, 9, 0, 0, 0, time.UTC)
	if err := s.SaveMessage("existing", "bob:key", "bob", "bob:key", []byte(`{"file_path":"/local/file"}`), createdAt); err != nil {
		t.Fatalf("SaveMessage: %v", err)
	}
	delta := []ReplicatedMessage{
		{
			MessageUID: "existing", SenderLogin: "bob", Login: "bob:key", ChatID: "bob:key",
			Data: []byte(`{"file_path":""}`), CreatedAt: createdAt.UnixMicro(),
		},
		{
			MessageUID: "new", SenderLogin: "me", Login: "carol:key", ChatID: "carol:key",
			Data: []byte(`{"text":"new"}`), CreatedAt: createdAt.Add(time.Second).UnixMicro(),
		},
	}

	inserted, err := s.ImportStateSyncMessages(delta)
	if err != nil {
		t.Fatalf("ImportStateSyncMessages: %v", err)
	}
	if len(inserted) != 1 || inserted[0].MessageUID != "new" {
		t.Fatalf("inserted = %#v, want only new", inserted)
	}
	inserted, err = s.ImportStateSyncMessages(delta)
	if err != nil {
		t.Fatalf("second ImportStateSyncMessages: %v", err)
	}
	if len(inserted) != 0 {
		t.Fatalf("second import inserted %d messages, want 0", len(inserted))
	}

	rows, err := s.GetMessages("bob:key")
	if err != nil {
		t.Fatalf("GetMessages: %v", err)
	}
	if len(rows) != 1 || string(rows[0]) != `{"file_path":"/local/file"}` {
		t.Fatalf("existing local data was overwritten: %q", rows)
	}
}
