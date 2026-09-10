package store

import "time"

// SQLite julianday does not understand every historical time.Time encoding
// (including the timezone suffix written by the Go SQLite driver).
func (s *SQLiteStore) deleteExpiredPayloads(table string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query("SELECT file_id, chunk_index, created_at, ttl_seconds FROM " + table + " WHERE ttl_seconds > 0")
	if err != nil {
		return err
	}
	type key struct {
		fileID string
		index  int
	}
	var expired []key
	for rows.Next() {
		var k key
		var created time.Time
		var ttl int
		if err := rows.Scan(&k.fileID, &k.index, &created, &ttl); err != nil {
			rows.Close()
			return err
		}
		if !now.Before(created.Add(time.Duration(ttl) * time.Second)) {
			expired = append(expired, k)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil || len(expired) == 0 {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	statement, err := tx.Prepare("DELETE FROM " + table + " WHERE file_id = ? AND chunk_index = ?")
	if err != nil {
		return err
	}
	defer statement.Close()
	for _, k := range expired {
		if _, err := statement.Exec(k.fileID, k.index); err != nil {
			return err
		}
	}
	return tx.Commit()
}
