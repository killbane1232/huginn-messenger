package store

import (
	"database/sql"
	"fmt"
	"time"
)

type FileDownloadState struct {
	FileID      string
	FirstSeenAt time.Time
	ExpiresAt   time.Time
	CompletedAt *time.Time
	StoppedAt   *time.Time
	LocalPath   string
}

func (s *SQLiteStore) EnsureFileDownload(fileID string, firstSeenAt time.Time, ttl time.Duration) (FileDownloadState, error) {
	if fileID == "" {
		return FileDownloadState{}, fmt.Errorf("file id is empty")
	}
	if ttl <= 0 {
		return FileDownloadState{}, fmt.Errorf("file download ttl must be positive")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	firstSeenAt = firstSeenAt.UTC()
	if _, err := s.db.Exec(`
		INSERT OR IGNORE INTO file_downloads (file_id, first_seen_at, expires_at)
		VALUES (?, ?, ?)`,
		fileID,
		firstSeenAt.Unix(),
		firstSeenAt.Add(ttl).Unix(),
	); err != nil {
		return FileDownloadState{}, fmt.Errorf("start file download %s: %w", fileID, err)
	}

	return readFileDownloadState(s.db.QueryRow(`
		SELECT file_id, first_seen_at, expires_at, completed_at, stopped_at, local_path
		FROM file_downloads
		WHERE file_id = ?`, fileID))
}

func (s *SQLiteStore) GetFileDownload(fileID string) (FileDownloadState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return readFileDownloadState(s.db.QueryRow(`
		SELECT file_id, first_seen_at, expires_at, completed_at, stopped_at, local_path
		FROM file_downloads
		WHERE file_id = ?`, fileID))
}

func readFileDownloadState(row *sql.Row) (FileDownloadState, error) {
	var state FileDownloadState
	var firstSeenAt int64
	var expiresAt int64
	var completedAt sql.NullInt64
	var stoppedAt sql.NullInt64
	if err := row.Scan(
		&state.FileID,
		&firstSeenAt,
		&expiresAt,
		&completedAt,
		&stoppedAt,
		&state.LocalPath,
	); err != nil {
		return FileDownloadState{}, err
	}
	state.FirstSeenAt = time.Unix(firstSeenAt, 0).UTC()
	state.ExpiresAt = time.Unix(expiresAt, 0).UTC()
	if completedAt.Valid {
		value := time.Unix(completedAt.Int64, 0).UTC()
		state.CompletedAt = &value
	}
	if stoppedAt.Valid {
		value := time.Unix(stoppedAt.Int64, 0).UTC()
		state.StoppedAt = &value
	}
	return state, nil
}

func (s *SQLiteStore) MarkFileDownloadCompleted(fileID, localPath string, completedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.Exec(`
		UPDATE file_downloads
		SET completed_at = ?, stopped_at = NULL, local_path = ?
		WHERE file_id = ?`,
		completedAt.UTC().Unix(), localPath, fileID,
	)
	if err != nil {
		return fmt.Errorf("complete file download %s: %w", fileID, err)
	}
	if rows, err := result.RowsAffected(); err != nil {
		return err
	} else if rows == 0 {
		return fmt.Errorf("file download %s not found", fileID)
	}
	return nil
}

func (s *SQLiteStore) ResetFileDownloadCompletion(fileID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`
		UPDATE file_downloads
		SET completed_at = NULL, local_path = ''
		WHERE file_id = ?`, fileID)
	return err
}

func (s *SQLiteStore) MarkFileDownloadStopped(fileID string, stoppedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`
		UPDATE file_downloads
		SET stopped_at = ?
		WHERE file_id = ? AND completed_at IS NULL`,
		stoppedAt.UTC().Unix(), fileID,
	)
	return err
}
