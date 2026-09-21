package store

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type FileDownloadState struct {
	FileID      string
	FirstSeenAt time.Time
	ExpiresAt   time.Time
	CompletedAt *time.Time
	StoppedAt   *time.Time
	CancelledAt *time.Time
	LocalPath   string
}

func (s *SQLiteStore) EnsureFileDownload(fileID string, firstSeenAt time.Time, ttl time.Duration) (FileDownloadState, error) {
	return s.TrackFileDownload(fileID, "", 0, firstSeenAt, ttl)
}

func (s *SQLiteStore) TrackFileDownload(fileID, filename string, totalChunks int, firstSeenAt time.Time, ttl time.Duration) (FileDownloadState, error) {
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
		INSERT INTO file_downloads (file_id, first_seen_at, expires_at, filename, total_chunks)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(file_id) DO UPDATE SET
			filename = CASE WHEN excluded.total_chunks > 0 THEN excluded.filename ELSE filename END,
			total_chunks = CASE WHEN excluded.total_chunks > 0 THEN excluded.total_chunks ELSE total_chunks END`,
		fileID,
		firstSeenAt.Unix(),
		firstSeenAt.Add(ttl).Unix(),
		filename,
		totalChunks,
	); err != nil {
		return FileDownloadState{}, fmt.Errorf("start file download %s: %w", fileID, err)
	}

	return readFileDownloadState(s.db.QueryRow(`
		SELECT file_id, first_seen_at, expires_at, completed_at, stopped_at, local_path, cancelled_at
		FROM file_downloads
		WHERE file_id = ?`, fileID))
}

func (s *SQLiteStore) GetFileDownload(fileID string) (FileDownloadState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return readFileDownloadState(s.db.QueryRow(`
		SELECT file_id, first_seen_at, expires_at, completed_at, stopped_at, local_path, cancelled_at
		FROM file_downloads
		WHERE file_id = ?`, fileID))
}

func readFileDownloadState(row *sql.Row) (FileDownloadState, error) {
	var state FileDownloadState
	var firstSeenAt int64
	var expiresAt int64
	var completedAt sql.NullInt64
	var stoppedAt sql.NullInt64
	var cancelledAt sql.NullInt64
	if err := row.Scan(
		&state.FileID,
		&firstSeenAt,
		&expiresAt,
		&completedAt,
		&stoppedAt,
		&state.LocalPath,
		&cancelledAt,
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
	if cancelledAt.Valid {
		value := time.Unix(cancelledAt.Int64, 0).UTC()
		state.CancelledAt = &value
	}
	return state, nil
}

func (s *SQLiteStore) MarkFileDownloadCompleted(fileID, localPath string, completedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.Exec(`
		UPDATE file_downloads
		SET completed_at = ?, stopped_at = NULL, local_path = ?
		WHERE file_id = ? AND cancelled_at IS NULL`,
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

var ErrDownloadNotActive = errors.New("download is no longer active")

func (s *SQLiteStore) CancelFileDownload(fileID string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := s.db.Exec(`UPDATE file_downloads SET cancelled_at = ?, stopped_at = ?
		WHERE file_id = ? AND completed_at IS NULL AND cancelled_at IS NULL`, now.Unix(), now.Unix(), fileID)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrDownloadNotActive
	}
	return nil
}

type FileDownloadProgress struct {
	FileID         string `json:"file_id"`
	Filename       string `json:"filename"`
	TotalChunks    int    `json:"total_chunks"`
	ReceivedChunks int    `json:"received_chunks"`
}

// Read metadata and counts without loading encrypted chunk bodies into memory.
// A file referenced by multiple messages appears only once.
func (s *SQLiteStore) ListActiveFileDownloads(now time.Time) ([]FileDownloadProgress, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`
		SELECT d.file_id, d.filename, d.total_chunks,
			(SELECT COUNT(*) FROM chunks c WHERE c.file_id = d.file_id
			 AND c.chunk_index >= 0 AND c.chunk_index < d.total_chunks) AS received
		FROM file_downloads d
		WHERE d.completed_at IS NULL AND d.stopped_at IS NULL AND d.cancelled_at IS NULL
			AND d.expires_at > ? AND d.total_chunks > 0
		ORDER BY d.first_seen_at, d.file_id`, now.Unix())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]FileDownloadProgress, 0)
	for rows.Next() {
		var item FileDownloadProgress
		if err := rows.Scan(&item.FileID, &item.Filename, &item.TotalChunks, &item.ReceivedChunks); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
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
