package messenger

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/store"
)

type fileDownloadContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (m *Messenger) GetFileDownloads() ([]store.FileDownloadProgress, error) {
	return m.store.ListActiveFileDownloads(time.Now())
}

func (m *Messenger) CancelFileDownload(fileID string) error {
	m.downloadMu.Lock()
	defer m.downloadMu.Unlock()
	if err := m.store.CancelFileDownload(fileID, time.Now()); err != nil {
		return err
	}
	if download, ok := m.downloadContexts[fileID]; ok {
		download.cancel()
		delete(m.downloadContexts, fileID)
	}
	m.removePendingFileDownload(fileID)
	return nil
}

func (m *Messenger) beginFileDownload(fileID string) (context.Context, error) {
	m.downloadMu.Lock()
	defer m.downloadMu.Unlock()
	state, err := m.store.GetFileDownload(fileID)
	if err != nil {
		return nil, err
	}
	if state.CancelledAt != nil {
		return nil, store.ErrDownloadNotActive
	}
	if download, ok := m.downloadContexts[fileID]; ok {
		return download.ctx, nil
	}
	parent := m.ctx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	if m.downloadContexts == nil {
		m.downloadContexts = make(map[string]fileDownloadContext)
	}
	m.downloadContexts[fileID] = fileDownloadContext{ctx: ctx, cancel: cancel}
	return ctx, nil
}

func (m *Messenger) endFileDownload(fileID string) {
	m.downloadMu.Lock()
	defer m.downloadMu.Unlock()
	if download, ok := m.downloadContexts[fileID]; ok {
		download.cancel()
		delete(m.downloadContexts, fileID)
	}
}

// Write outside the cancellation lock; only publishing the completed file and
// its state is serialized with CancelFileDownload.
func (m *Messenger) completeFileDownload(ctx context.Context, fileID, path string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".huginn-download-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	_, writeErr := tmp.Write(data)
	closeErr := tmp.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	m.downloadMu.Lock()
	defer m.downloadMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	state, err := m.store.GetFileDownload(fileID)
	if err != nil {
		return err
	}
	if state.CancelledAt != nil {
		return store.ErrDownloadNotActive
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	return m.store.MarkFileDownloadCompleted(fileID, path, time.Now())
}
