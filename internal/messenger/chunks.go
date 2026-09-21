package messenger

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/chunk"
	"github.com/killbane1232/huginn-messenger/internal/crypto"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
	"github.com/killbane1232/huginn-messenger/internal/store"
	"github.com/killbane1232/huginn-messenger/internal/webrtc"
	//"runtime/debug"
)

func (m *Messenger) handleChunkStore(peerID string, req webrtc.ChunkStoreRequest) error {
	if req.Hash != "" && chunk.RegisteredHash(req.Data) != req.Hash {
		return fmt.Errorf("chunk hash mismatch")
	}
	ttl := req.TTLSeconds
	if ttl <= 0 {
		ttl = 604800
	}
	if err := m.store.StoreChunk(req.FileID, req.ChunkIndex, req.Data, ttl); err != nil {
		return err
	}
	// A quality-report outage must neither discard data nor prevent the ACK
	// for data that is already safely stored.
	if req.RecipientID != "" && req.RecipientID != m.Key && req.Hash != "" && req.SenderID != "" {
		report := func() {
			payload := fmt.Sprintf("muninn/reported/v1\n%s\n%d\n%s\n%s", req.FileID, req.ChunkIndex, req.Hash, peerID)
			request := muninn.ChunkReportRequest{ReporterID: m.ID, FileID: req.FileID, ChunkIndex: req.ChunkIndex, Hash: req.Hash, Signature: crypto.EncodeKey(crypto.Sign(m.signPrivate, []byte(payload)))}
			if err := m.muninnClient.ReportChunk(m.ctx, peerID, request); err != nil {
				log.Printf("chunk %s/%d retained despite report failure", req.FileID, req.ChunkIndex)
			}
		}
		if m.async != nil {
			m.async.trySubmit(report)
		} else {
			report()
		}
	}
	m.pendingMu.Lock()
	_, waitingForFile := m.pendingFileDownloads[req.FileID]
	m.pendingMu.Unlock()
	if waitingForFile && m.async != nil {
		m.async.trySubmit(m.checkPendingFileDownloads)
	}
	return nil
}

func (m *Messenger) handleChunkGet(peerID string, req webrtc.ChunkGetRequest) ([]byte, bool) {
	data, err := m.store.GetChunk(req.FileID, req.ChunkIndex)
	if err != nil || data == nil {
		log.Printf("chunk get err: %v %s %d", err, req.FileID, req.ChunkIndex)
		return nil, false
	}
	log.Printf("sent chunk: %s %d", req.FileID, req.ChunkIndex)
	return data, true
}

func (m *Messenger) pendingChunkLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.distributePendingChunks()
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Messenger) chunkCleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			now := time.Now()
			if err := m.store.DeleteExpiredChunks(now); err != nil {
				log.Printf("cleanup expired chunks: %v", err)
			}
			if err := m.store.DeleteExpiredPendingChunks(now); err != nil {
				log.Printf("cleanup expired pending chunks: %v", err)
			}
			if err := m.store.DeleteChunksWithMessage(); err != nil {
				log.Printf("cleanup message chunks: %v", err)
			}
			if err := m.store.DeleteExpiredFailedChunks(time.Now().Unix()); err != nil {
				log.Printf("cleanup expired failed chunks: %v", err)
			}
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Messenger) retryFailedChunks(recipientID string) {
	failed, err := m.store.ListFailedChunks(recipientID)
	if err != nil || len(failed) == 0 {
		return
	}

	now := time.Now().Unix()
	seenFile := make(map[string]bool)

	for _, fc := range failed {
		if fc.CreatedAt+int64(fc.TTLSeconds) <= now {
			m.store.DeleteFailedChunk(fc.FileID, fc.ChunkIndex)
			continue
		}
		if seenFile[fc.FileID] {
			continue
		}
		seenFile[fc.FileID] = true

		records, err := m.muninnClient.GetChunksByFileID(m.ctx, fc.FileID)
		if err != nil || len(records) == 0 {
			continue
		}

		// Keep the retry record through validation, sender lookup, decryption,
		// and the message transaction. Receiving bytes alone is not delivery.
		m.collectAndProcessMessage(fc.FileID, records)
	}
}

func (m *Messenger) deleteChunksAndReturn(msgID string) {
	if err := m.store.DeleteChunks(msgID); err != nil {
		log.Printf("delete chunks for %s: %v", msgID, err)
	}
}

func (m *Messenger) requestMissingChunk(fileID string, chunkIndex int, senderID string) {
	m.requestMissingChunkWithContext(m.ctx, fileID, chunkIndex, senderID)
}

func (m *Messenger) requestMissingChunkWithContext(ctx context.Context, fileID string, chunkIndex int, senderID string) {
	targets := []string{}
	p := m.findPeerByKey(senderID)
	if p == nil {
		return
	}
	targets = p.IDS
	for _, pid := range targets {
		if ctx.Err() != nil {
			return
		}
		if m.IsPeerConnected(pid) {
			m.rtcManager.SendChunkGet(pid, webrtc.ChunkGetRequest{
				FileID: fileID, ChunkIndex: chunkIndex,
			})
		} else if pid == senderID {
			m.async.trySubmit(func() {
				if ctx.Err() != nil {
					return
				}
				if err := m.ConnectPeer(pid); err != nil {
					log.Printf("connect to %s for missing chunk: %v", pid, err)
				}
			})
		}
	}
}

func (m *Messenger) requestMissingChunkFromPeer(ctx context.Context, fileID string, chunkIndex int, senderID, preferredPeerID string) {
	if ctx.Err() != nil {
		return
	}
	if preferredPeerID != "" && m.IsPeerConnected(preferredPeerID) {
		_ = m.rtcManager.SendChunkGet(preferredPeerID, webrtc.ChunkGetRequest{
			FileID: fileID, ChunkIndex: chunkIndex,
		})
	} else if preferredPeerID != "" && chunkIndex == 0 {
		m.async.trySubmit(func() {
			if ctx.Err() != nil {
				return
			}
			if err := m.ConnectPeer(preferredPeerID); err != nil {
				log.Printf("connect to relogin source %s for file %s: %v", preferredPeerID, fileID, err)
				return
			}
			for attempt := 0; attempt < 50; attempt++ {
				select {
				case <-ctx.Done():
					return
				case <-time.After(100 * time.Millisecond):
				}
				if m.IsPeerConnected(preferredPeerID) {
					_ = m.rtcManager.SendChunkGet(preferredPeerID, webrtc.ChunkGetRequest{
						FileID: fileID, ChunkIndex: chunkIndex,
					})
					return
				}
			}
		})
	}

	m.requestMissingChunkWithContext(ctx, fileID, chunkIndex, senderID)
	records, err := m.muninnClient.GetChunksByFileID(ctx, fileID)
	if err != nil {
		return
	}
	for _, record := range records {
		if record.ChunkIndex != chunkIndex {
			continue
		}
		if _, ok := m.getChunkDataWithContext(ctx, record); ok {
			return
		}
	}
}

func (m *Messenger) getChunkData(rec muninn.ChunkRecord) ([]byte, bool) {
	return m.getChunkDataWithContext(m.ctx, rec)
}

func (m *Messenger) getChunkDataWithContext(ctx context.Context, rec muninn.ChunkRecord) ([]byte, bool) {
	if ctx.Err() != nil {
		return nil, false
	}
	data, err := m.store.GetChunk(rec.FileID, rec.ChunkIndex)
	if err == nil && data != nil {
		return data, true
	}

	if rec.PeerID == m.ID {
		return nil, false
	}

	log.Printf("send chunk get to %s", rec.PeerID)
	if m.IsPeerConnected(rec.PeerID) {
		m.rtcManager.SendChunkGet(rec.PeerID, webrtc.ChunkGetRequest{
			FileID:     rec.FileID,
			ChunkIndex: rec.ChunkIndex,
		})
	} else {
		m.ConnectPeer(rec.PeerID)
		m.async.trySubmit(func() {
			for i := 0; i < 50; i++ {
				select {
				case <-ctx.Done():
					return
				case <-time.After(100 * time.Millisecond):
				}
				if m.IsPeerConnected(rec.PeerID) {
					m.rtcManager.SendChunkGet(rec.PeerID, webrtc.ChunkGetRequest{
						FileID:     rec.FileID,
						ChunkIndex: rec.ChunkIndex,
					})
					return
				}
			}
			log.Printf("getChunkData: failed to connect to %s within 5s", rec.PeerID)
		})
	}

	return nil, false
}

func (m *Messenger) StoredChunkData(fileID string, chunkIndex int) ([]byte, bool) {
	data, err := m.store.GetChunk(fileID, chunkIndex)
	if err != nil || data == nil {
		return nil, false
	}
	return data, true
}

func (m *Messenger) InjectChunk(fileID string, chunkIndex int, data []byte) {
	if err := m.store.StoreChunk(fileID, chunkIndex, data, 604800); err != nil {
		log.Printf("inject chunk: %v", err)
	}
	m.async.submit(func() {
		m.checkPendingMessages()
		m.checkPendingFileDownloads()
	})
}

func (m *Messenger) ListFailedChunks() ([]store.FailedChunk, error) {
	return m.store.ListFailedChunks(m.Key)
}

func (m *Messenger) IsChunkFailed(fileID string, chunkIndex int) bool {
	return m.store.IsChunkFailed(fileID, chunkIndex)
}
