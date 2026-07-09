package messenger

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/chunk"
	"github.com/killbane1232/huginn-messenger/internal/config"
	"github.com/killbane1232/huginn-messenger/internal/crypto"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
	"github.com/killbane1232/huginn-messenger/internal/store"
	"github.com/killbane1232/huginn-messenger/internal/webrtc"
	"github.com/google/uuid"
    //"runtime/debug"
)

func (m *Messenger) handleChunkStore(peerID string, req webrtc.ChunkStoreRequest) {
	// Если мы не являемся конечным получателем — сообщаем серверу, что сохранили чанк
	if req.RecipientID != "" && req.RecipientID != m.Key && req.Hash != "" && req.SenderID != "" {
		reportedPayload := fmt.Sprintf("muninn/reported/v1\n%s\n%d\n%s\n%s",
			req.FileID, req.ChunkIndex, req.Hash, req.SenderID)
		sig := crypto.Sign(m.signPrivate, []byte(reportedPayload))
		reportReq := muninn.ChunkReportRequest{
			ReporterID: m.ID,
			FileID:     req.FileID,
			ChunkIndex: req.ChunkIndex,
			Hash:       req.Hash,
			Signature:  crypto.EncodeKey(sig),
		}
		if err := m.muninnClient.ReportChunk(m.ctx, req.SenderID, reportReq); err != nil {
			log.Printf("report chunk %s/%d failed, not saving: %v", req.FileID, req.ChunkIndex, err)
			return
		}
		log.Printf("reported chunk %s/%d as storage peer", req.FileID, req.ChunkIndex)
	}

	ttl := req.TTLSeconds
	if ttl <= 0 {
		ttl = 604800
	}
	if err := m.store.StoreChunk(req.FileID, req.ChunkIndex, req.Data, ttl); err != nil {
		log.Printf("store chunk %s/%d: %v", req.FileID, req.ChunkIndex, err)
		return
	}
		log.Printf("stored chunk %s/%d from %s", req.FileID, req.ChunkIndex, peerID)
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

func (m *Messenger) distributePendingChunks() {
	chunks, err := m.store.GetUnplacedChunks()
	if err != nil {
		log.Printf("get unplaced chunks: %v", err)
		return
	}
	if len(chunks) == 0 {
		return
	}

	byRecipient := make(map[string][]store.PendingChunk)
	for _, c := range chunks {
		if (byRecipient[c.RecipientID] == nil) {
			byRecipient[c.RecipientID] = []store.PendingChunk{}
		}
		byRecipient[c.RecipientID] = append(byRecipient[c.RecipientID], c)
	}

	for recipientID, recipientChunks := range byRecipient {
		m.distributeChunksForRecipient(recipientID, recipientChunks)
	}
}


func (m *Messenger) distributeChunksForRecipient(recipientID string, chunks []store.PendingChunk) {
	onlinePeers, err := m.muninnClient.GetBestPeers(m.ctx, 10)
	if err != nil {
		onlinePeers = m.getOnlinePeers()
	}

	// Подключаем пиры
	var storagePeers []string
	for _, p := range onlinePeers {
		if p.ID == m.ID || p.Key() == recipientID {
			continue
		}
		if !m.IsPeerConnected(p.ID) {
			m.ConnectPeer(p.ID)
		}
		storagePeers = append(storagePeers, p.ID)
	}

	
	if len(storagePeers) == 0 {
		return
	}

	for i := 0; i < 30 && len(storagePeers) > 0; i++ {
		time.Sleep(100 * time.Millisecond)
		allConnected := true
		for _, pid := range storagePeers {
			if !m.IsPeerConnected(pid) {
				allConnected = false
				break
			}
		}
		if allConnected {
			break
		}
	}

	byPeer := make(map[string][]store.PendingChunk)
	for i, c := range chunks {
		pid := storagePeers[i%len(storagePeers)]
		byPeer[pid] = append(byPeer[pid], c)
	}

	for pid, peerChunks := range byPeer {
		if !m.IsPeerConnected(pid) {
			continue
		}

		byFile := make(map[string][]store.PendingChunk)
		for _, c := range peerChunks {
			byFile[c.FileID] = append(byFile[c.FileID], c)
		}

		for fileID, fileChunks := range byFile {
			ttlSeconds := fileChunks[0].TTLSeconds
			batch := make([]webrtc.ChunkStoreRequest, len(fileChunks))
			regBatch := make([]muninn.RegisterChunkBatchEntry, len(fileChunks))
			for i, c := range fileChunks {
				batch[i] = webrtc.ChunkStoreRequest{
					FileID: c.FileID, ChunkIndex: c.ChunkIndex, Data: c.Data,
					SenderID: c.SenderID, RecipientID: c.RecipientID, Hash: c.Hash,
					Signature: c.Signature, TTLSeconds: ttlSeconds,
				}
				regBatch[i] = muninn.RegisterChunkBatchEntry{
					ChunkIndex: c.ChunkIndex, SenderID: c.SenderID, RecipientID: c.RecipientID,
					Hash: c.Hash, Signature: c.Signature, PeerID: pid,
				}
			}

			if err := m.muninnClient.RegisterChunks(m.ctx, fileID, muninn.RegisterChunkBatchRequest{Chunks: regBatch}); err != nil {
				log.Printf("register batch %s on %s: %v", fileID, pid, err)
				continue
			}

			if err := m.rtcManager.SendChunkStoreBatch(pid, webrtc.ChunkStoreBatchRequest{Chunks: batch}); err != nil {
				log.Printf("distribute batch %s to %s: %v", fileID, pid, err)
				continue
			}

			for _, c := range fileChunks {
				if err := m.store.MarkChunkPlaced(c.FileID, c.ChunkIndex); err != nil {
					log.Printf("mark chunk placed %s/%d: %v", c.FileID, c.ChunkIndex, err)
				}
			}
		}
	}
}

func (m *Messenger) replicatePendingChunks() {
	return
	fileIDs, err := m.store.ListChunkFiles()
	if err != nil {
		log.Printf("list chunk files: %v", err)
		return
	}
	if len(fileIDs) == 0 {
		return
	}

	peers := m.getConnectedPeers()
	if len(peers) == 0 {
		return
	}

	for _, fileID := range fileIDs {
		chunkMap, err := m.store.ListChunks(fileID)
		if err != nil {
			continue
		}
		for _, peer := range peers {
			batch := make([]webrtc.ChunkStoreRequest, 0, len(chunkMap))
			for idx, data := range chunkMap {
				batch = append(batch, webrtc.ChunkStoreRequest{
					FileID: fileID, ChunkIndex: idx, Data: data, TTLSeconds: 604800,
				})
			}
			if len(batch) == 0 {
				continue
			}
			if err := m.rtcManager.SendChunkStoreBatch(peer.ID, webrtc.ChunkStoreBatchRequest{Chunks: batch}); err != nil {
				log.Printf("replicate chunks %s to %s: %v", fileID, peer.ID, err)
				m.DisconnectPeer(peer.ID)
			}
		}
	}
}

func (m *Messenger) sendFileChunks(recipientID, filePath string, ttlSeconds int) (*FileMeta, error) {
	filedata, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}

	fileID := uuid.New().String()
	filename := filepath.Base(filePath)

	aesKey := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, aesKey); err != nil {
		return nil, fmt.Errorf("generate file key: %w", err)
	}
	fileHash := sha256.Sum256(filedata)
	fileHashB64 := base64.StdEncoding.EncodeToString(fileHash[:])

	envelopes, err := chunk.SplitAndEncryptFile(fileID, m.ID, filedata, aesKey, m.signPrivate)
	if err != nil {
		return nil, fmt.Errorf("split encrypt file: %w", err)
	}

	if ttlSeconds <= 0 {
		ttlSeconds = config.ChunkTTLSeconds("1w")
	}

	type fileChunkData struct {
		envData []byte
		hash    string
		sig     string
	}
	chunks := make([]fileChunkData, len(envelopes))
	for i, env := range envelopes {
		envData, err := chunk.MarshalEnvelope(env)
		if err != nil {
			return nil, fmt.Errorf("marshal file env %d: %w", i, err)
		}
		if err := m.store.StoreChunk(fileID, i, envData, ttlSeconds); err != nil {
			return nil, fmt.Errorf("store file chunk %d: %w", i, err)
		}
		chunkHash := chunk.RegisteredHash(envData)
		expectedPayload := fmt.Sprintf("muninn/expected/v1\n%s\n%d\n%s", fileID, i, chunkHash)
		sig := crypto.Sign(m.signPrivate, []byte(expectedPayload))
		chunks[i] = fileChunkData{envData, chunkHash, crypto.EncodeKey(sig)}
	}

	thickPeers, err := m.muninnClient.GetBestThickPeers(m.ctx, 5)
	if err != nil {
		log.Printf("get best thick peers: %v, fallback to best peers", err)
		allPeers, err2 := m.muninnClient.GetBestPeers(m.ctx, 5)
		if err2 != nil {
			thickPeers = m.getOnlinePeers()
		} else {
			thickPeers = allPeers
		}
	}

	storagePeers := []string{}
	for _, p := range thickPeers {
		if p.ID == m.ID {
			continue
		}
		if !m.IsPeerConnected(p.ID) {
			m.ConnectPeer(p.ID)
		}
		storagePeers = append(storagePeers, p.ID)
	}

	for i := 0; i < 30 && len(storagePeers) > 0; i++ {
		time.Sleep(100 * time.Millisecond)
		allConnected := true
		for _, pid := range storagePeers {
			if !m.IsPeerConnected(pid) {
				allConnected = false
				break
			}
		}
		if allConnected {
			break
		}
	}

		for _, pid := range storagePeers {
		if !m.IsPeerConnected(pid) {
			continue
		}

		batch := make([]webrtc.ChunkStoreRequest, len(chunks))
		regBatch := make([]muninn.RegisterChunkBatchEntry, len(chunks))
		for i, c := range chunks {
			batch[i] = webrtc.ChunkStoreRequest{
				FileID: fileID, ChunkIndex: i, Data: c.envData,
				SenderID: m.Key, Hash: c.hash, Signature: c.sig,
				TTLSeconds: ttlSeconds,
			}
			regBatch[i] = muninn.RegisterChunkBatchEntry{
				ChunkIndex: i, SenderID: m.Key, Hash: c.hash,
				Signature: c.sig, PeerID: pid, Persist: true,
			}
		}

		if err := m.muninnClient.RegisterChunks(m.ctx, fileID, muninn.RegisterChunkBatchRequest{Chunks: regBatch}); err != nil {
			log.Printf("register file chunks %s on %s: %v", fileID, pid, err)
			continue
		}

		if err := m.rtcManager.SendChunkStoreBatch(pid, webrtc.ChunkStoreBatchRequest{Chunks: batch}); err != nil {
			log.Printf("distribute file chunks %s to %s: %v", fileID, pid, err)
		}
	}

	localRegBatch := make([]muninn.RegisterChunkBatchEntry, len(chunks))
	for i, c := range chunks {
		localRegBatch[i] = muninn.RegisterChunkBatchEntry{
			ChunkIndex: i, SenderID: m.Key,
			Hash: c.hash, Signature: c.sig, PeerID: m.ID, Persist: true,
		}
	}
	if err := m.muninnClient.RegisterChunks(m.ctx, fileID, muninn.RegisterChunkBatchRequest{Chunks: localRegBatch}); err != nil {
		log.Printf("register file chunks %s on self: %v", fileID, err)
	}

	log.Printf("file %s sent as %s (%d chunks)", filename, fileID, len(chunks))
	return &FileMeta{FileID: fileID, FileHash: fileHashB64, DecryptionKey: crypto.EncodeKey(aesKey), TotalChunks: len(chunks), Filename: filename}, nil
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

		allOk := true
		for _, rec := range records {
			if !m.store.IsChunkFailed(rec.FileID, rec.ChunkIndex) {
				continue
			}
			data, ok := m.getChunkData(rec)
			if !ok {
				allOk = false
				continue
			}
			if rec.Hash != "" && chunk.RegisteredHash(data) != rec.Hash {
				allOk = false
				continue
			}
			m.store.DeleteFailedChunk(rec.FileID, rec.ChunkIndex)
		}
		if allOk {
			m.collectAndProcessMessage(fc.FileID, records)
		}
	}
}

func (m *Messenger) deleteChunksAndReturn(msgID string) {
	if err := m.store.DeleteChunks(msgID); err != nil {
		log.Printf("delete chunks for %s: %v", msgID, err)
	}
}

func (m *Messenger) requestMissingChunk(fileID string, chunkIndex int, senderID string) {
	targets := []string{}	
	p := m.findPeerByKey(senderID)
	if p == nil {
		return
	}
	targets = p.IDS
	for _, pid := range targets {
		if m.IsPeerConnected(pid) {
			m.rtcManager.SendChunkGet(pid, webrtc.ChunkGetRequest{
				FileID: fileID, ChunkIndex: chunkIndex,
			})
		} else if pid == senderID {
			go m.ConnectPeer(pid)
		}
	}
}

func (m *Messenger) getChunkData(rec muninn.ChunkRecord) ([]byte, bool) {
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
		go func() {
			for i := 0; i < 50; i++ {
				select {
				case <-m.ctx.Done():
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
		}()
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
	go m.checkPendingMessages()
	go m.checkPendingFileDownloads()
}

func (m *Messenger) ListFailedChunks() ([]store.FailedChunk, error) {
	return m.store.ListFailedChunks(m.Key)
}

func (m *Messenger) IsChunkFailed(fileID string, chunkIndex int) bool {
	return m.store.IsChunkFailed(fileID, chunkIndex)
}