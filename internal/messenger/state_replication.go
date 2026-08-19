package messenger

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/killbane1232/huginn-messenger/internal/crypto"
	"github.com/killbane1232/huginn-messenger/internal/store"
	"github.com/killbane1232/huginn-messenger/internal/webrtc"
)

const (
	stateSyncVersion        = 1
	stateSyncMaxChunkCount  = 4096
	stateSyncMaxDecodedSize = 256 * 1024 * 1024
)

type stateSyncIncomingTransfer struct {
	transferID   string
	checkpoint   int64
	expectedHash string
	chunks       [][]byte
	received     int
	processing   bool
}

type stateSyncOutgoingTransfer struct {
	transferID string
	checkpoint int64
}

func (m *Messenger) refreshReplicaConnections() {
	if m.Username == "" || len(m.signPublic) == 0 {
		return
	}

	signature := crypto.EncodeKey(m.signPublic)
	peers, err := m.muninnClient.GetAllByKey(m.ctx, m.Username, signature)
	if err != nil {
		if m.ctx.Err() == nil {
			log.Printf("state sync: discover equivalent peers: %v", err)
		}
		return
	}

	now := time.Now()
	seen := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		if peer.ID == "" || peer.ID == m.ID || peer.IsFake || peer.Key() != m.Key {
			continue
		}
		if peer.TTLSeconds <= 0 || !peer.LastSeen.Add(time.Duration(peer.TTLSeconds)*time.Second).After(now) {
			continue
		}
		if _, duplicate := seen[peer.ID]; duplicate {
			continue
		}
		seen[peer.ID] = struct{}{}

		m.stateSyncMu.Lock()
		m.replicaPeers[peer.ID] = true
		m.stateSyncMu.Unlock()

		if m.IsPeerConnected(peer.ID) {
			m.handlePeerConnected(peer.ID)
			continue
		}
		if err := m.ConnectPeer(peer.ID); err != nil {
			log.Printf("state sync: connect equivalent peer %s: %v", peer.ID, err)
		}
	}
}

func (m *Messenger) isAuthorizedReplicaPeer(peerID string) bool {
	if peerID == "" || peerID == m.ID {
		return false
	}
	m.stateSyncMu.Lock()
	authorized := m.replicaPeers[peerID]
	m.stateSyncMu.Unlock()
	if authorized {
		return true
	}

	peer, err := m.muninnClient.Get(m.ctx, peerID)
	if err != nil || peer == nil {
		return false
	}
	if peer.ID != peerID || peer.IsFake || peer.Key() != m.Key || peer.TTLSeconds <= 0 {
		return false
	}
	if !peer.LastSeen.Add(time.Duration(peer.TTLSeconds) * time.Second).After(time.Now()) {
		return false
	}

	m.stateSyncMu.Lock()
	m.replicaPeers[peerID] = true
	m.stateSyncMu.Unlock()
	return true
}

func (m *Messenger) handlePeerConnected(peerID string) {
	if !m.isAuthorizedReplicaPeer(peerID) {
		return
	}

	m.stateSyncMu.Lock()
	if m.stateSyncRequested[peerID] {
		m.stateSyncMu.Unlock()
		return
	}
	m.stateSyncRequested[peerID] = true
	m.stateSyncMu.Unlock()

	if err := m.rtcManager.SendStateSyncRequest(peerID, webrtc.StateSyncRequest{}); err != nil {
		m.stateSyncMu.Lock()
		delete(m.stateSyncRequested, peerID)
		m.stateSyncMu.Unlock()
		log.Printf("state sync: request messages from %s: %v", peerID, err)
	}
}

func (m *Messenger) handlePeerDisconnected(peerID string) {
	if m.IsPeerConnected(peerID) {
		return
	}
	m.stateSyncMu.Lock()
	delete(m.stateSyncRequested, peerID)
	delete(m.stateSyncIncoming, peerID)
	delete(m.stateSyncOutgoing, peerID)
	m.stateSyncMu.Unlock()
}

func (m *Messenger) handleStateSyncRequest(peerID string, _ webrtc.StateSyncRequest) {
	if !m.isAuthorizedReplicaPeer(peerID) {
		log.Printf("state sync: rejected request from non-equivalent peer %s", peerID)
		return
	}
	if err := m.sendStateSync(peerID); err != nil {
		log.Printf("state sync: send messages to %s: %v", peerID, err)
		_ = m.rtcManager.SendStateSyncManifest(peerID, webrtc.StateSyncManifest{Error: "build or send state sync"})
	}
}

func (m *Messenger) sendStateSync(peerID string) error {
	since, err := m.store.GetLastStateCheck(peerID)
	if err != nil {
		return err
	}
	checkpoint := time.Now().UTC()
	delta, err := m.store.ExportMessagesForStateSync(since, checkpoint)
	if err != nil {
		return err
	}
	if err := m.sanitizeStateSyncMessages(delta.Messages); err != nil {
		return err
	}

	compressed, digest, err := encodeStateSyncDelta(delta)
	if err != nil {
		return err
	}
	transferID := uuid.NewString()
	chunkCount := (len(compressed) + reloginChunkSize - 1) / reloginChunkSize
	if chunkCount <= 0 || chunkCount > stateSyncMaxChunkCount {
		return fmt.Errorf("state sync requires invalid chunk count %d", chunkCount)
	}

	transfer := &stateSyncOutgoingTransfer{
		transferID: transferID,
		checkpoint: delta.Checkpoint,
	}
	m.stateSyncMu.Lock()
	m.stateSyncOutgoing[peerID] = transfer
	m.stateSyncMu.Unlock()
	clearTransfer := true
	defer func() {
		if !clearTransfer {
			return
		}
		m.stateSyncMu.Lock()
		if m.stateSyncOutgoing[peerID] == transfer {
			delete(m.stateSyncOutgoing, peerID)
		}
		m.stateSyncMu.Unlock()
	}()

	if err := m.rtcManager.SendStateSyncManifest(peerID, webrtc.StateSyncManifest{
		TransferID: transferID,
		Checkpoint: delta.Checkpoint,
		ChunkCount: chunkCount,
		SHA256:     digest,
	}); err != nil {
		return err
	}
	for index := 0; index < chunkCount; index++ {
		if err := m.waitForReloginSendCapacity(peerID); err != nil {
			return err
		}
		start := index * reloginChunkSize
		end := min(start+reloginChunkSize, len(compressed))
		if err := m.rtcManager.SendStateSyncChunk(peerID, webrtc.StateSyncChunk{
			TransferID: transferID,
			Index:      index,
			Data:       compressed[start:end],
		}); err != nil {
			return err
		}
	}
	clearTransfer = false
	log.Printf("state sync: sent %d messages in %d chunks to %s", len(delta.Messages), chunkCount, peerID)
	return nil
}

func (m *Messenger) sanitizeStateSyncMessages(messages []store.ReplicatedMessage) error {
	for index := range messages {
		var message ChatMessage
		if err := json.Unmarshal(messages[index].Data, &message); err != nil {
			return fmt.Errorf("decode state sync message %s: %w", messages[index].MessageUID, err)
		}
		message.Files = withoutLocalFilePaths(message.Files)
		for fileIndex := range message.Files {
			message.Files[fileIndex].SourcePeerID = m.ID
		}
		data, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("encode state sync message %s: %w", messages[index].MessageUID, err)
		}
		messages[index].Data = data
	}
	return nil
}

func encodeStateSyncDelta(delta store.MessageReplicationDelta) ([]byte, string, error) {
	raw, err := json.Marshal(delta)
	if err != nil {
		return nil, "", fmt.Errorf("encode state sync delta: %w", err)
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(raw); err != nil {
		return nil, "", fmt.Errorf("compress state sync delta: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, "", fmt.Errorf("finish state sync compression: %w", err)
	}
	digest := sha256.Sum256(compressed.Bytes())
	return compressed.Bytes(), hex.EncodeToString(digest[:]), nil
}

func decodeStateSyncDelta(compressed []byte) (store.MessageReplicationDelta, error) {
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return store.MessageReplicationDelta{}, fmt.Errorf("open state sync delta: %w", err)
	}
	defer reader.Close()
	raw, err := io.ReadAll(io.LimitReader(reader, stateSyncMaxDecodedSize+1))
	if err != nil {
		return store.MessageReplicationDelta{}, fmt.Errorf("decompress state sync delta: %w", err)
	}
	if len(raw) > stateSyncMaxDecodedSize {
		return store.MessageReplicationDelta{}, fmt.Errorf("state sync delta exceeds %d bytes", stateSyncMaxDecodedSize)
	}
	var delta store.MessageReplicationDelta
	if err := json.Unmarshal(raw, &delta); err != nil {
		return store.MessageReplicationDelta{}, fmt.Errorf("decode state sync delta: %w", err)
	}
	if delta.Version != stateSyncVersion {
		return store.MessageReplicationDelta{}, fmt.Errorf("unsupported state sync version %d", delta.Version)
	}
	return delta, nil
}

func (m *Messenger) handleStateSyncManifest(peerID string, manifest webrtc.StateSyncManifest) {
	m.stateSyncMu.Lock()
	requested := m.stateSyncRequested[peerID]
	if !requested {
		m.stateSyncMu.Unlock()
		return
	}
	if manifest.Error != "" {
		delete(m.stateSyncRequested, peerID)
		delete(m.stateSyncIncoming, peerID)
		m.stateSyncMu.Unlock()
		log.Printf("state sync: peer %s returned error: %s", peerID, manifest.Error)
		return
	}
	if manifest.TransferID == "" || manifest.Checkpoint <= 0 || manifest.ChunkCount <= 0 ||
		manifest.ChunkCount > stateSyncMaxChunkCount || manifest.SHA256 == "" {
		delete(m.stateSyncRequested, peerID)
		delete(m.stateSyncIncoming, peerID)
		m.stateSyncMu.Unlock()
		log.Printf("state sync: invalid manifest from %s", peerID)
		return
	}
	m.stateSyncIncoming[peerID] = &stateSyncIncomingTransfer{
		transferID:   manifest.TransferID,
		checkpoint:   manifest.Checkpoint,
		expectedHash: manifest.SHA256,
		chunks:       make([][]byte, manifest.ChunkCount),
	}
	m.stateSyncMu.Unlock()
}

func (m *Messenger) handleStateSyncChunk(peerID string, chunk webrtc.StateSyncChunk) {
	m.stateSyncMu.Lock()
	transfer := m.stateSyncIncoming[peerID]
	if transfer == nil || transfer.transferID != chunk.TransferID || transfer.processing {
		m.stateSyncMu.Unlock()
		return
	}
	if chunk.Index < 0 || chunk.Index >= len(transfer.chunks) || len(chunk.Data) > reloginChunkSize {
		delete(m.stateSyncRequested, peerID)
		delete(m.stateSyncIncoming, peerID)
		m.stateSyncMu.Unlock()
		_ = m.rtcManager.SendStateSyncAck(peerID, webrtc.StateSyncAck{
			TransferID: chunk.TransferID,
			Checkpoint: transfer.checkpoint,
			Error:      "invalid state sync chunk",
		})
		return
	}
	if transfer.chunks[chunk.Index] == nil {
		transfer.chunks[chunk.Index] = append([]byte(nil), chunk.Data...)
		transfer.received++
	}
	if transfer.received != len(transfer.chunks) {
		m.stateSyncMu.Unlock()
		return
	}
	transfer.processing = true
	m.stateSyncMu.Unlock()

	if !m.async.submit(func() { m.finishStateSyncImport(peerID, transfer) }) {
		m.failStateSyncImport(peerID, transfer, fmt.Errorf("messenger is shutting down"))
	}
}

func (m *Messenger) finishStateSyncImport(peerID string, transfer *stateSyncIncomingTransfer) {
	total := 0
	for _, chunk := range transfer.chunks {
		total += len(chunk)
	}
	compressed := make([]byte, 0, total)
	for _, chunk := range transfer.chunks {
		compressed = append(compressed, chunk...)
	}
	digest := sha256.Sum256(compressed)
	if hex.EncodeToString(digest[:]) != transfer.expectedHash {
		m.failStateSyncImport(peerID, transfer, fmt.Errorf("state sync checksum mismatch"))
		return
	}
	delta, err := decodeStateSyncDelta(compressed)
	if err != nil {
		m.failStateSyncImport(peerID, transfer, err)
		return
	}
	if delta.Checkpoint != transfer.checkpoint {
		m.failStateSyncImport(peerID, transfer, fmt.Errorf("state sync checkpoint mismatch"))
		return
	}
	inserted, err := m.store.ImportStateSyncMessages(delta.Messages)
	if err != nil {
		m.failStateSyncImport(peerID, transfer, err)
		return
	}
	if len(inserted) > 0 {
		m.queueReplicatedFiles(inserted, peerID, m.Username)
	}

	m.clearIncomingStateSync(peerID, transfer)
	if err := m.rtcManager.SendStateSyncAck(peerID, webrtc.StateSyncAck{
		TransferID: transfer.transferID,
		Checkpoint: transfer.checkpoint,
	}); err != nil {
		log.Printf("state sync: acknowledge import from %s: %v", peerID, err)
		return
	}
	log.Printf("state sync: imported %d new messages from %s", len(inserted), peerID)
}

func (m *Messenger) failStateSyncImport(peerID string, transfer *stateSyncIncomingTransfer, syncErr error) {
	m.clearIncomingStateSync(peerID, transfer)
	log.Printf("state sync: import from %s: %v", peerID, syncErr)
	_ = m.rtcManager.SendStateSyncAck(peerID, webrtc.StateSyncAck{
		TransferID: transfer.transferID,
		Checkpoint: transfer.checkpoint,
		Error:      syncErr.Error(),
	})
}

func (m *Messenger) clearIncomingStateSync(peerID string, transfer *stateSyncIncomingTransfer) {
	m.stateSyncMu.Lock()
	if m.stateSyncIncoming[peerID] == transfer {
		delete(m.stateSyncIncoming, peerID)
		delete(m.stateSyncRequested, peerID)
	}
	m.stateSyncMu.Unlock()
}

func (m *Messenger) handleStateSyncAck(peerID string, ack webrtc.StateSyncAck) {
	m.stateSyncMu.Lock()
	transfer := m.stateSyncOutgoing[peerID]
	if transfer == nil || transfer.transferID != ack.TransferID || transfer.checkpoint != ack.Checkpoint {
		m.stateSyncMu.Unlock()
		return
	}
	if ack.Error != "" {
		delete(m.stateSyncOutgoing, peerID)
		m.stateSyncMu.Unlock()
		log.Printf("state sync: peer %s rejected transfer: %s", peerID, ack.Error)
		return
	}
	m.stateSyncMu.Unlock()

	checkedAt := time.UnixMicro(ack.Checkpoint).UTC()
	if err := m.store.SetLastStateCheck(peerID, checkedAt); err != nil {
		log.Printf("state sync: persist checkpoint for %s: %v", peerID, err)
		return
	}
	m.stateSyncMu.Lock()
	if m.stateSyncOutgoing[peerID] == transfer {
		delete(m.stateSyncOutgoing, peerID)
	}
	m.stateSyncMu.Unlock()
	log.Printf("state sync: peer %s acknowledged checkpoint %s", peerID, checkedAt.Format(time.RFC3339Nano))
}
