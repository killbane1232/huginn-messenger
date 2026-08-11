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
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/killbane1232/huginn-messenger/internal/store"
	"github.com/killbane1232/huginn-messenger/internal/webrtc"
)

const (
	reloginSnapshotVersion   = 1
	reloginChunkSize         = 32 * 1024
	reloginMaxBufferedAmount = 512 * 1024
)

type reloginResult struct {
	keysData string
	snapshot store.ReplicationSnapshot
	err      error
}

type reloginTransferState struct {
	expectedPeerID string
	transferID     string
	keysData       string
	expectedHash   string
	chunks         [][]byte
	received       int
	processing     bool
	completed      bool
	result         chan reloginResult
}

func newReloginTransferID() string {
	return uuid.NewString()
}

func (m *Messenger) buildReloginReplica() ([]byte, string, error) {
	snapshot, err := m.store.ExportReplicationSnapshot()
	if err != nil {
		return nil, "", err
	}
	for i := range snapshot.Messages {
		var message ChatMessage
		if err := json.Unmarshal(snapshot.Messages[i].Data, &message); err != nil {
			return nil, "", fmt.Errorf("decode message %s: %w", snapshot.Messages[i].MessageUID, err)
		}
		message.Files = withoutLocalFilePaths(message.Files)
		for fileIndex := range message.Files {
			message.Files[fileIndex].SourcePeerID = m.ID
		}
		data, err := json.Marshal(message)
		if err != nil {
			return nil, "", fmt.Errorf("encode message %s: %w", snapshot.Messages[i].MessageUID, err)
		}
		snapshot.Messages[i].Data = data
	}

	raw, err := json.Marshal(snapshot)
	if err != nil {
		return nil, "", fmt.Errorf("encode replication snapshot: %w", err)
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(raw); err != nil {
		return nil, "", fmt.Errorf("compress replication snapshot: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, "", fmt.Errorf("finish replication compression: %w", err)
	}
	digest := sha256.Sum256(compressed.Bytes())
	return compressed.Bytes(), hex.EncodeToString(digest[:]), nil
}

func decodeReloginReplica(compressed []byte) (store.ReplicationSnapshot, error) {
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return store.ReplicationSnapshot{}, fmt.Errorf("open replication snapshot: %w", err)
	}
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	if err != nil {
		return store.ReplicationSnapshot{}, fmt.Errorf("decompress replication snapshot: %w", err)
	}
	var snapshot store.ReplicationSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return store.ReplicationSnapshot{}, fmt.Errorf("decode replication snapshot: %w", err)
	}
	if snapshot.Version != reloginSnapshotVersion {
		return store.ReplicationSnapshot{}, fmt.Errorf("unsupported replication snapshot version %d", snapshot.Version)
	}
	return snapshot, nil
}

func (m *Messenger) waitForReloginSendCapacity(peerID string) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		amount, connected := m.rtcManager.BufferedAmount(peerID)
		if !connected {
			return fmt.Errorf("data channel closed")
		}
		if amount <= reloginMaxBufferedAmount {
			return nil
		}
		select {
		case <-ticker.C:
		case <-m.ctx.Done():
			return m.ctx.Err()
		}
	}
}

func (m *Messenger) acceptReloginResponse(peerID string, response webrtc.ReloginResponse) {
	m.reloginMu.Lock()
	transfer := m.reloginTransfer
	if transfer == nil || transfer.expectedPeerID != peerID || transfer.completed {
		m.reloginMu.Unlock()
		return
	}
	if response.Error != "" {
		m.reloginMu.Unlock()
		m.completeReloginTransfer(transfer, reloginResult{err: fmt.Errorf("relogin source: %s", response.Error)})
		return
	}
	if response.KeysData == "" {
		m.reloginMu.Unlock()
		m.completeReloginTransfer(transfer, reloginResult{err: fmt.Errorf("relogin source returned empty keys")})
		return
	}
	if response.TransferID == "" || response.ChunkCount == 0 {
		m.reloginMu.Unlock()
		m.completeReloginTransfer(transfer, reloginResult{keysData: response.KeysData})
		return
	}
	if response.ChunkCount < 0 || response.ChunkCount > 1_000_000 {
		m.reloginMu.Unlock()
		m.completeReloginTransfer(transfer, reloginResult{err: fmt.Errorf("invalid replication chunk count %d", response.ChunkCount)})
		return
	}
	transfer.transferID = response.TransferID
	transfer.keysData = response.KeysData
	transfer.expectedHash = response.SHA256
	transfer.chunks = make([][]byte, response.ChunkCount)
	m.reloginMu.Unlock()
}

func (m *Messenger) acceptReloginChunk(peerID string, chunk webrtc.ReloginChunk) {
	m.reloginMu.Lock()
	transfer := m.reloginTransfer
	if transfer == nil || transfer.expectedPeerID != peerID || transfer.transferID != chunk.TransferID || transfer.completed {
		m.reloginMu.Unlock()
		return
	}
	if chunk.Index < 0 || chunk.Index >= len(transfer.chunks) {
		m.reloginMu.Unlock()
		m.completeReloginTransfer(transfer, reloginResult{err: fmt.Errorf("invalid replication chunk index %d", chunk.Index)})
		return
	}
	if transfer.chunks[chunk.Index] == nil {
		transfer.chunks[chunk.Index] = append([]byte(nil), chunk.Data...)
		transfer.received++
	}
	if transfer.received != len(transfer.chunks) || transfer.processing {
		m.reloginMu.Unlock()
		return
	}
	transfer.processing = true
	m.reloginMu.Unlock()

	if !m.async.submit(func() { m.finishReloginTransfer(transfer) }) {
		m.completeReloginTransfer(transfer, reloginResult{err: fmt.Errorf("messenger is shutting down")})
	}
}

func (m *Messenger) finishReloginTransfer(transfer *reloginTransferState) {
	total := 0
	for _, chunk := range transfer.chunks {
		total += len(chunk)
	}
	compressed := make([]byte, 0, total)
	for _, chunk := range transfer.chunks {
		compressed = append(compressed, chunk...)
	}
	digest := sha256.Sum256(compressed)
	if got := hex.EncodeToString(digest[:]); transfer.expectedHash == "" || got != transfer.expectedHash {
		m.completeReloginTransfer(transfer, reloginResult{err: fmt.Errorf("replication snapshot checksum mismatch")})
		return
	}
	snapshot, err := decodeReloginReplica(compressed)
	if err != nil {
		m.completeReloginTransfer(transfer, reloginResult{err: err})
		return
	}
	if err := m.store.ImportReplicationSnapshot(snapshot); err != nil {
		m.completeReloginTransfer(transfer, reloginResult{err: err})
		return
	}
	m.completeReloginTransfer(transfer, reloginResult{
		keysData: transfer.keysData,
		snapshot: snapshot,
	})
}

func (m *Messenger) completeReloginTransfer(transfer *reloginTransferState, result reloginResult) {
	m.reloginMu.Lock()
	if m.reloginTransfer != transfer || transfer.completed {
		m.reloginMu.Unlock()
		return
	}
	transfer.completed = true
	m.reloginMu.Unlock()
	select {
	case transfer.result <- result:
	default:
	}
}

func (m *Messenger) clearReloginTransfer(transfer *reloginTransferState) {
	m.reloginMu.Lock()
	if m.reloginTransfer == transfer {
		m.reloginTransfer = nil
	}
	m.reloginMu.Unlock()
}

func (m *Messenger) reloadReplicatedPeers() {
	peers, err := m.store.GetStoredPeers()
	if err != nil {
		log.Printf("relogin: reload replicated peers: %v", err)
		return
	}
	m.mu.Lock()
	for _, storedPeer := range peers {
		peer := storedPeer.ToMuninnPeer()
		if existing, ok := m.peersMap[peer.Key()]; ok {
			peer.ID = existing.ID
			peer.IDS = existing.IDS
			peer.TTLSeconds = existing.TTLSeconds
		}
		m.peersMap[peer.Key()] = peer
	}
	m.peers = m.PeerSlice()
	m.mu.Unlock()

	m.subsMu.Lock()
	for _, subscriber := range m.peerSubs {
		select {
		case subscriber <- struct{}{}:
		default:
		}
	}
	m.subsMu.Unlock()
}

func (m *Messenger) queueReplicatedFiles(messages []store.ReplicatedMessage, sourcePeerID, sourceUsername string) {
	for _, record := range messages {
		var message ChatMessage
		if err := json.Unmarshal(record.Data, &message); err != nil {
			continue
		}
		senderID := record.Login
		if message.From == sourceUsername || strings.HasPrefix(message.From, sourceUsername+":") {
			senderID = m.Key
		}
		for _, file := range message.Files {
			if file.FileID == "" || file.TotalChunks <= 0 {
				continue
			}
			file := file
			preferredPeerID := file.SourcePeerID
			if preferredPeerID == "" {
				preferredPeerID = sourcePeerID
			}
			if preferredPeerID == "" {
				continue
			}
			m.pendingMu.Lock()
			m.pendingFileDownloads[file.FileID] = &pendingFileDownload{
				fileMeta:        file,
				senderID:        senderID,
				preferredPeerID: preferredPeerID,
			}
			m.pendingMu.Unlock()
			m.async.trySubmit(func() {
				m.processReceivedFileFromPeer(file, senderID, preferredPeerID)
			})
		}
	}
}

func (m *Messenger) resumeReplicatedFileDownloads() {
	snapshot, err := m.store.ExportReplicationSnapshot()
	if err != nil {
		log.Printf("resume replicated file downloads: %v", err)
		return
	}
	m.queueReplicatedFiles(snapshot.Messages, "", m.Username)
}
