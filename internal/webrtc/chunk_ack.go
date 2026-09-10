package webrtc

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

func (m *Manager) storeChunkBatch(peerID string, batch ChunkStoreBatchRequest) {
	ack := ChunkStoreAck{RequestID: batch.RequestID}
	for _, req := range batch.Chunks {
		if err := m.chunkStore(peerID, req); err != nil {
			ack.Error = "chunk storage failed"
			break
		}
	}
	if batch.RequestID != "" {
		_ = m.sendEnvelope(peerID, MsgTypeChunkStoreAck, ack)
	}
}

func (m *Manager) acceptChunkStoreAck(peerID string, ack ChunkStoreAck) {
	m.chunkAckMu.Lock()
	defer m.chunkAckMu.Unlock()
	waiter, ok := m.chunkAcks[ack.RequestID]
	if !ok || waiter.peerID != peerID {
		return
	}
	select {
	case waiter.result <- ack:
	default:
	}
}

// SendChunkStoreBatchConfirmed succeeds only after the receiving application
// has committed every chunk. Legacy peers can receive data but cannot confirm it.
func (m *Manager) SendChunkStoreBatchConfirmed(ctx context.Context, peerID string, batch ChunkStoreBatchRequest) error {
	batch.RequestID = uuid.NewString()
	waiter := chunkStoreWaiter{peerID: peerID, result: make(chan ChunkStoreAck, 1)}
	m.chunkAckMu.Lock()
	m.chunkAcks[batch.RequestID] = waiter
	m.chunkAckMu.Unlock()
	defer func() { m.chunkAckMu.Lock(); delete(m.chunkAcks, batch.RequestID); m.chunkAckMu.Unlock() }()
	if err := m.sendEnvelope(peerID, MsgTypeChunkStoreBatch, batch); err != nil {
		return err
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case ack := <-waiter.result:
		if ack.Error != "" {
			return fmt.Errorf("storage peer rejected chunk batch")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("storage acknowledgement timed out")
	}
}
