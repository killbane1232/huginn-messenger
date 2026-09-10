package messenger

import (
	"log"
	"math"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/muninn"
	"github.com/killbane1232/huginn-messenger/internal/store"
	"github.com/killbane1232/huginn-messenger/internal/webrtc"
)

// Keep each WebRTC frame comfortably below the default SCTP message limit.
const chunkDeliveryBatchSize = 16

func remainingChunkTTL(c store.PendingChunk) int {
	return int(math.Ceil(time.Until(c.CreatedAt.Add(time.Duration(c.TTLSeconds) * time.Second)).Seconds()))
}

func (m *Messenger) distributePendingChunks() {
	chunks, err := m.store.GetUnplacedChunks()
	if err != nil {
		log.Printf("load outgoing queue: %v", err)
		return
	}
	m.distributeChunks(chunks)
}

func (m *Messenger) distributeChunks(chunks []store.PendingChunk) {
	// Leave other accepted sends in SQLite instead of occupying every async
	// worker while waiting for storage acknowledgements.
	if !m.deliveryMu.TryLock() {
		return
	}
	defer m.deliveryMu.Unlock()
	byRecipient := make(map[string][]store.PendingChunk)
	for _, c := range chunks {
		if remainingChunkTTL(c) > 0 {
			byRecipient[c.RecipientID] = append(byRecipient[c.RecipientID], c)
		}
	}
	for recipient, chunks := range byRecipient {
		if m.ctx.Err() != nil {
			return
		}
		m.distributeChunksForRecipient(recipient, chunks)
	}
}

func (m *Messenger) distributeChunksForRecipient(recipient string, chunks []store.PendingChunk) {
	byFile := make(map[string][]store.PendingChunk)
	for _, c := range chunks {
		byFile[c.FileID] = append(byFile[c.FileID], c)
	}
	// Always retry advertising the sender's durable copy, even if the only
	// other online peer is the recipient, or there are no online peers at all.
	for fileID, parts := range byFile {
		if !m.registerChunkLocation(fileID, m.ID, parts) {
			log.Printf("local chunk registration for %s remains queued", fileID)
		}
	}
	var peers []muninn.Peer
	var err error
	if recipient == "" {
		peers, err = m.muninnClient.GetBestThickPeers(m.ctx, 5)
	} else {
		peers, err = m.muninnClient.GetBestPeers(m.ctx, 10)
	}
	if err != nil {
		peers = m.getOnlinePeers()
	}
	for _, peer := range peers {
		if m.ctx.Err() != nil {
			return
		}
		if peer.ID == m.ID || peer.IsFake || peer.Key() == recipient {
			continue
		}
		if !m.IsPeerConnected(peer.ID) {
			if err := m.ConnectPeer(peer.ID); err != nil {
				continue
			}
			if !m.waitForPeerConnection(peer.ID, peerConnectTimeout) {
				continue
			}
		}
		for fileID, parts := range byFile {
			if !m.registerChunkLocation(fileID, peer.ID, parts) {
				continue
			}
			for start := 0; start < len(parts); start += chunkDeliveryBatchSize {
				end := min(start+chunkDeliveryBatchSize, len(parts))
				batch := webrtc.ChunkStoreBatchRequest{}
				var pending []store.PendingChunk
				for _, c := range parts[start:end] {
					ttl := remainingChunkTTL(c)
					if ttl <= 0 {
						continue
					}
					batch.Chunks = append(batch.Chunks, webrtc.ChunkStoreRequest{FileID: c.FileID, ChunkIndex: c.ChunkIndex, Data: c.Data, SenderID: c.SenderID, RecipientID: c.RecipientID, Hash: c.Hash, Signature: c.Signature, TTLSeconds: ttl})
					pending = append(pending, c)
				}
				if len(batch.Chunks) == 0 {
					continue
				}
				if err := m.rtcManager.SendChunkStoreBatchConfirmed(m.ctx, peer.ID, batch); err != nil {
					log.Printf("chunk batch %s awaits storage confirmation: %v", fileID, err)
					continue
				}
				for _, c := range pending {
					if err := m.store.MarkChunkPlaced(c.FileID, c.ChunkIndex); err != nil {
						log.Printf("record stored chunk: %v", err)
					}
				}
			}
		}
	}
}

func (m *Messenger) registerChunkLocation(fileID, peerID string, chunks []store.PendingChunk) bool {
	// Avoid adding duplicate directory rows on every retry or after a lost ACK.
	records, err := m.muninnClient.GetChunksByFileID(m.ctx, fileID)
	if err != nil {
		records = nil
	}
	existing := make(map[int]string)
	now := time.Now().Unix()
	for _, r := range records {
		if r.PeerID == peerID && (r.TTL <= 0 || r.CreatedAt+int64(r.TTL) > now) {
			existing[r.ChunkIndex] = r.Hash
		}
	}
	missing := muninn.RegisterChunkBatchRequest{}
	for _, c := range chunks {
		ttl := remainingChunkTTL(c)
		if ttl <= 0 || existing[c.ChunkIndex] == c.Hash {
			continue
		}
		missing.Chunks = append(missing.Chunks, muninn.RegisterChunkBatchEntry{ChunkIndex: c.ChunkIndex, SenderID: c.SenderID, RecipientID: c.RecipientID, Hash: c.Hash, Signature: c.Signature, PeerID: peerID, Persist: c.Persist, TTL: ttl})
	}
	if len(missing.Chunks) == 0 {
		return true
	}
	return m.muninnClient.RegisterChunks(m.ctx, fileID, missing) == nil
}
