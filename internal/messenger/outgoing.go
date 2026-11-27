package messenger

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/killbane1232/huginn-messenger/internal/chunk"
	"github.com/killbane1232/huginn-messenger/internal/config"
	"github.com/killbane1232/huginn-messenger/internal/crypto"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
	"github.com/killbane1232/huginn-messenger/internal/store"
)

type outgoingMessage struct {
	message ChatMessage
	peer    muninn.Peer
	chunks  []store.PendingChunk
}

func (m *Messenger) prepareOutgoing(to, text string, filePaths []string, ttl int) (*outgoingMessage, error) {
	msg := ChatMessage{MsgID: uuid.NewString(), From: m.Username, ChatID: to, Text: text, Timestamp: time.Now().UTC()}
	return m.prepareOutgoingMessage(msg, filePaths, ttl, false)
}

func (m *Messenger) prepareOutgoingMessage(message ChatMessage, filePaths []string, ttl int, retry bool) (*outgoingMessage, error) {
	if err := m.ctx.Err(); err != nil {
		return nil, err
	}
	if ttl <= 0 {
		ttl = config.ChunkTTLSeconds("1w")
	}
	peer := m.findPeerByKey(message.ChatID)
	if peer == nil {
		if group, err := m.store.GetGroupChat(message.ChatID); err == nil {
			peer = &muninn.Peer{ID: group.UID, Login: group.UID, EncryptionKey: group.EncPublic, SignatureKey: group.SignPublic, IsFake: true}
		}
	}
	if peer == nil {
		if retry {
			return nil, fmt.Errorf("recipient not found")
		}
		message.SendError, message.TTLSeconds = "recipient not found", ttl
		for _, path := range filePaths {
			message.Files = append(message.Files, FileMeta{FileID: uuid.NewString(), Filename: filepath.Base(path), FilePath: path})
		}
		data, err := json.Marshal(message)
		if err != nil {
			return nil, err
		}
		if err := m.store.SaveMessage(message.MsgID, message.ChatID, message.From, message.ChatID, data, message.Timestamp); err != nil {
			return nil, fmt.Errorf("save failed message: %w", err)
		}
		m.publishOutgoing(message)
		return &outgoingMessage{message: message}, nil
	}
	recipientKey, err := crypto.DecodeKey(peer.EncryptionKey)
	if err != nil {
		return nil, fmt.Errorf("invalid recipient encryption key")
	}
	sentAt := message.Timestamp
	queuedAt := time.Now().UTC()
	message.Files, message.SendError, message.TTLSeconds = nil, "", 0
	outgoing := &outgoingMessage{peer: *peer, message: message}
	for _, path := range filePaths {
		meta, chunks, err := m.prepareFileChunks(path, queuedAt, ttl)
		if err != nil {
			return nil, err
		}
		outgoing.message.Files = append(outgoing.message.Files, *meta)
		outgoing.chunks = append(outgoing.chunks, chunks...)
	}
	msg := &outgoing.message
	msg.ChatID = peer.Key()
	if peer.IsFake && peer.ID != "" {
		msg.ChatID = peer.ID
	}
	payload, err := json.Marshal(MessagePayload{Text: msg.Text, Timestamp: sentAt, Files: withoutLocalFilePaths(msg.Files)})
	if err != nil {
		return nil, fmt.Errorf("encode message: %w", err)
	}
	envelopes, err := chunk.SplitAndEncrypt(msg.MsgID, m.ID, peer.ID, payload, recipientKey, m.signPrivate)
	if err != nil {
		return nil, fmt.Errorf("encrypt message: %w", err)
	}
	chunks, err := m.pendingEnvelopes(envelopes, peer.Key(), queuedAt, ttl, false)
	if err != nil {
		return nil, err
	}
	outgoing.chunks = append(outgoing.chunks, chunks...)
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	queue := m.store.QueueOutgoing
	if retry {
		queue = m.store.RetryOutgoing
	}
	if err := queue(msg.MsgID, peer.Key(), msg.From, msg.ChatID, data, sentAt, outgoing.chunks); err != nil {
		return nil, fmt.Errorf("queue outgoing message: %w", err)
	}
	m.publishOutgoing(*msg)
	return outgoing, nil
}

func (m *Messenger) publishOutgoing(msg ChatMessage) {
	m.msgSubsMu.Lock()
	for _, subscriber := range m.msgSubs {
		select {
		case subscriber <- msg:
		default:
		}
	}
	m.msgSubsMu.Unlock()
}

func (m *Messenger) prepareFileChunks(path string, created time.Time, ttl int) (*FileMeta, []store.PendingChunk, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot read attachment")
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, nil, err
	}
	id := uuid.NewString()
	envelopes, err := chunk.SplitAndEncryptFile(id, m.ID, data, key, m.signPrivate)
	if err != nil {
		return nil, nil, err
	}
	chunks, err := m.pendingEnvelopes(envelopes, "", created, ttl, true)
	if err != nil {
		return nil, nil, err
	}
	hash := sha256.Sum256(data)
	return &FileMeta{FileID: id, FileHash: crypto.EncodeKey(hash[:]), DecryptionKey: crypto.EncodeKey(key), TotalChunks: len(chunks), Filename: filepath.Base(path), FilePath: path}, chunks, nil
}

func (m *Messenger) pendingEnvelopes(envelopes []chunk.Envelope, recipient string, created time.Time, ttl int, persist bool) ([]store.PendingChunk, error) {
	result := make([]store.PendingChunk, 0, len(envelopes))
	for _, envelope := range envelopes {
		data, err := chunk.MarshalEnvelope(envelope)
		if err != nil {
			return nil, err
		}
		hash := chunk.RegisteredHash(data)
		signature := crypto.Sign(m.signPrivate, []byte(fmt.Sprintf("muninn/expected/v1\n%s\n%d\n%s", envelope.MessageID, envelope.ChunkIndex, hash)))
		result = append(result, store.PendingChunk{FileID: envelope.MessageID, ChunkIndex: envelope.ChunkIndex, RecipientID: recipient, SenderID: m.Key, Data: data, Hash: hash, Signature: crypto.EncodeKey(signature), CreatedAt: created, TTLSeconds: ttl, Persist: persist})
	}
	return result, nil
}

func (m *Messenger) deliverOutgoing(outgoing *outgoingMessage) {
	peer, msg := outgoing.peer, outgoing.message
	if peer.ID == "" && !peer.IsFake && len(msg.Files) == 0 {
		if resolved := m.refreshPeerByKey(m.ctx, peer.Key()); resolved != nil {
			peer = *resolved
		}
	}
	if peer.ID != "" && !peer.IsFake && len(msg.Files) == 0 {
		if !m.IsPeerConnected(peer.ID) {
			if err := m.ConnectPeer(peer.ID); err == nil {
				m.waitForPeerConnection(peer.ID, peerConnectTimeout)
			}
		}
		if m.IsPeerConnected(peer.ID) {
			if err := m.rtcManager.SendMessage(peer.ID, msg.Text, msg.Timestamp, msg.MsgID); err != nil {
				log.Printf("direct send %s deferred to durable queue: %v", msg.MsgID, err)
			}
		}
	}
	m.distributeChunks(outgoing.chunks)
}
