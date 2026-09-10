package messenger

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/chunk"
	"github.com/killbane1232/huginn-messenger/internal/crypto"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
	//"runtime/debug"
)

const peerConnectTimeout = 5 * time.Second

func (m *Messenger) processRTCMessages() {
	for {
		select {
		case msg := <-m.rtcMsgChan:
			displayText := m.checkInviteText(msg.Text)
			receivedAt := msg.Timestamp.UTC()
			if msg.Timestamp.IsZero() {
				receivedAt = time.Now().UTC()
			}

			fromKey := msg.From
			fromLogin := strings.SplitN(msg.From, ":", 2)[0]
			if p := m.findPeerByID(msg.From); p != nil {
				fromKey = p.Key()
				fromLogin = p.Login
			}

			cm := ChatMessage{
				From:      fromLogin,
				ChatID:    fromKey,
				Text:      displayText,
				Timestamp: receivedAt,
				MsgID:     msg.MsgID,
			}
			jsonData, _ := json.Marshal(cm)
			if err := m.store.SaveMessage(msg.MsgID, fromKey, fromLogin, cm.ChatID, jsonData, cm.Timestamp); err != nil {
				log.Printf("save message: %v", err)
				continue
			}
			m.msgSubsMu.Lock()
			for _, sub := range m.msgSubs {
				select {
				case sub <- cm:
				default:
				}
			}
			m.msgSubsMu.Unlock()
		case <-m.ctx.Done():
			return
		}
	}
}

// SendMessage acknowledges durable local acceptance, not remote delivery.
func (m *Messenger) SendMessage(to, text string, filePaths []string, ttlSeconds int) error {
	outgoing, err := m.prepareOutgoing(to, text, filePaths, ttlSeconds)
	if err != nil {
		return err
	}
	// Queue saturation or shutdown cannot discard an accepted message: the
	// periodic worker (including after restart) reads the same SQLite outbox.
	m.async.trySubmit(func() { m.deliverOutgoing(outgoing) })
	return nil
}

func (m *Messenger) SendMessageSync(to, text string, filePaths []string, ttlSeconds int) error {
	outgoing, err := m.prepareOutgoing(to, text, filePaths, ttlSeconds)
	if err != nil {
		return err
	}
	m.deliverOutgoing(outgoing)
	return nil
}

func (m *Messenger) waitForPeerConnection(peerID string, timeout time.Duration) bool {
	if m.IsPeerConnected(peerID) {
		return true
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return false
		case <-timer.C:
			return m.IsPeerConnected(peerID)
		case <-ticker.C:
			if m.IsPeerConnected(peerID) {
				return true
			}
		}
	}
}

func (m *Messenger) checkPendingMessages() {
	log.Printf("check pending messages for %s", m.Key)
	m.checkRecipientMessages(m.Key)

	groups, err := m.store.GetGroupChats()
	if err != nil {
		return
	}
	for _, g := range groups {
		m.checkRecipientMessages(g.UID + ":" + g.SignPublic)
	}
}

func (m *Messenger) checkRecipientMessages(recipientID string) {
	lastCheck := m.store.GetLastChunkCheck(recipientID)
	chunks, err := m.muninnClient.GetChunksByRecipient(m.ctx, recipientID, lastCheck-1)
	if err != nil {
		log.Printf("check %s: GetChunksByRecipient err: %v", recipientID, err)
		return
	}
	if len(chunks) == 0 {
		m.retryFailedChunks(recipientID)
		return
	}
	log.Printf("check %s: got %d chunk records", recipientID, len(chunks))
	newLastCheck := lastCheck
	safeToAdvance := true
	if len(chunks) > 0 {
		byMsg := make(map[string][]muninn.ChunkRecord)
		for _, c := range chunks {
			updated := c.UpdatedAt
			if updated == 0 {
				updated = c.CreatedAt
			}
			if updated > newLastCheck {
				newLastCheck = updated
			}
			if m.store.IsChunkFailed(c.FileID, c.ChunkIndex) {
				log.Printf("check %s: skip failed chunk %s/%d", recipientID, c.FileID, c.ChunkIndex)
				continue
			}
			hasMsg, _ := m.store.FindMessageById(c.FileID)
			if hasMsg {
				log.Printf("collecting %s skipped", c.FileID)
				continue
			}
			log.Printf("check %s: chunk %s/%d confirmed=%v peer=%s", recipientID, c.FileID, c.ChunkIndex, c.Confirmed, c.PeerID)
			byMsg[c.FileID] = append(byMsg[c.FileID], c)
		}
		log.Printf("check %s: %d unique messages", recipientID, len(byMsg))
		for msgID, msgChunks := range byMsg {
			if !m.collectAndProcessMessage(msgID, msgChunks) {
				safeToAdvance = false
			}
		}
	}

	if safeToAdvance && newLastCheck > lastCheck {
		m.store.SetLastChunkCheck(recipientID, newLastCheck)
	}
	m.retryFailedChunks(recipientID)
}

func (m *Messenger) tryProcessMsg(msgID string) bool {
	m.processingMu.Lock()
	if m.processingMsg[msgID] {
		m.processingMu.Unlock()
		return false
	}
	m.processingMsg[msgID] = true
	m.processingMu.Unlock()
	return true
}

func (m *Messenger) releaseProcessMsg(msgID string) {
	m.processingMu.Lock()
	delete(m.processingMsg, msgID)
	m.processingMu.Unlock()
}

func (m *Messenger) collectAndProcessMessage(msgID string, records []muninn.ChunkRecord) (tracked bool) {
	if len(records) == 0 {
		return false
	}
	if !m.tryProcessMsg(msgID) {
		return
	}
	defer m.releaseProcessMsg(msgID)
	hasMsg, _ := m.store.FindMessageById(msgID)
	if hasMsg {
		_ = m.store.DeleteFailedMessage(msgID)
		return true
	}
	// Persist retry state before allowing the directory cursor to move past it.
	for _, record := range records {
		if err := m.store.StoreFailedChunk(msgID, record.ChunkIndex, record.RecipientID, record.TTL); err != nil {
			return false
		}
	}
	tracked = true
	log.Printf("collecting %s (%d chunk records, persist=%v)", msgID, len(records), len(records) > 0 && records[0].Persist)

	seen := make(map[int]bool)
	var chunkData [][]byte

	for _, rec := range records {
		if seen[rec.ChunkIndex] {
			continue
		}

		data, ok := m.getChunkData(rec)
		if !ok {
			log.Printf("not collected any data: %s/%d", rec.FileID, rec.ChunkIndex)
			ttl := rec.TTL
			if ttl <= 0 {
				ttl = 604800
			}
			m.store.StoreFailedChunk(rec.FileID, rec.ChunkIndex, rec.RecipientID, ttl)
			continue
		}
		if rec.Hash != "" && chunk.RegisteredHash(data) != rec.Hash {
			_ = m.store.DeleteChunk(rec.FileID, rec.ChunkIndex)
			log.Printf("hash mismatch for chunk %s/%d: got %s, expected %s",
				rec.FileID, rec.ChunkIndex, chunk.RegisteredHash(data), rec.Hash)
			continue
		}
		chunkData = append(chunkData, data)
		seen[rec.ChunkIndex] = true
	}

	if len(chunkData) == 0 {
		log.Printf("not collected any data: %s", msgID)
		return
	}

	var envelopes []chunk.Envelope
	for _, data := range chunkData {
		env, err := chunk.UnmarshalEnvelope(data)
		if err != nil {
			log.Printf("invalid envelope for chunk: %v", err)
			continue
		}
		envelopes = append(envelopes, env)
	}

	if len(envelopes) != len(chunkData) {
		log.Printf("incomplete %s: got %d envelopes", msgID, len(envelopes))
		return
	}

	totalChunks := envelopes[0].TotalChunks
	if len(envelopes) < totalChunks {
		log.Printf("%s: got %d/%d chunks, waiting for more", msgID, len(envelopes), totalChunks)
		return
	}

	senderPeer := m.findPeerByKey(records[0].SenderID)
	if senderPeer == nil {
		log.Printf("sender %s not found for %s", records[0].SenderID, msgID)
		return
	}

	senderSignKey, err := crypto.DecodeKey(senderPeer.SignatureKey)
	if err != nil {
		log.Printf("decode sender sign key: %v", err)
		return
	}

	if records[0].Persist {
		_ = m.store.DeleteFailedMessage(msgID)
		log.Printf("file chunks %s ready in store (%d envelopes), waiting for message with decryption key", msgID, len(envelopes))
		return
	}

	encPrivate := m.encPrivate
	encPublic := m.encPublic
	recipientID := records[0].RecipientID
	if recipientID != "" && recipientID != m.Key {
		uid := strings.SplitN(recipientID, ":", 2)[0]
		if gc, err := m.store.GetGroupChat(uid); err == nil {
			if priv, err := crypto.DecodeKey(gc.EncPrivate); err == nil {
				encPrivate = priv
			}
			if pub, err := crypto.DecodeKey(gc.EncPublic); err == nil {
				encPublic = pub
			}
		}
	}

	plaintext, err := chunk.AssembleAndDecrypt(envelopes, encPrivate, encPublic, senderSignKey)
	if err != nil {
		log.Printf("assemble/decrypt message %s: %v", msgID, err)
		return
	}

	var payload MessagePayload
	if err := json.Unmarshal(plaintext, &payload); err != nil {
		payload = MessagePayload{Text: string(plaintext)}
	}
	if payload.Timestamp.IsZero() {
		payload.Timestamp = time.Now().UTC()
	} else {
		payload.Timestamp = payload.Timestamp.UTC()
	}
	payload.Files = withoutLocalFilePaths(payload.Files)

	chatID := senderPeer.Key()
	if recipientID != "" && recipientID != m.Key {
		groupUID := strings.SplitN(recipientID, ":", 2)[0]
		if _, err := m.store.GetGroupChat(groupUID); err == nil {
			chatID = groupUID
		} else {
			chatID = recipientID
		}
	}

	displayText := m.checkInviteText(payload.Text)

	for _, f := range payload.Files {
		m.processReceivedFile(f, records[0].SenderID)
	}

	decryptedMsg := ChatMessage{
		From:      senderPeer.Login,
		ChatID:    chatID,
		Text:      displayText,
		Timestamp: payload.Timestamp,
		MsgID:     msgID,
		Files:     payload.Files,
	}

	jsonData, _ := json.Marshal(decryptedMsg)
	if err := m.store.SaveMessage(msgID, senderPeer.Key(), senderPeer.Login, chatID, jsonData, decryptedMsg.Timestamp); err != nil {
		log.Printf("save message: %v", err)
		return
	}
	_ = m.store.DeleteFailedMessage(msgID)

	m.msgSubsMu.Lock()
	for _, sub := range m.msgSubs {
		select {
		case sub <- decryptedMsg:
		default:
		}
	}
	m.msgSubsMu.Unlock()

	log.Printf("message %s delivered from %s", msgID, records[0].SenderID)
	return
}

func withoutLocalFilePaths(files []FileMeta) []FileMeta {
	if len(files) == 0 {
		return nil
	}

	result := make([]FileMeta, len(files))
	copy(result, files)
	for i := range result {
		result[i].FilePath = ""
	}
	return result
}

func (m *Messenger) MarkMessageRead(msgID string) error {
	payload := fmt.Sprintf("muninn/read/v1\n%s", msgID)
	sig := crypto.Sign(m.signPrivate, []byte(payload))
	req := muninn.ReadChunkRequest{
		RecipientID: m.Key,
		FileID:      msgID,
		Signature:   base64.StdEncoding.EncodeToString(sig),
	}
	return m.muninnClient.ReadChunk(m.ctx, req)
}

func (m *Messenger) GetMessages(peerID string) []ChatMessage {
	dataList, err := m.store.GetMessages(peerID)
	if err != nil {
		return nil
	}
	result := make([]ChatMessage, 0, len(dataList))
	for _, data := range dataList {
		var msg ChatMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		result = append(result, msg)
	}
	return result
}

func (m *Messenger) GetMessagesDesc(peerID string, limit, offset int) []ChatMessage {
	dataList, err := m.store.GetMessagesDesc(peerID, limit, offset)
	if err != nil {
		return nil
	}
	result := make([]ChatMessage, 0, len(dataList))
	for _, data := range dataList {
		var msg ChatMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		result = append(result, msg)
	}
	return result
}

func (m *Messenger) SubscribeMessages() chan ChatMessage {
	ch := make(chan ChatMessage, 50)
	m.msgSubsMu.Lock()
	m.msgSubs = append(m.msgSubs, ch)
	m.msgSubsMu.Unlock()
	return ch
}

func (m *Messenger) UnsubscribeMessages(ch chan ChatMessage) {
	m.msgSubsMu.Lock()
	for i, c := range m.msgSubs {
		if c == ch {
			m.msgSubs = append(m.msgSubs[:i], m.msgSubs[i+1:]...)
			close(ch)
			break
		}
	}
	m.msgSubsMu.Unlock()
}
