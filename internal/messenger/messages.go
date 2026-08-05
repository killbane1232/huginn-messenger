package messenger

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/killbane1232/huginn-messenger/internal/chunk"
	"github.com/killbane1232/huginn-messenger/internal/config"
	"github.com/killbane1232/huginn-messenger/internal/crypto"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
	"github.com/killbane1232/huginn-messenger/internal/store"
	"github.com/killbane1232/huginn-messenger/internal/webrtc"
	//"runtime/debug"
)

func (m *Messenger) processRTCMessages() {
	for {
		select {
		case msg := <-m.rtcMsgChan:
			displayText := m.checkInviteText(msg.Text)

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
				Timestamp: msg.Timestamp,
				MsgID:     msg.MsgID,
			}
			if cm.Timestamp.IsZero() {
				cm.Timestamp = time.Now()
			}
			jsonData, _ := json.Marshal(cm)
			if err := m.store.SaveMessage(msg.MsgID, fromKey, fromLogin, cm.ChatID, jsonData, cm.Timestamp); err != nil {
				log.Printf("save message: %v", err)
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

func (m *Messenger) SendMessage(to, text string, filePaths []string, ttlSeconds int) error {
	if !m.async.submit(func() {
		if err := m.sendMessage(to, text, filePaths, ttlSeconds); err != nil {
			log.Printf("send message to %s: %v", to, err)
		}
	}) {
		return fmt.Errorf("messenger is shutting down")
	}
	return nil
}

func (m *Messenger) SendMessageSync(to, text string, filePaths []string, ttlSeconds int) error {
	result := make(chan error, 1)
	if !m.async.submit(func() {
		result <- m.sendMessage(to, text, filePaths, ttlSeconds)
	}) {
		return fmt.Errorf("messenger is shutting down")
	}
	select {
	case err := <-result:
		return err
	case <-m.ctx.Done():
		return m.ctx.Err()
	}
}

func (m *Messenger) sendMessage(to, text string, filePaths []string, ttlSeconds int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in sendMessageAsync: %v", r)
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	if ttlSeconds <= 0 {
		ttlSeconds = config.ChunkTTLSeconds("1w")
	}

	peer := m.findPeerByKey(to)

	if peer == nil {
		if gc, err := m.store.GetGroupChat(to); err == nil {
			peer = &muninn.Peer{
				ID:            gc.UID,
				Login:         gc.UID,
				EncryptionKey: gc.EncPublic,
				SignatureKey:  gc.SignPublic,
				IsFake:        true,
			}
		}
	}

	if peer == nil {
		return fmt.Errorf("peer %s not found", to)
	}

	var files []FileMeta
	for _, fp := range filePaths {
		meta, err := m.sendFileChunks(to, fp, ttlSeconds)
		if err != nil {
			return fmt.Errorf("send file %s: %w", fp, err)
		}
		files = append(files, *meta)
	}

	msgID := uuid.New().String()
	now := time.Now()
	chatID := peer.Key()
	if peer.IsFake && peer.ID != "" {
		chatID = peer.ID
	}
	cm := ChatMessage{
		From:      m.Username,
		ChatID:    chatID,
		Text:      text,
		Timestamp: now,
		MsgID:     msgID,
		Files:     files,
	}
	jsonData, _ := json.Marshal(cm)
	if err := m.store.SaveMessage(msgID, peer.Key(), peer.Login, chatID, jsonData, cm.Timestamp); err != nil {
		log.Printf("save message: %v", err)
	}
	m.msgSubsMu.Lock()
	for _, sub := range m.msgSubs {
		select {
		case sub <- cm:
		default:
		}
	}
	m.msgSubsMu.Unlock()

	onlinePeerID := peer.ID
	if onlinePeerID == "" {
		if p := m.findPeerByKey(to); p != nil {
			onlinePeerID = p.ID
		}
	}

	if onlinePeerID != "" && m.IsPeerOnline(onlinePeerID) {
		if !m.IsPeerConnected(onlinePeerID) {
			m.ConnectPeer(onlinePeerID)
		}
	}

	if onlinePeerID != "" && m.IsPeerConnected(onlinePeerID) {
		now := time.Now()
		if err := m.rtcManager.SendMessage(onlinePeerID, text, now, msgID); err != nil {
			return m.sendOffline(msgID, text, peer, ttlSeconds, files)
		}
		_ = m.sendOffline(msgID, text, peer, ttlSeconds, files)
		return nil
	}

	return m.sendOffline(msgID, text, peer, ttlSeconds, files)
}

func (m *Messenger) sendOffline(msgID, text string, peer *muninn.Peer, ttlSeconds int, files []FileMeta) error {
	log.Printf("sendOffline[%s]: start peer.ID=%q peer.Key=%q", msgID, peer.ID, peer.Key())

	now := time.Now()

	payload := MessagePayload{Text: text, Timestamp: now, Files: files}
	payloadData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	recipientPubKey, err := crypto.DecodeKey(peer.EncryptionKey)
	if err != nil {
		return fmt.Errorf("decode recipient enc key: %w", err)
	}

	envelopes, err := chunk.SplitAndEncrypt(msgID, m.ID, peer.ID, payloadData, recipientPubKey, m.signPrivate)
	if err != nil {
		return fmt.Errorf("split encrypt: %w", err)
	}
	log.Printf("sendOffline[%s]: split into %d chunks", msgID, len(envelopes))

	if ttlSeconds <= 0 {
		ttlSeconds = config.ChunkTTLSeconds("1w")
	}

	type chunkData struct {
		envData []byte
		hash    string
		sig     string
	}
	chunks := make([]chunkData, len(envelopes))
	for i, env := range envelopes {
		envData, err := chunk.MarshalEnvelope(env)
		if err != nil {
			return fmt.Errorf("marshal env %d: %w", i, err)
		}
		if err := m.store.StoreChunk(msgID, i, envData, ttlSeconds); err != nil {
			return fmt.Errorf("store chunk %d: %w", i, err)
		}
		chunkHash := chunk.RegisteredHash(envData)
		expectedPayload := fmt.Sprintf("muninn/expected/v1\n%s\n%d\n%s", msgID, i, chunkHash)
		sig := crypto.Sign(m.signPrivate, []byte(expectedPayload))
		chunks[i] = chunkData{envData, chunkHash, crypto.EncodeKey(sig)}
	}
	log.Printf("sendOffline[%s]: stored %d chunks locally", msgID, len(chunks))

	for i := range chunks {
		if err := m.store.StorePendingChunk(&store.PendingChunk{
			FileID:      msgID,
			ChunkIndex:  i,
			RecipientID: peer.Key(),
			SenderID:    m.Key,
			Data:        chunks[i].envData,
			Hash:        chunks[i].hash,
			Signature:   chunks[i].sig,
			CreatedAt:   time.Now(),
			Placed:      false,
			TTLSeconds:  ttlSeconds,
		}); err != nil {
			log.Printf("store pending chunk %s/%d: %v", msgID, i, err)
		}
	}
	log.Printf("sendOffline[%s]: stored pending chunks", msgID)

	localRegBatch := make([]muninn.RegisterChunkBatchEntry, len(chunks))
	for i, c := range chunks {
		localRegBatch[i] = muninn.RegisterChunkBatchEntry{
			ChunkIndex: i, SenderID: m.Key, RecipientID: peer.Key(),
			Hash: c.hash, Signature: c.sig, PeerID: m.ID, TTL: ttlSeconds,
		}
	}
	log.Printf("sendOffline[%s]: registering chunks locally...", msgID)
	if err := m.muninnClient.RegisterChunks(m.ctx, msgID, muninn.RegisterChunkBatchRequest{Chunks: localRegBatch}); err != nil {
		log.Printf("register batch %s on self: %v", msgID, err)
	} else {
		log.Printf("sendOffline[%s]: registered chunks locally", msgID)
	}

	log.Printf("sendOffline[%s]: getting best peers...", msgID)
	onlinePeers, err := m.muninnClient.GetBestPeers(m.ctx, 10)
	if err != nil {
		log.Printf("sendOffline[%s]: GetBestPeers failed: %v, fallback to local", msgID, err)
		onlinePeers = m.getOnlinePeers()
	}
	log.Printf("sendOffline[%s]: got %d best peers", msgID, len(onlinePeers))

	storagePeers := []string{}
	for _, p := range onlinePeers {
		if p.ID == m.ID || (peer.ID != "" && p.ID == peer.ID) {
			continue
		}
		if !m.IsPeerConnected(p.ID) {
			m.ConnectPeer(p.ID)
		}
		storagePeers = append(storagePeers, p.ID)
	}
	log.Printf("sendOffline[%s]: connecting to %d storage peers", msgID, len(storagePeers))

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

	connected := 0
	for _, pid := range storagePeers {
		if !m.IsPeerConnected(pid) {
			continue
		}
		connected++

		batch := make([]webrtc.ChunkStoreRequest, len(chunks))
		regBatch := make([]muninn.RegisterChunkBatchEntry, len(chunks))
		for i, c := range chunks {
			batch[i] = webrtc.ChunkStoreRequest{
				FileID: msgID, ChunkIndex: i, Data: c.envData,
				SenderID: m.Key, RecipientID: peer.Key(), Hash: c.hash,
				Signature: c.sig, TTLSeconds: ttlSeconds,
			}
			regBatch[i] = muninn.RegisterChunkBatchEntry{
				ChunkIndex: i, SenderID: m.Key, RecipientID: peer.Key(),
				Hash: c.hash, Signature: c.sig, PeerID: pid, TTL: ttlSeconds,
			}
		}
		log.Printf("sendOffline[%s]: registering chunks on peer %s...", msgID, pid)
		if err := m.muninnClient.RegisterChunks(m.ctx, msgID, muninn.RegisterChunkBatchRequest{Chunks: regBatch}); err != nil {
			log.Printf("register batch %s on %s: %v", msgID, pid, err)
			continue
		}
		log.Printf("sendOffline[%s]: sending chunk batch to %s...", msgID, pid)
		if err := m.rtcManager.SendChunkStoreBatch(pid, webrtc.ChunkStoreBatchRequest{Chunks: batch}); err != nil {
			log.Printf("sendOffline[%s]: send chunk batch to %s: %v", msgID, pid, err)
			continue
		}
		for i := range chunks {
			m.store.MarkChunkPlaced(msgID, i)
		}
		log.Printf("sendOffline[%s]: chunks sent to %s", msgID, pid)
	}
	log.Printf("sendOffline[%s]: done, connected=%d/%d", msgID, connected, len(storagePeers))
	return nil
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
	newLastCheck := int64(0)
	if len(chunks) > 0 {
		byMsg := make(map[string][]muninn.ChunkRecord)
		for _, c := range chunks {
			if m.store.IsChunkFailed(c.FileID, c.ChunkIndex) {
				log.Printf("check %s: skip failed chunk %s/%d", recipientID, c.FileID, c.ChunkIndex)
				continue
			}
			if c.CreatedAt > newLastCheck {
				newLastCheck = c.CreatedAt
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
			m.collectAndProcessMessage(msgID, msgChunks)
		}
	}

	if newLastCheck > lastCheck {
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

func (m *Messenger) collectAndProcessMessage(msgID string, records []muninn.ChunkRecord) {
	if !m.tryProcessMsg(msgID) {
		return
	}
	defer m.releaseProcessMsg(msgID)
	hasMsg, _ := m.store.FindMessageById(msgID)
	if hasMsg {
		log.Printf("collecting %s skipped", msgID)
		return
	}
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
		m.store.DeleteFailedChunk(rec.FileID, rec.ChunkIndex)
		if rec.Hash != "" && chunk.RegisteredHash(data) != rec.Hash {
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
		payload.Timestamp = time.Now()
	}

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
	}

	m.msgSubsMu.Lock()
	for _, sub := range m.msgSubs {
		select {
		case sub <- decryptedMsg:
		default:
		}
	}
	m.msgSubsMu.Unlock()

	log.Printf("message %s delivered from %s", msgID, records[0].SenderID)
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
