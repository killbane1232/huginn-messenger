package messenger

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/config"
	"github.com/killbane1232/huginn-messenger/internal/crypto"
	"github.com/killbane1232/huginn-messenger/internal/webrtc"
	//"runtime/debug"
)

const reloginTimeout = 2 * time.Minute

func (m *Messenger) GenerateReloginSignature() (string, error) {
	challenge := make([]byte, 32)
	if _, err := rand.Read(challenge); err != nil {
		return "", fmt.Errorf("generate challenge: %w", err)
	}
	sig := crypto.Sign(m.signPrivate, challenge)
	challengeB64 := base64.StdEncoding.EncodeToString(challenge)
	sigB64 := base64.StdEncoding.EncodeToString(sig)
	return fmt.Sprintf("%s:%s.%s", m.ID, challengeB64, sigB64), nil
}

func (m *Messenger) ApplyReloginSignature(signature string) error {
	parts := strings.SplitN(signature, ":", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid signature: missing peer ID")
	}
	peerID := parts[0]

	dataParts := strings.SplitN(parts[1], ".", 2)
	if len(dataParts) != 2 {
		return fmt.Errorf("invalid signature: missing challenge or signature")
	}
	challengeB64, sigB64 := dataParts[0], dataParts[1]

	challenge, err := base64.StdEncoding.DecodeString(challengeB64)
	if err != nil {
		return fmt.Errorf("decode challenge: %w", err)
	}
	sig, err := base64.StdEncoding.DecodeString(sigB64)
	if err != nil {
		return fmt.Errorf("decode signature: %w", err)
	}

	peer := m.findPeerByID(peerID)
	if peer == nil {
		return fmt.Errorf("peer %s not found", peerID)
	}
	peerSignKey, err := crypto.DecodeKey(peer.SignatureKey)
	if err != nil {
		return fmt.Errorf("decode peer sign key: %w", err)
	}
	if !crypto.Verify(ed25519.PublicKey(peerSignKey), challenge, sig) {
		return fmt.Errorf("invalid signature: not authorized")
	}

	if !m.rtcManager.IsConnected(peerID) {
		if err := m.ConnectPeer(peerID); err != nil {
			return fmt.Errorf("connect to %s: %w", peerID, err)
		}
	}

	for i := 0; i < 50; i++ {
		if m.rtcManager.IsConnected(peerID) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !m.rtcManager.IsConnected(peerID) {
		return fmt.Errorf("timed out waiting for WebRTC connection to %s", peerID)
	}

	resultChannel := make(chan reloginResult, 1)
	transfer := &reloginTransferState{
		expectedPeerID: peerID,
		result:         resultChannel,
	}
	m.reloginMu.Lock()
	if m.reloginTransfer != nil {
		m.reloginMu.Unlock()
		return fmt.Errorf("relogin is already in progress")
	}
	m.reloginTransfer = transfer
	m.reloginMu.Unlock()
	defer m.clearReloginTransfer(transfer)

	if err := m.rtcManager.SendReloginRequest(peerID, webrtc.ReloginRequest{Signature: signature}); err != nil {
		return fmt.Errorf("send relogin request: %w", err)
	}

	timer := time.NewTimer(reloginTimeout)
	defer timer.Stop()
	var result reloginResult
	select {
	case result = <-resultChannel:
	case <-timer.C:
		return fmt.Errorf("relogin: timed out waiting for replication from %s", peerID)
	case <-m.ctx.Done():
		return fmt.Errorf("relogin: %w", m.ctx.Err())
	}
	if result.err != nil {
		return result.err
	}
	if result.keysData == "" {
		return fmt.Errorf("relogin: source returned empty keys")
	}

	if err := m.store.SaveKeysJSON(result.keysData); err != nil {
		return fmt.Errorf("save keys: %w", err)
	}
	m.signPublic, m.signPrivate, m.encPrivate, m.encPublic, err = crypto.ParseKeyFile([]byte(result.keysData))
	if err != nil {
		return fmt.Errorf("parse relogin keys: %w", err)
	}
	targetPeerID := m.ID
	m.Key = peer.Key()
	peerUsername := peer.Login
	m.Username = peerUsername

	if m.appConfig == nil {
		m.appConfig = &config.Config{}
	}
	m.appConfig.Username = peerUsername
	m.appConfig.PeerID = targetPeerID
	if err := m.store.SaveAppConfig(m.appConfig); err != nil {
		return fmt.Errorf("save config: %w", err)
	}

	m.reloadReplicatedPeers()
	if result.snapshot.Version == reloginSnapshotVersion {
		m.queueReplicatedFiles(result.snapshot.Messages, peerID, peerUsername)
	}
	return nil
}

func (m *Messenger) handleReloginRequest(peerID string, req webrtc.ReloginRequest) {
	parts := strings.SplitN(req.Signature, ":", 2)
	log.Printf("relogin: handle")
	if len(parts) != 2 {
		return
	}
	dataParts := strings.SplitN(parts[1], ".", 2)
	if len(dataParts) != 2 {
		return
	}
	challenge, err := base64.StdEncoding.DecodeString(dataParts[0])
	if err != nil {
		return
	}
	sig, err := base64.StdEncoding.DecodeString(dataParts[1])
	if err != nil {
		return
	}
	if !crypto.Verify(m.signPublic, challenge, sig) {
		log.Printf("relogin: invalid signature from %s", peerID)
		return
	}
	keysData, err := m.store.GetKeysJSON()
	if err != nil {
		log.Printf("relogin: read keys from db: %v", err)
		_ = m.rtcManager.SendReloginResponse(peerID, webrtc.ReloginResponse{Error: "read source keys"})
		return
	}
	replica, replicaHash, err := m.buildReloginReplica()
	if err != nil {
		log.Printf("relogin: build replication snapshot: %v", err)
		_ = m.rtcManager.SendReloginResponse(peerID, webrtc.ReloginResponse{Error: "build replication snapshot"})
		return
	}
	transferID := newReloginTransferID()
	chunkCount := (len(replica) + reloginChunkSize - 1) / reloginChunkSize
	response := webrtc.ReloginResponse{
		KeysData:   keysData,
		TransferID: transferID,
		ChunkCount: chunkCount,
		SHA256:     replicaHash,
	}
	if err := m.rtcManager.SendReloginResponse(peerID, response); err != nil {
		log.Printf("relogin: send response to %s: %v", peerID, err)
		return
	}
	for index := 0; index < chunkCount; index++ {
		if err := m.waitForReloginSendCapacity(peerID); err != nil {
			log.Printf("relogin: wait for send capacity to %s: %v", peerID, err)
			return
		}
		start := index * reloginChunkSize
		end := min(start+reloginChunkSize, len(replica))
		if err := m.rtcManager.SendReloginChunk(peerID, webrtc.ReloginChunk{
			TransferID: transferID,
			Index:      index,
			Data:       replica[start:end],
		}); err != nil {
			log.Printf("relogin: send replication chunk %d/%d to %s: %v", index+1, chunkCount, peerID, err)
			return
		}
	}
	log.Printf("relogin: sent %d replication chunks to %s", chunkCount, peerID)
}

func (m *Messenger) handleReloginResponse(peerID string, resp webrtc.ReloginResponse) {
	m.acceptReloginResponse(peerID, resp)
}

func (m *Messenger) handleReloginChunk(peerID string, chunk webrtc.ReloginChunk) {
	m.acceptReloginChunk(peerID, chunk)
}
