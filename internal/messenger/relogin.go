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

	m.reloginMu.Lock()
	m.reloginKeys = ""
	m.reloginMu.Unlock()

	m.rtcManager.SendReloginRequest(peerID, webrtc.ReloginRequest{Signature: signature})

	for i := 0; i < 300; i++ {
		m.reloginMu.Lock()
		data := m.reloginKeys
		m.reloginMu.Unlock()
		if data != "" {
			if err := m.store.SaveKeysJSON(data); err != nil {
				return fmt.Errorf("save keys: %w", err)
			}
			m.signPublic, m.signPrivate, m.encPrivate, m.encPublic, err = crypto.ParseKeyFile([]byte(data))
			if err != nil {
				return fmt.Errorf("parse relogin keys: %w", err)
			}
			m.Key = peer.Key()
			peerUsername := peer.Login
			m.Username = peerUsername

			if m.appConfig == nil {
				m.appConfig = &config.Config{}
			}
			m.appConfig.Username = peerUsername
			m.appConfig.PeerID = peer.ID
			if err := m.store.SaveAppConfig(m.appConfig); err != nil {
				return fmt.Errorf("save config: %w", err)
			}
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("relogin: timed out waiting for response from %s", peerID)
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
		return
	}
	m.rtcManager.SendReloginResponse(peerID, webrtc.ReloginResponse{KeysData: keysData})
}

func (m *Messenger) handleReloginResponse(peerID string, resp webrtc.ReloginResponse) {
	m.reloginMu.Lock()
	m.reloginKeys = resp.KeysData
	m.reloginMu.Unlock()
}