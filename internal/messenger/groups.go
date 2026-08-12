package messenger

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/killbane1232/huginn-messenger/internal/crypto"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
	"github.com/killbane1232/huginn-messenger/internal/store"
	//"runtime/debug"
)

const invitePrefix = "__group_invite__:"

type groupInvitePayload struct {
	UID         string `json:"uid"`
	Name        string `json:"name"`
	EncPrivate  string `json:"enc_private"`
	EncPublic   string `json:"enc_public"`
	SignPrivate string `json:"sign_private"`
	SignPublic  string `json:"sign_public"`
}

func parseInvitePayload(text string) (*groupInvitePayload, error) {
	raw := strings.TrimPrefix(text, invitePrefix)
	var inv groupInvitePayload
	if err := json.Unmarshal([]byte(raw), &inv); err != nil {
		return nil, err
	}
	return &inv, nil
}

func (m *Messenger) checkInviteText(text string) string {
	if !strings.HasPrefix(text, invitePrefix) {
		return text
	}
	inv, err := parseInvitePayload(text)
	if err != nil {
		log.Printf("parse invite: %v", err)
		return text
	}
	existing, _ := m.store.GetGroupChat(inv.UID)
	if existing == nil {
		gc := &store.GroupChat{
			UID:         inv.UID,
			Name:        inv.Name,
			EncPrivate:  inv.EncPrivate,
			EncPublic:   inv.EncPublic,
			SignPrivate: inv.SignPrivate,
			SignPublic:  inv.SignPublic,
			CreatedAt:   time.Now(),
		}
		if err := m.store.CreateGroupChat(gc); err != nil {
			log.Printf("save group invite: %v", err)
		} else {
			log.Printf("joined group %s (%s) via invite", inv.Name, inv.UID)
		}
	}
	return fmt.Sprintf("You were invited to group chat: %s", inv.Name)
}

func (m *Messenger) CreateGroupChat(name string) (*store.GroupChat, error) {
	if existing, _ := m.store.GetGroupChatByName(name); existing != nil {
		return nil, fmt.Errorf("group chat %q already exists", name)
	}

	signPub, signPriv, err := crypto.GenerateSigningKey()
	if err != nil {
		return nil, fmt.Errorf("generate group signing key: %w", err)
	}
	encPriv, encPub, err := crypto.GenerateEncryptionKey()
	if err != nil {
		return nil, fmt.Errorf("generate group enc key: %w", err)
	}

	uid := uuid.New().String()
	gc := &store.GroupChat{
		UID:         uid,
		Name:        name,
		EncPrivate:  crypto.EncodeKey(encPriv),
		EncPublic:   crypto.EncodeKey(encPub),
		SignPrivate: crypto.EncodeKey(signPriv),
		SignPublic:  crypto.EncodeKey(signPub),
		CreatedAt:   time.Now(),
	}

	if err := m.store.CreateGroupChat(gc); err != nil {
		return nil, fmt.Errorf("save group chat: %w", err)
	}

	m.upsertPeer(uid, uid+":"+gc.SignPublic, name, gc.EncPublic, gc.SignPublic, time.Now(), 86400, true)

	log.Printf("group chat %s created with uid %s", name, uid)
	return gc, nil
}

func (m *Messenger) GetGroupChats() ([]store.GroupChat, error) {
	return m.store.GetGroupChats()
}

func (m *Messenger) registerGroupPeer(gc store.GroupChat) {
	fake := true
	req := &muninn.RegisterRequest{
		ID:            gc.UID,
		Login:         gc.UID,
		EncryptionKey: gc.EncPublic,
		SignatureKey:  gc.SignPublic,
		TTLSeconds:    groupPeerTTLSeconds,
		PeerFlag:      muninn.PeerFlagVeryThick,
		Fake:          &fake,
	}
	if err := m.muninnClient.Register(m.ctx, req); err != nil {
		log.Printf("register group peer %s (%s): %v", gc.Name, gc.UID, err)
	}
}

func (m *Messenger) registerStoredGroupPeers() {
	if m.store == nil {
		return
	}
	groups, err := m.store.GetGroupChats()
	if err != nil {
		log.Printf("load group peers for registration: %v", err)
		return
	}
	for _, group := range groups {
		m.registerGroupPeer(group)
	}
}

func (m *Messenger) groupRefreshLoop() {
	m.registerStoredGroupPeers()
	ticker := time.NewTicker(groupRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.registerStoredGroupPeers()
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Messenger) InviteToGroupChat(groupUID, memberID string) error {
	gc, err := m.store.GetGroupChat(groupUID)
	if err != nil {
		return fmt.Errorf("group not found: %w", err)
	}

	inv := groupInvitePayload{
		UID:         gc.UID,
		Name:        gc.Name,
		EncPrivate:  gc.EncPrivate,
		EncPublic:   gc.EncPublic,
		SignPrivate: gc.SignPrivate,
		SignPublic:  gc.SignPublic,
	}
	invData, _ := json.Marshal(inv)
	inviteText := invitePrefix + string(invData)

	return m.SendMessage(memberID, inviteText, nil, 604800)
}
