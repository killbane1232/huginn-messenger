package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unsafe"

	"github.com/killbane1232/huginn-messenger/internal/config"
	"github.com/killbane1232/huginn-messenger/internal/messenger"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
)

type instance struct {
	m      *messenger.Messenger
	cfg    *config.Config
	events chan Event
	done   chan struct{}
}

type Event struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

var (
	instances   = make(map[int64]*instance)
	instancesMu sync.Mutex
	nextID      int64 = 1
)

func getInstance(handle int64) *instance {
	instancesMu.Lock()
	defer instancesMu.Unlock()
	return instances[handle]
}

func storeInstance(inst *instance) int64 {
	instancesMu.Lock()
	defer instancesMu.Unlock()
	id := nextID
	nextID++
	instances[id] = inst
	return id
}

func removeInstance(handle int64) {
	instancesMu.Lock()
	defer instancesMu.Unlock()
	delete(instances, handle)
}

// Legacy entry points retain their ABI; the three reserved arguments are ignored.
// An empty muninnAddr loads saved settings. Returns -4 if none are configured.
//
//export messenger_create
func messenger_create(username, muninnAddr, dbPath, chunkTTL, reserved1, reserved2, reserved3 *C.char) C.long {
	return createMessenger(C.GoString(username), C.GoString(muninnAddr), C.GoString(dbPath), C.GoString(chunkTTL), "", nil, nil)
}

//export messenger_create_with_peer_flag
func messenger_create_with_peer_flag(username, muninnAddr, dbPath, chunkTTL, reserved1, reserved2, reserved3, peerFlag *C.char) C.long {
	return createMessenger(C.GoString(username), C.GoString(muninnAddr), C.GoString(dbPath), C.GoString(chunkTTL), C.GoString(peerFlag), nil, nil)
}

// messenger_create_with_config receives the full candidate list before startup
// selection or registration. Omitted candidates load the saved SQLite settings.
//
//export messenger_create_with_config
func messenger_create_with_config(jsonConfig, dbPath *C.char) C.long {
	var cfg config.Config
	if err := json.Unmarshal([]byte(C.GoString(jsonConfig)), &cfg); err != nil {
		return -3
	}
	return createMessenger(cfg.Username, cfg.MuninnAddr, C.GoString(dbPath), cfg.ChunkTTL, cfg.PeerFlag, cfg.MuninnServers, cfg.MuninnICE)
}

func createMessenger(goUser, goMuninn, goDB, goTTL, requestedPeerFlag string, servers []string, profiles map[string]config.ICEConfig) C.long {
	if goUser == "" {
		return -1
	}
	if goDB == "" {
		goDB = "huginn.db"
	}
	if goTTL == "" {
		goTTL = "1w"
	}
	peerFlag, ok := parsePeerFlag(requestedPeerFlag)
	if !ok {
		return -3
	}

	absDB, err := filepath.Abs(goDB)
	if err == nil {
		goDB = absDB
	}

	cfg := &config.Config{
		Username:   goUser,
		MuninnAddr: goMuninn,
		DBPath:     goDB,
		ChunkTTL:   goTTL,
		PeerFlag:   string(peerFlag),
	}

	var mc *muninn.Client
	if cfg.MuninnAddr != "" {
		mc = muninn.NewClient(cfg.MuninnAddr)
	}

	mOpts := []messenger.MessengerOption{
		messenger.WithPeerFlag(peerFlag),
	}
	if servers != nil {
		mOpts = append(mOpts, messenger.WithMuninnServers(servers))
	}
	if profiles != nil {
		mOpts = append(mOpts, messenger.WithMuninnICE(profiles))
	}
	m, err := messenger.New(cfg.Username, mc, cfg.DBPath, mOpts...)
	if err != nil {
		if errors.Is(err, messenger.ErrMuninnAddressRequired) {
			return -4
		}
		log.Printf("messenger_create: %v", err)
		return -2
	}

	storedCfg := m.Config()
	cfg.MuninnICE = config.CloneMuninnICE(storedCfg.MuninnICE)
	cfg.MuninnServers = append([]string(nil), storedCfg.MuninnServers...)
	if storedCfg.Username != "" {
		cfg.Username = storedCfg.Username
	}
	if storedCfg.MuninnAddr != "" {
		cfg.MuninnAddr = storedCfg.MuninnAddr
	}
	if storedCfg.ChunkTTL != "" {
		cfg.ChunkTTL = storedCfg.ChunkTTL
	}
	if storedCfg.PeerFlag != "" && requestedPeerFlag == "" {
		cfg.PeerFlag = storedCfg.PeerFlag
	}
	if storedCfg.PeerID != "" {
		cfg.PeerID = storedCfg.PeerID
	}
	cfg.DBPath = goDB
	if goTTL != "" && goTTL != "1w" {
		cfg.ChunkTTL = goTTL
	}
	if requestedPeerFlag != "" {
		cfg.PeerFlag = string(peerFlag)
	}
	if err := m.SaveConfig(cfg); err != nil {
		log.Printf("save config to db: %v", err)
	}
	log.Printf("config loaded from db, username=%s", cfg.Username)

	go func() {
		if err := m.Register(); err != nil {
			log.Printf("messenger_create: register warning: %v", err)
		}
	}()

	inst := &instance{
		m:      m,
		cfg:    cfg,
		events: make(chan Event, 100),
		done:   make(chan struct{}),
	}

	go inst.eventLoop()

	return C.long(storeInstance(inst))
}

func parsePeerFlag(value string) (muninn.PeerFlag, bool) {
	if value == "" {
		return muninn.PeerFlagThin, true
	}
	flag := muninn.PeerFlag(value)
	switch flag {
	case muninn.PeerFlagThin, muninn.PeerFlagThick, muninn.PeerFlagVeryThick:
		return flag, true
	default:
		return "", false
	}
}

func (inst *instance) eventLoop() {
	peerCh := inst.m.SubscribePeers()
	msgCh := inst.m.SubscribeMessages()
	fileReadyCh := inst.m.SubscribeFileReady()
	defer inst.m.UnsubscribePeers(peerCh)
	defer inst.m.UnsubscribeMessages(msgCh)
	defer inst.m.UnsubscribeFileReady(fileReadyCh)
	connectionTick := time.NewTicker(250 * time.Millisecond)
	defer connectionTick.Stop()
	lastConnection := ""
	inst.pushConnectionState(&lastConnection)

	for {
		select {
		case <-connectionTick.C:
			inst.pushConnectionState(&lastConnection)

		case <-peerCh:
			peers := inst.m.GetPeers()
			if peers == nil {
				peers = []muninn.Peer{}
			}
			type peerResp struct {
				muninn.Peer
				Online bool `json:"online"`
			}
			resp := make([]peerResp, len(peers))
			for i, p := range peers {
				resp[i] = peerResp{Peer: p, Online: inst.m.IsPeerOnline(p.ID)}
			}
			data, _ := json.Marshal(resp)
			inst.pushEvent("peers", data)

		case msg := <-msgCh:
			data, _ := json.Marshal(msg)
			inst.pushEvent("message", data)

		case fileReady := <-fileReadyCh:
			data, _ := json.Marshal(fileReady)
			inst.pushEvent("file_ready", data)

		case <-inst.done:
			return
		}
	}
}

// Retry a changed status if the event queue was full, so the UI cannot remain
// stuck in an old state after a burst of messages. Sampling only reads local
// WebSocket state; it never probes the server or blocks the UI on network I/O.
func (inst *instance) pushConnectionState(last *string) {
	state := "connecting"
	if inst.m.IsMuninnConnected() {
		state = "connected"
	}
	if state == *last {
		return
	}
	data, _ := json.Marshal(map[string]string{"state": state})
	if inst.pushEvent("muninn_connection", data) {
		*last = state
	}
}

func (inst *instance) pushEvent(typ string, data json.RawMessage) bool {
	select {
	case inst.events <- Event{Type: typ, Data: data}:
		return true
	default:
		return false
	}
}

//export messenger_destroy
func messenger_destroy(handle C.long) {
	h := int64(handle)
	inst := getInstance(h)
	if inst == nil {
		return
	}
	close(inst.done)
	inst.m.Shutdown()
	removeInstance(h)
}

//export messenger_get_me
func messenger_get_me(handle C.long) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return errorJSON("invalid handle")
	}
	resp := map[string]string{
		"id":       inst.m.ID,
		"username": inst.m.Username,
		"peer_id":  inst.cfg.PeerID,
	}
	data, _ := json.Marshal(resp)
	return C.CString(string(data))
}

type peerWithOnline struct {
	muninn.Peer
	Online bool `json:"online"`
}

func peersToJSON(inst *instance, peers []muninn.Peer) *C.char {
	if peers == nil {
		return C.CString("[]")
	}
	resp := make([]peerWithOnline, len(peers))
	for i, p := range peers {
		resp[i] = peerWithOnline{Peer: p, Online: inst.m.IsPeerOnline(p.ID)}
	}
	data, _ := json.Marshal(resp)
	return C.CString(string(data))
}

func errorJSON(msg string) *C.char {
	resp := map[string]string{"error": msg}
	data, _ := json.Marshal(resp)
	return C.CString(string(data))
}

//export messenger_get_peers
func messenger_get_peers(handle C.long) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return errorJSON("invalid handle")
	}
	return peersToJSON(inst, inst.m.GetPeers())
}

//export messenger_search_peers
func messenger_search_peers(handle C.long, query *C.char) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return errorJSON("invalid handle")
	}
	return peersToJSON(inst, inst.m.SearchPeers(C.GoString(query)))
}

//export messenger_delete_chat
func messenger_delete_chat(handle C.long, chatID *C.char) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return errorJSON("invalid handle")
	}
	if err := inst.m.DeleteChat(C.GoString(chatID)); err != nil {
		return errorJSON(err.Error())
	}
	return okJSON()
}

//export messenger_get_hidden_chats
func messenger_get_hidden_chats(handle C.long) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return errorJSON("invalid handle")
	}
	ids, err := inst.m.GetHiddenChatIDs()
	if err != nil {
		return errorJSON(err.Error())
	}
	data, _ := json.Marshal(ids)
	return C.CString(string(data))
}

//export messenger_get_messages
func messenger_get_messages(handle C.long, peerID *C.char) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return errorJSON("invalid handle")
	}
	messages := inst.m.GetMessages(C.GoString(peerID))
	if messages == nil {
		return C.CString("[]")
	}
	// Sort by timestamp
	data, _ := json.Marshal(messages)
	return C.CString(string(data))
}

//export messenger_get_messages_paginated
func messenger_get_messages_paginated(handle C.long, peerID *C.char, limit, offset C.int) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return errorJSON("invalid handle")
	}
	messages := inst.m.GetMessagesDesc(C.GoString(peerID), int(limit), int(offset))
	if messages == nil {
		return C.CString("[]")
	}
	data, _ := json.Marshal(messages)
	return C.CString(string(data))
}

// Success means the message and retry payloads were committed locally.
// Validation, attachment and database errors are returned before acceptance.
//
//export messenger_send_message
func messenger_send_message(handle C.long, to, text *C.char, ttl C.int) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return errorJSON("invalid handle")
	}
	ttlSeconds := int(ttl)
	if ttlSeconds <= 0 {
		ttlSeconds = config.ChunkTTLSeconds(inst.cfg.ChunkTTL)
	}
	if err := inst.m.SendMessage(C.GoString(to), C.GoString(text), nil, ttlSeconds); err != nil {
		return errorJSON(err.Error())
	}
	return okJSON()
}

// Success includes durable storage of the encrypted attachment for retry.
//
//export messenger_send_file
func messenger_send_file(handle C.long, to, text, filePath *C.char, ttl C.int) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return errorJSON("invalid handle")
	}
	ttlSeconds := int(ttl)
	if ttlSeconds <= 0 {
		ttlSeconds = config.ChunkTTLSeconds(inst.cfg.ChunkTTL)
	}
	if err := inst.m.SendMessage(C.GoString(to), C.GoString(text), []string{C.GoString(filePath)}, ttlSeconds); err != nil {
		return errorJSON(err.Error())
	}
	return okJSON()
}

//export messenger_get_config
func messenger_get_config(handle C.long) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return errorJSON("invalid handle")
	}
	cfg := inst.m.Config()
	resp := map[string]interface{}{
		"muninn_servers": cfg.MuninnServers,
		"muninn_ice":     cfg.MuninnICE,
		"username":       cfg.Username,
		"peer_id":        cfg.PeerID,
		"muninn":         cfg.MuninnAddr,
		"chunk_ttl":      cfg.ChunkTTL,
		"db_path":        cfg.DBPath,
	}
	data, _ := json.Marshal(resp)
	return C.CString(string(data))
}

//export messenger_save_config
func messenger_save_config(handle C.long, jsonConfig *C.char) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return errorJSON("invalid handle")
	}
	var req struct {
		Username      string                      `json:"username"`
		PeerID        string                      `json:"peer_id"`
		Muninn        string                      `json:"muninn"`
		MuninnServers *[]string                   `json:"muninn_servers"`
		MuninnICE     map[string]config.ICEConfig `json:"muninn_ice"`
		ChunkTTL      string                      `json:"chunk_ttl"`
	}
	if err := json.Unmarshal([]byte(C.GoString(jsonConfig)), &req); err != nil {
		return errorJSON("bad request: " + err.Error())
	}
	cfg := inst.m.Config()
	if req.MuninnICE != nil {
		cfg.MuninnICE = config.CloneMuninnICE(req.MuninnICE)
	}
	if req.MuninnServers != nil {
		cfg.MuninnServers = append([]string(nil), (*req.MuninnServers)...)
	}
	if req.Username != "" {
		cfg.Username = req.Username
	}
	if req.PeerID != "" {
		cfg.PeerID = req.PeerID
	}
	if req.Muninn != "" {
		cfg.MuninnAddr = req.Muninn
	}
	if req.ChunkTTL != "" {
		cfg.ChunkTTL = req.ChunkTTL
	}
	if err := inst.m.SaveConfig(cfg); err != nil {
		return errorJSON("failed to save: " + err.Error())
	}
	inst.cfg = inst.m.Config()
	return okJSON()
}

//export messenger_get_event
func messenger_get_event(handle C.long, timeoutMs C.int) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return C.CString("")
	}
	if timeoutMs <= 0 {
		select {
		case evt := <-inst.events:
			data, _ := json.Marshal(evt)
			return C.CString(string(data))
		default:
			return C.CString("")
		}
	}
	timer := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
	defer timer.Stop()
	select {
	case evt := <-inst.events:
		data, _ := json.Marshal(evt)
		return C.CString(string(data))
	case <-timer.C:
		return C.CString("")
	}
}

//export messenger_create_group
func messenger_create_group(handle C.long, name *C.char) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return errorJSON("invalid handle")
	}
	gc, err := inst.m.CreateGroupChat(C.GoString(name))
	if err != nil {
		return errorJSON(err.Error())
	}
	data, _ := json.Marshal(gc)
	return C.CString(string(data))
}

//export messenger_get_groups
func messenger_get_groups(handle C.long) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return errorJSON("invalid handle")
	}
	groups, err := inst.m.GetGroupChats()
	if err != nil {
		return errorJSON(err.Error())
	}
	if groups == nil {
		return C.CString("[]")
	}
	data, _ := json.Marshal(groups)
	return C.CString(string(data))
}

//export messenger_invite_to_group
func messenger_invite_to_group(handle C.long, groupUID, userID *C.char) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return errorJSON("invalid handle")
	}
	if err := inst.m.InviteToGroupChat(C.GoString(groupUID), C.GoString(userID)); err != nil {
		return errorJSON(err.Error())
	}
	return okJSON()
}

//export messenger_mark_read
func messenger_mark_read(handle C.long, msgID *C.char) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return errorJSON("invalid handle")
	}
	if err := inst.m.MarkMessageRead(C.GoString(msgID)); err != nil {
		return errorJSON(err.Error())
	}
	return okJSON()
}

//export messenger_generate_relogin_signature
func messenger_generate_relogin_signature(handle C.long) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return errorJSON("invalid handle")
	}
	sig, err := inst.m.GenerateReloginSignature()
	if err != nil {
		return errorJSON(err.Error())
	}
	resp := map[string]string{"signature": sig}
	data, _ := json.Marshal(resp)
	return C.CString(string(data))
}

//export messenger_apply_relogin_signature
func messenger_apply_relogin_signature(handle C.long, signature *C.char) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return errorJSON("invalid handle")
	}
	if err := inst.m.ApplyReloginSignature(C.GoString(signature)); err != nil {
		return errorJSON(err.Error())
	}
	return okJSON()
}

//export messenger_get_file_downloads
func messenger_get_file_downloads(handle C.long) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return errorJSON("invalid handle")
	}
	downloads, err := inst.m.GetFileDownloads()
	if err != nil {
		return errorJSON(err.Error())
	}
	data, _ := json.Marshal(downloads)
	return C.CString(string(data))
}

//export messenger_cancel_file_download
func messenger_cancel_file_download(handle C.long, fileID *C.char) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return errorJSON("invalid handle")
	}
	if err := inst.m.CancelFileDownload(C.GoString(fileID)); err != nil {
		return errorJSON(err.Error())
	}
	return okJSON()
}

//export messenger_set_downloads_dir
func messenger_set_downloads_dir(handle C.long, dir *C.char) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return errorJSON("invalid handle")
	}
	goDir := C.GoString(dir)
	if goDir == "" {
		return errorJSON("empty path")
	}
	if err := os.MkdirAll(goDir, 0755); err != nil {
		return errorJSON("mkdir: " + err.Error())
	}
	inst.m.SetDownloadsDir(goDir)
	return okJSON()
}

//export messenger_get_downloads_dir
func messenger_get_downloads_dir(handle C.long) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return C.CString("")
	}
	return C.CString(inst.m.DownloadsDir())
}

//export messenger_get_file_path
func messenger_get_file_path(handle C.long, fileID *C.char) *C.char {
	inst := getInstance(int64(handle))
	if inst == nil {
		return C.CString("")
	}
	return C.CString(inst.m.DownloadsDir() + "/" + C.GoString(fileID))
}

//export messenger_is_peer_online
func messenger_is_peer_online(handle C.long, peerID *C.char) C.int {
	inst := getInstance(int64(handle))
	if inst == nil {
		return 0
	}
	if inst.m.IsPeerOnline(C.GoString(peerID)) {
		return 1
	}
	return 0
}

//export messenger_free_string
func messenger_free_string(s *C.char) {
	C.free(unsafe.Pointer(s))
}

func okJSON() *C.char {
	return C.CString(`{"status":"ok"}`)
}
