package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"encoding/json"
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

//export messenger_create
func messenger_create(username, muninnAddr, dbPath, chunkTTL, turnAddr, turnUser, turnPass *C.char) C.long {
	goUser := C.GoString(username)
	goMuninn := C.GoString(muninnAddr)
	goDB := C.GoString(dbPath)
	goTTL := C.GoString(chunkTTL)
	goTurnAddr := C.GoString(turnAddr)
	goTurnUser := C.GoString(turnUser)
	goTurnPass := C.GoString(turnPass)

	if goUser == "" {
		return -1
	}
	if goMuninn == "" {
		goMuninn = "http://158.160.123.117:3080"
	}
	if goDB == "" {
		goDB = "huginn.db"
	}
	if goTTL == "" {
		goTTL = "1w"
	}

	absDB, err := filepath.Abs(goDB)
	if err == nil {
		goDB = absDB
	}

	cfg := &config.Config{
		Username:     goUser,
		MuninnAddr:   goMuninn,
		DBPath:       goDB,
		ChunkTTL:     goTTL,
		PeerFlag:     "thin",
		TurnAddr:     goTurnAddr,
		TurnUsername: goTurnUser,
		TurnPassword: goTurnPass,
	}

	mc := muninn.NewClient(cfg.MuninnAddr)

	mOpts := []messenger.MessengerOption{
		messenger.WithPeerFlag(muninn.PeerFlag(cfg.PeerFlag)),
		messenger.WithTURN(cfg.TurnAddr, cfg.TurnUsername, cfg.TurnPassword),
	}
	m, err := messenger.New(cfg.Username, mc, cfg.DBPath, mOpts...)
	if err != nil {
		log.Printf("messenger_create: %v", err)
		return -2
	}

	storedCfg := m.Config()
	if storedCfg.Username != "" {
		cfg.Username = storedCfg.Username
	}
	if storedCfg.MuninnAddr != "" {
		cfg.MuninnAddr = storedCfg.MuninnAddr
	}
	if storedCfg.ChunkTTL != "" {
		cfg.ChunkTTL = storedCfg.ChunkTTL
	}
	if storedCfg.PeerFlag != "" {
		cfg.PeerFlag = storedCfg.PeerFlag
	}
	if storedCfg.TurnAddr != "" {
		cfg.TurnAddr = storedCfg.TurnAddr
	}
	if storedCfg.TurnUsername != "" {
		cfg.TurnUsername = storedCfg.TurnUsername
	}
	if storedCfg.TurnPassword != "" {
		cfg.TurnPassword = storedCfg.TurnPassword
	}
	if storedCfg.PeerID != "" {
		cfg.PeerID = storedCfg.PeerID
	}
	cfg.DBPath = goDB
	if goTurnAddr != "" {
		cfg.TurnAddr = goTurnAddr
		cfg.TurnUsername = goTurnUser
		cfg.TurnPassword = goTurnPass
	}
	if goMuninn != "" && goMuninn != "http://158.160.123.117:3080" {
		cfg.MuninnAddr = goMuninn
	}
	if goTTL != "" && goTTL != "1w" {
		cfg.ChunkTTL = goTTL
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

func (inst *instance) eventLoop() {
	peerCh := inst.m.SubscribePeers()
	msgCh := inst.m.SubscribeMessages()
	fileReadyCh := inst.m.SubscribeFileReady()
	defer inst.m.UnsubscribePeers(peerCh)
	defer inst.m.UnsubscribeMessages(msgCh)
	defer inst.m.UnsubscribeFileReady(fileReadyCh)

	for {
		select {
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

func (inst *instance) pushEvent(typ string, data json.RawMessage) {
	select {
	case inst.events <- Event{Type: typ, Data: data}:
	default:
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
	resp := map[string]interface{}{
		"username":  inst.cfg.Username,
		"peer_id":   inst.cfg.PeerID,
		"muninn":    inst.cfg.MuninnAddr,
		"chunk_ttl": inst.cfg.ChunkTTL,
		"db_path":   inst.cfg.DBPath,
		"turn_addr": inst.cfg.TurnAddr,
		"turn_user": inst.cfg.TurnUsername,
		"turn_pass": inst.cfg.TurnPassword,
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
		Username string `json:"username"`
		PeerID   string `json:"peer_id"`
		Muninn   string `json:"muninn"`
		ChunkTTL string `json:"chunk_ttl"`
		TurnAddr string `json:"turn_addr"`
		TurnUser string `json:"turn_user"`
		TurnPass string `json:"turn_pass"`
	}
	if err := json.Unmarshal([]byte(C.GoString(jsonConfig)), &req); err != nil {
		return errorJSON("bad request: " + err.Error())
	}
	if req.Username != "" {
		inst.cfg.Username = req.Username
	}
	if req.PeerID != "" {
		inst.cfg.PeerID = req.PeerID
	}
	if req.Muninn != "" {
		inst.cfg.MuninnAddr = req.Muninn
	}
	if req.ChunkTTL != "" {
		inst.cfg.ChunkTTL = req.ChunkTTL
	}
	if req.TurnAddr != "" {
		inst.cfg.TurnAddr = req.TurnAddr
	}
	if req.TurnUser != "" {
		inst.cfg.TurnUsername = req.TurnUser
	}
	if req.TurnPass != "" {
		inst.cfg.TurnPassword = req.TurnPass
	}
	if err := inst.m.SaveConfig(inst.cfg); err != nil {
		return errorJSON("failed to save: " + err.Error())
	}
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
