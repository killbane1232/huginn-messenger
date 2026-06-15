package ui

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/config"
	"github.com/killbane1232/huginn-messenger/internal/messenger"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
)

//go:embed static/*.css static/*.js
var staticFiles embed.FS

type Server struct {
	addr      string
	messenger *messenger.Messenger
	cfg       *config.Config
	srv       *http.Server
}

func NewServer(cfg *config.Config, m *messenger.Messenger) *Server {
	return &Server{
		addr:      fmt.Sprintf(":%d", cfg.UIPort),
		messenger: m,
		cfg:       cfg,
	}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()

	staticFS, _ := fs.Sub(staticFiles, "static")
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))

	mux.HandleFunc("GET /", s.handleIndex)
	mux.HandleFunc("GET /api/me", s.handleMe)
	mux.HandleFunc("GET /api/peers/search", s.handlePeerSearch)
	mux.HandleFunc("GET /api/peers", s.handlePeers)
	mux.HandleFunc("GET /api/messages/{peer}", s.handleMessages)
	mux.HandleFunc("POST /api/send", s.handleSend)
	mux.HandleFunc("GET /api/events", s.handleSSE)
	mux.HandleFunc("POST /api/send-file", s.handleSendFile)
	mux.HandleFunc("GET /api/files/{fileID}", s.handleGetFile)
	mux.HandleFunc("GET /api/config", s.handleGetConfig)
	mux.HandleFunc("POST /api/config", s.handleSaveConfig)

	s.srv = &http.Server{
		Addr:         s.addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0,
	}
	return s.srv.ListenAndServe()
}

func (s *Server) Shutdown() error {
	if s.srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.srv.Shutdown(ctx)
	}
	return nil
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	tmpl := template.Must(template.New("index").Parse(indexHTML))
	tmpl.Execute(w, map[string]string{
		"Username": s.messenger.Username,
	})
}

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{
		"id":       s.messenger.ID,
		"username": s.messenger.Username,
	})
}

func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]interface{}{
		"username":  s.cfg.Username,
		"muninn":    s.cfg.MuninnAddr,
		"ui_port":   s.cfg.UIPort,
		"chunk_ttl": s.cfg.ChunkTTL,
	})
}

func (s *Server) handleSaveConfig(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Muninn   string `json:"muninn"`
		UIPort   int    `json:"ui_port"`
		ChunkTTL string `json:"chunk_ttl"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	s.cfg.Username = req.Username
	s.cfg.MuninnAddr = req.Muninn
	s.cfg.UIPort = req.UIPort
	if req.ChunkTTL != "" {
		s.cfg.ChunkTTL = req.ChunkTTL
	}

	if err := s.cfg.Save(); err != nil {
		http.Error(w, "failed to save config: "+err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

type peerResponse struct {
	muninn.Peer
	Online bool `json:"online"`
}

func (s *Server) handlePeerSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	peers := s.messenger.SearchPeers(q)
	resp := make([]peerResponse, len(peers))
	for i, p := range peers {
		resp[i] = peerResponse{Peer: p, Online: s.messenger.IsPeerOnline(p.ID)}
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handlePeers(w http.ResponseWriter, r *http.Request) {
	peers := s.messenger.GetPeers()
	if peers == nil {
		peers = []muninn.Peer{}
	}
	resp := make([]peerResponse, len(peers))
	for i, p := range peers {
		resp[i] = peerResponse{Peer: p, Online: s.messenger.IsPeerOnline(p.ID)}
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	peerID := r.PathValue("peer")
	messages := s.messenger.GetMessages(peerID)
	if messages == nil {
		messages = []messenger.ChatMessage{}
	}
	sort.Slice(messages, func(i, j int) bool {
		return messages[i].Timestamp.Before(messages[j].Timestamp)
	})
	json.NewEncoder(w).Encode(messages)
}

func (s *Server) handleSend(w http.ResponseWriter, r *http.Request) {
	var req struct {
		To   string `json:"to"`
		Text string `json:"text"`
		TTL  int    `json:"ttl"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	ttl := req.TTL
	if ttl <= 0 {
		ttl = config.ChunkTTLSeconds(s.cfg.ChunkTTL)
	}
	if err := s.messenger.SendMessage(req.To, req.Text, ttl); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleSendFile(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(100 << 20); err != nil {
		http.Error(w, "failed to parse form: "+err.Error(), http.StatusBadRequest)
		return
	}

	toPeer := r.FormValue("to")
	text := r.FormValue("text")
	ttlStr := r.FormValue("ttl")
	ttl := 0
	if ttlStr != "" {
		fmt.Sscanf(ttlStr, "%d", &ttl)
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "missing file: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	tmpDir := filepath.Join(os.TempDir(), "huginn-uploads")
	os.MkdirAll(tmpDir, 0755)
	tmpPath := filepath.Join(tmpDir, header.Filename)
	dst, err := os.Create(tmpPath)
	if err != nil {
		http.Error(w, "failed to save upload: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := io.Copy(dst, file); err != nil {
		dst.Close()
		os.Remove(tmpPath)
		http.Error(w, "failed to save upload: "+err.Error(), http.StatusInternalServerError)
		return
	}
	dst.Close()
	defer os.Remove(tmpPath)

	if ttl <= 0 {
		ttl = config.ChunkTTLSeconds(s.cfg.ChunkTTL)
	}

	if err := s.messenger.SendMessageWithFiles(toPeer, text, []string{tmpPath}, ttl); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleGetFile(w http.ResponseWriter, r *http.Request) {
	fileID := r.PathValue("fileName")
	if fileID == nil || fileID == "" {
		fileID = r.PathValue("fileId")
	}
	fp := filepath.Join(s.messenger.DownloadsDir(), fileID)
	w.Header().Set("Content-Disposition", "attachment; filename=\""+fileID+"\"")
	http.ServeFile(w, r, fp)
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	peerCh := s.messenger.SubscribePeers()
	msgCh := s.messenger.SubscribeMessages()
	defer s.messenger.UnsubscribePeers(peerCh)
	defer s.messenger.UnsubscribeMessages(msgCh)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-peerCh:
			peers := s.messenger.GetPeers()
			resp := make([]peerResponse, len(peers))
			for i, p := range peers {
				resp[i] = peerResponse{Peer: p, Online: s.messenger.IsPeerOnline(p.ID)}
			}
			data, _ := json.Marshal(resp)
			fmt.Fprintf(w, "event: peers\ndata: %s\n\n", data)
			flusher.Flush()

		case msg := <-msgCh:
			data, _ := json.Marshal(msg)
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", data)
			flusher.Flush()

		case <-r.Context().Done():
			return
		}
	}
}

var indexHTML = `<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Huginn Messenger</title>
<link rel="stylesheet" href="/static/style.css">
</head>
<body>
<div id="app">
  <aside id="sidebar">
    <div class="sidebar-header">
      <h2>Huginn</h2>
      <div class="user-badge">
        <span>{{.Username}}</span>
        <button id="settings-btn" title="Settings">&#9881;</button>
      </div>
    </div>
    <input type="text" id="peer-search" placeholder="Search users..." />
    <div id="peer-list"></div>
  </aside>
  <main id="main">
    <div id="chat-header"></div>
    <div id="messages"></div>
    <div id="input-area">
      <input type="text" id="msg-input" placeholder="Type a message..." autofocus>
      <label id="file-btn-label" title="Attach file">
        <input type="file" id="file-input" hidden>
        &#128206;
      </label>
      <span id="file-name"></span>
      <select id="ttl-select">
        <option value="0">Default TTL</option>
        <option value="86400">1 day</option>
        <option value="604800" selected>1 week</option>
        <option value="2592000">1 month</option>
      </select>
      <button id="send-btn">Send</button>
    </div>
  </main>
  <main id="config-panel" style="display:none">
    <div id="config-header">Configuration</div>
    <div class="config-form">
      <label for="cfg-username">Username</label>
      <input type="text" id="cfg-username" placeholder="your username">

      <label for="cfg-muninn">Muninn address</label>
      <input type="text" id="cfg-muninn" placeholder="http://localhost:8080">

      <label for="cfg-ui-port">UI port (0 = random)</label>
      <input type="number" id="cfg-ui-port" placeholder="0">

      <label for="cfg-chunk-ttl">Chunk TTL</label>
      <select id="cfg-chunk-ttl">
        <option value="1d">1 day</option>
        <option value="1w">1 week</option>
        <option value="1m">1 month</option>
      </select>

      <div class="config-actions">
        <button id="cfg-save" class="btn-primary">Save</button>
        <button id="cfg-cancel" class="btn-secondary">Cancel</button>
      </div>
      <div id="cfg-status"></div>
    </div>
  </main>
</div>
<div id="no-chat">
  <p>Select a peer to start chatting</p>
</div>
<script src="/static/app.js"></script>
</body>
</html>`
