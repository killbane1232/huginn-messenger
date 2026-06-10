package ui

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"sort"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/messenger"
	"github.com/killbane1232/huginn-messenger/internal/muninn"
)

//go:embed static/*.css static/*.js
var staticFiles embed.FS

type Server struct {
	addr      string
	messenger *messenger.Messenger
	srv       *http.Server
}

func NewServer(port int, m *messenger.Messenger) *Server {
	return &Server{
		addr:      fmt.Sprintf(":%d", port),
		messenger: m,
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
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err := s.messenger.SendMessage(req.To, req.Text); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
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
      <div class="user-badge">{{.Username}}</div>
    </div>
    <input type="text" id="peer-search" placeholder="Search users..." />
    <div id="peer-list"></div>
  </aside>
  <main id="main">
    <div id="chat-header"></div>
    <div id="messages"></div>
    <div id="input-area">
      <input type="text" id="msg-input" placeholder="Type a message..." autofocus>
      <button id="send-btn">Send</button>
    </div>
  </main>
</div>
<div id="no-chat">
  <p>Select a peer to start chatting</p>
</div>
<script src="/static/app.js"></script>
</body>
</html>`
