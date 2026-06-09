package p2p

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type StoredChunk struct {
	MessageID  string    `json:"message_id"`
	ChunkIndex int       `json:"chunk_index"`
	Data       []byte    `json:"data"`
	StoredAt   time.Time `json:"stored_at"`
}

type SignalMsg struct {
	From string `json:"from"`
	Type string `json:"type"`
	Data string `json:"data"`
}

type Server struct {
	addr     string
	mux      *http.ServeMux
	srv      *http.Server
	chunks   map[string]*StoredChunk
	chunksMu sync.RWMutex
	signals  map[string][]SignalMsg
	signalsMu sync.RWMutex
}

func NewServer(port int) *Server {
	s := &Server{
		addr:    fmt.Sprintf(":%d", port),
		mux:     http.NewServeMux(),
		chunks:  make(map[string]*StoredChunk),
		signals: make(map[string][]SignalMsg),
	}
	s.mux.HandleFunc("POST /api/chunk/store", s.handleStoreChunk)
	s.mux.HandleFunc("GET /api/chunk/{message_id}/{index}", s.handleGetChunk)
	s.mux.HandleFunc("DELETE /api/chunk/{message_id}/{index}", s.handleDeleteChunk)
	s.mux.HandleFunc("POST /api/signal/{peer_id}", s.handleStoreSignal)
	s.mux.HandleFunc("GET /api/signal/{peer_id}/pending", s.handlePendingSignals)
	return s
}

func (s *Server) Addr() string {
	return s.addr
}

func chunkKey(messageID string, index int) string {
	return messageID + ":" + fmt.Sprintf("%d", index)
}

func (s *Server) handleStoreChunk(w http.ResponseWriter, r *http.Request) {
	var req struct {
		MessageID  string `json:"message_id"`
		ChunkIndex int    `json:"chunk_index"`
		Data       []byte `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.MessageID == "" || len(req.Data) == 0 {
		http.Error(w, "invalid chunk data", http.StatusBadRequest)
		return
	}
	sc := &StoredChunk{
		MessageID:  req.MessageID,
		ChunkIndex: req.ChunkIndex,
		Data:       req.Data,
		StoredAt:   time.Now(),
	}
	s.chunksMu.Lock()
	s.chunks[chunkKey(req.MessageID, req.ChunkIndex)] = sc
	s.chunksMu.Unlock()
	json.NewEncoder(w).Encode(map[string]string{"status": "stored"})
}

func (s *Server) handleGetChunk(w http.ResponseWriter, r *http.Request) {
	messageID := r.PathValue("message_id")
	indexStr := r.PathValue("index")
	var index int
	fmt.Sscanf(indexStr, "%d", &index)
	if messageID == "" {
		http.Error(w, "message_id required", http.StatusBadRequest)
		return
	}
	s.chunksMu.RLock()
	sc, ok := s.chunks[chunkKey(messageID, index)]
	s.chunksMu.RUnlock()
	if !ok {
		http.Error(w, "chunk not found", http.StatusNotFound)
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"message_id":   sc.MessageID,
		"chunk_index":  sc.ChunkIndex,
		"data":         sc.Data,
		"stored_at":    sc.StoredAt,
	})
}

func (s *Server) handleDeleteChunk(w http.ResponseWriter, r *http.Request) {
	messageID := r.PathValue("message_id")
	indexStr := r.PathValue("index")
	var index int
	fmt.Sscanf(indexStr, "%d", &index)
	s.chunksMu.Lock()
	delete(s.chunks, chunkKey(messageID, index))
	s.chunksMu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleStoreSignal(w http.ResponseWriter, r *http.Request) {
	peerID := r.PathValue("peer_id")
	var sig SignalMsg
	if err := json.NewDecoder(r.Body).Decode(&sig); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	s.signalsMu.Lock()
	s.signals[peerID] = append(s.signals[peerID], sig)
	s.signalsMu.Unlock()
	json.NewEncoder(w).Encode(map[string]string{"status": "stored"})
}

func (s *Server) handlePendingSignals(w http.ResponseWriter, r *http.Request) {
	peerID := r.PathValue("peer_id")
	s.signalsMu.Lock()
	sigs := s.signals[peerID]
	delete(s.signals, peerID)
	s.signalsMu.Unlock()
	if sigs == nil {
		sigs = []SignalMsg{}
	}
	json.NewEncoder(w).Encode(sigs)
}

func (s *Server) Start() error {
	s.srv = &http.Server{
		Addr:    s.addr,
		Handler: s.mux,
	}
	return s.srv.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.srv != nil {
		return s.srv.Shutdown(ctx)
	}
	return nil
}

func StoreChunk(ctx context.Context, addr string, messageID string, chunkIndex int, data []byte) error {
	req := map[string]interface{}{
		"message_id":   messageID,
		"chunk_index":  chunkIndex,
		"data":         data,
	}
	body, _ := json.Marshal(req)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/api/chunk/store", addr), bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("store chunk failed (status %d): %s", resp.StatusCode, string(b))
	}
	return nil
}

func GetChunk(ctx context.Context, addr string, messageID string, chunkIndex int) ([]byte, error) {
	url := fmt.Sprintf("http://%s/api/chunk/%s/%d", addr, messageID, chunkIndex)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get chunk failed (status %d): %s", resp.StatusCode, string(b))
	}
	var result struct {
		Data []byte `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result.Data, nil
}

func DeleteChunk(ctx context.Context, addr string, messageID string, chunkIndex int) error {
	url := fmt.Sprintf("http://%s/api/chunk/%s/%d", addr, messageID, chunkIndex)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete chunk failed (status %d): %s", resp.StatusCode, string(b))
	}
	return nil
}

func SendSignal(ctx context.Context, addr string, sig SignalMsg) error {
	body, _ := json.Marshal(sig)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/api/signal/%s", addr, sig.From), bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("send signal failed (status %d): %s", resp.StatusCode, string(b))
	}
	return nil
}

func PollSignals(ctx context.Context, addr string, peerID string) ([]SignalMsg, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/api/signal/%s/pending", addr, peerID), nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("poll signals failed (status %d): %s", resp.StatusCode, string(b))
	}
	var sigs []SignalMsg
	if err := json.NewDecoder(resp.Body).Decode(&sigs); err != nil {
		return nil, err
	}
	return sigs, nil
}
