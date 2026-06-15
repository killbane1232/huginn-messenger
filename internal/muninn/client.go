package muninn

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type PeerFlag string

const (
	PeerFlagThin      PeerFlag = "thin"
	PeerFlagThick     PeerFlag = "thick"
	PeerFlagVeryThick PeerFlag = "very_thick"
)

type QualityStats struct {
	ValidReports   int `json:"valid_reports"`
	InvalidReports int `json:"invalid_reports"`
}

type Signal struct {
	From string `json:"from"`
	Type string `json:"type"`
	Data string `json:"data"`
}

type Client struct {
	baseURL    string
	httpClient *http.Client
}

func (c *Client) BaseURL() string {
	return c.baseURL
}

func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

type Peer struct {
	ID            string            `json:"id"`
	Keys          []Key             `json:"keys"`
	Addresses     []string          `json:"addresses"`
	PublicKey     string            `json:"public_key,omitempty"`
	EncryptionKey string            `json:"encryption_key,omitempty"`
	SignatureKey  string            `json:"signature_key,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	LastSeen      time.Time         `json:"last_seen"`
	TTLSeconds    int               `json:"ttl_seconds"`
	QualityScore  int               `json:"quality_score"`
	Quality       QualityStats      `json:"quality"`
	PeerFlag      PeerFlag          `json:"peer_flag,omitempty"`
}

type Key struct {
	Login     string `json:"login"`
	Signature string `json:"signature"`
}

type RegisterRequest struct {
	ID            string            `json:"id"`
	Keys          []Key             `json:"keys"`
	Addresses     []string          `json:"addresses"`
	EncryptionKey string            `json:"encryption_key,omitempty"`
	SignatureKey  string            `json:"signature_key,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	TTLSeconds    int               `json:"ttl_seconds,omitempty"`
	PeerFlag      PeerFlag          `json:"peer_flag,omitempty"`
}

type RegisterChunkRequest struct {
	SenderID    string `json:"sender_id"`
	RecipientID string `json:"recipient_id"`
	Hash        string `json:"hash"`
	Signature   string `json:"signature"`
	PeerID      string `json:"peer_id"`
	Persist     bool   `json:"persist"`
}

type RegisterChunkBatchEntry struct {
	ChunkIndex  int    `json:"chunk_index"`
	SenderID    string `json:"sender_id"`
	RecipientID string `json:"recipient_id"`
	Hash        string `json:"hash"`
	Signature   string `json:"signature"`
	PeerID      string `json:"peer_id"`
	Persist     bool   `json:"persist"`
}

type RegisterChunkBatchRequest struct {
	Chunks []RegisterChunkBatchEntry `json:"chunks"`
}

type ChunkReportRequest struct {
	ReporterID string `json:"reporter_id"`
	FileID     string `json:"file_id"`
	ChunkIndex int    `json:"chunk_index"`
	Hash       string `json:"hash"`
	Signature  string `json:"signature"`
}

type ChunkRecord struct {
	FileID      string `json:"file_id"`
	ChunkIndex  int    `json:"chunk_index"`
	SenderID    string `json:"sender_id"`
	RecipientID string `json:"recipient_id"`
	Hash        string `json:"hash"`
	PeerID      string `json:"peer_id"`
	Persist     bool   `json:"persist"`
	Confirmed   bool   `json:"confirmed"`
}

type ChunkReportResult struct {
	Valid        bool   `json:"valid"`
	ExpectedHash string `json:"expected_hash"`
	ReportedHash string `json:"reported_hash"`
	Delta        int    `json:"delta"`
	Peer         Peer   `json:"peer"`
}

type ConfirmChunkRequest struct {
	RecipientID string `json:"recipient_id"`
	FileID      string `json:"file_id"`
	ChunkIndex  int    `json:"chunk_index"`
	Hash        string `json:"hash"`
	Signature   string `json:"signature"`
}

type ConfirmChunkBatchEntry struct {
	FileID     string `json:"file_id"`
	ChunkIndex int    `json:"chunk_index"`
	Hash       string `json:"hash"`
	Signature  string `json:"signature"`
}

type ConfirmChunkBatchRequest struct {
	RecipientID string                   `json:"recipient_id"`
	Chunks      []ConfirmChunkBatchEntry `json:"chunks"`
}

type ConfirmChunkResult struct {
	Valid         bool   `json:"valid"`
	ExpectedHash  string `json:"expected_hash"`
	ConfirmedHash string `json:"confirmed_hash"`
	Delta         int    `json:"delta"`
	Peer          Peer   `json:"peer"`
}

func (c *Client) SendSignal(ctx context.Context, peerID string, sig Signal) error {
	body, err := json.Marshal(sig)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	url := fmt.Sprintf("%s/api/v1/peers/%s/signals", c.baseURL, peerID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("send signal failed (status %d): %s", resp.StatusCode, string(b))
	}
	return nil
}

func (c *Client) PollSignals(ctx context.Context, peerID string) ([]Signal, error) {
	url := fmt.Sprintf("%s/api/v1/peers/%s/signals", c.baseURL, peerID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("request: %w", err)
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("poll signals failed (status %d): %s", resp.StatusCode, string(b))
	}
	var sigs []Signal
	if err := json.NewDecoder(resp.Body).Decode(&sigs); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return sigs, nil
}

func (c *Client) Register(ctx context.Context, req *RegisterRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/peers", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("register failed (status %d): %s", resp.StatusCode, string(b))
	}
	return nil
}

func (c *Client) List(ctx context.Context) ([]Peer, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/peers", nil)
	if err != nil {
		return nil, fmt.Errorf("request: %w", err)
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list failed (status %d): %s", resp.StatusCode, string(b))
	}
	var peers []Peer
	if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return peers, nil
}

func (c *Client) Get(ctx context.Context, id string) (*Peer, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/peers/"+id, nil)
	if err != nil {
		return nil, fmt.Errorf("request: %w", err)
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get failed (status %d): %s", resp.StatusCode, string(b))
	}
	var peer Peer
	if err := json.NewDecoder(resp.Body).Decode(&peer); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return &peer, nil
}

func (c *Client) GetBestPeers(ctx context.Context, n int) ([]Peer, error) {
	url := fmt.Sprintf("%s/api/v1/peers/best?n=%d", c.baseURL, n)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("request: %w", err)
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("best peers failed (status %d): %s", resp.StatusCode, string(b))
	}
	var peers []Peer
	if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return peers, nil
}

func (c *Client) GetBestThickPeers(ctx context.Context, n int) ([]Peer, error) {
	url := fmt.Sprintf("%s/api/v1/peers/best/thick?n=%d", c.baseURL, n)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("request: %w", err)
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("best thick peers failed (status %d): %s", resp.StatusCode, string(b))
	}
	var peers []Peer
	if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return peers, nil
}

func (c *Client) Heartbeat(ctx context.Context, id string, ttlSeconds int) error {
	body, _ := json.Marshal(map[string]int{"ttl_seconds": ttlSeconds})
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/peers/"+id+"/heartbeat", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("heartbeat failed (status %d): %s", resp.StatusCode, string(b))
	}
	return nil
}

func (c *Client) Delete(ctx context.Context, id string) error {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+"/api/v1/peers/"+id, nil)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete failed (status %d): %s", resp.StatusCode, string(b))
	}
	return nil
}

func (c *Client) RegisterChunks(ctx context.Context, fileID string, req RegisterChunkBatchRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	url := fmt.Sprintf("%s/api/v1/files/%s/chunks", c.baseURL, fileID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("register chunks failed (status %d): %s", resp.StatusCode, string(b))
	}
	return nil
}

func (c *Client) RegisterChunk(ctx context.Context, fileID string, chunkIndex int, req RegisterChunkRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	url := fmt.Sprintf("%s/api/v1/files/%s/chunks/%d", c.baseURL, fileID, chunkIndex)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("register chunk failed (status %d): %s", resp.StatusCode, string(b))
	}
	return nil
}

func (c *Client) ReportChunk(ctx context.Context, sourcePeerID string, req ChunkReportRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	url := fmt.Sprintf("%s/api/v1/peers/%s/chunk-reports", c.baseURL, sourcePeerID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("report chunk failed (status %d): %s", resp.StatusCode, string(b))
	}
	return nil
}

func (c *Client) GetChunksByRecipient(ctx context.Context, recipientID string) ([]ChunkRecord, error) {
	url := fmt.Sprintf("%s/api/v1/recipient/%s/chunks", c.baseURL, recipientID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("request: %w", err)
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get chunks failed (status %d): %s", resp.StatusCode, string(b))
	}
	var records []ChunkRecord
	if err := json.NewDecoder(resp.Body).Decode(&records); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return records, nil
}

func (c *Client) GetChunksByFileID(ctx context.Context, fileID string) ([]ChunkRecord, error) {
	url := fmt.Sprintf("%s/api/v1/files/%s/chunks", c.baseURL, fileID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("request: %w", err)
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get chunks by fileID failed (status %d): %s", resp.StatusCode, string(b))
	}
	var records []ChunkRecord
	if err := json.NewDecoder(resp.Body).Decode(&records); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return records, nil
}

func (c *Client) DeleteChunksByRecipient(ctx context.Context, recipientID string, fileID string) error {
	url := fmt.Sprintf("%s/api/v1/recipient/%s/chunks/%s", c.baseURL, recipientID, fileID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("do: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("delete chunks failed (status %d)", resp.StatusCode)
	}
	return nil
}

func (c *Client) GetByKey(ctx context.Context, login, signature string) (*Peer, error) {
	url := fmt.Sprintf("%s/api/v1/keys/%s?signature=%s", c.baseURL, login, signature)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("request: %w", err)
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get by key failed (status %d): %s", resp.StatusCode, string(b))
	}
	var peer Peer
	if err := json.NewDecoder(resp.Body).Decode(&peer); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return &peer, nil
}

func (c *Client) ConfirmChunk(ctx context.Context, req ConfirmChunkRequest) (*ConfirmChunkResult, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/chunks/confirm", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("confirm chunk failed (status %d): %s", resp.StatusCode, string(b))
	}
	var result ConfirmChunkResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return &result, nil
}

func (c *Client) ConfirmChunkBatch(ctx context.Context, req ConfirmChunkBatchRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/chunks/confirm-batch", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("confirm chunk batch failed (status %d): %s", resp.StatusCode, string(b))
	}
	return nil
}
