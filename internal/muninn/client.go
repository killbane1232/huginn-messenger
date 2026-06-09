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

type Client struct {
	baseURL    string
	httpClient *http.Client
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
}

type RegisterChunkRequest struct {
	SenderID    string `json:"sender_id"`
	RecipientID string `json:"recipient_id"`
	Hash        string `json:"hash"`
	Signature   string `json:"signature"`
	PeerID      string `json:"peer_id"`
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
