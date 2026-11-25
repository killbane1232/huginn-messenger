package muninn

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	probeTimeout     = time.Second
	minimumResidence = time.Minute
	maxServers       = 17
)

type serverMeasurement struct {
	latency               time.Duration
	healthy               bool
	failures              int
	blockedUntil          time.Time
	websocketBlockedUntil time.Time
}

type responseLease struct {
	io.ReadCloser
	once    sync.Once
	release func()
	failed  func()
}

func (b *responseLease) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && err != io.EOF {
		b.failed()
	}
	return n, err
}
func (b *responseLease) Close() error { err := b.ReadCloser.Close(); b.once.Do(b.release); return err }

// do holds the endpoint lease until the response is consumed and closed. A
// switch drains all HTTP/WS operations before publishing another active server.
// Requests are never replayed automatically on a second server.
func (c *Client) do(req *http.Request) (*http.Response, error) {
	c.endpointMu.RLock()
	base, err := url.Parse(c.baseURL)
	if err != nil {
		c.endpointMu.RUnlock()
		return nil, err
	}
	request := req.Clone(req.Context())
	u := *req.URL
	u.Scheme = base.Scheme
	u.Host = base.Host
	request.URL = &u
	endpoint := c.baseURL
	response, err := c.httpClient.Do(request)
	if err != nil {
		c.observe(endpoint, req.Context(), true)
		c.endpointMu.RUnlock()
		return nil, err
	}
	c.observe(endpoint, req.Context(), response.StatusCode >= 500 || response.StatusCode == 429 || response.StatusCode >= 300 && response.StatusCode < 400)
	var failed sync.Once
	response.Body = &responseLease{ReadCloser: response.Body, release: c.endpointMu.RUnlock, failed: func() { failed.Do(func() { c.observe(endpoint, req.Context(), true) }) }}
	return response, nil
}

func canonicalServer(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || strings.Contains(raw, "#") || (u.Path != "" && u.Path != "/") {
		return "", fmt.Errorf("invalid Muninn origin")
	}
	u.Path = ""
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	port := u.Port()
	if port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return "", fmt.Errorf("invalid Muninn port")
		}
	}
	if u.Scheme == "http" && port == "80" || u.Scheme == "https" && port == "443" {
		port = ""
	}
	if port != "" {
		u.Host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		u.Host = "[" + host + "]"
	} else {
		u.Host = host
	}
	return u.String(), nil
}

// NormalizeServers validates the complete configured list before any network
// traffic or persistence, and rejects overflow instead of dropping servers.
func NormalizeServers(addresses []string) ([]string, error) {
	result := []string{}
	seen := map[string]bool{}
	for _, raw := range addresses {
		address, err := canonicalServer(raw)
		if err != nil {
			return nil, err
		}
		if !seen[address] {
			result = append(result, address)
			seen[address] = true
		}
	}
	if len(result) > maxServers {
		return nil, fmt.Errorf("at most %d Muninn servers are supported", maxServers)
	}
	return result, nil
}

// SetServers accepts only configured/remembered addresses; remote responses do
// not silently expand the list of servers eligible to receive client traffic.
func (c *Client) SetServers(addresses []string) {
	current := c.BaseURL()
	known := []string{current}
	seen := map[string]bool{current: true}
	for _, raw := range addresses {
		address, err := canonicalServer(raw)
		if err == nil && !seen[address] && len(known) < maxServers {
			known = append(known, address)
			seen[address] = true
		}
	}
	c.selectionMu.Lock()
	c.servers = known
	for _, address := range known {
		if c.measurements[address] == nil {
			c.measurements[address] = &serverMeasurement{}
		}
	}
	c.selectionMu.Unlock()
}
func (c *Client) Servers() []string {
	c.selectionMu.Lock()
	defer c.selectionMu.Unlock()
	return append([]string(nil), c.servers...)
}
func (c *Client) OtherServers() []string { return withoutServer(c.Servers(), c.BaseURL()) }
func withoutServer(addresses []string, selected string) []string {
	result := []string{}
	for _, address := range addresses {
		if address != selected && len(result) < 16 {
			result = append(result, address)
		}
	}
	return result
}
func (c *Client) observe(endpoint string, ctx context.Context, failed bool) {
	if ctx.Err() != nil {
		return
	}
	c.selectionMu.Lock()
	measurement := c.measurements[endpoint]
	if measurement == nil {
		measurement = &serverMeasurement{}
		c.measurements[endpoint] = measurement
	}
	if failed {
		measurement.failures++
	} else {
		measurement.failures = 0
	}
	c.selectionMu.Unlock()
	if failed {
		select {
		case c.failures <- struct{}{}:
		default:
		}
	}
}
func (c *Client) FailureEvents() <-chan struct{} { return c.failures }
func (c *Client) NeedsSelection() bool {
	current := c.BaseURL()
	c.selectionMu.Lock()
	defer c.selectionMu.Unlock()
	m := c.measurements[current]
	return m != nil && m.failures >= 2
}

// An HTTP health response must not hide a failed WebSocket handshake. Keep
// this separate from API failures, which successful HTTP traffic can reset.
func (c *Client) observeWebSocket(endpoint string, ctx context.Context, failed bool) {
	if ctx.Err() != nil {
		return
	}
	c.selectionMu.Lock()
	defer c.selectionMu.Unlock()
	m := c.measurements[endpoint]
	if m == nil {
		m = &serverMeasurement{}
		c.measurements[endpoint] = m
	}
	if failed {
		m.websocketBlockedUntil = time.Now().Add(time.Minute)
	} else {
		m.websocketBlockedUntil = time.Time{}
	}
}

// SelectFastest probes health only. Business requests always use one active
// endpoint; slower healthy alternatives are retained for a later failover.
func (c *Client) SelectFastest(ctx context.Context) (bool, error) {
	return c.selectFastest(ctx, false)
}

// SelectAfterDisconnect checks candidates without the residence/latency
// thresholds for a live connection. The current server can still win if its
// WebSocket has not failed to reconnect and it remains the fastest candidate.
func (c *Client) SelectAfterDisconnect(ctx context.Context) (bool, error) {
	return c.selectFastest(ctx, true)
}

func (c *Client) selectFastest(ctx context.Context, disconnected bool) (bool, error) {
	c.probeMu.Lock()
	defer c.probeMu.Unlock()
	addresses := c.Servers()
	if len(addresses) < 2 {
		return false, nil
	}
	probeCtx, cancel := context.WithTimeout(ctx, 2500*time.Millisecond)
	defer cancel()
	type result struct {
		address string
		latency time.Duration
		err     error
	}
	results := make(chan result, len(addresses))
	for _, address := range addresses {
		go func(address string) {
			latency, err := c.probe(probeCtx, address)
			results <- result{address, latency, err}
		}(address)
	}
	observations := map[string]result{}
	for range addresses {
		select {
		case result := <-results:
			observations[result.address] = result
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	current := c.BaseURL()
	c.selectionMu.Lock()
	now := time.Now()
	best := ""
	var bestLatency time.Duration
	for _, address := range addresses {
		observation := observations[address]
		m := c.measurements[address]
		m.healthy = observation.err == nil
		if !m.healthy || now.Before(m.blockedUntil) || now.Before(m.websocketBlockedUntil) || address == current && m.failures >= 2 {
			continue
		}
		if m.latency == 0 {
			m.latency = observation.latency
		} else {
			m.latency = (m.latency*7 + observation.latency*3) / 10
		}
		if best == "" || m.latency < bestLatency {
			best = address
			bestLatency = m.latency
		}
	}
	old := c.measurements[current]
	initial := c.lastSelection.IsZero()
	c.lastSelection = now
	if best == "" || best == current {
		c.selectionMu.Unlock()
		return false, nil
	}
	failed := disconnected || !old.healthy || old.failures >= 2 || now.Before(old.websocketBlockedUntil)
	if !failed && !initial && (now.Sub(c.lastSwitch) < minimumResidence || old.latency-bestLatency < 10*time.Millisecond || bestLatency*5 > old.latency*4) {
		c.selectionMu.Unlock()
		return false, nil
	}
	c.selectionMu.Unlock()
	if err := c.switchServer(ctx, best); err != nil {
		c.selectionMu.Lock()
		c.measurements[best].blockedUntil = time.Now().Add(time.Minute)
		c.selectionMu.Unlock()
		return false, err
	}
	return true, nil
}

func (c *Client) probe(ctx context.Context, address string) (time.Duration, error) {
	var total time.Duration
	for i := 0; i < 2; i++ {
		attempt, stop := context.WithTimeout(ctx, probeTimeout)
		req, err := http.NewRequestWithContext(attempt, http.MethodGet, address+"/health", nil)
		if err != nil {
			stop()
			return 0, err
		}
		start := time.Now()
		resp, err := c.httpClient.Do(req)
		if err != nil {
			stop()
			return 0, err
		}
		var health struct {
			Status string `json:"status"`
		}
		err = json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&health)
		resp.Body.Close()
		elapsed := time.Since(start)
		stop()
		if err != nil || resp.StatusCode != 200 || health.Status != "ok" {
			return 0, fmt.Errorf("Muninn health check failed")
		}
		total += elapsed
	}
	return total / 2, nil
}

func (c *Client) rememberRegistration(req *RegisterRequest) {
	copy := *req
	copy.MuninnServers = append([]string(nil), req.MuninnServers...)
	if req.Fake != nil {
		fake := *req.Fake
		copy.Fake = &fake
	}
	c.registrationMu.Lock()
	c.registrations[req.ID] = copy
	c.registrationMu.Unlock()
}

func (c *Client) switchServer(ctx context.Context, address string) error {
	c.endpointMu.Lock()
	defer c.endpointMu.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if address == c.baseURL {
		return nil
	}
	for ws := range c.websockets {
		ws.disconnect()
	}
	// Pinned clients bypass this client's lease while the transition is exclusive.
	next := NewClient(address)
	next.httpClient = c.httpClient
	c.registrationMu.Lock()
	registrations := make([]RegisterRequest, 0, len(c.registrations))
	for _, req := range c.registrations {
		registrations = append(registrations, req)
	}
	c.registrationMu.Unlock()
	sort.Slice(registrations, func(i, j int) bool { return registrations[i].ID < registrations[j].ID })
	servers := c.Servers()
	transition, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	for _, req := range registrations {
		req.MuninnServers = withoutServer(servers, address)
		if err := next.Register(transition, &req); err != nil {
			// Keep the old registration intact. Prepared records expire by TTL;
			// a failed switch never removes data from either server.
			return fmt.Errorf("prepare Muninn %s: %w", address, err)
		}
	}
	c.baseURL = address
	c.selectionMu.Lock()
	c.lastSwitch = time.Now()
	c.measurements[address].failures = 0
	c.selectionMu.Unlock()
	return nil
}

func (c *Client) bind(ws *WSClient) {
	c.endpointMu.Lock()
	c.websockets[ws] = struct{}{}
	c.endpointMu.Unlock()
}
