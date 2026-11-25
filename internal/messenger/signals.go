package messenger

import (
	"encoding/json"
	"log"
	"time"

	"github.com/killbane1232/huginn-messenger/internal/muninn"
	pion "github.com/pion/webrtc/v4"
)

func (m *Messenger) processPendingSignals() {
	if m.pollSignal {
		sigs, err := m.muninnClient.PollSignals(m.ctx, m.ID)
		if err != nil {
			return
		}
		for _, sig := range sigs {
			m.handleSignal(sig)
		}
	}

	for {
		select {
		case sig := <-m.signalChan:
			m.handleSignal(sig)
		default:
			return
		}
	}
}

func (m *Messenger) handleSignal(sig muninn.Signal) {
	switch sig.Type {
	case "offer":
		m.mu.RLock()
		_, dialing := m.peersConnecting[sig.From]
		m.mu.RUnlock()
		if m.rtcManager.IsConnected(sig.From) {
			log.Printf("ignoring duplicate offer from %s: data channel already open", sig.From)
			return
		}
		if (dialing || m.rtcManager.HasConnection(sig.From)) && m.ID < sig.From {
			log.Printf("ignoring colliding offer from %s: keeping local offer", sig.From)
			return
		}
		var offer pion.SessionDescription
		if err := json.Unmarshal([]byte(sig.Data), &offer); err != nil {
			return
		}
		answer, err := m.rtcManager.HandleOffer(sig.From, offer)
		if err != nil {
			log.Printf("handle offer from %s: %v", sig.From, err)
			return
		}
		ansData, _ := json.Marshal(answer)

		if m.wsClient != nil && m.wsClient.IsConnected() {
			if err := m.wsClient.RelaySignal(m.ctx, sig.From, "answer", string(ansData)); err != nil {
				log.Printf("rtc relay answer to %s: %v, fallback to http", sig.From, err)
				m.muninnClient.SendSignal(m.ctx, sig.From, muninn.Signal{From: m.ID, Type: "answer", Data: string(ansData)})
			}
		} else {
			m.muninnClient.SendSignal(m.ctx, sig.From, muninn.Signal{From: m.ID, Type: "answer", Data: string(ansData)})
		}

	case "answer":
		var answer pion.SessionDescription
		if err := json.Unmarshal([]byte(sig.Data), &answer); err != nil {
			return
		}
		if err := m.rtcManager.SetRemoteDescription(sig.From, answer); err != nil {
			log.Printf("set remote desc from %s: %v", sig.From, err)
		}
	}
}

func (m *Messenger) rtcReconnectLoop() {
	const retryDelay = 5 * time.Second
	retry := time.NewTimer(0)
	defer retry.Stop()
	needsSelection := false // Startup already selected a server.
	var lastCheck time.Time
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.wsDisconnected:
			needsSelection = true
		case <-retry.C:
		}
		if m.ctx.Err() != nil {
			return
		}
		if m.wsClient.IsConnected() {
			continue // Ignore a delayed disconnect notification after reconnecting.
		}
		retry.Stop()
		if needsSelection {
			// Bound retries during an outage or rapidly flapping connection.
			if delay := time.Until(lastCheck.Add(retryDelay)); delay > 0 {
				retry.Reset(delay)
				continue
			}
			lastCheck = time.Now()
			m.selectMuninnAfterDisconnect()
		}
		if m.ctx.Err() != nil {
			return
		}
		log.Printf("[ws] attempting to connect to muninn...")
		if err := m.wsClient.Connect(m.ctx); err != nil {
			if m.ctx.Err() != nil {
				return
			}
			log.Printf("[ws] connect failed: %v", err)
			needsSelection = true
			retry.Reset(retryDelay)
		} else {
			needsSelection = false
			log.Printf("[ws] connected to muninn")
		}
	}
}

func (m *Messenger) signalPollLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.processPendingSignals()
		case sig := <-m.signalChan:
			m.handleSignal(sig)
		case <-m.ctx.Done():
			return
		}
	}
}
