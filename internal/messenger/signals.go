package messenger

import (
	"encoding/json"
	"log"
	"time"

	"runtime/debug"

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
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		if !m.wsClient.IsConnected() {
			log.Printf("[ws] attempting to connect to muninn...")
			if err := m.wsClient.Connect(m.ctx); err != nil {
				if m.ctx.Err() != nil {
					return
				}
				log.Printf("[ws] connect failed: %v", err)
				log.Printf("[ws] connect stack:\n%s", debug.Stack())
			} else {
				log.Printf("[ws] connected to muninn")
			}
		}

		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
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
