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
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}

		if m.wsClient.IsConnected() {
			continue
		}

		log.Printf("[ws] attempting to reconnect to muninn...")
		if err := m.wsClient.Connect(m.ctx); err != nil {
			log.Printf("[ws] reconnect failed: %v", err)
			log.Printf("[ws] reconnect stack:\n%s", debug.Stack())
			continue
		}
		log.Printf("[ws] reconnected to muninn")
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
