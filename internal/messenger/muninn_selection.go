package messenger

import "log"

// Only the reconnect loop calls this, while the WebSocket is disconnected.
// Health probes are the only requests sent to alternatives.
func (m *Messenger) selectMuninnAfterDisconnect() {
	changed, err := m.muninnClient.SelectAfterDisconnect(m.ctx)
	if err != nil {
		if m.ctx.Err() == nil {
			log.Printf("select Muninn: %v", err)
		}
		return
	}
	if !changed {
		return
	}
	address := m.muninnClient.BaseURL()
	m.configMu.Lock()
	cfg := copyConfig(m.appConfig)
	if !selectConfiguredMuninn(cfg, address) {
		// A manual replacement has been saved and is awaiting recreation.
		m.configMu.Unlock()
		return
	}
	m.appConfig = cfg
	m.applyMuninnICE(cfg, address)
	if err := m.store.SaveAppConfig(cfg); err != nil {
		log.Printf("save selected Muninn: %v", err)
	}
	m.configMu.Unlock()
	log.Printf("selected Muninn: %s", address)
}
