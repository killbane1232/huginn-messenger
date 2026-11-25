package messenger

// IsMuninnConnected reports the signaling connection, independently of cached
// peers, successful health probes, or connections to other Huginn clients.
func (m *Messenger) IsMuninnConnected() bool {
	return m.wsClient != nil && m.wsClient.IsConnected()
}
