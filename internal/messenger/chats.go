package messenger

func (m *Messenger) DeleteChat(chatID string) error {
	return m.store.DeleteChat(chatID)
}

func (m *Messenger) GetHiddenChatIDs() ([]string, error) {
	return m.store.GetHiddenChatIDs()
}
