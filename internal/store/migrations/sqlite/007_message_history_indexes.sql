CREATE INDEX IF NOT EXISTS idx_messages_chat_id_created_at
ON messages (chat_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_messages_login_created_at
ON messages (login, created_at DESC);
