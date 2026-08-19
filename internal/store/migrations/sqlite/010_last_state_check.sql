CREATE TABLE IF NOT EXISTS last_state_check (
    peer_id TEXT PRIMARY KEY,
    last_check_datetime INTEGER NOT NULL
);

ALTER TABLE messages ADD COLUMN state_updated_at INTEGER NOT NULL DEFAULT 0;

UPDATE messages
SET state_updated_at = CAST(created_at AS INTEGER)
WHERE state_updated_at = 0;

CREATE INDEX IF NOT EXISTS idx_messages_state_updated_at
ON messages (state_updated_at);
