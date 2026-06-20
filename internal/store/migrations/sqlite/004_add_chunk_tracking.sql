CREATE TABLE IF NOT EXISTS last_check (
    recipient_id TEXT PRIMARY KEY,
    last_check INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS failed_chunks (
    file_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    recipient_id TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL DEFAULT 0,
    ttl_seconds INTEGER NOT NULL DEFAULT 604800,
    PRIMARY KEY (file_id, chunk_index)
);
