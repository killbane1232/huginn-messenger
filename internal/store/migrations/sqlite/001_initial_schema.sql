CREATE TABLE IF NOT EXISTS chunks (
    file_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    data BLOB NOT NULL,
    PRIMARY KEY (file_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS stored_peers (
    login TEXT NOT NULL,
    peer_id TEXT NOT NULL,
    encryption_key TEXT NOT NULL DEFAULT '',
    signature_key TEXT NOT NULL DEFAULT '',
    last_seen DATETIME NOT NULL,
    PRIMARY KEY (login, peer_id)
);

CREATE TABLE IF NOT EXISTS messages (
    message_uid TEXT PRIMARY KEY,
    login TEXT NOT NULL,
    sender_login TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    data BLOB NOT NULL,
    created_at DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS pending_chunks (
    file_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    recipient_id TEXT NOT NULL,
    sender_id TEXT NOT NULL,
    data BLOB NOT NULL,
    hash TEXT NOT NULL,
    signature TEXT NOT NULL,
    created_at DATETIME NOT NULL,
    placed BOOLEAN NOT NULL DEFAULT 0,
    PRIMARY KEY (file_id, chunk_index)
);
