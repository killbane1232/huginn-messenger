CREATE TABLE IF NOT EXISTS file_downloads (
    file_id TEXT PRIMARY KEY,
    first_seen_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    completed_at INTEGER,
    stopped_at INTEGER,
    local_path TEXT NOT NULL DEFAULT ''
);
