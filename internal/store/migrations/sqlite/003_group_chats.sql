CREATE TABLE IF NOT EXISTS group_chats (
    uid TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    enc_private TEXT NOT NULL,
    enc_public TEXT NOT NULL,
    sign_private TEXT NOT NULL,
    sign_public TEXT NOT NULL,
    login_prefix TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
