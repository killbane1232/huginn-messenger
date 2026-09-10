ALTER TABLE pending_chunks ADD COLUMN persist BOOLEAN NOT NULL DEFAULT 0;
-- Previous versions marked placement on socket send, without a storage ACK.
UPDATE pending_chunks SET placed = 0;
-- Revisit retained manifests once; older cursors could skip undecrypted messages.
DELETE FROM last_check;
