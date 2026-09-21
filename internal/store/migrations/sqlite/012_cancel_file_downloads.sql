ALTER TABLE file_downloads ADD COLUMN cancelled_at INTEGER;
ALTER TABLE file_downloads ADD COLUMN filename TEXT NOT NULL DEFAULT '';
ALTER TABLE file_downloads ADD COLUMN total_chunks INTEGER NOT NULL DEFAULT 0;

UPDATE file_downloads SET
    filename = COALESCE((
        SELECT json_extract(f.value, '$.filename')
        FROM messages m, json_each(CASE WHEN json_valid(m.data) THEN m.data ELSE '{}' END, '$.files') f
        WHERE json_extract(f.value, '$.file_id') = file_downloads.file_id LIMIT 1
    ), ''),
    total_chunks = COALESCE((
        SELECT CAST(json_extract(f.value, '$.total_chunks') AS INTEGER)
        FROM messages m, json_each(CASE WHEN json_valid(m.data) THEN m.data ELSE '{}' END, '$.files') f
        WHERE json_extract(f.value, '$.file_id') = file_downloads.file_id LIMIT 1
    ), 0);
