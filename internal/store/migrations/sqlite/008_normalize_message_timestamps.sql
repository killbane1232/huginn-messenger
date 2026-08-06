WITH message_timestamps AS (
    SELECT
        message_uid,
        json_extract(CAST(data AS TEXT), '$.timestamp') AS timestamp
    FROM messages
    WHERE typeof(created_at) != 'integer'
      AND json_valid(CAST(data AS TEXT))
      AND json_type(CAST(data AS TEXT), '$.timestamp') = 'text'
), normalized AS (
    SELECT
        message_uid,
        timestamp,
        CASE
            WHEN instr(timestamp, '.') = 0 THEN ''
            WHEN substr(timestamp, -1) = 'Z' THEN substr(
                timestamp,
                instr(timestamp, '.') + 1,
                length(timestamp) - instr(timestamp, '.') - 1
            )
            ELSE substr(
                timestamp,
                instr(timestamp, '.') + 1,
                length(timestamp) - instr(timestamp, '.') - 6
            )
        END AS fractional_seconds
    FROM message_timestamps
    WHERE unixepoch(timestamp) IS NOT NULL
)
UPDATE messages
SET created_at = CAST(
    unixepoch(normalized.timestamp) * 1000000
    + CAST(substr(normalized.fractional_seconds || '000000', 1, 6) AS INTEGER)
    AS INTEGER
)
FROM normalized
WHERE messages.message_uid = normalized.message_uid;
