-- Deletion is local to this device and must survive peer refreshes and replay.
CREATE TABLE hidden_chats (chat_id TEXT PRIMARY KEY);
CREATE TABLE deleted_messages (message_uid TEXT PRIMARY KEY);

CREATE TRIGGER ignore_deleted_messages BEFORE INSERT ON messages
WHEN EXISTS (SELECT 1 FROM deleted_messages WHERE message_uid = NEW.message_uid)
BEGIN
    SELECT RAISE(IGNORE);
END;

-- Covers live delivery, the durable outbox, and device state synchronization.
CREATE TRIGGER reopen_chat_on_message AFTER INSERT ON messages
BEGIN
    DELETE FROM hidden_chats WHERE chat_id = NEW.chat_id;
END;
