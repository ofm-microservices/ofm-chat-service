DROP TRIGGER IF EXISTS chats_by_order_outbox_event ON chats_by_order;
DROP TRIGGER IF EXISTS chat_messages_outbox_event ON chat_messages;
DROP FUNCTION IF EXISTS emit_chat_outbox_event();
DROP TABLE IF EXISTS outbox_events;
