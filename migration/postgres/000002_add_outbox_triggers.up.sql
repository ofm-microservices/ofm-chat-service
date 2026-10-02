CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type text NOT NULL,
    aggregate_id text NOT NULL,
    event_type text NOT NULL,
    operation text NOT NULL,
    schema_version integer NOT NULL DEFAULT 1,
    payload jsonb NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS outbox_events_occurred_at_idx
    ON outbox_events (occurred_at);

CREATE OR REPLACE FUNCTION emit_chat_outbox_event()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, operation, payload)
        VALUES ('chat', OLD.order_id, 'chat-service.chat.changed', 'deactivated', to_jsonb(OLD));
        RETURN OLD;
    END IF;
    INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, operation, payload)
    VALUES (
        'chat',
        NEW.order_id,
        'chat-service.chat.changed',
        CASE TG_OP WHEN 'INSERT' THEN 'created' ELSE 'updated' END,
        to_jsonb(NEW)
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS chats_by_order_outbox_event ON chats_by_order;
CREATE TRIGGER chats_by_order_outbox_event
AFTER INSERT OR UPDATE OR DELETE ON chats_by_order
FOR EACH ROW EXECUTE FUNCTION emit_chat_outbox_event();

DROP TRIGGER IF EXISTS chat_messages_outbox_event ON chat_messages;
CREATE TRIGGER chat_messages_outbox_event
AFTER INSERT OR UPDATE OR DELETE ON chat_messages
FOR EACH ROW EXECUTE FUNCTION emit_chat_outbox_event();

INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, operation, payload, occurred_at)
SELECT 'chat', order_id, 'chat-service.chat.changed', 'created', to_jsonb(chats_by_order), created_at
FROM chats_by_order;

INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, operation, payload, occurred_at)
SELECT 'chat', order_id, 'chat-service.chat.changed', 'created', to_jsonb(chat_messages), created_at
FROM chat_messages;
