CREATE TABLE IF NOT EXISTS chats_by_order (
    order_id text PRIMARY KEY,
    buyer_id text NOT NULL,
    seller_id text NOT NULL,
    status text NOT NULL,
    close_reason text NOT NULL DEFAULT '',
    last_message_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS chat_messages (
    order_id text NOT NULL,
    message_id text NOT NULL,
    sender_user_id text NOT NULL,
    message_type text NOT NULL,
    ciphertext text NOT NULL,
    has_text boolean NOT NULL DEFAULT false,
    deleted boolean NOT NULL DEFAULT false,
    edited boolean NOT NULL DEFAULT false,
    attachments_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (order_id, message_id)
);

CREATE INDEX IF NOT EXISTS chat_messages_timeline_idx
    ON chat_messages (order_id, created_at DESC, message_id DESC);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id text PRIMARY KEY,
    event_type text NOT NULL,
    source_service text NOT NULL,
    aggregate_type text NOT NULL,
    aggregate_id text NOT NULL,
    aggregate_version bigint NOT NULL,
    processed_at timestamptz NOT NULL
);
