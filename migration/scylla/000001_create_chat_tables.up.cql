CREATE TABLE IF NOT EXISTS chats_by_order (
    order_id TEXT PRIMARY KEY,
    buyer_id TEXT,
    seller_id TEXT,
    status TEXT,
    close_reason TEXT,
    last_message_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS chat_messages_by_order (
    order_id TEXT,
    created_at TIMESTAMP,
    message_id TEXT,
    sender_user_id TEXT,
    message_type TEXT,
    ciphertext TEXT,
    has_text BOOLEAN,
    deleted BOOLEAN,
    edited BOOLEAN,
    attachments_json TEXT,
    updated_at TIMESTAMP,
    PRIMARY KEY ((order_id), created_at, message_id)
) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC);

CREATE TABLE IF NOT EXISTS chat_messages_by_id (
    order_id TEXT,
    message_id TEXT,
    sender_user_id TEXT,
    message_type TEXT,
    ciphertext TEXT,
    has_text BOOLEAN,
    deleted BOOLEAN,
    edited BOOLEAN,
    attachments_json TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    PRIMARY KEY ((order_id), message_id)
);

ALTER TABLE chats_by_order WITH cdc = {'enabled': true, 'preimage': 'full', 'postimage': true};
ALTER TABLE chat_messages_by_order WITH cdc = {'enabled': true, 'preimage': 'full', 'postimage': true};
ALTER TABLE chat_messages_by_id WITH cdc = {'enabled': true, 'preimage': 'full', 'postimage': true};
