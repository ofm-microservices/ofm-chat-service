CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT,
    source_service TEXT,
    aggregate_type TEXT,
    aggregate_id TEXT,
    aggregate_version BIGINT,
    processed_at TIMESTAMP
);
