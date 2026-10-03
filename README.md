# OFM Chat Service

## Purpose

The Chat Service owns chat threads and chat messages between marketplace participants. It is the source of truth for chat persistence and publishes events used by realtime delivery and migration projections. Status: active.

## Boundaries and flow

Order-saga or public commands request chat creation through the service boundary. The service writes the chat or message and its outbox record in one transaction. Debezium captures the committed outbox change, Kafka carries the event, and realtime-service delivers relevant notifications through WebSocket. The client does not own background chat creation.

The service owns chat/message validation and persistence; it does not own order state, user identity, file metadata, or WebSocket connection management.

## Configuration

.env.example groups are DB_*, MIGRATIONS_*, NATS commands, Kafka/CDC, HTTP/gRPC listeners, Redis if enabled, and OpenTelemetry. Database values select chat storage; broker values select command subjects and consumers.

## Local development

    cp .env.example .env
    just run
    go test ./...

## Build and operations

Dockerfile builds ofm/chat-service:<tag> and Helm deploys the workload. Diagnose chat outbox state, Debezium logs, Kafka consumer lag, realtime logs, trace IDs, and projection audit records together.

