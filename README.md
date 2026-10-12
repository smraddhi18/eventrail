# EventRail

EventRail accepts tenant events over HTTP, durably records them in PostgreSQL, publishes them to Kafka, and maintains queryable daily counts. Its replay API rebuilds a separate version of the projection from stored events. This is a backend engineering portfolio project focused on delivery guarantees and recovery behavior.

## What is implemented

- Tenant API keys stored as SHA-256 hashes, with tenant-scoped event, replay, and projection queries.
- Idempotent ingestion. A repeated `Idempotency-Key` and identical body returns the original event; a changed body returns HTTP 409.
- A transactional outbox: event and publish intent commit together through the Django ORM.
- Kafka publishing with broker acknowledgement, bounded backoff, a claim lease, and a manually recoverable terminal state.
- A manually committed Kafka consumer with a PostgreSQL uniqueness marker and atomic projection update.
- Dead-letter publication for malformed or unknown records.
- Replay jobs with a fixed upper event ID and progress cursor. Each job targets a new projection version, preserving the live projection.
- Keyset pagination for projection results.

## Data flow

```text
tenant -> POST /v1/events -> PostgreSQL Event + OutboxMessage (one transaction)
                               |
                               v
                         publisher -> Kafka eventrail.events.v1
                                         |
                                         v
                              consumer -> ProcessedEvent + DailyEventCount

tenant -> POST /v1/replays -> ReplayJob -> publisher -> same Kafka topic
                                                  -> new projection version
```

This design provides **at-least-once delivery with idempotent projection writes**. A crash between a Kafka acknowledgement and the outbox or replay cursor update can publish an event twice. The consumer's `(tenant, event_id, projection_version)` uniqueness marker prevents duplicate counts. The Kafka offset is committed only after the database transaction succeeds. It does not claim end-to-end exactly-once delivery.

## Run locally

Requires Docker Engine and Docker Compose. The Compose stack is a **single-node local demo** with plaintext Kafka and development credentials.

```bash
docker compose up --build -d
docker compose exec web python manage.py provision_tenant demo
```

Save the one-time `api_key` shown by the second command. The command stores only its SHA-256 digest. For the examples below, put it in your shell as `API_KEY`.

```bash
curl -sS -X POST http://localhost:8000/v1/events \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -H "Idempotency-Key: order-42-created" \
  -d '{"event_type":"order.created","schema_version":1,"partition_key":"order-42","occurred_at":"2026-10-12T10:00:00Z","payload":{"order_id":"order-42","amount_cents":2500}}'

curl -sS -H "X-API-Key: $API_KEY" http://localhost:8000/v1/projections/live

curl -sS -X POST http://localhost:8000/v1/replays \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d '{"projection_version":"rebuild-1"}'

curl -sS -H "X-API-Key: $API_KEY" http://localhost:8000/v1/projections/rebuild-1
```

Use the `replay_id` from the replay response with `GET /v1/replays/{replay_id}`. `published_count` tracks messages acknowledged by Kafka. `processed_count` tracks events applied to that version. A job's `completed` status means publishing finished; projection consumption may still be catching up.

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Process health |
| `POST` | `/v1/events` | Accept an event (requires `Idempotency-Key`) |
| `POST` | `/v1/replays` | Start a versioned replay |
| `GET` | `/v1/replays/{uuid}` | Inspect replay publish and process progress |
| `GET` | `/v1/projections/{version}` | Read daily event counts, 500 rows per page |

All `/v1` requests require `X-API-Key`. Replay accepts optional `from_time` and `to_time` ISO 8601 timestamps with timezone; these filter by **accepted time**, with an inclusive lower and exclusive upper bound. The snapshot upper event ID is fixed when the job is created. Projection pagination returns `next_cursor`; pass it as the `after` query parameter until null. The `event_type` format is dot-separated lowercase names such as `order.created`.

## Recovery and ordering

The outbox claim uses row locking and a lease. The publisher waits for Kafka acknowledgement before marking a row published. Retries use exponential backoff. After 12 failed attempts, the row becomes `dead` and later events with the same tenant and partition key remain blocked to preserve order. An operator can inspect the reason and run:

```bash
docker compose exec web python manage.py retry_outbox EVENT_UUID
```

Kafka uses `tenant_id:partition_key` as the message key, so events sharing a key use one partition. The outbox also blocks later events for that key while an earlier one is unresolved. This provides per-key order for live events with the single publisher in the Compose stack. There is no global ordering guarantee. Replay versions are consumed separately from live counts.

Malformed messages are broker-acknowledged into `eventrail.dead_letters.v1` before the source offset is committed. A database copy is also retained in `DeadLetter`. If the process fails between these operations, the dead-letter topic can contain a duplicate; its source topic, partition, and offset identify the original record.

## Operational scope

The stack demonstrates a single-node deployment and one publisher process. Before deploying for real tenants, add TLS and Kafka ACLs, secret management, a replicated broker cluster, observability and alerting, rate limits, payload schema governance, and an operational dead-letter recovery workflow. The health endpoint currently checks process availability, not downstream dependencies.

Source references: [Apache Kafka Docker example](https://github.com/apache/kafka/blob/trunk/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml), [Confluent Python client](https://docs.confluent.io/kafka-clients/python/current/overview.html).
