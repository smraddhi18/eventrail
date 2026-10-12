import json
import time
from typing import Any

from confluent_kafka import Producer
from django.conf import settings


def new_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "eventrail-publisher",
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": 15000,
        }
    )


def publish_confirmed(producer: Producer, topic: str, key: str, value: dict[str, Any]) -> None:
    deliveries: list[Exception | None] = []

    def delivered(error, message) -> None:
        deliveries.append(error)

    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        on_delivery=delivered,
    )
    deadline = time.monotonic() + 18
    while not deliveries and time.monotonic() < deadline:
        producer.poll(0.5)
    if not deliveries:
        raise TimeoutError("Kafka delivery acknowledgement timed out")
    if deliveries[0] is not None:
        raise RuntimeError(f"Kafka delivery failed: {deliveries[0]}")


def envelope(event, projection_version: str = "live", replay_id: str | None = None) -> dict[str, Any]:
    return {
        "event_id": str(event.event_id),
        "tenant_id": str(event.tenant_id),
        "event_type": event.event_type,
        "schema_version": event.schema_version,
        "partition_key": event.partition_key,
        "payload": event.payload,
        "occurred_at": event.occurred_at.isoformat(),
        "projection_version": projection_version,
        "replay_id": replay_id,
    }


def kafka_key(event) -> str:
    return f"{event.tenant_id}:{event.partition_key}"
