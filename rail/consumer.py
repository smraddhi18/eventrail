import base64
import json
import logging
import re
import time
import uuid

from confluent_kafka import Consumer, KafkaException, TopicPartition
from django.conf import settings
from django.db import IntegrityError, close_old_connections, transaction
from django.db.models import F

from .errors import PermanentEventError
from .kafka_io import new_producer, publish_confirmed
from .models import DailyEventCount, DeadLetter, Event, ProcessedEvent


logger = logging.getLogger(__name__)
VERSION = re.compile(r"^[a-z][a-z0-9-]{0,63}$")


def decode_record(raw: bytes | None) -> tuple[Event, str]:
    try:
        body = json.loads(raw or b"")
        event_id = uuid.UUID(body["event_id"])
        tenant_id = uuid.UUID(body["tenant_id"])
        version = body["projection_version"]
        if not isinstance(version, str) or VERSION.fullmatch(version) is None:
            raise ValueError("Invalid projection version")
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        raise PermanentEventError("malformed_record", "Invalid event envelope") from exc
    event = Event.objects.filter(event_id=event_id, tenant_id=tenant_id).first()
    if event is None:
        raise PermanentEventError("unknown_event", "Event is not in the durable event store")
    if version != "live":
        from .models import ReplayJob

        if not ReplayJob.objects.filter(tenant_id=tenant_id, projection_version=version).exists():
            raise PermanentEventError("unknown_projection", "Projection version does not exist")
    return event, version


def apply_projection(event: Event, version: str) -> bool:
    try:
        with transaction.atomic():
            ProcessedEvent.objects.create(tenant_id=event.tenant_id, event_id=event.event_id, projection_version=version)
            day = event.occurred_at.date()
            counter, _ = DailyEventCount.objects.get_or_create(
                tenant_id=event.tenant_id,
                projection_version=version,
                event_type=event.event_type,
                day=day,
            )
            DailyEventCount.objects.filter(pk=counter.pk).update(count=F("count") + 1)
        return True
    except IntegrityError:
        if ProcessedEvent.objects.filter(
            tenant_id=event.tenant_id, event_id=event.event_id, projection_version=version
        ).exists():
            return False
        raise


def dead_letter(record, error: PermanentEventError, producer) -> None:
    payload = base64.b64encode(record.value() or b"").decode("ascii")
    data = {
        "source_topic": record.topic(),
        "source_partition": record.partition(),
        "source_offset": record.offset(),
        "reason": error.code,
        "payload_base64": payload,
    }
    publish_confirmed(producer, settings.KAFKA_DLQ_TOPIC, f"{record.partition()}:{record.offset()}", data)
    DeadLetter.objects.get_or_create(
        topic=record.topic(),
        partition=record.partition(),
        offset=record.offset(),
        defaults={"reason": error.code, "payload": payload},
    )


def run_consumer() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": settings.KAFKA_CONSUMER_GROUP,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "client.id": "eventrail-projector",
        }
    )
    producer = new_producer()
    consumer.subscribe([settings.KAFKA_EVENTS_TOPIC])
    try:
        while True:
            record = consumer.poll(1.0)
            if record is None:
                continue
            close_old_connections()
            if record.error():
                raise KafkaException(record.error())
            try:
                event, version = decode_record(record.value())
                apply_projection(event, version)
            except PermanentEventError as exc:
                dead_letter(record, exc, producer)
                logger.warning("Dead-lettered partition=%s offset=%s reason=%s", record.partition(), record.offset(), exc.code)
            except Exception:
                logger.exception("Projection failed; record will be retried")
                consumer.seek(TopicPartition(record.topic(), record.partition(), record.offset()))
                time.sleep(2)
                continue
            consumer.commit(message=record, asynchronous=False)
    finally:
        consumer.close()
