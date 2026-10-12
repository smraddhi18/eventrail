import logging
from datetime import timedelta

from django.conf import settings
from django.db import transaction
from django.db.models import Exists, OuterRef, Q
from django.utils import timezone

from .kafka_io import envelope, kafka_key, publish_confirmed
from .models import Event, OutboxMessage, ReplayJob


logger = logging.getLogger(__name__)
LEASE_SECONDS = 45
MAX_ATTEMPTS = 12


def claim_outbox() -> OutboxMessage | None:
    now = timezone.now()
    eligible = Q(status="pending", available_at__lte=now) | Q(status="publishing", lease_until__lte=now)
    older_same_key = (
        OutboxMessage.objects.filter(
            event__tenant_id=OuterRef("event__tenant_id"),
            event__partition_key=OuterRef("event__partition_key"),
            event_id__lt=OuterRef("event_id"),
        )
        .exclude(status="published")
    )
    with transaction.atomic():
        row = (
            OutboxMessage.objects.select_for_update(skip_locked=True)
            .select_related("event")
            .filter(eligible)
            .annotate(blocked=Exists(older_same_key))
            .filter(blocked=False)
            .order_by("event_id")
            .first()
        )
        if row is None:
            return None
        row.status = "publishing"
        row.lease_until = now + timedelta(seconds=LEASE_SECONDS)
        row.attempts += 1
        row.save(update_fields=["status", "lease_until", "attempts"])
        return row


def publish_outbox_once(producer) -> bool:
    row = claim_outbox()
    if row is None:
        return False
    try:
        publish_confirmed(producer, settings.KAFKA_EVENTS_TOPIC, kafka_key(row.event), envelope(row.event))
    except Exception as exc:
        delay = min(2 ** min(row.attempts, 8), 300)
        row.status = "dead" if row.attempts >= MAX_ATTEMPTS else "pending"
        row.available_at = timezone.now() + timedelta(seconds=delay)
        row.lease_until = None
        row.last_error = str(exc)[:300]
        row.save(update_fields=["status", "available_at", "lease_until", "last_error"])
        logger.exception("Outbox delivery failed for event %s", row.event.event_id)
        return True
    row.status = "published"
    row.published_at = timezone.now()
    row.lease_until = None
    row.last_error = ""
    row.save(update_fields=["status", "published_at", "lease_until", "last_error"])
    return True


def publish_replay_once(producer) -> bool:
    job = ReplayJob.objects.filter(status__in=["queued", "running"]).order_by("created_at").first()
    if job is None:
        return False
    if job.status == "queued":
        job.status = "running"
        job.save(update_fields=["status"])
    events = Event.objects.filter(
        tenant_id=job.tenant_id,
        id__gt=job.cursor_event_id,
        id__lte=job.upper_event_id,
    )
    if job.from_time:
        events = events.filter(accepted_at__gte=job.from_time)
    if job.to_time:
        events = events.filter(accepted_at__lt=job.to_time)
    event = events.order_by("id").first()
    if event is None:
        job.status = "completed"
        job.finished_at = timezone.now()
        job.save(update_fields=["status", "finished_at"])
        return True
    publish_confirmed(
        producer,
        settings.KAFKA_EVENTS_TOPIC,
        kafka_key(event),
        envelope(event, job.projection_version, str(job.id)),
    )
    job.cursor_event_id = event.id
    job.published_count += 1
    job.save(update_fields=["cursor_event_id", "published_count"])
    return True
