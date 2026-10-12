import uuid

from django.db import models
from django.db.models import Q


class Tenant(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=120, unique=True)
    api_key_hash = models.CharField(max_length=64, unique=True)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)


class Event(models.Model):
    id = models.BigAutoField(primary_key=True)
    event_id = models.UUIDField(default=uuid.uuid4, unique=True, editable=False)
    tenant = models.ForeignKey(Tenant, on_delete=models.PROTECT)
    idempotency_key = models.CharField(max_length=128)
    request_hash = models.CharField(max_length=64)
    event_type = models.CharField(max_length=100)
    schema_version = models.PositiveSmallIntegerField(default=1)
    partition_key = models.CharField(max_length=128)
    payload = models.JSONField()
    occurred_at = models.DateTimeField()
    accepted_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["tenant", "idempotency_key"], name="event_tenant_idem_unique")
        ]
        indexes = [
            models.Index(fields=["tenant", "id", "accepted_at"], name="event_replay_scan_idx"),
            models.Index(fields=["tenant", "partition_key", "id"], name="event_partition_order_idx"),
        ]


class OutboxMessage(models.Model):
    event = models.OneToOneField(Event, on_delete=models.CASCADE)
    status = models.CharField(max_length=16, default="pending")
    attempts = models.PositiveIntegerField(default=0)
    available_at = models.DateTimeField(auto_now_add=True)
    lease_until = models.DateTimeField(null=True, blank=True)
    published_at = models.DateTimeField(null=True, blank=True)
    last_error = models.CharField(max_length=300, blank=True)

    class Meta:
        indexes = [models.Index(fields=["status", "available_at"], name="outbox_claim_idx")]


class ReplayJob(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey(Tenant, on_delete=models.PROTECT)
    projection_version = models.SlugField(max_length=64)
    from_time = models.DateTimeField(null=True, blank=True)
    to_time = models.DateTimeField(null=True, blank=True)
    upper_event_id = models.BigIntegerField(default=0)
    cursor_event_id = models.BigIntegerField(default=0)
    published_count = models.PositiveIntegerField(default=0)
    status = models.CharField(max_length=16, default="queued")
    created_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["tenant", "projection_version"], name="replay_version_unique"),
            models.CheckConstraint(condition=~Q(projection_version="live"), name="replay_not_live"),
        ]


class ProcessedEvent(models.Model):
    tenant = models.ForeignKey(Tenant, on_delete=models.PROTECT)
    event_id = models.UUIDField()
    projection_version = models.SlugField(max_length=64)
    processed_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["tenant", "event_id", "projection_version"], name="processed_once_unique")
        ]


class DailyEventCount(models.Model):
    tenant = models.ForeignKey(Tenant, on_delete=models.PROTECT)
    projection_version = models.SlugField(max_length=64)
    event_type = models.CharField(max_length=100)
    day = models.DateField()
    count = models.PositiveBigIntegerField(default=0)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["tenant", "projection_version", "event_type", "day"], name="daily_count_unique"
            )
        ]


class DeadLetter(models.Model):
    topic = models.CharField(max_length=160)
    partition = models.IntegerField()
    offset = models.BigIntegerField()
    reason = models.CharField(max_length=300)
    payload = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [models.UniqueConstraint(fields=["topic", "partition", "offset"], name="dead_letter_unique")]
