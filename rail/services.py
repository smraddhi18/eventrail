import hashlib
import json
import re
from datetime import datetime
from typing import Any

from django.db import IntegrityError, transaction
from django.db.models import Max
from django.utils.dateparse import parse_datetime

from .errors import DomainError
from .models import Event, OutboxMessage, ReplayJob, Tenant


EVENT_TYPE = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$")
VERSION = re.compile(r"^[a-z][a-z0-9-]{0,63}$")


def require_object(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise DomainError("invalid_body", "Expected a JSON object")
    return value


def parse_timestamp(value: Any, field: str) -> datetime:
    parsed = parse_datetime(value) if isinstance(value, str) else None
    if parsed is None or parsed.tzinfo is None:
        raise DomainError("invalid_timestamp", f"{field} must be an ISO 8601 timestamp with a timezone")
    return parsed


def require_string(value: Any, field: str, max_length: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > max_length:
        raise DomainError("invalid_field", f"{field} must be a nonempty string of at most {max_length} characters")
    return value.strip()


def authenticate(api_key: str | None) -> Tenant:
    if not api_key or len(api_key) > 256:
        raise DomainError("unauthorized", "Valid X-API-Key required", 401)
    key_hash = hashlib.sha256(api_key.encode("utf-8")).hexdigest()
    tenant = Tenant.objects.filter(api_key_hash=key_hash, is_active=True).first()
    if tenant is None:
        raise DomainError("unauthorized", "Valid X-API-Key required", 401)
    return tenant


def accept_event(tenant: Tenant, body: dict[str, Any], idempotency_key: str | None) -> tuple[Event, bool]:
    key = require_string(idempotency_key, "Idempotency-Key", 128)
    event_type = require_string(body.get("event_type"), "event_type", 100)
    partition_key = require_string(body.get("partition_key"), "partition_key", 128)
    payload = require_object(body.get("payload"))
    schema_version = body.get("schema_version", 1)
    if EVENT_TYPE.fullmatch(event_type) is None:
        raise DomainError("invalid_event_type", "event_type must be dot separated lowercase names")
    if type(schema_version) is not int or not 1 <= schema_version <= 32767:
        raise DomainError("invalid_schema_version", "schema_version must be a positive integer")
    occurred_at = parse_timestamp(body.get("occurred_at"), "occurred_at")
    normalized = {
        "event_type": event_type,
        "partition_key": partition_key,
        "payload": payload,
        "schema_version": schema_version,
        "occurred_at": occurred_at.isoformat(),
    }
    request_hash = hashlib.sha256(
        json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    try:
        with transaction.atomic():
            event = Event.objects.create(
                tenant=tenant,
                idempotency_key=key,
                request_hash=request_hash,
                event_type=event_type,
                partition_key=partition_key,
                payload=payload,
                schema_version=schema_version,
                occurred_at=occurred_at,
            )
            OutboxMessage.objects.create(event=event)
        return event, True
    except IntegrityError:
        existing = Event.objects.filter(tenant=tenant, idempotency_key=key).first()
        if existing is None:
            raise
        if existing.request_hash != request_hash:
            raise DomainError("idempotency_conflict", "Idempotency-Key was used with different event data", 409)
        return existing, False


def create_replay(tenant: Tenant, body: dict[str, Any]) -> ReplayJob:
    version = require_string(body.get("projection_version"), "projection_version", 64)
    if version == "live" or VERSION.fullmatch(version) is None:
        raise DomainError("invalid_version", "Use a lowercase projection_version other than live")
    from_time = parse_timestamp(body["from_time"], "from_time") if body.get("from_time") else None
    to_time = parse_timestamp(body["to_time"], "to_time") if body.get("to_time") else None
    if from_time and to_time and from_time >= to_time:
        raise DomainError("invalid_range", "from_time must precede to_time")
    with transaction.atomic():
        upper_id = Event.objects.filter(tenant=tenant).aggregate(value=Max("id"))["value"] or 0
        try:
            with transaction.atomic():
                return ReplayJob.objects.create(
                    tenant=tenant,
                    projection_version=version,
                    from_time=from_time,
                    to_time=to_time,
                    upper_event_id=upper_id,
                )
        except IntegrityError as exc:
            raise DomainError("version_conflict", "Projection version already exists for this tenant", 409) from exc
