import json
import logging
from functools import wraps

from django.db.models import Q
from django.http import HttpRequest, JsonResponse
from django.core.exceptions import RequestDataTooBig
from django.utils.dateparse import parse_date
from django.views.decorators.csrf import csrf_exempt

from .errors import DomainError
from .models import DailyEventCount, ProcessedEvent, ReplayJob
from .services import authenticate, accept_event, create_replay, require_object


logger = logging.getLogger(__name__)


def reject_nonfinite(value: str):
    raise ValueError(f"Nonfinite JSON number: {value}")


def api_view(fn):
    @csrf_exempt
    @wraps(fn)
    def wrapped(request: HttpRequest, *args, **kwargs) -> JsonResponse:
        try:
            return fn(request, *args, **kwargs)
        except DomainError as exc:
            return JsonResponse({"error": {"code": exc.code, "message": exc.message}}, status=exc.status)
        except RequestDataTooBig:
            return JsonResponse({"error": {"code": "body_too_large", "message": "Maximum body size is 64 KB"}}, status=413)
        except Exception:
            logger.exception("Unhandled API error")
            return JsonResponse({"error": {"code": "internal_error", "message": "Internal server error"}}, status=500)

    return wrapped


def method(request: HttpRequest, allowed: str) -> None:
    if request.method != allowed:
        raise DomainError("method_not_allowed", f"Use {allowed}", 405)


def json_body(request: HttpRequest) -> dict:
    if request.content_type != "application/json":
        raise DomainError("invalid_content_type", "Content-Type must be application/json", 415)
    try:
        return require_object(json.loads(request.body, parse_constant=reject_nonfinite))
    except (ValueError, UnicodeDecodeError) as exc:
        raise DomainError("invalid_json", "Request body must be valid JSON") from exc


def tenant_for(request: HttpRequest):
    return authenticate(request.headers.get("X-API-Key"))


@api_view
def health(request: HttpRequest) -> JsonResponse:
    method(request, "GET")
    return JsonResponse({"status": "ok"})


@api_view
def ingest_event(request: HttpRequest) -> JsonResponse:
    method(request, "POST")
    tenant = tenant_for(request)
    event, created = accept_event(tenant, json_body(request), request.headers.get("Idempotency-Key"))
    return JsonResponse(
        {"event_id": str(event.event_id), "accepted_at": event.accepted_at.isoformat(), "created": created},
        status=202,
    )


@api_view
def replays(request: HttpRequest) -> JsonResponse:
    method(request, "POST")
    job = create_replay(tenant_for(request), json_body(request))
    return JsonResponse({"replay_id": str(job.id), "status": job.status, "projection_version": job.projection_version}, status=202)


@api_view
def replay_detail(request: HttpRequest, replay_id) -> JsonResponse:
    method(request, "GET")
    job = ReplayJob.objects.filter(id=replay_id, tenant=tenant_for(request)).first()
    if job is None:
        raise DomainError("not_found", "Replay not found", 404)
    return JsonResponse(
        {
            "replay_id": str(job.id),
            "status": job.status,
            "projection_version": job.projection_version,
            "upper_event_id": job.upper_event_id,
            "cursor_event_id": job.cursor_event_id,
            "published_count": job.published_count,
            "processed_count": ProcessedEvent.objects.filter(
                tenant=job.tenant, projection_version=job.projection_version
            ).count(),
            "finished_at": job.finished_at.isoformat() if job.finished_at else None,
        }
    )


@api_view
def projection(request: HttpRequest, version: str) -> JsonResponse:
    method(request, "GET")
    tenant = tenant_for(request)
    cursor = request.GET.get("after")
    queryset = DailyEventCount.objects.filter(tenant=tenant, projection_version=version)
    if cursor:
        parts = cursor.split("|", 1)
        if len(parts) != 2 or parse_date(parts[0]) is None or not parts[1]:
            raise DomainError("invalid_cursor", "after must be a day and event type separated by |")
        day = parse_date(parts[0])
        queryset = queryset.filter(Q(day__gt=day) | Q(day=day, event_type__gt=parts[1]))
    rows = list(queryset.order_by("day", "event_type")[:501])
    page = rows[:500]
    return JsonResponse(
        {
            "projection_version": version,
            "counts": [
                {"day": row.day.isoformat(), "event_type": row.event_type, "count": row.count}
                for row in page
            ],
            "next_cursor": f"{page[-1].day.isoformat()}|{page[-1].event_type}" if len(rows) > 500 else None,
        }
    )
