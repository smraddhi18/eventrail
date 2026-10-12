from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from rail.models import OutboxMessage


class Command(BaseCommand):
    help = "Explicitly requeue a failed outbox message by event UUID"

    def add_arguments(self, parser) -> None:
        parser.add_argument("event_id")

    def handle(self, *args, **options) -> None:
        row = OutboxMessage.objects.filter(event__event_id=options["event_id"], status="dead").first()
        if row is None:
            raise CommandError("Failed outbox event not found")
        row.status = "pending"
        row.attempts = 0
        row.available_at = timezone.now()
        row.last_error = ""
        row.save(update_fields=["status", "attempts", "available_at", "last_error"])
        self.stdout.write("requeued")
