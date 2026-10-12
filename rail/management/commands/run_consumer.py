from django.core.management.base import BaseCommand

from rail.consumer import run_consumer


class Command(BaseCommand):
    help = "Build idempotent tenant projections from Kafka events"

    def handle(self, *args, **options) -> None:
        run_consumer()
