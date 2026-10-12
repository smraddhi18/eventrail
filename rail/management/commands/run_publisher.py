import logging
import time

from django.core.management.base import BaseCommand
from django.db import close_old_connections

from rail.kafka_io import new_producer
from rail.publisher import publish_outbox_once, publish_replay_once


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Publish durable outbox events and replay jobs to Kafka"

    def handle(self, *args, **options) -> None:
        producer = new_producer()
        while True:
            close_old_connections()
            try:
                sent_live = publish_outbox_once(producer)
                sent_replay = publish_replay_once(producer)
                if not sent_live and not sent_replay:
                    time.sleep(0.5)
            except Exception:
                logger.exception("Publisher iteration failed")
                time.sleep(2)
