import hashlib
import secrets

from django.core.management.base import BaseCommand, CommandError

from rail.models import Tenant


class Command(BaseCommand):
    help = "Create a tenant and show a one-time API key"

    def add_arguments(self, parser) -> None:
        parser.add_argument("name")

    def handle(self, *args, **options) -> None:
        key = f"er_{secrets.token_urlsafe(32)}"
        key_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()
        if Tenant.objects.filter(name=options["name"]).exists():
            raise CommandError("Tenant name already exists")
        tenant = Tenant.objects.create(name=options["name"], api_key_hash=key_hash)
        self.stdout.write(f"tenant_id={tenant.id}\napi_key={key}")
