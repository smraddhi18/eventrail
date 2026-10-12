import uuid

import django.db.models.deletion
from django.db import migrations, models
from django.db.models import Q


class Migration(migrations.Migration):
    initial = True
    dependencies = []
    operations = [
        migrations.CreateModel(
            name="Tenant",
            fields=[
                ("id", models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ("name", models.CharField(max_length=120, unique=True)),
                ("api_key_hash", models.CharField(max_length=64, unique=True)),
                ("is_active", models.BooleanField(default=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
        ),
        migrations.CreateModel(
            name="Event",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("event_id", models.UUIDField(default=uuid.uuid4, editable=False, unique=True)),
                ("idempotency_key", models.CharField(max_length=128)),
                ("request_hash", models.CharField(max_length=64)),
                ("event_type", models.CharField(max_length=100)),
                ("schema_version", models.PositiveSmallIntegerField(default=1)),
                ("partition_key", models.CharField(max_length=128)),
                ("payload", models.JSONField()),
                ("occurred_at", models.DateTimeField()),
                ("accepted_at", models.DateTimeField(auto_now_add=True)),
                ("tenant", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to="rail.tenant")),
            ],
            options={
                "indexes": [
                    models.Index(fields=["tenant", "id", "accepted_at"], name="event_replay_scan_idx"),
                    models.Index(fields=["tenant", "partition_key", "id"], name="event_partition_order_idx"),
                ]
            },
        ),
        migrations.CreateModel(
            name="OutboxMessage",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("status", models.CharField(default="pending", max_length=16)),
                ("attempts", models.PositiveIntegerField(default=0)),
                ("available_at", models.DateTimeField(auto_now_add=True)),
                ("lease_until", models.DateTimeField(blank=True, null=True)),
                ("published_at", models.DateTimeField(blank=True, null=True)),
                ("last_error", models.CharField(blank=True, max_length=300)),
                ("event", models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, to="rail.event")),
            ],
            options={"indexes": [models.Index(fields=["status", "available_at"], name="outbox_claim_idx")]},
        ),
        migrations.CreateModel(
            name="ReplayJob",
            fields=[
                ("id", models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ("projection_version", models.SlugField(max_length=64)),
                ("from_time", models.DateTimeField(blank=True, null=True)),
                ("to_time", models.DateTimeField(blank=True, null=True)),
                ("upper_event_id", models.BigIntegerField(default=0)),
                ("cursor_event_id", models.BigIntegerField(default=0)),
                ("published_count", models.PositiveIntegerField(default=0)),
                ("status", models.CharField(default="queued", max_length=16)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("finished_at", models.DateTimeField(blank=True, null=True)),
                ("tenant", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to="rail.tenant")),
            ],
        ),
        migrations.CreateModel(
            name="ProcessedEvent",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("event_id", models.UUIDField()),
                ("projection_version", models.SlugField(max_length=64)),
                ("processed_at", models.DateTimeField(auto_now_add=True)),
                ("tenant", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to="rail.tenant")),
            ],
        ),
        migrations.CreateModel(
            name="DailyEventCount",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("projection_version", models.SlugField(max_length=64)),
                ("event_type", models.CharField(max_length=100)),
                ("day", models.DateField()),
                ("count", models.PositiveBigIntegerField(default=0)),
                ("tenant", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, to="rail.tenant")),
            ],
        ),
        migrations.CreateModel(
            name="DeadLetter",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("topic", models.CharField(max_length=160)),
                ("partition", models.IntegerField()),
                ("offset", models.BigIntegerField()),
                ("reason", models.CharField(max_length=300)),
                ("payload", models.TextField()),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
        ),
        migrations.AddConstraint(
            model_name="event",
            constraint=models.UniqueConstraint(fields=("tenant", "idempotency_key"), name="event_tenant_idem_unique"),
        ),
        migrations.AddConstraint(
            model_name="replayjob",
            constraint=models.UniqueConstraint(fields=("tenant", "projection_version"), name="replay_version_unique"),
        ),
        migrations.AddConstraint(
            model_name="replayjob",
            constraint=models.CheckConstraint(condition=~Q(projection_version="live"), name="replay_not_live"),
        ),
        migrations.AddConstraint(
            model_name="processedevent",
            constraint=models.UniqueConstraint(fields=("tenant", "event_id", "projection_version"), name="processed_once_unique"),
        ),
        migrations.AddConstraint(
            model_name="dailyeventcount",
            constraint=models.UniqueConstraint(
                fields=("tenant", "projection_version", "event_type", "day"), name="daily_count_unique"
            ),
        ),
        migrations.AddConstraint(
            model_name="deadletter",
            constraint=models.UniqueConstraint(fields=("topic", "partition", "offset"), name="dead_letter_unique"),
        ),
    ]
