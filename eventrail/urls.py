from django.urls import path

from rail import views


urlpatterns = [
    path("healthz", views.health),
    path("v1/events", views.ingest_event),
    path("v1/replays", views.replays),
    path("v1/replays/<uuid:replay_id>", views.replay_detail),
    path("v1/projections/<slug:version>", views.projection),
]
