from __future__ import annotations

from datetime import datetime, timezone as dt_timezone
from typing import cast, override

from bot_vstrechi.calendar.gateway import CalendarApiClient, GoogleCalendarGateway


class _FakeCalendarClient(CalendarApiClient):
    @override
    def insert_event(
        self,
        *,
        organizer_email: str,
        payload: dict[str, object],
        idempotency_key: str | None = None,
    ) -> str:
        del organizer_email, payload, idempotency_key
        return "evt-1"

    @override
    def patch_event(
        self,
        *,
        google_event_id: str,
        initiator_google_email: str,
        payload: dict[str, object],
        idempotency_key: str | None = None,
    ) -> None:
        del google_event_id, initiator_google_email, payload, idempotency_key

    @override
    def query_free_busy(
        self,
        *,
        emails: tuple[str, ...],
        time_min: datetime,
        time_max: datetime,
    ) -> dict[str, list[tuple[datetime, datetime]]]:
        del emails, time_min, time_max
        return {}

    @override
    def list_events(
        self,
        *,
        email: str,
        time_min: datetime,
        time_max: datetime,
        max_results: int = 100,
    ) -> list[dict[str, object]]:
        del email, time_min, time_max, max_results
        return []


def test_each_occurrence_creates_unique_cycle() -> None:
    gateway = GoogleCalendarGateway(
        api_client=cast(CalendarApiClient, _FakeCalendarClient())
    )

    first = gateway.get_occurrence_identity(
        event={
            "id": "evt-a",
            "recurringEventId": "series-1",
            "originalStartTime": {"dateTime": "2026-02-20T08:00:00Z"},
        }
    )
    second = gateway.get_occurrence_identity(
        event={
            "id": "evt-b",
            "recurringEventId": "series-1",
            "originalStartTime": {"dateTime": "2026-02-21T08:00:00Z"},
        }
    )

    assert first.series_event_id == "series-1"
    assert second.series_event_id == "series-1"
    assert first.event_id != second.event_id
    assert first.occurrence_start_at == datetime(
        2026, 2, 20, 8, 0, tzinfo=dt_timezone.utc
    )
    assert second.occurrence_start_at == datetime(
        2026, 2, 21, 8, 0, tzinfo=dt_timezone.utc
    )


def test_occurrence_exception_updates_only_target() -> None:
    gateway = GoogleCalendarGateway(
        api_client=cast(CalendarApiClient, _FakeCalendarClient())
    )

    recurring_target = gateway.get_occurrence_identity(
        event={
            "id": "evt-exception",
            "recurringEventId": "series-2",
            "originalStartTime": {"date": "2026-03-02"},
            "start": {"dateTime": "2026-03-02T10:30:00Z"},
        }
    )
    standalone = gateway.get_occurrence_identity(
        event={
            "id": "evt-standalone",
            "start": {"dateTime": "2026-03-02T10:30:00Z"},
        }
    )

    assert recurring_target.series_event_id == "series-2"
    assert recurring_target.event_id == "evt-exception"
    assert recurring_target.occurrence_start_at == datetime(
        2026,
        3,
        2,
        0,
        0,
        tzinfo=dt_timezone.utc,
    )

    assert standalone.series_event_id is None
    assert standalone.event_id == "evt-standalone"
