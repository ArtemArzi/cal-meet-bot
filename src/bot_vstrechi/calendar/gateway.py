from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, time, timezone as dt_timezone
from typing import Protocol, cast
from zoneinfo import ZoneInfo

from bot_vstrechi.domain.models import Meeting, Outcome, ReasonCode


class CalendarApiClient(Protocol):
    def insert_event(
        self,
        *,
        organizer_email: str,
        payload: dict[str, object],
        idempotency_key: str | None = None,
    ) -> str: ...

    def patch_event(
        self,
        *,
        google_event_id: str,
        initiator_google_email: str,
        payload: dict[str, object],
        idempotency_key: str | None = None,
    ) -> None: ...

    def query_free_busy(
        self,
        *,
        emails: tuple[str, ...],
        time_min: datetime,
        time_max: datetime,
    ) -> dict[str, list[tuple[datetime, datetime]]]: ...

    def list_events(
        self,
        *,
        email: str,
        time_min: datetime,
        time_max: datetime,
        max_results: int = 100,
    ) -> list[dict[str, object]]: ...


@dataclass(frozen=True)
class CalendarGatewayResult:
    outcome: Outcome
    reason_code: ReasonCode


@dataclass(frozen=True)
class DaySlotAvailability:
    start_at: datetime
    end_at: datetime
    busy_emails: tuple[str, ...]

    @property
    def is_free(self) -> bool:
        return not self.busy_emails


@dataclass(frozen=True)
class CalendarOccurrenceIdentity:
    event_id: str
    series_event_id: str | None
    occurrence_start_at: datetime | None


class GoogleCalendarGateway:
    def __init__(self, api_client: CalendarApiClient) -> None:
        self._api_client: CalendarApiClient = api_client

    def search_free_slots(
        self,
        *,
        emails: tuple[str, ...],
        duration_minutes: int,
        timezone: str,
        now: datetime,
    ) -> list[tuple[datetime, datetime]]:
        def _to_utc(value: datetime) -> datetime:
            if value.tzinfo is None:
                return value.replace(tzinfo=dt_timezone.utc)
            return value.astimezone(dt_timezone.utc)

        user_tz = ZoneInfo(timezone)
        now_utc = _to_utc(now)
        time_min = now_utc
        time_max = now_utc + timedelta(days=7)

        busy_map = self._api_client.query_free_busy(
            emails=emails,
            time_min=time_min,
            time_max=time_max,
        )

        all_busy: list[tuple[datetime, datetime]] = []
        for intervals in busy_map.values():
            for interval_start, interval_end in intervals:
                busy_start = _to_utc(interval_start)
                busy_end = _to_utc(interval_end)
                if busy_end <= busy_start:
                    continue
                all_busy.append((busy_start, busy_end))

        all_busy.sort(key=lambda x: x[0])

        merged_busy: list[tuple[datetime, datetime]] = []
        if all_busy:
            current_start, current_end = all_busy[0]
            for next_start, next_end in all_busy[1:]:
                if next_start < current_end:
                    current_end = max(current_end, next_end)
                else:
                    merged_busy.append((current_start, current_end))
                    current_start, current_end = next_start, next_end
            merged_busy.append((current_start, current_end))

        working_start_time = time(9, 0)
        working_end_time = time(18, 0)

        slots: list[tuple[datetime, datetime]] = []
        search_ptr = now_utc.astimezone(user_tz) + timedelta(minutes=1)

        while len(slots) < 4 and search_ptr.astimezone(dt_timezone.utc) < time_max:
            rem = search_ptr.minute % 15
            if rem != 0 or search_ptr.second != 0 or search_ptr.microsecond != 0:
                search_ptr = (search_ptr + timedelta(minutes=15 - rem)).replace(
                    second=0, microsecond=0
                )

            if search_ptr.weekday() >= 5:  # Sat, Sun
                search_ptr = (search_ptr + timedelta(days=1)).replace(hour=9, minute=0)
                continue

            day_end = search_ptr.replace(hour=18, minute=0)
            if search_ptr.time() < working_start_time:
                search_ptr = search_ptr.replace(hour=9, minute=0)
            elif search_ptr.time() >= working_end_time:
                search_ptr = (search_ptr + timedelta(days=1)).replace(hour=9, minute=0)
                continue

            slot_start = search_ptr
            slot_end = slot_start + timedelta(minutes=duration_minutes)

            if slot_end > day_end:
                search_ptr = (search_ptr + timedelta(days=1)).replace(hour=9, minute=0)
                continue

            utc_start = slot_start.astimezone(dt_timezone.utc)
            utc_end = slot_end.astimezone(dt_timezone.utc)

            overlap = False
            for b_start, b_end in merged_busy:
                if utc_start < b_end and utc_end > b_start:
                    overlap = True
                    search_ptr = b_end.astimezone(user_tz)
                    break

            if not overlap:
                slots.append((utc_start, utc_end))
                search_ptr = slot_end

        return slots

    def list_day_slot_availability(
        self,
        *,
        emails: tuple[str, ...],
        duration_minutes: int,
        timezone: str,
        day: date,
        step_minutes: int = 30,
    ) -> list[DaySlotAvailability]:
        if duration_minutes <= 0:
            return []
        if step_minutes <= 0:
            return []

        user_tz = ZoneInfo(timezone)
        local_day_start = datetime.combine(day, time(9, 0), tzinfo=user_tz)
        local_day_end = datetime.combine(day, time(18, 0), tzinfo=user_tz)
        day_start_utc = local_day_start.astimezone(dt_timezone.utc)
        day_end_utc = local_day_end.astimezone(dt_timezone.utc)

        busy_map = self._api_client.query_free_busy(
            emails=emails,
            time_min=day_start_utc,
            time_max=day_end_utc,
        )

        normalized_busy_map: dict[str, list[tuple[datetime, datetime]]] = {}
        for email in emails:
            intervals = busy_map.get(email, [])
            normalized: list[tuple[datetime, datetime]] = []
            for start_at, end_at in intervals:
                if start_at.tzinfo is None:
                    start_utc = start_at.replace(tzinfo=dt_timezone.utc)
                else:
                    start_utc = start_at.astimezone(dt_timezone.utc)
                if end_at.tzinfo is None:
                    end_utc = end_at.replace(tzinfo=dt_timezone.utc)
                else:
                    end_utc = end_at.astimezone(dt_timezone.utc)
                if end_utc <= start_utc:
                    continue
                normalized.append((start_utc, end_utc))
            normalized.sort(key=lambda item: item[0])
            normalized_busy_map[email] = normalized

        slot_span = timedelta(minutes=duration_minutes)
        step_span = timedelta(minutes=step_minutes)
        result: list[DaySlotAvailability] = []

        ptr = local_day_start
        while ptr + slot_span <= local_day_end:
            slot_start = ptr
            slot_end = ptr + slot_span
            slot_start_utc = slot_start.astimezone(dt_timezone.utc)
            slot_end_utc = slot_end.astimezone(dt_timezone.utc)

            busy_emails: list[str] = []
            for email in emails:
                intervals = normalized_busy_map.get(email, [])
                has_overlap = any(
                    slot_start_utc < busy_end and slot_end_utc > busy_start
                    for busy_start, busy_end in intervals
                )
                if has_overlap:
                    busy_emails.append(email)

            result.append(
                DaySlotAvailability(
                    start_at=slot_start_utc,
                    end_at=slot_end_utc,
                    busy_emails=tuple(busy_emails),
                )
            )
            ptr += step_span

        return result

    def patch_event_for_meeting(
        self,
        *,
        meeting: Meeting,
        google_event_id: str,
        initiator_google_email: str,
        payload: dict[str, object],
        idempotency_key: str | None = None,
    ) -> CalendarGatewayResult:
        if not meeting.created_by_bot:
            return CalendarGatewayResult(
                outcome=Outcome.REJECTED,
                reason_code=ReasonCode.NOT_BOT_CREATED_EVENT,
            )

        self._api_client.patch_event(
            google_event_id=google_event_id,
            initiator_google_email=initiator_google_email,
            payload=payload,
            idempotency_key=idempotency_key,
        )
        return CalendarGatewayResult(
            outcome=Outcome.OK,
            reason_code=ReasonCode.UPDATED,
        )

    def get_occurrence_identity(
        self,
        *,
        event: Mapping[str, object],
    ) -> CalendarOccurrenceIdentity:
        raw_event_id = event.get("id")
        event_id = raw_event_id.strip() if isinstance(raw_event_id, str) else ""

        raw_series_event_id = event.get("recurringEventId")
        series_event_id = (
            str(raw_series_event_id) if isinstance(raw_series_event_id, str) else None
        )

        occurrence_start_at = self._extract_occurrence_start_at(event)

        if event_id:
            stable_id = event_id
        elif series_event_id is not None and occurrence_start_at is not None:
            stable_id = f"{series_event_id}:{occurrence_start_at.isoformat()}"
        else:
            stable_id = "unknown"

        return CalendarOccurrenceIdentity(
            event_id=stable_id,
            series_event_id=series_event_id,
            occurrence_start_at=occurrence_start_at,
        )

    def _extract_occurrence_start_at(
        self,
        event: Mapping[str, object],
    ) -> datetime | None:
        original_start = event.get("originalStartTime")
        if isinstance(original_start, Mapping):
            parsed_original = self._parse_google_event_start(
                cast(Mapping[str, object], original_start)
            )
            if parsed_original is not None:
                return parsed_original

        start = event.get("start")
        if isinstance(start, Mapping):
            return self._parse_google_event_start(cast(Mapping[str, object], start))
        return None

    def _parse_google_event_start(
        self,
        payload: Mapping[str, object],
    ) -> datetime | None:
        raw_datetime = payload.get("dateTime")
        if isinstance(raw_datetime, str):
            try:
                parsed = datetime.fromisoformat(raw_datetime.replace("Z", "+00:00"))
            except ValueError:
                return None
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=dt_timezone.utc)
            return parsed.astimezone(dt_timezone.utc)

        raw_date = payload.get("date")
        if isinstance(raw_date, str):
            try:
                parsed_date = date.fromisoformat(raw_date)
            except ValueError:
                return None
            return datetime.combine(parsed_date, time.min, tzinfo=dt_timezone.utc)

        return None

    def list_schedule_events(
        self,
        *,
        email: str,
        now: datetime,
        days: int = 7,
        max_results: int = 100,
    ) -> list[tuple[datetime, datetime, str]]:
        time_min = now
        time_max = now + timedelta(days=days)
        rows = self._api_client.list_events(
            email=email,
            time_min=time_min,
            time_max=time_max,
            max_results=max_results,
        )

        events: list[tuple[datetime, datetime, str]] = []
        for row in rows:
            start_obj = row.get("start")
            end_obj = row.get("end")
            title_obj = row.get("summary", "Без названия")
            if (
                isinstance(start_obj, datetime)
                and isinstance(end_obj, datetime)
                and isinstance(title_obj, str)
            ):
                events.append((start_obj, end_obj, title_obj))

        events.sort(key=lambda item: item[0])
        return events
