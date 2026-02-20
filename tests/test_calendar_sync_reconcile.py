from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import override

from bot_vstrechi.application.service import MeetingWorkflowService
from bot_vstrechi.calendar.gateway import GoogleCalendarGateway
from bot_vstrechi.db.repository import SQLiteRepository
from bot_vstrechi.domain.models import (
    Decision,
    Meeting,
    MeetingParticipant,
    RecurringConfirmationMode,
    MeetingState,
    OutboxEffectType,
    OutboxStatus,
)
from bot_vstrechi.workers.calendar_sync import CalendarSyncWorker


def _repo(tmp_path: Path) -> SQLiteRepository:
    repository = SQLiteRepository(
        str(tmp_path / "bot_vstrechi_calendar_sync_reconcile.db")
    )
    repository.initialize_schema()
    return repository


@dataclass(frozen=True)
class _DeltaPage:
    items: list[dict[str, object]]
    next_page_token: str | None = None
    next_sync_token: str | None = "sync-token-1"
    full_sync_required: bool = False


class _FakeCalendarClient:
    def __init__(self, *, pages: list[_DeltaPage]) -> None:
        self._pages: list[_DeltaPage] = pages
        self._cursor: int = 0

    def query_free_busy(
        self, **_: object
    ) -> dict[str, list[tuple[datetime, datetime]]]:
        return {}

    def insert_event(self, **_: object) -> str:
        return "new-event-id"

    def patch_event(self, **_: object) -> None:
        return None

    def list_events(self, **_: object) -> list[dict[str, object]]:
        return []

    def list_event_deltas(
        self,
        *,
        email: str,
        sync_token: str | None,
        page_token: str | None,
        time_min: datetime,
        max_results: int = 250,
    ) -> _DeltaPage:
        del email, sync_token, page_token, time_min, max_results
        if self._cursor >= len(self._pages):
            return _DeltaPage(items=[], next_sync_token="sync-token-1")
        page = self._pages[self._cursor]
        self._cursor += 1
        return page


class _FailOnceDeltaClient(_FakeCalendarClient):
    def __init__(self, *, pages: list[_DeltaPage]) -> None:
        super().__init__(pages=pages)
        self._failed_once: bool = False

    @override
    def list_event_deltas(
        self,
        *,
        email: str,
        sync_token: str | None,
        page_token: str | None,
        time_min: datetime,
        max_results: int = 250,
    ) -> _DeltaPage:
        if not self._failed_once:
            self._failed_once = True
            raise RuntimeError("temporary sync failure")
        return super().list_event_deltas(
            email=email,
            sync_token=sync_token,
            page_token=page_token,
            time_min=time_min,
            max_results=max_results,
        )


def _seed_users(repository: SQLiteRepository, *, now: datetime) -> None:
    repository.upsert_user_mapping(
        telegram_user_id=100,
        telegram_username="init",
        full_name="Initiator",
        google_email="init@example.com",
        now=now,
    )
    repository.upsert_user_mapping(
        telegram_user_id=200,
        telegram_username="part",
        full_name="Participant",
        google_email="part@example.com",
        now=now,
    )
    repository.grant_manager_role(
        telegram_user_id=100,
        granted_by=None,
        now=now,
    )


def _seed_sync_token(
    repository: SQLiteRepository,
    *,
    calendar_id: str,
    now: datetime,
) -> None:
    repository.upsert_calendar_sync_state(
        calendar_id=calendar_id,
        sync_token="existing-sync-token",
        watch_channel_id=None,
        watch_resource_id=None,
        watch_expiration_at=None,
        last_message_number=0,
        now=now,
    )


def test_calendar_sync_creates_pending_meeting_from_external_event(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0, tzinfo=timezone.utc)
    repository = _repo(tmp_path)
    _seed_users(repository, now=now)
    _seed_sync_token(repository, calendar_id="init@example.com", now=now)

    event: dict[str, object] = {
        "id": "ext-event-1",
        "status": "confirmed",
        "summary": "Calendar-first sync test",
        "organizer": {"email": "init@example.com"},
        "attendees": [
            {"email": "init@example.com", "responseStatus": "accepted"},
            {"email": "part@example.com", "responseStatus": "needsAction"},
        ],
        "start": {"dateTime": "2026-02-20T12:00:00Z"},
        "end": {"dateTime": "2026-02-20T13:00:00Z"},
    }

    fake_client = _FakeCalendarClient(pages=[_DeltaPage(items=[event])])
    gateway = GoogleCalendarGateway(fake_client)
    service = MeetingWorkflowService(repository, gateway)
    worker = CalendarSyncWorker(
        repository=repository,
        workflow_service=service,
        calendar_gateway=gateway,
        calendar_client=fake_client,
    )

    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="init@example.com",
        external_event_id="wh-1",
        resource_state="exists",
        message_number=1,
        now=now,
    )

    tick = worker.run_once(now=now)

    assert tick.processed is True
    meetings = repository.list_initiator_meetings(
        initiator_telegram_user_id=100,
        now=now,
        states=(MeetingState.PENDING,),
        limit=10,
    )
    assert len(meetings) == 1
    created = meetings[0]
    assert created.google_event_id == "ext-event-1"
    assert created.state == MeetingState.PENDING
    assert repository.count_outbox(status=OutboxStatus.PENDING) >= 2
    repository.close()


def test_calendar_sync_uses_preferred_chat_id_for_new_meeting(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0, tzinfo=timezone.utc)
    repository = _repo(tmp_path)
    _seed_users(repository, now=now)
    _seed_sync_token(repository, calendar_id="init@example.com", now=now)
    _ = repository.set_preferred_chat_id(
        telegram_user_id=100,
        preferred_chat_id=-1005151698406,
        now=now,
    )

    event: dict[str, object] = {
        "id": "ext-event-pref-chat",
        "status": "confirmed",
        "summary": "Preferred chat target",
        "organizer": {"email": "init@example.com"},
        "attendees": [
            {"email": "init@example.com", "responseStatus": "accepted"},
            {"email": "part@example.com", "responseStatus": "needsAction"},
        ],
        "start": {"dateTime": "2026-02-20T12:00:00Z"},
        "end": {"dateTime": "2026-02-20T13:00:00Z"},
    }

    fake_client = _FakeCalendarClient(pages=[_DeltaPage(items=[event])])
    gateway = GoogleCalendarGateway(fake_client)
    service = MeetingWorkflowService(repository, gateway)
    worker = CalendarSyncWorker(
        repository=repository,
        workflow_service=service,
        calendar_gateway=gateway,
        calendar_client=fake_client,
    )

    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="init@example.com",
        external_event_id="wh-pref-chat",
        resource_state="exists",
        message_number=1,
        now=now,
    )

    tick = worker.run_once(now=now)

    assert tick.processed is True
    meetings = repository.list_initiator_meetings(
        initiator_telegram_user_id=100,
        now=now,
        states=(MeetingState.PENDING,),
        limit=10,
    )
    assert len(meetings) == 1
    created = meetings[0]
    assert created.chat_id == -1005151698406
    repository.close()


def test_calendar_sync_reschedules_existing_meeting(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0, tzinfo=timezone.utc)
    repository = _repo(tmp_path)
    _seed_users(repository, now=now)
    _seed_sync_token(repository, calendar_id="init@example.com", now=now)
    fake_client = _FakeCalendarClient(pages=[_DeltaPage(items=[])])
    gateway = GoogleCalendarGateway(fake_client)
    service = MeetingWorkflowService(repository, gateway)

    meeting = Meeting(
        meeting_id="m-sync-reschedule",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.PENDING,
        scheduled_start_at=now + timedelta(hours=2),
        scheduled_end_at=now + timedelta(hours=3),
        title="Sync reschedule",
        google_event_id="ext-event-2",
        google_calendar_id="init@example.com",
        participants=(
            MeetingParticipant(telegram_user_id=100, is_required=False),
            MeetingParticipant(
                telegram_user_id=200, is_required=True, decision=Decision.NONE
            ),
        ),
    )
    repository.insert_meeting(meeting, now=now)

    event: dict[str, object] = {
        "id": "ext-event-2",
        "status": "confirmed",
        "summary": "Sync reschedule",
        "organizer": {"email": "init@example.com"},
        "attendees": [
            {"email": "init@example.com", "responseStatus": "accepted"},
            {"email": "part@example.com", "responseStatus": "needsAction"},
        ],
        "start": {"dateTime": "2026-02-20T15:30:00Z"},
        "end": {"dateTime": "2026-02-20T16:30:00Z"},
    }
    fake_client._pages = [_DeltaPage(items=[event])]
    fake_client._cursor = 0
    worker = CalendarSyncWorker(
        repository=repository,
        workflow_service=service,
        calendar_gateway=gateway,
        calendar_client=fake_client,
    )
    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="init@example.com",
        external_event_id="wh-2",
        resource_state="exists",
        message_number=2,
        now=now,
    )

    _ = worker.run_once(now=now)

    updated = repository.get_meeting("m-sync-reschedule")
    assert updated is not None
    assert updated.confirmation_round == 2
    assert updated.scheduled_start_at == datetime(
        2026, 2, 20, 15, 30, tzinfo=timezone.utc
    )
    repository.close()


def test_calendar_sync_adds_new_attendee_to_existing_pending_meeting(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0, tzinfo=timezone.utc)
    repository = _repo(tmp_path)
    _seed_users(repository, now=now)
    repository.upsert_user_mapping(
        telegram_user_id=300,
        telegram_username="late",
        full_name="Late Participant",
        google_email="late@example.com",
        now=now,
    )
    _seed_sync_token(repository, calendar_id="init@example.com", now=now)
    gateway = GoogleCalendarGateway(_FakeCalendarClient(pages=[]))
    service = MeetingWorkflowService(repository, gateway)

    meeting = Meeting(
        meeting_id="m-sync-add-attendee",
        initiator_telegram_user_id=100,
        chat_id=-100777,
        state=MeetingState.PENDING,
        scheduled_start_at=now + timedelta(hours=2),
        scheduled_end_at=now + timedelta(hours=3),
        title="Sync add attendee",
        google_event_id="ext-event-add-attendee",
        google_calendar_id="init@example.com",
        confirmation_deadline_at=now + timedelta(minutes=30),
        participants=(
            MeetingParticipant(telegram_user_id=100, is_required=False),
            MeetingParticipant(
                telegram_user_id=200, is_required=True, decision=Decision.NONE
            ),
        ),
    )
    repository.insert_meeting(meeting, now=now)

    event: dict[str, object] = {
        "id": "ext-event-add-attendee",
        "status": "confirmed",
        "summary": "Sync add attendee",
        "organizer": {"email": "init@example.com"},
        "attendees": [
            {"email": "init@example.com", "responseStatus": "accepted"},
            {"email": "part@example.com", "responseStatus": "needsAction"},
            {"email": "late@example.com", "responseStatus": "needsAction"},
        ],
        "start": {"dateTime": "2026-02-20T12:00:00Z"},
        "end": {"dateTime": "2026-02-20T13:00:00Z"},
    }
    fake_client = _FakeCalendarClient(pages=[_DeltaPage(items=[event])])
    worker = CalendarSyncWorker(
        repository=repository,
        workflow_service=service,
        calendar_gateway=gateway,
        calendar_client=fake_client,
    )
    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="init@example.com",
        external_event_id="wh-add-attendee",
        resource_state="exists",
        message_number=5,
        now=now,
    )

    _ = worker.run_once(now=now)

    updated = repository.get_meeting("m-sync-add-attendee")
    assert updated is not None
    participant_ids = sorted(p.telegram_user_id for p in updated.participants)
    assert participant_ids == [100, 200, 300]

    participant_request_found = False
    while True:
        outbox = repository.claim_due_outbox(now=now)
        if outbox is None:
            break
        if (
            outbox.effect_type == OutboxEffectType.TELEGRAM_SEND_MESSAGE
            and outbox.payload.get("telegram_user_id") == 300
        ):
            text_obj = outbox.payload.get("text")
            buttons_obj = outbox.payload.get("buttons")
            assert isinstance(text_obj, str)
            assert "Подтвердите участие" in text_obj
            assert isinstance(buttons_obj, list)
            assert len(buttons_obj) == 2
            participant_request_found = True

    assert participant_request_found is True
    repository.close()


def test_calendar_sync_cancels_existing_meeting(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0, tzinfo=timezone.utc)
    repository = _repo(tmp_path)
    _seed_users(repository, now=now)
    _seed_sync_token(repository, calendar_id="init@example.com", now=now)
    gateway = GoogleCalendarGateway(_FakeCalendarClient(pages=[]))
    service = MeetingWorkflowService(repository, gateway)

    meeting = Meeting(
        meeting_id="m-sync-cancel",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.PENDING,
        scheduled_start_at=now + timedelta(hours=2),
        scheduled_end_at=now + timedelta(hours=3),
        title="Sync cancel",
        google_event_id="ext-event-3",
        google_calendar_id="init@example.com",
        participants=(
            MeetingParticipant(telegram_user_id=100, is_required=False),
            MeetingParticipant(
                telegram_user_id=200, is_required=True, decision=Decision.NONE
            ),
        ),
    )
    repository.insert_meeting(meeting, now=now)

    event: dict[str, object] = {
        "id": "ext-event-3",
        "status": "cancelled",
        "organizer": {"email": "init@example.com"},
        "start": {"dateTime": "2026-02-20T12:00:00Z"},
        "end": {"dateTime": "2026-02-20T13:00:00Z"},
    }
    fake_client = _FakeCalendarClient(pages=[_DeltaPage(items=[event])])
    worker = CalendarSyncWorker(
        repository=repository,
        workflow_service=service,
        calendar_gateway=gateway,
        calendar_client=fake_client,
    )
    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="init@example.com",
        external_event_id="wh-3",
        resource_state="exists",
        message_number=3,
        now=now,
    )

    _ = worker.run_once(now=now)

    updated = repository.get_meeting("m-sync-cancel")
    assert updated is not None
    assert updated.state == MeetingState.CANCELLED
    repository.close()


def test_calendar_sync_ignores_non_required_initiator_attendee_decision(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0, tzinfo=timezone.utc)
    repository = _repo(tmp_path)
    _seed_users(repository, now=now)
    _seed_sync_token(repository, calendar_id="init@example.com", now=now)

    meeting = Meeting(
        meeting_id="m-sync-initiator-attendee",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.PENDING,
        scheduled_start_at=now + timedelta(hours=2),
        scheduled_end_at=now + timedelta(hours=3),
        title="Initiator decision ignore",
        google_event_id="ext-event-4",
        google_calendar_id="init@example.com",
        participants=(
            MeetingParticipant(
                telegram_user_id=100, is_required=False, decision=Decision.NONE
            ),
            MeetingParticipant(
                telegram_user_id=200, is_required=True, decision=Decision.NONE
            ),
        ),
    )
    repository.insert_meeting(meeting, now=now)

    event: dict[str, object] = {
        "id": "ext-event-4",
        "status": "confirmed",
        "summary": "Initiator decision ignore",
        "organizer": {"email": "init@example.com"},
        "attendees": [
            {"email": "init@example.com", "responseStatus": "accepted"},
            {"email": "part@example.com", "responseStatus": "needsAction"},
        ],
        "start": {"dateTime": "2026-02-20T12:00:00Z"},
        "end": {"dateTime": "2026-02-20T13:00:00Z"},
    }
    fake_client = _FakeCalendarClient(pages=[_DeltaPage(items=[event])])
    gateway = GoogleCalendarGateway(fake_client)
    service = MeetingWorkflowService(repository, gateway)
    worker = CalendarSyncWorker(
        repository=repository,
        workflow_service=service,
        calendar_gateway=gateway,
        calendar_client=fake_client,
    )

    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="init@example.com",
        external_event_id="wh-4",
        resource_state="exists",
        message_number=4,
        now=now,
    )

    _ = worker.run_once(now=now)

    updated = repository.get_meeting("m-sync-initiator-attendee")
    assert updated is not None
    initiator = next(
        p
        for p in updated.participants
        if p.telegram_user_id == updated.initiator_telegram_user_id
    )
    assert initiator.decision == Decision.NONE
    repository.close()


def test_calendar_sync_bootstrap_sets_token_without_creating_meetings(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0, tzinfo=timezone.utc)
    repository = _repo(tmp_path)
    _seed_users(repository, now=now)

    event: dict[str, object] = {
        "id": "ext-event-bootstrap",
        "status": "confirmed",
        "summary": "Should not bootstrap-create",
        "organizer": {"email": "init@example.com"},
        "attendees": [
            {"email": "init@example.com", "responseStatus": "accepted"},
            {"email": "part@example.com", "responseStatus": "needsAction"},
        ],
        "start": {"dateTime": "2026-02-21T12:00:00Z"},
        "end": {"dateTime": "2026-02-21T13:00:00Z"},
    }
    fake_client = _FakeCalendarClient(
        pages=[_DeltaPage(items=[event], next_sync_token="bootstrap-token")]
    )
    gateway = GoogleCalendarGateway(fake_client)
    service = MeetingWorkflowService(repository, gateway)
    worker = CalendarSyncWorker(
        repository=repository,
        workflow_service=service,
        calendar_gateway=gateway,
        calendar_client=fake_client,
    )

    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="init@example.com",
        external_event_id="bootstrap-1",
        resource_state="poll",
        message_number=10,
        now=now,
    )

    _ = worker.run_once(now=now)

    meetings = repository.list_initiator_meetings(
        initiator_telegram_user_id=100,
        now=now,
        states=(MeetingState.PENDING,),
        limit=10,
    )
    assert meetings == []
    state = repository.get_calendar_sync_state(calendar_id="init@example.com")
    assert state is not None
    assert state.get("sync_token") == "bootstrap-token"
    repository.close()


def test_calendar_sync_retry_preserves_message_number_until_success(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0, tzinfo=timezone.utc)
    repository = _repo(tmp_path)
    repository.upsert_calendar_sync_state(
        calendar_id="init@example.com",
        sync_token="existing-sync-token",
        watch_channel_id=None,
        watch_resource_id=None,
        watch_expiration_at=None,
        last_message_number=40,
        now=now,
    )

    fake_client = _FailOnceDeltaClient(
        pages=[_DeltaPage(items=[], next_sync_token="sync-token-2")]
    )
    gateway = GoogleCalendarGateway(fake_client)
    service = MeetingWorkflowService(repository, gateway)
    worker = CalendarSyncWorker(
        repository=repository,
        workflow_service=service,
        calendar_gateway=gateway,
        calendar_client=fake_client,
    )

    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="init@example.com",
        external_event_id="wh-retry-preserve",
        resource_state="exists",
        message_number=41,
        now=now,
    )

    first_tick = worker.run_once(now=now)
    assert first_tick.processed is True
    assert first_tick.status == OutboxStatus.PENDING
    state_after_failure = repository.get_calendar_sync_state(
        calendar_id="init@example.com"
    )
    assert state_after_failure is not None
    assert state_after_failure.get("last_message_number") == 40

    second_tick = worker.run_once(now=now + timedelta(seconds=6))
    assert second_tick.processed is True
    assert second_tick.status == OutboxStatus.DONE
    state_after_success = repository.get_calendar_sync_state(
        calendar_id="init@example.com"
    )
    assert state_after_success is not None
    assert state_after_success.get("last_message_number") == 41
    assert state_after_success.get("sync_token") == "sync-token-2"
    repository.close()


def test_calendar_sync_does_not_create_for_non_manager_organizer(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0, tzinfo=timezone.utc)
    repository = _repo(tmp_path)
    repository.upsert_user_mapping(
        telegram_user_id=101,
        telegram_username="non_manager",
        full_name="Non Manager",
        google_email="non.manager@example.com",
        now=now,
    )
    repository.upsert_user_mapping(
        telegram_user_id=200,
        telegram_username="part",
        full_name="Participant",
        google_email="part@example.com",
        now=now,
    )
    _seed_sync_token(repository, calendar_id="non.manager@example.com", now=now)

    event: dict[str, object] = {
        "id": "ext-event-non-manager",
        "status": "confirmed",
        "summary": "Should not be imported",
        "organizer": {"email": "non.manager@example.com"},
        "attendees": [
            {"email": "non.manager@example.com", "responseStatus": "accepted"},
            {"email": "part@example.com", "responseStatus": "needsAction"},
        ],
        "start": {"dateTime": "2026-02-20T12:00:00Z"},
        "end": {"dateTime": "2026-02-20T13:00:00Z"},
    }

    fake_client = _FakeCalendarClient(pages=[_DeltaPage(items=[event])])
    gateway = GoogleCalendarGateway(fake_client)
    service = MeetingWorkflowService(repository, gateway)
    worker = CalendarSyncWorker(
        repository=repository,
        workflow_service=service,
        calendar_gateway=gateway,
        calendar_client=fake_client,
    )
    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="non.manager@example.com",
        external_event_id="wh-non-manager",
        resource_state="exists",
        message_number=11,
        now=now,
    )

    _ = worker.run_once(now=now)

    meetings = repository.list_initiator_meetings(
        initiator_telegram_user_id=101,
        now=now,
        states=(MeetingState.PENDING, MeetingState.DRAFT),
        limit=10,
    )
    assert meetings == []
    repository.close()


def test_calendar_sync_recurring_creates_only_one_open_occurrence(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0, tzinfo=timezone.utc)
    repository = _repo(tmp_path)
    _seed_users(repository, now=now)
    _seed_sync_token(repository, calendar_id="init@example.com", now=now)

    first_event: dict[str, object] = {
        "id": "ext-rec-1",
        "recurringEventId": "series-1",
        "originalStartTime": {"dateTime": "2026-02-20T12:00:00Z"},
        "status": "confirmed",
        "summary": "Daily",
        "organizer": {"email": "init@example.com"},
        "attendees": [
            {"email": "init@example.com", "responseStatus": "accepted"},
            {"email": "part@example.com", "responseStatus": "needsAction"},
        ],
        "start": {"dateTime": "2026-02-20T12:00:00Z"},
        "end": {"dateTime": "2026-02-20T12:30:00Z"},
    }
    second_event: dict[str, object] = {
        "id": "ext-rec-2",
        "recurringEventId": "series-1",
        "originalStartTime": {"dateTime": "2026-02-21T12:00:00Z"},
        "status": "confirmed",
        "summary": "Daily",
        "organizer": {"email": "init@example.com"},
        "attendees": [
            {"email": "init@example.com", "responseStatus": "accepted"},
            {"email": "part@example.com", "responseStatus": "needsAction"},
        ],
        "start": {"dateTime": "2026-02-21T12:00:00Z"},
        "end": {"dateTime": "2026-02-21T12:30:00Z"},
    }

    fake_client = _FakeCalendarClient(
        pages=[_DeltaPage(items=[first_event, second_event], next_sync_token="sync-2")]
    )
    gateway = GoogleCalendarGateway(fake_client)
    service = MeetingWorkflowService(repository, gateway)
    worker = CalendarSyncWorker(
        repository=repository,
        workflow_service=service,
        calendar_gateway=gateway,
        calendar_client=fake_client,
    )

    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="init@example.com",
        external_event_id="wh-series",
        resource_state="exists",
        message_number=12,
        now=now,
    )

    _ = worker.run_once(now=now)

    meetings = repository.list_initiator_meetings(
        initiator_telegram_user_id=100,
        now=now,
        states=(MeetingState.PENDING, MeetingState.DRAFT),
        limit=10,
    )
    assert len(meetings) == 1
    assert meetings[0].series_event_id == "series-1"
    repository.close()


def test_calendar_sync_recurring_updates_google_event_id_to_real_instance_id(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0, tzinfo=timezone.utc)
    repository = _repo(tmp_path)
    _seed_users(repository, now=now)
    _seed_sync_token(repository, calendar_id="init@example.com", now=now)

    meeting = Meeting(
        meeting_id="m-rec-real-id",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.PENDING,
        scheduled_start_at=now + timedelta(hours=2),
        scheduled_end_at=now + timedelta(hours=2, minutes=30),
        title="Daily",
        google_event_id="series-1:2026-02-20T12:00:00+00:00",
        google_calendar_id="init@example.com",
        series_event_id="series-1",
        occurrence_start_at=now + timedelta(hours=2),
        participants=(
            MeetingParticipant(telegram_user_id=100, is_required=False),
            MeetingParticipant(telegram_user_id=200, is_required=True),
        ),
    )
    repository.insert_meeting(meeting, now=now)

    event: dict[str, object] = {
        "id": "ext-rec-real-1",
        "recurringEventId": "series-1",
        "originalStartTime": {"dateTime": "2026-02-20T12:00:00Z"},
        "status": "confirmed",
        "summary": "Daily",
        "organizer": {"email": "init@example.com"},
        "attendees": [
            {"email": "init@example.com", "responseStatus": "accepted"},
            {"email": "part@example.com", "responseStatus": "needsAction"},
        ],
        "start": {"dateTime": "2026-02-20T12:00:00Z"},
        "end": {"dateTime": "2026-02-20T12:30:00Z"},
    }

    fake_client = _FakeCalendarClient(pages=[_DeltaPage(items=[event])])
    gateway = GoogleCalendarGateway(fake_client)
    service = MeetingWorkflowService(repository, gateway)
    worker = CalendarSyncWorker(
        repository=repository,
        workflow_service=service,
        calendar_gateway=gateway,
        calendar_client=fake_client,
    )

    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="init@example.com",
        external_event_id="wh-rec-real-id",
        resource_state="exists",
        message_number=18,
        now=now,
    )

    _ = worker.run_once(now=now)

    updated = repository.get_meeting("m-rec-real-id")
    assert updated is not None
    assert updated.google_event_id == "ext-rec-real-1"
    repository.close()


def test_calendar_sync_recurring_exceptions_only_autoconfirms_without_dm_poll(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0, tzinfo=timezone.utc)
    repository = _repo(tmp_path)
    _seed_users(repository, now=now)
    _seed_sync_token(repository, calendar_id="init@example.com", now=now)

    event: dict[str, object] = {
        "id": "ext-rec-ex-1",
        "recurringEventId": "series-ex-1",
        "originalStartTime": {"dateTime": "2026-02-20T12:00:00Z"},
        "status": "confirmed",
        "summary": "Daily no-spam",
        "organizer": {"email": "init@example.com"},
        "attendees": [
            {"email": "init@example.com", "responseStatus": "accepted"},
            {"email": "part@example.com", "responseStatus": "needsAction"},
        ],
        "start": {"dateTime": "2026-02-20T12:00:00Z"},
        "end": {"dateTime": "2026-02-20T13:00:00Z"},
    }

    fake_client = _FakeCalendarClient(pages=[_DeltaPage(items=[event])])
    gateway = GoogleCalendarGateway(fake_client)
    service = MeetingWorkflowService(repository, gateway)
    worker = CalendarSyncWorker(
        repository=repository,
        workflow_service=service,
        calendar_gateway=gateway,
        calendar_client=fake_client,
        recurring_exceptions_only_enabled=True,
    )
    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="init@example.com",
        external_event_id="wh-rec-ex-1",
        resource_state="exists",
        message_number=13,
        now=now,
    )

    _ = worker.run_once(now=now)

    meetings = repository.list_initiator_meetings(
        initiator_telegram_user_id=100,
        now=now,
        states=(MeetingState.CONFIRMED,),
        limit=10,
    )
    assert len(meetings) == 1
    assert (
        meetings[0].recurring_confirmation_mode
        == RecurringConfirmationMode.EXCEPTIONS_ONLY
    )

    poll_dm_found = False
    while True:
        outbox = repository.claim_due_outbox(now=now)
        if outbox is None:
            break
        if outbox.effect_type != OutboxEffectType.TELEGRAM_SEND_MESSAGE:
            continue
        text_obj = outbox.payload.get("text")
        if isinstance(text_obj, str) and "Подтвердите участие" in text_obj:
            poll_dm_found = True

    assert poll_dm_found is False
    repository.close()


def test_calendar_sync_recurring_exceptions_only_time_change_opens_pending_round(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0, tzinfo=timezone.utc)
    repository = _repo(tmp_path)
    _seed_users(repository, now=now)
    _seed_sync_token(repository, calendar_id="init@example.com", now=now)
    gateway = GoogleCalendarGateway(_FakeCalendarClient(pages=[]))
    service = MeetingWorkflowService(repository, gateway)

    meeting = Meeting(
        meeting_id="m-rec-ex-time",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.CONFIRMED,
        scheduled_start_at=now + timedelta(hours=2),
        scheduled_end_at=now + timedelta(hours=3),
        title="Recurring time change",
        google_event_id="ext-rec-ex-time",
        google_calendar_id="init@example.com",
        series_event_id="series-ex-time",
        occurrence_start_at=now + timedelta(hours=2),
        recurring_confirmation_mode=RecurringConfirmationMode.EXCEPTIONS_ONLY,
        participants=(
            MeetingParticipant(telegram_user_id=100, is_required=False),
            MeetingParticipant(telegram_user_id=200, is_required=True),
        ),
    )
    repository.insert_meeting(meeting, now=now)

    event: dict[str, object] = {
        "id": "ext-rec-ex-time",
        "recurringEventId": "series-ex-time",
        "originalStartTime": {"dateTime": "2026-02-20T12:00:00Z"},
        "status": "confirmed",
        "summary": "Recurring time change",
        "organizer": {"email": "init@example.com"},
        "attendees": [
            {"email": "init@example.com", "responseStatus": "accepted"},
            {"email": "part@example.com", "responseStatus": "needsAction"},
        ],
        "start": {"dateTime": "2026-02-20T13:30:00Z"},
        "end": {"dateTime": "2026-02-20T14:30:00Z"},
    }

    fake_client = _FakeCalendarClient(pages=[_DeltaPage(items=[event])])
    worker = CalendarSyncWorker(
        repository=repository,
        workflow_service=service,
        calendar_gateway=gateway,
        calendar_client=fake_client,
        recurring_exceptions_only_enabled=True,
    )
    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="init@example.com",
        external_event_id="wh-rec-ex-time",
        resource_state="exists",
        message_number=14,
        now=now,
    )

    _ = worker.run_once(now=now)

    updated = repository.get_meeting("m-rec-ex-time")
    assert updated is not None
    assert updated.state == MeetingState.PENDING
    assert updated.confirmation_round == 2
    repository.close()


def test_calendar_sync_recurring_exceptions_only_added_user_gets_single_notice(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0, tzinfo=timezone.utc)
    repository = _repo(tmp_path)
    _seed_users(repository, now=now)
    repository.upsert_user_mapping(
        telegram_user_id=300,
        telegram_username="late",
        full_name="Late Participant",
        google_email="late@example.com",
        now=now,
    )
    _seed_sync_token(repository, calendar_id="init@example.com", now=now)
    gateway = GoogleCalendarGateway(_FakeCalendarClient(pages=[]))
    service = MeetingWorkflowService(repository, gateway)

    meeting = Meeting(
        meeting_id="m-rec-ex-add",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.CONFIRMED,
        scheduled_start_at=now + timedelta(hours=2),
        scheduled_end_at=now + timedelta(hours=3),
        title="Recurring add participant",
        google_event_id="ext-rec-ex-add",
        google_calendar_id="init@example.com",
        series_event_id="series-ex-add",
        occurrence_start_at=now + timedelta(hours=2),
        recurring_confirmation_mode=RecurringConfirmationMode.EXCEPTIONS_ONLY,
        participants=(
            MeetingParticipant(telegram_user_id=100, is_required=False),
            MeetingParticipant(telegram_user_id=200, is_required=True),
        ),
    )
    repository.insert_meeting(meeting, now=now)

    event: dict[str, object] = {
        "id": "ext-rec-ex-add",
        "recurringEventId": "series-ex-add",
        "originalStartTime": {"dateTime": "2026-02-20T12:00:00Z"},
        "status": "confirmed",
        "summary": "Recurring add participant",
        "organizer": {"email": "init@example.com"},
        "attendees": [
            {"email": "init@example.com", "responseStatus": "accepted"},
            {"email": "part@example.com", "responseStatus": "needsAction"},
            {"email": "late@example.com", "responseStatus": "needsAction"},
        ],
        "start": {"dateTime": "2026-02-20T12:00:00Z"},
        "end": {"dateTime": "2026-02-20T13:00:00Z"},
    }

    fake_client = _FakeCalendarClient(pages=[_DeltaPage(items=[event])])
    worker = CalendarSyncWorker(
        repository=repository,
        workflow_service=service,
        calendar_gateway=gateway,
        calendar_client=fake_client,
        recurring_exceptions_only_enabled=True,
    )
    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="init@example.com",
        external_event_id="wh-rec-ex-add",
        resource_state="exists",
        message_number=15,
        now=now,
    )

    _ = worker.run_once(now=now)

    added_notice_found = False
    poll_buttons_found = False
    while True:
        outbox = repository.claim_due_outbox(now=now)
        if outbox is None:
            break
        if outbox.effect_type != OutboxEffectType.TELEGRAM_SEND_MESSAGE:
            continue
        text_obj = outbox.payload.get("text")
        if (
            outbox.payload.get("telegram_user_id") == 300
            and isinstance(text_obj, str)
            and "Вас добавили в регулярную встречу" in text_obj
        ):
            added_notice_found = True
        buttons_obj = outbox.payload.get("buttons")
        if isinstance(buttons_obj, list) and buttons_obj:
            poll_buttons_found = True

    assert added_notice_found is True
    assert poll_buttons_found is False
    repository.close()


def test_calendar_sync_title_update_preserves_recurring_mode(tmp_path: Path) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0, tzinfo=timezone.utc)
    repository = _repo(tmp_path)
    _seed_users(repository, now=now)
    _seed_sync_token(repository, calendar_id="init@example.com", now=now)
    fake_client = _FakeCalendarClient(pages=[])
    gateway = GoogleCalendarGateway(fake_client)
    service = MeetingWorkflowService(repository, gateway)

    meeting = Meeting(
        meeting_id="m-rec-ex-title",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.CONFIRMED,
        scheduled_start_at=now + timedelta(hours=2),
        scheduled_end_at=now + timedelta(hours=3),
        title="Old title",
        google_event_id="ext-rec-ex-title",
        google_calendar_id="init@example.com",
        series_event_id="series-title",
        occurrence_start_at=now + timedelta(hours=2),
        recurring_confirmation_mode=RecurringConfirmationMode.EXCEPTIONS_ONLY,
        participants=(
            MeetingParticipant(telegram_user_id=100, is_required=False),
            MeetingParticipant(telegram_user_id=200, is_required=True),
        ),
    )
    repository.insert_meeting(meeting, now=now)

    event: dict[str, object] = {
        "id": "ext-rec-ex-title",
        "recurringEventId": "series-title",
        "originalStartTime": {"dateTime": "2026-02-20T12:00:00Z"},
        "status": "confirmed",
        "summary": "New title",
        "organizer": {"email": "init@example.com"},
        "attendees": [
            {"email": "init@example.com", "responseStatus": "accepted"},
            {"email": "part@example.com", "responseStatus": "needsAction"},
        ],
        "start": {"dateTime": "2026-02-20T12:00:00Z"},
        "end": {"dateTime": "2026-02-20T13:00:00Z"},
    }

    fake_client._pages = [_DeltaPage(items=[event])]
    fake_client._cursor = 0
    worker = CalendarSyncWorker(
        repository=repository,
        workflow_service=service,
        calendar_gateway=gateway,
        calendar_client=fake_client,
        recurring_exceptions_only_enabled=True,
    )
    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="init@example.com",
        external_event_id="wh-rec-ex-title",
        resource_state="exists",
        message_number=16,
        now=now,
    )

    _ = worker.run_once(now=now)

    updated = repository.get_meeting("m-rec-ex-title")
    assert updated is not None
    assert updated.title == "New title"
    assert (
        updated.recurring_confirmation_mode == RecurringConfirmationMode.EXCEPTIONS_ONLY
    )
    repository.close()


def test_calendar_sync_whitespace_series_id_does_not_enable_exceptions_only(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 20, 10, 0, 0, tzinfo=timezone.utc)
    repository = _repo(tmp_path)
    _seed_users(repository, now=now)
    _seed_sync_token(repository, calendar_id="init@example.com", now=now)

    event: dict[str, object] = {
        "id": "ext-rec-space-1",
        "recurringEventId": "   ",
        "originalStartTime": {"dateTime": "2026-02-20T12:00:00Z"},
        "status": "confirmed",
        "summary": "Whitespace recurring id",
        "organizer": {"email": "init@example.com"},
        "attendees": [
            {"email": "init@example.com", "responseStatus": "accepted"},
            {"email": "part@example.com", "responseStatus": "needsAction"},
        ],
        "start": {"dateTime": "2026-02-20T12:00:00Z"},
        "end": {"dateTime": "2026-02-20T13:00:00Z"},
    }

    fake_client = _FakeCalendarClient(pages=[_DeltaPage(items=[event])])
    gateway = GoogleCalendarGateway(fake_client)
    service = MeetingWorkflowService(repository, gateway)
    worker = CalendarSyncWorker(
        repository=repository,
        workflow_service=service,
        calendar_gateway=gateway,
        calendar_client=fake_client,
        recurring_exceptions_only_enabled=True,
    )
    _ = repository.enqueue_calendar_sync_signal(
        calendar_id="init@example.com",
        external_event_id="wh-rec-space-1",
        resource_state="exists",
        message_number=17,
        now=now,
    )

    _ = worker.run_once(now=now)

    meetings = repository.list_initiator_meetings(
        initiator_telegram_user_id=100,
        now=now,
        states=(MeetingState.PENDING,),
        limit=10,
    )
    assert len(meetings) == 1
    created = meetings[0]
    assert created.series_event_id is None
    assert created.recurring_confirmation_mode == RecurringConfirmationMode.STRICT
    repository.close()
