from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

from bot_vstrechi.application.service import MeetingWorkflowService
from bot_vstrechi.calendar.gateway import GoogleCalendarGateway
from bot_vstrechi.db.repository import SQLiteRepository
from bot_vstrechi.domain.commands import HandleInitiatorTimeout
from bot_vstrechi.domain.models import (
    CallbackActionToken,
    CallbackActionType,
    Decision,
    Meeting,
    MeetingParticipant,
    MeetingState,
    OutboxEffectType,
    Outcome,
    ReasonCode,
)
from bot_vstrechi.domain.state_machine import handle_initiator_timeout
from bot_vstrechi.telegram.adapter import TelegramWebhookAdapter
from bot_vstrechi.workers.calendar_sync import CalendarSyncWorker


def _repo(tmp_path: Path, name: str = "edge.db") -> SQLiteRepository:
    repository = SQLiteRepository(str(tmp_path / name))
    repository.initialize_schema()
    return repository


def _needs_initiator_meeting(
    *,
    meeting_id: str = "m-nid",
    now: datetime,
    scheduled_start_at: datetime,
) -> Meeting:
    return Meeting(
        meeting_id=meeting_id,
        initiator_telegram_user_id=10,
        chat_id=100,
        state=MeetingState.NEEDS_INITIATOR_DECISION,
        scheduled_start_at=scheduled_start_at,
        scheduled_end_at=scheduled_start_at + timedelta(hours=1),
        confirmation_round=1,
        initiator_decision_deadline_at=now + timedelta(minutes=15),
        participants=(
            MeetingParticipant(
                telegram_user_id=20,
                is_required=True,
                decision=Decision.CANCEL,
                decision_received_at=now - timedelta(minutes=5),
            ),
        ),
    )


class TestInitiatorTimeoutAfterStart:
    def test_timeout_before_start_cancels(self) -> None:
        base = datetime(2026, 3, 10, 14, 10, 0, tzinfo=timezone.utc)
        meeting = _needs_initiator_meeting(
            now=base,
            scheduled_start_at=datetime(2026, 3, 10, 15, 0, 0, tzinfo=timezone.utc),
        )
        now_past_deadline = datetime(2026, 3, 10, 14, 26, 0, tzinfo=timezone.utc)

        result, updated = handle_initiator_timeout(meeting, now=now_past_deadline)

        assert result.outcome == Outcome.OK
        assert updated.state == MeetingState.CANCELLED

    def test_timeout_after_start_should_expire_not_cancel(self) -> None:
        base = datetime(2026, 3, 10, 9, 45, 0, tzinfo=timezone.utc)
        meeting = _needs_initiator_meeting(
            now=base,
            scheduled_start_at=datetime(2026, 3, 10, 10, 0, 0, tzinfo=timezone.utc),
        )
        now_after_start = datetime(2026, 3, 10, 10, 5, 0, tzinfo=timezone.utc)

        result, updated = handle_initiator_timeout(meeting, now=now_after_start)

        assert result.outcome == Outcome.OK, f"Expected OK, got {result.outcome}"
        assert updated.state == MeetingState.EXPIRED, (
            f"Timeout after scheduled_start_at must give EXPIRED, got {updated.state}"
        )

    def test_command_timeout_after_start_should_expire_not_cancel(self) -> None:
        base = datetime(2026, 3, 10, 9, 45, 0, tzinfo=timezone.utc)
        meeting = _needs_initiator_meeting(
            now=base,
            scheduled_start_at=datetime(2026, 3, 10, 10, 0, 0, tzinfo=timezone.utc),
        )
        now_after_start = datetime(2026, 3, 10, 10, 5, 0, tzinfo=timezone.utc)

        execution = HandleInitiatorTimeout(meeting, round=1, now=now_after_start)

        assert execution.result.outcome == Outcome.OK
        assert execution.meeting.state == MeetingState.EXPIRED, (
            f"HandleInitiatorTimeout command produced {execution.meeting.state}, expected EXPIRED"
        )

    def test_timeout_exactly_at_start_should_expire(self) -> None:
        base = datetime(2026, 3, 10, 9, 45, 0, tzinfo=timezone.utc)
        meeting = _needs_initiator_meeting(
            now=base,
            scheduled_start_at=datetime(2026, 3, 10, 10, 0, 0, tzinfo=timezone.utc),
        )
        now_at_start = datetime(2026, 3, 10, 10, 0, 0, tzinfo=timezone.utc)

        result, updated = handle_initiator_timeout(meeting, now=now_at_start)

        assert result.outcome == Outcome.OK
        assert updated.state == MeetingState.EXPIRED, (
            f"Timeout at exactly scheduled_start_at should give EXPIRED, got {updated.state}"
        )


def _static_meeting(state: MeetingState, *, meeting_id: str, now: datetime) -> Meeting:
    decision = Decision.CONFIRM if state == MeetingState.CONFIRMED else Decision.CANCEL
    return Meeting(
        meeting_id=meeting_id,
        initiator_telegram_user_id=10,
        chat_id=100,
        state=state,
        scheduled_start_at=now + timedelta(hours=4),
        scheduled_end_at=now + timedelta(hours=5),
        confirmation_round=1,
        participants=(
            MeetingParticipant(
                telegram_user_id=20,
                is_required=True,
                decision=decision,
                decision_received_at=now - timedelta(minutes=5),
            ),
        ),
    )


def _token(meeting: Meeting, *, tok: str, now: datetime) -> CallbackActionToken:
    return CallbackActionToken(
        token=tok,
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        action_type=CallbackActionType.PARTICIPANT_CONFIRM,
        allowed_user_id=20,
        expires_at=now + timedelta(hours=1),
    )


def _callback_update(
    update_id: int, callback_id: str, tok: str, message_id: int = 99
) -> dict[str, object]:
    return {
        "update_id": update_id,
        "callback_query": {
            "id": callback_id,
            "from": {"id": 20},
            "data": f"act:{tok}",
            "message": {
                "message_id": message_id,
                "chat": {"id": 20},
                "text": "Подтвердите участие.\nВстреча\nВыберите действие:",
            },
        },
    }


def _drain_edits(
    repository: SQLiteRepository, *, now: datetime
) -> list[dict[str, object]]:
    edits: list[dict[str, object]] = []
    while True:
        outbox = repository.claim_due_outbox(now=now)
        if outbox is None:
            break
        if outbox.effect_type == OutboxEffectType.TELEGRAM_EDIT_MESSAGE:
            edits.append(dict(outbox.payload))
    return edits


class TestExpiredConfirmedButtonCleanup:
    def test_cancelled_cleans_buttons_baseline(self, tmp_path: Path) -> None:
        now = datetime(2026, 3, 10, 14, 0, 0)
        repository = _repo(tmp_path, "cancelled.db")
        meeting = _static_meeting(MeetingState.CANCELLED, meeting_id="m-c", now=now)
        repository.insert_meeting(meeting=meeting, now=now)
        t = _token(meeting, tok="tok-c", now=now)
        repository.upsert_callback_action_token(callback_token=t, now=now)

        adapter = TelegramWebhookAdapter(
            repository=repository,
            workflow_service=MeetingWorkflowService(
                repository, calendar_gateway=MagicMock()
            ),
        )
        result = adapter.handle_update(
            update=_callback_update(1, "cb-c", "tok-c"),
            now=now,
        )

        assert result.outcome == Outcome.NOOP
        assert result.reason_code == ReasonCode.INVALID_STATE
        edits = _drain_edits(repository, now=now)
        assert any(e.get("buttons") == [] for e in edits)
        repository.close()

    def test_expired_meeting_click_cleans_up_buttons(self, tmp_path: Path) -> None:
        now = datetime(2026, 3, 10, 14, 0, 0)
        repository = _repo(tmp_path, "expired.db")
        meeting = _static_meeting(MeetingState.EXPIRED, meeting_id="m-e", now=now)
        repository.insert_meeting(meeting=meeting, now=now)
        t = _token(meeting, tok="tok-e", now=now)
        repository.upsert_callback_action_token(callback_token=t, now=now)

        adapter = TelegramWebhookAdapter(
            repository=repository,
            workflow_service=MeetingWorkflowService(
                repository, calendar_gateway=MagicMock()
            ),
        )
        result = adapter.handle_update(
            update=_callback_update(10, "cb-e", "tok-e"),
            now=now,
        )

        assert result.outcome == Outcome.NOOP, (
            f"Expected NOOP for EXPIRED click, got {result.outcome}"
        )
        assert result.reason_code == ReasonCode.INVALID_STATE, (
            f"Expected INVALID_STATE for EXPIRED click, got {result.reason_code}"
        )
        edits = _drain_edits(repository, now=now)
        assert any(e.get("buttons") == [] for e in edits), (
            "EXPIRED meeting click must enqueue EDIT with buttons=[] to remove stale buttons"
        )
        repository.close()

    def test_confirmed_meeting_click_cleans_up_buttons(self, tmp_path: Path) -> None:
        now = datetime(2026, 3, 10, 14, 0, 0)
        repository = _repo(tmp_path, "confirmed.db")
        meeting = _static_meeting(MeetingState.CONFIRMED, meeting_id="m-conf", now=now)
        repository.insert_meeting(meeting=meeting, now=now)
        t = _token(meeting, tok="tok-conf", now=now)
        repository.upsert_callback_action_token(callback_token=t, now=now)

        adapter = TelegramWebhookAdapter(
            repository=repository,
            workflow_service=MeetingWorkflowService(
                repository, calendar_gateway=MagicMock()
            ),
        )
        result = adapter.handle_update(
            update=_callback_update(11, "cb-conf", "tok-conf"),
            now=now,
        )

        assert result.outcome == Outcome.NOOP, (
            f"Expected NOOP for CONFIRMED click, got {result.outcome}"
        )
        assert result.reason_code == ReasonCode.INVALID_STATE, (
            f"Expected INVALID_STATE for CONFIRMED click, got {result.reason_code}"
        )
        edits = _drain_edits(repository, now=now)
        assert any(e.get("buttons") == [] for e in edits), (
            "CONFIRMED meeting click must enqueue EDIT with buttons=[] to remove stale buttons"
        )
        repository.close()


@dataclass(frozen=True)
class _DeltaPage:
    items: list[dict[str, object]]
    next_page_token: str | None = None
    next_sync_token: str | None = "sync-token-after"
    full_sync_required: bool = False


class _FakeCalClient:
    def __init__(self, *, pages: list[_DeltaPage]) -> None:
        self._pages = pages
        self._cursor = 0

    def query_free_busy(
        self, **_: object
    ) -> dict[str, list[tuple[datetime, datetime]]]:
        return {}

    def insert_event(self, **_: object) -> str:
        return "new-event-id"

    def patch_event(self, **_: object) -> None:
        pass

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
            return _DeltaPage(items=[], next_sync_token="sync-token-after")
        page = self._pages[self._cursor]
        self._cursor += 1
        return page


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
    repository.grant_manager_role(telegram_user_id=100, granted_by=None, now=now)


def _seed_sync(repository: SQLiteRepository, *, now: datetime) -> None:
    repository.upsert_calendar_sync_state(
        calendar_id="init@example.com",
        sync_token="existing-token",
        watch_channel_id=None,
        watch_resource_id=None,
        watch_expiration_at=None,
        last_message_number=0,
        now=now,
    )


class TestCalendarRescheduleDuringNeedsInitiatorDecision:
    def _sync_reschedule(
        self,
        *,
        tmp_path: Path,
        db_name: str,
        meeting_id: str,
        message_number: int,
        new_start: datetime,
        now: datetime,
    ) -> tuple[SQLiteRepository, Meeting]:
        repository = _repo(tmp_path, db_name)
        _seed_users(repository, now=now)
        _seed_sync(repository, now=now)

        meeting = Meeting(
            meeting_id=meeting_id,
            initiator_telegram_user_id=100,
            chat_id=100,
            state=MeetingState.NEEDS_INITIATOR_DECISION,
            scheduled_start_at=now + timedelta(hours=2),
            scheduled_end_at=now + timedelta(hours=3),
            title="NID reschedule",
            google_event_id=f"ext-{meeting_id}",
            google_calendar_id="init@example.com",
            confirmation_round=1,
            initiator_decision_deadline_at=now + timedelta(minutes=15),
            participants=(
                MeetingParticipant(
                    telegram_user_id=100, is_required=False, decision=Decision.NONE
                ),
                MeetingParticipant(
                    telegram_user_id=200,
                    is_required=True,
                    decision=Decision.CANCEL,
                    decision_received_at=now - timedelta(minutes=5),
                ),
            ),
        )
        repository.insert_meeting(meeting=meeting, now=now)

        new_end = new_start + timedelta(hours=1)
        event: dict[str, object] = {
            "id": f"ext-{meeting_id}",
            "status": "confirmed",
            "summary": "NID reschedule",
            "organizer": {"email": "init@example.com"},
            "attendees": [
                {"email": "init@example.com", "responseStatus": "accepted"},
                {"email": "part@example.com", "responseStatus": "declined"},
            ],
            "start": {"dateTime": new_start.strftime("%Y-%m-%dT%H:%M:%SZ")},
            "end": {"dateTime": new_end.strftime("%Y-%m-%dT%H:%M:%SZ")},
        }

        fake_client = _FakeCalClient(pages=[_DeltaPage(items=[event])])
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
            external_event_id=f"wh-{meeting_id}",
            resource_state="exists",
            message_number=message_number,
            now=now,
        )
        _ = worker.run_once(now=now)

        updated = repository.get_meeting(meeting_id)
        assert updated is not None
        return repository, updated

    def test_reschedule_resets_state_to_pending_and_bumps_round(
        self, tmp_path: Path
    ) -> None:
        now = datetime(2026, 3, 10, 10, 0, 0, tzinfo=timezone.utc)
        new_start = now + timedelta(hours=5)

        repository, updated = self._sync_reschedule(
            tmp_path=tmp_path,
            db_name="nid-state.db",
            meeting_id="m-nid-state",
            message_number=50,
            new_start=new_start,
            now=now,
        )

        assert updated.confirmation_round == 2, (
            f"Expected round=2 after reschedule, got {updated.confirmation_round}"
        )
        assert updated.state == MeetingState.PENDING, (
            f"Expected PENDING after reschedule from NEEDS_INITIATOR_DECISION, got {updated.state}"
        )
        assert updated.scheduled_start_at == new_start, (
            f"Expected new start {new_start}, got {updated.scheduled_start_at}"
        )
        repository.close()

    def test_reschedule_resets_participant_decisions(self, tmp_path: Path) -> None:
        now = datetime(2026, 3, 10, 10, 0, 0, tzinfo=timezone.utc)
        new_start = now + timedelta(hours=5)

        repository, updated = self._sync_reschedule(
            tmp_path=tmp_path,
            db_name="nid-decisions.db",
            meeting_id="m-nid-decisions",
            message_number=51,
            new_start=new_start,
            now=now,
        )

        participant_200 = next(
            p for p in updated.participants if p.telegram_user_id == 200
        )
        assert participant_200.decision == Decision.NONE, (
            f"Participant decision must be reset after reschedule, got {participant_200.decision}"
        )
        repository.close()
