from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from bot_vstrechi.domain.commands import CommandExecution
from bot_vstrechi.db.repository import SQLiteRepository
from bot_vstrechi.domain.models import (
    CommandResult,
    Meeting,
    MeetingParticipant,
    MeetingState,
    OutboxEffectType,
    Outcome,
    ReasonCode,
)
from bot_vstrechi.workers.outbox import OutboxDispatcher, OutboxWorker


def _repo(tmp_path: Path) -> SQLiteRepository:
    repository = SQLiteRepository(str(tmp_path / "bot_vstrechi_group_status.db"))
    repository.initialize_schema()
    return repository


class _FakeTelegramClient:
    def __init__(self, *, next_message_id: int = 1000) -> None:
        self.next_message_id = next_message_id
        self.sent: list[dict[str, object]] = []
        self.edited: list[dict[str, object]] = []
        self.raise_on_edit: RuntimeError | None = None

    def send_message(
        self,
        *,
        telegram_user_id: int,
        text: str,
        buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
        keyboard: list[list[str]] | None = None,
        idempotency_key: str | None = None,
    ) -> int | None:
        self.sent.append(
            {
                "telegram_user_id": telegram_user_id,
                "text": text,
                "buttons": buttons,
                "keyboard": keyboard,
                "idempotency_key": idempotency_key,
            }
        )
        message_id = self.next_message_id
        self.next_message_id += 1
        return message_id

    def edit_message(
        self,
        *,
        telegram_user_id: int,
        message_id: int,
        text: str,
        buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
        idempotency_key: str | None = None,
    ) -> None:
        if self.raise_on_edit is not None:
            raise self.raise_on_edit
        self.edited.append(
            {
                "telegram_user_id": telegram_user_id,
                "message_id": message_id,
                "text": text,
                "buttons": buttons,
                "idempotency_key": idempotency_key,
            }
        )

    def answer_callback_query(
        self,
        *,
        callback_query_id: str,
        text: str | None = None,
        idempotency_key: str | None = None,
    ) -> None:
        del callback_query_id, text, idempotency_key


class _FakeCalendarClient:
    def query_free_busy(
        self,
        *,
        emails: tuple[str, ...],
        time_min: datetime,
        time_max: datetime,
    ) -> dict[str, list[tuple[datetime, datetime]]]:
        del emails, time_min, time_max
        return {}

    def insert_event(
        self,
        *,
        organizer_email: str,
        payload: dict[str, object],
        idempotency_key: str | None = None,
    ) -> str:
        del organizer_email, payload, idempotency_key
        return "event-id"

    def patch_event(
        self,
        *,
        google_event_id: str,
        initiator_google_email: str,
        payload: dict[str, object],
        idempotency_key: str | None = None,
    ) -> None:
        del google_event_id, initiator_google_email, payload, idempotency_key

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


def _meeting(now: datetime) -> Meeting:
    return Meeting(
        meeting_id="m-group",
        initiator_telegram_user_id=100,
        chat_id=-100123,
        state=MeetingState.PENDING,
        scheduled_start_at=now + timedelta(hours=2),
        scheduled_end_at=now + timedelta(hours=3),
        title="Group lifecycle",
        participants=(
            MeetingParticipant(telegram_user_id=100, is_required=False),
            MeetingParticipant(telegram_user_id=200, is_required=True),
        ),
    )


def test_group_status_first_send_stores_message_pointer(tmp_path: Path) -> None:
    now = datetime(2026, 2, 21, 10, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now)
    repository.insert_meeting(meeting, now=now)

    _ = repository.enqueue_outbox(
        effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
        payload={
            "telegram_user_id": meeting.chat_id,
            "text": "pending",
            "_group_status_message": True,
            "_meeting_id": meeting.meeting_id,
        },
        idempotency_key="group:first-send",
        now=now,
    )

    telegram = _FakeTelegramClient(next_message_id=501)
    worker = OutboxWorker(
        repository=repository,
        dispatcher=OutboxDispatcher(
            repository=repository,
            telegram_client=telegram,
            calendar_client=_FakeCalendarClient(),
        ),
    )

    tick = worker.run_once(now=now)
    assert tick.processed is True
    assert len(telegram.sent) == 1

    updated = repository.get_meeting(meeting.meeting_id)
    assert updated is not None
    assert updated.group_status_message_id == 501
    repository.close()


def test_group_status_edit_failure_posts_single_replacement(tmp_path: Path) -> None:
    now = datetime(2026, 2, 21, 10, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now)
    repository.insert_meeting(meeting, now=now)

    with_pointer = Meeting(
        meeting_id=meeting.meeting_id,
        initiator_telegram_user_id=meeting.initiator_telegram_user_id,
        chat_id=meeting.chat_id,
        state=meeting.state,
        scheduled_start_at=meeting.scheduled_start_at,
        scheduled_end_at=meeting.scheduled_end_at,
        title=meeting.title,
        google_event_id=meeting.google_event_id,
        google_calendar_id=meeting.google_calendar_id,
        series_event_id=meeting.series_event_id,
        occurrence_start_at=meeting.occurrence_start_at,
        group_status_message_id=777,
        created_by_bot=meeting.created_by_bot,
        confirmation_round=meeting.confirmation_round,
        confirmation_deadline_at=meeting.confirmation_deadline_at,
        initiator_decision_deadline_at=meeting.initiator_decision_deadline_at,
        participants=meeting.participants,
    )
    _ = repository.apply_execution(
        before=meeting,
        execution=CommandExecution(
            result=CommandResult(Outcome.OK, ReasonCode.UPDATED),
            meeting=with_pointer,
        ),
        now=now,
    )

    _ = repository.enqueue_outbox(
        effect_type=OutboxEffectType.TELEGRAM_EDIT_MESSAGE,
        payload={
            "telegram_user_id": meeting.chat_id,
            "message_id": 777,
            "text": "updated status",
            "_group_status_message": True,
            "_meeting_id": meeting.meeting_id,
        },
        idempotency_key="group:edit",
        now=now,
    )

    telegram = _FakeTelegramClient(next_message_id=888)
    telegram.raise_on_edit = RuntimeError(
        "Telegram edit non-retryable status 400: message to edit not found"
    )
    worker = OutboxWorker(
        repository=repository,
        dispatcher=OutboxDispatcher(
            repository=repository,
            telegram_client=telegram,
            calendar_client=_FakeCalendarClient(),
        ),
    )

    tick = worker.run_once(now=now)
    assert tick.processed is True
    assert len(telegram.edited) == 0
    assert len(telegram.sent) == 1

    updated = repository.get_meeting(meeting.meeting_id)
    assert updated is not None
    assert updated.group_status_message_id == 888
    repository.close()
