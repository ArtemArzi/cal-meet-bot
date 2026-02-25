from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

from bot_vstrechi.domain import (
    Decision,
    Meeting,
    MeetingParticipant,
    MeetingState,
    OutboxEffectType,
    OutboxStatus,
)
from bot_vstrechi.workers.outbox import (
    OutboxDispatcher,
    OutboxWorker,
    RetryableOutboxError,
)
from bot_vstrechi.db.repository import SQLiteRepository


def _repo(tmp_path: Path) -> SQLiteRepository:
    repository = SQLiteRepository(str(tmp_path / "bot_vstrechi_phase5_outbox.db"))
    repository.initialize_schema()
    return repository


def _meeting(now: datetime, *, meeting_id: str) -> Meeting:
    return Meeting(
        meeting_id=meeting_id,
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.PENDING,
        scheduled_start_at=now + timedelta(hours=1),
        scheduled_end_at=now + timedelta(hours=2),
        participants=(
            MeetingParticipant(
                telegram_user_id=100,
                is_required=False,
                decision=Decision.NONE,
            ),
        ),
    )


def test_outbox_enqueue_is_idempotent_by_idempotency_key(tmp_path: Path) -> None:
    now = datetime(2026, 2, 12, 11, 0, 0)
    repository = _repo(tmp_path)

    first = repository.enqueue_outbox(
        effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
        payload={"telegram_user_id": 100, "text": "hello"},
        idempotency_key="tg:hello:100",
        now=now,
    )
    second = repository.enqueue_outbox(
        effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
        payload={"telegram_user_id": 100, "text": "hello"},
        idempotency_key="tg:hello:100",
        now=now,
    )

    assert first is True
    assert second is False
    assert repository.count_outbox(status=OutboxStatus.PENDING) == 1
    repository.close()


def test_outbox_worker_retries_retryable_error_then_succeeds(tmp_path: Path) -> None:
    now = datetime(2026, 2, 12, 11, 0, 0)
    repository = _repo(tmp_path)
    _ = repository.enqueue_outbox(
        effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
        payload={"telegram_user_id": 200, "text": "retry me"},
        idempotency_key="tg:retry:200",
        now=now,
    )

    class FakeTelegramClient:
        def __init__(self) -> None:
            self.calls: int = 0

        def send_message(
            self,
            *,
            telegram_user_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            keyboard: list[list[str]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, text, buttons, keyboard, idempotency_key
            self.calls += 1
            if self.calls == 1:
                raise RetryableOutboxError("temporary")

        def edit_message(
            self,
            *,
            telegram_user_id: int,
            message_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, message_id, text, buttons, idempotency_key

        def answer_callback_query(
            self,
            *,
            callback_query_id: str,
            text: str | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del callback_query_id, text, idempotency_key

    class FakeCalendarClient:
        def query_free_busy(
            self,
            *,
            emails: tuple[str, ...],
            time_min: datetime,
            time_max: datetime,
        ) -> dict[str, list[tuple[datetime, datetime]]]:
            del time_min, time_max
            return {email: [] for email in emails}

        def insert_event(
            self,
            *,
            organizer_email: str,
            payload: dict[str, object],
            idempotency_key: str | None = None,
        ) -> str:
            del organizer_email, payload, idempotency_key
            return "evt-fake"

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

    dispatcher = OutboxDispatcher(
        repository=repository,
        telegram_client=cast(Any, FakeTelegramClient()),
        calendar_client=FakeCalendarClient(),
    )
    worker = OutboxWorker(repository=repository, dispatcher=dispatcher)

    first_tick = worker.run_once(now=now)
    assert first_tick.processed is True
    assert first_tick.status == OutboxStatus.PENDING
    assert repository.count_outbox(status=OutboxStatus.PENDING) == 1

    second_tick = worker.run_once(now=now + timedelta(seconds=6))
    assert second_tick.processed is True
    assert second_tick.status == OutboxStatus.DONE
    assert repository.count_outbox(status=OutboxStatus.DONE) == 1
    repository.close()


def test_outbox_worker_retries_transient_runtimeerror(tmp_path: Path) -> None:
    now = datetime(2026, 2, 12, 11, 0, 0)
    repository = _repo(tmp_path)
    _ = repository.enqueue_outbox(
        effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
        payload={"telegram_user_id": 200, "text": "retry runtime"},
        idempotency_key="tg:retry-runtime:200",
        now=now,
    )

    class FakeTelegramClient:
        def __init__(self) -> None:
            self.calls: int = 0

        def send_message(
            self,
            *,
            telegram_user_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            keyboard: list[list[str]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, text, buttons, keyboard, idempotency_key
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("Telegram sendMessage failed with status 503")

        def edit_message(
            self,
            *,
            telegram_user_id: int,
            message_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, message_id, text, buttons, idempotency_key

        def answer_callback_query(
            self,
            *,
            callback_query_id: str,
            text: str | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del callback_query_id, text, idempotency_key

    class FakeCalendarClient:
        def query_free_busy(
            self,
            *,
            emails: tuple[str, ...],
            time_min: datetime,
            time_max: datetime,
        ) -> dict[str, list[tuple[datetime, datetime]]]:
            del time_min, time_max
            return {email: [] for email in emails}

        def insert_event(
            self,
            *,
            organizer_email: str,
            payload: dict[str, object],
            idempotency_key: str | None = None,
        ) -> str:
            del organizer_email, payload, idempotency_key
            return "evt-fake"

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

    dispatcher = OutboxDispatcher(
        repository=repository,
        telegram_client=cast(Any, FakeTelegramClient()),
        calendar_client=FakeCalendarClient(),
    )
    worker = OutboxWorker(repository=repository, dispatcher=dispatcher)

    first_tick = worker.run_once(now=now)
    assert first_tick.processed is True
    assert first_tick.status == OutboxStatus.PENDING

    second_tick = worker.run_once(now=now + timedelta(seconds=6))
    assert second_tick.processed is True
    assert second_tick.status == OutboxStatus.DONE
    assert repository.count_outbox(status=OutboxStatus.DONE) == 1
    repository.close()


def test_outbox_worker_does_not_retry_non_retryable_runtimeerror(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 12, 11, 0, 0)
    repository = _repo(tmp_path)
    _ = repository.enqueue_outbox(
        effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
        payload={"telegram_user_id": 200, "text": "fail runtime"},
        idempotency_key="tg:fail-runtime:200",
        now=now,
    )

    class FakeTelegramClient:
        def send_message(
            self,
            *,
            telegram_user_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            keyboard: list[list[str]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, text, buttons, keyboard, idempotency_key
            raise RuntimeError(
                "Telegram sendMessage non-retryable status 400: bad request"
            )

        def edit_message(
            self,
            *,
            telegram_user_id: int,
            message_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, message_id, text, buttons, idempotency_key

        def answer_callback_query(
            self,
            *,
            callback_query_id: str,
            text: str | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del callback_query_id, text, idempotency_key

    class FakeCalendarClient:
        def query_free_busy(
            self,
            *,
            emails: tuple[str, ...],
            time_min: datetime,
            time_max: datetime,
        ) -> dict[str, list[tuple[datetime, datetime]]]:
            del time_min, time_max
            return {email: [] for email in emails}

        def insert_event(
            self,
            *,
            organizer_email: str,
            payload: dict[str, object],
            idempotency_key: str | None = None,
        ) -> str:
            del organizer_email, payload, idempotency_key
            return "evt-fake"

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

    dispatcher = OutboxDispatcher(
        repository=repository,
        telegram_client=cast(Any, FakeTelegramClient()),
        calendar_client=FakeCalendarClient(),
    )
    worker = OutboxWorker(repository=repository, dispatcher=dispatcher)

    tick = worker.run_once(now=now)
    assert tick.processed is True
    assert tick.status == OutboxStatus.FAILED
    assert repository.count_outbox(status=OutboxStatus.FAILED) == 1
    repository.close()


def test_outbox_worker_marks_non_retryable_error_as_failed(tmp_path: Path) -> None:
    now = datetime(2026, 2, 12, 11, 0, 0)
    repository = _repo(tmp_path)
    _ = repository.enqueue_outbox(
        effect_type=OutboxEffectType.CALENDAR_PATCH_EVENT,
        payload={"google_event_id": "evt-1"},
        idempotency_key="cal:evt-1",
        now=now,
    )

    class FakeTelegramClient:
        def send_message(
            self,
            *,
            telegram_user_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            keyboard: list[list[str]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, text, buttons, keyboard, idempotency_key

        def edit_message(
            self,
            *,
            telegram_user_id: int,
            message_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, message_id, text, buttons, idempotency_key

        def answer_callback_query(
            self,
            *,
            callback_query_id: str,
            text: str | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del callback_query_id, text, idempotency_key

    class FakeCalendarClient:
        def query_free_busy(
            self,
            *,
            emails: tuple[str, ...],
            time_min: datetime,
            time_max: datetime,
        ) -> dict[str, list[tuple[datetime, datetime]]]:
            del time_min, time_max
            return {email: [] for email in emails}

        def insert_event(
            self,
            *,
            organizer_email: str,
            payload: dict[str, object],
            idempotency_key: str | None = None,
        ) -> str:
            del organizer_email, payload, idempotency_key
            return "evt-fake"

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

    dispatcher = OutboxDispatcher(
        repository=repository,
        telegram_client=cast(Any, FakeTelegramClient()),
        calendar_client=FakeCalendarClient(),
    )
    worker = OutboxWorker(repository=repository, dispatcher=dispatcher)

    tick = worker.run_once(now=now)
    assert tick.processed is True
    assert tick.status == OutboxStatus.FAILED
    assert repository.count_outbox(status=OutboxStatus.FAILED) == 1
    repository.close()


def test_calendar_insert_updates_meeting_google_fields(tmp_path: Path) -> None:
    now = datetime(2026, 2, 12, 11, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now, meeting_id="m-5-insert")
    repository.insert_meeting(meeting, now=now)

    _ = repository.enqueue_outbox(
        effect_type=OutboxEffectType.CALENDAR_INSERT_EVENT,
        payload={
            "organizer_email": "initiator@example.com",
            "meeting_id": meeting.meeting_id,
            "payload": {
                "summary": "Sync",
                "start": {"dateTime": meeting.scheduled_start_at.isoformat()},
                "end": {"dateTime": meeting.scheduled_end_at.isoformat()},
            },
        },
        idempotency_key="cal:insert:m-5-insert",
        now=now,
    )

    class FakeTelegramClient:
        def send_message(
            self,
            *,
            telegram_user_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            keyboard: list[list[str]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, text, buttons, keyboard, idempotency_key

        def edit_message(
            self,
            *,
            telegram_user_id: int,
            message_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, message_id, text, buttons, idempotency_key

        def answer_callback_query(
            self,
            *,
            callback_query_id: str,
            text: str | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del callback_query_id, text, idempotency_key

    class FakeCalendarClient:
        def query_free_busy(
            self,
            *,
            emails: tuple[str, ...],
            time_min: datetime,
            time_max: datetime,
        ) -> dict[str, list[tuple[datetime, datetime]]]:
            del time_min, time_max
            return {email: [] for email in emails}

        def insert_event(
            self,
            *,
            organizer_email: str,
            payload: dict[str, object],
            idempotency_key: str | None = None,
        ) -> str:
            del organizer_email, payload, idempotency_key
            return "evt-42"

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

    dispatcher = OutboxDispatcher(
        repository=repository,
        telegram_client=cast(Any, FakeTelegramClient()),
        calendar_client=FakeCalendarClient(),
    )
    worker = OutboxWorker(repository=repository, dispatcher=dispatcher)

    tick = worker.run_once(now=now)
    assert tick.processed is True
    assert tick.status == OutboxStatus.DONE

    updated = repository.get_meeting(meeting.meeting_id)
    assert updated is not None
    assert updated.google_event_id == "evt-42"
    assert updated.google_calendar_id == "initiator@example.com"
    repository.close()


def test_outbox_dispatches_telegram_edit_message(tmp_path: Path) -> None:
    now = datetime(2026, 2, 12, 11, 0, 0)
    repository = _repo(tmp_path)
    _ = repository.enqueue_outbox(
        effect_type=OutboxEffectType.TELEGRAM_EDIT_MESSAGE,
        payload={
            "telegram_user_id": 200,
            "message_id": 17,
            "text": "updated",
            "buttons": [],
        },
        idempotency_key="tg:edit:200:17",
        now=now,
    )

    edits: list[tuple[int, int, str]] = []

    class FakeTelegramClient:
        def send_message(
            self,
            *,
            telegram_user_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            keyboard: list[list[str]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, text, buttons, keyboard, idempotency_key

        def edit_message(
            self,
            *,
            telegram_user_id: int,
            message_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del buttons, idempotency_key
            edits.append((telegram_user_id, message_id, text))

        def answer_callback_query(
            self,
            *,
            callback_query_id: str,
            text: str | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del callback_query_id, text, idempotency_key

    class FakeCalendarClient:
        def query_free_busy(
            self,
            *,
            emails: tuple[str, ...],
            time_min: datetime,
            time_max: datetime,
        ) -> dict[str, list[tuple[datetime, datetime]]]:
            del time_min, time_max
            return {email: [] for email in emails}

        def insert_event(
            self,
            *,
            organizer_email: str,
            payload: dict[str, object],
            idempotency_key: str | None = None,
        ) -> str:
            del organizer_email, payload, idempotency_key
            return "evt-fake"

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

    dispatcher = OutboxDispatcher(
        repository=repository,
        telegram_client=cast(Any, FakeTelegramClient()),
        calendar_client=FakeCalendarClient(),
    )
    worker = OutboxWorker(repository=repository, dispatcher=dispatcher)

    tick = worker.run_once(now=now)
    assert tick.processed is True
    assert tick.status == OutboxStatus.DONE
    assert edits == [(200, 17, "updated")]
    repository.close()


def test_outbox_dispatches_telegram_callback_answer(tmp_path: Path) -> None:
    now = datetime(2026, 2, 12, 11, 0, 0)
    repository = _repo(tmp_path)
    _ = repository.enqueue_outbox(
        effect_type=OutboxEffectType.TELEGRAM_ANSWER_CALLBACK,
        payload={"callback_query_id": "cb-77", "text": "ok"},
        idempotency_key="tg:cb:77",
        now=now,
    )

    answers: list[tuple[str, str | None]] = []

    class FakeTelegramClient:
        def send_message(
            self,
            *,
            telegram_user_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            keyboard: list[list[str]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, text, buttons, keyboard, idempotency_key

        def edit_message(
            self,
            *,
            telegram_user_id: int,
            message_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, message_id, text, buttons, idempotency_key

        def answer_callback_query(
            self,
            *,
            callback_query_id: str,
            text: str | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del idempotency_key
            answers.append((callback_query_id, text))

    class FakeCalendarClient:
        def query_free_busy(
            self,
            *,
            emails: tuple[str, ...],
            time_min: datetime,
            time_max: datetime,
        ) -> dict[str, list[tuple[datetime, datetime]]]:
            del time_min, time_max
            return {email: [] for email in emails}

        def insert_event(
            self,
            *,
            organizer_email: str,
            payload: dict[str, object],
            idempotency_key: str | None = None,
        ) -> str:
            del organizer_email, payload, idempotency_key
            return "evt-fake"

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

    dispatcher = OutboxDispatcher(
        repository=repository,
        telegram_client=cast(Any, FakeTelegramClient()),
        calendar_client=FakeCalendarClient(),
    )
    worker = OutboxWorker(repository=repository, dispatcher=dispatcher)

    tick = worker.run_once(now=now)
    assert tick.processed is True
    assert tick.status == OutboxStatus.DONE
    assert answers == [("cb-77", "ok")]
    repository.close()


def test_calendar_patch_success_sends_final_status(tmp_path: Path) -> None:
    now = datetime(2026, 2, 12, 11, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now, meeting_id="m-5-final-success")
    meeting = Meeting(
        meeting_id=meeting.meeting_id,
        initiator_telegram_user_id=meeting.initiator_telegram_user_id,
        chat_id=-100500,
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
    repository.insert_meeting(meeting, now=now)
    _ = repository.enqueue_outbox(
        effect_type=OutboxEffectType.CALENDAR_PATCH_EVENT,
        payload={
            "google_event_id": "evt-final-success",
            "initiator_google_email": "initiator@example.com",
            "payload": {"summary": "done"},
            "_post_patch_group_status": {
                "meeting_id": meeting.meeting_id,
                "round": meeting.confirmation_round,
                "target_state": "confirmed",
                "chat_id": meeting.chat_id,
                "initiator_user_id": meeting.initiator_telegram_user_id,
                "text": "✅ Встреча подтверждена",
            },
        },
        idempotency_key="cal:patch:final-success",
        now=now,
    )

    class FakeTelegramClient:
        def send_message(
            self,
            *,
            telegram_user_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            keyboard: list[list[str]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, text, buttons, keyboard, idempotency_key

        def edit_message(
            self,
            *,
            telegram_user_id: int,
            message_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, message_id, text, buttons, idempotency_key

        def answer_callback_query(
            self,
            *,
            callback_query_id: str,
            text: str | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del callback_query_id, text, idempotency_key

    class FakeCalendarClient:
        def query_free_busy(
            self,
            *,
            emails: tuple[str, ...],
            time_min: datetime,
            time_max: datetime,
        ) -> dict[str, list[tuple[datetime, datetime]]]:
            del time_min, time_max
            return {email: [] for email in emails}

        def insert_event(
            self,
            *,
            organizer_email: str,
            payload: dict[str, object],
            idempotency_key: str | None = None,
        ) -> str:
            del organizer_email, payload, idempotency_key
            return "evt-fake"

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

    worker = OutboxWorker(
        repository=repository,
        dispatcher=OutboxDispatcher(
            repository=repository,
            telegram_client=cast(Any, FakeTelegramClient()),
            calendar_client=FakeCalendarClient(),
        ),
    )

    tick = worker.run_once(now=now)
    assert tick.processed is True
    assert tick.status == OutboxStatus.DONE

    final_outbox = repository.claim_due_outbox(now=now)
    assert final_outbox is not None
    assert final_outbox.effect_type == OutboxEffectType.TELEGRAM_EDIT_MESSAGE
    assert final_outbox.payload.get("message_id") == 777
    assert final_outbox.idempotency_key == (
        f"cal_sync_final:{meeting.meeting_id}:r{meeting.confirmation_round}:confirmed:ok"
    )
    repository.close()


def test_calendar_patch_permanent_fail_sends_warning(tmp_path: Path) -> None:
    now = datetime(2026, 2, 12, 11, 0, 0)
    repository = _repo(tmp_path)
    repository.grant_manager_role(telegram_user_id=100, granted_by=None, now=now)
    repository.grant_manager_role(telegram_user_id=300, granted_by=100, now=now)
    _ = repository.enqueue_outbox(
        effect_type=OutboxEffectType.CALENDAR_PATCH_EVENT,
        payload={
            "google_event_id": "evt-final-fail",
            "initiator_google_email": "initiator@example.com",
            "payload": {"summary": "done"},
            "_post_patch_group_status": {
                "meeting_id": "m-5-final-fail",
                "round": 1,
                "target_state": "cancelled",
                "chat_id": -100777,
                "initiator_user_id": 100,
                "text": "❌ Встреча отменена",
            },
        },
        idempotency_key="cal:patch:final-fail",
        now=now,
    )

    class FakeTelegramClient:
        def send_message(
            self,
            *,
            telegram_user_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            keyboard: list[list[str]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, text, buttons, keyboard, idempotency_key

        def edit_message(
            self,
            *,
            telegram_user_id: int,
            message_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, message_id, text, buttons, idempotency_key

        def answer_callback_query(
            self,
            *,
            callback_query_id: str,
            text: str | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del callback_query_id, text, idempotency_key

    class FakeCalendarClient:
        def query_free_busy(
            self,
            *,
            emails: tuple[str, ...],
            time_min: datetime,
            time_max: datetime,
        ) -> dict[str, list[tuple[datetime, datetime]]]:
            del time_min, time_max
            return {email: [] for email in emails}

        def insert_event(
            self,
            *,
            organizer_email: str,
            payload: dict[str, object],
            idempotency_key: str | None = None,
        ) -> str:
            del organizer_email, payload, idempotency_key
            return "evt-fake"

        def patch_event(
            self,
            *,
            google_event_id: str,
            initiator_google_email: str,
            payload: dict[str, object],
            idempotency_key: str | None = None,
        ) -> None:
            del google_event_id, initiator_google_email, payload, idempotency_key
            raise RuntimeError("Calendar patch non-retryable status 400: bad request")

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

    worker = OutboxWorker(
        repository=repository,
        dispatcher=OutboxDispatcher(
            repository=repository,
            telegram_client=cast(Any, FakeTelegramClient()),
            calendar_client=FakeCalendarClient(),
        ),
    )

    tick = worker.run_once(now=now)
    assert tick.processed is True
    assert tick.status == OutboxStatus.FAILED

    recipients: set[int] = set()
    while True:
        outbox = repository.claim_due_outbox(now=now)
        if outbox is None:
            break
        if outbox.effect_type != OutboxEffectType.TELEGRAM_SEND_MESSAGE:
            continue
        key = outbox.idempotency_key
        if not isinstance(key, str) or ":fail:" not in key:
            continue
        recipient_obj = outbox.payload.get("telegram_user_id")
        if isinstance(recipient_obj, int):
            recipients.add(recipient_obj)

    assert recipients == {100, 300, -100777}
    repository.close()


def test_running_stale_participant_dm_is_skipped_by_dispatch_guard(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 12, 11, 0, 0)
    repository = _repo(tmp_path)
    meeting = Meeting(
        meeting_id="m-5-stale-dm-guard",
        initiator_telegram_user_id=100,
        chat_id=-100500,
        state=MeetingState.PENDING,
        scheduled_start_at=now + timedelta(hours=2),
        scheduled_end_at=now + timedelta(hours=3),
        confirmation_deadline_at=now + timedelta(minutes=20),
        participants=(
            MeetingParticipant(telegram_user_id=100, is_required=False),
            MeetingParticipant(telegram_user_id=200, is_required=True),
        ),
    )
    repository.insert_meeting(meeting, now=now)

    _ = repository.enqueue_outbox(
        effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
        payload={
            "telegram_user_id": 300,
            "text": "Подтвердите участие",
            "buttons": [
                {"text": "✅", "callback_data": "act:x"},
                {"text": "❌", "callback_data": "act:y"},
            ],
            "_meeting_id": meeting.meeting_id,
            "_meeting_round": meeting.confirmation_round,
            "_participant_user_id": 300,
            "_pending_participant_request": True,
        },
        idempotency_key=(
            f"notify:{meeting.meeting_id}:r{meeting.confirmation_round}:pending:participant:300"
        ),
        now=now,
    )

    sent_to: list[int] = []

    class FakeTelegramClient:
        def send_message(
            self,
            *,
            telegram_user_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            keyboard: list[list[str]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del text, buttons, keyboard, idempotency_key
            sent_to.append(telegram_user_id)

        def edit_message(
            self,
            *,
            telegram_user_id: int,
            message_id: int,
            text: str,
            buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del telegram_user_id, message_id, text, buttons, idempotency_key

        def answer_callback_query(
            self,
            *,
            callback_query_id: str,
            text: str | None = None,
            idempotency_key: str | None = None,
        ) -> None:
            del callback_query_id, text, idempotency_key

    class FakeCalendarClient:
        def query_free_busy(
            self,
            *,
            emails: tuple[str, ...],
            time_min: datetime,
            time_max: datetime,
        ) -> dict[str, list[tuple[datetime, datetime]]]:
            del time_min, time_max
            return {email: [] for email in emails}

        def insert_event(
            self,
            *,
            organizer_email: str,
            payload: dict[str, object],
            idempotency_key: str | None = None,
        ) -> str:
            del organizer_email, payload, idempotency_key
            return "evt-fake"

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

    worker = OutboxWorker(
        repository=repository,
        dispatcher=OutboxDispatcher(
            repository=repository,
            telegram_client=cast(Any, FakeTelegramClient()),
            calendar_client=FakeCalendarClient(),
        ),
    )

    tick = worker.run_once(now=now)
    assert tick.processed is True
    assert tick.status == OutboxStatus.DONE
    assert sent_to == []
    repository.close()
