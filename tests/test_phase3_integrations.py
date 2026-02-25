from __future__ import annotations
from unittest.mock import MagicMock

from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from bot_vstrechi.domain import (
    CallbackActionToken,
    CallbackActionType,
    Decision,
    InboundEventSource,
    JobStatus,
    JobType,
    Meeting,
    MeetingParticipant,
    MeetingState,
    OutboxEffectType,
    OutboxStatus,
    Outcome,
    ReasonCode,
    ScheduledJobSpec,
)
from bot_vstrechi.calendar.gateway import GoogleCalendarGateway
from bot_vstrechi.db.repository import SQLiteRepository
from bot_vstrechi.application.service import MeetingWorkflowService
from bot_vstrechi.telegram.adapter import STALE_ACTION_MESSAGE, TelegramWebhookAdapter
from bot_vstrechi.workers.scheduler import SchedulerWorker


def _repo(tmp_path: Path) -> SQLiteRepository:
    repository = SQLiteRepository(str(tmp_path / "bot_vstrechi_phase3.db"))
    repository.initialize_schema()
    return repository


def _pending_meeting(now: datetime, *, round: int = 1) -> Meeting:
    return Meeting(
        meeting_id="m-3",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.PENDING,
        scheduled_start_at=now + timedelta(hours=1),
        scheduled_end_at=now + timedelta(hours=2),
        confirmation_deadline_at=now + timedelta(minutes=20),
        confirmation_round=round,
        participants=(
            MeetingParticipant(
                telegram_user_id=100,
                is_required=False,
                decision=Decision.NONE,
            ),
            MeetingParticipant(
                telegram_user_id=200,
                is_required=True,
                decision=Decision.NONE,
            ),
        ),
    )


def test_webhook_dedup_duplicate_callback_query_id(tmp_path: Path) -> None:
    now = datetime(2026, 2, 11, 16, 0, 0)
    repository = _repo(tmp_path)
    meeting = _pending_meeting(now)
    repository.insert_meeting(meeting, now=now)

    token = CallbackActionToken(
        token="tok-confirm-1",
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        action_type=CallbackActionType.PARTICIPANT_CONFIRM,
        allowed_user_id=200,
        expires_at=now + timedelta(minutes=5),
    )
    repository.upsert_callback_action_token(callback_token=token, now=now)

    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())
    adapter = TelegramWebhookAdapter(repository=repository, workflow_service=service)

    callback_update = {
        "update_id": 10,
        "callback_query": {
            "id": "cb-dup-1",
            "from": {"id": 200},
            "data": "act:tok-confirm-1",
        },
    }

    first = adapter.handle_update(update=callback_update, now=now)
    second = adapter.handle_update(
        update=callback_update, now=now + timedelta(seconds=1)
    )

    assert first.outcome == Outcome.OK
    assert second.outcome == Outcome.NOOP
    assert second.reason_code == ReasonCode.DUPLICATE_INBOUND_EVENT
    assert second.message == "Повторное обновление проигнорировано"
    repository.close()


def test_webhook_stale_callback_round_mismatch(tmp_path: Path) -> None:
    now = datetime(2026, 2, 11, 16, 0, 0)
    repository = _repo(tmp_path)
    meeting = _pending_meeting(now, round=2)
    repository.insert_meeting(meeting, now=now)

    stale_token = CallbackActionToken(
        token="tok-stale-round",
        meeting_id=meeting.meeting_id,
        round=1,
        action_type=CallbackActionType.PARTICIPANT_CONFIRM,
        allowed_user_id=200,
        expires_at=now + timedelta(minutes=5),
    )
    repository.upsert_callback_action_token(callback_token=stale_token, now=now)

    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())
    adapter = TelegramWebhookAdapter(repository=repository, workflow_service=service)

    result = adapter.handle_update(
        update={
            "update_id": 20,
            "callback_query": {
                "id": "cb-stale-1",
                "from": {"id": 200},
                "data": "act:tok-stale-round",
            },
        },
        now=now,
    )

    reloaded = repository.get_meeting(meeting.meeting_id)
    assert result.outcome == Outcome.NOOP
    assert result.reason_code == ReasonCode.STALE_ACTION
    assert result.message == STALE_ACTION_MESSAGE
    assert reloaded is not None
    assert reloaded.state == MeetingState.PENDING
    assert reloaded.confirmation_round == 2
    repository.close()


def test_removed_participant_pending_callback_is_rejected(tmp_path: Path) -> None:
    now = datetime(2026, 2, 11, 16, 0, 0)
    repository = _repo(tmp_path)
    meeting = _pending_meeting(now)
    repository.insert_meeting(meeting, now=now)

    stale_token = CallbackActionToken(
        token="tok-removed-user",
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        action_type=CallbackActionType.PARTICIPANT_CONFIRM,
        allowed_user_id=300,
        expires_at=now + timedelta(minutes=5),
    )
    repository.upsert_callback_action_token(callback_token=stale_token, now=now)

    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())
    adapter = TelegramWebhookAdapter(repository=repository, workflow_service=service)

    result = adapter.handle_update(
        update={
            "update_id": 20_001,
            "callback_query": {
                "id": "cb-removed-user",
                "from": {"id": 300},
                "data": "act:tok-removed-user",
            },
        },
        now=now,
    )

    assert result.outcome == Outcome.NOOP
    assert result.reason_code == ReasonCode.STALE_ACTION
    assert result.message == STALE_ACTION_MESSAGE
    repository.close()


def test_duplicate_callback_still_enqueues_callback_answer(tmp_path: Path) -> None:
    now = datetime(2026, 2, 11, 16, 0, 0)
    repository = _repo(tmp_path)

    _ = repository.register_inbound_event(
        source=InboundEventSource.TELEGRAM_CALLBACK,
        external_event_id="cb-dup-answer",
        received_at=now,
    )

    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())
    adapter = TelegramWebhookAdapter(repository=repository, workflow_service=service)

    result = adapter.handle_update(
        update={
            "update_id": 999,
            "callback_query": {
                "id": "cb-dup-answer",
                "from": {"id": 200},
                "data": "act:any-token",
            },
        },
        now=now + timedelta(seconds=1),
    )

    assert result.outcome == Outcome.NOOP
    assert result.reason_code == ReasonCode.DUPLICATE_INBOUND_EVENT
    outbox = repository.claim_due_outbox(now=now + timedelta(seconds=1))
    assert outbox is not None
    assert outbox.effect_type == OutboxEffectType.TELEGRAM_ANSWER_CALLBACK
    assert outbox.payload.get("callback_query_id") == "cb-dup-answer"
    repository.close()


def test_participant_confirm_callback_updates_message_text_after_cleanup(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 11, 16, 0, 0)
    repository = _repo(tmp_path)
    meeting = _pending_meeting(now)
    repository.insert_meeting(meeting, now=now)

    token = CallbackActionToken(
        token="tok-confirm-cleanup",
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        action_type=CallbackActionType.PARTICIPANT_CONFIRM,
        allowed_user_id=200,
        expires_at=now + timedelta(minutes=5),
    )
    repository.upsert_callback_action_token(callback_token=token, now=now)

    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())
    adapter = TelegramWebhookAdapter(repository=repository, workflow_service=service)

    result = adapter.handle_update(
        update={
            "update_id": 21,
            "callback_query": {
                "id": "cb-cleanup-1",
                "from": {"id": 200},
                "data": "act:tok-confirm-cleanup",
                "message": {
                    "message_id": 88,
                    "chat": {"id": 200},
                    "text": (
                        "🗳️ Нужен ваш ответ по встрече.\n"
                        "«Планерка»\n"
                        "Когда: Ср, 11 февраля 17:00-18:00\n"
                        "ID: m-3\n"
                        "Выберите действие:"
                    ),
                },
            },
        },
        now=now,
    )

    assert result.outcome == Outcome.OK
    assert result.reason_code == ReasonCode.UPDATED
    assert result.message == "Готово, отметили ваше участие ✅"

    cleanup_message = None
    while True:
        outbox = repository.claim_due_outbox(now=now)
        if outbox is None:
            break
        if outbox.effect_type == OutboxEffectType.TELEGRAM_EDIT_MESSAGE:
            cleanup_message = outbox
            break

    assert cleanup_message is not None
    assert cleanup_message.payload.get("text") == "Готово, отметили ваше участие ✅"
    assert cleanup_message.payload.get("buttons") == []
    repository.close()


def test_cancelled_meeting_participant_callback_cleans_up_buttons(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 11, 16, 0, 0)
    repository = _repo(tmp_path)
    meeting = replace(_pending_meeting(now), state=MeetingState.CANCELLED)
    repository.insert_meeting(meeting, now=now)

    token = CallbackActionToken(
        token="tok-cancelled-cleanup",
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        action_type=CallbackActionType.PARTICIPANT_CONFIRM,
        allowed_user_id=200,
        expires_at=now + timedelta(minutes=5),
    )
    repository.upsert_callback_action_token(callback_token=token, now=now)

    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())
    adapter = TelegramWebhookAdapter(repository=repository, workflow_service=service)

    result = adapter.handle_update(
        update={
            "update_id": 22,
            "callback_query": {
                "id": "cb-cleanup-cancelled",
                "from": {"id": 200},
                "data": "act:tok-cancelled-cleanup",
                "message": {
                    "message_id": 91,
                    "chat": {"id": 200},
                    "text": (
                        "Подтвердите участие, пожалуйста.\n"
                        "«Планерка»\n"
                        "Когда: Ср, 11 февраля 17:00-18:00\n"
                        "Выберите действие:"
                    ),
                },
            },
        },
        now=now,
    )

    assert result.outcome == Outcome.NOOP
    assert result.reason_code == ReasonCode.INVALID_STATE
    assert "Встреча уже отменена" in result.message

    cleanup_message = None
    while True:
        outbox = repository.claim_due_outbox(now=now)
        if outbox is None:
            break
        if outbox.effect_type == OutboxEffectType.TELEGRAM_EDIT_MESSAGE:
            cleanup_message = outbox
            break

    assert cleanup_message is not None
    assert cleanup_message.payload.get("message_id") == 91
    assert cleanup_message.payload.get("text") == result.message
    assert cleanup_message.payload.get("buttons") == []
    repository.close()


@pytest.mark.parametrize(
    (
        "meeting_state",
        "token_value",
        "callback_id",
        "message_id",
        "expected_state_word",
    ),
    (
        (
            MeetingState.CONFIRMED,
            "tok-confirmed-cleanup",
            "cb-cleanup-confirmed",
            92,
            "подтверждена",
        ),
        (
            MeetingState.EXPIRED,
            "tok-expired-cleanup",
            "cb-cleanup-expired",
            93,
            "истекла",
        ),
    ),
)
def test_finalized_meeting_participant_callback_cleans_up_buttons(
    tmp_path: Path,
    meeting_state: MeetingState,
    token_value: str,
    callback_id: str,
    message_id: int,
    expected_state_word: str,
) -> None:
    now = datetime(2026, 2, 11, 16, 0, 0)
    repository = _repo(tmp_path)
    meeting = replace(_pending_meeting(now), state=meeting_state)
    repository.insert_meeting(meeting, now=now)

    token = CallbackActionToken(
        token=token_value,
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        action_type=CallbackActionType.PARTICIPANT_CONFIRM,
        allowed_user_id=200,
        expires_at=now + timedelta(minutes=5),
    )
    repository.upsert_callback_action_token(callback_token=token, now=now)

    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())
    adapter = TelegramWebhookAdapter(repository=repository, workflow_service=service)

    result = adapter.handle_update(
        update={
            "update_id": message_id,
            "callback_query": {
                "id": callback_id,
                "from": {"id": 200},
                "data": f"act:{token_value}",
                "message": {
                    "message_id": message_id,
                    "chat": {"id": 200},
                    "text": (
                        "Подтвердите участие, пожалуйста.\n"
                        "«Планерка»\n"
                        "Когда: Ср, 11 февраля 17:00-18:00\n"
                        "Выберите действие:"
                    ),
                },
            },
        },
        now=now,
    )

    assert result.outcome == Outcome.NOOP
    assert result.reason_code in (
        ReasonCode.INVALID_STATE,
        ReasonCode.LATE_RESPONSE_RECORDED,
        ReasonCode.ALREADY_FINAL,
    )
    assert any(
        phrase in result.message.lower()
        for phrase in ("не нужен", "не требуется", expected_state_word)
    )

    cleanup_message = None
    while True:
        outbox = repository.claim_due_outbox(now=now)
        if outbox is None:
            break
        if outbox.effect_type == OutboxEffectType.TELEGRAM_EDIT_MESSAGE:
            cleanup_message = outbox
            break

    assert cleanup_message is not None
    assert cleanup_message.payload.get("message_id") == message_id
    assert cleanup_message.payload.get("buttons") == []
    repository.close()


def test_webhook_retry_same_update_after_handler_exception(tmp_path: Path) -> None:
    now = datetime(2026, 2, 11, 16, 0, 0)
    repository = _repo(tmp_path)
    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    def exploding_enqueue_outbox(*args: object, **kwargs: object) -> str:
        del args, kwargs
        raise RuntimeError("boom")

    setattr(repository, "enqueue_outbox", exploding_enqueue_outbox)
    adapter = TelegramWebhookAdapter(
        repository=repository,
        workflow_service=service,
    )
    update = {
        "update_id": 999,
        "message": {
            "text": "/help",
            "from": {"id": 100},
            "chat": {"id": 100},
        },
    }

    with pytest.raises(RuntimeError, match="boom"):
        _ = adapter.handle_update(update=update, now=now)

    with pytest.raises(RuntimeError, match="boom"):
        _ = adapter.handle_update(update=update, now=now + timedelta(seconds=1))

    repository.close()


def test_calendar_gateway_blocks_non_bot_created_meeting() -> None:
    now = datetime(2026, 2, 11, 16, 0, 0)
    meeting = Meeting(
        meeting_id="m-calendar-1",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.PENDING,
        scheduled_start_at=now + timedelta(hours=1),
        scheduled_end_at=now + timedelta(hours=2),
        created_by_bot=False,
        participants=(),
    )

    class FakeCalendarClient:
        def __init__(self) -> None:
            self.called: bool = False

        def patch_event(
            self,
            *,
            google_event_id: str,
            initiator_google_email: str,
            payload: dict[str, object],
            idempotency_key: str | None = None,
        ) -> None:
            del google_event_id, initiator_google_email, payload, idempotency_key
            self.called = True

    client = FakeCalendarClient()
    gateway = GoogleCalendarGateway(api_client=cast(Any, client))
    result = gateway.patch_event_for_meeting(
        meeting=meeting,
        google_event_id="evt-1",
        initiator_google_email="initiator@example.com",
        payload={"summary": "Updated"},
    )

    assert result.outcome == Outcome.REJECTED
    assert result.reason_code == ReasonCode.NOT_BOT_CREATED_EVENT
    assert client.called is False


def test_restart_safe_job_recovery_processes_stale_running_job(tmp_path: Path) -> None:
    now = datetime(2026, 2, 11, 16, 0, 0)
    repository = _repo(tmp_path)
    meeting = _pending_meeting(now)
    meeting = replace(meeting, confirmation_deadline_at=now - timedelta(seconds=1))
    repository.insert_meeting(meeting, now=now)
    repository.enqueue_jobs(
        (
            ScheduledJobSpec(
                job_type=JobType.CONFIRM_DEADLINE,
                meeting_id=meeting.meeting_id,
                round=meeting.confirmation_round,
                run_at=now - timedelta(seconds=1),
            ),
        ),
        now=now,
    )

    claimed = repository.claim_due_job(now=now)
    assert claimed is not None
    assert (
        repository.count_jobs(
            meeting_id=meeting.meeting_id,
            status=JobStatus.RUNNING,
            job_type=JobType.CONFIRM_DEADLINE,
        )
        == 1
    )

    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())
    worker = SchedulerWorker(repository=repository, service=service)
    recovered = worker.reconcile_on_startup(now=now + timedelta(minutes=6))
    assert recovered == 1

    tick = worker.run_once(now=now + timedelta(minutes=6))
    updated = repository.get_meeting(meeting.meeting_id)

    assert tick.processed is True
    assert updated is not None
    assert updated.state == MeetingState.NEEDS_INITIATOR_DECISION
    repository.close()


def test_initiator_replan_callback_requests_calendar_replan(tmp_path: Path) -> None:
    now = datetime(2026, 2, 11, 16, 0, 0)
    repository = _repo(tmp_path)
    meeting = replace(
        _pending_meeting(now),
        state=MeetingState.NEEDS_INITIATOR_DECISION,
        initiator_decision_deadline_at=now + timedelta(minutes=10),
    )
    repository.insert_meeting(meeting, now=now)

    token = CallbackActionToken(
        token="tok-replan-1",
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        action_type=CallbackActionType.INITIATOR_REPLAN,
        allowed_user_id=meeting.initiator_telegram_user_id,
        expires_at=now + timedelta(minutes=5),
    )
    repository.upsert_callback_action_token(callback_token=token, now=now)

    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())
    adapter = TelegramWebhookAdapter(repository=repository, workflow_service=service)

    result = adapter.handle_update(
        update={
            "update_id": 21,
            "callback_query": {
                "id": "cb-replan-1",
                "from": {"id": meeting.initiator_telegram_user_id},
                "data": "act:tok-replan-1",
            },
        },
        now=now,
    )

    assert result.outcome == Outcome.OK
    assert result.reason_code == ReasonCode.UPDATED
    assert "Google Calendar" in result.message
    assert repository.count_outbox(status=OutboxStatus.PENDING) == 1
    outbox = repository.claim_due_outbox(now=now)
    assert outbox is not None
    assert outbox.effect_type == OutboxEffectType.TELEGRAM_ANSWER_CALLBACK
    repository.close()


def test_participant_confirm_callback_in_needs_initiator_decision_confirms(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 11, 16, 0, 0)
    repository = _repo(tmp_path)
    meeting = Meeting(
        meeting_id="m-3-late-confirm",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.NEEDS_INITIATOR_DECISION,
        scheduled_start_at=now + timedelta(hours=1),
        scheduled_end_at=now + timedelta(hours=2),
        confirmation_deadline_at=now - timedelta(minutes=5),
        confirmation_round=1,
        initiator_decision_deadline_at=now + timedelta(minutes=10),
        participants=(
            MeetingParticipant(
                telegram_user_id=100,
                is_required=False,
                decision=Decision.CONFIRM,
                decision_received_at=now - timedelta(minutes=20),
            ),
            MeetingParticipant(
                telegram_user_id=200,
                is_required=True,
                decision=Decision.NONE,
            ),
        ),
    )
    repository.insert_meeting(meeting, now=now)

    token = CallbackActionToken(
        token="tok-needs-confirm",
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        action_type=CallbackActionType.PARTICIPANT_CONFIRM,
        allowed_user_id=200,
        expires_at=now + timedelta(minutes=5),
    )
    repository.upsert_callback_action_token(callback_token=token, now=now)

    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())
    adapter = TelegramWebhookAdapter(repository=repository, workflow_service=service)

    result = adapter.handle_update(
        update={
            "update_id": 29,
            "callback_query": {
                "id": "cb-needs-confirm",
                "from": {"id": 200},
                "data": "act:tok-needs-confirm",
            },
        },
        now=now,
    )

    assert result.outcome == Outcome.OK
    assert result.reason_code == ReasonCode.UPDATED
    updated = repository.get_meeting(meeting.meeting_id)
    assert updated is not None
    assert updated.state == MeetingState.CONFIRMED
    repository.close()


def test_manager_decision_conflict_first_valid_wins(tmp_path: Path) -> None:
    now = datetime(2026, 2, 11, 16, 0, 0)
    repository = _repo(tmp_path)
    repository.grant_manager_role(telegram_user_id=100, granted_by=None, now=now)
    repository.grant_manager_role(telegram_user_id=300, granted_by=100, now=now)

    meeting = Meeting(
        meeting_id="m-3-manager-conflict",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.NEEDS_INITIATOR_DECISION,
        scheduled_start_at=now + timedelta(hours=1),
        scheduled_end_at=now + timedelta(hours=2),
        confirmation_round=2,
        initiator_decision_deadline_at=now + timedelta(minutes=15),
        participants=(
            MeetingParticipant(telegram_user_id=100, is_required=False),
            MeetingParticipant(
                telegram_user_id=200, is_required=True, decision=Decision.CANCEL
            ),
        ),
    )
    repository.insert_meeting(meeting, now=now)

    cancel_token = CallbackActionToken(
        token="tok-manager-cancel",
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        action_type=CallbackActionType.INITIATOR_CANCEL,
        allowed_user_id=300,
        expires_at=now + timedelta(minutes=5),
    )
    proceed_token = CallbackActionToken(
        token="tok-manager-proceed",
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        action_type=CallbackActionType.INITIATOR_PROCEED_WITHOUT_SUBSET,
        allowed_user_id=300,
        expires_at=now + timedelta(minutes=5),
    )
    repository.upsert_callback_action_token(callback_token=cancel_token, now=now)
    repository.upsert_callback_action_token(callback_token=proceed_token, now=now)

    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())
    adapter = TelegramWebhookAdapter(repository=repository, workflow_service=service)

    first = adapter.handle_update(
        update={
            "update_id": 31,
            "callback_query": {
                "id": "cb-manager-1",
                "from": {"id": 300},
                "data": "act:tok-manager-cancel",
            },
        },
        now=now,
    )
    second = adapter.handle_update(
        update={
            "update_id": 32,
            "callback_query": {
                "id": "cb-manager-2",
                "from": {"id": 300},
                "data": "act:tok-manager-proceed",
            },
        },
        now=now + timedelta(seconds=1),
    )

    assert first.outcome == Outcome.OK
    assert second.outcome == Outcome.NOOP
    assert second.reason_code == ReasonCode.INVALID_STATE
    updated = repository.get_meeting(meeting.meeting_id)
    assert updated is not None
    assert updated.state == MeetingState.CANCELLED
    repository.close()
