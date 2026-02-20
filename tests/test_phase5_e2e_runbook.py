from __future__ import annotations
from unittest.mock import MagicMock

from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

from bot_vstrechi.telegram.callback_tokens import CallbackTokenService
from bot_vstrechi.domain import (
    Decision,
    JobStatus,
    JobType,
    Meeting,
    MeetingParticipant,
    MeetingState,
    Outcome,
    ReasonCode,
    ScheduledJobSpec,
)
from bot_vstrechi.domain.policies import DEADLINE_GRACE_WINDOW, URGENT_CONFIRM_BUFFER
from bot_vstrechi.db.repository import SQLiteRepository
from bot_vstrechi.application.service import MeetingWorkflowService
from bot_vstrechi.telegram.adapter import TelegramWebhookAdapter
from bot_vstrechi.workers.scheduler import SchedulerWorker


def _repo(tmp_path: Path) -> SQLiteRepository:
    repository = SQLiteRepository(str(tmp_path / "bot_vstrechi_phase5_e2e.db"))
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
        confirmation_deadline_at=now + timedelta(minutes=20),
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
            MeetingParticipant(
                telegram_user_id=300,
                is_required=True,
                decision=Decision.NONE,
            ),
        ),
    )


def _callback_update(
    *, callback_id: str, actor_user_id: int, callback_data: str
) -> dict[str, object]:
    return {
        "update_id": int(callback_id.split("-")[-1]),
        "callback_query": {
            "id": callback_id,
            "from": {"id": actor_user_id},
            "data": callback_data,
        },
    }


def test_runbook_happy_path_all_required_confirm(tmp_path: Path) -> None:
    now = datetime(2026, 2, 12, 12, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now, meeting_id="rb-happy")
    repository.insert_meeting(meeting, now=now)

    workflow = MeetingWorkflowService(repository, calendar_gateway=MagicMock())
    adapter = TelegramWebhookAdapter(repository=repository, workflow_service=workflow)
    tokens = CallbackTokenService(repository)
    confirm_200, _ = tokens.build_participant_decision_buttons(
        meeting=meeting,
        participant_user_id=200,
        now=now,
    )
    confirm_300, _ = tokens.build_participant_decision_buttons(
        meeting=meeting,
        participant_user_id=300,
        now=now,
    )

    first = adapter.handle_update(
        update=_callback_update(
            callback_id="cb-1001",
            actor_user_id=200,
            callback_data=confirm_200.callback_data,
        ),
        now=now,
    )
    second = adapter.handle_update(
        update=_callback_update(
            callback_id="cb-1002",
            actor_user_id=300,
            callback_data=confirm_300.callback_data,
        ),
        now=now + timedelta(seconds=5),
    )

    updated = repository.get_meeting(meeting.meeting_id)
    assert first.outcome == Outcome.OK
    assert second.outcome == Outcome.OK
    assert updated is not None
    assert updated.state == MeetingState.CONFIRMED
    repository.close()


def test_runbook_decline_path_cancels_meeting(tmp_path: Path) -> None:
    now = datetime(2026, 2, 12, 12, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now, meeting_id="rb-decline")
    repository.insert_meeting(meeting, now=now)

    workflow = MeetingWorkflowService(repository, calendar_gateway=MagicMock())
    adapter = TelegramWebhookAdapter(repository=repository, workflow_service=workflow)
    tokens = CallbackTokenService(repository)
    _, cancel_200 = tokens.build_participant_decision_buttons(
        meeting=meeting,
        participant_user_id=200,
        now=now,
    )

    result = adapter.handle_update(
        update=_callback_update(
            callback_id="cb-2001",
            actor_user_id=200,
            callback_data=cancel_200.callback_data,
        ),
        now=now,
    )

    updated = repository.get_meeting(meeting.meeting_id)
    assert result.outcome == Outcome.OK
    assert updated is not None
    assert updated.state == MeetingState.NEEDS_INITIATOR_DECISION
    repository.close()


def test_runbook_no_response_path_moves_to_needs_initiator_decision(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 12, 12, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now, meeting_id="rb-no-response")
    meeting = replace(
        meeting,
        participants=(
            MeetingParticipant(
                telegram_user_id=100,
                is_required=False,
                decision=Decision.NONE,
            ),
            MeetingParticipant(
                telegram_user_id=200,
                is_required=True,
                decision=Decision.CONFIRM,
                decision_received_at=now - timedelta(minutes=1),
            ),
            MeetingParticipant(
                telegram_user_id=300,
                is_required=True,
                decision=Decision.NONE,
            ),
        ),
        confirmation_deadline_at=now - DEADLINE_GRACE_WINDOW - timedelta(seconds=1),
    )
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

    worker = SchedulerWorker(
        repository=repository,
        service=MeetingWorkflowService(repository, calendar_gateway=MagicMock()),
    )
    tick = worker.run_once(now=now)
    updated = repository.get_meeting(meeting.meeting_id)

    assert tick.processed is True
    assert updated is not None
    assert updated.state == MeetingState.NEEDS_INITIATOR_DECISION
    repository.close()


def test_runbook_proceed_without_subset_path(tmp_path: Path) -> None:
    now = datetime(2026, 2, 12, 12, 0, 0)
    repository = _repo(tmp_path)
    meeting = replace(
        _meeting(now, meeting_id="rb-proceed"),
        state=MeetingState.NEEDS_INITIATOR_DECISION,
        initiator_decision_deadline_at=now + timedelta(minutes=10),
    )
    repository.insert_meeting(meeting, now=now)

    workflow = MeetingWorkflowService(repository, calendar_gateway=MagicMock())
    adapter = TelegramWebhookAdapter(repository=repository, workflow_service=workflow)
    token_service = CallbackTokenService(repository)
    _, _, proceed_button = token_service.build_initiator_decision_buttons(
        meeting=meeting,
        now=now,
    )

    result = adapter.handle_update(
        update=_callback_update(
            callback_id="cb-3001",
            actor_user_id=meeting.initiator_telegram_user_id,
            callback_data=proceed_button.callback_data,
        ),
        now=now,
    )

    updated = repository.get_meeting(meeting.meeting_id)
    assert result.outcome == Outcome.OK
    assert updated is not None
    assert updated.state == MeetingState.CONFIRMED
    repository.close()


def test_runbook_urgent_window(tmp_path: Path) -> None:
    now = datetime(2026, 2, 12, 12, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now, meeting_id="rb-fast-track")
    repository.insert_meeting(meeting, now=now)
    workflow = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    execution = workflow.select_slot(
        meeting_id=meeting.meeting_id,
        actor_user_id=meeting.initiator_telegram_user_id,
        chat_id=meeting.initiator_telegram_user_id,
        scheduled_start_at=now + timedelta(minutes=40),
        scheduled_end_at=now + timedelta(minutes=70),
        now=now,
    )

    assert execution.result.outcome == Outcome.OK
    assert execution.meeting.state == MeetingState.PENDING
    assert (
        execution.meeting.confirmation_deadline_at
        == now + timedelta(minutes=40) - URGENT_CONFIRM_BUFFER
    )
    repository.close()


def test_runbook_less_than_10_minutes_goes_directly_to_needs_initiator_decision(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 12, 12, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now, meeting_id="rb-lt3m")
    repository.insert_meeting(meeting, now=now)
    workflow = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    execution = workflow.select_slot(
        meeting_id=meeting.meeting_id,
        actor_user_id=meeting.initiator_telegram_user_id,
        chat_id=meeting.initiator_telegram_user_id,
        scheduled_start_at=now + timedelta(minutes=8),
        scheduled_end_at=now + timedelta(minutes=38),
        now=now,
    )

    assert execution.result.outcome == Outcome.OK
    assert execution.meeting.state == MeetingState.NEEDS_INITIATOR_DECISION
    assert execution.meeting.initiator_decision_deadline_at == now + timedelta(
        minutes=15
    )
    repository.close()


def test_runbook_initiator_timeout_path(tmp_path: Path) -> None:
    now = datetime(2026, 2, 12, 12, 0, 0)
    repository = _repo(tmp_path)
    meeting = replace(
        _meeting(now, meeting_id="rb-timeout"),
        state=MeetingState.NEEDS_INITIATOR_DECISION,
        confirmation_deadline_at=None,
        initiator_decision_deadline_at=now - timedelta(seconds=1),
    )
    repository.insert_meeting(meeting, now=now)
    repository.enqueue_jobs(
        (
            ScheduledJobSpec(
                job_type=JobType.INITIATOR_TIMEOUT,
                meeting_id=meeting.meeting_id,
                round=meeting.confirmation_round,
                run_at=now - timedelta(seconds=1),
            ),
        ),
        now=now,
    )

    worker = SchedulerWorker(
        repository=repository,
        service=MeetingWorkflowService(repository, calendar_gateway=MagicMock()),
    )
    tick = worker.run_once(now=now)
    updated = repository.get_meeting(meeting.meeting_id)

    assert tick.processed is True
    assert updated is not None
    assert updated.state == MeetingState.CANCELLED
    repository.close()


def test_runbook_duplicate_callback_path(tmp_path: Path) -> None:
    now = datetime(2026, 2, 12, 12, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now, meeting_id="rb-duplicate")
    repository.insert_meeting(meeting, now=now)

    workflow = MeetingWorkflowService(repository, calendar_gateway=MagicMock())
    adapter = TelegramWebhookAdapter(repository=repository, workflow_service=workflow)
    tokens = CallbackTokenService(repository)
    confirm_200, _ = tokens.build_participant_decision_buttons(
        meeting=meeting,
        participant_user_id=200,
        now=now,
    )
    update = _callback_update(
        callback_id="cb-4001",
        actor_user_id=200,
        callback_data=confirm_200.callback_data,
    )

    first = adapter.handle_update(update=update, now=now)
    second = adapter.handle_update(update=update, now=now + timedelta(seconds=1))

    assert first.outcome == Outcome.OK
    assert second.outcome == Outcome.NOOP
    assert second.reason_code == ReasonCode.DUPLICATE_INBOUND_EVENT
    repository.close()


def test_runbook_restart_recovery_path(tmp_path: Path) -> None:
    now = datetime(2026, 2, 12, 12, 0, 0)
    repository = _repo(tmp_path)
    meeting = replace(
        _meeting(now, meeting_id="rb-restart"),
        confirmation_deadline_at=now - timedelta(seconds=1),
    )
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

    worker = SchedulerWorker(
        repository=repository,
        service=MeetingWorkflowService(repository, calendar_gateway=MagicMock()),
    )
    recovered = worker.reconcile_on_startup(now=now + timedelta(minutes=6))
    tick = worker.run_once(now=now + timedelta(minutes=6))

    updated = repository.get_meeting(meeting.meeting_id)
    assert recovered == 1
    assert tick.processed is True
    assert updated is not None
    assert updated.state == MeetingState.NEEDS_INITIATOR_DECISION
    repository.close()
