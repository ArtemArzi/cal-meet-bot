from __future__ import annotations
from unittest.mock import MagicMock

from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

from bot_vstrechi.domain import (
    Decision,
    JobStatus,
    JobType,
    Meeting,
    MeetingParticipant,
    MeetingState,
    OutboxEffectType,
    OutboxStatus,
    Outcome,
    ScheduledJobSpec,
)
from bot_vstrechi.db.repository import SQLiteRepository
from bot_vstrechi.application.service import MeetingWorkflowService
from bot_vstrechi.workers.scheduler import SchedulerWorker


def _repo(tmp_path: Path) -> SQLiteRepository:
    repository = SQLiteRepository(str(tmp_path / "bot_vstrechi_phase7_side_effects.db"))
    repository.initialize_schema()
    return repository


def _meeting(now: datetime, *, meeting_id: str = "m-7") -> Meeting:
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


def test_confirm_transition_enqueues_outbox_messages(tmp_path: Path) -> None:
    now = datetime(2026, 2, 13, 10, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now, meeting_id="m-7-confirm")
    repository.insert_meeting(meeting, now=now)
    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    _ = service.record_participant_decision(
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        actor_user_id=200,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now,
    )
    second = service.record_participant_decision(
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        actor_user_id=300,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now + timedelta(seconds=1),
    )

    assert second.result.outcome == Outcome.OK
    assert second.meeting.state == MeetingState.CONFIRMED
    assert (
        repository.count_outbox(
            status=OutboxStatus.PENDING,
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
        )
        == 1
    )
    claimed = repository.claim_due_outbox(now=now + timedelta(seconds=1))
    assert claimed is not None
    assert claimed.payload["telegram_user_id"] == second.meeting.chat_id
    repository.close()


def test_confirm_transition_calendar_patch_updates_description(tmp_path: Path) -> None:
    now = datetime(2026, 2, 13, 10, 0, 0)
    repository = _repo(tmp_path)
    repository.upsert_user_mapping(
        telegram_user_id=100,
        google_email="initiator@4sell.ai",
        now=now,
        telegram_username="initiator",
        timezone="UTC",
    )
    meeting = replace(
        _meeting(now, meeting_id="m-7-confirm-cal-description"),
        google_event_id="evt-7-confirm-cal-description",
    )
    repository.insert_meeting(meeting, now=now)
    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    _ = service.record_participant_decision(
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        actor_user_id=200,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now,
    )
    second = service.record_participant_decision(
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        actor_user_id=300,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now + timedelta(seconds=1),
    )

    assert second.result.outcome == Outcome.OK
    assert second.meeting.state == MeetingState.CONFIRMED

    patch_message = None
    while True:
        outbox = repository.claim_due_outbox(now=now + timedelta(seconds=1))
        if outbox is None:
            break
        if outbox.effect_type != OutboxEffectType.CALENDAR_PATCH_EVENT:
            continue
        if outbox.idempotency_key == (
            f"cal_patch:{second.meeting.meeting_id}:"
            f"r{second.meeting.confirmation_round}:{second.meeting.state}"
        ):
            patch_message = outbox
            break

    assert patch_message is not None
    payload_obj = patch_message.payload.get("payload")
    assert isinstance(payload_obj, dict)
    assert payload_obj.get("summary") == "✅ Встреча"
    assert payload_obj.get("description") == (
        f"Встреча подтверждена в Telegram.\nID: {second.meeting.meeting_id}"
    )
    assert payload_obj.get("status") == "confirmed"
    assert payload_obj.get("transparency") == "opaque"
    repository.close()


def test_pending_decision_enqueues_immediate_calendar_attendee_patch(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 13, 10, 0, 0)
    repository = _repo(tmp_path)
    repository.upsert_user_mapping(
        telegram_user_id=100,
        google_email="initiator@4sell.ai",
        now=now,
        telegram_username="initiator",
        timezone="UTC",
    )
    repository.upsert_user_mapping(
        telegram_user_id=200,
        google_email="petya@4sell.ai",
        now=now,
        telegram_username="petya",
        timezone="UTC",
    )
    meeting = replace(
        _meeting(now, meeting_id="m-7-calendar-attendee-sync"),
        google_event_id="evt-7-attendee-sync",
    )
    repository.insert_meeting(meeting, now=now)
    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    execution = service.record_participant_decision(
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        actor_user_id=200,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now,
    )

    assert execution.result.outcome == Outcome.OK
    assert execution.meeting.state == MeetingState.PENDING
    assert (
        repository.count_outbox(
            status=OutboxStatus.PENDING,
            effect_type=OutboxEffectType.CALENDAR_PATCH_EVENT,
        )
        == 1
    )

    patch_message = repository.claim_due_outbox(now=now)
    assert patch_message is not None
    assert patch_message.effect_type == OutboxEffectType.CALENDAR_PATCH_EVENT
    assert patch_message.payload.get("google_event_id") == "evt-7-attendee-sync"
    assert patch_message.payload.get("initiator_google_email") == "initiator@4sell.ai"

    payload_obj = patch_message.payload.get("payload")
    assert isinstance(payload_obj, dict)
    assert payload_obj.get("_send_updates") == "none"
    assert payload_obj.get("attendeesOmitted") is True
    assert payload_obj.get("attendees") == [
        {"email": "petya@4sell.ai", "responseStatus": "accepted"}
    ]
    assert payload_obj.get("transparency") == "opaque"
    repository.close()


def test_pending_cancel_without_confirmed_participants_keeps_transparency_transparent(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 13, 10, 0, 0)
    repository = _repo(tmp_path)
    repository.upsert_user_mapping(
        telegram_user_id=100,
        google_email="initiator@4sell.ai",
        now=now,
        telegram_username="initiator",
        timezone="UTC",
    )
    repository.upsert_user_mapping(
        telegram_user_id=200,
        google_email="petya@4sell.ai",
        now=now,
        telegram_username="petya",
        timezone="UTC",
    )
    meeting = replace(
        _meeting(now, meeting_id="m-7-calendar-attendee-cancel-transparent"),
        google_event_id="evt-7-attendee-cancel-transparent",
    )
    repository.insert_meeting(meeting, now=now)
    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    execution = service.record_participant_decision(
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        actor_user_id=200,
        decision=Decision.CANCEL,
        source="telegram",
        now=now,
    )

    assert execution.result.outcome == Outcome.OK
    patch_message = None
    while True:
        outbox = repository.claim_due_outbox(now=now)
        if outbox is None:
            break
        if outbox.effect_type != OutboxEffectType.CALENDAR_PATCH_EVENT:
            continue
        patch_message = outbox
        break

    assert patch_message is not None
    payload_obj = patch_message.payload.get("payload")
    assert isinstance(payload_obj, dict)
    assert payload_obj.get("transparency") == "transparent"
    repository.close()


def test_pending_decision_updates_group_status_progress_message(tmp_path: Path) -> None:
    now = datetime(2026, 2, 13, 10, 0, 0)
    repository = _repo(tmp_path)
    repository.upsert_user_mapping(
        telegram_user_id=200,
        google_email="petya@4sell.ai",
        now=now,
        telegram_username="petya",
        timezone="UTC",
    )
    repository.upsert_user_mapping(
        telegram_user_id=300,
        google_email="vasya@4sell.ai",
        now=now,
        telegram_username="vasya",
        timezone="UTC",
    )
    meeting = replace(
        _meeting(now, meeting_id="m-7-pending-progress"),
        group_status_message_id=777,
    )
    repository.insert_meeting(meeting, now=now)
    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    execution = service.record_participant_decision(
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        actor_user_id=200,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now,
    )

    assert execution.result.outcome == Outcome.OK
    assert execution.meeting.state == MeetingState.PENDING

    group_update = None
    while True:
        outbox = repository.claim_due_outbox(now=now)
        if outbox is None:
            break
        if outbox.effect_type == OutboxEffectType.TELEGRAM_EDIT_MESSAGE:
            group_update = outbox
            break

    assert group_update is not None
    assert group_update.payload.get("telegram_user_id") == meeting.chat_id
    assert group_update.payload.get("message_id") == 777
    text_obj = group_update.payload.get("text")
    assert isinstance(text_obj, str)
    assert "Подтвердили: 1 (@petya)" in text_obj
    assert "Не подтвердили: 1 (@vasya)" in text_obj
    assert "Ожидаем ответы: @vasya" in text_obj
    repository.close()


def test_pending_progress_edits_have_distinct_keys_same_timestamp(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 13, 10, 0, 0)
    repository = _repo(tmp_path)
    repository.upsert_user_mapping(
        telegram_user_id=200,
        google_email="petya@4sell.ai",
        now=now,
        telegram_username="petya",
        timezone="UTC",
    )
    repository.upsert_user_mapping(
        telegram_user_id=300,
        google_email="vasya@4sell.ai",
        now=now,
        telegram_username="vasya",
        timezone="UTC",
    )
    repository.upsert_user_mapping(
        telegram_user_id=400,
        google_email="masha@4sell.ai",
        now=now,
        telegram_username="masha",
        timezone="UTC",
    )
    meeting = Meeting(
        meeting_id="m-7-pending-progress-keys",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.PENDING,
        scheduled_start_at=now + timedelta(hours=1),
        scheduled_end_at=now + timedelta(hours=2),
        confirmation_deadline_at=now + timedelta(minutes=20),
        group_status_message_id=777,
        participants=(
            MeetingParticipant(
                telegram_user_id=100, is_required=False, decision=Decision.NONE
            ),
            MeetingParticipant(
                telegram_user_id=200, is_required=True, decision=Decision.NONE
            ),
            MeetingParticipant(
                telegram_user_id=300, is_required=True, decision=Decision.NONE
            ),
            MeetingParticipant(
                telegram_user_id=400, is_required=True, decision=Decision.NONE
            ),
        ),
    )
    repository.insert_meeting(meeting, now=now)
    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    first = service.record_participant_decision(
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        actor_user_id=200,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now,
    )
    second = service.record_participant_decision(
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        actor_user_id=300,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now,
    )

    assert first.result.outcome == Outcome.OK
    assert second.result.outcome == Outcome.OK
    assert first.meeting.state == MeetingState.PENDING
    assert second.meeting.state == MeetingState.PENDING

    progress_keys: list[str] = []
    while True:
        outbox = repository.claim_due_outbox(now=now)
        if outbox is None:
            break
        if (
            outbox.effect_type == OutboxEffectType.TELEGRAM_EDIT_MESSAGE
            and isinstance(outbox.idempotency_key, str)
            and ":pending_progress:edit:" in outbox.idempotency_key
        ):
            progress_keys.append(outbox.idempotency_key)

    assert len(progress_keys) == 2
    assert progress_keys[0] != progress_keys[1]
    repository.close()


def test_pending_progress_update_skipped_without_group_pointer(tmp_path: Path) -> None:
    now = datetime(2026, 2, 13, 10, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now, meeting_id="m-7-pending-progress-no-pointer")
    repository.insert_meeting(meeting, now=now)
    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    execution = service.record_participant_decision(
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        actor_user_id=200,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now,
    )

    assert execution.result.outcome == Outcome.OK
    assert execution.meeting.state == MeetingState.PENDING
    assert (
        repository.count_outbox(
            status=OutboxStatus.PENDING,
            effect_type=OutboxEffectType.TELEGRAM_EDIT_MESSAGE,
        )
        == 0
    )
    repository.close()


def test_repeated_same_decision_does_not_enqueue_duplicate_calendar_patch(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 13, 10, 0, 0)
    repository = _repo(tmp_path)
    repository.upsert_user_mapping(
        telegram_user_id=100,
        google_email="initiator@4sell.ai",
        now=now,
        telegram_username="initiator",
        timezone="UTC",
    )
    repository.upsert_user_mapping(
        telegram_user_id=200,
        google_email="petya@4sell.ai",
        now=now,
        telegram_username="petya",
        timezone="UTC",
    )
    meeting = replace(
        _meeting(now, meeting_id="m-7-calendar-attendee-no-dup"),
        google_event_id="evt-7-no-dup",
    )
    repository.insert_meeting(meeting, now=now)
    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    first = service.record_participant_decision(
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        actor_user_id=200,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now,
    )
    second = service.record_participant_decision(
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        actor_user_id=200,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now + timedelta(seconds=1),
    )

    assert first.result.outcome == Outcome.OK
    assert second.result.outcome == Outcome.NOOP
    assert (
        repository.count_outbox(
            status=OutboxStatus.PENDING,
            effect_type=OutboxEffectType.CALENDAR_PATCH_EVENT,
        )
        == 1
    )
    repository.close()


def test_cancel_transition_enqueues_outbox_messages(tmp_path: Path) -> None:
    now = datetime(2026, 2, 13, 10, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now, meeting_id="m-7-cancel")
    repository.insert_meeting(meeting, now=now)
    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    execution = service.cancel_meeting(
        meeting_id=meeting.meeting_id,
        actor_user_id=meeting.initiator_telegram_user_id,
        reason="manual",
        now=now,
    )

    assert execution.result.outcome == Outcome.OK
    assert execution.meeting.state == MeetingState.CANCELLED
    assert (
        repository.count_outbox(
            status=OutboxStatus.PENDING,
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
        )
        == 1
    )
    claimed = repository.claim_due_outbox(now=now)
    assert claimed is not None
    assert claimed.payload["telegram_user_id"] == execution.meeting.chat_id
    repository.close()


def test_needs_initiator_decision_enqueues_to_initiator_only(tmp_path: Path) -> None:
    now = datetime(2026, 2, 13, 10, 0, 0)
    repository = _repo(tmp_path)
    repository.upsert_user_mapping(
        telegram_user_id=100,
        google_email="initiator@4sell.ai",
        now=now,
        telegram_username="initiator",
        timezone="UTC",
    )
    repository.upsert_user_mapping(
        telegram_user_id=200,
        google_email="petya@4sell.ai",
        now=now,
        telegram_username="petya",
        timezone="UTC",
    )
    repository.upsert_user_mapping(
        telegram_user_id=300,
        google_email="vasya@4sell.ai",
        now=now,
        telegram_username="vasya",
        timezone="UTC",
    )
    meeting = _meeting(now, meeting_id="m-7-needs-init")
    repository.insert_meeting(meeting, now=now)
    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    execution = service.record_participant_decision(
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        actor_user_id=200,
        decision=Decision.CANCEL,
        source="telegram",
        now=now,
    )

    assert execution.result.outcome == Outcome.OK
    assert execution.meeting.state == MeetingState.NEEDS_INITIATOR_DECISION
    assert (
        repository.count_outbox(
            status=OutboxStatus.PENDING,
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
        )
        == 1
    )

    claimed = repository.claim_due_outbox(now=now)
    assert claimed is not None
    assert claimed.payload["telegram_user_id"] == meeting.initiator_telegram_user_id
    text_obj = claimed.payload.get("text")
    assert isinstance(text_obj, str)
    assert "отказались: 1 (@petya)" in text_obj
    assert "без ответа: @vasya" in text_obj
    repository.close()


def test_needs_initiator_decision_sends_group_status_and_private_buttons(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 13, 10, 0, 0)
    repository = _repo(tmp_path)
    meeting = replace(
        _meeting(now, meeting_id="m-7-needs-init-group"),
        chat_id=-100777,
    )
    repository.insert_meeting(meeting, now=now)
    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    execution = service.record_participant_decision(
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        actor_user_id=200,
        decision=Decision.CANCEL,
        source="telegram",
        now=now,
    )

    assert execution.result.outcome == Outcome.OK
    assert execution.meeting.state == MeetingState.NEEDS_INITIATOR_DECISION
    assert (
        repository.count_outbox(
            status=OutboxStatus.PENDING,
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
        )
        == 2
    )

    first = repository.claim_due_outbox(now=now)
    second = repository.claim_due_outbox(now=now)
    assert first is not None
    assert second is not None

    payloads = (first.payload, second.payload)
    group_payload = next(
        payload
        for payload in payloads
        if payload.get("telegram_user_id") == meeting.chat_id
    )
    initiator_payload = next(
        payload
        for payload in payloads
        if payload.get("telegram_user_id") == meeting.initiator_telegram_user_id
    )

    group_text_obj = group_payload.get("text")
    assert isinstance(group_text_obj, str)
    assert "Ждем финальное решение инициатора" in group_text_obj
    assert "провести, перенести или отменить" in group_text_obj

    initiator_buttons_obj = initiator_payload.get("buttons")
    assert isinstance(initiator_buttons_obj, list)
    assert len(initiator_buttons_obj) == 3
    repository.close()


def test_needs_initiator_decision_notifies_active_managers(tmp_path: Path) -> None:
    now = datetime(2026, 2, 13, 10, 0, 0)
    repository = _repo(tmp_path)
    repository.grant_manager_role(
        telegram_user_id=100,
        granted_by=None,
        now=now,
    )
    repository.grant_manager_role(
        telegram_user_id=300,
        granted_by=100,
        now=now,
    )
    repository.upsert_user_mapping(
        telegram_user_id=100,
        google_email="initiator@4sell.ai",
        now=now,
        telegram_username="initiator",
        timezone="UTC",
    )
    repository.upsert_user_mapping(
        telegram_user_id=200,
        google_email="petya@4sell.ai",
        now=now,
        telegram_username="petya",
        timezone="UTC",
    )
    repository.upsert_user_mapping(
        telegram_user_id=300,
        google_email="manager@4sell.ai",
        now=now,
        telegram_username="manager",
        timezone="UTC",
    )
    meeting = _meeting(now, meeting_id="m-7-manager-notify")
    repository.insert_meeting(meeting, now=now)
    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    execution = service.record_participant_decision(
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round,
        actor_user_id=200,
        decision=Decision.CANCEL,
        source="telegram",
        now=now,
    )

    assert execution.result.outcome == Outcome.OK
    assert execution.meeting.state == MeetingState.NEEDS_INITIATOR_DECISION

    first = repository.claim_due_outbox(now=now)
    second = repository.claim_due_outbox(now=now)
    assert first is not None
    assert second is not None
    recipients = {
        first.payload.get("telegram_user_id"),
        second.payload.get("telegram_user_id"),
    }
    assert recipients == {100, 300}
    repository.close()


def test_select_slot_enqueues_participant_private_decision_requests(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 13, 10, 0, 0)
    repository = _repo(tmp_path)
    repository.upsert_user_mapping(
        telegram_user_id=100,
        google_email="initiator@4sell.ai",
        now=now,
        telegram_username="initiator",
        timezone="UTC",
    )
    meeting = Meeting(
        meeting_id="m-7-pending-dm",
        initiator_telegram_user_id=100,
        chat_id=-100888,
        state=MeetingState.DRAFT,
        title="Smoke test",
        scheduled_start_at=now,
        scheduled_end_at=now,
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
    repository.insert_meeting(meeting, now=now)

    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())
    execution = service.select_slot(
        meeting_id=meeting.meeting_id,
        actor_user_id=meeting.initiator_telegram_user_id,
        chat_id=meeting.chat_id,
        scheduled_start_at=now + timedelta(hours=1),
        scheduled_end_at=now + timedelta(hours=2),
        now=now,
    )

    assert execution.result.outcome == Outcome.OK
    assert execution.meeting.state == MeetingState.PENDING
    assert (
        repository.count_outbox(
            status=OutboxStatus.PENDING,
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
        )
        == 4
    )

    claimed_messages = []
    while True:
        claimed = repository.claim_due_outbox(now=now)
        if claimed is None:
            break
        claimed_messages.append(claimed)

    telegram_messages = [
        message
        for message in claimed_messages
        if message.effect_type == OutboxEffectType.TELEGRAM_SEND_MESSAGE
    ]
    assert len(telegram_messages) == 4

    participant_messages = [
        message
        for message in telegram_messages
        if message.payload.get("telegram_user_id") in {200, 300}
    ]
    assert len(participant_messages) == 2
    for message in participant_messages:
        buttons_obj = message.payload.get("buttons")
        assert isinstance(buttons_obj, list)
        assert len(buttons_obj) == 2

    initiator_messages = [
        message
        for message in telegram_messages
        if message.payload.get("telegram_user_id") == 100
        and isinstance(message.payload.get("text"), str)
        and "Назначена встреча" in str(message.payload.get("text"))
    ]
    assert len(initiator_messages) == 1

    insert_messages = [
        message
        for message in claimed_messages
        if message.effect_type == OutboxEffectType.CALENDAR_INSERT_EVENT
    ]
    assert len(insert_messages) == 1
    insert_payload_obj = insert_messages[0].payload.get("payload")
    assert isinstance(insert_payload_obj, dict)
    assert insert_payload_obj.get("transparency") == "transparent"
    repository.close()


def test_no_outbox_on_noop_outcome(tmp_path: Path) -> None:
    now = datetime(2026, 2, 13, 10, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now, meeting_id="m-7-noop")
    repository.insert_meeting(meeting, now=now)
    service = MeetingWorkflowService(repository, calendar_gateway=MagicMock())

    execution = service.record_participant_decision(
        meeting_id=meeting.meeting_id,
        round=meeting.confirmation_round + 1,
        actor_user_id=200,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now,
    )

    assert execution.result.outcome != Outcome.OK
    assert (
        repository.count_outbox(
            status=OutboxStatus.PENDING,
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
        )
        == 0
    )
    repository.close()


def test_reminder_job_enqueues_for_undecided_participants(tmp_path: Path) -> None:
    now = datetime(2026, 2, 13, 10, 0, 0)
    repository = _repo(tmp_path)
    repository.upsert_user_mapping(
        telegram_user_id=100,
        google_email="initiator@4sell.ai",
        now=now,
        telegram_username="initiator",
        timezone="UTC",
    )
    repository.upsert_user_mapping(
        telegram_user_id=200,
        google_email="petya@4sell.ai",
        now=now,
        telegram_username="petya",
        timezone="UTC",
    )
    repository.upsert_user_mapping(
        telegram_user_id=300,
        google_email="vasya@4sell.ai",
        now=now,
        telegram_username="vasya",
        timezone="UTC",
    )
    meeting = replace(
        _meeting(now, meeting_id="m-7-reminder"),
        participants=(
            MeetingParticipant(
                telegram_user_id=100,
                is_required=False,
                decision=Decision.CONFIRM,
                decision_received_at=now - timedelta(minutes=1),
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
    repository.insert_meeting(meeting, now=now)
    repository.enqueue_jobs(
        (
            ScheduledJobSpec(
                job_type=JobType.REMINDER,
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

    assert tick.processed is True
    assert (
        repository.count_outbox(
            status=OutboxStatus.PENDING,
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
        )
        == 2
    )
    claimed_first = repository.claim_due_outbox(now=now)
    claimed_second = repository.claim_due_outbox(now=now)
    assert claimed_first is not None
    assert claimed_second is not None
    recipients = {
        claimed_first.payload["telegram_user_id"],
        claimed_second.payload["telegram_user_id"],
    }
    assert recipients == {200, 300}
    first_text_obj = claimed_first.payload.get("text")
    second_text_obj = claimed_second.payload.get("text")
    assert isinstance(first_text_obj, str)
    assert isinstance(second_text_obj, str)
    assert "подтвердите участие" in first_text_obj.lower()
    assert "подтвердите участие" in second_text_obj.lower()
    repository.close()


def test_reminder_job_schedules_next_if_still_pending(tmp_path: Path) -> None:
    now = datetime(2026, 2, 13, 10, 0, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now, meeting_id="m-7-reminder-next")
    repository.insert_meeting(meeting, now=now)
    repository.enqueue_jobs(
        (
            ScheduledJobSpec(
                job_type=JobType.REMINDER,
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
    _ = worker.run_once(now=now)

    assert (
        repository.count_jobs(
            meeting_id=meeting.meeting_id,
            status=JobStatus.PENDING,
            job_type=JobType.REMINDER,
        )
        == 1
    )
    repository.close()


def test_reminder_idempotency_key_and_reschedule_are_bound_to_job_run_at(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 13, 10, 10, 0)
    repository = _repo(tmp_path)
    meeting = _meeting(now, meeting_id="m-7-reminder-run-at")
    repository.insert_meeting(meeting, now=now)

    reminder_run_at = now - timedelta(minutes=10)
    repository.enqueue_jobs(
        (
            ScheduledJobSpec(
                job_type=JobType.REMINDER,
                meeting_id=meeting.meeting_id,
                round=meeting.confirmation_round,
                run_at=reminder_run_at,
            ),
        ),
        now=now,
    )

    worker = SchedulerWorker(
        repository=repository,
        service=MeetingWorkflowService(repository, calendar_gateway=MagicMock()),
    )
    tick = worker.run_once(now=now)
    assert tick.processed is True

    first_message = repository.claim_due_outbox(now=now)
    second_message = repository.claim_due_outbox(now=now)
    assert first_message is not None
    assert second_message is not None
    run_at_tag = f"run_at:{reminder_run_at.isoformat(timespec='seconds')}"
    assert isinstance(first_message.idempotency_key, str)
    assert isinstance(second_message.idempotency_key, str)
    assert run_at_tag in first_message.idempotency_key
    assert run_at_tag in second_message.idempotency_key

    rescheduled = repository.claim_due_job(now=now)
    assert rescheduled is not None
    assert rescheduled.job_type == JobType.REMINDER
    assert rescheduled.run_at == reminder_run_at + timedelta(minutes=5)
    repository.close()


def test_reminder_reschedule_interval_stays_5_minutes_for_long_windows(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 2, 13, 10, 10, 0)
    repository = _repo(tmp_path)
    meeting = replace(
        _meeting(now, meeting_id="m-7-reminder-long-window"),
        confirmation_deadline_at=now + timedelta(hours=6),
    )
    repository.insert_meeting(meeting, now=now)

    reminder_run_at = now - timedelta(minutes=10)
    repository.enqueue_jobs(
        (
            ScheduledJobSpec(
                job_type=JobType.REMINDER,
                meeting_id=meeting.meeting_id,
                round=meeting.confirmation_round,
                run_at=reminder_run_at,
            ),
        ),
        now=now,
    )

    worker = SchedulerWorker(
        repository=repository,
        service=MeetingWorkflowService(repository, calendar_gateway=MagicMock()),
    )
    tick = worker.run_once(now=now)
    assert tick.processed is True

    rescheduled = repository.claim_due_job(now=now)
    assert rescheduled is not None
    assert rescheduled.job_type == JobType.REMINDER
    assert rescheduled.run_at == reminder_run_at + timedelta(minutes=5)
    repository.close()


def test_reminder_job_noop_for_non_pending_meeting(tmp_path: Path) -> None:
    now = datetime(2026, 2, 13, 10, 0, 0)
    repository = _repo(tmp_path)
    meeting = replace(
        _meeting(now, meeting_id="m-7-reminder-noop"),
        state=MeetingState.CONFIRMED,
        confirmation_deadline_at=None,
    )
    repository.insert_meeting(meeting, now=now)
    repository.enqueue_jobs(
        (
            ScheduledJobSpec(
                job_type=JobType.REMINDER,
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

    assert tick.processed is True
    assert (
        repository.count_outbox(
            status=OutboxStatus.PENDING,
            effect_type=OutboxEffectType.TELEGRAM_SEND_MESSAGE,
        )
        == 0
    )
    assert (
        repository.count_jobs(
            meeting_id=meeting.meeting_id,
            status=JobStatus.DONE,
            job_type=JobType.REMINDER,
        )
        == 1
    )
    repository.close()
