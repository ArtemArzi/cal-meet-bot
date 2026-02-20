from __future__ import annotations

from datetime import datetime, timedelta

from bot_vstrechi.domain.commands import (
    HandleConfirmDeadline,
    RecordParticipantDecision,
    RescheduleMeeting,
    SelectSlot,
)
from bot_vstrechi.domain import (
    Decision,
    JobType,
    Meeting,
    MeetingParticipant,
    MeetingState,
    Outcome,
    RecurringConfirmationMode,
    ReasonCode,
)
from bot_vstrechi.domain.policies import (
    DEADLINE_GRACE_WINDOW,
    REMINDER_INTERVAL,
    URGENT_CONFIRM_BUFFER,
)


def _base_meeting(now: datetime) -> Meeting:
    return Meeting(
        meeting_id="m-1",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.PENDING,
        scheduled_start_at=now + timedelta(hours=4),
        scheduled_end_at=now + timedelta(hours=5),
        participants=(
            MeetingParticipant(telegram_user_id=100, is_required=True),
            MeetingParticipant(telegram_user_id=200, is_required=True),
        ),
    )


def test_pending_to_confirmed_when_all_required_confirm() -> None:
    now = datetime(2026, 2, 11, 10, 0, 0)
    meeting = _base_meeting(now)

    slot = SelectSlot(
        meeting,
        actor_user_id=100,
        scheduled_start_at=now + timedelta(hours=4),
        scheduled_end_at=now + timedelta(hours=5),
        now=now,
    ).meeting

    first = RecordParticipantDecision(
        slot,
        round=slot.confirmation_round,
        actor_user_id=100,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now + timedelta(minutes=1),
    )
    second = RecordParticipantDecision(
        first.meeting,
        round=slot.confirmation_round,
        actor_user_id=200,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now + timedelta(minutes=2),
    )

    assert second.result.outcome == Outcome.OK
    assert second.meeting.state == MeetingState.CONFIRMED


def test_transition_to_needs_initiator_decision_on_deadline_no_response() -> None:
    now = datetime(2026, 2, 11, 10, 0, 0)
    meeting = _base_meeting(now)

    slot_exec = SelectSlot(
        meeting,
        actor_user_id=100,
        scheduled_start_at=now + timedelta(hours=1),
        scheduled_end_at=now + timedelta(hours=2),
        now=now,
    )
    slot = slot_exec.meeting

    confirm_one = RecordParticipantDecision(
        slot,
        round=slot.confirmation_round,
        actor_user_id=100,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now + timedelta(minutes=2),
    )
    deadline_exec = HandleConfirmDeadline(
        confirm_one.meeting,
        round=confirm_one.meeting.confirmation_round,
        now=now + timedelta(minutes=31),
    )

    assert deadline_exec.result.outcome == Outcome.OK
    assert deadline_exec.meeting.state == MeetingState.NEEDS_INITIATOR_DECISION
    assert deadline_exec.meeting.initiator_decision_deadline_at is not None


def test_late_participant_confirm_in_needs_initiator_decision_confirms_meeting() -> (
    None
):
    now = datetime(2026, 2, 11, 10, 0, 0)
    meeting = _base_meeting(now)

    slot_exec = SelectSlot(
        meeting,
        actor_user_id=100,
        scheduled_start_at=now + timedelta(hours=3),
        scheduled_end_at=now + timedelta(hours=4),
        now=now,
    )
    slot = slot_exec.meeting

    confirm_initiator = RecordParticipantDecision(
        slot,
        round=slot.confirmation_round,
        actor_user_id=100,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now + timedelta(minutes=2),
    )
    deadline_exec = HandleConfirmDeadline(
        confirm_initiator.meeting,
        round=confirm_initiator.meeting.confirmation_round,
        now=now + timedelta(minutes=61),
    )
    assert deadline_exec.meeting.state == MeetingState.NEEDS_INITIATOR_DECISION

    late_confirm = RecordParticipantDecision(
        deadline_exec.meeting,
        round=deadline_exec.meeting.confirmation_round,
        actor_user_id=200,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now + timedelta(minutes=63),
    )

    assert late_confirm.result.outcome == Outcome.OK
    assert late_confirm.result.reason_code == ReasonCode.UPDATED
    assert late_confirm.meeting.state == MeetingState.CONFIRMED


def test_confirm_deadline_waits_for_grace_window_before_transition() -> None:
    now = datetime(2026, 2, 11, 10, 0, 0)
    meeting = _base_meeting(now)

    slot = SelectSlot(
        meeting,
        actor_user_id=100,
        scheduled_start_at=now + timedelta(hours=1),
        scheduled_end_at=now + timedelta(hours=2),
        now=now,
    ).meeting

    assert slot.confirmation_deadline_at is not None
    before_grace_end = (
        slot.confirmation_deadline_at
        + DEADLINE_GRACE_WINDOW
        - timedelta(microseconds=1)
    )
    in_grace = HandleConfirmDeadline(
        slot,
        round=slot.confirmation_round,
        now=before_grace_end,
    )

    assert in_grace.result.outcome == Outcome.NOOP
    assert in_grace.result.reason_code == ReasonCode.TOO_EARLY
    assert in_grace.meeting.state == MeetingState.PENDING


def test_urgent_window_behavior() -> None:
    now = datetime(2026, 2, 11, 10, 0, 0)
    meeting = _base_meeting(now)

    fast_exec = SelectSlot(
        meeting,
        actor_user_id=100,
        scheduled_start_at=now + timedelta(minutes=40),
        scheduled_end_at=now + timedelta(minutes=70),
        now=now,
    )

    assert fast_exec.meeting.state == MeetingState.PENDING
    assert (
        fast_exec.meeting.confirmation_deadline_at
        == now + timedelta(minutes=40) - URGENT_CONFIRM_BUFFER
    )


def test_select_slot_enqueues_initial_reminder_for_standard_window() -> None:
    now = datetime(2026, 2, 11, 10, 0, 0)
    meeting = _base_meeting(now)

    execution = SelectSlot(
        meeting,
        actor_user_id=100,
        scheduled_start_at=now + timedelta(days=1, hours=2),
        scheduled_end_at=now + timedelta(days=1, hours=3),
        now=now,
    )

    assert execution.result.outcome == Outcome.OK
    reminder_jobs = tuple(
        job for job in execution.jobs if job.job_type == JobType.REMINDER
    )
    assert len(reminder_jobs) == 1
    assert reminder_jobs[0].run_at == now + REMINDER_INTERVAL


def test_urgent_select_slot_enqueues_initial_reminder() -> None:
    now = datetime(2026, 2, 11, 10, 0, 0)
    meeting = _base_meeting(now)

    execution = SelectSlot(
        meeting,
        actor_user_id=100,
        scheduled_start_at=now + timedelta(minutes=40),
        scheduled_end_at=now + timedelta(minutes=70),
        now=now,
    )

    reminder_jobs = tuple(
        job for job in execution.jobs if job.job_type == JobType.REMINDER
    )
    assert len(reminder_jobs) == 1


def test_recurring_exceptions_only_select_slot_autoconfirms_without_jobs() -> None:
    now = datetime(2026, 2, 11, 10, 0, 0)
    meeting = Meeting(
        meeting_id="m-recurring-auto",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.DRAFT,
        scheduled_start_at=now + timedelta(hours=3),
        scheduled_end_at=now + timedelta(hours=4),
        series_event_id="series-1",
        recurring_confirmation_mode=RecurringConfirmationMode.EXCEPTIONS_ONLY,
        participants=(
            MeetingParticipant(telegram_user_id=100, is_required=False),
            MeetingParticipant(telegram_user_id=200, is_required=True),
        ),
    )

    execution = SelectSlot(
        meeting,
        actor_user_id=100,
        scheduled_start_at=now + timedelta(hours=3),
        scheduled_end_at=now + timedelta(hours=4),
        now=now,
    )

    assert execution.result.outcome == Outcome.OK
    assert execution.meeting.state == MeetingState.CONFIRMED
    assert execution.meeting.confirmation_deadline_at is None
    assert execution.jobs == ()


def test_recurring_exceptions_only_deadline_silence_autoconfirms() -> None:
    now = datetime(2026, 2, 11, 10, 0, 0)
    deadline = now + timedelta(minutes=20)
    meeting = Meeting(
        meeting_id="m-recurring-deadline",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.PENDING,
        scheduled_start_at=now + timedelta(hours=2),
        scheduled_end_at=now + timedelta(hours=3),
        series_event_id="series-1",
        recurring_confirmation_mode=RecurringConfirmationMode.EXCEPTIONS_ONLY,
        confirmation_deadline_at=deadline,
        participants=(
            MeetingParticipant(telegram_user_id=100, is_required=False),
            MeetingParticipant(telegram_user_id=200, is_required=True),
        ),
    )

    execution = HandleConfirmDeadline(
        meeting,
        round=meeting.confirmation_round,
        now=deadline + DEADLINE_GRACE_WINDOW,
    )

    assert execution.result.outcome == Outcome.OK
    assert execution.meeting.state == MeetingState.CONFIRMED
    assert execution.jobs == ()


def test_recurring_exceptions_only_force_pending_skips_initial_reminder() -> None:
    now = datetime(2026, 2, 11, 10, 0, 0)
    meeting = Meeting(
        meeting_id="m-recurring-force-pending",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.DRAFT,
        scheduled_start_at=now + timedelta(hours=4),
        scheduled_end_at=now + timedelta(hours=5),
        series_event_id="series-1",
        recurring_confirmation_mode=RecurringConfirmationMode.EXCEPTIONS_ONLY,
        participants=(
            MeetingParticipant(telegram_user_id=100, is_required=False),
            MeetingParticipant(telegram_user_id=200, is_required=True),
        ),
    )

    execution = SelectSlot(
        meeting,
        actor_user_id=100,
        scheduled_start_at=now + timedelta(hours=4),
        scheduled_end_at=now + timedelta(hours=5),
        now=now,
        force_pending=True,
    )

    assert execution.meeting.state == MeetingState.PENDING
    reminder_jobs = tuple(
        job for job in execution.jobs if job.job_type == JobType.REMINDER
    )
    assert reminder_jobs == ()


def test_less_than_10m_goes_directly_to_needs_initiator_decision() -> None:
    now = datetime(2026, 2, 11, 10, 0, 0)
    meeting = _base_meeting(now)

    urgent_exec = SelectSlot(
        meeting,
        actor_user_id=100,
        scheduled_start_at=now + timedelta(minutes=8),
        scheduled_end_at=now + timedelta(minutes=38),
        now=now,
    )

    assert urgent_exec.result.outcome == Outcome.OK
    assert urgent_exec.meeting.state == MeetingState.NEEDS_INITIATOR_DECISION
    assert urgent_exec.meeting.confirmation_deadline_at is None
    assert urgent_exec.meeting.initiator_decision_deadline_at == now + timedelta(
        minutes=15
    )


def test_callback_idempotency_duplicate_press_returns_same_result() -> None:
    now = datetime(2026, 2, 11, 10, 0, 0)
    meeting = _base_meeting(now)
    slot = SelectSlot(
        meeting,
        actor_user_id=100,
        scheduled_start_at=now + timedelta(hours=4),
        scheduled_end_at=now + timedelta(hours=5),
        now=now,
    ).meeting

    first = RecordParticipantDecision(
        slot,
        round=slot.confirmation_round,
        actor_user_id=200,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now + timedelta(minutes=1),
    )
    duplicate = RecordParticipantDecision(
        first.meeting,
        round=slot.confirmation_round,
        actor_user_id=200,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now + timedelta(minutes=1),
    )

    assert first.result.outcome == Outcome.OK
    assert duplicate.result.outcome == Outcome.NOOP
    assert duplicate.meeting == first.meeting


def test_reschedule_resets_participant_decisions_for_new_round() -> None:
    now = datetime(2026, 2, 11, 10, 0, 0)
    meeting = _base_meeting(now)
    slot = SelectSlot(
        meeting,
        actor_user_id=100,
        scheduled_start_at=now + timedelta(hours=4),
        scheduled_end_at=now + timedelta(hours=5),
        now=now,
    ).meeting

    first_confirm = RecordParticipantDecision(
        slot,
        round=slot.confirmation_round,
        actor_user_id=200,
        decision=Decision.CONFIRM,
        source="telegram",
        now=now + timedelta(minutes=1),
    )
    updated_before_reschedule = first_confirm.meeting
    participant_before = next(
        p for p in updated_before_reschedule.participants if p.telegram_user_id == 200
    )
    assert participant_before.decision == Decision.CONFIRM
    assert participant_before.decision_received_at is not None

    rescheduled = RescheduleMeeting(
        updated_before_reschedule,
        actor_user_id=100,
        scheduled_start_at=now + timedelta(days=1, hours=2),
        scheduled_end_at=now + timedelta(days=1, hours=3),
        now=now + timedelta(minutes=2),
    )

    assert rescheduled.result.outcome == Outcome.OK
    assert rescheduled.meeting.confirmation_round == slot.confirmation_round + 1
    participant_after = next(
        p for p in rescheduled.meeting.participants if p.telegram_user_id == 200
    )
    assert participant_after.decision == Decision.NONE
    assert participant_after.decision_received_at is None


def test_reschedule_from_needs_initiator_decision_resets_all_decisions() -> None:
    now = datetime(2026, 2, 11, 10, 0, 0)
    meeting = Meeting(
        meeting_id="m-2",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.NEEDS_INITIATOR_DECISION,
        scheduled_start_at=now + timedelta(hours=2),
        scheduled_end_at=now + timedelta(hours=3),
        confirmation_round=3,
        initiator_decision_deadline_at=now + timedelta(minutes=10),
        participants=(
            MeetingParticipant(telegram_user_id=100, is_required=False),
            MeetingParticipant(
                telegram_user_id=200,
                is_required=True,
                decision=Decision.CONFIRM,
                decision_received_at=now - timedelta(minutes=4),
            ),
            MeetingParticipant(
                telegram_user_id=300,
                is_required=True,
                decision=Decision.CANCEL,
                decision_received_at=now - timedelta(minutes=3),
            ),
        ),
    )

    participant_a = next(p for p in meeting.participants if p.telegram_user_id == 200)
    participant_b = next(p for p in meeting.participants if p.telegram_user_id == 300)
    assert participant_a.decision == Decision.CONFIRM
    assert participant_b.decision == Decision.CANCEL

    rescheduled = RescheduleMeeting(
        meeting,
        actor_user_id=100,
        scheduled_start_at=now + timedelta(days=1, hours=2),
        scheduled_end_at=now + timedelta(days=1, hours=3),
        now=now + timedelta(minutes=1),
    )

    assert rescheduled.result.outcome == Outcome.OK
    assert rescheduled.meeting.state == MeetingState.PENDING
    assert rescheduled.meeting.confirmation_round == meeting.confirmation_round + 1
    assert all(
        participant.decision == Decision.NONE
        for participant in rescheduled.meeting.participants
    )
    assert all(
        participant.decision_received_at is None
        for participant in rescheduled.meeting.participants
    )
