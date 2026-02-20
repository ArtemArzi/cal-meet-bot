from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime

from .models import (
    CommandResult,
    Decision,
    DecisionSource,
    JobType,
    Meeting,
    MeetingState,
    Outcome,
    RecurringConfirmationMode,
    ReasonCode,
    ScheduledJobSpec,
)
from .policies import (
    INITIATOR_TIMEOUT,
    REMINDER_INTERVAL,
    ConfirmationMode,
    build_confirmation_plan,
)
from .state_machine import (
    apply_participant_decision,
    handle_confirm_deadline,
    handle_initiator_timeout,
)


@dataclass(frozen=True)
class CommandExecution:
    result: CommandResult
    meeting: Meeting
    jobs: tuple[ScheduledJobSpec, ...] = ()


def ProposeSlots(_: object | None = None) -> CommandExecution:
    return CommandExecution(
        result=CommandResult(Outcome.REJECTED, ReasonCode.INVALID_STATE),
        meeting=Meeting(
            meeting_id="",
            initiator_telegram_user_id=0,
            chat_id=0,
            state=MeetingState.EXPIRED,
            scheduled_start_at=datetime.min,
            scheduled_end_at=datetime.min,
        ),
    )


def SelectSlot(
    meeting: Meeting,
    *,
    actor_user_id: int,
    scheduled_start_at: datetime,
    scheduled_end_at: datetime,
    now: datetime,
    force_pending: bool = False,
) -> CommandExecution:
    if actor_user_id != meeting.initiator_telegram_user_id:
        return CommandExecution(
            result=CommandResult(Outcome.REJECTED, ReasonCode.PERMISSION_DENIED),
            meeting=meeting,
        )

    if not meeting.created_by_bot:
        return CommandExecution(
            result=CommandResult(Outcome.REJECTED, ReasonCode.NOT_BOT_CREATED_EVENT),
            meeting=meeting,
        )

    if scheduled_start_at <= now or scheduled_end_at <= scheduled_start_at:
        return CommandExecution(
            result=CommandResult(Outcome.REJECTED, ReasonCode.SLOT_IN_PAST),
            meeting=meeting,
        )

    plan = build_confirmation_plan(now=now, scheduled_start_at=scheduled_start_at)
    base = replace(
        meeting,
        scheduled_start_at=scheduled_start_at,
        scheduled_end_at=scheduled_end_at,
        confirmation_deadline_at=plan.confirmation_deadline_at,
    )

    should_auto_confirm_recurring = (
        meeting.recurring_confirmation_mode == RecurringConfirmationMode.EXCEPTIONS_ONLY
        and isinstance(meeting.series_event_id, str)
        and bool(meeting.series_event_id.strip())
        and not force_pending
    )
    if should_auto_confirm_recurring:
        return CommandExecution(
            result=CommandResult(Outcome.OK, ReasonCode.UPDATED),
            meeting=replace(
                base,
                state=MeetingState.CONFIRMED,
                confirmation_deadline_at=None,
                initiator_decision_deadline_at=None,
            ),
            jobs=(),
        )

    if plan.mode == ConfirmationMode.IMMEDIATE_INITIATOR_DECISION:
        updated = replace(
            base,
            state=MeetingState.NEEDS_INITIATOR_DECISION,
            initiator_decision_deadline_at=now + INITIATOR_TIMEOUT,
        )
        if updated.initiator_decision_deadline_at is None:
            return CommandExecution(
                result=CommandResult(Outcome.REJECTED, ReasonCode.INVALID_STATE),
                meeting=meeting,
            )
        initiator_deadline = updated.initiator_decision_deadline_at
        timeout_job = ScheduledJobSpec(
            job_type=JobType.INITIATOR_TIMEOUT,
            meeting_id=updated.meeting_id,
            round=updated.confirmation_round,
            run_at=initiator_deadline,
        )
        return CommandExecution(
            result=CommandResult(Outcome.OK, ReasonCode.UPDATED),
            meeting=updated,
            jobs=(timeout_job,),
        )

    updated = replace(
        base, state=MeetingState.PENDING, initiator_decision_deadline_at=None
    )
    if updated.confirmation_deadline_at is None:
        return CommandExecution(
            result=CommandResult(Outcome.REJECTED, ReasonCode.INVALID_STATE),
            meeting=meeting,
        )
    confirmation_deadline = updated.confirmation_deadline_at
    deadline_job = ScheduledJobSpec(
        job_type=JobType.CONFIRM_DEADLINE,
        meeting_id=updated.meeting_id,
        round=updated.confirmation_round,
        run_at=confirmation_deadline,
    )
    jobs: tuple[ScheduledJobSpec, ...] = (deadline_job,)
    if (
        confirmation_deadline - now >= REMINDER_INTERVAL
        and meeting.recurring_confirmation_mode
        != RecurringConfirmationMode.EXCEPTIONS_ONLY
    ):
        reminder_job = ScheduledJobSpec(
            job_type=JobType.REMINDER,
            meeting_id=updated.meeting_id,
            round=updated.confirmation_round,
            run_at=now + REMINDER_INTERVAL,
        )
        jobs = (deadline_job, reminder_job)
    return CommandExecution(
        result=CommandResult(Outcome.OK, ReasonCode.UPDATED),
        meeting=updated,
        jobs=jobs,
    )


def RecordParticipantDecision(
    meeting: Meeting,
    *,
    round: int,
    actor_user_id: int,
    decision: Decision,
    source: str,
    now: datetime,
) -> CommandExecution:
    del source
    if round != meeting.confirmation_round:
        return CommandExecution(
            result=CommandResult(Outcome.NOOP, ReasonCode.STALE_OR_OLDER_RESPONSE),
            meeting=meeting,
        )

    result, updated = apply_participant_decision(
        meeting,
        actor_telegram_user_id=actor_user_id,
        decision=decision,
        decision_received_at=now,
    )

    jobs: tuple[ScheduledJobSpec, ...] = ()
    if (
        result.outcome == Outcome.OK
        and updated.state == MeetingState.NEEDS_INITIATOR_DECISION
    ):
        if updated.initiator_decision_deadline_at is None:
            return CommandExecution(
                result=CommandResult(Outcome.REJECTED, ReasonCode.INVALID_STATE),
                meeting=meeting,
            )
        initiator_deadline = updated.initiator_decision_deadline_at
        timeout_job = ScheduledJobSpec(
            job_type=JobType.INITIATOR_TIMEOUT,
            meeting_id=updated.meeting_id,
            round=updated.confirmation_round,
            run_at=initiator_deadline,
        )
        jobs = (timeout_job,)

    return CommandExecution(result=result, meeting=updated, jobs=jobs)


def HandleConfirmDeadline(
    meeting: Meeting, *, round: int, now: datetime
) -> CommandExecution:
    if round != meeting.confirmation_round:
        return CommandExecution(
            result=CommandResult(Outcome.NOOP, ReasonCode.STALE_OR_OLDER_RESPONSE),
            meeting=meeting,
        )

    result, updated = handle_confirm_deadline(meeting, now=now)
    jobs: tuple[ScheduledJobSpec, ...] = ()
    if (
        result.outcome == Outcome.OK
        and updated.state == MeetingState.NEEDS_INITIATOR_DECISION
    ):
        if updated.initiator_decision_deadline_at is None:
            return CommandExecution(
                result=CommandResult(Outcome.REJECTED, ReasonCode.INVALID_STATE),
                meeting=meeting,
            )
        initiator_deadline = updated.initiator_decision_deadline_at
        timeout_job = ScheduledJobSpec(
            job_type=JobType.INITIATOR_TIMEOUT,
            meeting_id=updated.meeting_id,
            round=updated.confirmation_round,
            run_at=initiator_deadline,
        )
        jobs = (timeout_job,)
    return CommandExecution(result=result, meeting=updated, jobs=jobs)


def HandleInitiatorTimeout(
    meeting: Meeting, *, round: int, now: datetime
) -> CommandExecution:
    if round != meeting.confirmation_round:
        return CommandExecution(
            result=CommandResult(Outcome.NOOP, ReasonCode.STALE_OR_OLDER_RESPONSE),
            meeting=meeting,
        )
    result, updated = handle_initiator_timeout(meeting, now=now)
    return CommandExecution(result=result, meeting=updated)


def RescheduleMeeting(
    meeting: Meeting,
    *,
    actor_user_id: int,
    scheduled_start_at: datetime,
    scheduled_end_at: datetime,
    now: datetime,
    force_pending: bool = False,
) -> CommandExecution:
    if actor_user_id != meeting.initiator_telegram_user_id:
        return CommandExecution(
            result=CommandResult(Outcome.REJECTED, ReasonCode.PERMISSION_DENIED),
            meeting=meeting,
        )

    reset_participants = tuple(
        replace(
            participant,
            decision=Decision.NONE,
            decision_source=DecisionSource.NONE,
            decision_received_at=None,
        )
        for participant in meeting.participants
    )
    bumped = replace(
        meeting,
        confirmation_round=meeting.confirmation_round + 1,
        participants=reset_participants,
    )
    return SelectSlot(
        bumped,
        actor_user_id=actor_user_id,
        scheduled_start_at=scheduled_start_at,
        scheduled_end_at=scheduled_end_at,
        now=now,
        force_pending=force_pending,
    )


def CancelMeeting(
    meeting: Meeting, *, actor_user_id: int, reason: str
) -> CommandExecution:
    del reason
    if actor_user_id != meeting.initiator_telegram_user_id:
        return CommandExecution(
            result=CommandResult(Outcome.REJECTED, ReasonCode.PERMISSION_DENIED),
            meeting=meeting,
        )

    if not meeting.created_by_bot:
        return CommandExecution(
            result=CommandResult(Outcome.REJECTED, ReasonCode.NOT_BOT_CREATED_EVENT),
            meeting=meeting,
        )

    if meeting.state in (MeetingState.CANCELLED, MeetingState.EXPIRED):
        return CommandExecution(
            result=CommandResult(Outcome.NOOP, ReasonCode.ALREADY_FINAL),
            meeting=meeting,
        )

    return CommandExecution(
        result=CommandResult(Outcome.OK, ReasonCode.UPDATED),
        meeting=replace(meeting, state=MeetingState.CANCELLED),
    )


def ProceedWithoutSubset(meeting: Meeting, *, actor_user_id: int) -> CommandExecution:
    if actor_user_id != meeting.initiator_telegram_user_id:
        return CommandExecution(
            result=CommandResult(Outcome.REJECTED, ReasonCode.PERMISSION_DENIED),
            meeting=meeting,
        )

    if not meeting.created_by_bot:
        return CommandExecution(
            result=CommandResult(Outcome.REJECTED, ReasonCode.NOT_BOT_CREATED_EVENT),
            meeting=meeting,
        )

    if meeting.state != MeetingState.NEEDS_INITIATOR_DECISION:
        return CommandExecution(
            result=CommandResult(Outcome.NOOP, ReasonCode.INVALID_STATE),
            meeting=meeting,
        )

    return CommandExecution(
        result=CommandResult(Outcome.OK, ReasonCode.UPDATED),
        meeting=replace(meeting, state=MeetingState.CONFIRMED),
    )
