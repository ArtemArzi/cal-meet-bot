from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from .models import (
    CommandResult,
    Decision,
    Meeting,
    MeetingParticipant,
    MeetingState,
    Outcome,
    RecurringConfirmationMode,
    ReasonCode,
)
from .policies import DEADLINE_GRACE_WINDOW, INITIATOR_TIMEOUT, in_deadline_window


def _all_required_confirmed(participants: tuple[MeetingParticipant, ...]) -> bool:
    required = tuple(p for p in participants if p.is_required)
    return all(p.decision == Decision.CONFIRM for p in required)


def _has_required_cancel(participants: tuple[MeetingParticipant, ...]) -> bool:
    return any(p.is_required and p.decision == Decision.CANCEL for p in participants)


def _has_required_no_response(participants: tuple[MeetingParticipant, ...]) -> bool:
    return any(p.is_required and p.decision == Decision.NONE for p in participants)


def apply_participant_decision(
    meeting: Meeting,
    *,
    actor_telegram_user_id: int,
    decision: Decision,
    decision_received_at: datetime,
) -> tuple[CommandResult, Meeting]:
    if meeting.state not in (
        MeetingState.PENDING,
        MeetingState.NEEDS_INITIATOR_DECISION,
    ):
        return (
            CommandResult(Outcome.NOOP, ReasonCode.LATE_RESPONSE_RECORDED),
            meeting,
        )

    if decision_received_at >= meeting.scheduled_start_at:
        return (
            CommandResult(Outcome.OK, ReasonCode.UPDATED),
            replace(meeting, state=MeetingState.EXPIRED),
        )

    if meeting.state == MeetingState.PENDING:
        if meeting.confirmation_deadline_at is None:
            return (
                CommandResult(Outcome.REJECTED, ReasonCode.INVALID_STATE),
                meeting,
            )

        if not in_deadline_window(
            decision_received_at=decision_received_at,
            confirmation_deadline_at=meeting.confirmation_deadline_at,
        ):
            return (
                CommandResult(Outcome.NOOP, ReasonCode.LATE_RESPONSE_RECORDED),
                meeting,
            )

    if meeting.state == MeetingState.NEEDS_INITIATOR_DECISION:
        if meeting.initiator_decision_deadline_at is None:
            return (
                CommandResult(Outcome.REJECTED, ReasonCode.INVALID_STATE),
                meeting,
            )

        if decision_received_at > meeting.initiator_decision_deadline_at:
            return (
                CommandResult(Outcome.NOOP, ReasonCode.LATE_RESPONSE_RECORDED),
                meeting,
            )

    updated = []
    target_found = False
    for participant in meeting.participants:
        if participant.telegram_user_id != actor_telegram_user_id:
            updated.append(participant)
            continue

        target_found = True
        if (
            participant.decision_received_at is not None
            and decision_received_at <= participant.decision_received_at
        ):
            return (
                CommandResult(Outcome.NOOP, ReasonCode.STALE_OR_OLDER_RESPONSE),
                meeting,
            )

        if participant.decision == decision:
            return (
                CommandResult(Outcome.NOOP, ReasonCode.ALREADY_RECORDED),
                meeting,
            )

        updated.append(
            replace(
                participant,
                decision=decision,
                decision_received_at=decision_received_at,
            )
        )

    if not target_found:
        return (
            CommandResult(Outcome.REJECTED, ReasonCode.PARTICIPANT_NOT_FOUND),
            meeting,
        )

    updated_meeting = replace(meeting, participants=tuple(updated))

    if _all_required_confirmed(updated_meeting.participants):
        return (
            CommandResult(Outcome.OK, ReasonCode.UPDATED),
            replace(updated_meeting, state=MeetingState.CONFIRMED),
        )

    if meeting.state == MeetingState.PENDING and _has_required_cancel(
        updated_meeting.participants
    ):
        return (
            CommandResult(Outcome.OK, ReasonCode.UPDATED),
            replace(
                updated_meeting,
                state=MeetingState.NEEDS_INITIATOR_DECISION,
                initiator_decision_deadline_at=decision_received_at + INITIATOR_TIMEOUT,
            ),
        )

    if meeting.state == MeetingState.NEEDS_INITIATOR_DECISION and (
        _has_required_cancel(updated_meeting.participants)
        or _has_required_no_response(updated_meeting.participants)
    ):
        return (
            CommandResult(Outcome.OK, ReasonCode.UPDATED),
            replace(
                updated_meeting,
                state=MeetingState.NEEDS_INITIATOR_DECISION,
                initiator_decision_deadline_at=meeting.initiator_decision_deadline_at,
            ),
        )

    return (
        CommandResult(Outcome.OK, ReasonCode.UPDATED),
        updated_meeting,
    )


def handle_confirm_deadline(
    meeting: Meeting, *, now: datetime
) -> tuple[CommandResult, Meeting]:
    if meeting.state != MeetingState.PENDING:
        return (
            CommandResult(Outcome.NOOP, ReasonCode.INVALID_STATE),
            meeting,
        )

    if meeting.confirmation_deadline_at is None:
        return (
            CommandResult(Outcome.REJECTED, ReasonCode.INVALID_STATE),
            meeting,
        )

    if now >= meeting.scheduled_start_at:
        return (
            CommandResult(Outcome.OK, ReasonCode.UPDATED),
            replace(meeting, state=MeetingState.EXPIRED),
        )

    if now < meeting.confirmation_deadline_at + DEADLINE_GRACE_WINDOW:
        return (
            CommandResult(Outcome.NOOP, ReasonCode.TOO_EARLY),
            meeting,
        )

    if _all_required_confirmed(meeting.participants):
        return (
            CommandResult(Outcome.OK, ReasonCode.UPDATED),
            replace(meeting, state=MeetingState.CONFIRMED),
        )

    if _has_required_cancel(meeting.participants) or _has_required_no_response(
        meeting.participants
    ):
        if (
            _has_required_no_response(meeting.participants)
            and not _has_required_cancel(meeting.participants)
            and meeting.recurring_confirmation_mode
            == RecurringConfirmationMode.EXCEPTIONS_ONLY
            and isinstance(meeting.series_event_id, str)
            and bool(meeting.series_event_id.strip())
        ):
            return (
                CommandResult(Outcome.OK, ReasonCode.UPDATED),
                replace(meeting, state=MeetingState.CONFIRMED),
            )
        return (
            CommandResult(Outcome.OK, ReasonCode.UPDATED),
            replace(
                meeting,
                state=MeetingState.NEEDS_INITIATOR_DECISION,
                initiator_decision_deadline_at=now + INITIATOR_TIMEOUT,
            ),
        )

    return (
        CommandResult(Outcome.NOOP, ReasonCode.INVALID_STATE),
        meeting,
    )


def handle_initiator_timeout(
    meeting: Meeting, *, now: datetime
) -> tuple[CommandResult, Meeting]:
    if meeting.state != MeetingState.NEEDS_INITIATOR_DECISION:
        return (
            CommandResult(Outcome.NOOP, ReasonCode.INVALID_STATE),
            meeting,
        )

    if meeting.initiator_decision_deadline_at is None:
        return (
            CommandResult(Outcome.REJECTED, ReasonCode.INVALID_STATE),
            meeting,
        )

    if now < meeting.initiator_decision_deadline_at:
        return (
            CommandResult(Outcome.NOOP, ReasonCode.TOO_EARLY),
            meeting,
        )

    if now >= meeting.scheduled_start_at:
        return (
            CommandResult(Outcome.OK, ReasonCode.UPDATED),
            replace(meeting, state=MeetingState.EXPIRED),
        )

    return (
        CommandResult(Outcome.OK, ReasonCode.UPDATED),
        replace(meeting, state=MeetingState.CANCELLED),
    )
