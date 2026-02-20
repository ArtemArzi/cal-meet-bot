from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum


class MeetingState(StrEnum):
    DRAFT = "draft"
    PENDING = "pending"
    NEEDS_INITIATOR_DECISION = "needs_initiator_decision"
    CONFIRMED = "confirmed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class DecisionSource(StrEnum):
    TELEGRAM = "telegram"
    GOOGLE = "google"
    SYSTEM = "system"
    NONE = "none"


class Decision(StrEnum):
    CONFIRM = "confirm"
    CANCEL = "cancel"
    NONE = "none"


class RecurringConfirmationMode(StrEnum):
    STRICT = "strict"
    EXCEPTIONS_ONLY = "exceptions_only"


class JobType(StrEnum):
    REMINDER = "reminder"
    CONFIRM_DEADLINE = "confirm_deadline"
    INITIATOR_TIMEOUT = "initiator_timeout"


class JobStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"


class OutboxStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"


class OutboxEffectType(StrEnum):
    TELEGRAM_SEND_MESSAGE = "telegram_send_message"
    TELEGRAM_EDIT_MESSAGE = "telegram_edit_message"
    TELEGRAM_ANSWER_CALLBACK = "telegram_answer_callback"
    CALENDAR_INSERT_EVENT = "calendar_insert_event"
    CALENDAR_PATCH_EVENT = "calendar_patch_event"


class InboundEventSource(StrEnum):
    TELEGRAM_UPDATE = "telegram_update"
    TELEGRAM_CALLBACK = "telegram_callback"
    GOOGLE_WEBHOOK = "google_webhook"
    GOOGLE_POLL = "google_poll"


class CallbackActionType(StrEnum):
    PARTICIPANT_CONFIRM = "participant_confirm"
    PARTICIPANT_CANCEL = "participant_cancel"
    INITIATOR_REPLAN = "initiator_replan"
    INITIATOR_CANCEL = "initiator_cancel"
    INITIATOR_PROCEED_WITHOUT_SUBSET = "initiator_proceed_without_subset"


class Outcome(StrEnum):
    OK = "ok"
    NOOP = "noop"
    REJECTED = "rejected"


class ReasonCode(StrEnum):
    UPDATED = "updated"
    ALREADY_FINAL = "already_final"
    INVALID_STATE = "invalid_state"
    PERMISSION_DENIED = "permission_denied"
    NOT_BOT_CREATED_EVENT = "not_bot_created_event"
    TOO_EARLY = "too_early"
    LATE_RESPONSE_RECORDED = "late_response_recorded"
    ALREADY_RECORDED = "already_recorded"
    STALE_OR_OLDER_RESPONSE = "stale_or_older_response"
    PARTICIPANT_NOT_FOUND = "participant_not_found"
    SLOT_IN_PAST = "slot_in_past"
    OPTIMISTIC_CONFLICT = "optimistic_conflict"
    DUPLICATE_INBOUND_EVENT = "duplicate_inbound_event"
    INVALID_CALLBACK_FORMAT = "invalid_callback_format"
    STALE_ACTION = "stale_action"


@dataclass(frozen=True)
class CommandResult:
    outcome: Outcome
    reason_code: ReasonCode


@dataclass(frozen=True)
class MeetingParticipant:
    telegram_user_id: int
    is_required: bool = True
    decision_source: DecisionSource = DecisionSource.NONE
    decision: Decision = Decision.NONE
    decision_received_at: datetime | None = None


@dataclass(frozen=True)
class Meeting:
    meeting_id: str
    initiator_telegram_user_id: int
    chat_id: int
    state: MeetingState
    scheduled_start_at: datetime
    scheduled_end_at: datetime
    title: str = ""
    google_event_id: str | None = None
    google_calendar_id: str | None = None
    series_event_id: str | None = None
    occurrence_start_at: datetime | None = None
    group_status_message_id: int | None = None
    created_by_bot: bool = True
    confirmation_round: int = 1
    confirmation_deadline_at: datetime | None = None
    initiator_decision_deadline_at: datetime | None = None
    participants: tuple[MeetingParticipant, ...] = ()
    recurring_confirmation_mode: RecurringConfirmationMode = (
        RecurringConfirmationMode.STRICT
    )

    def with_participants(
        self, participants: tuple[MeetingParticipant, ...]
    ) -> "Meeting":
        return replace(self, participants=participants)


@dataclass(frozen=True)
class ScheduledJobSpec:
    job_type: JobType
    meeting_id: str
    round: int
    run_at: datetime


@dataclass(frozen=True)
class CallbackActionToken:
    token: str
    meeting_id: str
    round: int
    action_type: CallbackActionType
    allowed_user_id: int
    expires_at: datetime
