from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import StrEnum


WORKDAY_DEADLINE_HOUR = 18
TODAY_CONFIRM_BUFFER = timedelta(hours=2)
URGENT_THRESHOLD = timedelta(hours=1)
URGENT_CONFIRM_BUFFER = timedelta(minutes=10)

FAST_TRACK_THRESHOLD = URGENT_THRESHOLD
FAST_TRACK_WAIT = URGENT_CONFIRM_BUFFER
DEADLINE_GRACE_WINDOW = timedelta(seconds=5)
INITIATOR_TIMEOUT = timedelta(minutes=15)
REMINDER_INTERVAL = timedelta(minutes=5)


class ConfirmationMode(StrEnum):
    STANDARD = "standard"
    FAST_TRACK = "fast_track"
    IMMEDIATE_INITIATOR_DECISION = "immediate_initiator_decision"


@dataclass(frozen=True)
class ConfirmationPlan:
    mode: ConfirmationMode
    confirmation_deadline_at: datetime | None


def build_confirmation_plan(
    now: datetime, scheduled_start_at: datetime
) -> ConfirmationPlan:
    time_to_start = scheduled_start_at - now

    if time_to_start <= URGENT_CONFIRM_BUFFER:
        return ConfirmationPlan(
            mode=ConfirmationMode.IMMEDIATE_INITIATOR_DECISION,
            confirmation_deadline_at=None,
        )

    if time_to_start < URGENT_THRESHOLD:
        return ConfirmationPlan(
            mode=ConfirmationMode.FAST_TRACK,
            confirmation_deadline_at=scheduled_start_at - URGENT_CONFIRM_BUFFER,
        )

    if scheduled_start_at.date() == now.date():
        return ConfirmationPlan(
            mode=ConfirmationMode.STANDARD,
            confirmation_deadline_at=scheduled_start_at - TODAY_CONFIRM_BUFFER,
        )

    previous_workday = _previous_workday(scheduled_start_at.date())
    confirmation_deadline_at = datetime.combine(
        previous_workday,
        time(hour=WORKDAY_DEADLINE_HOUR),
        tzinfo=scheduled_start_at.tzinfo,
    )

    if confirmation_deadline_at <= now:
        confirmation_deadline_at = scheduled_start_at - TODAY_CONFIRM_BUFFER

    return ConfirmationPlan(
        mode=ConfirmationMode.STANDARD,
        confirmation_deadline_at=confirmation_deadline_at,
    )


def _previous_workday(target: date) -> date:
    day = target - timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


def in_deadline_window(
    *, decision_received_at: datetime, confirmation_deadline_at: datetime
) -> bool:
    return decision_received_at <= confirmation_deadline_at + DEADLINE_GRACE_WINDOW
