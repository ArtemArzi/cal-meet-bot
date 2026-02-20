from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from bot_vstrechi.domain.policies import ConfirmationMode, build_confirmation_plan


def test_deadline_matrix_normal_today_urgent() -> None:
    now = datetime(2026, 2, 11, 10, 0, 0)

    normal = build_confirmation_plan(
        now=now,
        scheduled_start_at=datetime(2026, 2, 12, 15, 0, 0),
    )
    assert normal.mode == ConfirmationMode.STANDARD
    assert normal.confirmation_deadline_at == datetime(2026, 2, 11, 18, 0, 0)

    today = build_confirmation_plan(
        now=now,
        scheduled_start_at=datetime(2026, 2, 11, 16, 0, 0),
    )
    assert today.mode == ConfirmationMode.STANDARD
    assert today.confirmation_deadline_at == datetime(2026, 2, 11, 14, 0, 0)

    urgent = build_confirmation_plan(
        now=now,
        scheduled_start_at=datetime(2026, 2, 11, 10, 40, 0),
    )
    assert urgent.mode == ConfirmationMode.FAST_TRACK
    assert urgent.confirmation_deadline_at == datetime(2026, 2, 11, 10, 30, 0)

    immediate = build_confirmation_plan(
        now=now,
        scheduled_start_at=datetime(2026, 2, 11, 10, 8, 0),
    )
    assert immediate.mode == ConfirmationMode.IMMEDIATE_INITIATOR_DECISION
    assert immediate.confirmation_deadline_at is None


def test_timezone_based_deadline_resolution() -> None:
    tz = ZoneInfo("Asia/Yekaterinburg")
    now = datetime(2026, 2, 11, 10, 0, 0, tzinfo=tz)
    scheduled = datetime(2026, 2, 12, 15, 0, 0, tzinfo=tz)

    plan = build_confirmation_plan(now=now, scheduled_start_at=scheduled)

    assert plan.mode == ConfirmationMode.STANDARD
    assert plan.confirmation_deadline_at == datetime(
        2026,
        2,
        11,
        18,
        0,
        0,
        tzinfo=tz,
    )


def test_previous_workday_skips_weekend() -> None:
    now = datetime(2026, 2, 13, 12, 0, 0)
    scheduled = datetime(2026, 2, 16, 11, 0, 0)

    plan = build_confirmation_plan(now=now, scheduled_start_at=scheduled)

    assert plan.mode == ConfirmationMode.STANDARD
    assert plan.confirmation_deadline_at == datetime(2026, 2, 13, 18, 0, 0)

def test_friday_evening_deadlock_returns_deadline_in_future() -> None:
    now = datetime(2026, 2, 13, 19, 0, 0)  # Friday 19:00
    scheduled = datetime(2026, 2, 16, 9, 0, 0)  # Monday 09:00

    plan = build_confirmation_plan(now=now, scheduled_start_at=scheduled)

    assert plan.confirmation_deadline_at is not None
    assert plan.confirmation_deadline_at > now
