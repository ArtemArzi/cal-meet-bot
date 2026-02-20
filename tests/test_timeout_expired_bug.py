from datetime import datetime, timedelta

from bot_vstrechi.domain import Decision, Meeting, MeetingParticipant, MeetingState
from bot_vstrechi.domain.state_machine import handle_initiator_timeout


def test_initiator_timeout_after_scheduled_start_transitions_to_expired() -> None:
    now = datetime(2026, 2, 11, 12, 0, 0)
    meeting = Meeting(
        meeting_id="m-timeout-expired",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.NEEDS_INITIATOR_DECISION,
        scheduled_start_at=now - timedelta(minutes=5),
        scheduled_end_at=now + timedelta(minutes=55),
        confirmation_round=1,
        initiator_decision_deadline_at=now - timedelta(seconds=1),
        participants=(
            MeetingParticipant(
                telegram_user_id=100,
                is_required=False,
                decision=Decision.NONE,
            ),
            MeetingParticipant(
                telegram_user_id=200,
                is_required=True,
                decision=Decision.CANCEL,
                decision_received_at=now - timedelta(minutes=20),
            ),
        ),
    )

    _, updated = handle_initiator_timeout(meeting, now=now)

    assert updated.state == MeetingState.EXPIRED
