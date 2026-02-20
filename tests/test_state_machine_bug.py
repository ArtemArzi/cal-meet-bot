from datetime import datetime, timedelta
from bot_vstrechi.domain import (
    Decision, Meeting, MeetingParticipant, MeetingState, Outcome, ReasonCode
)
from bot_vstrechi.domain.commands import RecordParticipantDecision

def test_same_decision_in_needs_initiator_decision_returns_ok() -> None:
    now = datetime(2026, 2, 11, 10, 0, 0)
    meeting = Meeting(
        meeting_id="m-1",
        initiator_telegram_user_id=100,
        chat_id=100,
        state=MeetingState.NEEDS_INITIATOR_DECISION,
        scheduled_start_at=now + timedelta(hours=4),
        scheduled_end_at=now + timedelta(hours=5),
        confirmation_round=1,
        initiator_decision_deadline_at=now + timedelta(minutes=15),
        participants=(
            MeetingParticipant(
                telegram_user_id=200, 
                is_required=True, 
                decision=Decision.CANCEL, 
                decision_received_at=now
            ),
        ),
    )

    # Participant 200 sends CANCEL again a minute later
    duplicate_decision = RecordParticipantDecision(
        meeting,
        round=1,
        actor_user_id=200,
        decision=Decision.CANCEL,
        source="telegram",
        now=now + timedelta(minutes=1),
    )

    # It returns OK, even though the state and decision are exactly the same
    assert duplicate_decision.result.outcome == Outcome.NOOP
    assert duplicate_decision.result.reason_code == ReasonCode.ALREADY_RECORDED
