# DOMAIN KNOWLEDGE BASE

## OVERVIEW
The core business logic of the Meeting Bot. This layer is "pure" and independent of external interfaces (Telegram, Google, SQLite).

## STRUCTURE
- `models.py`: Canonical entities (Meeting, MeetingParticipant) and value objects (MeetingState, Decision, Job).
- `state_machine.py`: Side-effect-free transitions between meeting states.
- `policies.py`: Business rules (ConfirmationMode) for deadlines, reminders, and fast-track windows.
- `commands.py`: Unit-of-work generators (SelectSlot, RecordParticipantDecision) that return updated state and planned jobs.

## WHERE TO LOOK
- **States:** `models.py` -> `MeetingState`.
- **State Transitions:** `state_machine.py` -> `apply_participant_decision`.
- **Deadline Logic:** `policies.py` -> `build_confirmation_plan`.
- **Job Scheduling:** `commands.py` -> `HandleConfirmDeadline`.

## CONVENTIONS
- **Frozen Dataclasses:** All domain entities are `frozen=True`. Use `replace(obj, ...)` for mutations.
- **StrEnum for States:** All lifecycle and status constants use `StrEnum`.
- **No Side Effects:** Logic must not perform I/O. It returns intent (updated models + job specs).
- **Deterministic Time:** Functions must accept a `now` parameter. No `datetime.now()`.

## ANTI-PATTERNS
- **NEVER** import from `infrastructure`, `db`, `application`, or `telegram` here.
- **DO NOT** use ORMs or Pydantic; stay with native dataclasses.
- **DO NOT** add logging here; return results for the orchestration layer to log.
