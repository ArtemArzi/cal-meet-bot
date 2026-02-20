# PROJECT KNOWLEDGE BASE - Bot Vstrechi

**Generated:** 2026-02-20
**Commit:** no-commit
**Branch:** no-branch

## OVERVIEW
Calendar-first Telegram bot for meeting confirmations. Google Calendar is the source of truth. Built with Python 3.12 using Hexagonal (Clean) Architecture.

## STRUCTURE
```
src/bot_vstrechi/
├── domain/         # Pure business logic (state machine, policies)
├── application/    # Orchestration layer (MeetingWorkflowService)
├── infrastructure/ # Low-level adapters (logging, settings, runtime)
├── db/             # Persistence (SQLite, Repository, Outbox)
├── telegram/       # Telegram adapter and presentation
├── calendar/       # Google Calendar API adapter
├── workers/        # Asynchronous background processors
├── api/            # Webhook endpoints (FastAPI)
└── asgi.py         # App entry point
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Business Rules | `src/bot_vstrechi/domain/` | State transitions, deadline policies |
| DB Queries | `src/bot_vstrechi/db/repository.py` | Raw SQL, atomic transactions |
| Bot UI/Commands | `src/bot_vstrechi/telegram/` | Keyboards, message formatting |
| API Integration | `src/bot_vstrechi/calendar/` | Google Calendar API calls |
| Sync/Jobs | `src/bot_vstrechi/workers/` | Outbox, CalendarSync, Scheduler |

## CONVENTIONS
- **Hexagonal Layering:** Domain (pure) → Application (service) → Infrastructure/Adapters.
- **Side-Effect-Free Domain:** Business logic computes intent; service layer executes it.
- **Deterministic Time:** `now` parameter must be passed into all time-dependent functions.
- **Atomic Persistence:** All state changes + outbox + jobs enqueued in one transaction.

## ANTI-PATTERNS (THIS PROJECT)
- **NO ORMs:** Raw SQL with dataclasses only.
- **NO Pydantic in Domain:** Use native frozen dataclasses.
- **NO Meeting Creation via Bot:** Meetings must originate from Google Calendar.
- **NO Identity via @username:** `telegram_user_id` (int) is the only primary key.

## COMMANDS
```bash
# Setup
pip install -e .
# Run (Dev)
uvicorn src.bot_vstrechi.asgi:app --reload
# Tests
pytest
```

## NOTES
- **Identity Safety:** `@username` is only an alias; never use as PK.
- **Idempotency:** Inbound events are deduplicated via `InboundEventDedup`.
- **Reliability:** Side-effects (TG messages, Calendar updates) use the Outbox Pattern.

