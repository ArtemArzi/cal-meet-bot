# PROJECT KNOWLEDGE BASE - Bot Vstrechi

**Generated:** 2026-02-25
**Commit:** 51a12bb
**Branch:** main

## OVERVIEW
Calendar-first Telegram bot for meeting confirmations. Google Calendar is the source of truth; Telegram is the interaction channel. Python 3.12, FastAPI, SQLite (WAL), Outbox workers.

## STRUCTURE
```
src/bot_vstrechi/
├── domain/         # Pure state machine, policies, immutable models
├── application/    # Workflow orchestration and outbox/job enqueue
├── db/             # SQLite schema, repository, claims, retention
├── telegram/       # Webhook adapter, callback tokens, rendering, TG client
├── calendar/       # Google Calendar client + gateway
├── workers/        # Outbox/Scheduler/CalendarSync loops
├── infrastructure/ # Runtime wiring, settings, logging, bootstrap
├── api/            # FastAPI webhook endpoints
├── asgi.py         # ASGI app entrypoint
└── worker_entrypoint.py # standalone background loop entrypoint
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Lifecycle transitions | `src/bot_vstrechi/domain/state_machine.py` | Canonical state graph |
| Main orchestration | `src/bot_vstrechi/application/service.py` | Transaction boundaries + outbox sync |
| Persistence/SQL | `src/bot_vstrechi/db/repository.py` | Raw SQL, WAL, atomic writes |
| Telegram callbacks | `src/bot_vstrechi/telegram/adapter.py` | Token validation + command routing |
| Calendar sync | `src/bot_vstrechi/workers/calendar_sync.py` | Polling/webhook reconciliation |
| Reliable delivery | `src/bot_vstrechi/workers/outbox.py` | Retries, fallback, finalization chain |
| Runtime wiring | `src/bot_vstrechi/infrastructure/runtime.py` | Startup/shutdown and worker loop |

## CONVENTIONS
- **Layering:** Domain -> Application -> Adapters/Infrastructure.
- **Deterministic time:** Pass `now` into time-dependent logic; avoid direct clock calls in domain/application.
- **Atomicity:** State updates + outbox + jobs must be committed in one transaction.
- **Identity model:** Primary identity is `telegram_user_id` (int). `@username` is display/search alias only.
- **Side effects:** Telegram and Calendar calls go through Outbox/Workers, not direct from workflow transitions.

## ANTI-PATTERNS (PROJECT-SPECIFIC)
- **No ORM layer:** Raw SQL only in repository.
- **No domain I/O:** No HTTP, DB, logging side effects in `domain/`.
- **No meeting creation from Telegram commands:** Meetings originate from Google Calendar flow.
- **No username-as-PK logic:** Never key business logic by `@username`.

## COMMANDS
```bash
# install
pip install -e .

# run API (dev)
uvicorn src.bot_vstrechi.asgi:app --reload

# run background workers (single process loop)
PYTHONPATH=src python3 -m bot_vstrechi.worker_entrypoint

# tests
pytest
```

## MODULE DOCS
- `src/bot_vstrechi/domain/AGENTS.md`
- `src/bot_vstrechi/application/AGENTS.md`
- `src/bot_vstrechi/db/AGENTS.md`
- `src/bot_vstrechi/telegram/AGENTS.md`
- `src/bot_vstrechi/calendar/AGENTS.md`
- `src/bot_vstrechi/workers/AGENTS.md`
- `src/bot_vstrechi/infrastructure/AGENTS.md`
- `src/bot_vstrechi/api/AGENTS.md`
- `tests/AGENTS.md`
