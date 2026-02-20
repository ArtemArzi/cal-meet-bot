# TESTING KNOWLEDGE BASE

## OVERVIEW
Comprehensive testing suite using `pytest`. Follows phased organization matching architectural milestones.

## STRUCTURE
- `conftest.py`: Shared fixtures and path management.
- `test_foundation.py`: Core domain logic and state transitions.
- `test_phase2-8`: Phased feature integration (Persistence, Integrations, Transport, etc.).
- `test_calendar_*`: Dedicated suites for reconciliation and sync workers.
- `test_docs_consistency.py`: Verification that docs and implementation align.

## CONVENTIONS
- **Hand-rolled Fakes:** Use explicit `Fake` classes (e.g., `FakeTelegramClient`) instead of `unittest.mock`.
- **Deterministic Time:** Inject `now_provider`. Avoid `datetime.now()`.
- **Database Isolation:** Use `tmp_path` fixture for per-test SQLite instances.
- **Phased Naming:** Follow `test_phaseN_...` for build sequence.

## ANTI-PATTERNS
- **NEVER** delete or skip failing tests; fix the root cause.
- **DO NOT** use live API credentials in automated tests.
- **DO NOT** rely on a global `local.db`; use temporary files.
- **AVOID** brittle mock-expectations; assert on final state or Outbox entries.
