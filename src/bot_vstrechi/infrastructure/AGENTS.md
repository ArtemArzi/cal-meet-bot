# INFRASTRUCTURE KNOWLEDGE BASE

## OVERVIEW
Runtime composition layer: loads settings, wires dependencies, configures logging, and controls app/worker lifecycle.

## STRUCTURE
- `settings.py`: Environment parsing and defaults.
- `bootstrap.py`: Creates runtime dependencies (Telegram/Calendar clients).
- `runtime.py`: DI graph, startup recovery, polling loop, FastAPI app factory.
- `logging.py`: `json|pretty|text` formatter setup.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Env vars/defaults | `settings.py` | `BOT_VSTRECHI_*`, Google, logging, retention |
| Dependency wiring | `bootstrap.py` | Validation for calendar credentials |
| Worker lifecycle | `runtime.py` | `startup`, reconcile, polling loop |
| API app creation | `runtime.py` | `create_application`, readiness probe |

## CONVENTIONS
- Keep composition here; business decisions belong to `domain/` and `application/`.
- Runtime timestamps use UTC provider; pass `now` into worker/service calls.
- Startup must reconcile stale `running` records for jobs/outbox/calendar signals.
- Readiness endpoint checks DB connectivity.

## ANTI-PATTERNS
- Do not duplicate domain logic in runtime/bootstrap.
- Do not bypass `Settings`; avoid hardcoded env reads in other layers.
- Do not instantiate external clients inside domain/application modules.
