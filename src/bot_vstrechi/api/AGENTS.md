# API LAYER KNOWLEDGE BASE

## OVERVIEW
Thin FastAPI transport for Telegram and Google webhooks with health/readiness probes.

## STRUCTURE
- `webhook.py`: `create_webhook_app` and HTTP routes.
- `__init__.py`: API package export surface.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Telegram webhook behavior | `webhook.py` | `/telegram/webhook`, secret token check |
| Google webhook behavior | `webhook.py` | `/calendar/webhook`, channel token check |
| Ops probes | `webhook.py` | `/health`, `/readiness` |

## CONVENTIONS
- API handlers stay transport-only; workflow changes go through `TelegramWebhookAdapter`.
- Token mismatches must return `403`.
- Update payload must be validated as JSON object before adapter call.
- Readiness returns `503` when DB probe fails.

## ANTI-PATTERNS
- Do not mutate meeting state directly from HTTP handlers.
- Do not embed business text/decision rules in API endpoints.
- Do not skip secret-token verification when configured.
