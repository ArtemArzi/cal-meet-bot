# CALENDAR INTEGRATION KNOWLEDGE

OVERVIEW: Client for Google Calendar API using Service Account with Domain-Wide Delegation.

## STRUCTURE
- `gateway.py`: Definition of `CalendarGateway` protocol and the `GoogleCalendarGateway` implementation.
- `client.py`: Low-level `GoogleServiceAccountCalendarClient` using `httpx` and `jwt` for DWD authentication.
- `__init__.py`: Package exports for clean imports of client and gateway components.

## WHERE TO LOOK
- Use `GoogleCalendarGateway` for all calendar interactions from the application layer.
- Check `GoogleServiceAccountCalendarClient` for raw HTTP request handling and token management.
- See `search_free_slots` in `gateway.py` for business-hour filtering and free-busy logic.
- See `list_event_deltas` in `client.py` for sync token handling and reconciliation.

## CONVENTIONS
- Always use `CalendarGateway` protocol for orchestration to enable unit testing with fakes.
- Authenticate via Service Account DWD to impersonate meeting organizers or participants.
- Pass a deterministic `now` timestamp into all time-dependent calendar methods.
- Handle reconciliation via `syncToken` and `pageToken` for incremental event updates.
- Use `CalendarOccurrenceIdentity` to handle recurring event instances correctly.

## ANTI-PATTERNS
- Do not mutate non-bot events. Check `meeting.created_by_bot` before performing updates.
- Do not bypass the gateway to call the Google API directly from application services.
- Do not hardcode credentials. Use `GoogleServiceAccountCredentials` populated from env.
- Do not use `@username` for lookups. Google Calendar API requires valid email addresses.
- Do not ignore 429 errors. The client implements exponential backoff for rate limits.
