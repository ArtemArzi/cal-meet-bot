# AGENTS.md - Asynchronous Workers

## OVERVIEW
Reliable background processors for calendar synchronization, deadline management, and outbox delivery.

## STRUCTURE
```
src/bot_vstrechi/workers/
├── outbox.py        # OutboxWorker: Delivers Telegram messages
├── calendar_sync.py # CalendarSyncWorker: Scans GCalendar events
└── scheduler.py     # SchedulerWorker: Processes timeouts and reminders
```

## WHERE TO LOOK
- **Reliable Side-Effects:** Check `outbox.py` for how the system ensures Telegram messages are sent exactly once or retried.
- **Calendar Updates:** Check `calendar_sync.py` for the polling logic that identifies new or modified meetings.
- **Business Timing:** Check `scheduler.py` for the state machine transitions triggered by deadlines and participant timeouts.

## CONVENTIONS
- **Long-Running Processes:** All workers run as infinite loops, polling the SQLite database for work items.
- **DB Polling:** Use a wait-interval (e.g., 1-5 seconds) between poll cycles to keep CPU usage low.
- **Idempotency:** Handle event duplication via `InboundEventDedup` or unique constraint checks in the repository layer.
- **Outbox Pattern:** Side-effects must never be executed directly in the main workflow; they are staged in the `outbox` table and delivered by `OutboxWorker`.

## ANTI-PATTERNS
- **NO HTTP calls in main thread:** Never perform Google Calendar or Telegram API calls outside of a worker process.
- **NO sleep() in logic:** Use the worker's polling interval or `now` parameter for time-based logic.
- **NO State Mutations outside Workers/API:** Only workers and the main API should transition meeting states to avoid race conditions.
