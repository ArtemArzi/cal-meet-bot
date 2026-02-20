# Backend Detailed Design (MVP)

## 1. Document purpose

Этот документ фиксирует каноничную backend-архитектуру Telegram-бота встреч для MVP.

- Это source of truth для доменной модели, состояний, дедлайнов, scheduler, интеграций и инвариантов.
- Этот документ достаточен, чтобы начать реализацию без дополнительных архитектурных сессий.
- Telegram UX и тексты сообщений описаны в `docs/bot-interaction-flow.md`, но не переопределяют backend-инварианты.

## 2. Scope and non-goals

### In scope (MVP)

- Calendar-first поток: события создаются/переносятся/отменяются в Google Calendar, Telegram используется для уведомлений, подтверждений и эскалаций.
- Telegram command surface ограничен `/start`, `/help`, `/people`.
- Жизненный цикл подтверждения: `pending -> needs_initiator_decision -> confirmed/cancelled/expired`.
- Google Calendar (Workspace + DWD), Telegram Bot API.
- SQLite как primary storage.
- SQLite-backed job scheduling для reminder/deadline/timeout.

### Calendar sync contract (MVP)

- Polling fallback является обязательным и самодостаточным механизмом доставки изменений.
- Webhook используется как ускоритель latency, но не является единственной гарантией обработки.
- Поля watch-канала (`watch_channel_id`, `watch_resource_id`, `watch_expiration_at`) сохраняются в sync state как metadata, но отсутствие активного watch не блокирует processing.

### Out of scope

- Outlook/Apple/Yandex calendars.
- Мульти-таймзоны.
- Recurring events.
- Horizontal scaling в MVP.
- Auto-room booking / advanced analytics.

## 3. Precedence Rule

При расхождениях источников применяется следующий порядок:

1. `OVERVIEW.md` — продуктовый канон поведения.
2. `docs/backend-detailed-design.md` — канон технической реализации.
3. `docs/bot-interaction-flow.md` — интерфейсный контракт, не может переопределять backend-инварианты.

## 4. Reference Integrity

Допустимые источники требований:

- `OVERVIEW.md`
- `docs/backend-detailed-design.md`
- `docs/bot-interaction-flow.md`

Недопустимо ссылаться на удаленные draft-файлы или старые планы как на нормативный источник.

## 5. High-level architecture

## 5.1 Components

- Telegram Adapter (webhook handler): принимает updates, валидирует источник, превращает в команды приложения.
- Application Services: `ProposeSlots`, `CreateMeetingDraft`, `ListMeetingDaySlots`, `SelectSlot`, `RecordParticipantDecision`, `HandleDeadlineTick`, `RescheduleMeeting`, `CancelMeeting`.
- Domain Layer: state machine, policies, invariants.
- Persistence Layer (SQLite): users, mappings, meetings, participants, jobs, idempotency, audit.
- Scheduler Worker: исполняет due jobs (`reminder`, `confirm_deadline`, `initiator_timeout`).
- Outbox Worker: исполняет отложенные side-effects (Telegram messages, Calendar mutations).
- Calendar Sync Worker: выполняет polling Google Calendar для обнаружения внешних изменений.
- Google Calendar Gateway: FreeBusy/Events.list/get/insert/patch/delete под impersonation инициатора.

## 5.2 Deployment model

- Single-instance process (MVP guardrail).
- Один API/webhook процесс + один worker loop в том же процессе.
- SQLite в WAL mode.

## 6. Domain model

## 6.1 Entities

- `UserMapping`
  - `id`
  - `telegram_user_id` (UNIQUE, canonical)
  - `telegram_username` (nullable alias)
  - `google_email` (UNIQUE)
  - `is_active`
  - `created_at`, `updated_at`

- `Meeting`
  - `id` (UUID)
  - `title`
  - `duration_minutes`
  - `initiator_telegram_user_id`
  - `initiator_google_email`
  - `chat_context_type` (`dm|group`)
  - `chat_context_id`
  - `state` (`draft|pending|needs_initiator_decision|confirmed|cancelled|expired`)
  - `state_updated_at`
  - `scheduled_start_at`, `scheduled_end_at`
  - `timezone`
  - `confirmation_deadline_at`
  - `initiator_decision_deadline_at` (nullable)
  - `google_calendar_id` (инициатора)
  - `google_event_id`
  - `created_by_bot` (bool, MUST be true для управляемых событий)
  - `confirmation_round` (int, starts at 1; increment on reschedule/new round)
  - `created_at`, `updated_at`

- `MeetingParticipant`
  - `id`
  - `meeting_id`
  - `telegram_user_id`
  - `google_email`
  - `is_required` (MVP: true для всех обязательных)
  - `decision_source` (`telegram|google|system|none`)
  - `decision` (`confirm|cancel|none`)
  - `decision_received_at` (server timestamp)
  - `last_seen_round`
  - UNIQUE(`meeting_id`, `telegram_user_id`)

- `Job`
  - `id`
  - `job_type` (`reminder|confirm_deadline|initiator_timeout`)
  - `meeting_id`
  - `round`
  - `run_at`
  - `status` (`pending|running|done|failed|cancelled`)
  - `attempt`
  - `payload_json`
  - UNIQUE(`job_type`, `meeting_id`, `round`, `run_at`)

- `InboundEventDedup`
  - `id`
  - `source` (`telegram_update|telegram_callback|google_poll`)
  - `external_event_id`
  - `processed_at`
  - UNIQUE(`source`, `external_event_id`)

- `AuditLog`
  - `id`
  - `meeting_id`
  - `actor_type` (`initiator|participant|system`)
  - `actor_telegram_user_id` (nullable)
  - `event_type`
  - `details_json`
  - `created_at`

## 6.2 Invariants

- `telegram_user_id` MUST быть canonical identity.
- `@username` MUST NOT использоваться как primary key.
- Список required участников фиксируется в момент выбора слота для текущего `confirmation_round`.
- Изменение встречи ботом допускается ONLY для `created_by_bot=true`.
- Только инициатор может выполнять reschedule/cancel.

## 7. State machine (canonical)

## 7.1 States

- `draft`: (Internal) встреча создана, но слот еще не выбран или инициатор в процессе подбора.
- `pending`: выбран слот, идёт сбор явных решений.
- `needs_initiator_decision`: есть отказ/неответ к дедлайну или срочный кейс.
- `confirmed`: meeting finalized.
- `cancelled`: явная отмена инициатором или авто-отмена по initiator timeout.
- `expired`: слот устарел до финального решения (например, start time passed).

## 7.2 Transition rules

| From | Trigger | To | Side effects |
|------|---------|----|--------------|
| draft/new | initiator selects slot | pending | create/patch Google event, schedule jobs, send initial requests |
| pending | all required confirmed before deadline | confirmed | cancel pending jobs, notify participants |
| pending | required cancel OR required no-response at deadline | needs_initiator_decision | schedule initiator timeout, notify initiator with options |
| pending | now > scheduled_start_at and no final decision | expired | cancel jobs, notify initiator |
| needs_initiator_decision | initiator chooses replan | pending (new round) | increment round, recalc deadline, reschedule jobs |
| needs_initiator_decision | initiator chooses cancel | cancelled | delete/cancel event, cancel jobs |
| needs_initiator_decision | initiator chooses proceed without subset | confirmed | patch attendees in Google event, audit decision |
| needs_initiator_decision | initiator timeout 15m | cancelled | system cancellation, notify |

## 8. Deadline and reminder policy

### 8.1 Confirmation Deadlines
Алгоритм расчета `confirm_deadline_at` (из `policies.py`):

- **IMMEDIATE**: Если `time_to_start <= 10m` -> переход сразу в `needs_initiator_decision` (режим `IMMEDIATE_INITIATOR_DECISION`).
- **FAST_TRACK**: Если `time_to_start < 1h` -> `deadline = start - 10m`.
- **STANDARD (today)**: Если встреча сегодня -> `deadline = start - 2h`.
- **STANDARD (future)**: Встречи на будущие дни -> `deadline = 18:00` предыдущего рабочего дня.

### 8.2 Other Policies
- **DEADLINE_GRACE_WINDOW**: 5 секунд (допустимое опоздание ответа).
- **INITIATOR_TIMEOUT**: 15 минут (время на решение инициатора в `needs_initiator_decision`).
- **REMINDER_INTERVAL**: 5 минут (интервал напоминаний участникам).

- Напоминания для non-responders:
  - normal window: каждые 5m,
  - short window (<5m): initial + one short reminder.
- Недоставляемость сообщения трактуется как `no response`.

## 9. Response ingestion policy

## 9.1 Explicit response channels

- Telegram callback (`confirm` / `cancel`).
- Google RSVP (accepted / declined).

## 9.2 Resolution rule

- До дедлайна действует `latest response wins` на уровне `decision_received_at` (server time).
- После дедлайна response логируется в `AuditLog`, но НЕ делает auto-transition назад.

## 9.3 Deadline race tie-break

- Сервер применяет grace window 5s к confirm deadline.
- Если `decision_received_at <= confirmation_deadline_at + 5s`, решение считается в дедлайн.
- Все transition-решения выполняются транзакционно с optimistic check по `meeting.state` + `confirmation_round`.

## 10. Scheduler design (SQLite-backed jobs)

- Job storage: таблица `Job` в SQLite.
- Worker polling interval: 1s-2s (configurable).
- Execution flow:
  1. pick due `pending` jobs ordered by `run_at`.
  2. atomically mark `running`.
  3. execute handler.
  4. mark `done`/`failed` and retry if retryable.

### Dedup and restart safety

- Jobs uniquely keyed by (`job_type`, `meeting_id`, `round`, `run_at`).
- On startup worker reconciles stale `running` jobs older than threshold back to `pending`.
- On round change previous round jobs are cancelled.

## 11. Google Calendar integration contract

## 11.1 Organizer model

- Organizer MUST be initiator.
- Bot acts via DWD impersonation of `initiator_google_email`.

## 11.2 Source of truth policy

- Bot DB state is workflow truth.
- Google event is synchronization target and secondary confirmation source (RSVP polling/list/get).

## 11.3 API usage

- FreeBusy for slot search (batch up to 50 calendars per request; for >50 use chunking).
- Events.insert on new pending cycle.
- Events.patch on reschedule or proceed-without subset.
- Events.delete on explicit/system cancel.

## 11.4 External edits policy

- If event time/attendees changed externally during `pending`, bot writes audit and moves to `needs_initiator_decision` (no silent overwrite).
- If event deleted externally, bot moves to `cancelled` with reason `external_delete`.

## 12. Telegram integration contract

- Webhook update handling MUST be idempotent by `update_id`/`callback_query.id`.
- Callback payload MUST be tokenized (not full business data) and validated against chat/user/round.
- Stale callback MUST return deterministic response (`Action is no longer valid`) without state mutation.

## 13. Idempotency and concurrency

- Inbound dedup table blocks repeated processing.
- Every write path uses transaction + `WHERE state=... AND confirmation_round=...` guards.
- Side effects (message send, calendar patch) must be outbox-like or retried safely with idempotent keys.

## 14. Security and compliance

- Service account key stored in secret storage, never in repository.
- Minimum OAuth scopes only (`calendar.events`, `calendar.readonly`).
- Audit log records all state transitions and actor actions.
- PII exposure constrained by visibility rules from `OVERVIEW.md`.

## 15. Observability

- Structured logs with keys: `meeting_id`, `round`, `state_from`, `state_to`, `job_type`, `actor`.
- Metrics:
  - pending_to_confirmed_rate
  - pending_to_needs_decision_rate
  - reminder_send_failures
  - callback_dedup_hits
  - deadline_race_conflicts
- Alerts (MVP): repeated job failures, calendar API 5xx bursts, DB busy timeout spikes.

## 16. Error handling and retries

- Retryable: network timeouts, 429/5xx from Google/Telegram.
- Non-retryable: invalid mapping, unauthorized actor, stale callback.
- Retry policy: exponential backoff, capped attempts; then audit + operator-visible error.

## 17. Launch Gates

## 17.1 dev

- Entry: local env + test credentials + migrations applied.
- Exit: happy path calendar-driven sync + Telegram confirmation pass; transitions logged correctly.

## 17.2 staging-like

- Entry: isolated token and workspace test users.
- Exit: restart-safe reminders verified; duplicate callback idempotency verified.

## 17.3 manual QA

- Entry: runbook approved and seeded test data present.
- Exit: all manual scenarios pass (happy/decline/no-response/fast-track/<3m/initiator-timeout).

## 17.4 rollout

- Entry: go-live checklist + rollback plan + alerting enabled.
- Exit: pilot group stable under agreed metrics.

## 18. Hybrid QA strategy

### Minimal automated tests (must-have)

- `test_pending_to_confirmed_when_all_required_confirm`
- `test_transition_to_needs_initiator_decision_on_deadline_no_response`
- `test_callback_idempotency_duplicate_press_returns_same_result`
- `test_fast_track_window_behavior`
- `test_less_than_3m_goes_directly_to_needs_initiator_decision`

### Manual E2E (must-have)

Подробный chat-level runbook описан в `docs/bot-interaction-flow.md`.

## 19. External contract for bot adapter

Bot adapter вызывает только эти application commands:

- `ProposeSlots(input)`
- `CreateMeetingDraft(input)`
- `ListMeetingDaySlots(meetingId, durationMinutes, timezone, day)`
- `SelectSlot(meetingId, actorUserId, chat_id, scheduled_start_at, scheduled_end_at)`
- `RecordParticipantDecision(meetingId, round, actorUserId, decision, source)`
- `HandleConfirmDeadline(meetingId, round)`
- `HandleInitiatorTimeout(meetingId, round)`
- `RescheduleMeeting(meetingId, actorUserId, chat_id, scheduled_start_at, scheduled_end_at)`
- `CancelMeeting(meetingId, actorUserId, reason)`
- `ProceedWithoutSubset(meetingId, actorUserId)`

Ответ каждого command MUST быть детерминированным outcome (`ok|noop|rejected`) + reason code.

## 20. Traceability Matrix

| Requirement ID | Requirement | Backend Section | Bot Section |
|----------------|-------------|-----------------|-------------|
| R1 | Lifecycle states | §7 | `docs/bot-interaction-flow.md` §8 |
| R2 | Deadline formula | §8 | `docs/bot-interaction-flow.md` §10 |
| R3 | Fast-track <1h | §8 | `docs/bot-interaction-flow.md` §10 |
| R4 | <=10m immediate decision | §8 | `docs/bot-interaction-flow.md` §10 |
| R5 | Initiator timeout 15m => cancelled | §7.2, §8 | `docs/bot-interaction-flow.md` §12 |
| R6 | Latest response wins | §9.2 | `docs/bot-interaction-flow.md` §11 |
| R7 | Telegram callback and/or Google RSVP | §9.1 | `docs/bot-interaction-flow.md` §11 |
| R8 | Reschedule/cancel only initiator | §6.2, §7.2 | `docs/bot-interaction-flow.md` §5 |
| R9 | user_id canonical | §6.1, §6.2 | `docs/bot-interaction-flow.md` §6 |
| R10 | bot-created events only | §6.2, §11.3 | `docs/bot-interaction-flow.md` §9 |
| R11 | single-instance MVP | §5.2 | `docs/bot-interaction-flow.md` §3 |
| R12 | Manual E2E runbook required | §18 | `docs/bot-interaction-flow.md` §15 |

## Appendix A. Consistency Checklist (PASS/FAIL)

| Item | Result |
|------|--------|
| States include `draft`, `pending`, `needs_initiator_decision`, `confirmed`, `cancelled`, `expired` | PASS |
| Deadline formula (STANDARD/FAST_TRACK/IMMEDIATE) from policies.py | PASS |
| Fast-track `<1h` window | PASS |
| <=10m immediate initiator decision | PASS |
| Initiator timeout 15m => `cancelled` | PASS |
| Channels: Telegram callback and/or Google RSVP; latest response wins | PASS |
| Roles: reschedule/cancel only initiator | PASS |
| Guardrails: single-instance + bot-created events only + user_id canonical | PASS |
