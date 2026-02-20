# Bot Interaction Flow (Calendar-First MVP)

## 1. Purpose

Этот документ фиксирует актуальный Telegram UX-контракт для calendar-first режима.

- Google Calendar является источником изменений по встречам.
- Telegram используется для уведомлений, подтверждений и эскалаций.
- Каноничная доменная логика находится в `docs/backend-detailed-design.md`.

## 2. Scope

В scope:

- Команды `/start`, `/help`, `/people`.
- Confirmation lifecycle: `pending -> needs_initiator_decision -> confirmed/cancelled/expired`.
- Callback-токены решений участников/инициатора (`act:*`) и stale-action поведение.
- DM reminders для участников без решения и эскалации менеджерам при недоставке DM.

Out of scope:

- Текстовые команды планирования/переноса/отмены в Telegram.
- Command-first wizard сценарии.

## 3. Command Contract

| Command | Purpose | Context | Auth |
|---|---|---|---|
| `/start` | Проверка привязки + краткий onboarding | DM/GROUP | All |
| `/help` | Описание calendar-first режима | DM/GROUP | All |
| `/people` | Управление списком участников | DM only | Managers only |

Если бот получает неподдерживаемую команду, неверный тип обновления или команду в неверном контексте, он возвращает `noop` (INVALID_STATE) без мутаций workflow.

## 4. High-Level UX Flow

1. Инициатор создает или редактирует событие в Google Calendar.
2. Google webhook/polling сигнал попадает в sync pipeline.
3. Бот синхронизирует состояние встречи и запускает/обновляет confirmation cycle.
4. В групповой чат публикуется статус встречи (single-message lifecycle).
5. Участники получают DM с кнопками подтверждения.
6. При дедлайне без полного подтверждения инициатор получает выбор (перенос/отмена/продолжить).

## 5. Callback Contract

- Все action callback-данные токенизированы (`act:<token>`).
- Токены валидируются через `CallbackTokenService`.
- `allowed_user_id` в токене обязателен.
- Просроченные, чужие или невалидные токены возвращают stale-action ответ.
- После валидного действия callback-кнопки очищаются edit-операцией.

## 6. Manager Decisions

Когда встреча переходит в `needs_initiator_decision` (таймаут участников):
- Инициатор получает DM с кнопками для принятия финального решения.
- Решение инициатора обрабатывается через тот же механизм токенизированных callback-ов.
- Таймаут на решение инициатора (15м) приводит к автоматической отмене встречи.

## 6. Notification Rules

- В `pending`:
  - в group: статус ожидания;
  - в DM required-участникам: кнопки confirm/cancel и дедлайн ответа.
- В `needs_initiator_decision`:
  - инициатор получает DM с decision-кнопками;
  - group получает только статус ожидания решения инициатора.
- В `confirmed` / `cancelled` / `expired`:
  - group и релевантные участники получают финальный статус.

## 7. Manual E2E Runbook (Short)

1. В Google Calendar создать/изменить событие с участниками из активных user mappings.
2. Убедиться, что webhook/sync сигнал обработан и встреча появилась/обновилась в БД.
3. Проверить outbox: есть group update + participant DM сообщения.
4. Нажать callback confirm/cancel в DM и проверить state transition.
5. Проверить финальные уведомления в group/DM и audit log.

## 8. Invariants

- Не использовать `@username` как primary identity; только `telegram_user_id`.
- Не добавлять новые lifecycle states.
- Не мутировать внешние не-bot-managed события.
- Все команды, не входящие в контракт, игнорируются (noop).
