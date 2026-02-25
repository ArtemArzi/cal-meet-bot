# Чек-лист готовности к ручному тестированию

Этот список поможет подготовить проект к тестированию в Telegram.

При запуске приложение автоматически выполняет миграции SQLite (добавляет колонки `chat_id`, `google_event_id`, `google_calendar_id`, `timezone`, если они отсутствуют).

## 1. Переменные окружения

Создайте или обновите `.env.local`:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_SECRET_TOKEN` (опционально)
- `BOT_VSTRECHI_DB_PATH=./var/bot_vstrechi.db`
- `BOT_VSTRECHI_CALENDAR_ENABLED=true`
- `GOOGLE_SA_CLIENT_EMAIL`
- `GOOGLE_SA_PRIVATE_KEY`
- `GOOGLE_SA_PRIVATE_KEY_ID` (опционально)
- `GOOGLE_SA_TOKEN_URI` (опционально, по умолчанию Google OAuth)
- `GOOGLE_IMPERSONATION_SUBJECT` (обязательно при включенном календаре)
- `GOOGLE_WEBHOOK_CHANNEL_TOKEN` (опционально)
- `LOG_LEVEL=INFO`
- `LOG_FORMAT=pretty` (pretty/json/text)

## 2. Настройка Google Workspace (DWD)

1. У сервис-аккаунта включен Domain-Wide Delegation.
2. В Google Workspace Admin -> Security -> API controls -> Domain-wide delegation добавлен Client ID сервис-аккаунта.
3. Добавлены области (scopes):
   - `https://www.googleapis.com/auth/calendar.events`
   - `https://www.googleapis.com/auth/calendar.readonly`
4. У тестовых пользователей есть календари в том же домене Workspace.
5. `GOOGLE_IMPERSONATION_SUBJECT` — это существующий почтовый ящик в том же домене.

## 3. Маппинг пользователей Telegram и Google

Запустите скрипт для привязки:

```bash
python3 scripts/seed_users.py \
  --db-path ./var/bot_vstrechi.db \
  --telegram-user-id 454049469 \
  --google-email user@example.com
```

Можно также добавить username:

```bash
python3 scripts/seed_users.py \
  --db-path ./var/bot_vstrechi.db \
  --telegram-user-id 454049469 \
  --telegram-username user_tg \
  --google-email user@example.com
```

## 4. Запуск сервера

```bash
./run_local.sh
```

Доступные эндпоинты:

- `POST /telegram/webhook`
- `POST /calendar/webhook`
- `GET /health`
- `GET /readiness`

## 5. Настройка вебхуков

Пример с использованием ngrok:

```bash
ngrok http 8000
```

Установите вебхук для Telegram:

```bash
curl -sS "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/setWebhook" \
  -d "url=${PUBLIC_URL}/telegram/webhook" \
  -d "secret_token=${TELEGRAM_SECRET_TOKEN}"
```

## 6. Сценарии ручного тестирования

В Telegram:

### Базовые команды
1. `/start` — проверка: показывает статус маппинга и кнопки меню.
2. `/help` — проверка: объясняет логику работы с календарем.
3. `/chat` — проверка: доступна только менеджерам в ЛС, запрашивает chat_id и сохраняет целевой чат статусов.
4. `/people` — проверка: доступна только менеджерам в ЛС. Для теста убедитесь, что ваш `telegram_user_id` помечен как менеджер в БД (таблица `users`, колонка `is_manager=1`).

### Проверка календаря
5. Создайте событие в Google Calendar.
6. Проверьте логи: `POST /calendar/webhook` должен вернуть 200.
7. Убедитесь, что встреча появилась в БД и участникам ушли уведомления.

### Путь участника
8. Участник нажимает «Подтвердить» в ЛС. Проверьте смену статуса и уведомления.
9. Участник нажимает «Отклонить». Если требуется решение организатора, проверьте переход в статус `needs_initiator_decision`.

### Путь организатора
10. Организатор получает кнопки для принятия решения в ЛС.
11. Выберите действие и проверьте финальный статус (`confirmed` или `cancelled`).
12. Проверьте, что событие в календаре обновилось.

### Обновление события
13. Перенесите время события в Google Calendar. Цикл подтверждения должен начаться заново.
14. Удалите событие в календаре. Встреча должна перейти в `cancelled`.

Ожидаемый результат: отсутствие 5xx в логах, записи в `audit_log` соответствуют переходам, аутбокс успешно отправляет сообщения.
