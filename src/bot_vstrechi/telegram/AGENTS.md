# TELEGRAM INTERFACE KNOWLEDGE BASE

## OVERVIEW
Adapter layer for Telegram Bot API. Handles updates, command routing, and message presentation.

## STRUCTURE
- `adapter.py`: `TelegramWebhookAdapter` handles routing for `/start`, `/help`, `/people`.
- `presentation.py`: Human-friendly formatting, keyboards, and `BOT_COMMANDS`.
- `callback_tokens.py`: `CallbackTokenService` for secure `act:{token}` lifecycle.
- `client.py`: `HttpxTelegramClient` for outbound API calls.

## WHERE TO LOOK
- **Update Handling:** `adapter.py` -> `handle_update`.
- **Manager Tools:** `/people` flow (Manager-only, DM-only).
- **Keyboards:** `presentation.py` -> `main_menu_keyboard`.
- **Token Validation:** `CallbackTokenService`.

## CONVENTIONS
- **Manager-Only:** `/people` requires `is_manager=1` in the database.
- **DM-Only:** `/people` logic enforces `chat_id == actor_user_id`.
- **Tokenized Callbacks:** All buttons use `act:{token}` to prevent tampering.
- **Identity Safety:** `@username` is only an alias; `telegram_user_id` is the primary key.

## ANTI-PATTERNS
- **NO** manual formatting in adapter; use `presentation.py`.
- **NEVER** use `/meet` or `/schedule` (meetings are synced from Google Calendar).
- **DO NOT** use `@username` as a primary key.
