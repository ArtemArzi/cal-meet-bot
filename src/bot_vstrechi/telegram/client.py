from __future__ import annotations

import time
from typing import cast

import httpx

from bot_vstrechi.telegram.presentation import telegram_commands_payload


TELEGRAM_API_BASE_URL = "https://api.telegram.org"


class HttpxTelegramClient:
    def __init__(
        self,
        *,
        bot_token: str,
        timeout_seconds: float = 10.0,
        max_attempts: int = 3,
        backoff_base_seconds: float = 0.3,
        http_client: httpx.Client | None = None,
    ) -> None:
        if not bot_token.strip():
            raise ValueError("Telegram bot token must be non-empty")

        self._bot_token: str = bot_token.strip()
        self._timeout_seconds: float = timeout_seconds
        self._max_attempts: int = max_attempts
        self._backoff_base_seconds: float = backoff_base_seconds
        self._http_client: httpx.Client = http_client or httpx.Client()
        self._sent_message_keys: set[str] = set()
        self._ui_configured: bool = False

    def configure_bot_ui(self) -> None:
        if self._ui_configured:
            return

        commands = telegram_commands_payload()
        scopes = (
            {"type": "default"},
            {"type": "all_private_chats"},
            {"type": "all_group_chats"},
        )
        for scope in scopes:
            _ = self._post_api(
                method="setMyCommands",
                payload={"commands": commands, "scope": scope},
            )

        _ = self._post_api(
            method="setChatMenuButton",
            payload={"menu_button": {"type": "commands"}},
        )
        self._ui_configured = True

    def send_message(
        self,
        *,
        telegram_user_id: int,
        text: str,
        buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
        keyboard: list[list[str]] | None = None,
        idempotency_key: str | None = None,
    ) -> int | None:
        if idempotency_key is not None and idempotency_key in self._sent_message_keys:
            return None

        payload: dict[str, object] = {"chat_id": telegram_user_id, "text": text}
        if buttons:
            payload["reply_markup"] = self._inline_reply_markup(buttons)
        elif keyboard:
            keyboard_rows: list[list[dict[str, str]]] = []
            for row in keyboard:
                cells = [{"text": label} for label in row if label.strip()]
                if cells:
                    keyboard_rows.append(cells)
            if keyboard_rows:
                payload["reply_markup"] = {
                    "keyboard": keyboard_rows,
                    "resize_keyboard": True,
                    "is_persistent": True,
                    "input_field_placeholder": "Выберите действие",
                }

        headers: dict[str, str] = {}
        if idempotency_key is not None:
            headers["X-Idempotency-Key"] = idempotency_key

        response = self._post_api(
            method="sendMessage", payload=payload, headers=headers
        )
        if idempotency_key is not None:
            self._sent_message_keys.add(idempotency_key)
        result_obj = response.get("result")
        if not isinstance(result_obj, dict):
            return None
        message_id_obj = result_obj.get("message_id")
        return message_id_obj if isinstance(message_id_obj, int) else None

    def edit_message(
        self,
        *,
        telegram_user_id: int,
        message_id: int,
        text: str,
        buttons: list[dict[str, str] | list[dict[str, str]]] | None = None,
        idempotency_key: str | None = None,
    ) -> None:
        if idempotency_key is not None and idempotency_key in self._sent_message_keys:
            return

        payload: dict[str, object] = {
            "chat_id": telegram_user_id,
            "message_id": message_id,
            "text": text,
            "reply_markup": self._inline_reply_markup(buttons),
        }
        headers: dict[str, str] = {}
        if idempotency_key is not None:
            headers["X-Idempotency-Key"] = idempotency_key

        _ = self._post_api(method="editMessageText", payload=payload, headers=headers)
        if idempotency_key is not None:
            self._sent_message_keys.add(idempotency_key)

    def answer_callback_query(
        self,
        *,
        callback_query_id: str,
        text: str | None = None,
        idempotency_key: str | None = None,
    ) -> None:
        if idempotency_key is not None and idempotency_key in self._sent_message_keys:
            return

        payload: dict[str, object] = {"callback_query_id": callback_query_id}
        if isinstance(text, str) and text.strip():
            payload["text"] = text.strip()

        headers: dict[str, str] = {}
        if idempotency_key is not None:
            headers["X-Idempotency-Key"] = idempotency_key

        _ = self._post_api(
            method="answerCallbackQuery",
            payload=payload,
            headers=headers,
        )
        if idempotency_key is not None:
            self._sent_message_keys.add(idempotency_key)

    def _inline_reply_markup(
        self,
        buttons: list[dict[str, str] | list[dict[str, str]]] | None,
    ) -> dict[str, object]:
        inline_keyboard: list[list[dict[str, str]]] = []
        if buttons:
            for item in buttons:
                if isinstance(item, list):
                    row = [
                        {
                            "text": button["text"],
                            "callback_data": button["callback_data"],
                        }
                        for button in item
                        if isinstance(button, dict)
                        and isinstance(button.get("text"), str)
                        and isinstance(button.get("callback_data"), str)
                    ]
                    if row:
                        inline_keyboard.append(row)
                    continue

                if (
                    isinstance(item, dict)
                    and isinstance(item.get("text"), str)
                    and isinstance(item.get("callback_data"), str)
                ):
                    inline_keyboard.append(
                        [
                            {
                                "text": item["text"],
                                "callback_data": item["callback_data"],
                            }
                        ]
                    )

        return {"inline_keyboard": inline_keyboard}

    def _post_api(
        self,
        *,
        method: str,
        payload: dict[str, object],
        headers: dict[str, str] | None = None,
    ) -> dict[str, object]:
        url = f"{TELEGRAM_API_BASE_URL}/bot{self._bot_token}/{method}"
        merged_headers: dict[str, str] = headers or {}

        for attempt in range(1, self._max_attempts + 1):
            try:
                response = self._http_client.post(
                    url,
                    json=payload,
                    headers=merged_headers,
                    timeout=self._timeout_seconds,
                )
            except httpx.HTTPError as error:
                if attempt == self._max_attempts:
                    raise RuntimeError(f"Telegram {method} request failed") from error
                time.sleep(self._backoff_seconds(attempt=attempt))
                continue

            if response.status_code in {429, 500, 502, 503, 504}:
                if attempt == self._max_attempts:
                    raise RuntimeError(
                        f"Telegram {method} failed with status {response.status_code}"
                    )
                time.sleep(
                    self._retry_after_seconds(response=response, attempt=attempt)
                )
                continue

            if response.is_error:
                raise RuntimeError(
                    f"Telegram {method} non-retryable status {response.status_code}: {response.text}"
                )

            body_obj = cast(object, response.json())
            if not isinstance(body_obj, dict):
                raise RuntimeError(f"Telegram {method} response payload invalid")
            body = cast(dict[str, object], body_obj)
            ok_obj = body.get("ok")
            if ok_obj is not True:
                raise RuntimeError(f"Telegram {method} response payload invalid")
            return body

        raise RuntimeError(f"Telegram {method} request exhausted attempts")

    def _backoff_seconds(self, *, attempt: int) -> float:
        return float(self._backoff_base_seconds * (2 ** (attempt - 1)))

    def _retry_after_seconds(self, *, response: httpx.Response, attempt: int) -> float:
        if "Retry-After" not in response.headers:
            return self._backoff_seconds(attempt=attempt)

        retry_after_raw = response.headers["Retry-After"]

        try:
            retry_after = float(retry_after_raw)
        except ValueError:
            return self._backoff_seconds(attempt=attempt)

        if retry_after < 0:
            return self._backoff_seconds(attempt=attempt)
        return retry_after
