from __future__ import annotations

import json

import httpx

from bot_vstrechi.telegram.client import HttpxTelegramClient


def test_telegram_client_posts_send_message_payload() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content.decode("utf-8"))
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}})

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = HttpxTelegramClient(
        bot_token="123:abc",
        http_client=http_client,
        max_attempts=1,
    )

    client.send_message(telegram_user_id=200, text="hi")

    assert seen["method"] == "POST"
    assert seen["path"] == "/bot123:abc/sendMessage"
    assert seen["body"] == {"chat_id": 200, "text": "hi"}


def test_telegram_client_retries_on_429() -> None:
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"Retry-After": "0"})
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 2}})

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = HttpxTelegramClient(
        bot_token="123:abc",
        http_client=http_client,
        max_attempts=2,
        backoff_base_seconds=0.0,
    )

    client.send_message(telegram_user_id=200, text="retry")

    assert calls == 2


def test_telegram_client_idempotency_prevents_duplicate_send() -> None:
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 3}})

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = HttpxTelegramClient(
        bot_token="123:abc",
        http_client=http_client,
        max_attempts=1,
    )

    client.send_message(
        telegram_user_id=300,
        text="dedup",
        idempotency_key="k-1",
    )
    client.send_message(
        telegram_user_id=300,
        text="dedup",
        idempotency_key="k-1",
    )

    assert calls == 1


def test_telegram_client_supports_reply_keyboard() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content.decode("utf-8"))
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 4}})

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = HttpxTelegramClient(
        bot_token="123:abc",
        http_client=http_client,
        max_attempts=1,
    )

    client.send_message(
        telegram_user_id=400,
        text="menu",
        keyboard=[["Кнопка 1", "Кнопка 2"], ["Кнопка 3"]],
    )

    assert seen["path"] == "/bot123:abc/sendMessage"
    body = seen["body"]
    assert isinstance(body, dict)
    reply_markup = body.get("reply_markup")
    assert isinstance(reply_markup, dict)
    assert reply_markup["resize_keyboard"] is True
    assert reply_markup["is_persistent"] is True


def test_telegram_client_configures_bot_ui_only_once() -> None:
    requests: list[tuple[str, dict[str, object]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payload_obj = json.loads(request.content.decode("utf-8"))
        assert isinstance(payload_obj, dict)
        requests.append((request.url.path, payload_obj))
        return httpx.Response(200, json={"ok": True, "result": True})

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = HttpxTelegramClient(
        bot_token="123:abc",
        http_client=http_client,
        max_attempts=1,
    )

    client.configure_bot_ui()
    client.configure_bot_ui()

    paths = [path for path, _ in requests]
    assert paths.count("/bot123:abc/setMyCommands") == 3
    assert paths.count("/bot123:abc/setChatMenuButton") == 1


def test_telegram_client_supports_multicolumn_inline_keyboard() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = json.loads(request.content.decode("utf-8"))
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 5}})

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = HttpxTelegramClient(
        bot_token="123:abc",
        http_client=http_client,
        max_attempts=1,
    )

    client.send_message(
        telegram_user_id=401,
        text="grid",
        buttons=[
            [
                {"text": "09:00 ✅", "callback_data": "x1"},
                {"text": "09:30 ✅", "callback_data": "x2"},
            ],
            [{"text": "10:00 ⛔", "callback_data": "x3"}],
        ],
    )

    body = seen["body"]
    assert isinstance(body, dict)
    reply_markup = body.get("reply_markup")
    assert isinstance(reply_markup, dict)
    keyboard = reply_markup.get("inline_keyboard")
    assert isinstance(keyboard, list)
    assert len(keyboard) == 2


def test_telegram_client_edits_message_and_clears_inline_keyboard() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content.decode("utf-8"))
        return httpx.Response(200, json={"ok": True, "result": True})

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = HttpxTelegramClient(
        bot_token="123:abc",
        http_client=http_client,
        max_attempts=1,
    )

    client.edit_message(
        telegram_user_id=401,
        message_id=77,
        text="updated",
        buttons=[],
    )

    assert seen["path"] == "/bot123:abc/editMessageText"
    body = seen["body"]
    assert isinstance(body, dict)
    assert body["chat_id"] == 401
    assert body["message_id"] == 77
    reply_markup = body.get("reply_markup")
    assert isinstance(reply_markup, dict)
    assert reply_markup.get("inline_keyboard") == []


def test_telegram_client_answers_callback_query() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content.decode("utf-8"))
        return httpx.Response(200, json={"ok": True, "result": True})

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = HttpxTelegramClient(
        bot_token="123:abc",
        http_client=http_client,
        max_attempts=1,
    )

    client.answer_callback_query(callback_query_id="cb-1", text="ok")

    assert seen["path"] == "/bot123:abc/answerCallbackQuery"
    body = seen["body"]
    assert isinstance(body, dict)
    assert body["callback_query_id"] == "cb-1"
    assert body["text"] == "ok"
