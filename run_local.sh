#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env.local}"

if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

VENV_DIR="${VENV_DIR:-$ROOT_DIR/.venv}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"
RELOAD="${RELOAD:-1}"

if [ ! -d "$VENV_DIR" ]; then
  "$PYTHON_BIN" -m venv "$VENV_DIR"
  "$VENV_DIR/bin/python" -m pip install --upgrade pip
  "$VENV_DIR/bin/python" -m pip install -r "$ROOT_DIR/requirements.txt"
fi

if [ "${1:-}" = "--install" ]; then
  "$VENV_DIR/bin/python" -m pip install --upgrade pip
  "$VENV_DIR/bin/python" -m pip install -r "$ROOT_DIR/requirements.txt"
  shift
fi

if [ "${1:-}" = "--setup-only" ]; then
  printf "Setup complete. venv=%s\n" "$VENV_DIR"
  exit 0
fi

mkdir -p "$ROOT_DIR/var"

export BOT_VSTRECHI_DB_PATH="${BOT_VSTRECHI_DB_PATH:-$ROOT_DIR/var/local.db}"
export TELEGRAM_SECRET_TOKEN="${TELEGRAM_SECRET_TOKEN-}"
export LOG_LEVEL="${LOG_LEVEL:-INFO}"
export LOG_FORMAT="${LOG_FORMAT:-pretty}"

cleanup() {
  if [ -n "${NGROK_PID:-}" ]; then
    kill "$NGROK_PID" 2>/dev/null || true
  fi
  if [ -n "${UVICORN_PID:-}" ]; then
    kill "$UVICORN_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

"$VENV_DIR/bin/uvicorn" bot_vstrechi.asgi:app --host "$HOST" --port "$PORT" &
UVICORN_PID=$!
sleep 2

ngrok http "$PORT" --log=stdout > /dev/null &
NGROK_PID=$!
sleep 3

NGROK_URL=""
for i in 1 2 3 4 5; do
  NGROK_URL=$(curl -s http://127.0.0.1:4040/api/tunnels 2>/dev/null \
    | "$VENV_DIR/bin/python" -c "import sys,json; tunnels=json.load(sys.stdin).get('tunnels',[]); print(next((t['public_url'] for t in tunnels if t['public_url'].startswith('https')), ''))" 2>/dev/null || true)
  if [ -n "$NGROK_URL" ]; then
    break
  fi
  sleep 2
done

if [ -z "$NGROK_URL" ]; then
  printf "\n*** ngrok не запустился. Проверь: ngrok http %s ***\n" "$PORT"
  wait "$UVICORN_PID"
  exit 1
fi

printf "\n===================================\n"
printf "ngrok URL: %s\n" "$NGROK_URL"
printf "===================================\n\n"

WEBHOOK_RESULT=$(curl -sS "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/setWebhook" \
  -d "url=${NGROK_URL}/telegram/webhook" \
  -d "secret_token=${TELEGRAM_SECRET_TOKEN}" 2>&1)

printf "Webhook set: %s\n\n" "$WEBHOOK_RESULT"

WEBHOOK_INFO=$(curl -sS "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getWebhookInfo" 2>&1)
printf "Webhook info: %s\n" "$WEBHOOK_INFO"

printf "\n===================================\n"
printf "Бот запущен. Открой Telegram и отправь /start\n"
printf "Ctrl+C для остановки\n"
printf "===================================\n\n"

wait "$UVICORN_PID"
