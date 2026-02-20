#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env.production}"

if [ ! -f "$ENV_FILE" ]; then
  printf "Missing env file: %s\n" "$ENV_FILE" >&2
  exit 1
fi

set -a
. "$ENV_FILE"
set +a

if [ -z "${DOMAIN:-}" ]; then
  printf "DOMAIN is required in %s\n" "$ENV_FILE" >&2
  exit 1
fi

if [ -z "${CERTBOT_EMAIL:-}" ]; then
  printf "CERTBOT_EMAIL is required in %s\n" "$ENV_FILE" >&2
  exit 1
fi

printf "Requesting certificate for %s...\n" "$DOMAIN"
docker compose --profile init run --rm certbot-init
printf "Certificate setup completed.\n"
