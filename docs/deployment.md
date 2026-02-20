# Production deployment (Docker Compose + HTTPS)

## 1) Server prerequisites

- Linux server with public IPv4.
- DNS `A` record for your domain to server IP.
- Open ports: `80/tcp`, `443/tcp`.
- Installed Docker Engine and Docker Compose plugin.

## 2) Prepare environment

```bash
cp .env.production.example .env.production
```

Required fields in `.env.production`:

- `DOMAIN`
- `CERTBOT_EMAIL`
- `TELEGRAM_BOT_TOKEN`

Recommended for split-process production mode:

- `BOT_VSTRECHI_RUN_BACKGROUND_WORKERS=false`

If Google Calendar integration is enabled, also set:

- `BOT_VSTRECHI_CALENDAR_ENABLED=true`
- `GOOGLE_SA_CLIENT_EMAIL`
- `GOOGLE_SA_PRIVATE_KEY`
- `GOOGLE_IMPERSONATION_SUBJECT`

## 3) Request the first TLS certificate

```bash
chmod +x scripts/init-letsencrypt.sh
./scripts/init-letsencrypt.sh
```

This runs the `certbot-init` profile and creates the first Let's Encrypt certificate.

## 4) Start application stack

```bash
docker compose up -d --build
```

Services:

- `app`: FastAPI webhook API.
- `worker`: background processing loop (scheduler/outbox/calendar sync).
- `nginx`: reverse proxy and TLS termination.
- `certbot`: periodic certificate renewal.

## 5) Configure Telegram webhook

```bash
curl -sS "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/setWebhook" \
  -d "url=https://${DOMAIN}/telegram/webhook" \
  -d "secret_token=${TELEGRAM_SECRET_TOKEN}"
```

Verify:

```bash
curl -sS "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getWebhookInfo"
curl -fsS "https://${DOMAIN}/health"
curl -fsS "https://${DOMAIN}/readiness"
```

## 6) Operations

```bash
docker compose logs -f app
docker compose logs -f nginx
docker compose logs -f certbot
```

Restart stack:

```bash
docker compose restart
```

Stop stack:

```bash
docker compose down
```
