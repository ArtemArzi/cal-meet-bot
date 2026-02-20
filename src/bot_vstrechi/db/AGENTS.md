# AGENTS - Persistence & Outbox (src/bot_vstrechi/db)

**OVERVIEW:** SQLiteRepository manages all persistent state and enqueues events via the Outbox pattern.

## STRUCTURE
- `repository.py`: Primary `SQLiteRepository` class with raw SQL methods.
- `__init__.py`: Package exports for repository and claim models.

## WHERE TO LOOK
- **Schema:** Defined inside `initialize_schema()` in `repository.py`.
- **Outbox:** Logic for `enqueue_outbox` and `claim_due_outbox` handles async task dispatch.
- **Jobs:** Scheduled jobs (reminders, deadlines) are stored in the `job` table.
- **Atomic Operations:** The `@atomic` context manager ensures transaction integrity.

## CONVENTIONS
- **Raw SQL Only:** Use standard SQL for all queries. No ORMs allowed.
- **Atomic Context:** Wrap multiple related writes in `with self.atomic():`.
- **Identity Safety:** Store `telegram_user_id` as an integer. Never use usernames for lookups or keys.
- **UTC Everywhere:** All timestamps must be ISO-8601 strings in UTC.
- **WAL Mode:** Database always operates in Write-Ahead Logging mode for concurrency.

## ANTI-PATTERNS
- **Non-Atomic Writes:** Don't perform multiple inserts or updates outside of an `atomic` block.
- **ORM usage:** Don't introduce SQLAlchemy, Peewee, or other ORMs to this layer.
- **Business Logic in SQL:** Keep complex domain logic in `src/bot_vstrechi/domain`.
- **Blocking Transactions:** Keep transaction blocks short to prevent `SQLITE_BUSY` errors.
