from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from bot_vstrechi.db.repository import SQLiteRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Add or update Telegram to Google user mapping",
    )
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to SQLite database (example: ./var/local.db)",
    )
    parser.add_argument("--telegram-user-id", type=int, required=True)
    parser.add_argument("--google-email", required=True)
    parser.add_argument("--telegram-username", default=None)
    parser.add_argument("--timezone", default="Asia/Yekaterinburg")
    parser.add_argument(
        "--inactive",
        action="store_true",
        help="Mark user mapping as inactive",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    repository = SQLiteRepository(args.db_path)
    repository.initialize_schema()
    try:
        repository.upsert_user_mapping(
            telegram_user_id=args.telegram_user_id,
            telegram_username=args.telegram_username,
            google_email=args.google_email,
            timezone=args.timezone,
            is_active=not args.inactive,
            now=datetime.now(tz=timezone.utc),
        )
    finally:
        repository.close()

    print(
        "user_mapping upserted:",
        f"telegram_user_id={args.telegram_user_id}",
        f"google_email={args.google_email}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
