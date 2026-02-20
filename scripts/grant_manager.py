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
        description="Grant or revoke manager (admin) role",
    )
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to SQLite database (example: ./var/local.db)",
    )
    parser.add_argument(
        "--telegram-user-id", type=int, required=True, help="Telegram ID of the user"
    )
    parser.add_argument(
        "--granted-by",
        type=int,
        default=None,
        help="Telegram ID of the admin who grants this (optional)",
    )
    parser.add_argument(
        "--revoke",
        action="store_true",
        help="Revoke manager role instead of granting",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    repository = SQLiteRepository(args.db_path)
    repository.initialize_schema()
    now = datetime.now(tz=timezone.utc)

    try:
        if args.revoke:
            repository.revoke_manager_role(
                telegram_user_id=args.telegram_user_id,
                revoked_by=args.granted_by,
                now=now,
            )
            print(f"Manager role REVOKED for telegram_user_id={args.telegram_user_id}")
        else:
            repository.grant_manager_role(
                telegram_user_id=args.telegram_user_id,
                granted_by=args.granted_by,
                now=now,
            )
            print(f"Manager role GRANTED for telegram_user_id={args.telegram_user_id}")
    finally:
        repository.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
