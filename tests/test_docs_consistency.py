from __future__ import annotations

from pathlib import Path
import re


def _read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def test_no_legacy_command_references_in_canonical_docs() -> None:
    docs = (
        _read("docs/backend-detailed-design.md"),
        _read("docs/bot-interaction-flow.md"),
        _read("docs/manual-testing-readiness.md"),
    )
    joined = "\n".join(docs)

    legacy_command_patterns = (
        r"/meet(?![a-z_])",
        r"/reschedule(?![a-z_])",
        r"/cancel_meet(?![a-z_])",
        r"/schedule(?![a-z_])",
        r"/free(?![a-z_])",
    )
    for pattern in legacy_command_patterns:
        assert re.search(pattern, joined) is None
