import re

with open("src/bot_vstrechi/db/repository.py", "r") as f:
    content = f.read()

content = content.replace("self._conn.commit()", "self._commit()")
content = content.replace("self._conn.rollback()", "self._rollback()")
content = content.replace('_ = self._conn.execute("BEGIN IMMEDIATE")', "self._begin_immediate()")

atomic_methods = """
    def _begin_immediate(self) -> None:
        if not getattr(self, "_in_transaction", False):
            self._conn.execute("BEGIN IMMEDIATE")

    def _commit(self) -> None:
        if not getattr(self, "_in_transaction", False):
            self._conn.commit()

    def _rollback(self) -> None:
        if not getattr(self, "_in_transaction", False):
            self._conn.rollback()

    from contextlib import contextmanager
    from collections.abc import Iterator

    @contextmanager
    def atomic(self) -> Iterator[None]:
        if getattr(self, "_in_transaction", False):
            yield
            return

        self._in_transaction = True
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        finally:
            self._in_transaction = False
"""

# Insert these methods right after def _configure_connection(self):
# ...
content = re.sub(
    r"(def _configure_connection\(self\) -> None:.*?)(?=\n    def )",
    r"\1" + atomic_methods,
    content,
    flags=re.DOTALL
)

with open("src/bot_vstrechi/db/repository.py", "w") as f:
    f.write(content)
