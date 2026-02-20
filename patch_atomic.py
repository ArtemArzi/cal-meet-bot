import re

with open("src/bot_vstrechi/db/repository.py", "r") as f:
    content = f.read()

old_atomic = """    @contextmanager
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
            self._in_transaction = False"""

new_atomic = """    @contextmanager
    def atomic(self) -> Iterator[None]:
        if getattr(self, "_in_transaction", False):
            yield
            return

        self._conn.execute("BEGIN IMMEDIATE")
        self._in_transaction = True
        try:
            yield
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        finally:
            self._in_transaction = False"""

content = content.replace(old_atomic, new_atomic)

old_begin = """        _ = self._conn.execute("BEGIN")
        try:"""

new_begin = """        self._begin_immediate()
        try:"""

content = content.replace(old_begin, new_begin)

with open("src/bot_vstrechi/db/repository.py", "w") as f:
    f.write(content)
