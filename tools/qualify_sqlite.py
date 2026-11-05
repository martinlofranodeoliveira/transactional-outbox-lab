"""Qualify the actual linked SQLite, using only a disposable in-memory database."""
import json
import sqlite3


def main():
    with sqlite3.connect(":memory:") as connection:
        version, source_id = connection.execute("SELECT sqlite_version(), sqlite_source_id()").fetchone()
        if version != "3.54.1" or source_id != "2026-10-16 14:45:20 555e07ef33f69a0957d24d8e555b702464a131d6f1607f4c3c1e4551e8e6ca76":
            raise RuntimeError("Unexpected linked SQLite identity")
        connection.execute("CREATE VIRTUAL TABLE smoke USING fts5(text, tokenize='unicode61 remove_diacritics 2')")
        connection.execute("INSERT INTO smoke VALUES (?)", ("Café",))
        if connection.execute("SELECT count(*) FROM smoke WHERE smoke MATCH ?", ("cafe",)).fetchone() != (1,):
            raise RuntimeError("Actual FTS5 smoke failed")
        print(json.dumps({"sqlite_version": version, "sqlite_source_id": source_id, "fts5_smoke": True}))


if __name__ == "__main__":
    main()
