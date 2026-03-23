"""
Database connection helper for solar_leads.db.
Returns a sqlite3 connection with row_factory set to Row for dict-like access.
"""
import sqlite3
from pathlib import Path

from config.settings import DATABASE_FILE


def get_db_connection() -> sqlite3.Connection:
    """
    Return an open sqlite3.Connection to data/solar_leads.db.
    Rows are accessible as both index and column name (sqlite3.Row).
    Foreign key enforcement is enabled per connection.

    Schema initialization (create_all_tables) and incremental migrations are
    applied automatically on each connection so new tables and columns are
    always present regardless of when the DB was first created.
    """
    DATABASE_FILE.parent.mkdir(parents=True, exist_ok=True)
    print(f"[db_connection] Opening {DATABASE_FILE.name} — schema init starting")
    conn = sqlite3.connect(str(DATABASE_FILE))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")

    from src.database.db_schema import create_all_tables, migrate_schema
    try:
        create_all_tables(conn)
        migrate_schema(conn)
    except Exception as exc:
        # Schema init failed — this is a hard infrastructure failure.
        # Close the connection and raise so callers don't silently write to a
        # partially initialised database.
        conn.close()
        raise RuntimeError(
            f"[db_connection] FATAL: schema init failed — {exc}\n"
            "Fix the schema DDL before running any pipeline step that uses the DB."
        ) from exc

    # Verify critical tables are present
    tables = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    required = {"email_verification", "companies", "contacts", "email_sends", "reply_events"}
    missing  = required - tables
    if missing:
        conn.close()
        raise RuntimeError(
            f"[db_connection] FATAL: required tables missing after schema init: {missing}\n"
            "Check create_all_tables() in db_schema.py."
        )

    print(
        f"[db_connection] Schema init OK — "
        f"{len(tables)} tables verified (email_verification ✓)"
    )
    return conn
