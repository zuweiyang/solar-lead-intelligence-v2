"""
Workflow 0 — Lead Database Layer: Database Initialisation Script

Creates data/solar_leads.db and all tables, then optionally syncs
any existing CSV files into the database.

Usage:
    py scripts/init_database.py           # init only
    py scripts/init_database.py --sync    # init + import existing CSVs
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.db_connection import get_db_connection
from src.database.db_schema import create_all_tables
from src.database.csv_sync import sync_all
from config.settings import DATABASE_FILE


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Initialise the solar_leads SQLite database."
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        help="After creating tables, import existing CSV files into the database.",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Workflow 0 — Lead Database Layer")
    print("=" * 60)
    print(f"Database : {DATABASE_FILE}")

    conn = get_db_connection()
    create_all_tables(conn)
    print(f"[init_database] Database ready at: {DATABASE_FILE}")

    if args.sync:
        print()
        results = sync_all(conn)
        print()
        print("Sync summary:")
        for table, n in results.items():
            print(f"  {table:<35} {n:>6} rows")

    conn.close()
    print()
    print("Done.")


if __name__ == "__main__":
    main()
