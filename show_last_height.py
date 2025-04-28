#!/usr/bin/env python3
"""Show the highest processed block height and its corresponding date."""

import os
import sqlite3
import argparse
from datetime import datetime
from dotenv import load_dotenv
from rich import print

# Load .env for default DB path
load_dotenv()
DEFAULT_DB = os.getenv("SQLITE_DB", "coinbase_spending.db")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show the highest processed block height and its timestamp"
    )
    parser.add_argument(
        "--db", default=DEFAULT_DB,
        help="Path to the SQLite database (default: from .env or coinbase_spending.db)"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = args.db

    if not os.path.exists(db_path):
        print(f"[red]Error:[/] database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)

    # Get last processed height
    cur = conn.execute(
        "SELECT value FROM metadata WHERE key = 'last_processed_height';"
    )
    row = cur.fetchone()
    if not row:
        print("[yellow]No 'last_processed_height' key found in metadata.[/yellow]")
        conn.close()
        return

    height = int(row[0])
    print(f"[bold]Highest processed block height:[/] {height}")

    # Lookup block timestamp from coinbase_spends
    cur2 = conn.execute(
        "SELECT creation_block_time FROM coinbase_spends WHERE creation_block_height = ? LIMIT 1;",
        (height,)
    )
    row2 = cur2.fetchone()
    conn.close()

    if not row2 or not row2[0]:
        print(f"[yellow]No coinbase entry found for height {height}. Maybe no coinbase outputs yet?[/yellow]")
        return

    time_str = row2[0]
    try:
        dt = datetime.fromisoformat(time_str)
        print(f"[bold]Block creation time:[/] {dt.isoformat()}")
    except ValueError:
        print(f"[bold]Block creation time:[/] {time_str} (raw)")


if __name__ == "__main__":
    main() 