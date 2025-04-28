#!/usr/bin/env python3
"""Count how many coinbases minted in Bitcoin's first year remain unspent."""

import os
import sqlite3
import argparse
from datetime import datetime
from dotenv import load_dotenv
from rich import print

# Load .env and defaults
load_dotenv()
DEFAULT_DB = os.getenv("SQLITE_DB", "coinbase_spending.db")
FIRST_YEAR_CUTOFF = datetime(2010, 1, 4).isoformat()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Count unspent first-year coinbase outputs"
    )
    p.add_argument(
        "--db", default=DEFAULT_DB,
        help="Path to the SQLite database (default from .env or coinbase_spending.db)"
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    db_path = args.db

    if not os.path.exists(db_path):
        print(f"[red]Error:[/] database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Total minted in first year
    cur.execute(
        "SELECT COUNT(*) FROM coinbase_spends WHERE creation_block_time < ?;",
        (FIRST_YEAR_CUTOFF,)
    )
    total = cur.fetchone()[0]

    # Unspent outputs
    cur.execute(
        "SELECT COUNT(*) FROM coinbase_spends WHERE creation_block_time < ? AND spend_txid IS NULL;",
        (FIRST_YEAR_CUTOFF,)
    )
    unspent = cur.fetchone()[0]

    conn.close()

    pct = (unspent / total * 100) if total else 0
    print(f"[bold]First-year coinbase outputs minted before {FIRST_YEAR_CUTOFF}:[/] {total}")
    print(f"[bold]Currently unspent outputs:[/] {unspent} ({pct:.2f}%)")


if __name__ == "__main__":
    main() 