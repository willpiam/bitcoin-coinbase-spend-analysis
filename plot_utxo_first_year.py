#!/usr/bin/env python3
"""Plot and report UTXO set size for coinbases minted in Bitcoin's first year.

This script can be run at any time while the collector is running. It reads the
local SQLite database, computes the remaining unspent outputs over time, and
saves a time series plot.

Usage:
    python plot_utxo_first_year.py [--db PATH] [--group {D,M,Y}] [--out FILE]

Options:
    --db PATH    Path to the SQLite DB (default: coinbase_spending.db or $SQLITE_DB)
    --group freq Resample frequency: D=daily (default), M=monthly, Y=yearly
    --out FILE   Output path for the plot (default: utxo_first_year.png)
"""

from __future__ import annotations
import os
import sqlite3
import argparse
from datetime import datetime

import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from rich import print

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
load_dotenv()
DEFAULT_DB_PATH = os.getenv("SQLITE_DB", "coinbase_spending.db")
FIRST_YEAR_END = datetime(2010, 1, 4)

# ---------------------------------------------------------------------------
# Data loading and processing
# ---------------------------------------------------------------------------
SQL = f"""
SELECT
    creation_block_time,
    spend_block_time
FROM coinbase_spends
WHERE creation_block_time IS NOT NULL
  AND creation_block_time < '{FIRST_YEAR_END.isoformat()}'
"""

def load_data(db_path: str) -> pd.DataFrame:
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"DB not found: {db_path}")
    print(f"[bold]Loading data from[/bold] {db_path}")
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(SQL, conn,
                               parse_dates=["creation_block_time", "spend_block_time"])
    print(f"Total minted in first year: {len(df):,}")
    unspent_now = df[ df['spend_block_time'].isnull() ].shape[0]
    print(f"Currently unspent: {unspent_now:,} ({unspent_now/len(df):.2%})")
    return df

# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------
def plot_utxo(df: pd.DataFrame, freq: str, out_path: str) -> None:
    df = df.copy()
    # floor spend times to date
    df['spend_date'] = df['spend_block_time'].dt.floor('D')

    # count spends per day
    spends = df.dropna(subset=['spend_date'])
    ts = spends.groupby('spend_date').size().rename('daily_spent')

    # resample and fill gaps
    if freq != 'D':
        ts = ts.resample(freq).sum()
    ts = ts.asfreq(freq, fill_value=0)

    cum_spent = ts.cumsum()
    total = len(df)
    utxo = total - cum_spent

    # Plot
    fig, ax = plt.subplots(figsize=(12, 6))
    utxo.plot(ax=ax, lw=1.5, label='UTXO size')
    ax.set_title("Unspent first-year coinbase outputs")
    ax.set_ylabel("Number of outputs")
    ax.set_xlabel("Date")
    ax.legend()
    plt.tight_layout()

    plt.savefig(out_path, dpi=150)
    print(f"[green]UTXO-plot saved to {out_path}[/green]")

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Plot UTXO set for first-year coinbases.")
    p.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite DB path")
    p.add_argument("--group", choices=["D","M","Y"], default="D",
                   help="Resample frequency: D=daily, M=monthly, Y=yearly")
    p.add_argument("--out", default="utxo_first_year.png",
                   help="Output graphic file path")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    df = load_data(args.db)
    plot_utxo(df, args.group, args.out)


if __name__ == "__main__":
    main() 