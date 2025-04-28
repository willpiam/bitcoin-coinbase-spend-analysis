#!/usr/bin/env python3
"""Plot frequency over time of coinbase outputs (minted in the first N years) being spent.

This script can be run *at any time* while the collector is running.
It uses only the rows currently present in `coinbase_spending.db`, so the
resulting plot will automatically extend as more data are collected.

Usage (from project root):
    python plot_coinbase_spending.py [--db PATH] [--first-years N] [--group {D,M,Y}] [--last-years Y] [--out FILE]

Options:
    --db PATH        Path to the SQLite database (default: coinbase_spending.db or $SQLITE_DB)
    --first-years N  Number of years from Bitcoin genesis to include as early period (default: 1)
    --group PERIOD   Resample frequency. One of:
                     D = daily (default), M = monthly, Y = yearly
    --last-years Y   Only include spends in the past Y years relative to latest data (optional)
    --out FILE       Output path for the saved plot (default: coinbase_spends_first_year.png)

The script always saves the output figure to the file specified by `--out`.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
from datetime import datetime
from typing import Literal

import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from rich import print
from rich.progress import track

# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------

load_dotenv()
DEFAULT_DB_PATH = os.getenv("SQLITE_DB", "coinbase_spending.db")
ResampleRule = Literal["D", "M", "Y"]


# ---------------------------------------------------------------------------
# Query / data wrangling
# ---------------------------------------------------------------------------

def load_dataframe(db_path: str, first_years: int) -> pd.DataFrame:
    """Load spent outputs from the SQLite DB in the first N years into a pandas DataFrame."""
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"SQLite database not found: {db_path}")

    # Compute cutoff date for the early period
    cutoff_date = datetime(2009 + first_years, 1, 4)
    print(f"[bold]Filtering creation_block_time < {cutoff_date.isoformat()}[/bold]")

    # Build and show the dynamic SQL
    sql_query = f"""
SELECT
    creation_block_time,
    spend_block_time
FROM coinbase_spends
WHERE creation_block_time IS NOT NULL
  AND spend_block_time IS NOT NULL
  AND creation_block_time < '{cutoff_date.isoformat()}'
"""
    print("[blue]SQL Query:[/]")
    print(sql_query)

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(sql_query, conn,
                               parse_dates=["creation_block_time", "spend_block_time"])
    print(f"Loaded {len(df):,} spent outputs (minted in first {first_years} years).")
    return df


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def make_plots(df: pd.DataFrame, rule: ResampleRule, out_path: str, last_years: int | None = None) -> None:
    """Generate and show the frequency + cumulative plots."""
    if df.empty:
        print("[yellow]No matching rows found. Has the collector processed any spends yet?[/yellow]")
        return

    df = df.copy()
    df["spend_date"] = df["spend_block_time"].dt.floor("D")  # normalize to midnight

    # Optionally filter to only the past N years
    if last_years is not None and last_years > 0:
        # compute cutoff relative to the latest spend_date
        last_date = df["spend_date"].max()
        cutoff = last_date - pd.DateOffset(years=last_years)
        print(f"[bold]Filtering to last {last_years} years: {cutoff.date()} – {last_date.date()}[/bold]")
        df = df[df["spend_date"] >= cutoff]

    counts = df.groupby("spend_date").size().rename("count")

    # Resample per user choice (rule) – keep smaller intervals if asked for daily
    if rule != "D":
        counts = counts.resample(rule).sum()

    cumulative = counts.cumsum()

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

    counts.plot(ax=ax1, lw=1.5)
    ax1.set_title("Frequency of spending coinbases minted in Bitcoin's first year")
    ax1.set_ylabel("Outputs spent")

    cumulative.plot(ax=ax2, lw=1.5, color="tab:orange")
    ax2.set_title("Cumulative count")
    ax2.set_ylabel("Cumulative outputs")

    ax2.set_xlabel("Spend date")
    plt.tight_layout()

    # Save to file
    plt.savefig(out_path, dpi=150)
    print(f"[green]Figure saved to {out_path}[/green]")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot coinbase spending frequency.")
    parser.add_argument("--db", default=DEFAULT_DB_PATH,
                        help="Path to the SQLite database (default from .env or coinbase_spending.db)")
    parser.add_argument("--first-years", "-f", type=int, default=1,
                        help="Number of years from Bitcoin genesis to include as early period (default: 1)")
    parser.add_argument("--group", choices=["D", "M", "Y"], default="D",
                        help="Time binning: D=daily, M=monthly, Y=yearly (default D)")
    parser.add_argument("--last-years", "-y", type=int, default=None,
                        help="Only include spends from the past N years relative to the latest data point (default: all)")
    parser.add_argument("--out", "-o", default="coinbase_spends_first_year.png",
                        help="File path to save the plot")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    # Load & filter for the first N years
    df = load_dataframe(args.db, args.first_years)
    # Plot and save
    make_plots(df, args.group, args.out, args.last_years)


if __name__ == "__main__":
    main() 