#!/usr/bin/env python3
"""Plot bar-chart view of coinbase‐spending frequency.

This script provides an alternative visualisation to
`plot_coinbase_spending.py`.  Instead of a line graph, the frequency
subplot is rendered as a bar chart with sensible bar widths on a date
axis.

The database query and filtering logic are reused from
`plot_coinbase_spending.load_dataframe`, so you can run this at any
point during data collection.

Usage (from project root):
    python plot_coinbase_spending_bar.py [--db PATH] [--first-years N]
                                         [--group {D,M,Y}] [--last-years Y]
                                         [--out FILE]

Options:
    --db PATH        SQLite DB path (default: coinbase_spending.db or $SQLITE_DB)
    --first-years N  Number of years after Bitcoin genesis to include (default: 1)
    --group PERIOD   Resample frequency: D=daily (default), M=monthly, Y=yearly
    --last-years Y   Only include spends from the past Y years (optional)
    --out FILE       Output image path. If omitted, uses naming convention:
                     `bar_m<first-years>l<last-years>_<group>.png` (e.g. bar_m2l10_m.png)

The figure contains a single bar chart of spend frequency for the selected period.
"""

from __future__ import annotations

import argparse
from typing import Optional

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from rich import print

# Re-use helper from the line-plot script to avoid duplicating SQL logic
from plot_coinbase_spending import (
    DEFAULT_DB_PATH,
    ResampleRule,
    load_dataframe,
)

# ---------------------------------------------------------------------------
# Plotting helpers
# ---------------------------------------------------------------------------

def make_bar_plots(
    df: pd.DataFrame,
    rule: ResampleRule,
    out_path: str,
    last_years: Optional[int] = None,
    first_years: int = 1,
) -> None:
    """Generate and save bar + cumulative plots.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing creation and spend timestamps.
    rule : ResampleRule
        Resampling rule for frequency aggregation.
    out_path : str
        Destination path for the saved figure.
    last_years : int | None
        Restrict to spends from the last *N* years (relative to most recent
        spend) if provided.
    first_years : int
        Number of early years used when filtering coinbase creation dates –
        only needed for title text.
    """

    if df.empty:
        print(
            "[yellow]No matching rows found. Has the collector processed any spends yet?[/yellow]"
        )
        return

    # ------------------------------------------------------------------
    # Prepare time-series
    # ------------------------------------------------------------------
    df = df.copy()
    df["spend_date"] = df["spend_block_time"].dt.floor("D")

    if last_years is not None and last_years > 0:
        last_date = df["spend_date"].max()
        cutoff = last_date - pd.DateOffset(years=last_years)
        print(
            f"[bold]Filtering to last {last_years} years: {cutoff.date()} – {last_date.date()}[/bold]"
        )
        df = df[df["spend_date"] >= cutoff]

    # Daily counts then optional resample
    counts = df.groupby("spend_date").size().rename("count")
    if rule != "D":
        counts = counts.resample(rule).sum()

    # ------------------------------------------------------------------
    # Plotting
    # ------------------------------------------------------------------
    fig, ax1 = plt.subplots(figsize=(12, 8))

    period_map = {"D": "daily", "M": "monthly", "Y": "yearly"}
    period = period_map[rule]
    title = (
        f"Frequency of spending coinbases minted in Bitcoin's first {first_years} year"
        f"{'s' if first_years > 1 else ''} ({period} bar view)"
    )
    if last_years is not None:
        title += f" – Last {last_years} year{'s' if last_years > 1 else ''} only"

    # Convert datetime index to Matplotlib date numbers for proper bar widths
    dates_num = mdates.date2num(counts.index.to_pydatetime())
    if len(dates_num) > 1:
        median_delta = np.median(np.diff(np.sort(dates_num)))
        bar_width = median_delta * 0.9  # 90 % of median gap
    else:
        bar_width = 0.8  # arbitrary default when only one bar

    ax1.bar(dates_num, counts.values, width=bar_width, color="tab:blue", align="center")
    ax1.set_title(title)
    ax1.set_ylabel("Outputs spent")
    ax1.set_xlabel("Spend date")
    # Draw horizontal gridlines behind bars
    ax1.set_axisbelow(True)
    ax1.grid(True, axis='y', linestyle='--', alpha=0.6)

    # Format date ticks so that every year is shown
    ax1.xaxis_date()
    ax1.xaxis.set_major_locator(mdates.YearLocator())
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    fig.autofmt_xdate()

    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    print(f"[green]Figure saved to {out_path}[/green]")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot bar-chart view of coinbase spending frequency.")
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        help="Path to the SQLite database (default from .env or coinbase_spending.db)",
    )
    parser.add_argument(
        "--first-years",
        "-f",
        type=int,
        default=1,
        help="Number of years from Bitcoin genesis to include as early period (default: 1)",
    )
    parser.add_argument(
        "--group",
        choices=["D", "M", "Y"],
        default="D",
        help="Time binning: D=daily, M=monthly, Y=yearly (default D)",
    )
    parser.add_argument(
        "--last-years",
        "-y",
        type=int,
        default=None,
        help="Only include spends from the past N years relative to the latest data point (default: all)",
    )
    parser.add_argument(
        "--out",
        "-o",
        default=None,
        help="File path to save the plot; if omitted, uses bar_m<M>l<L>_<group>.png naming",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    # Load & filter for the first N years
    df = load_dataframe(args.db, args.first_years)
    # Determine output filename if not provided,
    # following `bar_m<first-years>l<last-years>_<group>.png` pattern
    if args.out is None:
        span_label = f"l{args.last_years}" if args.last_years is not None else "lall"
        args.out = f"bar_m{args.first_years}{span_label}_{args.group.lower()}.png"
    # Generate bar plot
    make_bar_plots(df, args.group, args.out, args.last_years, args.first_years)


if __name__ == "__main__":
    main() 