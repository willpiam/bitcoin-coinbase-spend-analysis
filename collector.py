import os
import sqlite3
from datetime import datetime
from typing import Optional

from google.cloud import bigquery
from google.api_core.exceptions import BadRequest, GoogleAPICallError
from tqdm import tqdm
from dotenv import load_dotenv


load_dotenv()

# Constants ---------------------------------------------------------------
BILLING_PROJECT = os.getenv("BQ_PROJECT")  # project that will be billed for the query jobs
DATA_PROJECT = os.getenv("BQ_DATA_PROJECT", "bigquery-public-data")  # where the crypto_bitcoin dataset lives
if not BILLING_PROJECT:
    raise EnvironmentError("Environment variable BQ_PROJECT (billing project) is required but not set.")

DATASET = "crypto_bitcoin"
TRANSACTIONS_TABLE = f"{DATA_PROJECT}.{DATASET}.transactions"
INPUTS_TABLE = f"{DATA_PROJECT}.{DATASET}.inputs"

SQLITE_DB_PATH = os.getenv("SQLITE_DB", "coinbase_spending.db")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))  # number of blocks per batch

# SQL statements ----------------------------------------------------------
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS coinbase_spends (
    coinbase_txid TEXT NOT NULL,
    output_index INTEGER NOT NULL,
    value_sats INTEGER NOT NULL,
    creation_block_height INTEGER NOT NULL,
    creation_block_time TEXT NOT NULL,
    spend_txid TEXT,
    spend_block_height INTEGER,
    spend_block_time TEXT,
    PRIMARY KEY (coinbase_txid, output_index)
);
"""

CREATE_META_SQL = """
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""

INSERT_DATA_SQL = """
INSERT OR IGNORE INTO coinbase_spends (
    coinbase_txid, output_index, value_sats, creation_block_height, creation_block_time,
    spend_txid, spend_block_height, spend_block_time
) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
"""

UPSERT_META_SQL = """
INSERT INTO metadata(key, value) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value;
"""

SELECT_META_SQL = "SELECT value FROM metadata WHERE key = ?;"

# BigQuery query template -------------------------------------------------
QUERY_TEMPLATE = """
DECLARE start_height INT64 DEFAULT @start_height;
DECLARE end_height INT64 DEFAULT @end_height;

WITH coinbase_outputs AS (
  SELECT
    tx.block_number AS creation_block_height,
    tx.block_timestamp AS creation_block_time,
    tx.hash AS coinbase_txid,
    out.index AS output_index,
    out.value AS value_sats
  FROM `{transactions}` AS tx, UNNEST(tx.outputs) AS out
  WHERE tx.block_number BETWEEN start_height AND end_height
    AND tx.is_coinbase = TRUE
)
SELECT
  c.creation_block_height,
  c.creation_block_time,
  c.coinbase_txid,
  c.output_index,
  c.value_sats,
  i.transaction_hash AS spend_txid,
  i.block_number AS spend_block_height,
  i.block_timestamp AS spend_block_time
FROM coinbase_outputs AS c
LEFT JOIN `{inputs}` i
  ON i.spent_transaction_hash = c.coinbase_txid AND i.spent_output_index = c.output_index;
""".format(transactions=TRANSACTIONS_TABLE, inputs=INPUTS_TABLE)


# Helper functions --------------------------------------------------------

def to_int(value):
    """Cast BigQuery numeric scalar (or numpy types) to built-in int so that
    sqlite3 can bind the parameter. Returns None unchanged."""
    if value is None:
        return None
    return int(value)


def get_last_processed_height(conn: sqlite3.Connection) -> int:
    cur = conn.execute(SELECT_META_SQL, ("last_processed_height",))
    row = cur.fetchone()
    return int(row[0]) if row else -1


def set_last_processed_height(conn: sqlite3.Connection, height: int):
    conn.execute(UPSERT_META_SQL, ("last_processed_height", str(height)))
    conn.commit()


def get_max_block_height(client: bigquery.Client) -> int:
    query = f"SELECT MAX(block_number) AS max_height FROM `{TRANSACTIONS_TABLE}`"
    result = client.query(query).result()
    for row in result:
        return to_int(row["max_height"])
    raise RuntimeError("Failed to retrieve max block height")


def fetch_batch(client: bigquery.Client, start: int, end: int):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_height", "INT64", start),
            bigquery.ScalarQueryParameter("end_height", "INT64", end),
        ]
    )
    query_job = client.query(QUERY_TEMPLATE, job_config=job_config)
    return query_job.result()


def ensure_schema(conn: sqlite3.Connection):
    conn.execute(CREATE_TABLE_SQL)
    conn.execute(CREATE_META_SQL)
    conn.commit()


# Main routine ------------------------------------------------------------

def main():
    print("Starting Coinbase Spending Collector")
    client = bigquery.Client(project=BILLING_PROJECT)

    conn = sqlite3.connect(SQLITE_DB_PATH)
    ensure_schema(conn)

    last_height = get_last_processed_height(conn)
    print(f"Last processed block height: {last_height}")

    try:
        max_height = get_max_block_height(client)
    except GoogleAPICallError as exc:
        print(f"Error fetching max block height: {exc}")
        return

    print(f"Current blockchain height in dataset: {max_height}")

    if last_height >= max_height:
        print("Database is already up to date. Nothing to do.")
        return

    start = last_height + 1
    end = max_height

    with tqdm(total=(end - start + 1), unit="block") as pbar:
        batch_start = start
        while batch_start <= end:
            batch_end = min(batch_start + BATCH_SIZE - 1, end)
            try:
                results = fetch_batch(client, batch_start, batch_end)
            except BadRequest as exc:
                print(f"BigQuery error for batch {batch_start}-{batch_end}: {exc}")
                return

            rows_to_insert = [
                (
                    row["coinbase_txid"],
                    to_int(row["output_index"]),
                    to_int(row["value_sats"]),
                    to_int(row["creation_block_height"]),
                    row["creation_block_time"].isoformat() if row["creation_block_time"] else None,
                    row["spend_txid"],
                    to_int(row["spend_block_height"]),
                    row["spend_block_time"].isoformat() if row["spend_block_time"] else None,
                )
                for row in results
            ]

            conn.executemany(INSERT_DATA_SQL, rows_to_insert)
            conn.commit()

            set_last_processed_height(conn, batch_end)
            pbar.update(batch_end - batch_start + 1)

            batch_start = batch_end + 1

    print("Data collection completed successfully.")


if __name__ == "__main__":
    main() 