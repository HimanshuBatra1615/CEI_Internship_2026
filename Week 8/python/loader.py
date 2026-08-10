"""
loader.py
---------
Loads the cleaned CSVs into a SQLite database using the schema in sql/schema.sql.

Run directly:
    python loader.py
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from config import CLEAN_DIR, DB_PATH, SCHEMA_PATH
from utils import get_logger, timer

logger = get_logger(__name__)

# Order matters: parent tables before child tables (FK constraints)
TABLE_LOAD_ORDER = ["customers", "products", "orders", "order_items", "returns"]


def init_db(db_path: Path = DB_PATH, schema_path: Path = SCHEMA_PATH) -> sqlite3.Connection:
    """Create (or reset) the SQLite database from schema.sql and return an open connection."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    schema_sql = schema_path.read_text(encoding="utf-8")
    conn.executescript(schema_sql)
    conn.commit()
    logger.info("Database initialized at %s", db_path)
    return conn


def load_table(conn: sqlite3.Connection, table_name: str, csv_path: Path) -> int:
    """Load a single cleaned CSV into its matching table. Returns row count loaded."""
    df = pd.read_csv(csv_path)
    df.to_sql(table_name, conn, if_exists="append", index=False)
    conn.commit()
    logger.info("Loaded %d rows into %s", len(df), table_name)
    return len(df)


@timer(logger)
def load_all(conn: sqlite3.Connection | None = None) -> sqlite3.Connection:
    """Initialize the DB and load all cleaned tables in FK-safe order."""
    if conn is None:
        conn = init_db()
    for table in TABLE_LOAD_ORDER:
        csv_path = CLEAN_DIR / f"{table}.csv"
        if not csv_path.exists():
            if table == "returns":
                logger.warning("returns.csv not found — skipping returns load.")
                continue
            raise FileNotFoundError(
                f"Missing cleaned file: {csv_path}. Run clean_data.py first."
            )
        load_table(conn, table, csv_path)
    return conn


if __name__ == "__main__":
    connection = load_all()
    connection.close()
    logger.info("Load complete.")
