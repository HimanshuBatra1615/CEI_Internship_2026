"""
init_db.py
----------
Standalone database initialization script. Wipes and recreates
data/ecommerce.db from sql/schema.sql without loading any data — useful
for resetting the database or verifying the schema applies cleanly on
its own (e.g. in CI, before an integration test loads fixture data).

For a full reset-and-load, use loader.py's load_all() instead.

Run directly:
    python init_db.py
"""

from __future__ import annotations

from config import DB_PATH, SCHEMA_PATH
from utils import get_logger
from loader import init_db

logger = get_logger(__name__)

if __name__ == "__main__":
    conn = init_db(DB_PATH, SCHEMA_PATH)
    conn.close()
    logger.info("Schema-only initialization complete (%s tables created, no data loaded).", DB_PATH.name)
