"""
config.py
---------
Central configuration for the E-Commerce Order Analytics System.
All paths, thresholds, and tunable constants live here so nothing is
hardcoded inside the pipeline scripts.
"""

from __future__ import annotations

from pathlib import Path

# --------------------------------------------------------------------------- #
# Paths
# --------------------------------------------------------------------------- #

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
CLEAN_DIR = BASE_DIR / "data" / "cleaned"
REJECTED_DIR = BASE_DIR / "data" / "rejected"   # quarantine for bad rows
REPORT_DIR = BASE_DIR / "data" / "reports"
SQL_DIR = BASE_DIR / "sql"
DOCS_DIR = BASE_DIR / "docs"
SCREENSHOT_DIR = BASE_DIR / "screenshots"
DB_PATH = BASE_DIR / "data" / "ecommerce.db"
SCHEMA_PATH = SQL_DIR / "schema.sql"

# --------------------------------------------------------------------------- #
# Data generation volumes
# --------------------------------------------------------------------------- #

RANDOM_SEED = 42
N_CUSTOMERS = 600
N_PRODUCTS = 220
N_ORDERS = 1500

# --------------------------------------------------------------------------- #
# Injected data-quality issue rates
# --------------------------------------------------------------------------- #

PCT_NULL_CUSTOMER_ID = 0.05
PCT_BAD_DATE_FORMAT = 0.06
PCT_MESSY_PRODUCT_NAME = 0.08
PCT_INVALID_EMAIL = 0.02
PCT_ORPHAN_ORDER_ITEMS = 0.01
PCT_INVALID_DISCOUNT = 0.015
PCT_FUTURE_DATE = 0.01

# --------------------------------------------------------------------------- #
# Domain constants
# --------------------------------------------------------------------------- #

REGIONS = ["NA-EAST", "NA-WEST", "EU-CENTRAL", "EU-WEST", "APAC", "LATAM"]
STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
STATUS_WEIGHTS = [0.10, 0.15, 0.55, 0.12, 0.08]
CUSTOMER_TYPES = ["REGULAR", "PREMIUM", "VIP"]
CUSTOMER_TYPE_WEIGHTS = [0.70, 0.22, 0.08]
VALID_CATEGORIES = {"Electronics", "Clothing", "Home", "Books"}

CATEGORY_MAP = {
    "Electronics": ["Phones", "Laptops", "Audio", "Accessories", "Cameras"],
    "Clothing": ["Men", "Women", "Kids", "Footwear", "Winterwear"],
    "Home": ["Kitchen", "Furniture", "Decor", "Bedding", "Appliances"],
    "Books": ["Fiction", "Non-Fiction", "Comics", "Academic", "Children"],
}

# --------------------------------------------------------------------------- #
# CLI / report thresholds
# --------------------------------------------------------------------------- #

TOP_N_PRODUCTS = 3
AT_RISK_AVG_GAP_DAYS = 30
