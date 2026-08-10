"""
generate_data.py
-----------------
Generates synthetic e-commerce data for the Order Analytics System.

Produces five related CSV files (5 entities):
    - customers.csv
    - products.csv
    - orders.csv
    - order_items.csv
    - returns.csv       (dedicated returns entity, 5th table)

The data simulates realistic e-commerce behaviour (seasonal ordering,
VIP/repeat customers, regional spread) while injecting the specific,
controlled data-quality problems required by the assignment so that the
cleaning/validation pipeline has real issues to catch.

Run directly:
    python generate_data.py
"""

from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from config import (
    CATEGORY_MAP,
    CUSTOMER_TYPES,
    CUSTOMER_TYPE_WEIGHTS,
    N_CUSTOMERS,
    N_ORDERS,
    N_PRODUCTS,
    PCT_BAD_DATE_FORMAT,
    PCT_FUTURE_DATE,
    PCT_INVALID_DISCOUNT,
    PCT_INVALID_EMAIL,
    PCT_MESSY_PRODUCT_NAME,
    PCT_NULL_CUSTOMER_ID,
    PCT_ORPHAN_ORDER_ITEMS,
    RANDOM_SEED,
    RAW_DIR,
    REGIONS,
    STATUSES,
    STATUS_WEIGHTS,
)
from utils import ProgressBar, get_logger, timer

logger = get_logger(__name__)
random.seed(RANDOM_SEED)

FIRST_NAMES = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
               "Priya", "Arjun", "Wei", "Mei", "Carlos", "Sofia", "Ahmed", "Fatima", "Liam",
               "Olivia", "Noah", "Emma", "Yuki", "Hana", "Diego", "Valentina"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
              "Patel", "Kumar", "Chen", "Wang", "Rodriguez", "Silva", "Khan", "Ali", "Muller",
              "Rossi", "Kim", "Park", "Suzuki", "Sato", "Costa", "Almeida"]

PRODUCT_ADJECTIVES = ["Pro", "Ultra", "Max", "Lite", "Classic", "Deluxe", "Compact", "Smart", "Essential", "Premium"]
PRODUCT_NOUNS = {
    "Phones": ["Smartphone", "Flip Phone", "5G Handset"],
    "Laptops": ["Laptop", "Ultrabook", "Notebook"],
    "Audio": ["Headphones", "Earbuds", "Speaker"],
    "Accessories": ["Charger", "Case", "Cable", "Power Bank"],
    "Cameras": ["DSLR Camera", "Action Camera", "Webcam"],
    "Men": ["T-Shirt", "Jeans", "Jacket", "Shirt"],
    "Women": ["Dress", "Top", "Leggings", "Blouse"],
    "Kids": ["T-Shirt", "Shorts", "Onesie"],
    "Footwear": ["Sneakers", "Sandals", "Boots"],
    "Winterwear": ["Sweater", "Coat", "Scarf"],
    "Kitchen": ["Blender", "Cookware Set", "Knife Set"],
    "Furniture": ["Office Chair", "Bookshelf", "Coffee Table"],
    "Decor": ["Wall Art", "Vase", "Candle Set"],
    "Bedding": ["Pillow Set", "Comforter", "Bedsheet Set"],
    "Appliances": ["Air Fryer", "Microwave", "Vacuum Cleaner"],
    "Fiction": ["Novel", "Short Story Collection", "Thriller"],
    "Non-Fiction": ["Biography", "Self-Help Book", "History Book"],
    "Comics": ["Graphic Novel", "Comic Bundle"],
    "Academic": ["Textbook", "Study Guide"],
    "Children": ["Picture Book", "Activity Book"],
}


RETURN_REASONS = ["DEFECTIVE", "WRONG_ITEM", "NOT_AS_DESCRIBED", "CHANGED_MIND", "DAMAGED_SHIPPING", "OTHER"]
RETURN_REASON_WEIGHTS = [0.30, 0.20, 0.20, 0.15, 0.10, 0.05]


@dataclass
class GenerationStats:
    """Tracks how many intentional issues were injected, for the run summary."""
    null_customer_ids: int = 0
    bad_date_formats: int = 0
    future_dates: int = 0
    orphan_order_items: int = 0
    invalid_discounts: int = 0
    messy_product_names: int = 0
    invalid_emails: int = 0
    returns_generated: int = 0


STATS = GenerationStats()


# --------------------------------------------------------------------------- #
# Customers
# --------------------------------------------------------------------------- #

def generate_customers(n: int) -> list[dict]:
    """Generate customer records with a mix of regular/premium/VIP shoppers."""
    customers = []
    start = datetime(2022, 1, 1)
    end = datetime(2026, 6, 1)
    span_days = (end - start).days

    for cid in range(1, n + 1):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        name = f"{first} {last}"
        email = f"{first.lower()}.{last.lower()}{cid}@example.com"

        # Inject invalid emails (missing @ or missing domain)
        if random.random() < PCT_INVALID_EMAIL:
            variant = random.choice(["no_at", "no_domain"])
            if variant == "no_at":
                email = email.replace("@", "")
            else:
                email = email.split("@")[0] + "@"
            STATS.invalid_emails += 1

        reg_date = start + timedelta(days=random.randint(0, span_days))
        customer_type = random.choices(CUSTOMER_TYPES, weights=CUSTOMER_TYPE_WEIGHTS, k=1)[0]

        customers.append({
            "customer_id": cid,
            "customer_name": name,
            "email": email,
            "registration_date": reg_date.strftime("%Y-%m-%d"),
            "customer_type": customer_type,
        })
    return customers


# --------------------------------------------------------------------------- #
# Products
# --------------------------------------------------------------------------- #

def generate_products(n: int) -> list[dict]:
    """Generate a product catalog spanning 4 categories with realistic price tiers."""
    products = []
    for pid in range(1, n + 1):
        category = random.choice(list(CATEGORY_MAP.keys()))
        subcategory = random.choice(CATEGORY_MAP[category])
        noun = random.choice(PRODUCT_NOUNS[subcategory])
        adjective = random.choice(PRODUCT_ADJECTIVES)
        name = f"{adjective} {noun}"

        # Category-appropriate cost price ranges (Electronics costliest)
        price_ranges = {
            "Electronics": (40, 1800),
            "Clothing": (8, 150),
            "Home": (10, 500),
            "Books": (5, 60),
        }
        low, high = price_ranges[category]
        cost_price = round(random.uniform(low, high), 2)

        # Inject messy formatting: extra whitespace / inconsistent casing
        if random.random() < PCT_MESSY_PRODUCT_NAME:
            variant = random.choice(["upper", "lower", "spaces"])
            if variant == "upper":
                name = name.upper()
            elif variant == "lower":
                name = name.lower()
            else:
                name = f"  {name}   "
            STATS.messy_product_names += 1

        products.append({
            "product_id": pid,
            "product_name": name,
            "category": category,
            "subcategory": subcategory,
            "cost_price": cost_price,
        })
    return products


# --------------------------------------------------------------------------- #
# Orders
# --------------------------------------------------------------------------- #

def _seasonal_weight(dt: datetime) -> float:
    """Boost order likelihood around Nov-Dec (holiday season) and mid-year sales."""
    weight = 1.0
    if dt.month in (11, 12):
        weight *= 2.2
    if dt.month == 7:
        weight *= 1.4
    return weight


def generate_orders(n: int, customer_ids: list[int]) -> list[dict]:
    """Generate orders with seasonal skew, VIP repeat-buyer behaviour, and injected issues."""
    orders = []
    start = datetime(2024, 1, 1)
    end = datetime(2026, 8, 1)
    span_days = (end - start).days

    # A subset of customers behave as frequent "repeat buyers"
    repeat_buyers = set(random.sample(customer_ids, k=max(1, n // 10)))

    bar = ProgressBar(total=n, label="Generating orders")
    order_id = 1
    while order_id <= n:
        # Weighted date pick favouring seasonal months (simple rejection sampling)
        while True:
            candidate = start + timedelta(days=random.randint(0, span_days))
            if random.random() < _seasonal_weight(candidate) / 2.2:
                order_dt = candidate
                break

        # Repeat buyers are picked more often
        if random.random() < 0.4 and repeat_buyers:
            customer_id = random.choice(list(repeat_buyers))
        else:
            customer_id = random.choice(customer_ids)

        status = random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
        region = random.choice(REGIONS)

        # Inject NULL customer_id
        customer_id_out: int | str = customer_id
        if random.random() < PCT_NULL_CUSTOMER_ID:
            customer_id_out = ""
            STATS.null_customer_ids += 1

        # Inject a wrong-format date (DD-MM-YYYY) vs the standard YYYY-MM-DD HH:MM:SS
        if random.random() < PCT_BAD_DATE_FORMAT:
            order_date_str = order_dt.strftime("%d-%m-%Y")
            STATS.bad_date_formats += 1
        elif random.random() < PCT_FUTURE_DATE:
            future_dt = datetime(2027, random.randint(1, 12), random.randint(1, 28))
            order_date_str = future_dt.strftime("%Y-%m-%d %H:%M:%S")
            STATS.future_dates += 1
        else:
            order_date_str = order_dt.strftime("%Y-%m-%d %H:%M:%S")

        orders.append({
            "order_id": order_id,
            "customer_id": customer_id_out,
            "order_date": order_date_str,
            "status": status,
            "region_code": region,
        })
        order_id += 1
        bar.update(1)

    bar.finish()
    return orders


# --------------------------------------------------------------------------- #
# Order Items
# --------------------------------------------------------------------------- #

def generate_order_items(orders: list[dict], product_ids: list[int]) -> list[dict]:
    """Generate 1-4 line items per order with realistic discounting."""
    items = []
    item_id = 1
    max_valid_order_id = max(o["order_id"] for o in orders)

    bar = ProgressBar(total=len(orders), label="Generating order_items")
    for order in orders:
        n_items = random.choices([1, 2, 3, 4], weights=[0.45, 0.30, 0.17, 0.08], k=1)[0]
        for _ in range(n_items):
            product_id = random.choice(product_ids)
            quantity = random.randint(1, 5)          # always positive; returns now in returns.csv
            unit_price = round(random.uniform(5, 1800), 2)
            discount_percent = round(random.choices(
                [0, 5, 10, 15, 20, 25, 30],
                weights=[0.35, 0.15, 0.15, 0.15, 0.10, 0.06, 0.04], k=1
            )[0] + random.uniform(0, 2), 2)

            # Inject invalid discount (> 100)
            if random.random() < PCT_INVALID_DISCOUNT:
                discount_percent = round(random.uniform(101, 150), 2)
                STATS.invalid_discounts += 1

            order_id_out = order["order_id"]
            # Inject orphan order_items referencing a non-existent order_id
            if random.random() < PCT_ORPHAN_ORDER_ITEMS:
                order_id_out = max_valid_order_id + random.randint(1000, 9999)
                STATS.orphan_order_items += 1

            items.append({
                "item_id": item_id,
                "order_id": order_id_out,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_percent": discount_percent,
            })
            item_id += 1
        bar.update(1)

    bar.finish()
    return items


# --------------------------------------------------------------------------- #
# Returns (dedicated 5th entity)
# --------------------------------------------------------------------------- #

PCT_RETURN = 0.12   # ~12% of DELIVERED orders have at least one return


def generate_returns(orders: list[dict], order_items: list[dict]) -> list[dict]:
    """Generate explicit return records linked to delivered orders.

    Each return references a real (order_id, product_id) pair from order_items
    so foreign-key constraints hold after the cleaning pipeline runs.
    """
    # Build a lookup: order_id -> list of (product_id, unit_price, quantity)
    items_by_order: dict[int, list[dict]] = {}
    for item in order_items:
        oid = item["order_id"]
        items_by_order.setdefault(oid, []).append(item)

    delivered_orders = [
        o for o in orders
        if o["status"] == "DELIVERED" and str(o["order_id"]) in
        {str(k) for k in items_by_order}
    ]

    returns: list[dict] = []
    return_id = 1
    bar = ProgressBar(total=len(delivered_orders), label="Generating returns")
    for order in delivered_orders:
        bar.update(1)
        if random.random() >= PCT_RETURN:
            continue
        # Choose 1 item from the order to return
        candidates = items_by_order.get(order["order_id"], [])
        if not candidates:
            continue
        item = random.choice(candidates)
        return_qty = random.randint(1, max(1, item["quantity"]))
        # Return date is 1-30 days after order date (best-effort parse)
        try:
            from datetime import datetime, timedelta
            order_dt = datetime.strptime(str(order["order_date"])[:10], "%Y-%m-%d")
        except ValueError:
            continue
        return_dt = order_dt + timedelta(days=random.randint(1, 30))
        refund = round(return_qty * item["unit_price"] * (1 - item["discount_percent"] / 100.0), 2)
        reason = random.choices(RETURN_REASONS, weights=RETURN_REASON_WEIGHTS, k=1)[0]
        returns.append({
            "return_id":     return_id,
            "order_id":      order["order_id"],
            "product_id":    item["product_id"],
            "quantity":      return_qty,
            "return_date":   return_dt.strftime("%Y-%m-%d"),
            "reason":        reason,
            "refund_amount": max(0.0, refund),
        })
        return_id += 1
        STATS.returns_generated += 1
    bar.finish()
    return returns


# --------------------------------------------------------------------------- #
# I/O helpers
# --------------------------------------------------------------------------- #

def write_csv(rows: list[dict], path: Path) -> None:
    """Write a list of dict rows to CSV, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        logger.warning("No rows to write for %s", path)
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    logger.info("Wrote %d rows -> %s", len(rows), path)


@timer(logger)
def main() -> None:
    logger.info("Starting synthetic data generation (seed=%d)", RANDOM_SEED)

    customers = generate_customers(N_CUSTOMERS)
    write_csv(customers, RAW_DIR / "customers.csv")

    products = generate_products(N_PRODUCTS)
    write_csv(products, RAW_DIR / "products.csv")

    customer_ids = [c["customer_id"] for c in customers]
    orders = generate_orders(N_ORDERS, customer_ids)
    write_csv(orders, RAW_DIR / "orders.csv")

    product_ids = [p["product_id"] for p in products]
    order_items = generate_order_items(orders, product_ids)
    write_csv(order_items, RAW_DIR / "order_items.csv")

    # 5th entity: dedicated returns table
    returns = generate_returns(orders, order_items)
    write_csv(returns, RAW_DIR / "returns.csv")

    logger.info("Generation complete. Injected-issue summary: %s", STATS)


if __name__ == "__main__":
    main()
