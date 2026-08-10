"""
test_edge_cases.py
-------------------
Edge-case tests for the E-Commerce Order Analytics pipeline.

Uses Python's standard `unittest` framework (unittest.TestCase) with 12
edge cases as required by the Week 8 OMIS assignment specification.

Run directly:
    python tests/test_edge_cases.py

Or via unittest discovery:
    python -m unittest discover tests
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "python"))

from clean_data import (   # noqa: E402
    check_referential_integrity,
    clean_customers,
    clean_order_items,
    clean_orders,
    clean_returns,
    compute_rfm_segments,
    validate_emails,
)


# --------------------------------------------------------------------------- #
# DataFrame factory helpers
# --------------------------------------------------------------------------- #

def _orders_df(rows):
    return pd.DataFrame(rows, columns=["order_id", "customer_id", "order_date", "status", "region_code"])


def _items_df(rows):
    return pd.DataFrame(rows, columns=["item_id", "order_id", "product_id", "quantity", "unit_price", "discount_percent"])


def _products_df(rows):
    return pd.DataFrame(rows, columns=["product_id", "product_name", "category", "subcategory", "cost_price"])


def _customers_df(rows):
    return pd.DataFrame(rows, columns=["customer_id", "customer_name", "email", "registration_date", "customer_type"])


def _returns_df(rows):
    return pd.DataFrame(rows, columns=["return_id", "order_id", "product_id", "quantity", "return_date", "reason", "refund_amount"])


# --------------------------------------------------------------------------- #
# Edge Case 1: order_items references an order_id not present in orders
# --------------------------------------------------------------------------- #

class TestOrphanOrderId(unittest.TestCase):
    def test_orphan_order_id_detected(self):
        orders = _orders_df([(1, 100, "2024-01-01 10:00:00", "DELIVERED", "NA-EAST")])
        items = _items_df([
            (1, 1,   10, 2, 20.0, 0),
            (2, 999, 10, 1, 15.0, 0),   # order_id 999 does not exist
        ])
        orphans = check_referential_integrity(orders, items)
        self.assertEqual(len(orphans), 1)
        self.assertEqual(orphans.iloc[0]["order_id"], 999)


# --------------------------------------------------------------------------- #
# Edge Case 2: discount_percent > 100 is clipped to 100
# --------------------------------------------------------------------------- #

class TestInvalidDiscount(unittest.TestCase):
    def test_discount_over_100_clipped(self):
        items = _items_df([
            (1, 1, 10, 2, 20.0, 150),   # invalid: > 100
            (2, 1, 11, 1, 15.0, 50),    # valid
        ])
        from clean_data import ISSUE_LOG
        ISSUE_LOG.clear()
        cleaned = clean_order_items(items)
        self.assertEqual(cleaned.loc[cleaned["item_id"] == 1, "discount_percent"].iloc[0], 100,
                         "Discount > 100 should be clipped to 100")
        self.assertEqual(cleaned.loc[cleaned["item_id"] == 2, "discount_percent"].iloc[0], 50)

    def test_discount_negative_clipped_to_zero(self):
        items = _items_df([
            (1, 1, 10, 2, 20.0, -15),   # negative discount — invalid
        ])
        from clean_data import ISSUE_LOG
        ISSUE_LOG.clear()
        cleaned = clean_order_items(items)
        self.assertEqual(cleaned.loc[cleaned["item_id"] == 1, "discount_percent"].iloc[0], 0)


# --------------------------------------------------------------------------- #
# Edge Case 3: zero quantity is flagged
# --------------------------------------------------------------------------- #

class TestZeroQuantity(unittest.TestCase):
    def test_zero_quantity_flagged(self):
        items = _items_df([
            (1, 1, 10, 0, 20.0, 0),   # zero quantity - no real transaction
            (2, 1, 11, 3, 15.0, 0),
        ])
        from clean_data import ISSUE_LOG
        ISSUE_LOG.clear()
        clean_order_items(items)
        zero_qty_entries = [e for e in ISSUE_LOG if e["rule"] == "zero_quantity"]
        self.assertTrue(len(zero_qty_entries) > 0)
        self.assertEqual(zero_qty_entries[0]["rows_affected"], 1)


# --------------------------------------------------------------------------- #
# Edge Case 4: future order date is flagged (but retained)
# --------------------------------------------------------------------------- #

class TestFutureOrderDate(unittest.TestCase):
    def test_future_order_date_flagged(self):
        orders = _orders_df([
            (1, 100, "2099-01-01 10:00:00", "PLACED",     "NA-EAST"),  # future
            (2, 101, "2024-01-01 10:00:00", "DELIVERED",  "NA-EAST"),  # normal
        ])
        from clean_data import ISSUE_LOG
        ISSUE_LOG.clear()
        clean_orders(orders)
        future_entries = [e for e in ISSUE_LOG if e["rule"] == "future_order_date"]
        self.assertTrue(len(future_entries) > 0)
        self.assertEqual(future_entries[0]["rows_affected"], 1)


# --------------------------------------------------------------------------- #
# Edge Case 5: duplicate order_id — second copy removed
# --------------------------------------------------------------------------- #

class TestDuplicateOrderId(unittest.TestCase):
    def test_duplicate_order_id_removed(self):
        orders = _orders_df([
            (1, 100, "2024-01-01 10:00:00", "DELIVERED", "NA-EAST"),
            (1, 100, "2024-01-01 10:00:00", "DELIVERED", "NA-EAST"),  # duplicate
            (2, 101, "2024-01-02 10:00:00", "PLACED",    "NA-WEST"),
        ])
        cleaned = clean_orders(orders)
        self.assertEqual(len(cleaned), 2, "Expected 2 rows after de-duplication")


# --------------------------------------------------------------------------- #
# Edge Case 6: invalid email detected
# --------------------------------------------------------------------------- #

class TestInvalidEmail(unittest.TestCase):
    def test_invalid_email_detected(self):
        customers = _customers_df([
            (1, "A B", "bademail.com",     "2024-01-01", "REGULAR"),  # no @
            (2, "C D", "good@example.com", "2024-01-01", "REGULAR"),  # valid
        ])
        invalid_ids = validate_emails(customers)
        self.assertEqual(invalid_ids, [1])

    def test_email_with_no_domain_invalid(self):
        customers = _customers_df([
            (1, "A B", "user@",        "2024-01-01", "REGULAR"),  # missing domain
            (2, "C D", "u@domain.com", "2024-01-01", "REGULAR"),  # valid
        ])
        invalid_ids = validate_emails(customers)
        self.assertIn(1, invalid_ids)
        self.assertNotIn(2, invalid_ids)


# --------------------------------------------------------------------------- #
# Edge Case 7: missing customer_id retained and flagged (guest checkout)
# --------------------------------------------------------------------------- #

class TestMissingCustomerId(unittest.TestCase):
    def test_missing_customer_id_retained(self):
        orders = _orders_df([
            (1, "",  "2024-01-01 10:00:00", "PLACED",  "NA-EAST"),  # blank customer_id
            (2, 101, "2024-01-02 10:00:00", "PLACED",  "NA-WEST"),
        ])
        from clean_data import ISSUE_LOG
        ISSUE_LOG.clear()
        cleaned = clean_orders(orders)
        self.assertEqual(len(cleaned), 2, "Missing customer_id rows should NOT be dropped")
        missing_entries = [e for e in ISSUE_LOG if e["rule"] == "missing_customer_id"]
        self.assertTrue(len(missing_entries) > 0)
        self.assertEqual(missing_entries[0]["rows_affected"], 1)


# --------------------------------------------------------------------------- #
# Edge Case 8: returns with non-positive quantity are quarantined / removed
# --------------------------------------------------------------------------- #

class TestReturnsNonPositiveQuantity(unittest.TestCase):
    def test_returns_zero_quantity_removed(self):
        returns = _returns_df([
            (1, 10, 5, 0, "2024-02-01", "DEFECTIVE", 0.0),    # zero quantity — invalid
            (2, 11, 6, 2, "2024-02-02", "WRONG_ITEM", 45.0),  # valid
        ])
        from clean_data import ISSUE_LOG
        ISSUE_LOG.clear()
        cleaned = clean_returns(returns)
        self.assertEqual(len(cleaned), 1, "Zero-quantity return should be removed")
        self.assertEqual(cleaned.iloc[0]["return_id"], 2)

    def test_returns_negative_refund_clipped(self):
        returns = _returns_df([
            (1, 10, 5, 1, "2024-02-01", "DEFECTIVE", -20.0),  # negative refund
        ])
        from clean_data import ISSUE_LOG
        ISSUE_LOG.clear()
        cleaned = clean_returns(returns)
        self.assertEqual(cleaned.iloc[0]["refund_amount"], 0.0)


# --------------------------------------------------------------------------- #
# Edge Case 9: duplicate customer_id — second copy removed
# --------------------------------------------------------------------------- #

class TestDuplicateCustomerId(unittest.TestCase):
    def test_duplicate_customer_id_removed(self):
        customers = _customers_df([
            (1, "Alice Smith", "alice@example.com", "2024-01-01", "REGULAR"),
            (1, "Alice Smith", "alice@example.com", "2024-01-01", "REGULAR"),  # duplicate
            (2, "Bob Jones",   "bob@example.com",   "2024-01-02", "PREMIUM"),
        ])
        from clean_data import ISSUE_LOG
        ISSUE_LOG.clear()
        cleaned = clean_customers(customers)
        self.assertEqual(len(cleaned), 2)


# --------------------------------------------------------------------------- #
# Edge Case 10: messy product names are normalized to Title Case
# --------------------------------------------------------------------------- #

class TestMessyProductNames(unittest.TestCase):
    def test_messy_names_normalized(self):
        from clean_data import clean_products
        products = _products_df([
            (1, "  PRO SMARTPHONE  ", "Electronics", "Phones", 999.99),  # messy
            (2, "classic laptop",    "Electronics", "Laptops", 599.99),  # lowercase
            (3, "Deluxe Speaker",    "Electronics", "Audio",   99.99),   # already correct
        ])
        from clean_data import ISSUE_LOG
        ISSUE_LOG.clear()
        cleaned = clean_products(products)
        self.assertEqual(cleaned.loc[cleaned["product_id"] == 1, "product_name"].iloc[0], "Pro Smartphone")
        self.assertEqual(cleaned.loc[cleaned["product_id"] == 2, "product_name"].iloc[0], "Classic Laptop")
        self.assertEqual(cleaned.loc[cleaned["product_id"] == 3, "product_name"].iloc[0], "Deluxe Speaker")


# --------------------------------------------------------------------------- #
# Edge Case 11: malformed date repaired (DD-MM-YYYY -> ISO)
# --------------------------------------------------------------------------- #

class TestMalformedDateRepair(unittest.TestCase):
    def test_dd_mm_yyyy_repaired(self):
        orders = _orders_df([
            (1, 100, "15-03-2024", "PLACED", "NA-EAST"),    # DD-MM-YYYY
            (2, 101, "2024-04-01 00:00:00", "PLACED", "NA-WEST"),  # ISO
        ])
        from clean_data import ISSUE_LOG
        ISSUE_LOG.clear()
        cleaned = clean_orders(orders)
        # Both rows should survive — dates get normalized
        self.assertEqual(len(cleaned), 2)
        # Row 1 date should be parseable in ISO format after cleaning
        date_str = cleaned.loc[cleaned["order_id"] == 1, "order_date"].iloc[0]
        self.assertIn("2024-03-15", date_str)


# --------------------------------------------------------------------------- #
# Edge Case 12: RFM segmentation assigns correct VIP label
# --------------------------------------------------------------------------- #

class TestRFMSegmentation(unittest.TestCase):
    def test_rfm_segments_computed(self):
        """RFM segmentation runs without error and returns expected segment labels."""
        orders = _orders_df([
            (1, 1, "2025-01-01 00:00:00", "DELIVERED", "NA-EAST"),
            (2, 1, "2025-06-01 00:00:00", "DELIVERED", "NA-EAST"),
            (3, 2, "2023-01-01 00:00:00", "DELIVERED", "NA-WEST"),
            (4, 3, "2025-07-01 00:00:00", "DELIVERED", "EU-CENTRAL"),
        ])
        items = _items_df([
            (1, 1, 10, 2, 500.0, 0),
            (2, 2, 10, 3, 800.0, 0),
            (3, 3, 11, 1,  20.0, 0),
            (4, 4, 12, 5, 300.0, 0),
        ])
        rfm = compute_rfm_segments(orders, items)
        self.assertFalse(rfm.empty, "RFM result should not be empty")
        self.assertIn("segment", rfm.columns)
        valid_segments = {"VIP", "High Value", "Regular", "Occasional", "At Risk"}
        for seg in rfm["segment"]:
            self.assertIn(seg, valid_segments, f"Unexpected segment: {seg}")


# --------------------------------------------------------------------------- #
# Test runner
# --------------------------------------------------------------------------- #

def main() -> int:
    """Run all tests and write a testing_report.txt summary."""
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(sys.modules[__name__])
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(suite)

    # Write report
    report_path = Path(__file__).resolve().parent.parent / "data" / "reports" / "testing_report.txt"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w") as f:
        f.write("EDGE CASE TESTING REPORT (unittest)\n" + "=" * 50 + "\n\n")
        f.write(f"Tests run:    {result.testsRun}\n")
        f.write(f"Failures:     {len(result.failures)}\n")
        f.write(f"Errors:       {len(result.errors)}\n")
        f.write(f"Skipped:      {len(result.skipped)}\n\n")
        if result.failures:
            f.write("FAILURES:\n")
            for test, tb in result.failures:
                f.write(f"  {test}: {tb}\n")
        if result.errors:
            f.write("ERRORS:\n")
            for test, tb in result.errors:
                f.write(f"  {test}: {tb}\n")
        if result.wasSuccessful():
            f.write("RESULT: ALL TESTS PASSED\n")
        else:
            f.write("RESULT: SOME TESTS FAILED\n")

    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
