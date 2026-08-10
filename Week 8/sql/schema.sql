-- schema.sql
-- SQLite schema for the E-Commerce Order Analytics System.
-- Run via loader.py, or manually: sqlite3 ecommerce.db < schema.sql
-- Schema: 3NF Star/Snowflake hybrid, 5 entities

PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS returns;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    customer_id       INTEGER PRIMARY KEY,
    customer_name     TEXT NOT NULL,
    email             TEXT,
    registration_date TEXT NOT NULL,   -- ISO date YYYY-MM-DD
    customer_type     TEXT NOT NULL CHECK (customer_type IN ('REGULAR', 'PREMIUM', 'VIP'))
);

CREATE TABLE products (
    product_id   INTEGER PRIMARY KEY,
    product_name TEXT NOT NULL,
    category     TEXT NOT NULL,
    subcategory  TEXT,
    cost_price   REAL NOT NULL CHECK (cost_price > 0)
);

CREATE TABLE orders (
    order_id     INTEGER PRIMARY KEY,
    customer_id  INTEGER,              -- nullable: guest / missing customer
    order_date   TEXT NOT NULL,        -- ISO datetime YYYY-MM-DD HH:MM:SS
    status       TEXT NOT NULL CHECK (status IN ('PLACED','SHIPPED','DELIVERED','CANCELLED','RETURNED')),
    region_code  TEXT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers (customer_id) ON DELETE CASCADE
);

CREATE TABLE order_items (
    item_id          INTEGER PRIMARY KEY,
    order_id         INTEGER NOT NULL,
    product_id       INTEGER NOT NULL,
    quantity         INTEGER NOT NULL CHECK (quantity != 0),  -- non-zero; positives = purchases
    unit_price       REAL NOT NULL CHECK (unit_price >= 0),
    discount_percent REAL NOT NULL CHECK (discount_percent BETWEEN 0 AND 100),
    FOREIGN KEY (order_id)    REFERENCES orders   (order_id)   ON DELETE CASCADE,
    FOREIGN KEY (product_id)  REFERENCES products (product_id) ON DELETE CASCADE
);

-- Returns: dedicated 5th entity (explicit return events, separate from order_items)
CREATE TABLE returns (
    return_id     INTEGER PRIMARY KEY,
    order_id      INTEGER NOT NULL,
    product_id    INTEGER NOT NULL,
    quantity      INTEGER NOT NULL CHECK (quantity > 0),  -- always positive; records units returned
    return_date   TEXT NOT NULL,          -- ISO date YYYY-MM-DD
    reason        TEXT NOT NULL CHECK (reason IN ('DEFECTIVE','WRONG_ITEM','NOT_AS_DESCRIBED','CHANGED_MIND','DAMAGED_SHIPPING','OTHER')),
    refund_amount REAL NOT NULL CHECK (refund_amount >= 0),
    FOREIGN KEY (order_id)   REFERENCES orders   (order_id)   ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products (product_id) ON DELETE CASCADE
);

CREATE INDEX idx_orders_customer   ON orders      (customer_id);
CREATE INDEX idx_orders_date       ON orders      (order_date);
CREATE INDEX idx_items_order       ON order_items (order_id);
CREATE INDEX idx_items_product     ON order_items (product_id);
CREATE INDEX idx_products_category ON products    (category);
CREATE INDEX idx_returns_order     ON returns     (order_id);
CREATE INDEX idx_returns_date      ON returns     (return_date);
