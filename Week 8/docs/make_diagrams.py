"""
make_diagrams.py (one-off doc generation script, not part of the pipeline)
----------------------------------------------------------------------------
Generates docs/architecture.png and docs/er_diagram.png using matplotlib.
Run once to (re)produce the diagrams referenced in README.md.
"""

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

OUT_DIR = "/home/claude/ecommerce_analytics/docs"

NAVY = "#1f2a44"
BLUE = "#3b6ea5"
TEAL = "#2f8f8f"
AMBER = "#c98a2c"
GREY = "#5b6472"
LIGHT = "#eef1f6"


def box(ax, xy, w, h, text, color, fontsize=10, text_color="white"):
    x, y = xy
    fb = FancyBboxPatch((x, y), w, h,
                         boxstyle="round,pad=0.02,rounding_size=0.08",
                         linewidth=1.2, edgecolor=color, facecolor=color, alpha=0.92)
    ax.add_patch(fb)
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center",
             fontsize=fontsize, color=text_color, weight="bold", wrap=True)
    return (x, y, w, h)


def arrow(ax, start, end, color=GREY):
    a = FancyArrowPatch(start, end, arrowstyle="-|>", mutation_scale=16,
                         linewidth=1.6, color=color, shrinkA=2, shrinkB=2)
    ax.add_patch(a)


def make_architecture_diagram():
    fig, ax = plt.subplots(figsize=(12, 7))
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 7)
    ax.axis("off")
    ax.set_facecolor(LIGHT)
    fig.patch.set_facecolor(LIGHT)

    ax.text(6, 6.6, "E-Commerce Order Analytics — Pipeline Architecture",
            ha="center", fontsize=15, weight="bold", color=NAVY)

    stages = [
        ("1. GENERATE\ngenerate_data.py", BLUE, 0.4),
        ("2. CLEAN & VALIDATE\nclean_data.py / validators.py", TEAL, 3.1),
        ("3. LOAD\nloader.py -> SQLite", AMBER, 5.8),
        ("4. ANALYZE\nsql/*.sql (16 queries)", BLUE, 8.5),
    ]
    y = 4.3
    w, h = 2.5, 1.4
    boxes = []
    for label, color, x in stages:
        boxes.append(box(ax, (x, y), w, h, label, color, fontsize=9.5))

    for i in range(len(boxes) - 1):
        x1, y1, w1, h1 = boxes[i]
        x2, y2, w2, h2 = boxes[i + 1]
        arrow(ax, (x1 + w1, y1 + h1 / 2), (x2, y2 + h2 / 2))

    # Data artifacts row
    box(ax, (0.4, 2.2), 2.5, 0.8, "data/raw/*.csv\n(with injected issues)", GREY, fontsize=8.5)
    box(ax, (3.1, 2.2), 2.5, 0.8, "data/cleaned/*.csv\n+ quality_report.csv", GREY, fontsize=8.5)
    box(ax, (5.8, 2.2), 2.5, 0.8, "data/ecommerce.db\n(SQLite)", GREY, fontsize=8.5)
    box(ax, (8.5, 2.2), 2.5, 0.8, "Query results\n(basic/intermediate/advanced)", GREY, fontsize=8.5)

    for x in [0.4, 3.1, 5.8, 8.5]:
        arrow(ax, (x + w / 2, 4.3), (x + 1.25, 3.0), color="#9aa3b0")

    # CLI + tests row
    box(ax, (8.5, 0.5), 2.5, 1.2, "5. REPORT\ncli.py / report_generator.py\n(menu-driven CLI)", NAVY, fontsize=9)
    arrow(ax, (9.75, 2.2), (9.75, 1.7), color="#9aa3b0")

    box(ax, (0.4, 0.5), 2.5, 1.2, "TESTING\ntests/test_edge_cases.py\n(7 assertion-based tests)", "#8a3b3b", fontsize=9)

    plt.tight_layout()
    plt.savefig(f"{OUT_DIR}/architecture.png", dpi=160, facecolor=fig.get_facecolor())
    plt.close(fig)


def make_er_diagram():
    fig, ax = plt.subplots(figsize=(13, 9.3))
    ax.set_xlim(0, 13)
    ax.set_ylim(0, 9.3)
    ax.axis("off")
    fig.patch.set_facecolor("white")

    ax.text(6.5, 9.0, "Entity-Relationship Diagram", ha="center", fontsize=15, weight="bold", color=NAVY)

    row_h = 0.34
    header_h = 0.5
    margin = 0.25

    def sized(fields):
        return header_h + margin + row_h * len(fields)

    fields_customers = ["PK  customer_id", "customer_name", "email", "registration_date", "customer_type"]
    fields_orders = ["PK  order_id", "FK  customer_id", "order_date", "status", "region_code"]
    fields_items = ["PK  item_id", "FK  order_id", "FK  product_id", "quantity", "unit_price", "discount_percent"]
    fields_products = ["PK  product_id", "product_name", "category", "subcategory", "cost_price"]

    entities = {
        "customers": ((0.5, 6.1), 2.9, sized(fields_customers), fields_customers, BLUE),
        "orders": ((5.0, 6.1), 2.9, sized(fields_orders), fields_orders, TEAL),
        "order_items": ((5.0, 1.5), 2.9, sized(fields_items), fields_items, AMBER),
        "products": ((9.6, 2.1), 2.9, sized(fields_products), fields_products, NAVY),
    }

    positions = {}
    for name, (xy, w, h, fields, color) in entities.items():
        x, y = xy
        fb = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.05",
                             linewidth=1.4, edgecolor=color, facecolor="white")
        ax.add_patch(fb)
        header = FancyBboxPatch((x, y + h - header_h), w, header_h,
                                 boxstyle="round,pad=0.0,rounding_size=0.05",
                                 linewidth=0, facecolor=color)
        ax.add_patch(header)
        ax.text(x + w / 2, y + h - header_h / 2, name, ha="center", va="center",
                fontsize=11, weight="bold", color="white")
        for i, field in enumerate(fields):
            fy = y + h - header_h - margin / 2 - row_h * i - row_h / 2
            weight = "bold" if field.startswith(("PK", "FK")) else "normal"
            ax.text(x + 0.18, fy, field, ha="left", va="center", fontsize=8.5, weight=weight, color="#222")
        positions[name] = (x, y, w, h)

    def connect_horizontal(a, b, label, reverse=False):
        """Right edge of a -> left edge of b (or reversed), both at the same relative height."""
        xa, ya, wa, ha_ = positions[a]
        xb, yb, wb, hb = positions[b]
        if not reverse:
            p1 = (xa + wa, ya + ha_ - header_h - 0.2)
            p2 = (xb, yb + hb - header_h - 0.2)
        else:
            p1 = (xa, ya + ha_ - header_h - 0.2)
            p2 = (xb + wb, yb + hb - header_h - 0.2)
        arrow(ax, p1, p2, color="#666")
        mx, my = (p1[0] + p2[0]) / 2, (p1[1] + p2[1]) / 2
        ax.text(mx, my + 0.18, label, ha="center", fontsize=8, style="italic", color="#444")

    def connect_vertical(a, b, label):
        """Bottom edge of a -> top edge of b."""
        xa, ya, wa, ha_ = positions[a]
        xb, yb, wb, hb = positions[b]
        p1 = (xa + wa / 2, ya)
        p2 = (xb + wb / 2, yb + hb)
        arrow(ax, p1, p2, color="#666")
        mx, my = (p1[0] + p2[0]) / 2, (p1[1] + p2[1]) / 2
        ax.text(mx + 0.25, my, label, ha="left", fontsize=8, style="italic", color="#444")

    connect_horizontal("customers", "orders", "1 : N")
    connect_vertical("orders", "order_items", "1 : N")
    connect_horizontal("products", "order_items", "N : 1", reverse=True)

    plt.tight_layout()
    plt.savefig(f"{OUT_DIR}/er_diagram.png", dpi=160, facecolor="white")
    plt.close(fig)


if __name__ == "__main__":
    make_architecture_diagram()
    make_er_diagram()
    print("Diagrams written to", OUT_DIR)
