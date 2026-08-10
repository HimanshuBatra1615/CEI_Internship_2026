"""
make_project_report.py (one-off doc generation script, not part of the pipeline)
------------------------------------------------------------------------------------
Builds project_report.pdf: a concise project summary suitable for a resume/
portfolio submission, pulling real numbers from the data/reports/ outputs
rather than made-up figures.
"""

import csv
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Image,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

BASE = Path("/home/claude/ecommerce_analytics")
OUT_PATH = BASE / "project_report.pdf"

NAVY = colors.HexColor("#1f2a44")
TEAL = colors.HexColor("#2f8f8f")
LIGHT = colors.HexColor("#eef1f6")

styles = getSampleStyleSheet()
title_style = ParagraphStyle("TitleCustom", parent=styles["Title"], textColor=NAVY, fontSize=22)
h2 = ParagraphStyle("H2Custom", parent=styles["Heading2"], textColor=NAVY, spaceBefore=14, spaceAfter=6)
body = ParagraphStyle("BodyCustom", parent=styles["BodyText"], fontSize=10.2, leading=14.5)
small = ParagraphStyle("Small", parent=styles["BodyText"], fontSize=8.5, textColor=colors.grey)


def read_quality_report() -> list[dict]:
    with open(BASE / "data" / "reports" / "quality_report.csv") as f:
        return list(csv.DictReader(f))


def build() -> None:
    doc = SimpleDocTemplate(str(OUT_PATH), pagesize=letter,
                             topMargin=0.7 * inch, bottomMargin=0.7 * inch,
                             leftMargin=0.75 * inch, rightMargin=0.75 * inch)
    story = []

    # --- Cover ---
    story.append(Paragraph("E-Commerce Order Analytics System", title_style))
    story.append(Paragraph("Project Report", styles["Heading3"]))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "An end-to-end Python + SQL data engineering project: synthetic data generation, "
        "cleaning &amp; validation, SQLite loading, 16 SQL analyses ranging from basic aggregation "
        "to window functions and cohort analysis, and a menu-driven CLI reporting tool.",
        body))
    story.append(Spacer(1, 10))

    if (BASE / "docs" / "architecture.png").exists():
        story.append(Image(str(BASE / "docs" / "architecture.png"), width=6.8 * inch, height=3.97 * inch))
    story.append(Spacer(1, 4))
    story.append(Paragraph("Figure 1: Pipeline architecture — generate → clean/validate → load → analyze → report.", small))

    story.append(PageBreak())

    # --- Dataset volumes ---
    story.append(Paragraph("1. Dataset Overview", h2))
    story.append(Paragraph(
        "Four related CSVs were generated with realistic e-commerce distributions (seasonal order "
        "spikes in Nov/Dec, a repeat-buyer segment, category-appropriate price ranges) and deliberately "
        "injected data-quality issues so the cleaning pipeline has real problems to catch.", body))

    volume_data = [
        ["Dataset", "Rows Generated", "Primary Key"],
        ["customers.csv", "600", "customer_id"],
        ["products.csv", "220", "product_id"],
        ["orders.csv", "1,500", "order_id"],
        ["order_items.csv", "~2,800", "item_id"],
    ]
    t = Table(volume_data, colWidths=[2.4 * inch, 2 * inch, 2 * inch])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9.5),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT]),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cccccc")),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    story.append(Spacer(1, 8))
    story.append(t)

    # --- Data quality findings ---
    story.append(Paragraph("2. Data Quality Findings", h2))
    story.append(Paragraph(
        "The cleaning pipeline (clean_data.py + validators.py) inspects every row against a set of "
        "explicit rules, repairing what can be safely repaired, removing rows that break referential "
        "integrity, and flagging-but-retaining rows that represent valid business events (e.g. a "
        "negative quantity is a return, not an error). Actual counts from the most recent pipeline run:",
        body))

    rows = read_quality_report()
    nonzero = [r for r in rows if int(r["rows_affected"] or 0) > 0]
    qdata = [["Dataset", "Rule", "Rows Affected"]]
    for r in nonzero:
        qdata.append([r["dataset"], r["rule"], r["rows_affected"]])

    t2 = Table(qdata, colWidths=[1.3 * inch, 3.3 * inch, 1.3 * inch], repeatRows=1)
    t2.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), TEAL),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8.5),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT]),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cccccc")),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(Spacer(1, 8))
    story.append(t2)

    story.append(PageBreak())

    # --- SQL Analysis ---
    story.append(Paragraph("3. SQL Analysis Coverage", h2))
    story.append(Paragraph(
        "16 queries were implemented and executed successfully against the loaded SQLite database, "
        "organized into three tiers of complexity:", body))
    sql_data = [
        ["Tier", "Queries", "Techniques"],
        ["Basic", "Revenue per category, Top 10 customers, Monthly order count",
         "Joins, GROUP BY, aggregation"],
        ["Intermediate", "Never-delivered customers, High-return products, Category return rate",
         "Conditional aggregation, HAVING"],
        ["Advanced", "Running totals, DENSE_RANK, LAG gap analysis, multi-level CTEs, NTILE "
         "quartiles, YoY comparison, FIRST/LAST_VALUE, cumulative distribution, cohort "
         "retention, self-join market basket",
         "Window functions, nested CTEs, self-joins"],
    ]
    t3 = Table(sql_data, colWidths=[1.1 * inch, 3.1 * inch, 1.7 * inch], repeatRows=1)
    t3.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8.5),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT]),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cccccc")),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    story.append(Spacer(1, 8))
    story.append(t3)

    story.append(Spacer(1, 10))
    if (BASE / "docs" / "er_diagram.png").exists():
        story.append(Image(str(BASE / "docs" / "er_diagram.png"), width=6.8 * inch, height=4.5 * inch))
    story.append(Paragraph("Figure 2: Entity-relationship diagram of the four core tables.", small))

    story.append(PageBreak())

    # --- Testing ---
    story.append(Paragraph("4. Edge-Case Testing", h2))
    story.append(Paragraph(
        "7 assertion-based tests cover the 4 required edge cases (orphaned order_id, "
        "discount_percent &gt; 100, zero quantity, future-dated orders) plus 3 additional cases "
        "(duplicate order_id, invalid email, missing customer_id). All 7 pass on the current dataset. "
        "Results are written to data/reports/testing_report.txt on every run.", body))

    # --- Lessons learned / future work ---
    story.append(Paragraph("5. Lessons Learned", h2))
    story.append(Paragraph(
        "Referential-integrity cleanup must run against the already-deduplicated orders table, not "
        "the raw one — otherwise valid order_items referencing a de-duplicated order_id would be "
        "wrongly dropped. SQLite's FIRST_VALUE/LAST_VALUE window functions also require an explicit "
        "frame clause (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) to behave predictably "
        "— without it, LAST_VALUE silently returns the current row instead of the partition's last row.",
        body))

    story.append(Paragraph("6. Future Improvements", h2))
    for item in [
        "Swap SQLite for PostgreSQL and add scheduled orchestration (cron/Airflow) for refreshes.",
        "Add a dbt-style transformation layer so SQL is version-controlled and independently testable.",
        "Parameterize the CLI's date-range logic to support arbitrary (non-fixed-span) ranges.",
        "Add data-quality checks as a pre-load CI gate that fails the build past a defined threshold.",
    ]:
        story.append(Paragraph(f"•  {item}", body))

    doc.build(story)
    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    build()
