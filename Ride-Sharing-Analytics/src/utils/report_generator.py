"""
================================================================================
RideSharing Analytics Platform — Report & Visualization Generator
================================================================================
Module      : src/utils/report_generator.py
Description : Generates publication-quality Matplotlib/Seaborn charts and
              Markdown reports from Gold-layer Parquet tables.
              All charts are saved to reports/ and embedded in the final report.

Author      : RideSharing Platform Engineering
================================================================================
"""

from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")  # Non-interactive backend for server-side rendering
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("utils.report_generator")

# Chart aesthetics — consistent across all outputs
PALETTE      = "viridis"
ACCENT_COLOR = "#4C6FD4"
BG_COLOR     = "#0F1117"
GRID_COLOR   = "#2A2D3A"
TEXT_COLOR   = "#E8EAF6"
FIG_DPI      = 150
FONT_FAMILY  = "DejaVu Sans"

plt.rcParams.update({
    "figure.facecolor":  BG_COLOR,
    "axes.facecolor":    BG_COLOR,
    "axes.edgecolor":    GRID_COLOR,
    "axes.labelcolor":   TEXT_COLOR,
    "axes.titlecolor":   TEXT_COLOR,
    "xtick.color":       TEXT_COLOR,
    "ytick.color":       TEXT_COLOR,
    "text.color":        TEXT_COLOR,
    "grid.color":        GRID_COLOR,
    "grid.alpha":        0.4,
    "font.family":       FONT_FAMILY,
    "figure.dpi":        FIG_DPI,
})

REPORTS_DIR = Path("reports")
CHARTS_DIR  = REPORTS_DIR / "charts"


def _ensure_dirs() -> None:
    REPORTS_DIR.mkdir(exist_ok=True)
    CHARTS_DIR.mkdir(exist_ok=True)


def _save_fig(fig: plt.Figure, name: str) -> str:
    """Save figure and return relative path."""
    _ensure_dirs()
    path = CHARTS_DIR / f"{name}.png"
    fig.savefig(path, dpi=FIG_DPI, bbox_inches="tight", facecolor=BG_COLOR)
    plt.close(fig)
    logger.info("Chart saved: %s", path)
    return str(path)


# ---------------------------------------------------------------------------
# Chart 1: Daily Revenue Trend
# ---------------------------------------------------------------------------

def plot_revenue_trend(revenue_df: pd.DataFrame) -> str:
    """Line chart of daily revenue with 3-day rolling average overlay."""
    fig, ax = plt.subplots(figsize=(12, 5))

    ax.fill_between(revenue_df["trip_date"].astype(str),
                    revenue_df["daily_revenue"], alpha=0.25, color=ACCENT_COLOR)
    ax.plot(revenue_df["trip_date"].astype(str),
            revenue_df["daily_revenue"],
            color=ACCENT_COLOR, linewidth=2.5, marker="o", markersize=6, label="Daily Revenue")

    if "rolling_3d_revenue" in revenue_df.columns:
        ax.plot(revenue_df["trip_date"].astype(str),
                revenue_df["rolling_3d_revenue"],
                color="#FF6B6B", linewidth=2, linestyle="--", label="3-Day Rolling Avg")

    ax.set_title("📈 Daily Revenue Trend", fontsize=16, fontweight="bold", pad=15)
    ax.set_xlabel("Date")
    ax.set_ylabel("Revenue (₹)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"₹{x:,.0f}"))
    ax.legend(facecolor=BG_COLOR, edgecolor=GRID_COLOR)
    ax.grid(True, axis="y")
    plt.xticks(rotation=30, ha="right")
    fig.tight_layout()
    return _save_fig(fig, "01_revenue_trend")


# ---------------------------------------------------------------------------
# Chart 2: City Revenue Leaderboard (Horizontal Bar)
# ---------------------------------------------------------------------------

def plot_city_revenue(city_df: pd.DataFrame) -> str:
    """Horizontal bar chart showing revenue by city."""
    city_sorted = city_df.sort_values("total_revenue", ascending=True)
    fig, ax = plt.subplots(figsize=(10, 5))

    colors = plt.cm.viridis([i / len(city_sorted) for i in range(len(city_sorted))])
    bars = ax.barh(city_sorted["city"], city_sorted["total_revenue"],
                   color=colors, edgecolor="none", height=0.6)

    for bar, val in zip(bars, city_sorted["total_revenue"]):
        ax.text(bar.get_width() + 50, bar.get_y() + bar.get_height() / 2,
                f"₹{val:,.0f}", va="center", fontsize=9, color=TEXT_COLOR)

    ax.set_title("🏙️ City Revenue Leaderboard", fontsize=16, fontweight="bold", pad=15)
    ax.set_xlabel("Total Revenue (₹)")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"₹{x/1000:.0f}K"))
    ax.grid(True, axis="x", alpha=0.3)
    fig.tight_layout()
    return _save_fig(fig, "02_city_revenue")


# ---------------------------------------------------------------------------
# Chart 3: Completion vs Cancellation Rate by City
# ---------------------------------------------------------------------------

def plot_completion_rates(city_df: pd.DataFrame) -> str:
    """Grouped bar chart: completion % vs cancellation % per city."""
    fig, ax = plt.subplots(figsize=(10, 5))
    x = range(len(city_df))
    w = 0.35

    ax.bar([i - w/2 for i in x],
           (city_df["completion_rate"] * 100).round(1),
           w, label="Completion %", color="#4ECDC4", edgecolor="none")
    ax.bar([i + w/2 for i in x],
           (city_df["cancellation_rate"] * 100).round(1),
           w, label="Cancellation %", color="#FF6B6B", edgecolor="none")

    ax.set_xticks(list(x))
    ax.set_xticklabels(city_df["city"], rotation=15, ha="right")
    ax.set_ylabel("Rate (%)")
    ax.set_title("✅ Completion vs ❌ Cancellation Rate by City", fontsize=15, fontweight="bold", pad=15)
    ax.legend(facecolor=BG_COLOR, edgecolor=GRID_COLOR)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    return _save_fig(fig, "03_completion_cancellation_city")


# ---------------------------------------------------------------------------
# Chart 4: Top 10 Drivers by Revenue
# ---------------------------------------------------------------------------

def plot_top_drivers(perf_df: pd.DataFrame) -> str:
    """Bar chart of top 10 drivers by total revenue."""
    top10 = perf_df.nlargest(10, "total_revenue").sort_values("total_revenue", ascending=True)
    fig, ax = plt.subplots(figsize=(11, 6))

    norm = plt.Normalize(top10["total_revenue"].min(), top10["total_revenue"].max())
    colors = plt.cm.plasma(norm(top10["total_revenue"]))

    bars = ax.barh(top10["name"], top10["total_revenue"], color=colors, edgecolor="none", height=0.65)
    for bar, val in zip(bars, top10["total_revenue"]):
        ax.text(bar.get_width() + 20, bar.get_y() + bar.get_height() / 2,
                f"₹{val:,.0f}", va="center", fontsize=9, color=TEXT_COLOR)

    ax.set_title("🏆 Top 10 Drivers by Total Revenue", fontsize=16, fontweight="bold", pad=15)
    ax.set_xlabel("Total Revenue (₹)")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"₹{x/1000:.1f}K"))
    ax.grid(True, axis="x", alpha=0.3)
    fig.tight_layout()
    return _save_fig(fig, "04_top_drivers_revenue")


# ---------------------------------------------------------------------------
# Chart 5: Driver Rating Distribution
# ---------------------------------------------------------------------------

def plot_rating_distribution(drivers_df: pd.DataFrame) -> str:
    """KDE + Histogram of driver ratings."""
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.hist(drivers_df["rating"], bins=20, color=ACCENT_COLOR, alpha=0.7,
            edgecolor="none", label="Driver Count")
    ax2 = ax.twinx()
    drivers_df["rating"].plot.kde(ax=ax2, color="#FF6B6B", linewidth=2.5, label="KDE")
    ax.set_title("⭐ Driver Rating Distribution", fontsize=16, fontweight="bold", pad=15)
    ax.set_xlabel("Rating")
    ax.set_ylabel("Driver Count")
    ax2.set_ylabel("Density")
    ax2.set_facecolor(BG_COLOR)
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend(loc="upper left", facecolor=BG_COLOR)
    ax2.legend(loc="upper right", facecolor=BG_COLOR)
    fig.tight_layout()
    return _save_fig(fig, "05_rating_distribution")


# ---------------------------------------------------------------------------
# Chart 6: Peak Hour Demand Heatmap
# ---------------------------------------------------------------------------

def plot_peak_hour(peak_df: pd.DataFrame) -> str:
    """Bar chart of trips and revenue by hour of day."""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

    ax1.bar(peak_df["trip_hour"], peak_df["total_trips"],
            color=ACCENT_COLOR, alpha=0.8, edgecolor="none")
    ax1.set_title("⏰ Trip Demand by Hour of Day", fontsize=15, fontweight="bold", pad=12)
    ax1.set_ylabel("Total Trips")
    ax1.grid(True, axis="y", alpha=0.3)

    ax2.bar(peak_df["trip_hour"], peak_df["revenue"],
            color="#4ECDC4", alpha=0.8, edgecolor="none")
    ax2.set_title("💰 Revenue by Hour of Day", fontsize=15, fontweight="bold", pad=12)
    ax2.set_ylabel("Revenue (₹)")
    ax2.set_xlabel("Hour of Day (0-23)")
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"₹{x:,.0f}"))
    ax2.grid(True, axis="y", alpha=0.3)

    plt.xticks(range(0, 24))
    fig.tight_layout()
    return _save_fig(fig, "06_peak_hour_demand")


# ---------------------------------------------------------------------------
# Chart 7: Trip Distance Distribution
# ---------------------------------------------------------------------------

def plot_distance_distribution(trips_df: pd.DataFrame) -> str:
    """Box plot of distance by trip status."""
    fig, ax = plt.subplots(figsize=(10, 5))
    completed = trips_df[trips_df["trip_status"] == "Completed"]["distance_km"]
    cancelled = trips_df[trips_df["trip_status"] == "Cancelled"]["distance_km"]

    bp = ax.boxplot([completed, cancelled],
                    labels=["Completed", "Cancelled"],
                    patch_artist=True,
                    medianprops={"color": "white", "linewidth": 2})
    colors_box = ["#4ECDC4", "#FF6B6B"]
    for patch, color in zip(bp["boxes"], colors_box):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)

    ax.set_title("📏 Trip Distance Distribution by Status", fontsize=15, fontweight="bold", pad=15)
    ax.set_ylabel("Distance (km)")
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    return _save_fig(fig, "07_distance_distribution")


# ---------------------------------------------------------------------------
# Chart 8: Driver Segmentation Pie
# ---------------------------------------------------------------------------

def plot_driver_segmentation(seg_df: pd.DataFrame) -> str:
    """Donut chart of driver segments."""
    seg_counts = seg_df["segment"].value_counts()
    fig, ax = plt.subplots(figsize=(8, 8))
    colors_pie = ["#4ECDC4", "#4C6FD4", "#FFD93D", "#FF6B6B"]
    wedges, texts, autotexts = ax.pie(
        seg_counts,
        labels=seg_counts.index,
        autopct="%1.1f%%",
        colors=colors_pie[:len(seg_counts)],
        startangle=90,
        pctdistance=0.82,
        wedgeprops={"edgecolor": BG_COLOR, "linewidth": 2},
    )
    for t in texts + autotexts:
        t.set_color(TEXT_COLOR)
        t.set_fontsize(12)

    # Donut hole
    centre_circle = plt.Circle((0, 0), 0.65, fc=BG_COLOR)
    ax.add_patch(centre_circle)
    ax.text(0, 0, f"{seg_counts.sum()}\nDrivers",
            ha="center", va="center", fontsize=14, color=TEXT_COLOR, fontweight="bold")

    ax.set_title("🎯 Driver Performance Segmentation", fontsize=16, fontweight="bold", pad=20)
    fig.tight_layout()
    return _save_fig(fig, "08_driver_segmentation")


# ---------------------------------------------------------------------------
# Chart 9: Delay Distribution
# ---------------------------------------------------------------------------

def plot_delay_distribution(logs_df: pd.DataFrame) -> str:
    """Histogram of delay minutes for completed trips."""
    completed_logs = logs_df[logs_df["cancellation_flag"] == 0]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.hist(completed_logs["delay_minutes"].dropna(), bins=20,
            color="#FFD93D", alpha=0.8, edgecolor="none")
    avg_delay = completed_logs["delay_minutes"].mean()
    ax.axvline(avg_delay, color="#FF6B6B", linewidth=2.5, linestyle="--",
               label=f"Avg: {avg_delay:.1f} min")
    ax.set_title("⏱️ Delay Distribution (Completed Trips)", fontsize=15, fontweight="bold", pad=15)
    ax.set_xlabel("Delay (minutes)")
    ax.set_ylabel("Trip Count")
    ax.legend(facecolor=BG_COLOR)
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    return _save_fig(fig, "09_delay_distribution")


# ---------------------------------------------------------------------------
# Chart 10: Revenue per KM by City
# ---------------------------------------------------------------------------

def plot_revenue_per_km(perf_df: pd.DataFrame) -> str:
    """Violin plot of revenue_per_km by city."""
    fig, ax = plt.subplots(figsize=(11, 6))
    cities   = sorted(perf_df["city"].dropna().unique())
    data_grp = [perf_df[perf_df["city"] == c]["revenue_per_km"].dropna().tolist() for c in cities]
    data_grp = [d if d else [0.0] for d in data_grp]

    vp = ax.violinplot(data_grp, showmeans=True, showmedians=True)
    for i, body in enumerate(vp["bodies"]):
        body.set_facecolor(plt.cm.viridis(i / len(cities)))
        body.set_alpha(0.7)
    vp["cmeans"].set_color("#FFD93D")
    vp["cmedians"].set_color("white")

    ax.set_xticks(range(1, len(cities) + 1))
    ax.set_xticklabels(cities, rotation=15, ha="right")
    ax.set_title("💡 Revenue per KM Distribution by City", fontsize=15, fontweight="bold", pad=15)
    ax.set_ylabel("Revenue per KM (₹/km)")
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    return _save_fig(fig, "10_revenue_per_km_city")


# ---------------------------------------------------------------------------
# Markdown Report Generator
# ---------------------------------------------------------------------------

def generate_markdown_report(
    kpi_rows: list,
    chart_paths: dict,
    gold_summaries: dict,
) -> str:
    """Build the final Markdown business report."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# 📊 RideSharing Analytics Platform — Business Report",
        f"> **Generated:** {ts}  |  **Pipeline Version:** 1.0.0  |  **Layer:** Gold",
        "",
        "---",
        "",
        "## 🎯 Executive KPI Summary",
        "",
        "| KPI | Value | Category |",
        "|-----|-------|----------|",
    ]
    for row in kpi_rows:
        lines.append(f"| {row[0]} | **{row[1]}** | {row[3]} |")

    lines += [
        "",
        "---",
        "",
        "## 📈 Revenue Analytics",
        "",
        f"![Daily Revenue Trend]({chart_paths.get('revenue_trend', '')})",
        "",
        f"![City Revenue Leaderboard]({chart_paths.get('city_revenue', '')})",
        "",
        "---",
        "",
        "## 🚗 Driver Performance",
        "",
        f"![Top 10 Drivers]({chart_paths.get('top_drivers', '')})",
        "",
        f"![Driver Segmentation]({chart_paths.get('driver_segmentation', '')})",
        "",
        f"![Rating Distribution]({chart_paths.get('rating_dist', '')})",
        "",
        "---",
        "",
        "## ⏰ Demand & Operations",
        "",
        f"![Peak Hour Demand]({chart_paths.get('peak_hour', '')})",
        "",
        f"![Delay Distribution]({chart_paths.get('delay_dist', '')})",
        "",
        f"![Distance Distribution]({chart_paths.get('distance_dist', '')})",
        "",
        "---",
        "",
        "## 🏙️ City Analytics",
        "",
        f"![Completion vs Cancellation]({chart_paths.get('completion_rates', '')})",
        "",
        f"![Revenue per KM]({chart_paths.get('revenue_per_km', '')})",
        "",
        "---",
        "",
        "## 💡 Business Recommendations",
        "",
        "1. **Cancellation Reduction**: ~59% cancellation rate is above industry benchmark of 20-30%.",
        "   Implement driver commitment scoring and pre-trip driver confirmation nudges.",
        "2. **Peak Hour Surge**: Align driver availability with peak hours identified in demand analysis.",
        "   Offer per-hour incentives during Morning Rush (07-10) and Evening Rush (17-20).",
        "3. **At-Risk Driver Intervention**: Drivers in 'At-Risk' segment need coaching.",
        "   Focus on completion rate improvement — 1% improvement translates to direct revenue gain.",
        "4. **Same-Location Trip Audit**: Investigate same-location pickup/drop trips for GPS accuracy.",
        "5. **City Expansion**: High-performing cities should receive additional driver recruitment budget.",
        "",
        "---",
        "",
        "*Report generated by RideSharing Analytics Platform v1.0.0*",
    ]

    report_path = REPORTS_DIR / "business_report.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Business report written: %s", report_path)
    return str(report_path)


def generate_all_reports(spark) -> str:
    """Orchestrates loading Gold tables, creating visualizations, and rendering the Markdown report."""
    logger.info("Starting report and visualization generation...")
    _ensure_dirs()

    # Read Gold tables into pandas DataFrames
    rev_df   = spark.read.parquet("data/gold/revenue_analytics").toPandas()
    city_df  = spark.read.parquet("data/gold/city_analytics").toPandas()
    perf_df  = spark.read.parquet("data/gold/driver_performance").toPandas()
    dr_df    = spark.read.parquet("data/silver/drivers").toPandas()
    tr_df    = spark.read.parquet("data/silver/trips").toPandas()
    peak_df  = spark.read.parquet("data/gold/peak_hour_dataset").toPandas()
    seg_df   = spark.read.parquet("data/gold/driver_segmentation").toPandas()
    logs_df  = spark.read.parquet("data/silver/trip_logs").toPandas()
    kpi_df   = spark.read.parquet("data/gold/executive_kpis").toPandas()

    chart_paths = {
        "revenue_trend":    plot_revenue_trend(rev_df),
        "city_revenue":     plot_city_revenue(city_df),
        "completion_rates": plot_completion_rates(city_df),
        "top_drivers":      plot_top_drivers(perf_df),
        "rating_dist":      plot_rating_distribution(dr_df),
        "peak_hour":        plot_peak_hour(peak_df),
        "distance_dist":    plot_distance_distribution(tr_df),
        "driver_segmentation": plot_driver_segmentation(seg_df),
        "delay_dist":       plot_delay_distribution(logs_df),
        "revenue_per_km":   plot_revenue_per_km(perf_df),
    }

    kpi_rows = kpi_df.values.tolist()
    report_file = generate_markdown_report(kpi_rows, chart_paths, {})
    logger.info("All reports and visualizations successfully generated!")
    return report_file

