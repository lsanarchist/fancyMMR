from __future__ import annotations

import textwrap

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
from matplotlib.patheffects import withStroke

from .aggregate import pct, usd_short
from .config import ACCENT, BUILD_PATHS, MUTED, PRIMARY, SUBTEXT, SVG_SAVE_METADATA, TEXT
from .schemas import SummaryArtifacts


def wrap_label(label: str, width: int = 26) -> str:
    return "\n".join(textwrap.wrap(str(label), width=width, break_long_words=False))


def clean_axes(ax) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#cbd5e1")
    ax.spines["bottom"].set_color("#cbd5e1")


def save_dual(fig, base) -> None:
    fig.savefig(base.with_suffix(".png"), dpi=220, bbox_inches="tight")
    # Keep SVG exports stable across identical rebuilds.
    with matplotlib.rc_context({"svg.hashsalt": base.stem}):
        fig.savefig(
            base.with_suffix(".svg"),
            bbox_inches="tight",
            metadata=SVG_SAVE_METADATA,
        )
    plt.close(fig)


def configure_matplotlib() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "savefig.facecolor": "white",
            "axes.edgecolor": "#cbd5e1",
            "axes.labelcolor": "#0f172a",
            "axes.titlesize": 13,
            "axes.titleweight": "bold",
            "axes.grid": True,
            "grid.color": "#e2e8f0",
            "grid.linewidth": 0.8,
            "grid.alpha": 1.0,
            "font.family": "DejaVu Sans",
            "font.size": 10.5,
            "xtick.color": "#0f172a",
            "ytick.color": "#0f172a",
        }
    )


def plot_category_share_map(summary: SummaryArtifacts) -> None:
    data = summary.category_summary.copy()
    label_full = ["E-commerce", "Content Creation", "SaaS", "AI", "Marketing", "Education"]
    label_zoom = [
        "Content Creation",
        "SaaS",
        "AI",
        "Marketing",
        "Education",
        "Entertainment",
        "Marketplace",
        "Analytics",
        "Security",
        "Health & Fitness",
    ]
    offsets_full = {
        "E-commerce": (0.006, 0.006),
        "Content Creation": (0.004, 0.006),
        "SaaS": (0.004, 0.004),
        "AI": (0.004, -0.007),
        "Marketing": (0.004, -0.006),
        "Education": (0.004, -0.006),
    }
    offsets_zoom = {
        "Content Creation": (0.004, 0.006),
        "SaaS": (0.003, 0.004),
        "AI": (0.003, -0.006),
        "Marketing": (0.003, -0.005),
        "Education": (0.003, -0.006),
        "Entertainment": (0.004, 0.005),
        "Marketplace": (0.004, 0.005),
        "Analytics": (0.003, 0.003),
        "Security": (0.003, -0.005),
        "Health & Fitness": (0.003, 0.003),
    }
    fig, axes = plt.subplots(1, 2, figsize=(14, 6.5))
    ax = axes[0]
    ax.scatter(data["startup_share"], data["revenue_share"], s=65, color=MUTED, edgecolor="white", linewidth=0.8, zorder=2)
    full_hi = data[data["category"].isin(label_full)]
    ax.scatter(
        full_hi["startup_share"],
        full_hi["revenue_share"],
        s=85,
        color=[ACCENT if category == "E-commerce" else PRIMARY for category in full_hi["category"]],
        edgecolor="white",
        linewidth=0.9,
        zorder=3,
    )
    ax.plot([0, 0.5], [0, 0.5], linestyle="--", linewidth=1.0, color="#64748b", zorder=1)
    for _, row in full_hi.iterrows():
        dx, dy = offsets_full.get(row["category"], (0.004, 0.004))
        text = ax.text(
            row["startup_share"] + dx,
            row["revenue_share"] + dy,
            row["category"],
            fontsize=10,
            color=TEXT,
            zorder=4,
        )
        text.set_path_effects([withStroke(linewidth=3, foreground="white")])
    ax.set_xlim(0, 0.5)
    ax.set_ylim(0, 0.5)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.set_xlabel("Share of startups")
    ax.set_ylabel("Share of visible revenue")
    ax.set_title("Full view", loc="left")
    clean_axes(ax)

    ax = axes[1]
    zoom_data = data[data["category"] != "E-commerce"].copy()
    ax.scatter(zoom_data["startup_share"], zoom_data["revenue_share"], s=60, color=MUTED, edgecolor="white", linewidth=0.8, zorder=2)
    zoom_hi = zoom_data[zoom_data["category"].isin(label_zoom)]
    ax.scatter(zoom_hi["startup_share"], zoom_hi["revenue_share"], s=80, color=PRIMARY, edgecolor="white", linewidth=0.9, zorder=3)
    ax.plot([0, 0.12], [0, 0.12], linestyle="--", linewidth=1.0, color="#64748b", zorder=1)
    for _, row in zoom_hi.iterrows():
        dx, dy = offsets_zoom.get(row["category"], (0.003, 0.003))
        text = ax.text(
            row["startup_share"] + dx,
            row["revenue_share"] + dy,
            row["category"],
            fontsize=9.2,
            color=TEXT,
            zorder=4,
        )
        text.set_path_effects([withStroke(linewidth=3, foreground="white")])
    ax.set_xlim(0, 0.12)
    ax.set_ylim(0, 0.15)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.set_xlabel("Share of startups")
    ax.set_title("Zoom view (excluding E-commerce)", loc="left")
    clean_axes(ax)

    fig.suptitle("Category share map: representation vs visible revenue", x=0.05, ha="left", fontsize=16, fontweight="bold")
    fig.text(
        0.05,
        0.01,
        "Notes: each point is a category. The dashed line marks equal share. The zoom panel fixes the original outlier-driven label collision.",
        fontsize=9,
        color=SUBTEXT,
    )
    fig.tight_layout(rect=[0, 0.04, 1, 0.94])
    save_dual(fig, BUILD_PATHS.charts_dir / "category_share_map")


def plot_top_categories(summary: SummaryArtifacts) -> None:
    data = summary.category_summary.head(10).sort_values("total_revenue")
    fig, ax = plt.subplots(figsize=(11, 6))
    colors = [ACCENT if category in {"E-commerce", "Content Creation"} else PRIMARY for category in data["category"]]
    ax.barh(data["category"], data["total_revenue"] / 1_000_000, color=colors, edgecolor="white")
    for index, (_, row) in enumerate(data.iterrows()):
        ax.text(
            row["total_revenue"] / 1_000_000 + 0.08,
            index,
            f"{usd_short(row['total_revenue'])} · {int(row['startup_count'])} startups",
            va="center",
            fontsize=9,
            color=TEXT,
        )
    ax.set_title("Top categories by visible 30-day revenue", loc="left")
    ax.set_xlabel("Visible revenue (USD, millions)")
    clean_axes(ax)
    save_dual(fig, BUILD_PATHS.charts_dir / "top_categories_revenue")


def plot_over_index(summary: SummaryArtifacts) -> None:
    data = summary.category_summary.sort_values("performance_index", ascending=False).head(12).sort_values("performance_index")
    fig, ax = plt.subplots(figsize=(10.5, 6.2))
    colors = [ACCENT if value > 1 else MUTED for value in data["performance_index"]]
    ax.barh(data["category"], data["performance_index"], color=colors, edgecolor="white")
    ax.axvline(1.0, color="#64748b", linestyle="--", linewidth=1.2)
    for index, (_, row) in enumerate(data.iterrows()):
        ax.text(row["performance_index"] + 0.05, index, f"{row['performance_index']:.2f}×", va="center", fontsize=9.3, color=TEXT)
    ax.set_title("Category over-index vs representation", loc="left")
    ax.set_xlabel("Revenue share / startup share")
    clean_axes(ax)
    save_dual(fig, BUILD_PATHS.charts_dir / "category_over_index")


def plot_model_mix(summary: SummaryArtifacts) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    for ax, data, name_col, title in [
        (axes[0], summary.biz_summary.sort_values("total_revenue"), "biz_model", "Business model mix"),
        (axes[1], summary.gtm_summary.sort_values("total_revenue"), "gtm_model", "Go-to-market mix"),
    ]:
        labels = [wrap_label(value, 28) for value in data[name_col]]
        ax.barh(labels, data["total_revenue"] / 1_000_000, color="#93c5fd", edgecolor="white")
        for index, (_, row) in enumerate(data.iterrows()):
            ax.text(
                row["total_revenue"] / 1_000_000 + 0.08,
                index,
                f"{pct(row['revenue_share'])} revenue · {int(row['startup_count'])} startups",
                va="center",
                fontsize=8.8,
                color=TEXT,
            )
        ax.set_title(title, loc="left")
        ax.set_xlabel("Visible revenue (USD, millions)")
        clean_axes(ax)
    fig.suptitle("Business-model and GTM composition", x=0.05, ha="left", fontsize=16, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    save_dual(fig, BUILD_PATHS.charts_dir / "model_mix")


def plot_distribution(summary: SummaryArtifacts) -> None:
    sorted_desc = np.sort(summary.visible_sample["revenue_30d"].to_numpy())[::-1]
    cum_rev = np.cumsum(sorted_desc) / sorted_desc.sum()
    cum_share = np.arange(1, len(sorted_desc) + 1) / len(sorted_desc)
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    ax = axes[0]
    labels = [value.replace("$", "") for value in summary.revenue_band_summary["revenue_band"].astype(str)]
    bars = ax.bar(labels, summary.revenue_band_summary["startup_count"], color="#93c5fd", edgecolor="white")
    for bar, (_, row) in zip(bars, summary.revenue_band_summary.iterrows()):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 3,
            f"{int(row['startup_count'])} startups\n{pct(row['revenue_share'])} revenue",
            ha="center",
            va="bottom",
            fontsize=8.8,
            color=TEXT,
        )
    ax.set_title("Revenue bands", loc="left")
    ax.set_ylabel("Startup count")
    clean_axes(ax)

    ax = axes[1]
    ax.plot(cum_share, cum_rev, color=PRIMARY, linewidth=2.6)
    ax.fill_between(cum_share, cum_rev, color="#93c5fd", alpha=0.45)
    top_n = 10
    top_n_share = top_n / summary.sample_size
    top_n_revenue_share = float(summary.visible_sample["revenue_30d"].head(top_n).sum() / summary.total_revenue)
    ax.scatter([top_n_share], [top_n_revenue_share], color=ACCENT, s=45, zorder=3)
    ax.text(
        top_n_share + 0.03,
        top_n_revenue_share - 0.05,
        f"Top 10 startups = {pct(top_n_revenue_share)} of visible revenue",
        fontsize=9.5,
        color=TEXT,
    )
    ax.set_title("Revenue concentration curve", loc="left")
    ax.set_xlabel("Share of startups (sorted by revenue, descending)")
    ax.set_ylabel("Cumulative share of revenue")
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.set_ylim(0, 1.05)
    clean_axes(ax)

    fig.suptitle("Distribution and concentration", x=0.05, ha="left", fontsize=16, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    save_dual(fig, BUILD_PATHS.charts_dir / "distribution_and_concentration")


def render_all_charts(summary: SummaryArtifacts) -> None:
    plot_category_share_map(summary)
    plot_top_categories(summary)
    plot_over_index(summary)
    plot_model_mix(summary)
    plot_distribution(summary)
