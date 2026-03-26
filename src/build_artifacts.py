from pathlib import Path
import json
import textwrap
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from matplotlib.patheffects import withStroke

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CHARTS_DIR = ROOT / "charts"

REQUIRED_COLS = ["name", "category", "revenue_30d", "biz_model", "gtm_model", "source_url"]
REVENUE_BINS = [5000, 10000, 50000, 100000, 500000, 1000000, float("inf")]
REVENUE_LABELS = ["$5k–$10k", "$10k–$50k", "$50k–$100k", "$100k–$500k", "$500k–$1M", "$1M+"]

PRIMARY = "#2563eb"
ACCENT = "#f59e0b"
MUTED = "#94a3b8"
TEXT = "#0f172a"
SUBTEXT = "#475569"

def usd_short(value: float) -> str:
    value = float(value)
    if abs(value) >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    if abs(value) >= 1_000:
        return f"${value/1_000:.1f}k"
    return f"${value:,.0f}"

def pct(value: float, digits: int = 1) -> str:
    return f"{value*100:.{digits}f}%"

def wrap_label(label: str, width: int = 26) -> str:
    return "\n".join(textwrap.wrap(str(label), width=width, break_long_words=False))

def clean_axes(ax):
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#cbd5e1")
    ax.spines["bottom"].set_color("#cbd5e1")

def save_dual(fig, base: Path):
    fig.savefig(base.with_suffix(".png"), dpi=220, bbox_inches="tight")
    fig.savefig(base.with_suffix(".svg"), bbox_inches="tight")
    plt.close(fig)

def configure_matplotlib():
    plt.rcParams.update({
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
    })

def load_data():
    df = pd.read_csv(DATA_DIR / "visible_sample.csv")
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    if df["revenue_30d"].isna().any():
        raise ValueError("Null revenue values found")
    if (df["revenue_30d"] < 5000).any():
        raise ValueError("Found revenue values below the inclusion threshold")
    df["revenue_band"] = pd.cut(
        df["revenue_30d"],
        bins=REVENUE_BINS,
        labels=REVENUE_LABELS,
        right=False,
        include_lowest=True,
    )
    return df.sort_values(["revenue_30d", "name"], ascending=[False, True]).reset_index(drop=True)

def summarize(df):
    total_revenue = int(df["revenue_30d"].sum())
    sample_size = int(len(df))
    category_summary = (
        df.groupby("category", as_index=False)
        .agg(
            startup_count=("name", "count"),
            total_revenue=("revenue_30d", "sum"),
            median_revenue=("revenue_30d", "median"),
        )
    )
    category_summary["startup_share"] = category_summary["startup_count"] / sample_size
    category_summary["revenue_share"] = category_summary["total_revenue"] / total_revenue
    category_summary["performance_index"] = category_summary["revenue_share"] / category_summary["startup_share"]
    category_summary = category_summary.sort_values("total_revenue", ascending=False).reset_index(drop=True)

    def summarize_model(column: str) -> pd.DataFrame:
        out = (
            df.groupby(column, as_index=False)
            .agg(
                startup_count=("name", "count"),
                total_revenue=("revenue_30d", "sum"),
                median_revenue=("revenue_30d", "median"),
            )
            .sort_values("total_revenue", ascending=False)
            .reset_index(drop=True)
        )
        out["startup_share"] = out["startup_count"] / sample_size
        out["revenue_share"] = out["total_revenue"] / total_revenue
        return out

    biz_summary = summarize_model("biz_model")
    gtm_summary = summarize_model("gtm_model")
    revenue_band_summary = (
        df.groupby("revenue_band", observed=True, as_index=False)
        .agg(startup_count=("name", "count"), total_revenue=("revenue_30d", "sum"))
    )
    revenue_band_summary["startup_share"] = revenue_band_summary["startup_count"] / sample_size
    revenue_band_summary["revenue_share"] = revenue_band_summary["total_revenue"] / total_revenue
    return total_revenue, sample_size, category_summary, biz_summary, gtm_summary, revenue_band_summary

def write_summaries(df, total_revenue, sample_size, category_summary, biz_summary, gtm_summary, revenue_band_summary):
    category_summary.to_csv(DATA_DIR / "category_summary.csv", index=False)
    biz_summary.to_csv(DATA_DIR / "business_model_summary.csv", index=False)
    gtm_summary.to_csv(DATA_DIR / "gtm_model_summary.csv", index=False)
    revenue_band_summary.to_csv(DATA_DIR / "revenue_band_summary.csv", index=False)
    pd.DataFrame({"source_url": sorted(df["source_url"].unique())}).to_csv(DATA_DIR / "public_source_pages.csv", index=False)
    metrics = {
        "sample_size": sample_size,
        "total_visible_revenue_usd": total_revenue,
        "median_revenue_usd": float(df["revenue_30d"].median()),
        "p75_revenue_usd": float(df["revenue_30d"].quantile(0.75)),
        "p90_revenue_usd": float(df["revenue_30d"].quantile(0.90)),
        "top_1_revenue_share": float(df["revenue_30d"].head(1).sum() / total_revenue),
        "top_5_revenue_share": float(df["revenue_30d"].head(5).sum() / total_revenue),
        "top_10_revenue_share": float(df["revenue_30d"].head(10).sum() / total_revenue),
        "top_20_revenue_share": float(df["revenue_30d"].head(20).sum() / total_revenue),
        "dominant_category": category_summary.iloc[0]["category"],
        "dominant_category_revenue_share": float(category_summary.iloc[0]["revenue_share"]),
        "dominant_category_startup_share": float(category_summary.iloc[0]["startup_share"]),
    }
    with open(DATA_DIR / "metrics.json", "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    return metrics

def plot_category_share_map(category_summary):
    data = category_summary.copy()
    label_full = ["E-commerce", "Content Creation", "SaaS", "AI", "Marketing", "Education"]
    label_zoom = ["Content Creation", "SaaS", "AI", "Marketing", "Education", "Entertainment", "Marketplace", "Analytics", "Security", "Health & Fitness"]
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
    ax.scatter(full_hi["startup_share"], full_hi["revenue_share"], s=85, color=[ACCENT if c == "E-commerce" else PRIMARY for c in full_hi["category"]], edgecolor="white", linewidth=0.9, zorder=3)
    ax.plot([0, 0.5], [0, 0.5], linestyle="--", linewidth=1.0, color="#64748b", zorder=1)
    for _, row in full_hi.iterrows():
        dx, dy = offsets_full.get(row["category"], (0.004, 0.004))
        t = ax.text(row["startup_share"] + dx, row["revenue_share"] + dy, row["category"], fontsize=10, color=TEXT, zorder=4)
        t.set_path_effects([withStroke(linewidth=3, foreground="white")])
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
        t = ax.text(row["startup_share"] + dx, row["revenue_share"] + dy, row["category"], fontsize=9.2, color=TEXT, zorder=4)
        t.set_path_effects([withStroke(linewidth=3, foreground="white")])
    ax.set_xlim(0, 0.12)
    ax.set_ylim(0, 0.15)
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.set_xlabel("Share of startups")
    ax.set_title("Zoom view (excluding E-commerce)", loc="left")
    clean_axes(ax)

    fig.suptitle("Category share map: representation vs visible revenue", x=0.05, ha="left", fontsize=16, fontweight="bold")
    fig.text(0.05, 0.01, "Notes: each point is a category. The dashed line marks equal share. The zoom panel fixes the original outlier-driven label collision.", fontsize=9, color=SUBTEXT)
    fig.tight_layout(rect=[0, 0.04, 1, 0.94])
    save_dual(fig, CHARTS_DIR / "category_share_map")

def plot_top_categories(category_summary):
    data = category_summary.head(10).sort_values("total_revenue")
    fig, ax = plt.subplots(figsize=(11, 6))
    colors = [ACCENT if c in {"E-commerce", "Content Creation"} else PRIMARY for c in data["category"]]
    ax.barh(data["category"], data["total_revenue"] / 1_000_000, color=colors, edgecolor="white")
    for i, (_, row) in enumerate(data.iterrows()):
        ax.text(row["total_revenue"] / 1_000_000 + 0.08, i, f"{usd_short(row['total_revenue'])} · {int(row['startup_count'])} startups", va="center", fontsize=9, color=TEXT)
    ax.set_title("Top categories by visible 30-day revenue", loc="left")
    ax.set_xlabel("Visible revenue (USD, millions)")
    clean_axes(ax)
    save_dual(fig, CHARTS_DIR / "top_categories_revenue")

def plot_over_index(category_summary):
    data = category_summary.sort_values("performance_index", ascending=False).head(12).sort_values("performance_index")
    fig, ax = plt.subplots(figsize=(10.5, 6.2))
    colors = [ACCENT if x > 1 else MUTED for x in data["performance_index"]]
    ax.barh(data["category"], data["performance_index"], color=colors, edgecolor="white")
    ax.axvline(1.0, color="#64748b", linestyle="--", linewidth=1.2)
    for i, (_, row) in enumerate(data.iterrows()):
        ax.text(row["performance_index"] + 0.05, i, f"{row['performance_index']:.2f}×", va="center", fontsize=9.3, color=TEXT)
    ax.set_title("Category over-index vs representation", loc="left")
    ax.set_xlabel("Revenue share / startup share")
    clean_axes(ax)
    save_dual(fig, CHARTS_DIR / "category_over_index")

def plot_model_mix(biz_summary, gtm_summary):
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    for ax, data, name_col, title in [
        (axes[0], biz_summary.sort_values("total_revenue"), "biz_model", "Business model mix"),
        (axes[1], gtm_summary.sort_values("total_revenue"), "gtm_model", "Go-to-market mix"),
    ]:
        labels = [wrap_label(x, 28) for x in data[name_col]]
        ax.barh(labels, data["total_revenue"] / 1_000_000, color="#93c5fd", edgecolor="white")
        for i, (_, row) in enumerate(data.iterrows()):
            ax.text(row["total_revenue"] / 1_000_000 + 0.08, i, f"{pct(row['revenue_share'])} revenue · {int(row['startup_count'])} startups", va="center", fontsize=8.8, color=TEXT)
        ax.set_title(title, loc="left")
        ax.set_xlabel("Visible revenue (USD, millions)")
        clean_axes(ax)
    fig.suptitle("Business-model and GTM composition", x=0.05, ha="left", fontsize=16, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    save_dual(fig, CHARTS_DIR / "model_mix")

def plot_distribution(df, revenue_band_summary, total_revenue, sample_size):
    sorted_desc = np.sort(df["revenue_30d"].to_numpy())[::-1]
    cum_rev = np.cumsum(sorted_desc) / sorted_desc.sum()
    cum_share = np.arange(1, len(sorted_desc) + 1) / len(sorted_desc)
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    ax = axes[0]
    labels = [x.replace("$", "") for x in revenue_band_summary["revenue_band"].astype(str)]
    bars = ax.bar(labels, revenue_band_summary["startup_count"], color="#93c5fd", edgecolor="white")
    for bar, (_, row) in zip(bars, revenue_band_summary.iterrows()):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 3, f"{int(row['startup_count'])} startups\n{pct(row['revenue_share'])} revenue", ha="center", va="bottom", fontsize=8.8, color=TEXT)
    ax.set_title("Revenue bands", loc="left")
    ax.set_ylabel("Startup count")
    clean_axes(ax)

    ax = axes[1]
    ax.plot(cum_share, cum_rev, color=PRIMARY, linewidth=2.6)
    ax.fill_between(cum_share, cum_rev, color="#93c5fd", alpha=0.45)
    n = 10
    x = n / sample_size
    y = float(df["revenue_30d"].head(n).sum() / total_revenue)
    ax.scatter([x], [y], color=ACCENT, s=45, zorder=3)
    ax.text(x + 0.03, y - 0.05, f"Top 10 startups = {pct(y)} of visible revenue", fontsize=9.5, color=TEXT)
    ax.set_title("Revenue concentration curve", loc="left")
    ax.set_xlabel("Share of startups (sorted by revenue, descending)")
    ax.set_ylabel("Cumulative share of revenue")
    ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.set_ylim(0, 1.05)
    clean_axes(ax)

    fig.suptitle("Distribution and concentration", x=0.05, ha="left", fontsize=16, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    save_dual(fig, CHARTS_DIR / "distribution_and_concentration")

def write_readme(metrics):
    readme = f"""# TrustMRR visible-sample research

Independent, GitHub-ready packaging of a **visible public sample** of startups with `Revenue (30d) >= $5,000`.

## What is in scope

- Startup-level visible sample
- Category, business-model, GTM, and revenue-band summaries
- Publication-grade charts in both PNG and SVG
- Reproducible build script
- Methodology, data notes, and release checklist

## Key takeaways

- Visible sample size: **{metrics['sample_size']} startups**
- Total visible 30-day revenue: **{usd_short(metrics['total_visible_revenue_usd'])}**
- Median visible 30-day revenue: **{usd_short(metrics['median_revenue_usd'])}**
- Top 10 startups capture **{pct(metrics['top_10_revenue_share'])}** of visible revenue
- The largest category is **{metrics['dominant_category']}**, accounting for **{pct(metrics['dominant_category_revenue_share'])}** of visible revenue with **{pct(metrics['dominant_category_startup_share'])}** of visible startups

## Main charts

### Category share map
![Category share map](charts/category_share_map.png)

### Top categories by visible revenue
![Top categories by revenue](charts/top_categories_revenue.png)

### Category over-index vs representation
![Category over-index](charts/category_over_index.png)

### Business-model and GTM composition
![Business-model and GTM composition](charts/model_mix.png)

### Distribution and concentration
![Distribution and concentration](charts/distribution_and_concentration.png)

## Repository layout

```text
.
├── charts/
├── data/
├── docs/
│   └── methodology.md
├── src/
│   └── build_artifacts.py
├── CHANGELOG.md
├── DATA-NOTICE.md
├── LICENSE-CODE-MIT.txt
├── RELEASE_CHECKLIST.md
└── requirements.txt
```

## Method note

This repository is based on a **source-derived visible sample**, not a full platform export. The `biz_model` and `gtm_model` fields are heuristic labels. See [docs/methodology.md](docs/methodology.md) for details.

## Rebuild

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/build_artifacts.py
```

## Publishing note

I did **not** hard-code a final repository license into this bundle because the repo mixes original code/docs with source-derived data. Use:

- `LICENSE-CODE-MIT.txt` as a starting point for code/docs if that is your final choice
- `DATA-NOTICE.md` to preserve the scope and attribution caveat
- `RELEASE_CHECKLIST.md` before you publish
"""
    (ROOT / "README.md").write_text(readme, encoding="utf-8")

def main():
    configure_matplotlib()
    df = load_data()
    total_revenue, sample_size, category_summary, biz_summary, gtm_summary, revenue_band_summary = summarize(df)
    metrics = write_summaries(df, total_revenue, sample_size, category_summary, biz_summary, gtm_summary, revenue_band_summary)
    plot_category_share_map(category_summary)
    plot_top_categories(category_summary)
    plot_over_index(category_summary)
    plot_model_mix(biz_summary, gtm_summary)
    plot_distribution(df, revenue_band_summary, total_revenue, sample_size)
    write_readme(metrics)

if __name__ == "__main__":
    main()
