from __future__ import annotations

from dataclasses import asdict
import json

import numpy as np
import pandas as pd

from .config import BUILD_PATHS, REVENUE_BINS, REVENUE_LABELS
from .publication import read_publication_input
from .schemas import MetricsSnapshot, SummaryArtifacts
from .validation import ensure_validation_passes, validate_visible_sample


def usd_short(value: float) -> str:
    value = float(value)
    if abs(value) >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    if abs(value) >= 1_000:
        return f"${value/1_000:.1f}k"
    return f"${value:,.0f}"


def pct(value: float, digits: int = 1) -> str:
    return f"{value*100:.{digits}f}%"


def gini_coefficient(values) -> float:
    array = np.sort(np.asarray(values, dtype=np.float64))
    if array.size == 0:
        return 0.0

    total = float(array.sum())
    if total <= 0:
        return 0.0

    index = np.arange(1, array.size + 1, dtype=np.float64)
    return float(np.sum((2 * index - array.size - 1) * array) / (array.size * total))


def load_visible_sample() -> pd.DataFrame:
    publication_input = read_publication_input()
    df = pd.read_csv(publication_input.dataset_path)
    ensure_validation_passes(validate_visible_sample(df))
    df["revenue_band"] = pd.cut(
        df["revenue_30d"],
        bins=REVENUE_BINS,
        labels=REVENUE_LABELS,
        right=False,
        include_lowest=True,
    )
    return df.sort_values(["revenue_30d", "name"], ascending=[False, True]).reset_index(drop=True)


def summarize_visible_sample(df: pd.DataFrame) -> SummaryArtifacts:
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

    return SummaryArtifacts(
        visible_sample=df,
        total_revenue=total_revenue,
        sample_size=sample_size,
        category_summary=category_summary,
        biz_summary=biz_summary,
        gtm_summary=gtm_summary,
        revenue_band_summary=revenue_band_summary,
    )


def build_metrics(summary: SummaryArtifacts) -> MetricsSnapshot:
    return MetricsSnapshot(
        sample_size=summary.sample_size,
        total_visible_revenue_usd=summary.total_revenue,
        median_revenue_usd=float(summary.visible_sample["revenue_30d"].median()),
        p75_revenue_usd=float(summary.visible_sample["revenue_30d"].quantile(0.75)),
        p90_revenue_usd=float(summary.visible_sample["revenue_30d"].quantile(0.90)),
        top_1_revenue_share=float(summary.visible_sample["revenue_30d"].head(1).sum() / summary.total_revenue),
        top_5_revenue_share=float(summary.visible_sample["revenue_30d"].head(5).sum() / summary.total_revenue),
        top_10_revenue_share=float(summary.visible_sample["revenue_30d"].head(10).sum() / summary.total_revenue),
        top_20_revenue_share=float(summary.visible_sample["revenue_30d"].head(20).sum() / summary.total_revenue),
        gini_coefficient=gini_coefficient(summary.visible_sample["revenue_30d"]),
        dominant_category=str(summary.category_summary.iloc[0]["category"]),
        dominant_category_revenue_share=float(summary.category_summary.iloc[0]["revenue_share"]),
        dominant_category_startup_share=float(summary.category_summary.iloc[0]["startup_share"]),
    )


def write_summary_outputs(summary: SummaryArtifacts) -> MetricsSnapshot:
    summary.category_summary.to_csv(BUILD_PATHS.data_dir / "category_summary.csv", index=False)
    summary.biz_summary.to_csv(BUILD_PATHS.data_dir / "business_model_summary.csv", index=False)
    summary.gtm_summary.to_csv(BUILD_PATHS.data_dir / "gtm_model_summary.csv", index=False)
    summary.revenue_band_summary.to_csv(BUILD_PATHS.data_dir / "revenue_band_summary.csv", index=False)
    pd.DataFrame({"source_url": sorted(summary.visible_sample["source_url"].unique())}).to_csv(
        BUILD_PATHS.data_dir / "public_source_pages.csv",
        index=False,
    )
    metrics = build_metrics(summary)
    (BUILD_PATHS.data_dir / "metrics.json").write_text(
        json.dumps(asdict(metrics), indent=2),
        encoding="utf-8",
    )
    return metrics
