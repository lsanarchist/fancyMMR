from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class BuildPaths:
    root: Path
    data_dir: Path
    charts_dir: Path


@dataclass
class SummaryArtifacts:
    visible_sample: pd.DataFrame
    total_revenue: int
    sample_size: int
    category_summary: pd.DataFrame
    biz_summary: pd.DataFrame
    gtm_summary: pd.DataFrame
    revenue_band_summary: pd.DataFrame


@dataclass(frozen=True)
class MetricsSnapshot:
    sample_size: int
    total_visible_revenue_usd: int
    median_revenue_usd: float
    p75_revenue_usd: float
    p90_revenue_usd: float
    top_1_revenue_share: float
    top_5_revenue_share: float
    top_10_revenue_share: float
    top_20_revenue_share: float
    gini_coefficient: float
    dominant_category: str
    dominant_category_revenue_share: float
    dominant_category_startup_share: float

