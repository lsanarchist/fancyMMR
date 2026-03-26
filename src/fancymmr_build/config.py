from __future__ import annotations

from pathlib import Path

from .schemas import BuildPaths


ROOT = Path(__file__).resolve().parents[2]
BUILD_PATHS = BuildPaths(
    root=ROOT,
    data_dir=ROOT / "data",
    charts_dir=ROOT / "charts",
)

PROJECT_NAME = "fancyMMR / TrustMRR visible-sample research pipeline"
BUILD_COMMAND = "python src/build_artifacts.py"
PROMOTION_COMMAND = "python src/promote_live_bundle.py"
PYTHON_VERSION_FLOOR = "3.12"
REQUIRED_COLS = ["name", "category", "revenue_30d", "biz_model", "gtm_model", "source_url"]
MIN_REVENUE_30D = 5000
REVENUE_BINS = [MIN_REVENUE_30D, 10000, 50000, 100000, 500000, 1000000, float("inf")]
REVENUE_LABELS = ["$5k–$10k", "$10k–$50k", "$50k–$100k", "$100k–$500k", "$500k–$1M", "$1M+"]
PUBLICATION_INPUT_MANIFEST = "data/publication_input.json"
DEFAULT_PUBLICATION_DATASET = "data/visible_sample.csv"
PROMOTED_PUBLICATION_DATASET = "data/promoted_visible_sample.csv"

CHART_STEMS = [
    "category_share_map",
    "top_categories_revenue",
    "category_over_index",
    "model_mix",
    "distribution_and_concentration",
]
SUMMARY_OUTPUTS = [
    "data/category_summary.csv",
    "data/business_model_summary.csv",
    "data/gtm_model_summary.csv",
    "data/revenue_band_summary.csv",
    "data/public_source_pages.csv",
    "data/metrics.json",
]
REPORT_OUTPUTS = [
    "data/validation_report.json",
    "data/source_coverage_report.json",
    "data/source_pipeline_diagnostics.json",
]
MANIFEST_OUTPUT = "data/pipeline_manifest.json"
GENERATED_OUTPUTS = SUMMARY_OUTPUTS + REPORT_OUTPUTS + [
    output
    for stem in CHART_STEMS
    for output in (f"charts/{stem}.png", f"charts/{stem}.svg")
] + [
    PUBLICATION_INPUT_MANIFEST,
    "README.md",
]

PRIMARY = "#2563eb"
ACCENT = "#f59e0b"
MUTED = "#94a3b8"
TEXT = "#0f172a"
SUBTEXT = "#475569"

SVG_SAVE_METADATA = {"Date": None, "Creator": None}
