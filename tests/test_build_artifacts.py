from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fancymmr_build.aggregate import build_metrics, load_visible_sample, summarize_visible_sample
from fancymmr_build.config import REVENUE_BINS, REVENUE_LABELS
from fancymmr_build.publication import default_publication_input_payload
from fancymmr_build.schemas import MetricsSnapshot, SummaryArtifacts
from fancymmr_build.validation import ensure_validation_passes, validate_visible_sample

EXPECTED_METRICS = json.loads((ROOT / "data" / "metrics.json").read_text(encoding="utf-8"))
EXPECTED_PUBLICATION_INPUT = json.loads((ROOT / "data" / "publication_input.json").read_text(encoding="utf-8"))
EXPECTED_SOURCE_PAGE_COUNT = pd.read_csv(ROOT / "data" / "visible_sample.csv")["source_url"].nunique()
EXPECTED_INPUT_COLUMNS = list(pd.read_csv(ROOT / "data" / "visible_sample.csv", nrows=0).columns)
EXPECTED_CHARTS = {
    "category_share_map.png",
    "category_share_map.svg",
    "top_categories_revenue.png",
    "top_categories_revenue.svg",
    "category_over_index.png",
    "category_over_index.svg",
    "model_mix.png",
    "model_mix.svg",
    "distribution_and_concentration.png",
    "distribution_and_concentration.svg",
}


def expected_seed_metrics() -> dict[str, object]:
    seed_df = pd.read_csv(ROOT / "data" / "visible_sample.csv")
    seed_df["revenue_band"] = pd.cut(
        seed_df["revenue_30d"],
        bins=REVENUE_BINS,
        labels=REVENUE_LABELS,
        right=False,
        include_lowest=True,
    )
    seed_df = seed_df.sort_values(["revenue_30d", "name"], ascending=[False, True]).reset_index(drop=True)
    return asdict(build_metrics(summarize_visible_sample(seed_df)))


def prepare_workspace(tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache")
    shutil.copytree(ROOT / "src", workspace / "src", ignore=ignore)
    shutil.copytree(ROOT / "data", workspace / "data", ignore=ignore)
    (workspace / "charts").mkdir()
    return workspace


def run_build(workspace: Path) -> None:
    result = subprocess.run(
        [sys.executable, "src/build_artifacts.py"],
        cwd=workspace,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def svg_hashes(workspace: Path) -> dict[str, str]:
    return {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted((workspace / "charts").glob("*.svg"))
    }


def test_build_artifacts_smoke_and_metrics_contract(tmp_path: Path) -> None:
    workspace = prepare_workspace(tmp_path)
    (workspace / "data" / "publication_input.json").unlink()

    run_build(workspace)

    assert (workspace / "README.md").exists()
    readme = (workspace / "README.md").read_text(encoding="utf-8")
    assert "visible public sample" in readme.lower()
    assert "python -m pytest" in readme
    assert "python src/build_site.py" in readme
    assert "python src/build_all.py --limit 1" in readme
    assert ".github/workflows/build.yml" in readme
    assert ".github/workflows/pages.yml" in readme
    assert "LICENSE-CODE-MIT.txt" in readme
    assert "DATA-NOTICE.md" in readme
    assert "data/publication_input.json" in readme
    assert "data/source_pipeline_diagnostics.json" in readme
    assert "python src/promote_live_bundle.py" in readme
    assert "every source in `data/public_source_pages.csv`" in readme
    assert "does **not** collapse those scopes into one top-level detected `LICENSE` file" in readme

    actual_charts = {path.name for path in (workspace / "charts").iterdir()}
    assert actual_charts == EXPECTED_CHARTS

    seed_metrics = expected_seed_metrics()
    built_metrics = json.loads((workspace / "data" / "metrics.json").read_text(encoding="utf-8"))
    assert built_metrics == seed_metrics

    publication_input = json.loads((workspace / "data" / "publication_input.json").read_text(encoding="utf-8"))
    source_pipeline_diagnostics = json.loads(
        (workspace / "data" / "source_pipeline_diagnostics.json").read_text(encoding="utf-8")
    )
    validation_report = json.loads((workspace / "data" / "validation_report.json").read_text(encoding="utf-8"))
    source_coverage_report = json.loads(
        (workspace / "data" / "source_coverage_report.json").read_text(encoding="utf-8")
    )
    pipeline_manifest = json.loads((workspace / "data" / "pipeline_manifest.json").read_text(encoding="utf-8"))

    assert publication_input == default_publication_input_payload()
    assert source_pipeline_diagnostics["available"] is False
    assert source_pipeline_diagnostics["publication_dataset_kind"] == "seed_visible_sample"
    assert source_pipeline_diagnostics["detail_page_target_count"] is None
    assert source_pipeline_diagnostics["failed_detail_page_count"] is None
    assert source_pipeline_diagnostics["detail_parse_failure_source_count"] is None
    assert source_pipeline_diagnostics["detail_parse_status_counts"] is None
    assert source_pipeline_diagnostics["detail_field_population_counts"] is None
    assert source_pipeline_diagnostics["detail_parse_failure_sources"] == []
    assert source_pipeline_diagnostics["downloadable_staged_artifacts"] == []
    assert validation_report["status"] in {"passed", "passed_with_warnings"}
    assert validation_report["sample_row_count"] == seed_metrics["sample_size"]
    assert source_coverage_report["source_page_count"] == EXPECTED_SOURCE_PAGE_COUNT
    assert source_coverage_report["total_visible_startups"] == seed_metrics["sample_size"]
    assert pipeline_manifest["publication_input"]["path"] == "data/publication_input.json"
    default_publication_input = default_publication_input_payload()
    assert pipeline_manifest["publication_input"]["dataset_kind"] == default_publication_input["dataset_kind"]
    assert pipeline_manifest["publication_input"]["dataset_path"] == default_publication_input["dataset_path"]
    assert pipeline_manifest["input_dataset"]["path"] == default_publication_input["dataset_path"]
    assert pipeline_manifest["input_dataset"]["columns"] == EXPECTED_INPUT_COLUMNS
    assert pipeline_manifest["validation"]["status"] == validation_report["status"]
    assert any(
        artifact["path"] == "data/validation_report.json" for artifact in pipeline_manifest["generated_outputs"]
    )
    assert any(
        artifact["path"] == "data/source_coverage_report.json" for artifact in pipeline_manifest["generated_outputs"]
    )
    assert any(
        artifact["path"] == "data/source_pipeline_diagnostics.json"
        for artifact in pipeline_manifest["generated_outputs"]
    )
    assert any(
        artifact["path"] == "data/publication_input.json" for artifact in pipeline_manifest["generated_outputs"]
    )
    assert any(artifact["path"] == "README.md" for artifact in pipeline_manifest["generated_outputs"])
    assert pipeline_manifest["validation"]["source_pipeline_diagnostics_report_path"] == "data/source_pipeline_diagnostics.json"
    assert pipeline_manifest["source_pipeline_diagnostics"]["available"] is False


def test_refactored_module_surfaces_match_expected_metrics() -> None:
    visible_sample = load_visible_sample()
    summary = summarize_visible_sample(visible_sample)
    metrics = build_metrics(summary)

    assert isinstance(summary, SummaryArtifacts)
    assert isinstance(metrics, MetricsSnapshot)
    assert metrics.sample_size == EXPECTED_METRICS["sample_size"]
    assert metrics.gini_coefficient == EXPECTED_METRICS["gini_coefficient"]
    assert metrics.dominant_category == EXPECTED_METRICS["dominant_category"]
    assert set(summary.category_summary.columns) >= {
        "category",
        "startup_count",
        "total_revenue",
        "median_revenue",
        "startup_share",
        "revenue_share",
        "performance_index",
    }


def test_build_artifacts_writes_source_pipeline_diagnostics_for_promoted_manifest(tmp_path: Path) -> None:
    workspace = prepare_workspace(tmp_path)

    run_build(workspace)

    diagnostics = json.loads((workspace / "data" / "source_pipeline_diagnostics.json").read_text(encoding="utf-8"))
    pipeline_manifest = json.loads((workspace / "data" / "pipeline_manifest.json").read_text(encoding="utf-8"))

    assert diagnostics["available"] is True
    assert diagnostics["publication_dataset_kind"] == "source_pipeline_promotion"
    assert diagnostics["validation_status"] == "passed"
    assert diagnostics["selected_source_count"] == 30
    assert diagnostics["detail_page_target_count"] == 852
    assert diagnostics["parsed_detail_page_count"] == 0
    assert diagnostics["failed_detail_page_count"] == 0
    assert diagnostics["detail_parse_failure_source_count"] == 0
    assert diagnostics["detail_parse_status_counts"]["not_requested"] == 852
    assert diagnostics["detail_parse_status_counts"]["parsed"] == 0
    assert diagnostics["detail_field_population_counts"]["problem_solved"] == 0
    assert diagnostics["detail_field_population_counts"]["founder_name"] == 0
    assert diagnostics["detail_parse_failure_sources"] == []
    assert [artifact["path"] for artifact in diagnostics["downloadable_staged_artifacts"]] == [
        "data/source_pipeline/snapshots/run_manifest.json",
        "data/source_pipeline/processed/validation_report.json",
        "data/source_pipeline/processed/heuristic_override_report.json",
        "data/source_pipeline/processed/suspicious_duplicates.json",
        "data/source_pipeline/processed/detail_page_rows.csv",
        "data/source_pipeline/processed/detail_field_coverage.json",
    ]
    assert all(
        artifact["path"] == artifact["site_path"] for artifact in diagnostics["downloadable_staged_artifacts"]
    )
    for artifact in diagnostics["downloadable_staged_artifacts"]:
        artifact_path = workspace / artifact["path"]
        assert artifact["bytes"] == artifact_path.stat().st_size
        assert artifact["sha256"] == hashlib.sha256(artifact_path.read_bytes()).hexdigest()
    assert diagnostics["fully_mapped_visible_row_count"] == 249
    assert diagnostics["source_pages"]
    assert diagnostics["source_pages"][0]["parsed_card_count"] >= diagnostics["source_pages"][-1]["parsed_card_count"]
    assert "failed_detail_page_count" in diagnostics["source_pages"][0]
    assert "detail_parse_status_counts" in diagnostics["source_pages"][0]
    assert "detail_field_population_counts" in diagnostics["source_pages"][0]
    assert pipeline_manifest["source_pipeline_diagnostics"]["available"] is True
    assert pipeline_manifest["source_pipeline_diagnostics"]["path"] == "data/source_pipeline_diagnostics.json"
    assert pipeline_manifest["source_pipeline_diagnostics"]["failed_detail_page_count"] == 0
    assert pipeline_manifest["source_pipeline_diagnostics"]["detail_parse_failure_source_count"] == 0
    assert pipeline_manifest["source_pipeline_diagnostics"]["detail_parse_status_counts"]["not_requested"] == 852
    assert pipeline_manifest["source_pipeline_diagnostics"]["detail_field_population_counts"]["problem_solved"] == 0
    assert [
        artifact["site_path"]
        for artifact in pipeline_manifest["source_pipeline_diagnostics"]["downloadable_staged_artifacts"]
    ] == [
        "data/source_pipeline/snapshots/run_manifest.json",
        "data/source_pipeline/processed/validation_report.json",
        "data/source_pipeline/processed/heuristic_override_report.json",
        "data/source_pipeline/processed/suspicious_duplicates.json",
        "data/source_pipeline/processed/detail_page_rows.csv",
        "data/source_pipeline/processed/detail_field_coverage.json",
    ]
    assert (
        pipeline_manifest["source_pipeline_diagnostics"]["downloadable_staged_artifacts"]
        == diagnostics["downloadable_staged_artifacts"]
    )


def test_validation_report_fails_for_missing_columns_and_threshold_violations() -> None:
    invalid = pd.DataFrame(
        {
            "name": ["TinyCo"],
            "category": ["AI"],
            "revenue_30d": [4900],
            "biz_model": ["SaaS"],
            "gtm_model": ["PLG"],
        }
    )

    report = validate_visible_sample(invalid)
    checks = {check["id"]: check for check in report["checks"]}

    assert report["status"] == "failed"
    assert report["missing_columns"] == ["source_url"]
    assert not checks["required_columns_present"]["passed"]
    assert not checks["revenue_threshold_respected"]["passed"]

    with pytest.raises(ValueError, match="Validation failed"):
        ensure_validation_passes(report)


def test_validation_report_flags_duplicate_name_source_url_pairs() -> None:
    duplicated = pd.DataFrame(
        {
            "name": ["Twin", "Twin"],
            "category": ["AI", "AI"],
            "revenue_30d": [7500, 7600],
            "biz_model": ["SaaS", "SaaS"],
            "gtm_model": ["PLG", "PLG"],
            "source_url": ["https://example.com/ai", "https://example.com/ai"],
        }
    )

    report = validate_visible_sample(duplicated)
    checks = {check["id"]: check for check in report["checks"]}

    assert report["status"] == "failed"
    assert report["duplicate_name_count"] == 1
    assert report["duplicate_name_source_url_count"] == 1
    assert not checks["duplicate_name_source_url_pairs"]["passed"]


def test_svg_exports_are_deterministic_across_rebuilds(tmp_path: Path) -> None:
    workspace = prepare_workspace(tmp_path)

    run_build(workspace)
    first_hashes = svg_hashes(workspace)

    for path in sorted((workspace / "charts").glob("*.svg")):
        text = path.read_text(encoding="utf-8")
        assert "<dc:date>" not in text
        assert "<dc:creator>" not in text

    run_build(workspace)
    second_hashes = svg_hashes(workspace)

    assert second_hashes == first_hashes
