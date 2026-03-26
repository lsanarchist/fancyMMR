from __future__ import annotations

from hashlib import file_digest
import json
from pathlib import Path

import pandas as pd

from .config import (
    BUILD_COMMAND,
    BUILD_PATHS,
    DEFAULT_PUBLICATION_DATASET,
    GENERATED_OUTPUTS,
    MANIFEST_OUTPUT,
    MIN_REVENUE_30D,
    PROMOTION_COMMAND,
    PROJECT_NAME,
    PYTHON_VERSION_FLOOR,
    REQUIRED_COLS,
)
from .publication import read_publication_input
from .schemas import MetricsSnapshot, SummaryArtifacts


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return file_digest(handle, "sha256").hexdigest()


def _status_from_checks(checks: list[dict[str, object]]) -> str:
    has_errors = any(check["severity"] == "error" and not check["passed"] for check in checks)
    if has_errors:
        return "failed"

    has_warnings = any(check["severity"] == "warning" and not check["passed"] for check in checks)
    if has_warnings:
        return "passed_with_warnings"

    return "passed"


def _check(
    *,
    check_id: str,
    severity: str,
    passed: bool,
    message: str,
    details: dict[str, object],
) -> dict[str, object]:
    return {
        "id": check_id,
        "severity": severity,
        "passed": passed,
        "message": message,
        "details": details,
    }


def validate_visible_sample(df: pd.DataFrame) -> dict[str, object]:
    missing_columns = [column for column in REQUIRED_COLS if column not in df.columns]
    row_count = int(len(df))
    source_page_count = int(df["source_url"].nunique()) if "source_url" in df.columns else 0
    null_counts = {column: int(df[column].isna().sum()) for column in sorted(df.columns)}
    revenue_is_present = "revenue_30d" in df.columns
    revenue_series = df["revenue_30d"] if revenue_is_present else pd.Series(dtype="float64")
    null_revenue_count = int(revenue_series.isna().sum()) if revenue_is_present else row_count
    below_threshold_count = (
        int((revenue_series.dropna() < MIN_REVENUE_30D).sum()) if revenue_is_present else row_count
    )
    min_revenue_30d = (
        float(revenue_series.min()) if revenue_is_present and not revenue_series.dropna().empty else None
    )
    duplicate_name_count = int(df.duplicated(subset=["name"]).sum()) if "name" in df.columns else row_count
    duplicate_name_source_url_count = (
        int(df.duplicated(subset=["name", "source_url"]).sum())
        if {"name", "source_url"}.issubset(df.columns)
        else row_count
    )
    missing_source_url_count = int(df["source_url"].isna().sum()) if "source_url" in df.columns else row_count
    biz_model_null_count = int(df["biz_model"].isna().sum()) if "biz_model" in df.columns else row_count
    gtm_model_null_count = int(df["gtm_model"].isna().sum()) if "gtm_model" in df.columns else row_count

    checks = [
        _check(
            check_id="required_columns_present",
            severity="error",
            passed=not missing_columns,
            message="All required visible-sample columns are present.",
            details={"missing_columns": missing_columns},
        ),
        _check(
            check_id="row_count_positive",
            severity="error",
            passed=row_count > 0,
            message="The visible sample contains at least one row.",
            details={"row_count": row_count},
        ),
        _check(
            check_id="revenue_values_non_null",
            severity="error",
            passed=null_revenue_count == 0,
            message="`revenue_30d` contains no null values.",
            details={"null_revenue_count": null_revenue_count},
        ),
        _check(
            check_id="revenue_threshold_respected",
            severity="error",
            passed=below_threshold_count == 0,
            message="All visible startups meet the $5,000 inclusion threshold.",
            details={
                "threshold_usd": MIN_REVENUE_30D,
                "below_threshold_count": below_threshold_count,
                "min_revenue_30d": min_revenue_30d,
            },
        ),
        _check(
            check_id="source_url_present",
            severity="error",
            passed=missing_source_url_count == 0,
            message="Every row preserves a `source_url`.",
            details={"missing_source_url_count": missing_source_url_count},
        ),
        _check(
            check_id="duplicate_name_source_url_pairs",
            severity="error",
            passed=duplicate_name_source_url_count == 0,
            message="No duplicate `(name, source_url)` pairs were detected.",
            details={"duplicate_name_source_url_count": duplicate_name_source_url_count},
        ),
        _check(
            check_id="duplicate_names",
            severity="warning",
            passed=duplicate_name_count == 0,
            message="Duplicate startup names are tracked as a warning-only signal in the current bundle.",
            details={"duplicate_name_count": duplicate_name_count},
        ),
        _check(
            check_id="heuristic_label_completeness",
            severity="warning",
            passed=biz_model_null_count == 0 and gtm_model_null_count == 0,
            message="Heuristic label nulls are surfaced as warnings rather than hard failures for the seed bundle.",
            details={
                "biz_model_null_count": biz_model_null_count,
                "gtm_model_null_count": gtm_model_null_count,
            },
        ),
    ]

    return {
        "status": _status_from_checks(checks),
        "sample_row_count": row_count,
        "source_page_count": source_page_count,
        "required_columns": REQUIRED_COLS,
        "missing_columns": missing_columns,
        "null_counts": null_counts,
        "duplicate_name_count": duplicate_name_count,
        "duplicate_name_source_url_count": duplicate_name_source_url_count,
        "min_revenue_30d": min_revenue_30d,
        "checks": checks,
    }


def ensure_validation_passes(report: dict[str, object]) -> None:
    failed_error_checks = [
        check["id"]
        for check in report["checks"]
        if check["severity"] == "error" and not check["passed"]
    ]
    if failed_error_checks:
        raise ValueError(f"Validation failed: {', '.join(failed_error_checks)}")


def build_source_coverage_report(summary: SummaryArtifacts) -> dict[str, object]:
    coverage = (
        summary.visible_sample.groupby("source_url", as_index=False)
        .agg(
            startup_count=("name", "count"),
            total_revenue_usd=("revenue_30d", "sum"),
            median_revenue_usd=("revenue_30d", "median"),
            min_revenue_usd=("revenue_30d", "min"),
            max_revenue_usd=("revenue_30d", "max"),
            categories=("category", lambda values: sorted(set(values))),
        )
        .sort_values(["startup_count", "total_revenue_usd", "source_url"], ascending=[False, False, True])
        .reset_index(drop=True)
    )
    coverage["startup_share"] = coverage["startup_count"] / summary.sample_size
    coverage["revenue_share"] = coverage["total_revenue_usd"] / summary.total_revenue

    source_pages = [
        {
            "source_url": row["source_url"],
            "startup_count": int(row["startup_count"]),
            "startup_share": float(row["startup_share"]),
            "total_revenue_usd": int(row["total_revenue_usd"]),
            "revenue_share": float(row["revenue_share"]),
            "median_revenue_usd": float(row["median_revenue_usd"]),
            "min_revenue_usd": int(row["min_revenue_usd"]),
            "max_revenue_usd": int(row["max_revenue_usd"]),
            "categories": row["categories"],
        }
        for _, row in coverage.iterrows()
    ]

    return {
        "source_page_count": int(len(source_pages)),
        "total_visible_startups": summary.sample_size,
        "total_visible_revenue_usd": summary.total_revenue,
        "source_pages": source_pages,
    }


def write_validation_outputs(summary: SummaryArtifacts) -> tuple[dict[str, object], dict[str, object]]:
    validation_input = summary.visible_sample.loc[:, [column for column in summary.visible_sample.columns if column in REQUIRED_COLS]]
    validation_report = validate_visible_sample(validation_input)
    _write_json(BUILD_PATHS.data_dir / "validation_report.json", validation_report)
    ensure_validation_passes(validation_report)

    source_coverage_report = build_source_coverage_report(summary)
    _write_json(BUILD_PATHS.data_dir / "source_coverage_report.json", source_coverage_report)

    return validation_report, source_coverage_report


def _artifact_record(path: Path) -> dict[str, object]:
    relative_path = path.relative_to(BUILD_PATHS.root).as_posix()
    return {
        "path": relative_path,
        "bytes": int(path.stat().st_size),
        "sha256": _sha256(path),
    }


def write_pipeline_manifest(
    summary: SummaryArtifacts,
    metrics: MetricsSnapshot,
    validation_report: dict[str, object],
    source_coverage_report: dict[str, object],
) -> dict[str, object]:
    publication_input = read_publication_input()
    input_path = publication_input.dataset_path
    input_columns = list(pd.read_csv(input_path, nrows=0).columns)
    manifest = {
        "project": PROJECT_NAME,
        "build_command": BUILD_COMMAND,
        "promotion_command": PROMOTION_COMMAND,
        "entrypoint": "src/build_artifacts.py",
        "python_version_floor": PYTHON_VERSION_FLOOR,
        "publication_input": {
            "path": publication_input.manifest_path.relative_to(BUILD_PATHS.root).as_posix(),
            "dataset_kind": publication_input.dataset_kind,
            "dataset_path": publication_input.dataset_path_str,
            "source_label": publication_input.source_label,
            "expected_source_count": publication_input.expected_source_count,
            "selected_source_count": publication_input.selected_source_count,
            "promoted_from_visible_rows_path": publication_input.promoted_from_visible_rows_path,
            "source_pipeline_validation_report_path": publication_input.source_pipeline_validation_report_path,
            "source_pipeline_override_report_path": publication_input.source_pipeline_override_report_path,
            "source_pipeline_duplicates_report_path": publication_input.source_pipeline_duplicates_report_path,
            "source_pipeline_run_manifest_path": publication_input.source_pipeline_run_manifest_path,
            "promoted_at": publication_input.promoted_at,
            "staged_visible_rows_sha256": publication_input.staged_visible_rows_sha256,
            "promoted_dataset_sha256": publication_input.promoted_dataset_sha256,
            "promotion_gate_summary": publication_input.promotion_gate_summary,
            "uses_default_seed_dataset": publication_input.dataset_path_str == DEFAULT_PUBLICATION_DATASET,
        },
        "input_dataset": {
            "path": input_path.relative_to(BUILD_PATHS.root).as_posix(),
            "sha256": _sha256(input_path),
            "rows": summary.sample_size,
            "columns": input_columns,
        },
        "validation": {
            "status": validation_report["status"],
            "report_path": "data/validation_report.json",
            "source_coverage_report_path": "data/source_coverage_report.json",
        },
        "metrics_snapshot": {
            "sample_size": metrics.sample_size,
            "total_visible_revenue_usd": metrics.total_visible_revenue_usd,
            "dominant_category": metrics.dominant_category,
            "gini_coefficient": metrics.gini_coefficient,
        },
        "source_page_count": source_coverage_report["source_page_count"],
        "generated_outputs": [
            _artifact_record(BUILD_PATHS.root / relative_path)
            for relative_path in GENERATED_OUTPUTS
        ],
    }
    manifest_path = BUILD_PATHS.root / MANIFEST_OUTPUT
    _write_json(manifest_path, manifest)
    return manifest
