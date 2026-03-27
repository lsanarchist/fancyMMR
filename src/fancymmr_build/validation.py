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
    REPORT_OUTPUTS,
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


def _load_optional_json(relative_path: str | None) -> dict[str, object] | None:
    if not relative_path:
        return None
    path = BUILD_PATHS.root / relative_path
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _find_generated_output_path(run_manifest: dict[str, object], filename: str) -> str | None:
    for relative_path in run_manifest.get("generated_outputs", []):
        relative_path_str = str(relative_path)
        if relative_path_str == filename or relative_path_str.endswith(f"/{filename}"):
            return relative_path_str
    return None


def _int_dict(value: dict[str, object] | None) -> dict[str, int]:
    return {str(key): int(count) for key, count in (value or {}).items()}


def _build_downloadable_staged_artifacts(
    artifact_paths: dict[str, str | None],
) -> list[dict[str, object]]:
    artifacts: list[dict[str, object]] = []
    for artifact_key, label, description, artifact_format in [
        (
            "run_manifest_path",
            "Staged run manifest",
            "End-to-end staged fetch/parse/normalize snapshot for the active promoted source-pipeline bundle.",
            "json",
        ),
        (
            "validation_report_path",
            "Staged validation report",
            "Staged source-pipeline validation status, checks, and failure details for the active promoted bundle.",
            "json",
        ),
        (
            "override_report_path",
            "Staged override coverage",
            "Heuristic override coverage and alias-resolution counts for the staged promoted source-pipeline bundle.",
            "json",
        ),
        (
            "duplicates_report_path",
            "Staged duplicate review",
            "Suspicious duplicate review output for the staged promoted source-pipeline bundle.",
            "json",
        ),
        (
            "detail_rows_path",
            "Staged detail rows",
            "Flattened staged detail-page outcomes and parsed shared fields. This is staged provenance, not a promoted dataset column contract.",
            "csv",
        ),
        (
            "detail_field_coverage_path",
            "Staged detail coverage",
            "Aggregate and per-source shared detail-field coverage derived from the staged detail rows. This remains staged provenance.",
            "json",
        ),
    ]:
        relative_path = artifact_paths.get(artifact_key)
        if not relative_path:
            continue
        if not (BUILD_PATHS.root / relative_path).exists():
            continue
        artifacts.append(
            {
                "path": relative_path,
                "site_path": relative_path,
                "label": label,
                "description": description,
                "format": artifact_format,
            }
        )
    return artifacts


def build_source_pipeline_diagnostics_report(summary: SummaryArtifacts) -> dict[str, object]:
    publication_input = read_publication_input()
    report_paths = {
        "run_manifest_path": publication_input.source_pipeline_run_manifest_path,
        "validation_report_path": publication_input.source_pipeline_validation_report_path,
        "override_report_path": publication_input.source_pipeline_override_report_path,
        "duplicates_report_path": publication_input.source_pipeline_duplicates_report_path,
        "detail_rows_path": None,
        "detail_field_coverage_path": None,
    }
    report = {
        "schema_version": 1,
        "available": False,
        "publication_dataset_kind": publication_input.dataset_kind,
        "publication_dataset_path": publication_input.dataset_path_str,
        "source_label": publication_input.source_label,
        "promoted_at": publication_input.promoted_at,
        "expected_source_count": publication_input.expected_source_count,
        "selected_source_count": publication_input.selected_source_count,
        "published_visible_sample_row_count": summary.sample_size,
        "promotion_gate_summary": publication_input.promotion_gate_summary,
        "report_paths": report_paths,
        "missing_report_paths": [],
        "message": "The active publication dataset is not backed by a promoted source-pipeline manifest.",
        "validation_status": None,
        "run_manifest_validation_status": None,
        "normalized_row_count": None,
        "visible_sample_row_count": None,
        "detail_page_target_count": None,
        "fetched_detail_page_count": None,
        "parsed_detail_page_count": None,
        "failed_detail_page_count": None,
        "detail_parse_failure_source_count": None,
        "detail_parse_status_counts": None,
        "detail_field_population_counts": None,
        "fully_mapped_visible_row_count": None,
        "alias_resolved_visible_row_count": None,
        "unmapped_visible_row_count": None,
        "duplicate_detail_url_count": None,
        "duplicate_name_source_url_count": None,
        "suspicious_duplicate_group_count": None,
        "suspicious_duplicate_row_count": None,
        "missing_biz_model_count": None,
        "missing_gtm_model_count": None,
        "failing_warning_check_ids": [],
        "failing_error_check_ids": [],
        "detail_parse_failure_sources": [],
        "downloadable_staged_artifacts": [],
        "source_pages": [],
    }

    if publication_input.dataset_kind != "source_pipeline_promotion":
        return report

    missing_report_paths = [
        relative_path
        for relative_path in report_paths.values()
        if relative_path and not (BUILD_PATHS.root / relative_path).exists()
    ]
    if missing_report_paths:
        report["missing_report_paths"] = missing_report_paths
        report["message"] = (
            "The publication manifest points at a promoted source-pipeline dataset, "
            "but one or more staged diagnostics files are unavailable locally."
        )
        return report

    run_manifest = _load_optional_json(publication_input.source_pipeline_run_manifest_path)
    validation_report = _load_optional_json(publication_input.source_pipeline_validation_report_path)
    override_report = _load_optional_json(publication_input.source_pipeline_override_report_path)
    duplicates_report = _load_optional_json(publication_input.source_pipeline_duplicates_report_path)
    if not all((run_manifest, validation_report, override_report, duplicates_report)):
        report["message"] = (
            "The publication manifest references staged source-pipeline diagnostics, "
            "but one or more reports could not be loaded."
        )
        return report

    detail_field_coverage_path = _find_generated_output_path(run_manifest, "detail_field_coverage.json")
    detail_rows_path = _find_generated_output_path(run_manifest, "detail_page_rows.csv")
    report_paths["detail_rows_path"] = detail_rows_path
    report_paths["detail_field_coverage_path"] = detail_field_coverage_path
    detail_field_coverage = _load_optional_json(detail_field_coverage_path)
    if detail_field_coverage is None:
        report["missing_report_paths"] = (
            [detail_field_coverage_path] if detail_field_coverage_path else ["data/source_pipeline/processed/detail_field_coverage.json"]
        )
        report["message"] = (
            "The publication manifest references staged source-pipeline diagnostics, "
            "but the detail-field coverage summary could not be loaded."
        )
        return report

    selected_sources_by_id = {
        str(source["source_id"]): source for source in run_manifest.get("selected_sources", [])
    }
    coverage_sources_by_id = {
        str(source["source_id"]): source for source in detail_field_coverage.get("sources", [])
    }
    source_pages = []
    for source_output in run_manifest.get("per_source_outputs", []):
        source_id = str(source_output["source_id"])
        source_metadata = selected_sources_by_id.get(source_id, {})
        coverage_metadata = coverage_sources_by_id.get(source_id, {})
        source_pages.append(
            {
                "source_id": source_id,
                "source_url": str(source_output["source_url"]),
                "parser_strategy": source_metadata.get("parser_strategy"),
                "source_group": source_metadata.get("source_group"),
                "category_label": source_metadata.get("category_label"),
                "parsed_card_count": int(source_output["parsed_card_count"]),
                "visible_sample_row_count": int(source_output["visible_sample_row_count"]),
                "detail_page_target_count": int(source_output.get("detail_page_target_count") or 0),
                "fetched_detail_page_count": int(source_output.get("fetched_detail_page_count") or 0),
                "parsed_detail_page_count": int(source_output.get("parsed_detail_page_count") or 0),
                "failed_detail_page_count": int(source_output.get("failed_detail_page_count") or 0),
                "detail_parse_status_counts": _int_dict(
                    coverage_metadata.get("parse_status_counts")
                    if isinstance(coverage_metadata, dict)
                    else None
                ),
                "detail_field_population_counts": _int_dict(
                    coverage_metadata.get("field_population_counts")
                    if isinstance(coverage_metadata, dict)
                    else None
                ),
                "raw_html_path": source_output.get("raw_html_path"),
                "interim_path": source_output.get("interim_path"),
            }
        )
    source_pages.sort(
        key=lambda source: (
            -int(source["visible_sample_row_count"]),
            -int(source["parsed_card_count"]),
            str(source["source_id"]),
        )
    )
    detail_parse_failure_sources = [
        {
            "source_id": str(source["source_id"]),
            "source_url": str(source["source_url"]),
            "parser_strategy": source.get("parser_strategy"),
            "source_group": source.get("source_group"),
            "category_label": source.get("category_label"),
            "detail_page_target_count": int(source["detail_page_target_count"]),
            "fetched_detail_page_count": int(source["fetched_detail_page_count"]),
            "parsed_detail_page_count": int(source["parsed_detail_page_count"]),
            "failed_detail_page_count": int(source["failed_detail_page_count"]),
        }
        for source in sorted(
            source_pages,
            key=lambda source: (
                -int(source["failed_detail_page_count"]),
                -int(source["detail_page_target_count"]),
                str(source["source_id"]),
            ),
        )
        if int(source["failed_detail_page_count"]) > 0
    ]
    downloadable_staged_artifacts = _build_downloadable_staged_artifacts(report_paths)

    report.update(
        {
            "available": True,
            "message": "Diagnostics loaded from the staged source-pipeline reports referenced by the active publication manifest.",
            "validation_status": validation_report["status"],
            "run_manifest_validation_status": run_manifest.get("validation_status"),
            "normalized_row_count": int(run_manifest["normalized_row_count"]),
            "visible_sample_row_count": int(run_manifest["visible_sample_row_count"]),
            "detail_page_target_count": int(run_manifest.get("detail_page_target_count") or 0),
            "fetched_detail_page_count": int(run_manifest.get("fetched_detail_page_count") or 0),
            "parsed_detail_page_count": int(run_manifest.get("parsed_detail_page_count") or 0),
            "failed_detail_page_count": int(run_manifest.get("failed_detail_page_count") or 0),
            "detail_parse_failure_source_count": len(detail_parse_failure_sources),
            "detail_parse_status_counts": _int_dict(
                detail_field_coverage.get("aggregate", {}).get("parse_status_counts")
                if isinstance(detail_field_coverage.get("aggregate"), dict)
                else None
            ),
            "detail_field_population_counts": _int_dict(
                detail_field_coverage.get("aggregate", {}).get("field_population_counts")
                if isinstance(detail_field_coverage.get("aggregate"), dict)
                else None
            ),
            "fully_mapped_visible_row_count": int(override_report["fully_mapped_visible_row_count"]),
            "alias_resolved_visible_row_count": int(override_report["alias_resolved_visible_row_count"]),
            "unmapped_visible_row_count": int(override_report["unmapped_visible_row_count"]),
            "duplicate_detail_url_count": int(validation_report["duplicate_detail_url_count"]),
            "duplicate_name_source_url_count": int(validation_report["duplicate_name_source_url_count"]),
            "suspicious_duplicate_group_count": int(duplicates_report["group_count"]),
            "suspicious_duplicate_row_count": int(duplicates_report["row_count"]),
            "missing_biz_model_count": int(validation_report["missing_biz_model_count"]),
            "missing_gtm_model_count": int(validation_report["missing_gtm_model_count"]),
            "failing_warning_check_ids": [
                str(check["id"])
                for check in validation_report["checks"]
                if check["severity"] == "warning" and not check["passed"]
            ],
            "failing_error_check_ids": [
                str(check["id"])
                for check in validation_report["checks"]
                if check["severity"] == "error" and not check["passed"]
            ],
            "detail_parse_failure_sources": detail_parse_failure_sources,
            "downloadable_staged_artifacts": downloadable_staged_artifacts,
            "source_pages": source_pages,
        }
    )
    return report


def write_validation_outputs(
    summary: SummaryArtifacts,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    validation_input = summary.visible_sample.loc[:, [column for column in summary.visible_sample.columns if column in REQUIRED_COLS]]
    validation_report = validate_visible_sample(validation_input)
    _write_json(BUILD_PATHS.data_dir / "validation_report.json", validation_report)
    ensure_validation_passes(validation_report)

    source_coverage_report = build_source_coverage_report(summary)
    _write_json(BUILD_PATHS.data_dir / "source_coverage_report.json", source_coverage_report)
    source_pipeline_diagnostics_report = build_source_pipeline_diagnostics_report(summary)
    _write_json(BUILD_PATHS.data_dir / "source_pipeline_diagnostics.json", source_pipeline_diagnostics_report)

    return validation_report, source_coverage_report, source_pipeline_diagnostics_report


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
    source_pipeline_diagnostics_report: dict[str, object],
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
            "source_pipeline_diagnostics_report_path": REPORT_OUTPUTS[-1],
        },
        "metrics_snapshot": {
            "sample_size": metrics.sample_size,
            "total_visible_revenue_usd": metrics.total_visible_revenue_usd,
            "dominant_category": metrics.dominant_category,
            "gini_coefficient": metrics.gini_coefficient,
        },
        "source_pipeline_diagnostics": {
            "available": source_pipeline_diagnostics_report["available"],
            "path": REPORT_OUTPUTS[-1],
            "validation_status": source_pipeline_diagnostics_report["validation_status"],
            "selected_source_count": source_pipeline_diagnostics_report["selected_source_count"],
            "expected_source_count": source_pipeline_diagnostics_report["expected_source_count"],
            "parsed_detail_page_count": source_pipeline_diagnostics_report["parsed_detail_page_count"],
            "failed_detail_page_count": source_pipeline_diagnostics_report["failed_detail_page_count"],
            "detail_parse_failure_source_count": source_pipeline_diagnostics_report["detail_parse_failure_source_count"],
            "detail_parse_status_counts": source_pipeline_diagnostics_report["detail_parse_status_counts"],
            "detail_field_population_counts": source_pipeline_diagnostics_report["detail_field_population_counts"],
            "downloadable_staged_artifacts": source_pipeline_diagnostics_report["downloadable_staged_artifacts"],
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
