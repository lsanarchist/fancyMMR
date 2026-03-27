from __future__ import annotations

from datetime import datetime
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


def _count_values(
    rows: list[dict[str, object]],
    key: str,
    *,
    none_label: str,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = row.get(key)
        count_key = none_label if value is None else str(value)
        counts[count_key] = counts.get(count_key, 0) + 1
    return dict(sorted(counts.items()))


def _parse_timestamp(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _timestamp_bounds(
    rows: list[dict[str, object]],
    key: str,
) -> tuple[str | None, str | None]:
    parsed_rows = [
        (parsed_timestamp, str(row.get(key)))
        for row in rows
        if (parsed_timestamp := _parse_timestamp(row.get(key))) is not None
    ]
    if not parsed_rows:
        return None, None
    parsed_rows.sort(key=lambda item: item[0])
    return parsed_rows[0][1], parsed_rows[-1][1]


def _float_bounds(
    rows: list[dict[str, object]],
    key: str,
) -> tuple[float | None, float | None]:
    values: list[float] = []
    for row in rows:
        value = row.get(key)
        if value in (None, ""):
            continue
        try:
            values.append(float(value))
        except (TypeError, ValueError):
            continue
    if not values:
        return None, None
    return min(values), max(values)


def _robots_policy_label(value: object) -> str | None:
    if isinstance(value, bool):
        return "allowed" if value else "disallowed"
    if value in (None, ""):
        return None
    return str(value)


def _source_label(category_label: object, source_id: object) -> str:
    if category_label not in (None, ""):
        return str(category_label)
    if source_id not in (None, ""):
        return str(source_id)
    return "unknown"


def _snapshot_availability_label(value: object) -> str:
    if bool(value):
        return "available"
    return "missing"


def _status_code_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _fetch_failure_severity_label(
    *,
    robots_policy: object,
    status_code: object,
    error_type: object,
) -> str:
    if robots_policy == "disallowed":
        return "policy_blocked"

    parsed_status_code = _status_code_int(status_code)
    if parsed_status_code is not None:
        if parsed_status_code >= 500:
            return "server_error"
        if parsed_status_code >= 400:
            return "client_error"
        return "http_status_other"

    error_type_text = str(error_type or "")
    if error_type_text == "URLError":
        return "transport_error"
    if error_type_text:
        return "request_error"
    return "unknown"


def _fetch_failure_retryability_label(
    *,
    failure_severity: object,
    status_code: object,
) -> str:
    if failure_severity == "policy_blocked":
        return "do_not_retry"

    parsed_status_code = _status_code_int(status_code)
    if parsed_status_code == 429:
        return "retryable"

    if failure_severity in {"server_error", "transport_error"}:
        return "retryable"

    return "manual_review"


def _fetch_failure_next_action_label(
    *,
    failure_retryability: object,
    failure_severity: object,
    has_html_snapshot: bool,
) -> str:
    if failure_retryability == "do_not_retry":
        return "respect_robots_policy"
    if failure_retryability == "retryable":
        return "retry_after_backoff"
    if failure_severity == "client_error":
        return "inspect_request_configuration"
    if has_html_snapshot:
        return "inspect_failure_snapshot"
    return "manual_investigation"


def _artifact_download_record(
    *,
    relative_path: str,
    label: str,
    description: str,
    artifact_format: str,
) -> dict[str, object] | None:
    artifact_path = BUILD_PATHS.root / relative_path
    if not artifact_path.exists():
        return None
    return {
        "path": relative_path,
        "site_path": relative_path,
        "label": label,
        "description": description,
        "format": artifact_format,
        "bytes": int(artifact_path.stat().st_size),
        "sha256": _sha256(artifact_path),
    }


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
        artifact = _artifact_download_record(
            relative_path=relative_path,
            label=label,
            description=description,
            artifact_format=artifact_format,
        )
        if artifact is not None:
            artifacts.append(artifact)
    return artifacts


def _load_fetch_failure_sources(
    selected_sources_by_id: dict[str, dict[str, object]],
) -> list[dict[str, object]]:
    failure_snapshot_dir = BUILD_PATHS.root / "data" / "fetch_failures"
    if not failure_snapshot_dir.exists():
        return []

    failure_sources: list[dict[str, object]] = []
    for meta_path in sorted(failure_snapshot_dir.glob("*.json")):
        snapshot = json.loads(meta_path.read_text(encoding="utf-8"))
        if not isinstance(snapshot, dict):
            continue

        source_id = str(snapshot.get("source_id") or meta_path.stem)
        if source_id not in selected_sources_by_id:
            continue

        robots = snapshot.get("robots")
        robots_policy = None
        robots_status_code = None
        robots_url = None
        robots_effective_delay_seconds = None
        if isinstance(robots, dict):
            robots_policy = _robots_policy_label(robots.get("allowed"))
            robots_status_code = robots.get("status_code")
            robots_url = robots.get("robots_url")
            robots_effective_delay_seconds = robots.get("effective_delay_seconds")

        source_metadata = selected_sources_by_id.get(source_id, {})
        category_label = source_metadata.get("category_label")
        has_html_snapshot = bool(snapshot.get("html_snapshot_path"))
        error_type = str(snapshot.get("error_type") or "")
        status_code = snapshot.get("status_code")
        failure_severity = _fetch_failure_severity_label(
            robots_policy=robots_policy,
            status_code=status_code,
            error_type=error_type,
        )
        failure_retryability = _fetch_failure_retryability_label(
            failure_severity=failure_severity,
            status_code=status_code,
        )
        failure_sources.append(
            {
                "source_id": source_id,
                "source_url": str(snapshot.get("url") or source_metadata.get("source_url") or ""),
                "parser_strategy": snapshot.get("parser_strategy") or source_metadata.get("parser_strategy"),
                "source_group": snapshot.get("source_group") or source_metadata.get("source_group"),
                "category_label": category_label,
                "source_label": _source_label(category_label, source_id),
                "recorded_at": snapshot.get("recorded_at"),
                "error_type": error_type,
                "message": str(snapshot.get("message") or ""),
                "status_code": status_code,
                "robots_policy": robots_policy,
                "robots_status_code": robots_status_code,
                "robots_url": robots_url,
                "robots_effective_delay_seconds": robots_effective_delay_seconds,
                "failure_severity": failure_severity,
                "failure_retryability": failure_retryability,
                "failure_next_action": _fetch_failure_next_action_label(
                    failure_retryability=failure_retryability,
                    failure_severity=failure_severity,
                    has_html_snapshot=has_html_snapshot,
                ),
                "has_html_snapshot": has_html_snapshot,
                "html_snapshot_availability": _snapshot_availability_label(has_html_snapshot),
                "html_snapshot_path": snapshot.get("html_snapshot_path"),
            }
        )
    return failure_sources


def _build_fetch_failure_artifacts_for_source(
    failure_source: dict[str, object],
) -> list[dict[str, object]]:
    artifacts: list[dict[str, object]] = []
    source_id = str(failure_source["source_id"])
    category_label = str(failure_source.get("category_label") or source_id)
    source_url = str(failure_source.get("source_url") or source_id)
    status_code = failure_source.get("status_code")
    status_text = f"HTTP {status_code}" if status_code is not None else "no HTTP status"

    metadata_artifact = _artifact_download_record(
        relative_path=f"data/fetch_failures/{source_id}.json",
        label=f"Fetch failure metadata - {category_label}",
        description=(
            f"Structured staged fetch-failure metadata for {source_url} "
            f"({status_text}). This remains staged provenance."
        ),
        artifact_format="json",
    )
    if metadata_artifact is not None:
        artifacts.append(metadata_artifact)

    html_snapshot_path = str(failure_source.get("html_snapshot_path") or "")
    if html_snapshot_path:
        html_artifact = _artifact_download_record(
            relative_path=html_snapshot_path,
            label=f"Fetch failure HTML snapshot - {category_label}",
            description=(
                f"Captured staged response body for the fetch failure on {source_url} "
                f"({status_text}). This remains staged provenance."
            ),
            artifact_format="html",
        )
        if html_artifact is not None:
            artifacts.append(html_artifact)
    return artifacts


def _build_downloadable_fetch_failure_artifacts(
    fetch_failure_sources: list[dict[str, object]],
) -> list[dict[str, object]]:
    artifacts: list[dict[str, object]] = []
    for failure_source in fetch_failure_sources:
        artifacts.extend(_build_fetch_failure_artifacts_for_source(failure_source))
    return artifacts


def _summarize_fetch_failure_artifact_links(
    artifact_links: list[dict[str, object]],
) -> dict[str, object]:
    artifact_count = len(artifact_links)
    artifact_formats = sorted({str(artifact.get("format") or "").lower() for artifact in artifact_links if str(artifact.get("format") or "")})
    artifact_summary_parts = [f"{artifact_count} artifact" + ("" if artifact_count == 1 else "s")]
    if artifact_formats:
        artifact_summary_parts.append(", ".join(artifact_format.upper() for artifact_format in artifact_formats))
    return {
        "artifact_count": artifact_count,
        "artifact_formats": artifact_formats,
        "artifact_summary": " · ".join(artifact_summary_parts),
    }


def _count_fetch_failure_artifact_formats(
    artifact_links: list[dict[str, object]],
) -> dict[str, int]:
    format_counts: dict[str, int] = {}
    for artifact in artifact_links:
        artifact_format = str(artifact.get("format") or "").lower()
        if not artifact_format:
            continue
        format_counts[artifact_format] = format_counts.get(artifact_format, 0) + 1
    return {artifact_format: format_counts[artifact_format] for artifact_format in sorted(format_counts)}


def _summarize_fetch_failure_artifact_format_counts(
    artifact_format_counts: dict[str, int],
) -> str:
    if not artifact_format_counts:
        return "No staged fetch-failure artifact format counts"
    return ", ".join(
        f"{artifact_format.upper()}: {int(count)}"
        for artifact_format, count in artifact_format_counts.items()
    )


def _build_fetch_failure_next_action_artifact_format_source_lists(
    sources: list[dict[str, object]],
) -> list[dict[str, object]]:
    grouped_sources_by_format: dict[str, list[dict[str, object]]] = {}
    for source in sources:
        source_id = str(source.get("source_id") or "")
        source_summary = {
            "source_id": source_id,
            "source_label": str(source.get("source_label") or source_id or "unknown"),
            "source_url": str(source.get("source_url") or ""),
        }
        source_formats = sorted(
            {
                str(artifact.get("format") or "").lower()
                for artifact in source.get("artifact_links", [])
                if str(artifact.get("format") or "")
            }
        )
        for artifact_format in source_formats:
            grouped_sources_by_format.setdefault(artifact_format, []).append(source_summary)

    artifact_format_source_lists: list[dict[str, object]] = []
    for artifact_format in sorted(grouped_sources_by_format):
        format_sources = sorted(
            grouped_sources_by_format[artifact_format],
            key=lambda source: (
                source["source_label"],
                source["source_url"],
                source["source_id"],
            ),
        )
        artifact_format_source_lists.append(
            {
                "format": artifact_format,
                "source_count": len(format_sources),
                "sources": format_sources,
            }
        )
    return artifact_format_source_lists


def _count_fetch_failure_artifact_format_sources(
    artifact_format_source_lists: list[dict[str, object]],
) -> dict[str, int]:
    return {
        str(format_group.get("format") or "").lower(): int(format_group.get("source_count") or 0)
        for format_group in artifact_format_source_lists
        if str(format_group.get("format") or "")
    }


def _summarize_fetch_failure_artifact_format_source_counts(
    artifact_format_source_counts: dict[str, int],
) -> str:
    if not artifact_format_source_counts:
        return "No staged fetch-failure artifact-format source counts"
    return ", ".join(
        f"{artifact_format.upper()}: {int(source_count)} source"
        + ("" if int(source_count) == 1 else "s")
        for artifact_format, source_count in artifact_format_source_counts.items()
    )


def _summarize_fetch_failure_artifact_format_source_count_total(
    artifact_format_source_counts: dict[str, int],
) -> dict[str, object]:
    source_count_total = sum(int(source_count) for source_count in artifact_format_source_counts.values())
    return {
        "artifact_format_source_count_total": source_count_total,
        "artifact_format_source_count_total_summary": (
            f"{source_count_total} format-source entry"
            if source_count_total == 1
            else f"{source_count_total} format-source entries"
        ),
    }


def _summarize_fetch_failure_artifact_format_distinct_count(
    artifact_format_counts: dict[str, int],
) -> dict[str, object]:
    distinct_format_count = len(artifact_format_counts)
    return {
        "artifact_format_distinct_count": distinct_format_count,
        "artifact_format_distinct_count_summary": (
            f"{distinct_format_count} distinct artifact format"
            if distinct_format_count == 1
            else f"{distinct_format_count} distinct artifact formats"
        ),
    }


def _build_fetch_failure_next_action_artifact_rollups(
    fetch_failure_next_action_source_lists: list[dict[str, object]],
) -> list[dict[str, object]]:
    rollups: list[dict[str, object]] = []
    for action_group in fetch_failure_next_action_source_lists:
        sources = action_group.get("sources", [])
        artifact_links = [
            artifact_link
            for source in sources
            for artifact_link in source.get("artifact_links", [])
            if isinstance(artifact_link, dict)
        ]
        artifact_summary = _summarize_fetch_failure_artifact_links(artifact_links)
        artifact_format_counts = _count_fetch_failure_artifact_formats(artifact_links)
        artifact_format_source_lists = _build_fetch_failure_next_action_artifact_format_source_lists(
            sources
        )
        artifact_format_source_counts = _count_fetch_failure_artifact_format_sources(
            artifact_format_source_lists
        )
        artifact_format_source_count_total = _summarize_fetch_failure_artifact_format_source_count_total(
            artifact_format_source_counts
        )
        artifact_format_distinct_count = _summarize_fetch_failure_artifact_format_distinct_count(
            artifact_format_counts
        )
        rollups.append(
            {
                "failure_next_action": str(action_group.get("failure_next_action") or "unknown"),
                "source_count": int(action_group.get("source_count") or len(sources)),
                "artifact_count": int(artifact_summary["artifact_count"]),
                "artifact_formats": artifact_summary["artifact_formats"],
                "artifact_summary": str(artifact_summary["artifact_summary"]),
                "artifact_format_counts": artifact_format_counts,
                "artifact_format_count_summary": _summarize_fetch_failure_artifact_format_counts(
                    artifact_format_counts
                ),
                "artifact_format_source_lists": artifact_format_source_lists,
                "artifact_format_source_counts": artifact_format_source_counts,
                "artifact_format_source_count_summary": _summarize_fetch_failure_artifact_format_source_counts(
                    artifact_format_source_counts
                ),
                "artifact_format_source_count_total": int(
                    artifact_format_source_count_total["artifact_format_source_count_total"]
                ),
                "artifact_format_source_count_total_summary": str(
                    artifact_format_source_count_total["artifact_format_source_count_total_summary"]
                ),
                "artifact_format_distinct_count": int(
                    artifact_format_distinct_count["artifact_format_distinct_count"]
                ),
                "artifact_format_distinct_count_summary": str(
                    artifact_format_distinct_count["artifact_format_distinct_count_summary"]
                ),
            }
        )
    return rollups


def _group_fetch_failure_sources_by_next_action(
    fetch_failure_sources: list[dict[str, object]],
) -> list[dict[str, object]]:
    grouped_sources: dict[str, list[dict[str, object]]] = {}
    for failure_source in fetch_failure_sources:
        failure_next_action = str(failure_source.get("failure_next_action") or "unknown")
        source_id = str(failure_source.get("source_id") or "")
        recorded_at = str(failure_source.get("recorded_at") or "")
        status_code = failure_source.get("status_code")
        status_code_text = f"HTTP {status_code}" if status_code not in (None, "") else "HTTP n/a"
        error_type = str(failure_source.get("error_type") or "unknown")
        failure_severity = str(failure_source.get("failure_severity") or "unknown")
        failure_retryability = str(failure_source.get("failure_retryability") or "unknown")
        failure_context_parts = [status_code_text, error_type, failure_severity, failure_retryability]
        if recorded_at:
            failure_context_parts.insert(0, recorded_at)
        artifact_links = [
            {
                "label": str(artifact.get("label") or ""),
                "path": str(artifact.get("path") or ""),
                "site_path": str(artifact.get("site_path") or ""),
                "format": str(artifact.get("format") or ""),
            }
            for artifact in _build_fetch_failure_artifacts_for_source(failure_source)
        ]
        artifact_summary = _summarize_fetch_failure_artifact_links(artifact_links)
        grouped_sources.setdefault(failure_next_action, []).append(
            {
                "source_id": source_id,
                "source_label": str(failure_source.get("source_label") or source_id or "unknown"),
                "source_url": str(failure_source.get("source_url") or ""),
                "recorded_at": recorded_at,
                "status_code": status_code_text,
                "error_type": error_type,
                "failure_severity": failure_severity,
                "failure_retryability": failure_retryability,
                "failure_context_summary": " · ".join(failure_context_parts),
                "artifact_links": artifact_links,
                "artifact_count": int(artifact_summary["artifact_count"]),
                "artifact_formats": artifact_summary["artifact_formats"],
                "artifact_summary": str(artifact_summary["artifact_summary"]),
            }
        )

    grouped_rows: list[dict[str, object]] = []
    for failure_next_action in sorted(grouped_sources):
        sources = sorted(
            grouped_sources[failure_next_action],
            key=lambda source: (
                source["source_label"],
                source["source_url"],
                source["source_id"],
            ),
        )
        grouped_rows.append(
            {
                "failure_next_action": failure_next_action,
                "source_count": len(sources),
                "sources": sources,
            }
        )
    return grouped_rows


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
        "fetch_failure_source_count": None,
        "fetch_failure_error_type_counts": None,
        "fetch_failure_earliest_recorded_at": None,
        "fetch_failure_latest_recorded_at": None,
        "fetch_failure_robots_policy_counts": None,
        "fetch_failure_robots_status_code_counts": None,
        "fetch_failure_effective_delay_seconds_counts": None,
        "fetch_failure_min_effective_delay_seconds": None,
        "fetch_failure_max_effective_delay_seconds": None,
        "fetch_failure_source_label_counts": None,
        "fetch_failure_source_group_counts": None,
        "fetch_failure_parser_strategy_counts": None,
        "fetch_failure_html_snapshot_availability_counts": None,
        "fetch_failure_severity_counts": None,
        "fetch_failure_retryability_counts": None,
        "fetch_failure_next_action_counts": None,
        "fetch_failure_status_code_counts": None,
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
        "downloadable_fetch_failure_artifacts": [],
        "fetch_failure_sources": [],
        "fetch_failure_next_action_source_lists": [],
        "fetch_failure_next_action_artifact_rollups": [],
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
    fetch_failure_sources = _load_fetch_failure_sources(selected_sources_by_id)
    fetch_failure_next_action_source_lists = _group_fetch_failure_sources_by_next_action(
        fetch_failure_sources
    )
    fetch_failure_next_action_artifact_rollups = _build_fetch_failure_next_action_artifact_rollups(
        fetch_failure_next_action_source_lists
    )
    downloadable_fetch_failure_artifacts = _build_downloadable_fetch_failure_artifacts(fetch_failure_sources)
    fetch_failure_earliest_recorded_at, fetch_failure_latest_recorded_at = _timestamp_bounds(
        fetch_failure_sources,
        "recorded_at",
    )
    (
        fetch_failure_min_effective_delay_seconds,
        fetch_failure_max_effective_delay_seconds,
    ) = _float_bounds(
        fetch_failure_sources,
        "robots_effective_delay_seconds",
    )
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
            "fetch_failure_source_count": len(fetch_failure_sources),
            "fetch_failure_error_type_counts": _count_values(
                fetch_failure_sources,
                "error_type",
                none_label="unknown",
            ),
            "fetch_failure_earliest_recorded_at": fetch_failure_earliest_recorded_at,
            "fetch_failure_latest_recorded_at": fetch_failure_latest_recorded_at,
            "fetch_failure_robots_policy_counts": _count_values(
                fetch_failure_sources,
                "robots_policy",
                none_label="unknown",
            ),
            "fetch_failure_robots_status_code_counts": _count_values(
                fetch_failure_sources,
                "robots_status_code",
                none_label="n/a",
            ),
            "fetch_failure_effective_delay_seconds_counts": _count_values(
                fetch_failure_sources,
                "robots_effective_delay_seconds",
                none_label="n/a",
            ),
            "fetch_failure_min_effective_delay_seconds": fetch_failure_min_effective_delay_seconds,
            "fetch_failure_max_effective_delay_seconds": fetch_failure_max_effective_delay_seconds,
            "fetch_failure_source_label_counts": _count_values(
                fetch_failure_sources,
                "source_label",
                none_label="unknown",
            ),
            "fetch_failure_source_group_counts": _count_values(
                fetch_failure_sources,
                "source_group",
                none_label="unknown",
            ),
            "fetch_failure_parser_strategy_counts": _count_values(
                fetch_failure_sources,
                "parser_strategy",
                none_label="unknown",
            ),
            "fetch_failure_html_snapshot_availability_counts": _count_values(
                fetch_failure_sources,
                "html_snapshot_availability",
                none_label="unknown",
            ),
            "fetch_failure_severity_counts": _count_values(
                fetch_failure_sources,
                "failure_severity",
                none_label="unknown",
            ),
            "fetch_failure_retryability_counts": _count_values(
                fetch_failure_sources,
                "failure_retryability",
                none_label="unknown",
            ),
            "fetch_failure_next_action_counts": _count_values(
                fetch_failure_sources,
                "failure_next_action",
                none_label="unknown",
            ),
            "fetch_failure_status_code_counts": _count_values(
                fetch_failure_sources,
                "status_code",
                none_label="n/a",
            ),
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
            "downloadable_fetch_failure_artifacts": downloadable_fetch_failure_artifacts,
            "fetch_failure_sources": fetch_failure_sources,
            "fetch_failure_next_action_source_lists": fetch_failure_next_action_source_lists,
            "fetch_failure_next_action_artifact_rollups": fetch_failure_next_action_artifact_rollups,
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
            "fetch_failure_source_count": source_pipeline_diagnostics_report["fetch_failure_source_count"],
            "fetch_failure_error_type_counts": source_pipeline_diagnostics_report["fetch_failure_error_type_counts"],
            "fetch_failure_earliest_recorded_at": source_pipeline_diagnostics_report[
                "fetch_failure_earliest_recorded_at"
            ],
            "fetch_failure_latest_recorded_at": source_pipeline_diagnostics_report[
                "fetch_failure_latest_recorded_at"
            ],
            "fetch_failure_robots_policy_counts": source_pipeline_diagnostics_report[
                "fetch_failure_robots_policy_counts"
            ],
            "fetch_failure_robots_status_code_counts": source_pipeline_diagnostics_report[
                "fetch_failure_robots_status_code_counts"
            ],
            "fetch_failure_effective_delay_seconds_counts": source_pipeline_diagnostics_report[
                "fetch_failure_effective_delay_seconds_counts"
            ],
            "fetch_failure_min_effective_delay_seconds": source_pipeline_diagnostics_report[
                "fetch_failure_min_effective_delay_seconds"
            ],
            "fetch_failure_max_effective_delay_seconds": source_pipeline_diagnostics_report[
                "fetch_failure_max_effective_delay_seconds"
            ],
            "fetch_failure_source_label_counts": source_pipeline_diagnostics_report[
                "fetch_failure_source_label_counts"
            ],
            "fetch_failure_source_group_counts": source_pipeline_diagnostics_report[
                "fetch_failure_source_group_counts"
            ],
            "fetch_failure_parser_strategy_counts": source_pipeline_diagnostics_report[
                "fetch_failure_parser_strategy_counts"
            ],
            "fetch_failure_html_snapshot_availability_counts": source_pipeline_diagnostics_report[
                "fetch_failure_html_snapshot_availability_counts"
            ],
            "fetch_failure_severity_counts": source_pipeline_diagnostics_report[
                "fetch_failure_severity_counts"
            ],
            "fetch_failure_retryability_counts": source_pipeline_diagnostics_report[
                "fetch_failure_retryability_counts"
            ],
            "fetch_failure_next_action_counts": source_pipeline_diagnostics_report[
                "fetch_failure_next_action_counts"
            ],
            "fetch_failure_status_code_counts": source_pipeline_diagnostics_report["fetch_failure_status_code_counts"],
            "fetch_failure_next_action_source_lists": source_pipeline_diagnostics_report[
                "fetch_failure_next_action_source_lists"
            ],
            "fetch_failure_next_action_artifact_rollups": source_pipeline_diagnostics_report[
                "fetch_failure_next_action_artifact_rollups"
            ],
            "detail_parse_failure_source_count": source_pipeline_diagnostics_report["detail_parse_failure_source_count"],
            "detail_parse_status_counts": source_pipeline_diagnostics_report["detail_parse_status_counts"],
            "detail_field_population_counts": source_pipeline_diagnostics_report["detail_field_population_counts"],
            "downloadable_fetch_failure_artifacts": source_pipeline_diagnostics_report[
                "downloadable_fetch_failure_artifacts"
            ],
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
