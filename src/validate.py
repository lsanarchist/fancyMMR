from __future__ import annotations

import json
from pathlib import Path

from .fancymmr_build.config import MIN_REVENUE_30D


REQUIRED_NORMALIZED_FIELDS = (
    "name",
    "canonical_slug",
    "category",
    "revenue_30d",
    "source_url",
    "scraped_at",
    "detail_url",
)


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


def _status_from_checks(checks: list[dict[str, object]]) -> str:
    has_errors = any(check["severity"] == "error" and not check["passed"] for check in checks)
    if has_errors:
        return "failed"

    has_warnings = any(check["severity"] == "warning" and not check["passed"] for check in checks)
    if has_warnings:
        return "passed_with_warnings"

    return "passed"


def _duplicate_count(values: list[object]) -> int:
    seen: set[object] = set()
    duplicates = 0
    for value in values:
        if value in seen:
            duplicates += 1
            continue
        seen.add(value)
    return duplicates


def build_suspicious_duplicates_report(normalized_rows: list[dict[str, object]]) -> dict[str, object]:
    groups_by_slug: dict[str, list[dict[str, object]]] = {}
    visible_rows = [row for row in normalized_rows if row.get("included_in_visible_sample")]
    for row in visible_rows:
        canonical_slug = str(row.get("canonical_slug") or "")
        if not canonical_slug:
            continue
        groups_by_slug.setdefault(canonical_slug, []).append(row)

    groups = []
    for canonical_slug, rows in groups_by_slug.items():
        detail_urls = sorted({str(row["detail_url"]) for row in rows if row.get("detail_url")})
        if len(detail_urls) <= 1:
            continue
        groups.append(
            {
                "canonical_slug": canonical_slug,
                "row_count": len(rows),
                "visible_sample_row_count": sum(1 for row in rows if row.get("included_in_visible_sample")),
                "names": sorted({str(row["name"]) for row in rows if row.get("name")}),
                "source_urls": sorted({str(row["source_url"]) for row in rows if row.get("source_url")}),
                "detail_urls": detail_urls,
            }
        )

    groups.sort(key=lambda group: (-group["visible_sample_row_count"], -group["row_count"], group["canonical_slug"]))
    return {
        "group_count": len(groups),
        "row_count": sum(group["row_count"] for group in groups),
        "groups": groups,
    }


def validate_normalized_rows(
    normalized_rows: list[dict[str, object]],
    *,
    suspicious_duplicates_report: dict[str, object] | None = None,
) -> dict[str, object]:
    visible_rows = [row for row in normalized_rows if row.get("included_in_visible_sample")]
    if suspicious_duplicates_report is None:
        suspicious_duplicates_report = build_suspicious_duplicates_report(normalized_rows)
    missing_required_field_counts = {
        field: sum(1 for row in normalized_rows if row.get(field) in (None, ""))
        for field in REQUIRED_NORMALIZED_FIELDS
    }
    duplicate_detail_url_count = _duplicate_count(
        [row["detail_url"] for row in normalized_rows if row.get("detail_url")]
    )
    duplicate_name_source_url_count = _duplicate_count(
        [
            (row["name"], row["source_url"], row["canonical_slug"])
            for row in visible_rows
            if row.get("name") not in (None, "")
            and row.get("source_url") not in (None, "")
            and row.get("canonical_slug") not in (None, "")
        ]
    )
    below_threshold_visible_count = sum(
        1 for row in visible_rows if int(row["revenue_30d"]) < MIN_REVENUE_30D
    )
    excluded_below_threshold_count = sum(
        1 for row in normalized_rows if int(row["revenue_30d"]) < MIN_REVENUE_30D
    )
    min_visible_revenue_30d = (
        min(int(row["revenue_30d"]) for row in visible_rows) if visible_rows else None
    )
    missing_biz_model_count = sum(1 for row in visible_rows if not row.get("biz_model"))
    missing_gtm_model_count = sum(1 for row in visible_rows if not row.get("gtm_model"))

    checks = [
        _check(
            check_id="parsed_rows_present",
            severity="error",
            passed=bool(normalized_rows),
            message="At least one startup card was parsed from the selected sources.",
            details={"parsed_row_count": len(normalized_rows)},
        ),
        _check(
            check_id="visible_rows_present",
            severity="error",
            passed=bool(visible_rows),
            message="At least one parsed startup remains after the visible-sample threshold filter.",
            details={"visible_sample_row_count": len(visible_rows)},
        ),
        _check(
            check_id="required_fields_present",
            severity="error",
            passed=all(count == 0 for count in missing_required_field_counts.values()),
            message="Every normalized row keeps the required provenance and core metric fields.",
            details={"missing_required_field_counts": missing_required_field_counts},
        ),
        _check(
            check_id="visible_threshold_respected",
            severity="error",
            passed=below_threshold_visible_count == 0,
            message="No included visible-sample row falls below the $5,000 threshold.",
            details={
                "threshold_usd": MIN_REVENUE_30D,
                "below_threshold_visible_count": below_threshold_visible_count,
                "min_visible_revenue_30d": min_visible_revenue_30d,
            },
        ),
        _check(
            check_id="detail_urls_unique",
            severity="error",
            passed=duplicate_detail_url_count == 0,
            message="Parsed startup detail URLs are unique within the staged run.",
            details={"duplicate_detail_url_count": duplicate_detail_url_count},
        ),
        _check(
            check_id="visible_name_source_pairs_unique",
            severity="warning",
            passed=duplicate_name_source_url_count == 0,
            message="Visible-sample duplicate `(name, source_url)` pairs are surfaced as warnings only when they still collapse onto the same canonical slug after same-source disambiguation.",
            details={"duplicate_name_source_url_count": duplicate_name_source_url_count},
        ),
        _check(
            check_id="heuristic_override_completeness",
            severity="warning",
            passed=missing_biz_model_count == 0 and missing_gtm_model_count == 0,
            message="Heuristic overrides are tracked explicitly and surfaced as warnings when rows remain unmapped.",
            details={
                "missing_biz_model_count": missing_biz_model_count,
                "missing_gtm_model_count": missing_gtm_model_count,
            },
        ),
        _check(
            check_id="suspicious_canonical_slug_duplicates",
            severity="warning",
            passed=suspicious_duplicates_report["group_count"] == 0,
            message="Canonical-slug collisions across multiple detail URLs are surfaced as suspicious duplicate candidates.",
            details={
                "suspicious_duplicate_group_count": suspicious_duplicates_report["group_count"],
                "suspicious_duplicate_row_count": suspicious_duplicates_report["row_count"],
            },
        ),
    ]

    return {
        "status": _status_from_checks(checks),
        "parsed_row_count": len(normalized_rows),
        "visible_sample_row_count": len(visible_rows),
        "excluded_below_threshold_count": excluded_below_threshold_count,
        "threshold_usd": MIN_REVENUE_30D,
        "missing_required_field_counts": missing_required_field_counts,
        "missing_biz_model_count": missing_biz_model_count,
        "missing_gtm_model_count": missing_gtm_model_count,
        "duplicate_detail_url_count": duplicate_detail_url_count,
        "duplicate_name_source_url_count": duplicate_name_source_url_count,
        "suspicious_duplicate_group_count": suspicious_duplicates_report["group_count"],
        "suspicious_duplicate_row_count": suspicious_duplicates_report["row_count"],
        "min_visible_revenue_30d": min_visible_revenue_30d,
        "checks": checks,
    }


def ensure_validation_passes(report: dict[str, object]) -> None:
    failed_error_checks = [
        check["id"]
        for check in report["checks"]
        if check["severity"] == "error" and not check["passed"]
    ]
    if failed_error_checks:
        raise ValueError(f"Pipeline validation failed: {', '.join(failed_error_checks)}")


def write_validation_report(path: Path, report: dict[str, object]) -> None:
    path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
