from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from src.config import load_source_registry
from src.fancymmr_build.config import MIN_REVENUE_30D, PROMOTED_PUBLICATION_DATASET
from src.fancymmr_build.publication import (
    PUBLICATION_DATASET_COLUMNS,
    default_publication_input_payload,
    write_publication_input_payload,
)
from src.normalize import detail_slug_from_path, revenue_band_for_value, slugify_startup_name


PUBLIC_SOURCE_REGISTRY = "data/public_source_pages.csv"
STAGED_VISIBLE_ROWS = "data/source_pipeline/processed/visible_sample_rows.csv"
STAGED_OVERRIDE_REPORT = "data/source_pipeline/processed/heuristic_override_report.json"
STAGED_DUPLICATES_REPORT = "data/source_pipeline/processed/suspicious_duplicates.json"
STAGED_VALIDATION_REPORT = "data/source_pipeline/processed/validation_report.json"
STAGED_RUN_MANIFEST = "data/source_pipeline/snapshots/run_manifest.json"


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PUBLICATION_DATASET_COLUMNS, extrasaction="raise")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _humanize_slug(value: str) -> str:
    replacements = {
        "ai": "AI",
        "api": "API",
        "b2b": "B2B",
        "b2c": "B2C",
        "gmv": "GMV",
        "hr": "HR",
        "ios": "iOS",
        "llc": "LLC",
        "saas": "SaaS",
        "seo": "SEO",
        "ui": "UI",
        "ux": "UX",
    }
    tokens = [token for token in value.split("-") if token]
    if not tokens:
        return value
    return " ".join(replacements.get(token.casefold(), token.title()) for token in tokens)


def _merged_publication_name(base_name: str, humanized_slug: str) -> str:
    if humanized_slug.casefold() == base_name.casefold():
        return base_name
    base_prefix = f"{base_name.casefold()} "
    if humanized_slug.casefold().startswith(base_prefix):
        suffix = humanized_slug[len(base_name) :].strip()
        if suffix:
            return f"{base_name} {suffix}" if suffix.isdigit() else f"{base_name} ({suffix})"
    return humanized_slug


def _publication_name_candidates(row: dict[str, str]) -> list[str]:
    base_name = row["name"].strip()
    candidates = [base_name]
    base_slug = slugify_startup_name(base_name)

    canonical_slug = str(row.get("canonical_slug") or "").strip()
    if canonical_slug and canonical_slug != base_slug:
        candidates.append(_merged_publication_name(base_name, _humanize_slug(canonical_slug)))

    detail_slug = detail_slug_from_path(str(row.get("detail_path") or ""))
    if detail_slug and detail_slug != base_slug:
        candidates.append(_merged_publication_name(base_name, _humanize_slug(detail_slug)))

    position = str(row.get("position") or "").strip()
    if position:
        candidates.append(f"{base_name} #{position}")

    seen: set[str] = set()
    unique_candidates: list[str] = []
    for candidate in candidates:
        normalized = candidate.casefold()
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_candidates.append(candidate)
    return unique_candidates


def _resolve_publication_names(rows: list[dict[str, str]]) -> list[str]:
    groups: dict[tuple[str, str], list[int]] = {}
    for index, row in enumerate(rows):
        groups.setdefault((row["name"].strip(), row["source_url"].strip()), []).append(index)

    resolved_names = [row["name"].strip() for row in rows]
    for (_, _), indexes in groups.items():
        if len(indexes) <= 1:
            continue

        used_in_group: set[str] = set()
        for index in indexes:
            for candidate in _publication_name_candidates(rows[index]):
                normalized = candidate.casefold()
                if normalized in used_in_group:
                    continue
                resolved_names[index] = candidate
                used_in_group.add(normalized)
                break
            else:
                raise ValueError(
                    "Could not disambiguate duplicate publication names for "
                    f"{rows[index]['name']!r} from {rows[index]['source_url']!r}."
                )

    return resolved_names


def _project_publication_rows(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    if not rows:
        raise ValueError("No staged visible rows were found to promote.")

    missing_columns = [field for field in PUBLICATION_DATASET_COLUMNS if field not in rows[0]]
    if missing_columns:
        raise ValueError(f"Staged visible rows are missing required publication columns: {missing_columns}")

    resolved_names = _resolve_publication_names(rows)
    projected_rows: list[dict[str, object]] = []
    for row, resolved_name in zip(rows, resolved_names):
        revenue_30d = int(row["revenue_30d"])
        revenue_band = row.get("revenue_band") or revenue_band_for_value(revenue_30d)
        if revenue_band is None:
            raise ValueError(f"Visible row {row['name']!r} is below the publication threshold.")

        projected_row = {
            "name": resolved_name,
            "category": row["category"].strip(),
            "revenue_30d": revenue_30d,
            "biz_model": row["biz_model"].strip(),
            "gtm_model": row["gtm_model"].strip(),
            "source_url": row["source_url"].strip(),
            "revenue_band": revenue_band,
        }
        missing_values = [
            field
            for field, value in projected_row.items()
            if field not in {"revenue_30d", "biz_model", "gtm_model"} and not value
        ]
        if missing_values:
            raise ValueError(
                f"Visible row {row['name']!r} is missing required publication values: {missing_values}"
            )
        projected_rows.append(projected_row)

    duplicate_pairs: dict[tuple[str, str], int] = {}
    for row in projected_rows:
        key = (str(row["name"]), str(row["source_url"]))
        duplicate_pairs[key] = duplicate_pairs.get(key, 0) + 1
    repeated = [key for key, count in duplicate_pairs.items() if count > 1]
    if repeated:
        raise ValueError(
            "Projected publication rows still contain duplicate `(name, source_url)` pairs after disambiguation: "
            + ", ".join(f"{name!r} @ {source_url}" for name, source_url in repeated)
        )

    projected_rows.sort(
        key=lambda row: (
            -int(row["revenue_30d"]),
            str(row["name"]).casefold(),
            str(row["source_url"]).casefold(),
        )
    )
    return projected_rows


def _ensure_projected_rows_complete(projected_rows: list[dict[str, object]]) -> None:
    for row in projected_rows:
        missing_values = [field for field, value in row.items() if field != "revenue_30d" and not value]
        if missing_values:
            raise ValueError(
                f"Visible row {row['name']!r} is missing required publication values after promotion gating: "
                f"{missing_values}"
            )


def _build_gate_summary(
    *,
    expected_source_count: int,
    validation_report: dict[str, object],
    override_report: dict[str, object],
    duplicates_report: dict[str, object],
    run_manifest: dict[str, object],
    visible_rows: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "expected_source_count": expected_source_count,
        "validation_status": validation_report["status"],
        "run_manifest_validation_status": run_manifest["validation_status"],
        "visible_sample_row_count": len(visible_rows),
        "fully_mapped_visible_row_count": override_report["fully_mapped_visible_row_count"],
        "unmapped_visible_row_count": override_report["unmapped_visible_row_count"],
        "suspicious_duplicate_group_count": duplicates_report["group_count"],
        "selected_source_count": run_manifest["selected_source_count"],
    }


def ensure_promotion_ready(
    *,
    expected_source_count: int,
    validation_report: dict[str, object],
    override_report: dict[str, object],
    duplicates_report: dict[str, object],
    run_manifest: dict[str, object],
    visible_rows: list[dict[str, object]],
) -> None:
    problems: list[str] = []
    if validation_report.get("status") != "passed":
        problems.append(f"staged validation status is {validation_report.get('status')!r}, expected 'passed'")
    if run_manifest.get("validation_status") != "passed":
        problems.append(
            f"staged run manifest validation status is {run_manifest.get('validation_status')!r}, expected 'passed'"
        )
    if int(override_report.get("unmapped_visible_row_count", 0)) != 0:
        problems.append("staged override report still has unmapped visible rows")
    if int(duplicates_report.get("group_count", 0)) != 0:
        problems.append("staged suspicious duplicate groups are non-zero")
    if int(run_manifest.get("visible_sample_row_count", -1)) != len(visible_rows):
        problems.append("staged run manifest visible row count does not match the staged visible rows CSV")
    if int(validation_report.get("visible_sample_row_count", -1)) != len(visible_rows):
        problems.append("staged validation report visible row count does not match the staged visible rows CSV")
    if int(override_report.get("visible_sample_row_count", -1)) != len(visible_rows):
        problems.append("staged override report visible row count does not match the staged visible rows CSV")
    if int(run_manifest.get("selected_source_count", -1)) != expected_source_count:
        problems.append(
            "staged run does not cover the full registered source set "
            f"({run_manifest.get('selected_source_count')} of {expected_source_count})"
        )
    if any(int(row["revenue_30d"]) < MIN_REVENUE_30D for row in visible_rows):
        problems.append("staged visible rows include revenue values below the visible-sample threshold")
    if not visible_rows:
        problems.append("staged visible rows CSV is empty")

    if problems:
        raise ValueError("Promotion gates failed: " + "; ".join(problems))


def promote_live_bundle(*, root: Path = ROOT, dry_run: bool = False) -> dict[str, object]:
    staged_visible_rows_path = root / STAGED_VISIBLE_ROWS
    staged_override_report_path = root / STAGED_OVERRIDE_REPORT
    staged_duplicates_report_path = root / STAGED_DUPLICATES_REPORT
    staged_validation_report_path = root / STAGED_VALIDATION_REPORT
    staged_run_manifest_path = root / STAGED_RUN_MANIFEST
    source_registry_path = root / PUBLIC_SOURCE_REGISTRY
    expected_source_count = len(
        load_source_registry(
            public_source_pages_csv=source_registry_path,
            source_coverage_report_json=root / "data" / "source_coverage_report.json",
        )
    )

    staged_rows = _read_csv_rows(staged_visible_rows_path)
    projected_rows = _project_publication_rows(staged_rows)
    override_report = _read_json(staged_override_report_path)
    duplicates_report = _read_json(staged_duplicates_report_path)
    validation_report = _read_json(staged_validation_report_path)
    run_manifest = _read_json(staged_run_manifest_path)

    ensure_promotion_ready(
        expected_source_count=expected_source_count,
        validation_report=validation_report,
        override_report=override_report,
        duplicates_report=duplicates_report,
        run_manifest=run_manifest,
        visible_rows=projected_rows,
    )
    _ensure_projected_rows_complete(projected_rows)

    promotion_gate_summary = _build_gate_summary(
        expected_source_count=expected_source_count,
        validation_report=validation_report,
        override_report=override_report,
        duplicates_report=duplicates_report,
        run_manifest=run_manifest,
        visible_rows=projected_rows,
    )
    manifest_payload = default_publication_input_payload()
    manifest_payload.update(
        {
            "dataset_path": PROMOTED_PUBLICATION_DATASET,
            "dataset_kind": "source_pipeline_promotion",
            "source_label": "Promoted from staged source-pipeline visible rows",
            "expected_source_count": expected_source_count,
            "selected_source_count": int(run_manifest["selected_source_count"]),
            "promoted_from_visible_rows_path": STAGED_VISIBLE_ROWS,
            "source_pipeline_validation_report_path": STAGED_VALIDATION_REPORT,
            "source_pipeline_override_report_path": STAGED_OVERRIDE_REPORT,
            "source_pipeline_duplicates_report_path": STAGED_DUPLICATES_REPORT,
            "source_pipeline_run_manifest_path": STAGED_RUN_MANIFEST,
            "promoted_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "promotion_gate_summary": promotion_gate_summary,
            "staged_visible_rows_sha256": _sha256(staged_visible_rows_path),
        }
    )

    target_dataset_path = root / PROMOTED_PUBLICATION_DATASET
    if not dry_run:
        _write_csv_rows(target_dataset_path, projected_rows)
        manifest_payload["promoted_dataset_sha256"] = _sha256(target_dataset_path)
        write_publication_input_payload(root, manifest_payload)

    return {
        "status": "dry_run_ready" if dry_run else "promoted",
        "publication_input_manifest_path": "data/publication_input.json",
        "source_registry_path": PUBLIC_SOURCE_REGISTRY,
        "expected_source_count": expected_source_count,
        "dataset_path": PROMOTED_PUBLICATION_DATASET,
        "visible_sample_row_count": len(projected_rows),
        "selected_source_count": int(run_manifest["selected_source_count"]),
        "promotion_gate_summary": promotion_gate_summary,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Promote staged live source-pipeline visible rows into the published analytics input bundle."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT,
        help="Repository root to promote within. Defaults to the current repo root.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate the staged promotion gates and print the promotion summary without writing files.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        summary = promote_live_bundle(root=args.root.resolve(), dry_run=args.dry_run)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1) from error
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
