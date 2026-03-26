from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import sys
from typing import Callable


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from src.config import DEFAULT_FETCH_PATHS, FetchPaths, SourceConfig
from src.fetch import FetchResult, fetch_source, select_sources
from src.normalize import (
    NORMALIZED_ROW_FIELDS,
    VISIBLE_SAMPLE_ROW_FIELDS,
    build_heuristic_override_report,
    build_visible_sample_rows,
    dedupe_normalized_rows,
    detail_slug_from_path,
    normalize_parsed_cards,
)
from src.parse import (
    DETAIL_PAGE_PARSER_STRATEGY,
    ParsedStartupCard,
    parse_source_html,
    parse_startup_detail_html,
    parsed_cards_as_dicts,
    parsed_detail_as_dict,
)
from src.validate import (
    build_suspicious_duplicates_report,
    ensure_validation_passes,
    validate_normalized_rows,
    write_validation_report,
)

DETAIL_SOURCE_GROUP = "detail-page"


@dataclass(frozen=True)
class PipelinePaths:
    root: Path
    pipeline_dir: Path
    raw_dir: Path
    interim_dir: Path
    processed_dir: Path
    snapshots_dir: Path


DEFAULT_PIPELINE_PATHS = PipelinePaths(
    root=ROOT,
    pipeline_dir=ROOT / "data" / "source_pipeline",
    raw_dir=ROOT / "data" / "source_pipeline" / "raw",
    interim_dir=ROOT / "data" / "source_pipeline" / "interim",
    processed_dir=ROOT / "data" / "source_pipeline" / "processed",
    snapshots_dir=ROOT / "data" / "source_pipeline" / "snapshots",
)

DETAIL_PAGE_ROW_FIELDS = [
    "source_id",
    "source_url",
    "source_group",
    "category_label",
    "position",
    "name",
    "detail_slug",
    "detail_path",
    "detail_url",
    "detail_parser_strategy",
    "detail_parse_status",
    "detail_html_source",
    "detail_parse_error",
    "detail_fetch_source_id",
    "detail_fetch_cached",
    "detail_raw_html_path",
    "detail_raw_meta_path",
    "problem_solved",
    "pricing_summary",
    "target_audience",
    "business_detail_badges_json",
    "founder_name",
    "founder_role",
    "founder_quote",
    "product_updates_heading_present",
]

DETAIL_PARSE_STATUS_VALUES = (
    "not_requested",
    "html_not_supplied",
    "skipped_by_limit",
    "parse_failed",
    "parsed",
)

DETAIL_FIELD_COVERAGE_FIELDS = (
    "problem_solved",
    "pricing_summary",
    "target_audience",
    "business_detail_badges_json",
    "founder_name",
    "founder_role",
    "founder_quote",
    "product_updates_heading_present",
)


def _ensure_pipeline_dirs(paths: PipelinePaths) -> None:
    paths.pipeline_dir.mkdir(parents=True, exist_ok=True)
    paths.raw_dir.mkdir(parents=True, exist_ok=True)
    paths.interim_dir.mkdir(parents=True, exist_ok=True)
    paths.processed_dir.mkdir(parents=True, exist_ok=True)
    paths.snapshots_dir.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})


def _relative_to_root(path: Path, *, root: Path) -> str:
    return path.relative_to(root).as_posix()


def _json_compact(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _detail_source_config(source: SourceConfig, card: ParsedStartupCard) -> SourceConfig:
    detail_slug = detail_slug_from_path(card.detail_path) or f"position-{card.position}"
    return SourceConfig(
        source_id=f"{source.source_id}--detail--{detail_slug}",
        url=card.detail_url,
        parser_strategy=DETAIL_PAGE_PARSER_STRATEGY,
        category_slug=source.category_slug,
        category_label=source.category_label,
        source_group=DETAIL_SOURCE_GROUP,
    )


def _build_detail_page_scaffold_payload(
    source: SourceConfig,
    parsed_cards: list[ParsedStartupCard],
    *,
    fetch_paths: FetchPaths,
    pipeline_paths: PipelinePaths,
    force: bool,
    detail_page_html_resolver: Callable[[ParsedStartupCard], str | None] | None,
    fetch_detail_pages: bool,
    detail_page_limit_per_source: int | None,
    detail_page_fetcher: Callable[..., tuple[FetchResult, float | None]],
    last_live_fetch_at: float | None,
) -> tuple[dict[str, object], float | None]:
    detail_pages: list[dict[str, object]] = []
    fetched_detail_page_count = 0
    parsed_detail_page_count = 0
    failed_detail_page_count = 0

    for card in parsed_cards:
        detail_record: dict[str, object] = {
            "position": card.position,
            "name": card.name,
            "detail_path": card.detail_path,
            "detail_url": card.detail_url,
            "detail_slug": detail_slug_from_path(card.detail_path),
            "detail_parser_strategy": DETAIL_PAGE_PARSER_STRATEGY,
            "detail_parse_status": "not_requested",
            "detail_html_source": "not_requested",
            "detail_fetch_source_id": None,
            "detail_fetch_cached": None,
            "detail_raw_html_path": None,
            "detail_raw_meta_path": None,
            "detail_parse_error": None,
            "extracted_detail": None,
        }
        resolver_was_consulted = detail_page_html_resolver is not None
        detail_html: str | None = None

        if resolver_was_consulted:
            detail_html = detail_page_html_resolver(card)
            if detail_html is not None:
                detail_record["detail_html_source"] = "provided_html"

        if detail_html is None and fetch_detail_pages:
            if detail_page_limit_per_source is not None and fetched_detail_page_count >= detail_page_limit_per_source:
                detail_record["detail_parse_status"] = "skipped_by_limit"
                detail_record["detail_html_source"] = "fetch_skipped_by_limit"
            else:
                detail_source = _detail_source_config(source, card)
                detail_result, last_live_fetch_at = detail_page_fetcher(
                    detail_source,
                    paths=fetch_paths,
                    force=force,
                    last_live_fetch_at=last_live_fetch_at,
                )
                detail_raw_html_path, _, detail_raw_metadata = _copy_raw_artifacts(
                    detail_source,
                    detail_result,
                    fetch_paths=fetch_paths,
                    pipeline_paths=pipeline_paths,
                    raw_subdir="details",
                )
                detail_html = detail_raw_html_path.read_text(encoding="utf-8", errors="replace")
                detail_record["detail_html_source"] = "fetched_html"
                detail_record["detail_fetch_source_id"] = detail_source.source_id
                detail_record["detail_fetch_cached"] = detail_result.cached
                detail_record["detail_raw_html_path"] = detail_raw_metadata["raw_html_path"]
                detail_record["detail_raw_meta_path"] = detail_raw_metadata["raw_meta_path"]
                fetched_detail_page_count += 1

        if detail_html is not None:
            try:
                parsed_detail = parse_startup_detail_html(card, detail_html)
            except ValueError as exc:
                detail_record["detail_parse_status"] = "parse_failed"
                detail_record["detail_parse_error"] = str(exc)
                failed_detail_page_count += 1
            else:
                detail_record["detail_parse_status"] = "parsed"
                detail_record["extracted_detail"] = parsed_detail_as_dict(parsed_detail)
                parsed_detail_page_count += 1
        elif detail_record["detail_parse_status"] == "not_requested" and resolver_was_consulted:
            detail_record["detail_parse_status"] = "html_not_supplied"
            detail_record["detail_html_source"] = "resolver_missing"

        detail_pages.append(detail_record)

    return {
        "detail_page_target_count": len(detail_pages),
        "fetched_detail_page_count": fetched_detail_page_count,
        "parsed_detail_page_count": parsed_detail_page_count,
        "failed_detail_page_count": failed_detail_page_count,
        "detail_pages": detail_pages,
    }, last_live_fetch_at


def _copy_raw_artifacts(
    source: SourceConfig,
    result: FetchResult,
    *,
    fetch_paths: FetchPaths,
    pipeline_paths: PipelinePaths,
    raw_subdir: str | None = None,
) -> tuple[Path, Path, dict[str, object]]:
    cache_html_path = fetch_paths.root / result.cache_html_path
    cache_meta_path = fetch_paths.root / result.cache_meta_path
    raw_dir = pipeline_paths.raw_dir if raw_subdir is None else pipeline_paths.raw_dir / raw_subdir
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_html_path = raw_dir / f"{source.source_id}.html"
    raw_meta_path = raw_dir / f"{source.source_id}.json"

    shutil.copyfile(cache_html_path, raw_html_path)
    cache_metadata = json.loads(cache_meta_path.read_text(encoding="utf-8"))
    raw_metadata = {
        **cache_metadata,
        "raw_html_path": _relative_to_root(raw_html_path, root=pipeline_paths.root),
        "raw_meta_path": _relative_to_root(raw_meta_path, root=pipeline_paths.root),
        "copied_from_cache_html_path": result.cache_html_path,
        "copied_from_cache_meta_path": result.cache_meta_path,
    }
    _write_json(raw_meta_path, raw_metadata)
    return raw_html_path, raw_meta_path, raw_metadata


def _detail_page_row(source: SourceConfig, detail_record: dict[str, object]) -> dict[str, object]:
    extracted_detail = detail_record.get("extracted_detail") or {}
    business_detail_badges = extracted_detail.get("business_detail_badges")
    return {
        "source_id": source.source_id,
        "source_url": source.url,
        "source_group": source.source_group,
        "category_label": source.category_label,
        "position": detail_record.get("position"),
        "name": detail_record.get("name"),
        "detail_slug": detail_record.get("detail_slug"),
        "detail_path": detail_record.get("detail_path"),
        "detail_url": detail_record.get("detail_url"),
        "detail_parser_strategy": detail_record.get("detail_parser_strategy"),
        "detail_parse_status": detail_record.get("detail_parse_status"),
        "detail_html_source": detail_record.get("detail_html_source"),
        "detail_parse_error": detail_record.get("detail_parse_error"),
        "detail_fetch_source_id": detail_record.get("detail_fetch_source_id"),
        "detail_fetch_cached": detail_record.get("detail_fetch_cached"),
        "detail_raw_html_path": detail_record.get("detail_raw_html_path"),
        "detail_raw_meta_path": detail_record.get("detail_raw_meta_path"),
        "problem_solved": extracted_detail.get("problem_solved"),
        "pricing_summary": extracted_detail.get("pricing_summary"),
        "target_audience": extracted_detail.get("target_audience"),
        "business_detail_badges_json": (
            _json_compact(list(business_detail_badges))
            if business_detail_badges is not None
            else None
        ),
        "founder_name": extracted_detail.get("founder_name"),
        "founder_role": extracted_detail.get("founder_role"),
        "founder_quote": extracted_detail.get("founder_quote"),
        "product_updates_heading_present": extracted_detail.get("product_updates_heading_present"),
    }


def _is_populated_detail_field(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() not in {"", "[]"}
    return bool(value)


def _detail_parse_status_counts(detail_page_rows: list[dict[str, object]]) -> dict[str, int]:
    return {
        status: sum(1 for row in detail_page_rows if row.get("detail_parse_status") == status)
        for status in DETAIL_PARSE_STATUS_VALUES
    }


def _detail_field_population_counts(detail_page_rows: list[dict[str, object]]) -> dict[str, int]:
    return {
        field: sum(1 for row in detail_page_rows if _is_populated_detail_field(row.get(field)))
        for field in DETAIL_FIELD_COVERAGE_FIELDS
    }


def _build_detail_field_coverage_summary(detail_page_rows: list[dict[str, object]]) -> dict[str, object]:
    per_source_rows: dict[str, list[dict[str, object]]] = {}
    source_metadata: dict[str, dict[str, object]] = {}

    for row in detail_page_rows:
        source_id = str(row["source_id"])
        per_source_rows.setdefault(source_id, []).append(row)
        source_metadata.setdefault(
            source_id,
            {
                "source_id": row["source_id"],
                "source_url": row["source_url"],
                "source_group": row["source_group"],
                "category_label": row["category_label"],
            },
        )

    return {
        "detail_page_row_count": len(detail_page_rows),
        "aggregate": {
            "detail_page_row_count": len(detail_page_rows),
            "parse_status_counts": _detail_parse_status_counts(detail_page_rows),
            "field_population_counts": _detail_field_population_counts(detail_page_rows),
        },
        "sources": [
            {
                **source_metadata[source_id],
                "detail_page_row_count": len(source_rows),
                "parse_status_counts": _detail_parse_status_counts(source_rows),
                "field_population_counts": _detail_field_population_counts(source_rows),
            }
            for source_id, source_rows in per_source_rows.items()
        ],
    }


def _scraped_at_from_path(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _build_snapshot_manifest(
    *,
    selected_sources: list[SourceConfig],
    per_source_outputs: list[dict[str, object]],
    normalized_rows: list[dict[str, object]],
    visible_rows: list[dict[str, object]],
    heuristic_override_report: dict[str, object],
    suspicious_duplicates_report: dict[str, object],
    validation_report: dict[str, object],
    pipeline_paths: PipelinePaths,
) -> dict[str, object]:
    return {
        "selected_source_count": len(selected_sources),
        "detail_page_target_count": sum(int(source["detail_page_target_count"]) for source in per_source_outputs),
        "fetched_detail_page_count": sum(int(source["fetched_detail_page_count"]) for source in per_source_outputs),
        "parsed_detail_page_count": sum(int(source["parsed_detail_page_count"]) for source in per_source_outputs),
        "failed_detail_page_count": sum(int(source["failed_detail_page_count"]) for source in per_source_outputs),
        "selected_sources": [
            {
                "source_id": source.source_id,
                "source_url": source.url,
                "parser_strategy": source.parser_strategy,
                "source_group": source.source_group,
                "category_label": source.category_label,
            }
            for source in selected_sources
        ],
        "per_source_outputs": per_source_outputs,
        "normalized_row_count": len(normalized_rows),
        "visible_sample_row_count": len(visible_rows),
        "fully_mapped_visible_row_count": heuristic_override_report["fully_mapped_visible_row_count"],
        "suspicious_duplicate_group_count": suspicious_duplicates_report["group_count"],
        "validation_status": validation_report["status"],
        "generated_outputs": [
            _relative_to_root(pipeline_paths.processed_dir / "normalized_rows.csv", root=pipeline_paths.root),
            _relative_to_root(pipeline_paths.processed_dir / "visible_sample_rows.csv", root=pipeline_paths.root),
            _relative_to_root(
                pipeline_paths.processed_dir / "heuristic_override_report.json",
                root=pipeline_paths.root,
            ),
            _relative_to_root(
                pipeline_paths.processed_dir / "suspicious_duplicates.json",
                root=pipeline_paths.root,
            ),
            _relative_to_root(pipeline_paths.processed_dir / "detail_page_rows.csv", root=pipeline_paths.root),
            _relative_to_root(
                pipeline_paths.processed_dir / "detail_field_coverage.json",
                root=pipeline_paths.root,
            ),
            _relative_to_root(pipeline_paths.processed_dir / "validation_report.json", root=pipeline_paths.root),
        ],
    }


def run_pipeline(
    *,
    source_id: str | None = None,
    limit: int | None = None,
    force: bool = False,
    fetch_paths: FetchPaths = DEFAULT_FETCH_PATHS,
    pipeline_paths: PipelinePaths = DEFAULT_PIPELINE_PATHS,
    source_registry: list[SourceConfig] | None = None,
    fetcher: Callable[..., tuple[FetchResult, float | None]] = fetch_source,
    detail_page_html_resolver: Callable[[ParsedStartupCard], str | None] | None = None,
    fetch_detail_pages: bool = False,
    detail_page_limit_per_source: int | None = None,
    detail_page_fetcher: Callable[..., tuple[FetchResult, float | None]] = fetch_source,
) -> dict[str, object]:
    _ensure_pipeline_dirs(pipeline_paths)
    if detail_page_limit_per_source is not None and detail_page_limit_per_source < 0:
        raise ValueError("detail_page_limit_per_source must be non-negative")

    selected_sources = source_registry if source_registry is not None else select_sources(source_id=source_id, limit=limit)
    if not selected_sources:
        raise ValueError("No source pages were selected for the pipeline run.")

    per_source_outputs: list[dict[str, object]] = []
    detail_page_rows: list[dict[str, object]] = []
    normalized_rows: list[dict[str, object]] = []
    last_live_fetch_at: float | None = None

    for source in selected_sources:
        result, last_live_fetch_at = fetcher(
            source,
            paths=fetch_paths,
            force=force,
            last_live_fetch_at=last_live_fetch_at,
        )
        raw_html_path, _, raw_metadata = _copy_raw_artifacts(
            source,
            result,
            fetch_paths=fetch_paths,
            pipeline_paths=pipeline_paths,
        )
        parsed_cards = parse_source_html(source, raw_html_path.read_text(encoding="utf-8", errors="replace"))
        interim_path = pipeline_paths.interim_dir / f"{source.source_id}.json"
        detail_scaffold_path = pipeline_paths.interim_dir / f"{source.source_id}.detail_pages.json"
        interim_payload = {
            "source_id": source.source_id,
            "source_url": source.url,
            "card_count": len(parsed_cards),
            "cards": parsed_cards_as_dicts(parsed_cards),
        }
        scaffold_payload, last_live_fetch_at = _build_detail_page_scaffold_payload(
            source,
            parsed_cards,
            fetch_paths=fetch_paths,
            pipeline_paths=pipeline_paths,
            force=force,
            detail_page_html_resolver=detail_page_html_resolver,
            fetch_detail_pages=fetch_detail_pages,
            detail_page_limit_per_source=detail_page_limit_per_source,
            detail_page_fetcher=detail_page_fetcher,
            last_live_fetch_at=last_live_fetch_at,
        )
        detail_scaffold_payload = {
            "source_id": source.source_id,
            "source_url": source.url,
            **scaffold_payload,
        }
        _write_json(interim_path, interim_payload)
        _write_json(detail_scaffold_path, detail_scaffold_payload)
        detail_page_rows.extend(
            _detail_page_row(source, detail_record) for detail_record in detail_scaffold_payload["detail_pages"]
        )

        scraped_at = _scraped_at_from_path(raw_html_path)
        source_rows = normalize_parsed_cards(parsed_cards, scraped_at=scraped_at)
        normalized_rows.extend(source_rows)
        per_source_outputs.append(
            {
                "source_id": source.source_id,
                "source_url": source.url,
                "raw_html_path": raw_metadata["raw_html_path"],
                "interim_path": _relative_to_root(interim_path, root=pipeline_paths.root),
                "detail_scaffold_path": _relative_to_root(detail_scaffold_path, root=pipeline_paths.root),
                "parsed_card_count": len(parsed_cards),
                "visible_sample_row_count": sum(1 for row in source_rows if row["included_in_visible_sample"]),
                "detail_page_target_count": detail_scaffold_payload["detail_page_target_count"],
                "fetched_detail_page_count": detail_scaffold_payload["fetched_detail_page_count"],
                "parsed_detail_page_count": detail_scaffold_payload["parsed_detail_page_count"],
                "failed_detail_page_count": detail_scaffold_payload["failed_detail_page_count"],
            }
        )

    deduped_rows = dedupe_normalized_rows(normalized_rows)
    visible_rows = build_visible_sample_rows(deduped_rows)
    normalized_rows_path = pipeline_paths.processed_dir / "normalized_rows.csv"
    visible_rows_path = pipeline_paths.processed_dir / "visible_sample_rows.csv"
    detail_rows_path = pipeline_paths.processed_dir / "detail_page_rows.csv"
    detail_field_coverage_path = pipeline_paths.processed_dir / "detail_field_coverage.json"
    heuristic_override_report_path = pipeline_paths.processed_dir / "heuristic_override_report.json"
    suspicious_duplicates_path = pipeline_paths.processed_dir / "suspicious_duplicates.json"
    validation_path = pipeline_paths.processed_dir / "validation_report.json"
    snapshot_path = pipeline_paths.snapshots_dir / "run_manifest.json"

    _write_csv(normalized_rows_path, deduped_rows, NORMALIZED_ROW_FIELDS)
    _write_csv(visible_rows_path, visible_rows, VISIBLE_SAMPLE_ROW_FIELDS)
    _write_csv(detail_rows_path, detail_page_rows, DETAIL_PAGE_ROW_FIELDS)
    _write_json(detail_field_coverage_path, _build_detail_field_coverage_summary(detail_page_rows))
    heuristic_override_report = build_heuristic_override_report(deduped_rows)
    suspicious_duplicates_report = build_suspicious_duplicates_report(deduped_rows)
    _write_json(heuristic_override_report_path, heuristic_override_report)
    _write_json(suspicious_duplicates_path, suspicious_duplicates_report)
    validation_report = validate_normalized_rows(deduped_rows, suspicious_duplicates_report=suspicious_duplicates_report)
    write_validation_report(validation_path, validation_report)
    ensure_validation_passes(validation_report)

    snapshot_manifest = _build_snapshot_manifest(
        selected_sources=selected_sources,
        per_source_outputs=per_source_outputs,
        normalized_rows=deduped_rows,
        visible_rows=visible_rows,
        heuristic_override_report=heuristic_override_report,
        suspicious_duplicates_report=suspicious_duplicates_report,
        validation_report=validation_report,
        pipeline_paths=pipeline_paths,
    )
    _write_json(snapshot_path, snapshot_manifest)

    return {
        "selected_source_count": len(selected_sources),
        "detail_page_target_count": sum(int(source["detail_page_target_count"]) for source in per_source_outputs),
        "fetched_detail_page_count": sum(int(source["fetched_detail_page_count"]) for source in per_source_outputs),
        "parsed_detail_page_count": sum(int(source["parsed_detail_page_count"]) for source in per_source_outputs),
        "failed_detail_page_count": sum(int(source["failed_detail_page_count"]) for source in per_source_outputs),
        "normalized_row_count": len(deduped_rows),
        "visible_sample_row_count": len(visible_rows),
        "fully_mapped_visible_row_count": heuristic_override_report["fully_mapped_visible_row_count"],
        "suspicious_duplicate_group_count": suspicious_duplicates_report["group_count"],
        "validation_status": validation_report["status"],
        "normalized_rows_path": _relative_to_root(normalized_rows_path, root=pipeline_paths.root),
        "visible_rows_path": _relative_to_root(visible_rows_path, root=pipeline_paths.root),
        "detail_rows_path": _relative_to_root(detail_rows_path, root=pipeline_paths.root),
        "detail_field_coverage_path": _relative_to_root(detail_field_coverage_path, root=pipeline_paths.root),
        "heuristic_override_report_path": _relative_to_root(
            heuristic_override_report_path,
            root=pipeline_paths.root,
        ),
        "suspicious_duplicates_path": _relative_to_root(suspicious_duplicates_path, root=pipeline_paths.root),
        "validation_report_path": _relative_to_root(validation_path, root=pipeline_paths.root),
        "snapshot_path": _relative_to_root(snapshot_path, root=pipeline_paths.root),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python src/build_all.py",
        description="Fetch, parse, normalize, and validate the source-facing TrustMRR pipeline.",
    )
    parser.add_argument("--source-id", default=None, help="Run the pipeline for one source id")
    parser.add_argument("--limit", type=int, default=None, help="Only process the first N selected sources")
    parser.add_argument("--force", action="store_true", help="Ignore fresh cache entries and refetch")
    parser.add_argument(
        "--fetch-details",
        action="store_true",
        help="Explicitly fetch parsed startup detail pages into staged raw/detail artifacts",
    )
    parser.add_argument(
        "--detail-limit-per-source",
        type=int,
        default=None,
        help="Only fetch up to N startup detail pages per selected source when --fetch-details is enabled",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    summary = run_pipeline(
        source_id=args.source_id,
        limit=args.limit,
        force=args.force,
        fetch_detail_pages=args.fetch_details,
        detail_page_limit_per_source=args.detail_limit_per_source,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
