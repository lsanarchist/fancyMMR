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
    normalize_parsed_cards,
)
from src.parse import parse_source_html, parsed_cards_as_dicts
from src.validate import (
    build_suspicious_duplicates_report,
    ensure_validation_passes,
    validate_normalized_rows,
    write_validation_report,
)


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


def _copy_raw_artifacts(
    source: SourceConfig,
    result: FetchResult,
    *,
    fetch_paths: FetchPaths,
    pipeline_paths: PipelinePaths,
) -> tuple[Path, Path, dict[str, object]]:
    cache_html_path = fetch_paths.root / result.cache_html_path
    cache_meta_path = fetch_paths.root / result.cache_meta_path
    raw_html_path = pipeline_paths.raw_dir / f"{source.source_id}.html"
    raw_meta_path = pipeline_paths.raw_dir / f"{source.source_id}.json"

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
) -> dict[str, object]:
    _ensure_pipeline_dirs(pipeline_paths)

    selected_sources = source_registry if source_registry is not None else select_sources(source_id=source_id, limit=limit)
    if not selected_sources:
        raise ValueError("No source pages were selected for the pipeline run.")

    per_source_outputs: list[dict[str, object]] = []
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
        interim_payload = {
            "source_id": source.source_id,
            "source_url": source.url,
            "card_count": len(parsed_cards),
            "cards": parsed_cards_as_dicts(parsed_cards),
        }
        _write_json(interim_path, interim_payload)

        scraped_at = _scraped_at_from_path(raw_html_path)
        source_rows = normalize_parsed_cards(parsed_cards, scraped_at=scraped_at)
        normalized_rows.extend(source_rows)
        per_source_outputs.append(
            {
                "source_id": source.source_id,
                "source_url": source.url,
                "raw_html_path": raw_metadata["raw_html_path"],
                "interim_path": _relative_to_root(interim_path, root=pipeline_paths.root),
                "parsed_card_count": len(parsed_cards),
                "visible_sample_row_count": sum(1 for row in source_rows if row["included_in_visible_sample"]),
            }
        )

    visible_rows = build_visible_sample_rows(normalized_rows)
    normalized_rows_path = pipeline_paths.processed_dir / "normalized_rows.csv"
    visible_rows_path = pipeline_paths.processed_dir / "visible_sample_rows.csv"
    heuristic_override_report_path = pipeline_paths.processed_dir / "heuristic_override_report.json"
    suspicious_duplicates_path = pipeline_paths.processed_dir / "suspicious_duplicates.json"
    validation_path = pipeline_paths.processed_dir / "validation_report.json"
    snapshot_path = pipeline_paths.snapshots_dir / "run_manifest.json"

    _write_csv(normalized_rows_path, normalized_rows, NORMALIZED_ROW_FIELDS)
    _write_csv(visible_rows_path, visible_rows, VISIBLE_SAMPLE_ROW_FIELDS)
    heuristic_override_report = build_heuristic_override_report(normalized_rows)
    suspicious_duplicates_report = build_suspicious_duplicates_report(normalized_rows)
    _write_json(heuristic_override_report_path, heuristic_override_report)
    _write_json(suspicious_duplicates_path, suspicious_duplicates_report)
    validation_report = validate_normalized_rows(
        normalized_rows,
        suspicious_duplicates_report=suspicious_duplicates_report,
    )
    write_validation_report(validation_path, validation_report)
    ensure_validation_passes(validation_report)

    snapshot_manifest = _build_snapshot_manifest(
        selected_sources=selected_sources,
        per_source_outputs=per_source_outputs,
        normalized_rows=normalized_rows,
        visible_rows=visible_rows,
        heuristic_override_report=heuristic_override_report,
        suspicious_duplicates_report=suspicious_duplicates_report,
        validation_report=validation_report,
        pipeline_paths=pipeline_paths,
    )
    _write_json(snapshot_path, snapshot_manifest)

    return {
        "selected_source_count": len(selected_sources),
        "normalized_row_count": len(normalized_rows),
        "visible_sample_row_count": len(visible_rows),
        "fully_mapped_visible_row_count": heuristic_override_report["fully_mapped_visible_row_count"],
        "suspicious_duplicate_group_count": suspicious_duplicates_report["group_count"],
        "validation_status": validation_report["status"],
        "normalized_rows_path": _relative_to_root(normalized_rows_path, root=pipeline_paths.root),
        "visible_rows_path": _relative_to_root(visible_rows_path, root=pipeline_paths.root),
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
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    summary = run_pipeline(source_id=args.source_id, limit=args.limit, force=args.force)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
