from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from src.promote_live_bundle import promote_live_bundle


def prepare_workspace(tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache")
    shutil.copytree(ROOT / "src", workspace / "src", ignore=ignore)
    shutil.copytree(ROOT / "docs", workspace / "docs", ignore=ignore)
    (workspace / "charts").mkdir()
    (workspace / "data" / "source_pipeline" / "processed").mkdir(parents=True)
    (workspace / "data" / "source_pipeline" / "snapshots").mkdir(parents=True)
    shutil.copy2(ROOT / "DATA-NOTICE.md", workspace / "DATA-NOTICE.md")
    shutil.copy2(ROOT / "data" / "publication_input.json", workspace / "data" / "publication_input.json")
    return workspace


def write_source_registry(workspace: Path, source_urls: list[str]) -> None:
    registry_path = workspace / "data" / "public_source_pages.csv"
    with registry_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["source_url"])
        writer.writeheader()
        for source_url in source_urls:
            writer.writerow({"source_url": source_url})


def write_staged_artifacts(
    workspace: Path,
    *,
    unmapped_visible_row_count: int = 0,
    selected_source_count: int = 2,
    run_manifest_validation_status: str = "passed",
) -> None:
    visible_rows_path = workspace / "data" / "source_pipeline" / "processed" / "visible_sample_rows.csv"
    with visible_rows_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "name",
                "canonical_slug",
                "category",
                "revenue_30d",
                "biz_model",
                "gtm_model",
                "revenue_band",
                "source_url",
                "source_id",
                "source_group",
                "parser_strategy",
                "detail_url",
                "detail_path",
                "position",
                "badge",
                "description",
                "heuristic_override_key",
                "heuristic_override_source",
                "mrr",
                "total_revenue",
                "scraped_at",
            ],
        )
        writer.writeheader()
        writer.writerows(
            [
                {
                    "name": "Alpha",
                    "canonical_slug": "alpha",
                    "category": "AI",
                    "revenue_30d": 25000,
                    "biz_model": "Software / SaaS",
                    "gtm_model": "PLG / inbound software",
                    "revenue_band": "$10k–$50k",
                    "source_url": "https://example.com/category/ai",
                    "source_id": "category--ai",
                    "source_group": "category",
                    "parser_strategy": "fixture",
                    "detail_url": "https://example.com/startup/alpha",
                    "detail_path": "/startup/alpha",
                    "position": 1,
                    "badge": "",
                    "description": "Alpha description",
                    "heuristic_override_key": "https://example.com/category/ai::alpha",
                    "heuristic_override_source": "tracked_override",
                    "mrr": 26000,
                    "total_revenue": 100000,
                    "scraped_at": "2026-03-26T20:00:00Z",
                },
                {
                    "name": "Beta",
                    "canonical_slug": "beta",
                    "category": "Productivity",
                    "revenue_30d": 12000,
                    "biz_model": "" if unmapped_visible_row_count else "Software / SaaS",
                    "gtm_model": "" if unmapped_visible_row_count else "PLG / inbound software",
                    "revenue_band": "$10k–$50k",
                    "source_url": "https://example.com/category/productivity",
                    "source_id": "category--productivity",
                    "source_group": "category",
                    "parser_strategy": "fixture",
                    "detail_url": "https://example.com/startup/beta",
                    "detail_path": "/startup/beta",
                    "position": 1,
                    "badge": "",
                    "description": "Beta description",
                    "heuristic_override_key": "https://example.com/category/productivity::beta",
                    "heuristic_override_source": "tracked_override" if not unmapped_visible_row_count else "",
                    "mrr": 13000,
                    "total_revenue": 50000,
                    "scraped_at": "2026-03-26T20:00:00Z",
                },
            ]
        )

    (workspace / "data" / "source_pipeline" / "processed" / "heuristic_override_report.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "override_key_format": "<source_url>::<canonical_slug>",
                "override_files": [],
                "total_row_count": 2,
                "visible_sample_row_count": 2,
                "biz_model_override_count": 2 - unmapped_visible_row_count,
                "gtm_model_override_count": 2 - unmapped_visible_row_count,
                "fully_mapped_visible_row_count": 2 - unmapped_visible_row_count,
                "alias_resolved_visible_row_count": 0,
                "unmapped_visible_row_count": unmapped_visible_row_count,
                "unmapped_visible_rows": [] if not unmapped_visible_row_count else [{"name": "Beta"}],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (workspace / "data" / "source_pipeline" / "processed" / "suspicious_duplicates.json").write_text(
        json.dumps({"group_count": 0, "groups": [], "row_count": 0}, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (workspace / "data" / "source_pipeline" / "processed" / "validation_report.json").write_text(
        json.dumps(
            {
                "status": "passed",
                "parsed_row_count": 2,
                "visible_sample_row_count": 2,
                "excluded_below_threshold_count": 0,
                "threshold_usd": 5000,
                "missing_required_field_counts": {},
                "missing_biz_model_count": unmapped_visible_row_count,
                "missing_gtm_model_count": unmapped_visible_row_count,
                "duplicate_detail_url_count": 0,
                "duplicate_name_source_url_count": 0,
                "suspicious_duplicate_group_count": 0,
                "suspicious_duplicate_row_count": 0,
                "min_visible_revenue_30d": 12000,
                "checks": [],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (workspace / "data" / "source_pipeline" / "snapshots" / "run_manifest.json").write_text(
        json.dumps(
            {
                "selected_source_count": selected_source_count,
                "selected_sources": [],
                "per_source_outputs": [],
                "normalized_row_count": 2,
                "visible_sample_row_count": 2,
                "fully_mapped_visible_row_count": 2 - unmapped_visible_row_count,
                "suspicious_duplicate_group_count": 0,
                "validation_status": run_manifest_validation_status,
                "generated_outputs": [],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def run_build_commands(workspace: Path) -> None:
    build = subprocess.run([sys.executable, "src/build_artifacts.py"], cwd=workspace, capture_output=True, text=True)
    assert build.returncode == 0, build.stdout + build.stderr
    site = subprocess.run([sys.executable, "src/build_site.py"], cwd=workspace, capture_output=True, text=True)
    assert site.returncode == 0, site.stdout + site.stderr


def test_promote_live_bundle_writes_promoted_dataset_and_updates_manifest(tmp_path: Path) -> None:
    workspace = prepare_workspace(tmp_path)
    write_source_registry(
        workspace,
        [
            "https://example.com/category/ai",
            "https://example.com/category/productivity",
        ],
    )
    write_staged_artifacts(workspace)

    summary = promote_live_bundle(root=workspace)

    promoted_dataset = workspace / "data" / "promoted_visible_sample.csv"
    publication_input = json.loads((workspace / "data" / "publication_input.json").read_text(encoding="utf-8"))

    assert summary["status"] == "promoted"
    assert promoted_dataset.exists()
    assert publication_input["dataset_kind"] == "source_pipeline_promotion"
    assert publication_input["dataset_path"] == "data/promoted_visible_sample.csv"
    assert publication_input["expected_source_count"] == 2
    assert publication_input["selected_source_count"] == 2
    assert publication_input["staged_visible_rows_sha256"]
    assert publication_input["promoted_dataset_sha256"]
    assert publication_input["promotion_gate_summary"]["visible_sample_row_count"] == 2

    with promoted_dataset.open(encoding="utf-8", newline="") as handle:
        promoted_rows = list(csv.DictReader(handle))
    assert promoted_rows[0]["name"] == "Alpha"
    assert list(promoted_rows[0].keys()) == [
        "name",
        "category",
        "revenue_30d",
        "biz_model",
        "gtm_model",
        "source_url",
        "revenue_band",
    ]

    run_build_commands(workspace)
    pipeline_manifest = json.loads((workspace / "data" / "pipeline_manifest.json").read_text(encoding="utf-8"))
    assert pipeline_manifest["publication_input"]["dataset_kind"] == "source_pipeline_promotion"
    assert pipeline_manifest["publication_input"]["expected_source_count"] == 2
    assert pipeline_manifest["input_dataset"]["path"] == "data/promoted_visible_sample.csv"
    assert (workspace / "site" / "data" / "publication_input.json").exists()


def test_promote_live_bundle_refuses_unmapped_visible_rows(tmp_path: Path) -> None:
    workspace = prepare_workspace(tmp_path)
    write_source_registry(
        workspace,
        [
            "https://example.com/category/ai",
            "https://example.com/category/productivity",
        ],
    )
    write_staged_artifacts(workspace, unmapped_visible_row_count=1)

    try:
        promote_live_bundle(root=workspace)
    except ValueError as error:
        assert "unmapped visible rows" in str(error)
    else:
        raise AssertionError("Expected promote_live_bundle() to reject unmapped visible rows.")


def test_promote_live_bundle_refuses_partial_source_registry_runs(tmp_path: Path) -> None:
    workspace = prepare_workspace(tmp_path)
    write_source_registry(
        workspace,
        [
            "https://example.com/category/ai",
            "https://example.com/category/productivity",
        ],
    )
    write_staged_artifacts(workspace, selected_source_count=1)

    try:
        promote_live_bundle(root=workspace)
    except ValueError as error:
        assert "full registered source set" in str(error)
    else:
        raise AssertionError("Expected promote_live_bundle() to reject partial source-registry runs.")
