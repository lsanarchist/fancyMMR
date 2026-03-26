from __future__ import annotations

import hashlib
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def prepare_workspace(tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache")
    shutil.copytree(ROOT / "src", workspace / "src", ignore=ignore)
    shutil.copytree(ROOT / "data", workspace / "data", ignore=ignore)
    shutil.copytree(ROOT / "docs", workspace / "docs", ignore=ignore)
    shutil.copytree(ROOT / "charts", workspace / "charts", ignore=ignore)
    shutil.copy2(ROOT / "DATA-NOTICE.md", workspace / "DATA-NOTICE.md")
    return workspace


def run_site_build(workspace: Path) -> None:
    result = subprocess.run(
        [sys.executable, "src/build_site.py"],
        cwd=workspace,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def site_hashes(workspace: Path) -> dict[str, str]:
    site_root = workspace / "site"
    return {
        path.relative_to(site_root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(site_root.rglob("*"))
        if path.is_file()
    }


def test_build_site_outputs_pages_assets_and_copied_json(tmp_path: Path) -> None:
    workspace = prepare_workspace(tmp_path)

    run_site_build(workspace)

    site_root = workspace / "site"
    assert (site_root / ".nojekyll").exists()
    assert (site_root / "index.html").exists()
    assert (site_root / "methodology.html").exists()
    assert (site_root / "data.html").exists()
    assert (site_root / "assets" / "site.css").exists()
    assert (site_root / "assets" / "charts" / "category_share_map.png").exists()
    assert (site_root / "assets" / "charts" / "category_share_map.svg").exists()
    assert (site_root / "data" / "metrics.json").exists()
    assert (site_root / "data" / "publication_input.json").exists()
    assert (site_root / "data" / "validation_report.json").exists()
    assert (site_root / "data" / "source_coverage_report.json").exists()
    assert (site_root / "data" / "source_pipeline_diagnostics.json").exists()
    assert (site_root / "data" / "pipeline_manifest.json").exists()
    assert (site_root / "data" / "source_pipeline" / "processed" / "detail_page_rows.csv").exists()
    assert (site_root / "data" / "source_pipeline" / "processed" / "detail_field_coverage.json").exists()

    index_html = (site_root / "index.html").read_text(encoding="utf-8")
    methodology_html = (site_root / "methodology.html").read_text(encoding="utf-8")
    data_html = (site_root / "data.html").read_text(encoding="utf-8")

    assert "visible public sample" in index_html.lower()
    assert 'href="methodology.html"' in index_html
    assert 'href="data.html"' in index_html
    assert 'src="assets/charts/category_share_map.png"' in index_html
    assert 'loading="lazy"' in index_html
    assert 'href="/' not in index_html
    assert 'src="/' not in index_html
    assert "249 startups" in index_html
    assert "9 duplicate startup names" in index_html

    assert "not a full database export" in methodology_html.lower()
    assert "source-derived visible sample" in methodology_html.lower()
    assert "passed with warnings" in methodology_html.lower()
    assert "data/source_pipeline/" in methodology_html
    assert "license-code-mit.txt" in methodology_html.lower()
    assert "9 duplicate names / 0 heuristic gaps" in methodology_html

    assert "metrics.json" in data_html
    assert "publication_input.json" in data_html
    assert "validation_report.json" in data_html
    assert "source_coverage_report.json" in data_html
    assert "source_pipeline_diagnostics.json" in data_html
    assert "pipeline_manifest.json" in data_html
    assert "source_pipeline/processed/detail_page_rows.csv" in data_html
    assert "source_pipeline/processed/detail_field_coverage.json" in data_html
    assert "Source-pipeline diagnostics" in data_html
    assert "Detail-page staging" in data_html
    assert "Detail parse failures" in data_html
    assert "Detail-field coverage" in data_html
    assert "staged provenance" in data_html
    assert "No source pages in the active manifest currently report staged detail parse failures." in data_html
    assert "No staged detail rows in the active manifest currently populate the shared detail fields." in data_html


def test_build_site_is_deterministic_across_rebuilds(tmp_path: Path) -> None:
    workspace = prepare_workspace(tmp_path)

    run_site_build(workspace)
    first_hashes = site_hashes(workspace)

    run_site_build(workspace)
    second_hashes = site_hashes(workspace)

    assert second_hashes == first_hashes
