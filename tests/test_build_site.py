from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ESCAPED_CSP_SNIPPET = (
    "default-src &#x27;self&#x27;; "
    "img-src &#x27;self&#x27; data:; "
    "object-src &#x27;none&#x27;; "
    "base-uri &#x27;self&#x27;; "
    "form-action &#x27;self&#x27;"
)


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


def run_artifact_build(workspace: Path) -> None:
    result = subprocess.run(
        [sys.executable, "src/build_artifacts.py"],
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


def assert_text_order(text: str, snippets: list[str]) -> None:
    positions = [text.index(snippet) for snippet in snippets]
    assert positions == sorted(positions), {"snippets": snippets, "positions": positions}


def extract_hot_output_section(text: str, title: str) -> str:
    pattern = re.compile(
        rf'<div class="rail-command-group">\s*<div class="rail-command-group-head">\s*<p class="rail-command-group-title">{re.escape(title)}</p>.*?(?=<div class="rail-command-group">\s*<div class="rail-command-group-head">|</section>)',
        re.DOTALL,
    )
    match = pattern.search(text)
    assert match, title
    return match.group(0)


def extract_rail_module(text: str, kicker: str) -> str:
    marker = f'<p class="rail-kicker">{kicker}</p>'
    marker_index = text.find(marker)
    assert marker_index != -1, kicker
    section_start = text.rfind("<section", 0, marker_index)
    assert section_start != -1, kicker

    depth = 0
    cursor = section_start
    while cursor < len(text):
        next_open = text.find("<section", cursor)
        next_close = text.find("</section>", cursor)
        assert next_close != -1, kicker
        if next_open != -1 and next_open < next_close:
            depth += 1
            cursor = next_open + len("<section")
            continue
        depth -= 1
        cursor = next_close + len("</section>")
        if depth == 0:
            return text[section_start:cursor]

    raise AssertionError(kicker)


def extract_hero_aside(text: str, eyebrow: str) -> str:
    marker = f'<p class="eyebrow">{eyebrow}</p>'
    marker_index = text.find(marker)
    assert marker_index != -1, eyebrow
    container_start = text.rfind('<div class="hero-aside">', 0, marker_index)
    assert container_start != -1, eyebrow

    depth = 0
    cursor = container_start
    while cursor < len(text):
        next_open = text.find("<div", cursor)
        next_close = text.find("</div>", cursor)
        assert next_close != -1, eyebrow
        if next_open != -1 and next_open < next_close:
            depth += 1
            cursor = next_open + len("<div")
            continue
        depth -= 1
        cursor = next_close + len("</div>")
        if depth == 0:
            return text[container_start:cursor]

    raise AssertionError(eyebrow)


def extract_ticker_strip(text: str) -> str:
    pattern = re.compile(
        r'<div class="ticker-strip" aria-label="Workspace status">(.*?)</div>',
        re.DOTALL,
    )
    match = pattern.search(text)
    assert match, "ticker-strip"
    return match.group(1)


def extract_site_footer(text: str) -> str:
    pattern = re.compile(
        r'<footer class="site-shell site-footer">(.*?)</footer>',
        re.DOTALL,
    )
    match = pattern.search(text)
    assert match, "site-footer"
    return match.group(1)


def extract_workspace_command(text: str) -> str:
    pattern = re.compile(
        r'<section class="workspace-command"[^>]*>(.*?)</section>',
        re.DOTALL,
    )
    match = pattern.search(text)
    assert match, "workspace-command"
    return match.group(1)


def extract_brand(text: str) -> str:
    pattern = re.compile(
        r'<a class="brand" href="index.html">(.*?)</a>',
        re.DOTALL,
    )
    match = pattern.search(text)
    assert match, "brand"
    return match.group(1)


def extract_primary_nav(text: str) -> str:
    pattern = re.compile(
        r'<nav class="nav-links" aria-label="Primary">(.*?)</nav>',
        re.DOTALL,
    )
    match = pattern.search(text)
    assert match, "primary-nav"
    return match.group(1)


def extract_page_panels_nav(text: str) -> str:
    pattern = re.compile(
        r'<nav class="rail-command-links" aria-label="Page panels">(.*?)</nav>',
        re.DOTALL,
    )
    match = pattern.search(text)
    assert match, "page-panels-nav"
    return match.group(1)


def extract_route_registry_nav(text: str) -> str:
    pattern = re.compile(
        r'<nav class="rail-command-links" aria-label="Cross-page routes">(.*?)</nav>',
        re.DOTALL,
    )
    match = pattern.search(text)
    assert match, "route-registry-nav"
    return match.group(1)


def format_byte_count(byte_count: int) -> str:
    if byte_count == 1:
        return "1 byte"
    return f"{byte_count:,} bytes"


def format_average_byte_count(total_bytes: int, item_count: int) -> str:
    if item_count <= 0:
        return "avg 0 bytes"
    average_bytes = (total_bytes + (item_count // 2)) // item_count
    return f"avg {format_byte_count(average_bytes)}"


def format_byte_share(total_bytes: int, section_total_bytes: int) -> str:
    if section_total_bytes <= 0:
        return "0%"
    share = (100 * total_bytes) / section_total_bytes
    return f"{share:.0f}%"


def format_count_share(item_count: int, section_item_count: int) -> str:
    if section_item_count <= 0:
        return "0%"
    share = (100 * item_count) / section_item_count
    return f"{share:.0f}%"


def format_byte_range(min_bytes: int | None, max_bytes: int | None) -> str:
    if min_bytes is None or max_bytes is None:
        return "size n/a"
    if min_bytes == max_bytes:
        return format_byte_count(min_bytes)
    return f"{format_byte_count(min_bytes)} to {format_byte_count(max_bytes)}"


def median_byte_value(byte_values: list[int]) -> int | None:
    if not byte_values:
        return None
    ordered_values = sorted(byte_values)
    midpoint = len(ordered_values) // 2
    if len(ordered_values) % 2 == 1:
        return ordered_values[midpoint]
    lower = ordered_values[midpoint - 1]
    upper = ordered_values[midpoint]
    return (lower + upper + 1) // 2


def format_median_byte_count(byte_values: list[int]) -> str:
    median_bytes = median_byte_value(byte_values)
    if median_bytes is None:
        return "med 0 bytes"
    return f"med {format_byte_count(median_bytes)}"


def format_byte_spread_ratio(min_bytes: int | None, max_bytes: int | None) -> str:
    if min_bytes is None or max_bytes is None or min_bytes <= 0:
        return "spread n/a"
    spread_ratio = max_bytes / min_bytes
    if spread_ratio < 10:
        return f"spread {spread_ratio:.1f}x"
    return f"spread {spread_ratio:.0f}x"


def format_top_file_share(max_bytes: int | None, total_bytes: int) -> str:
    if max_bytes is None or total_bytes <= 0:
        return "top n/a"
    share = (100 * max_bytes) / total_bytes
    return f"top {share:.0f}%"


def format_smallest_file_share(min_bytes: int | None, total_bytes: int) -> str:
    if min_bytes is None or total_bytes <= 0:
        return "min n/a"
    share = (100 * min_bytes) / total_bytes
    return f"min {share:.0f}%"


def format_byte_delta(min_bytes: int | None, max_bytes: int | None) -> str:
    if min_bytes is None or max_bytes is None:
        return "delta n/a"
    return f"delta {format_byte_count(abs(max_bytes - min_bytes))}"


def format_share_gap(min_bytes: int | None, max_bytes: int | None, total_bytes: int) -> str:
    if min_bytes is None or max_bytes is None or total_bytes <= 0:
        return "gap n/a"
    gap = ((max_bytes - min_bytes) * 100) / total_bytes
    return f"gap {gap:.0f}pp"


def format_max_to_median_ratio(max_bytes: int | None, median_bytes: int | None) -> str:
    if max_bytes is None or median_bytes is None or median_bytes <= 0:
        return "max/med n/a"
    ratio = max_bytes / median_bytes
    if ratio < 10:
        return f"max/med {ratio:.1f}x"
    return f"max/med {ratio:.0f}x"


def format_byte_totals(items: list[dict[str, object]]) -> dict[str, str]:
    totals: dict[str, int] = {}
    for artifact in items:
        artifact_format = str(artifact.get("format") or "").strip().lower() or "other"
        artifact_bytes = artifact.get("bytes")
        if isinstance(artifact_bytes, int):
            totals[artifact_format] = totals.get(artifact_format, 0) + artifact_bytes
    return {artifact_format: format_byte_count(total_bytes) for artifact_format, total_bytes in totals.items()}


def byte_sizes_by_site_path(items: list[dict[str, object]]) -> dict[str, str]:
    return {
        str(artifact["site_path"]): format_byte_count(int(artifact["bytes"]))
        for artifact in items
        if isinstance(artifact, dict)
        and isinstance(artifact.get("site_path"), str)
        and isinstance(artifact.get("bytes"), int)
    }


def byte_shares_by_site_path(items: list[dict[str, object]]) -> dict[str, str]:
    section_total_bytes = sum(
        int(artifact["bytes"])
        for artifact in items
        if isinstance(artifact, dict)
        and isinstance(artifact.get("site_path"), str)
        and isinstance(artifact.get("bytes"), int)
    )
    return {
        str(artifact["site_path"]): format_byte_share(int(artifact["bytes"]), section_total_bytes)
        for artifact in items
        if isinstance(artifact, dict)
        and isinstance(artifact.get("site_path"), str)
        and isinstance(artifact.get("bytes"), int)
    }


def format_average_byte_sizes(items: list[dict[str, object]]) -> dict[str, str]:
    totals: dict[str, int] = {}
    counts: dict[str, int] = {}
    for artifact in items:
        artifact_format = str(artifact.get("format") or "").strip().lower() or "other"
        counts[artifact_format] = counts.get(artifact_format, 0) + 1
        artifact_bytes = artifact.get("bytes")
        if isinstance(artifact_bytes, int):
            totals[artifact_format] = totals.get(artifact_format, 0) + artifact_bytes
    return {
        artifact_format: format_average_byte_count(totals.get(artifact_format, 0), count)
        for artifact_format, count in counts.items()
    }


def format_byte_shares(items: list[dict[str, object]]) -> dict[str, str]:
    totals: dict[str, int] = {}
    section_total_bytes = 0
    for artifact in items:
        artifact_format = str(artifact.get("format") or "").strip().lower() or "other"
        artifact_bytes = artifact.get("bytes")
        if isinstance(artifact_bytes, int):
            totals[artifact_format] = totals.get(artifact_format, 0) + artifact_bytes
            section_total_bytes += artifact_bytes
    return {
        artifact_format: format_byte_share(total_bytes, section_total_bytes)
        for artifact_format, total_bytes in totals.items()
    }


def format_count_shares(items: list[dict[str, object]]) -> dict[str, str]:
    counts: dict[str, int] = {}
    section_item_count = 0
    for artifact in items:
        artifact_format = str(artifact.get("format") or "").strip().lower() or "other"
        counts[artifact_format] = counts.get(artifact_format, 0) + 1
        section_item_count += 1
    return {
        artifact_format: format_count_share(item_count, section_item_count)
        for artifact_format, item_count in counts.items()
    }


def format_byte_ranges(items: list[dict[str, object]]) -> dict[str, str]:
    minimums: dict[str, int] = {}
    maximums: dict[str, int] = {}
    for artifact in items:
        artifact_format = str(artifact.get("format") or "").strip().lower() or "other"
        artifact_bytes = artifact.get("bytes")
        if not isinstance(artifact_bytes, int):
            continue
        if artifact_format not in minimums or artifact_bytes < minimums[artifact_format]:
            minimums[artifact_format] = artifact_bytes
        if artifact_format not in maximums or artifact_bytes > maximums[artifact_format]:
            maximums[artifact_format] = artifact_bytes
    return {
        artifact_format: format_byte_range(minimums.get(artifact_format), maximums.get(artifact_format))
        for artifact_format in minimums
    }


def format_median_byte_sizes(items: list[dict[str, object]]) -> dict[str, str]:
    byte_values: dict[str, list[int]] = {}
    for artifact in items:
        artifact_format = str(artifact.get("format") or "").strip().lower() or "other"
        artifact_bytes = artifact.get("bytes")
        if not isinstance(artifact_bytes, int):
            continue
        byte_values.setdefault(artifact_format, []).append(artifact_bytes)
    return {
        artifact_format: format_median_byte_count(values)
        for artifact_format, values in byte_values.items()
    }


def format_byte_spread_ratios(items: list[dict[str, object]]) -> dict[str, str]:
    minimums: dict[str, int] = {}
    maximums: dict[str, int] = {}
    for artifact in items:
        artifact_format = str(artifact.get("format") or "").strip().lower() or "other"
        artifact_bytes = artifact.get("bytes")
        if not isinstance(artifact_bytes, int):
            continue
        if artifact_format not in minimums or artifact_bytes < minimums[artifact_format]:
            minimums[artifact_format] = artifact_bytes
        if artifact_format not in maximums or artifact_bytes > maximums[artifact_format]:
            maximums[artifact_format] = artifact_bytes
    return {
        artifact_format: format_byte_spread_ratio(minimums.get(artifact_format), maximums.get(artifact_format))
        for artifact_format in minimums
    }


def format_top_file_shares(items: list[dict[str, object]]) -> dict[str, str]:
    totals: dict[str, int] = {}
    maximums: dict[str, int] = {}
    for artifact in items:
        artifact_format = str(artifact.get("format") or "").strip().lower() or "other"
        artifact_bytes = artifact.get("bytes")
        if not isinstance(artifact_bytes, int):
            continue
        totals[artifact_format] = totals.get(artifact_format, 0) + artifact_bytes
        if artifact_format not in maximums or artifact_bytes > maximums[artifact_format]:
            maximums[artifact_format] = artifact_bytes
    return {
        artifact_format: format_top_file_share(maximums.get(artifact_format), totals.get(artifact_format, 0))
        for artifact_format in maximums
    }


def format_smallest_file_shares(items: list[dict[str, object]]) -> dict[str, str]:
    totals: dict[str, int] = {}
    minimums: dict[str, int] = {}
    for artifact in items:
        artifact_format = str(artifact.get("format") or "").strip().lower() or "other"
        artifact_bytes = artifact.get("bytes")
        if not isinstance(artifact_bytes, int):
            continue
        totals[artifact_format] = totals.get(artifact_format, 0) + artifact_bytes
        if artifact_format not in minimums or artifact_bytes < minimums[artifact_format]:
            minimums[artifact_format] = artifact_bytes
    return {
        artifact_format: format_smallest_file_share(minimums.get(artifact_format), totals.get(artifact_format, 0))
        for artifact_format in minimums
    }


def format_byte_deltas(items: list[dict[str, object]]) -> dict[str, str]:
    minimums: dict[str, int] = {}
    maximums: dict[str, int] = {}
    for artifact in items:
        artifact_format = str(artifact.get("format") or "").strip().lower() or "other"
        artifact_bytes = artifact.get("bytes")
        if not isinstance(artifact_bytes, int):
            continue
        if artifact_format not in minimums or artifact_bytes < minimums[artifact_format]:
            minimums[artifact_format] = artifact_bytes
        if artifact_format not in maximums or artifact_bytes > maximums[artifact_format]:
            maximums[artifact_format] = artifact_bytes
    return {
        artifact_format: format_byte_delta(minimums.get(artifact_format), maximums.get(artifact_format))
        for artifact_format in minimums
    }


def format_share_gaps(items: list[dict[str, object]]) -> dict[str, str]:
    totals: dict[str, int] = {}
    minimums: dict[str, int] = {}
    maximums: dict[str, int] = {}
    for artifact in items:
        artifact_format = str(artifact.get("format") or "").strip().lower() or "other"
        artifact_bytes = artifact.get("bytes")
        if not isinstance(artifact_bytes, int):
            continue
        totals[artifact_format] = totals.get(artifact_format, 0) + artifact_bytes
        if artifact_format not in minimums or artifact_bytes < minimums[artifact_format]:
            minimums[artifact_format] = artifact_bytes
        if artifact_format not in maximums or artifact_bytes > maximums[artifact_format]:
            maximums[artifact_format] = artifact_bytes
    return {
        artifact_format: format_share_gap(
            minimums.get(artifact_format),
            maximums.get(artifact_format),
            totals.get(artifact_format, 0),
        )
        for artifact_format in minimums
    }


def format_max_to_median_ratios(items: list[dict[str, object]]) -> dict[str, str]:
    maximums: dict[str, int] = {}
    byte_values: dict[str, list[int]] = {}
    for artifact in items:
        artifact_format = str(artifact.get("format") or "").strip().lower() or "other"
        artifact_bytes = artifact.get("bytes")
        if not isinstance(artifact_bytes, int):
            continue
        byte_values.setdefault(artifact_format, []).append(artifact_bytes)
        if artifact_format not in maximums or artifact_bytes > maximums[artifact_format]:
            maximums[artifact_format] = artifact_bytes
    return {
        artifact_format: format_max_to_median_ratio(
            maximums.get(artifact_format),
            median_byte_value(values),
        )
        for artifact_format, values in byte_values.items()
    }


def manifest_generated_download_total_bytes(workspace: Path, pipeline_manifest: dict[str, object]) -> int:
    total_bytes = 0
    seen_paths: set[str] = set()
    for artifact in pipeline_manifest.get("generated_outputs", []):
        if not isinstance(artifact, dict):
            continue
        path = str(artifact.get("path") or "")
        if not path.startswith("data/"):
            continue
        if Path(path).suffix.lower() not in {".json", ".csv"}:
            continue
        artifact_path = workspace / path
        if path in seen_paths or not artifact_path.exists():
            continue
        total_bytes += int(artifact.get("bytes") or artifact_path.stat().st_size)
        seen_paths.add(path)

    manifest_path = workspace / "data" / "pipeline_manifest.json"
    if "data/pipeline_manifest.json" not in seen_paths and manifest_path.exists():
        total_bytes += manifest_path.stat().st_size
    return total_bytes


def manifest_generated_download_items(workspace: Path, pipeline_manifest: dict[str, object]) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    seen_paths: set[str] = set()
    for artifact in pipeline_manifest.get("generated_outputs", []):
        if not isinstance(artifact, dict):
            continue
        path = str(artifact.get("path") or "")
        if not path.startswith("data/"):
            continue
        if Path(path).suffix.lower() not in {".json", ".csv"}:
            continue
        artifact_path = workspace / path
        if path in seen_paths or not artifact_path.exists():
            continue
        items.append(
            {
                "site_path": path,
                "format": Path(path).suffix.lstrip(".").lower(),
                "bytes": int(artifact.get("bytes") or artifact_path.stat().st_size),
            }
        )
        seen_paths.add(path)

    manifest_path = workspace / "data" / "pipeline_manifest.json"
    if "data/pipeline_manifest.json" not in seen_paths and manifest_path.exists():
        items.append(
            {
                "site_path": "data/pipeline_manifest.json",
                "format": "json",
                "bytes": manifest_path.stat().st_size,
            }
        )
    return items


def source_pipeline_artifact_total_bytes(
    pipeline_manifest: dict[str, object],
    artifact_key: str,
) -> int:
    source_pipeline_diagnostics = pipeline_manifest.get("source_pipeline_diagnostics", {})
    if not isinstance(source_pipeline_diagnostics, dict):
        return 0
    total_bytes = 0
    for artifact in source_pipeline_diagnostics.get(artifact_key, []):
        if not isinstance(artifact, dict):
            continue
        artifact_bytes = artifact.get("bytes")
        if isinstance(artifact_bytes, int):
            total_bytes += artifact_bytes
    return total_bytes


def test_build_site_outputs_pages_assets_and_copied_json(tmp_path: Path) -> None:
    workspace = prepare_workspace(tmp_path)

    run_site_build(workspace)

    site_root = workspace / "site"
    assert (site_root / ".nojekyll").exists()
    assert (site_root / "index.html").exists()
    assert (site_root / "methodology.html").exists()
    assert (site_root / "data.html").exists()
    assert (site_root / "assets" / "site.css").exists()
    assert (site_root / "assets" / "site.js").exists()
    assert (site_root / "assets" / "charts" / "category_share_map.png").exists()
    assert (site_root / "assets" / "charts" / "category_share_map.svg").exists()
    assert (site_root / "data" / "metrics.json").exists()
    assert (site_root / "data" / "category_summary.csv").exists()
    assert (site_root / "data" / "business_model_summary.csv").exists()
    assert (site_root / "data" / "gtm_model_summary.csv").exists()
    assert (site_root / "data" / "revenue_band_summary.csv").exists()
    assert (site_root / "data" / "public_source_pages.csv").exists()
    assert (site_root / "data" / "publication_input.json").exists()
    assert (site_root / "data" / "validation_report.json").exists()
    assert (site_root / "data" / "source_coverage_report.json").exists()
    assert (site_root / "data" / "source_pipeline_diagnostics.json").exists()
    assert (site_root / "data" / "pipeline_manifest.json").exists()
    assert (site_root / "data" / "source_pipeline" / "snapshots" / "run_manifest.json").exists()
    assert (site_root / "data" / "source_pipeline" / "processed" / "validation_report.json").exists()
    assert (site_root / "data" / "source_pipeline" / "processed" / "heuristic_override_report.json").exists()
    assert (site_root / "data" / "source_pipeline" / "processed" / "suspicious_duplicates.json").exists()
    assert (site_root / "data" / "source_pipeline" / "processed" / "detail_page_rows.csv").exists()
    assert (site_root / "data" / "source_pipeline" / "processed" / "detail_field_coverage.json").exists()

    index_html = (site_root / "index.html").read_text(encoding="utf-8")
    methodology_html = (site_root / "methodology.html").read_text(encoding="utf-8")
    data_html = (site_root / "data.html").read_text(encoding="utf-8")
    site_css = (site_root / "assets" / "site.css").read_text(encoding="utf-8")
    site_js = (site_root / "assets" / "site.js").read_text(encoding="utf-8")
    pipeline_manifest = json.loads((site_root / "data" / "pipeline_manifest.json").read_text(encoding="utf-8"))
    publication_total_bytes = format_byte_count(manifest_generated_download_total_bytes(workspace, pipeline_manifest))
    staged_total_bytes = format_byte_count(
        source_pipeline_artifact_total_bytes(pipeline_manifest, "downloadable_staged_artifacts")
    )
    fetch_failure_total_bytes = format_byte_count(
        source_pipeline_artifact_total_bytes(pipeline_manifest, "downloadable_fetch_failure_artifacts")
    )
    publication_format_byte_totals = format_byte_totals(
        manifest_generated_download_items(workspace, pipeline_manifest)
    )
    publication_link_byte_sizes = byte_sizes_by_site_path(
        manifest_generated_download_items(workspace, pipeline_manifest)
    )
    publication_link_byte_shares = byte_shares_by_site_path(
        manifest_generated_download_items(workspace, pipeline_manifest)
    )
    publication_format_byte_ranges = format_byte_ranges(
        manifest_generated_download_items(workspace, pipeline_manifest)
    )
    publication_format_spread_ratios = format_byte_spread_ratios(
        manifest_generated_download_items(workspace, pipeline_manifest)
    )
    publication_format_byte_deltas = format_byte_deltas(
        manifest_generated_download_items(workspace, pipeline_manifest)
    )
    publication_format_share_gaps = format_share_gaps(
        manifest_generated_download_items(workspace, pipeline_manifest)
    )
    publication_format_top_file_shares = format_top_file_shares(
        manifest_generated_download_items(workspace, pipeline_manifest)
    )
    publication_format_smallest_file_shares = format_smallest_file_shares(
        manifest_generated_download_items(workspace, pipeline_manifest)
    )
    publication_format_median_sizes = format_median_byte_sizes(
        manifest_generated_download_items(workspace, pipeline_manifest)
    )
    publication_format_max_to_median_ratios = format_max_to_median_ratios(
        manifest_generated_download_items(workspace, pipeline_manifest)
    )
    publication_format_average_sizes = format_average_byte_sizes(
        manifest_generated_download_items(workspace, pipeline_manifest)
    )
    publication_format_byte_shares = format_byte_shares(
        manifest_generated_download_items(workspace, pipeline_manifest)
    )
    publication_format_count_shares = format_count_shares(
        manifest_generated_download_items(workspace, pipeline_manifest)
    )
    staged_format_byte_totals = format_byte_totals(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_staged_artifacts"])
    )
    staged_link_byte_sizes = byte_sizes_by_site_path(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_staged_artifacts"])
    )
    staged_link_byte_shares = byte_shares_by_site_path(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_staged_artifacts"])
    )
    staged_format_byte_ranges = format_byte_ranges(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_staged_artifacts"])
    )
    staged_format_spread_ratios = format_byte_spread_ratios(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_staged_artifacts"])
    )
    staged_format_byte_deltas = format_byte_deltas(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_staged_artifacts"])
    )
    staged_format_share_gaps = format_share_gaps(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_staged_artifacts"])
    )
    staged_format_top_file_shares = format_top_file_shares(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_staged_artifacts"])
    )
    staged_format_smallest_file_shares = format_smallest_file_shares(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_staged_artifacts"])
    )
    staged_format_median_sizes = format_median_byte_sizes(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_staged_artifacts"])
    )
    staged_format_max_to_median_ratios = format_max_to_median_ratios(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_staged_artifacts"])
    )
    staged_format_average_sizes = format_average_byte_sizes(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_staged_artifacts"])
    )
    staged_format_byte_shares = format_byte_shares(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_staged_artifacts"])
    )
    staged_format_count_shares = format_count_shares(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_staged_artifacts"])
    )
    fetch_failure_format_byte_totals = format_byte_totals(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_link_byte_sizes = byte_sizes_by_site_path(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_link_byte_shares = byte_shares_by_site_path(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_format_byte_ranges = format_byte_ranges(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_format_spread_ratios = format_byte_spread_ratios(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_format_byte_deltas = format_byte_deltas(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_format_share_gaps = format_share_gaps(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_format_top_file_shares = format_top_file_shares(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_format_smallest_file_shares = format_smallest_file_shares(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_format_average_sizes = format_average_byte_sizes(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_format_byte_shares = format_byte_shares(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_format_count_shares = format_count_shares(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )

    assert "visible public sample" in index_html.lower()
    assert f'<meta http-equiv="Content-Security-Policy" content="{ESCAPED_CSP_SNIPPET}">' in index_html
    assert '<meta http-equiv="Referrer-Policy" content="no-referrer, strict-origin-when-cross-origin">' in index_html
    assert '<a class="skip-link" href="#workspace-main">Skip to workspace</a>' in index_html
    assert '<main class="workspace" id="workspace-main" tabindex="-1">' in index_html
    assert 'href="methodology.html"' in index_html
    assert 'href="data.html"' in index_html
    assert 'href="index.html" aria-current="page"' in index_html
    assert 'href="methodology.html" aria-current="page"' not in index_html
    assert 'src="assets/charts/category_share_map.png"' in index_html
    assert 'loading="lazy"' in index_html
    assert 'href="/' not in index_html
    assert 'src="/' not in index_html
    assert "249 startups" in index_html
    assert "9 duplicate startup names" in index_html
    assert "TrustMRR visible-sample terminal" in index_html
    assert "Command deck" in index_html
    assert "Route registry" in index_html
    assert "Hot outputs" in index_html
    assert "Publication outputs" in index_html
    assert "Staged provenance" in index_html
    assert "Fetch-failure evidence" in index_html
    assert "Registry cache" in index_html
    assert "Action posture" in index_html
    assert "No staged fetch-failure files are indexed in the command surface." in index_html
    assert "No retryable, blocked, or do-not-retry posture is attached to the active manifest." in index_html
    assert "6 JSON" in index_html
    assert "5 CSV" in index_html
    assert "5 JSON" in index_html
    assert "1 CSV" in index_html
    assert publication_total_bytes in index_html
    assert staged_total_bytes in index_html
    assert fetch_failure_total_bytes in index_html
    assert "Concentration ladder" in index_html
    assert "Category over-index" in index_html
    assert "Revenue checkpoints" in index_html
    assert "Top category lanes" in index_html
    assert "Representation gap" in index_html
    assert "Leader stack" in index_html
    assert "Outlier scan" in index_html
    assert "Model mix read" in index_html
    assert "Concentration curve" in index_html
    assert "These chart-adjacent story rails reuse the same static metrics plus category/business-model/GTM summaries" in index_html
    assert "These quick-read infographics reuse the same static metrics and category summaries" in index_html
    assert "Jump palette" in index_html
    assert 'aria-describedby="jump-palette-help jump-palette-shortcuts"' in index_html
    assert 'aria-keyshortcuts="/ Control+K"' in index_html
    assert 'aria-controls="workspace-main"' in index_html
    assert 'id="jump-palette-shortcuts"' in index_html
    assert "Focus palette" in index_html
    assert "Exit search" in index_html
    assert "Cycle local panels" in index_html
    assert "<kbd>/</kbd><kbd>Ctrl+K</kbd>" in index_html
    assert "<kbd>Enter</kbd>" in index_html
    assert "<kbd>Esc</kbd>" in index_html
    assert "<kbd>[</kbd><kbd>]</kbd>" in index_html
    assert 'class="command-status" role="status" aria-live="polite" aria-atomic="true" data-command-status' in index_html
    assert "GO / Overview" in index_html
    assert "GO / Data" in index_html
    assert "GO / Downloads" in index_html
    assert "GET metrics.json" in index_html
    assert "GET category_summary.csv" in index_html
    assert "GET public_source_pages.csv" in index_html
    assert "GET source_pipeline/snapshots/run_manifest.json" in index_html
    assert "GET source_pipeline/processed/detail_page_rows.csv" in index_html
    route_map_section = extract_rail_module(index_html, "Route map")
    command_deck_section = extract_rail_module(index_html, "Command deck")
    route_registry_section = extract_rail_module(index_html, "Route registry")
    operating_mode_section = extract_rail_module(index_html, "Operating mode")
    system_brief_section = extract_rail_module(index_html, "System brief")
    build_path_section = extract_rail_module(index_html, "Build path")
    revenue_lens_section = extract_rail_module(index_html, "Revenue lens")
    concentration_monitor_section = extract_rail_module(index_html, "Concentration monitor")
    warning_posture_section = extract_rail_module(index_html, "Warning posture")
    overview_hero_aside = extract_hero_aside(index_html, "Current build snapshot")
    index_brand = extract_brand(index_html)
    index_primary_nav = extract_primary_nav(index_html)
    index_page_panels_nav = extract_page_panels_nav(index_html)
    index_route_registry_nav = extract_route_registry_nav(index_html)
    index_ticker_strip = extract_ticker_strip(index_html)
    index_footer = extract_site_footer(index_html)
    index_workspace_command = extract_workspace_command(index_html)
    assert "Route spread" in route_map_section
    assert "3 routes keep overview, methodology, and data one jump away." in route_map_section
    assert ">Overview<" in route_map_section
    assert ">active<" in route_map_section
    assert ">Methodology<" in route_map_section
    assert ">Data<" in route_map_section
    assert ">standby<" in route_map_section
    assert "Deck spread" in command_deck_section
    assert "5 local jumps map the /index surface from #top to #category-leaders." in command_deck_section
    assert "Top anchor" in command_deck_section
    assert "Section anchors" in command_deck_section
    assert "Current route" in command_deck_section
    assert "/index" in command_deck_section
    assert "Cross-page span" in route_registry_section
    assert "5 global jumps keep top-level routes and data pivots one jump away." in route_registry_section
    assert "Page roots" in route_registry_section
    assert "Direct anchors" in route_registry_section
    assert "Data-bound" in route_registry_section
    assert "Mode contract" in operating_mode_section
    assert "Passed with warnings validation stays locked to a static, visible-public-sample shell." in operating_mode_section
    assert "5 local jumps, 5 cross-page routes, and 3 hot-output blocks" in operating_mode_section
    assert "Validation" in operating_mode_section
    assert "Runtime" in operating_mode_section
    assert "Claim scope" in operating_mode_section
    assert "static pages" in operating_mode_section
    assert "visible sample" in operating_mode_section
    assert "Surface brief" in system_brief_section
    assert "Passed with warnings validation keeps /index routed through the static operator shell." in system_brief_section
    assert "Current route" in system_brief_section
    assert "Validation" in system_brief_section
    assert "Surface" in system_brief_section
    assert "/index" in system_brief_section
    assert "Overview" in system_brief_section
    assert "Build contract" in build_path_section
    assert "`python src/build_site.py` rebuilds 5 local panels, 5 cross-page routes, and 3 hot-output blocks into static pages." in build_path_section
    assert "Local panels" in build_path_section
    assert "Cross-page routes" in build_path_section
    assert "Hot-output blocks" in build_path_section
    assert "Revenue stack" in revenue_lens_section
    assert "$24.39M of visible 30-day revenue is spread across 249 published startups." in revenue_lens_section
    assert "Visible revenue" in revenue_lens_section
    assert "Median startup" in revenue_lens_section
    assert "Sample size" in revenue_lens_section
    assert "Top-tail risk" in concentration_monitor_section
    assert "69.6% of visible revenue sits in the top 10 startups while E-commerce leads the category mix." in concentration_monitor_section
    assert "Top 10 share" in concentration_monitor_section
    assert "Leader share" in concentration_monitor_section
    assert "Leader" in concentration_monitor_section
    assert "Warning envelope" in warning_posture_section
    assert "Passed with warnings validation still tracks 9 duplicate startup names in the visible sample." in warning_posture_section
    assert "Duplicate names" in warning_posture_section
    assert "Heuristic gaps" in warning_posture_section
    assert "Snapshot aperture" in overview_hero_aside
    assert "249 visible startups and $24.39M anchor the current published snapshot." in overview_hero_aside
    assert "Sample size" in overview_hero_aside
    assert "Visible revenue" in overview_hero_aside
    assert "Validation" in overview_hero_aside
    assert "Current build snapshot" in overview_hero_aside
    assert "249 startups" in overview_hero_aside
    assert "fancyMMR" in index_brand
    assert "TrustMRR visible-sample terminal" in index_brand
    assert "Header status" in index_brand
    assert "Publication" in index_brand
    assert "Source-derived sample" in index_brand
    assert "Validation" in index_brand
    assert "Passed with warnings" in index_brand
    assert "Route" in index_brand
    assert "/index active" in index_brand
    assert "Overview" in index_primary_nav
    assert "/index" in index_primary_nav
    assert "active" in index_primary_nav
    assert "Methodology" in index_primary_nav
    assert "/methodology" in index_primary_nav
    assert "standby" in index_primary_nav
    assert "Data" in index_primary_nav
    assert "/data" in index_primary_nav
    assert 'class="nav-link is-active"' in index_primary_nav
    assert 'class="nav-link"' in index_primary_nav
    assert "nav-link-badge nav-link-badge-route" in index_primary_nav
    assert "nav-link-badge is-active" in index_primary_nav
    assert "nav-link-badge is-standby" in index_primary_nav
    assert "OV.00 Top" in index_page_panels_nav
    assert "OV.01 Snapshot" in index_page_panels_nav
    assert "OV.04 Leaders" in index_page_panels_nav
    assert "command-deck-link" in index_page_panels_nav
    assert "command-deck-label" in index_page_panels_nav
    assert "command-deck-meta" in index_page_panels_nav
    assert "command-deck-badge command-deck-badge-order" in index_page_panels_nav
    assert "command-deck-badge command-deck-badge-anchor" in index_page_panels_nav
    assert "01/05" in index_page_panels_nav
    assert "05/05" in index_page_panels_nav
    assert "#top" in index_page_panels_nav
    assert "#snapshot" in index_page_panels_nav
    assert "#category-leaders" in index_page_panels_nav
    assert "GO / Overview" in index_route_registry_nav
    assert "GO / Methodology" in index_route_registry_nav
    assert "GO / Data" in index_route_registry_nav
    assert "GO / Downloads" in index_route_registry_nav
    assert "GO / Diagnostics" in index_route_registry_nav
    assert "route-registry-link" in index_route_registry_nav
    assert "route-registry-label" in index_route_registry_nav
    assert "route-registry-meta" in index_route_registry_nav
    assert "route-registry-badge route-registry-badge-target" in index_route_registry_nav
    assert "route-registry-badge route-registry-badge-kind" in index_route_registry_nav
    assert "index.html" in index_route_registry_nav
    assert "methodology.html" in index_route_registry_nav
    assert "data.html#downloads" in index_route_registry_nav
    assert "data.html#source-pipeline-diagnostics" in index_route_registry_nav
    assert ">route<" in index_route_registry_nav
    assert "surface" in index_ticker_strip
    assert "static pages" in index_ticker_strip
    assert "validation" in index_ticker_strip
    assert "Passed with warnings" in index_ticker_strip
    assert "route" in index_ticker_strip
    assert "overview" in index_ticker_strip
    assert "local panels" in index_ticker_strip
    assert "5 indexed" in index_ticker_strip
    assert "global commands" in index_ticker_strip
    assert "22 indexed" in index_ticker_strip
    assert "hot outputs" in index_ticker_strip
    assert "17 files" in index_ticker_strip
    assert "mode" in index_ticker_strip
    assert "visible public sample" in index_ticker_strip
    assert "Provenance lock" in index_footer
    assert "Passed with warnings validation keeps /index published as a source-derived visible sample, not a full platform export." in index_footer
    assert "Validation" in index_footer
    assert "Current route" in index_footer
    assert "Claim scope" in index_footer
    assert "Build pack" in index_footer
    assert "4 checked-in input zones plus 5 local panels and 22 global commands keep the shell reproducible." in index_footer
    assert "Input zones" in index_footer
    assert "Local panels" in index_footer
    assert "Global commands" in index_footer
    assert "Hosting contract" in index_footer
    assert "Static pages currently expose 17 hot-output files with no runtime server, authenticated backend, or hidden publication layer." in index_footer
    assert "Surface" in index_footer
    assert "Hot outputs" in index_footer
    assert "Backend" in index_footer
    assert "Route scope" in index_workspace_command
    assert "/index keeps 5 local jumps and 22 global commands one query away from the jump palette." in index_workspace_command
    assert "Current route" in index_workspace_command
    assert "Local panels" in index_workspace_command
    assert "Global commands" in index_workspace_command
    assert "Anchor spread" in index_workspace_command
    assert "1 top anchor and 4 section anchors map the /index surface from #top to #category-leaders." in index_workspace_command
    assert "Top anchor" in index_workspace_command
    assert "Section anchors" in index_workspace_command
    assert "Furthest anchor" in index_workspace_command
    assert "Asset scope" in index_workspace_command
    assert "5 cross-page routes and 17 hot-output files expand the command surface beyond local panel anchors." in index_workspace_command
    assert "Cross-page routes" in index_workspace_command
    assert "Hot outputs" in index_workspace_command
    assert "Global scope" in index_workspace_command
    publication_output_section = extract_hot_output_section(index_html, "Publication outputs")
    staged_output_section = extract_hot_output_section(index_html, "Staged provenance")
    assert "Registry shape" in publication_output_section
    assert "Signal anchors" in publication_output_section
    assert "keep the publication command surface self-contained at" in publication_output_section
    assert publication_total_bytes in publication_output_section
    assert "Diagnostics, coverage, and manifest JSON dominate the publication registry byte weight." in publication_output_section
    assert "JSON files" in publication_output_section
    assert "CSV files" in publication_output_section
    assert "Total bytes" in publication_output_section
    assert "Source-pipeline diagnostics" in publication_output_section
    assert "Source coverage report" in publication_output_section
    assert "Pipeline manifest" in publication_output_section
    assert 'class="rail-command-link output-registry-link"' in publication_output_section
    assert "output-registry-label" in publication_output_section
    assert "output-registry-meta" in publication_output_section
    assert 'class="output-registry-badge output-registry-badge-target">data/metrics.json<' in publication_output_section
    assert 'class="output-registry-badge output-registry-badge-format">JSON<' in publication_output_section
    assert 'class="output-registry-badge output-registry-badge-target">data/business_model_summary.csv<' in publication_output_section
    assert 'class="output-registry-badge output-registry-badge-format">CSV<' in publication_output_section
    assert (
        f'class="output-registry-badge output-registry-badge-bytes">'
        f'{publication_link_byte_sizes["data/metrics.json"]}<'
    ) in publication_output_section
    assert (
        f'class="output-registry-badge output-registry-badge-bytes">'
        f'{publication_link_byte_sizes["data/business_model_summary.csv"]}<'
    ) in publication_output_section
    assert (
        f'class="output-registry-badge output-registry-badge-share">'
        f'{publication_link_byte_shares["data/metrics.json"]}<'
    ) in publication_output_section
    assert (
        f'class="output-registry-badge output-registry-badge-share">'
        f'{publication_link_byte_shares["data/business_model_summary.csv"]}<'
    ) in publication_output_section
    assert "Provenance pack" in staged_output_section
    assert "Staging posture" in staged_output_section
    assert staged_total_bytes in staged_output_section
    assert "of staged provenance one jump away." in staged_output_section
    assert "852 target detail pages remain opt-in, with 852 still not requested in the promoted run." in staged_output_section
    assert "JSON files" in staged_output_section
    assert "CSV files" in staged_output_section
    assert "Not requested" in staged_output_section
    assert "Fetched" in staged_output_section
    assert "Parsed" in staged_output_section
    assert "Staged detail rows" in staged_output_section
    assert 'class="rail-command-link output-registry-link"' in staged_output_section
    assert 'class="output-registry-badge output-registry-badge-target">data/source_pipeline/processed/detail_page_rows.csv<' in staged_output_section
    assert 'class="output-registry-badge output-registry-badge-target">data/source_pipeline/snapshots/run_manifest.json<' in staged_output_section
    assert 'class="output-registry-badge output-registry-badge-format">CSV<' in staged_output_section
    assert 'class="output-registry-badge output-registry-badge-format">JSON<' in staged_output_section
    assert (
        f'class="output-registry-badge output-registry-badge-bytes">'
        f'{staged_link_byte_sizes["data/source_pipeline/processed/detail_page_rows.csv"]}<'
    ) in staged_output_section
    assert (
        f'class="output-registry-badge output-registry-badge-bytes">'
        f'{staged_link_byte_sizes["data/source_pipeline/snapshots/run_manifest.json"]}<'
    ) in staged_output_section
    assert (
        f'class="output-registry-badge output-registry-badge-share">'
        f'{staged_link_byte_shares["data/source_pipeline/processed/detail_page_rows.csv"]}<'
    ) in staged_output_section
    assert (
        f'class="output-registry-badge output-registry-badge-share">'
        f'{staged_link_byte_shares["data/source_pipeline/snapshots/run_manifest.json"]}<'
    ) in staged_output_section
    assert 'rail-command-divider-label">CSV<' in publication_output_section
    assert 'rail-command-divider-label">JSON<' in publication_output_section
    assert 'rail-command-divider-count">5<' in publication_output_section
    assert 'rail-command-divider-count">6<' in publication_output_section
    assert publication_format_byte_totals["csv"] in publication_output_section
    assert publication_format_byte_totals["json"] in publication_output_section
    assert publication_format_byte_ranges["csv"] in publication_output_section
    assert publication_format_byte_ranges["json"] in publication_output_section
    assert publication_format_spread_ratios["csv"] in publication_output_section
    assert publication_format_spread_ratios["json"] in publication_output_section
    assert publication_format_byte_deltas["csv"] in publication_output_section
    assert publication_format_byte_deltas["json"] in publication_output_section
    assert publication_format_share_gaps["csv"] in publication_output_section
    assert publication_format_share_gaps["json"] in publication_output_section
    assert publication_format_top_file_shares["csv"] in publication_output_section
    assert publication_format_top_file_shares["json"] in publication_output_section
    assert publication_format_smallest_file_shares["csv"] in publication_output_section
    assert publication_format_smallest_file_shares["json"] in publication_output_section
    assert publication_format_median_sizes["csv"] in publication_output_section
    assert publication_format_median_sizes["json"] in publication_output_section
    assert publication_format_max_to_median_ratios["csv"] in publication_output_section
    assert publication_format_max_to_median_ratios["json"] in publication_output_section
    assert publication_format_average_sizes["csv"] in publication_output_section
    assert publication_format_average_sizes["json"] in publication_output_section
    assert publication_format_byte_shares["csv"] in publication_output_section
    assert publication_format_byte_shares["json"] in publication_output_section
    assert publication_format_count_shares["csv"] in publication_output_section
    assert publication_format_count_shares["json"] in publication_output_section
    assert 'rail-command-divider-label">CSV<' in staged_output_section
    assert 'rail-command-divider-label">JSON<' in staged_output_section
    assert 'rail-command-divider-count">1<' in staged_output_section
    assert 'rail-command-divider-count">5<' in staged_output_section
    assert staged_format_byte_totals["csv"] in staged_output_section
    assert staged_format_byte_totals["json"] in staged_output_section
    assert staged_format_byte_ranges["csv"] in staged_output_section
    assert staged_format_byte_ranges["json"] in staged_output_section
    assert staged_format_spread_ratios["csv"] in staged_output_section
    assert staged_format_spread_ratios["json"] in staged_output_section
    assert staged_format_byte_deltas["csv"] in staged_output_section
    assert staged_format_byte_deltas["json"] in staged_output_section
    assert staged_format_share_gaps["csv"] in staged_output_section
    assert staged_format_share_gaps["json"] in staged_output_section
    assert staged_format_top_file_shares["csv"] in staged_output_section
    assert staged_format_top_file_shares["json"] in staged_output_section
    assert staged_format_smallest_file_shares["csv"] in staged_output_section
    assert staged_format_smallest_file_shares["json"] in staged_output_section
    assert staged_format_median_sizes["csv"] in staged_output_section
    assert staged_format_median_sizes["json"] in staged_output_section
    assert staged_format_max_to_median_ratios["csv"] in staged_output_section
    assert staged_format_max_to_median_ratios["json"] in staged_output_section
    assert staged_format_average_sizes["csv"] in staged_output_section
    assert staged_format_average_sizes["json"] in staged_output_section
    assert staged_format_byte_shares["csv"] in staged_output_section
    assert staged_format_byte_shares["json"] in staged_output_section
    assert staged_format_count_shares["csv"] in staged_output_section
    assert staged_format_count_shares["json"] in staged_output_section
    assert_text_order(
        publication_output_section,
        [
            'rail-command-divider-label">CSV<',
            "GET business_model_summary.csv",
            "GET category_summary.csv",
            "GET gtm_model_summary.csv",
            "GET public_source_pages.csv",
            "GET revenue_band_summary.csv",
            'rail-command-divider-label">JSON<',
            "GET metrics.json",
            "GET pipeline_manifest.json",
        ],
    )
    assert_text_order(
        staged_output_section,
        [
            'rail-command-divider-label">CSV<',
            "GET source_pipeline/processed/detail_page_rows.csv",
            'rail-command-divider-label">JSON<',
            "GET source_pipeline/processed/detail_field_coverage.json",
            "GET source_pipeline/processed/heuristic_override_report.json",
            "GET source_pipeline/processed/suspicious_duplicates.json",
            "GET source_pipeline/processed/validation_report.json",
            "GET source_pipeline/snapshots/run_manifest.json",
        ],
    )
    assert 'data-command-kind="route"' in index_html
    assert 'data-command-kind="asset"' in index_html
    assert 'data-command-query="/data #downloads"' in index_html
    assert 'data-command-query="metrics.json"' in index_html
    assert 'data-command-query="category_summary.csv"' in index_html
    assert 'data-command-query="source_pipeline/snapshots/run_manifest.json"' in index_html
    assert 'src="assets/site.js"' in index_html
    assert "Ctrl+K" in index_html
    assert "data-command-input" in index_html

    assert "not a full database export" in methodology_html.lower()
    assert f'<meta http-equiv="Content-Security-Policy" content="{ESCAPED_CSP_SNIPPET}">' in methodology_html
    assert '<meta http-equiv="Referrer-Policy" content="no-referrer, strict-origin-when-cross-origin">' in methodology_html
    assert '<a class="skip-link" href="#workspace-main">Skip to workspace</a>' in methodology_html
    assert '<main class="workspace" id="workspace-main" tabindex="-1">' in methodology_html
    assert 'href="methodology.html" aria-current="page"' in methodology_html
    assert 'href="index.html" aria-current="page"' not in methodology_html
    assert "source-derived visible sample" in methodology_html.lower()
    assert "passed with warnings" in methodology_html.lower()
    assert "data/source_pipeline/" in methodology_html
    assert "license-code-mit.txt" in methodology_html.lower()
    assert "9 duplicate names / 0 heuristic gaps" in methodology_html
    assert "Signal board" in methodology_html
    assert "Visible-sample contract" in methodology_html
    assert "Gate posture" in methodology_html
    assert "Warning envelope" in methodology_html
    assert "Evidence surface" in methodology_html
    assert "MD.01 Signals" in methodology_html
    assert "These methodology-side infographics reuse the same validation report and hot-output registry metadata" in methodology_html
    methodology_inclusion_rule_section = extract_rail_module(methodology_html, "Inclusion rule")
    methodology_validation_posture_section = extract_rail_module(methodology_html, "Validation posture")
    methodology_publication_caveat_section = extract_rail_module(methodology_html, "Publication caveat")
    methodology_hero_aside = extract_hero_aside(methodology_html, "Warning-only signals")
    methodology_brand = extract_brand(methodology_html)
    methodology_primary_nav = extract_primary_nav(methodology_html)
    methodology_page_panels_nav = extract_page_panels_nav(methodology_html)
    methodology_route_registry_nav = extract_route_registry_nav(methodology_html)
    methodology_ticker_strip = extract_ticker_strip(methodology_html)
    methodology_footer = extract_site_footer(methodology_html)
    methodology_workspace_command = extract_workspace_command(methodology_html)
    assert "Gate contract" in methodology_inclusion_rule_section
    assert "The methodology keeps the visible sample tied to the public-page &gt;= $5,000 / 30d inclusion rule." in methodology_inclusion_rule_section
    assert "Threshold" in methodology_inclusion_rule_section
    assert "&gt;= $5k" in methodology_inclusion_rule_section
    assert "Source type" in methodology_inclusion_rule_section
    assert "Sample frame" in methodology_inclusion_rule_section
    assert "Method warning load" in methodology_validation_posture_section
    assert "Passed with warnings validation keeps 9 duplicate names / 0 heuristic gaps reviewable in the methodology lane." in methodology_validation_posture_section
    assert "Validation" in methodology_validation_posture_section
    assert "Duplicate names" in methodology_validation_posture_section
    assert "Heuristic gaps" in methodology_validation_posture_section
    assert "Scope lock" in methodology_publication_caveat_section
    assert "Not a full export keeps the caveat lane tied to 3 inspectable output blocks and the repository docs." in methodology_publication_caveat_section
    assert "Output blocks" in methodology_publication_caveat_section
    assert "Doc panes" in methodology_publication_caveat_section
    assert "Claim scope" in methodology_publication_caveat_section
    assert "Warning map" in methodology_hero_aside
    assert "9 duplicate names and 0 heuristic gaps define the current warning-only methodology envelope." in methodology_hero_aside
    assert "Validation" in methodology_hero_aside
    assert "Duplicate names" in methodology_hero_aside
    assert "Heuristic gaps" in methodology_hero_aside
    assert "Warning-only signals" in methodology_hero_aside
    assert "9 duplicate names / 0 heuristic gaps" in methodology_hero_aside
    assert "Publication" in methodology_brand
    assert "Source-derived sample" in methodology_brand
    assert "Validation" in methodology_brand
    assert "Passed with warnings" in methodology_brand
    assert "Route" in methodology_brand
    assert "/methodology active" in methodology_brand
    assert "Overview" in methodology_primary_nav
    assert "/index" in methodology_primary_nav
    assert "Methodology" in methodology_primary_nav
    assert "/methodology" in methodology_primary_nav
    assert "Data" in methodology_primary_nav
    assert "/data" in methodology_primary_nav
    assert "nav-link-badge is-active" in methodology_primary_nav
    assert "nav-link-badge is-standby" in methodology_primary_nav
    assert 'href="methodology.html" aria-current="page"' in methodology_primary_nav
    assert "MD.00 Top" in methodology_page_panels_nav
    assert "MD.04 Validation" in methodology_page_panels_nav
    assert "01/05" in methodology_page_panels_nav
    assert "05/05" in methodology_page_panels_nav
    assert "#top" in methodology_page_panels_nav
    assert "#validation-checks" in methodology_page_panels_nav
    assert "command-deck-badge command-deck-badge-order" in methodology_page_panels_nav
    assert "command-deck-badge command-deck-badge-anchor" in methodology_page_panels_nav
    assert "GO / Overview" in methodology_route_registry_nav
    assert "GO / Methodology" in methodology_route_registry_nav
    assert "GO / Data" in methodology_route_registry_nav
    assert "GO / Downloads" in methodology_route_registry_nav
    assert "GO / Diagnostics" in methodology_route_registry_nav
    assert "route-registry-badge route-registry-badge-target" in methodology_route_registry_nav
    assert "route-registry-badge route-registry-badge-kind" in methodology_route_registry_nav
    assert "index.html" in methodology_route_registry_nav
    assert "methodology.html" in methodology_route_registry_nav
    assert "data.html#downloads" in methodology_route_registry_nav
    assert ">route<" in methodology_route_registry_nav
    assert "route" in methodology_ticker_strip
    assert "methodology" in methodology_ticker_strip
    assert "local panels" in methodology_ticker_strip
    assert "5 indexed" in methodology_ticker_strip
    assert "global commands" in methodology_ticker_strip
    assert "22 indexed" in methodology_ticker_strip
    assert "hot outputs" in methodology_ticker_strip
    assert "17 files" in methodology_ticker_strip
    assert "Provenance lock" in methodology_footer
    assert "Passed with warnings validation keeps /methodology published as a source-derived visible sample, not a full platform export." in methodology_footer
    assert "Build pack" in methodology_footer
    assert "4 checked-in input zones plus 5 local panels and 22 global commands keep the shell reproducible." in methodology_footer
    assert "Hosting contract" in methodology_footer
    assert "Route scope" in methodology_workspace_command
    assert "/methodology keeps 5 local jumps and 22 global commands one query away from the jump palette." in methodology_workspace_command
    assert "Anchor spread" in methodology_workspace_command
    assert "1 top anchor and 4 section anchors map the /methodology surface from #top to #validation-checks." in methodology_workspace_command
    assert "Asset scope" in methodology_workspace_command
    assert "5 cross-page routes and 17 hot-output files expand the command surface beyond local panel anchors." in methodology_workspace_command

    assert "metrics.json" in data_html
    assert f'<meta http-equiv="Content-Security-Policy" content="{ESCAPED_CSP_SNIPPET}">' in data_html
    assert '<meta http-equiv="Referrer-Policy" content="no-referrer, strict-origin-when-cross-origin">' in data_html
    assert '<a class="skip-link" href="#workspace-main">Skip to workspace</a>' in data_html
    assert '<main class="workspace" id="workspace-main" tabindex="-1">' in data_html
    assert 'href="data.html" aria-current="page"' in data_html
    assert 'href="index.html" aria-current="page"' not in data_html
    assert "Category summary" in data_html
    assert "Business model summary" in data_html
    assert "GTM model summary" in data_html
    assert "Revenue band summary" in data_html
    assert "Public source pages" in data_html
    assert "publication_input.json" in data_html
    assert "validation_report.json" in data_html
    assert "source_coverage_report.json" in data_html
    assert "source_pipeline_diagnostics.json" in data_html
    assert "pipeline_manifest.json" in data_html
    assert "Signal board" in data_html
    assert "Bundle surface" in data_html
    assert "Revenue band pressure" in data_html
    assert "Category lanes" in data_html
    assert "Source-page leaders" in data_html
    assert "Bundle shape" in data_html
    assert "Byte weight" in data_html
    assert "Heavy anchors" in data_html
    assert "Provenance pack" in data_html
    assert "Detail-row weight" in data_html
    assert "Staging posture" in data_html
    assert "These download story rails reuse the same manifest-driven artifact metadata already attached to the publication bundle" in data_html
    assert "These staged-bundle story rails reuse the same staged artifact metadata and detail-staging counts already published in the bundle" in data_html
    assert "Failure cache" in data_html
    assert "Snapshot coverage" in data_html
    assert "Recovery posture" in data_html
    assert "No fetch-failure artifacts are attached to the active manifest." in data_html
    assert "No staged fetch failures are currently recorded, so the HTML/JSON cache stays empty." in data_html
    assert "No retryable, blocked, or do-not-retry failures are currently recorded." in data_html
    assert "These fetch-failure story rails reuse the same staged failure artifact metadata and publication-facing failure counts already published in the bundle" in data_html
    assert "These data-pane infographics are derived from the same static bundle outputs mirrored below" in data_html
    assert "Staged Bundle" in data_html
    assert "Fetch Failure Snapshots" in data_html
    assert "staged source-pipeline bundle" in data_html
    assert "source_pipeline/snapshots/run_manifest.json" in data_html
    assert "source_pipeline/processed/validation_report.json" in data_html
    assert "source_pipeline/processed/heuristic_override_report.json" in data_html
    assert "source_pipeline/processed/suspicious_duplicates.json" in data_html
    assert "source_pipeline/processed/detail_page_rows.csv" in data_html
    assert "source_pipeline/processed/detail_field_coverage.json" in data_html
    assert "Source-pipeline diagnostics" in data_html
    assert "Registry posture" in data_html
    assert "Mapping surface" in data_html
    assert "Detail staging posture" in data_html
    assert "Failure posture" in data_html
    assert "These diagnostics story rails reuse the same staged source-pipeline metadata already published in the bundle" in data_html
    assert "Detail-page staging" in data_html
    assert "Fetch failures" in data_html
    assert "Fetch-failure causes" in data_html
    assert "Fetch-failure timing" in data_html
    assert "Fetch-failure source context" in data_html
    assert "Fetch-failure parser context" in data_html
    assert "Fetch-failure severity" in data_html
    assert "Fetch-failure retryability" in data_html
    assert "Fetch-failure next actions" in data_html
    assert "Fetch-failure snapshot availability" in data_html
    assert "Fetch-failure delay context" in data_html
    assert "Fetch-failure robots context" in data_html
    assert "Detail parse failures" in data_html
    assert "Detail-field coverage" in data_html
    assert "staged provenance" in data_html
    assert "No staged fetch-failure snapshot downloads are currently attached to the active manifest." in data_html
    assert "No staged source fetch failures are currently recorded for the active manifest." in data_html
    assert "No staged fetch-failure causes are currently recorded for the active manifest." in data_html
    assert "No staged fetch-failure timing is currently recorded for the active manifest." in data_html
    assert "No staged fetch-failure source-label context is currently recorded for the active manifest." in data_html
    assert "No staged fetch-failure parser-strategy context is currently recorded for the active manifest." in data_html
    assert "No staged fetch-failure severity context is currently recorded for the active manifest." in data_html
    assert "No staged fetch-failure retryability context is currently recorded for the active manifest." in data_html
    assert (
        "No staged fetch-failure next-action recommendations, source lists, source details, artifact links, artifact summaries, artifact rollups, artifact format counts, artifact-format source lists, artifact-format source-count summaries, artifact-format source-count totals, or distinct artifact-format counts are currently recorded for the active manifest."
        in data_html
    )
    assert "No staged fetch-failure snapshot-availability context is currently recorded for the active manifest." in data_html
    assert "No staged fetch-failure delay context is currently recorded for the active manifest." in data_html
    assert "No staged fetch-failure robots context is currently recorded for the active manifest." in data_html
    assert "No source pages in the active manifest currently report staged detail parse failures." in data_html
    assert "No staged detail rows in the active manifest currently populate the shared detail fields." in data_html
    assert "Staged run manifest" in data_html
    assert "Staged validation report" in data_html
    assert "Staged override coverage" in data_html
    assert "Staged duplicate review" in data_html
    assert "SHA256" in data_html
    assert "System brief" in data_html
    assert "Command deck" in data_html
    assert "Jump palette" in data_html
    assert 'aria-describedby="jump-palette-help jump-palette-shortcuts"' in data_html
    assert 'aria-keyshortcuts="/ Control+K"' in data_html
    assert "Focus palette" in data_html
    assert "Exit search" in data_html
    assert "Cycle local panels" in data_html
    assert 'class="command-status" role="status" aria-live="polite" aria-atomic="true" data-command-status' in data_html
    assert "Route registry" in data_html
    assert "Hot outputs" in data_html
    assert "Publication outputs" in data_html
    assert "Staged provenance" in data_html
    assert "Fetch-failure evidence" in data_html
    data_route_map_section = extract_rail_module(data_html, "Route map")
    data_command_deck_section = extract_rail_module(data_html, "Command deck")
    data_route_registry_section = extract_rail_module(data_html, "Route registry")
    data_operating_mode_section = extract_rail_module(data_html, "Operating mode")
    data_system_brief_section = extract_rail_module(data_html, "System brief")
    data_build_path_section = extract_rail_module(data_html, "Build path")
    publication_source_section = extract_rail_module(data_html, "Publication source")
    download_surface_section = extract_rail_module(data_html, "Download surface")
    diagnostics_feed_section = extract_rail_module(data_html, "Diagnostics feed")
    data_hero_aside = extract_hero_aside(data_html, "Manifest summary")
    data_brand = extract_brand(data_html)
    data_primary_nav = extract_primary_nav(data_html)
    data_page_panels_nav = extract_page_panels_nav(data_html)
    data_route_registry_nav = extract_route_registry_nav(data_html)
    data_ticker_strip = extract_ticker_strip(data_html)
    data_footer = extract_site_footer(data_html)
    data_workspace_command = extract_workspace_command(data_html)
    assert "Route spread" in data_route_map_section
    assert "3 routes keep overview, methodology, and data one jump away." in data_route_map_section
    assert ">Data<" in data_route_map_section
    assert ">active<" in data_route_map_section
    assert ">Overview<" in data_route_map_section
    assert ">Methodology<" in data_route_map_section
    assert ">standby<" in data_route_map_section
    assert "Deck spread" in data_command_deck_section
    assert "10 local jumps map the /data surface from #top to #manifest-notes." in data_command_deck_section
    assert "Top anchor" in data_command_deck_section
    assert "Section anchors" in data_command_deck_section
    assert "Current route" in data_command_deck_section
    assert "/data" in data_command_deck_section
    assert "Cross-page span" in data_route_registry_section
    assert "5 global jumps keep top-level routes and data pivots one jump away." in data_route_registry_section
    assert "Page roots" in data_route_registry_section
    assert "Direct anchors" in data_route_registry_section
    assert "Data-bound" in data_route_registry_section
    assert "Mode contract" in data_operating_mode_section
    assert "Passed with warnings validation stays locked to a static, visible-public-sample shell." in data_operating_mode_section
    assert "10 local jumps, 5 cross-page routes, and 3 hot-output blocks" in data_operating_mode_section
    assert "Validation" in data_operating_mode_section
    assert "Runtime" in data_operating_mode_section
    assert "Claim scope" in data_operating_mode_section
    assert "static pages" in data_operating_mode_section
    assert "visible sample" in data_operating_mode_section
    assert "Surface brief" in data_system_brief_section
    assert "Passed with warnings validation keeps /data routed through the static operator shell." in data_system_brief_section
    assert "Current route" in data_system_brief_section
    assert "Validation" in data_system_brief_section
    assert "Surface" in data_system_brief_section
    assert "/data" in data_system_brief_section
    assert "Data" in data_system_brief_section
    assert "Build contract" in data_build_path_section
    assert "`python src/build_site.py` rebuilds 10 local panels, 5 cross-page routes, and 3 hot-output blocks into static pages." in data_build_path_section
    assert "Local panels" in data_build_path_section
    assert "Cross-page routes" in data_build_path_section
    assert "Hot-output blocks" in data_build_path_section
    assert "Bundle origin" in publication_source_section
    assert "Promoted from staged source-pipeline visible rows currently feeds 249 published rows across 30 public pages." in publication_source_section
    assert "Rows" in publication_source_section
    assert "Source pages" in publication_source_section
    assert "Generated outputs" in publication_source_section
    assert "Artifact spread" in download_surface_section
    assert "11 publication files, 6 staged provenance files, and 0 fetch-failure files stay inspectable in the static download surface." in download_surface_section
    assert "Publication" in download_surface_section
    assert "Staged" in download_surface_section
    assert "Failure cache" in download_surface_section
    assert "Diagnostics posture" in diagnostics_feed_section
    assert "30/30 staged sources are attached with Passed validation and no active fetch or detail failures." in diagnostics_feed_section
    assert "Registry coverage" in diagnostics_feed_section
    assert "Validation" in diagnostics_feed_section
    assert "Fetch failures" in diagnostics_feed_section
    assert "Manifest cut" in data_hero_aside
    assert "21 generated outputs package 249 visible rows from 30 source pages into the current build." in data_hero_aside
    assert "Outputs" in data_hero_aside
    assert "Visible rows" in data_hero_aside
    assert "Diagnostics" in data_hero_aside
    assert "Manifest summary" in data_hero_aside
    assert "21 generated outputs" in data_hero_aside
    assert "Publication" in data_brand
    assert "Source-derived sample" in data_brand
    assert "Validation" in data_brand
    assert "Passed with warnings" in data_brand
    assert "Route" in data_brand
    assert "/data active" in data_brand
    assert "Overview" in data_primary_nav
    assert "/index" in data_primary_nav
    assert "Methodology" in data_primary_nav
    assert "/methodology" in data_primary_nav
    assert "Data" in data_primary_nav
    assert "/data" in data_primary_nav
    assert "nav-link-badge is-active" in data_primary_nav
    assert "nav-link-badge is-standby" in data_primary_nav
    assert 'href="data.html" aria-current="page"' in data_primary_nav
    assert "DT.00 Top" in data_page_panels_nav
    assert "DT.09 Manifest" in data_page_panels_nav
    assert "01/10" in data_page_panels_nav
    assert "10/10" in data_page_panels_nav
    assert "#top" in data_page_panels_nav
    assert "#manifest-notes" in data_page_panels_nav
    assert "command-deck-badge command-deck-badge-order" in data_page_panels_nav
    assert "command-deck-badge command-deck-badge-anchor" in data_page_panels_nav
    assert "GO / Overview" in data_route_registry_nav
    assert "GO / Methodology" in data_route_registry_nav
    assert "GO / Data" in data_route_registry_nav
    assert "GO / Downloads" in data_route_registry_nav
    assert "GO / Diagnostics" in data_route_registry_nav
    assert "route-registry-badge route-registry-badge-target" in data_route_registry_nav
    assert "route-registry-badge route-registry-badge-kind" in data_route_registry_nav
    assert "index.html" in data_route_registry_nav
    assert "data.html#downloads" in data_route_registry_nav
    assert "data.html#source-pipeline-diagnostics" in data_route_registry_nav
    assert ">route<" in data_route_registry_nav
    assert "route" in data_ticker_strip
    assert "data" in data_ticker_strip
    assert "local panels" in data_ticker_strip
    assert "10 indexed" in data_ticker_strip
    assert "global commands" in data_ticker_strip
    assert "22 indexed" in data_ticker_strip
    assert "hot outputs" in data_ticker_strip
    assert "17 files" in data_ticker_strip
    assert "Provenance lock" in data_footer
    assert "Passed with warnings validation keeps /data published as a source-derived visible sample, not a full platform export." in data_footer
    assert "Build pack" in data_footer
    assert "4 checked-in input zones plus 10 local panels and 22 global commands keep the shell reproducible." in data_footer
    assert "Hosting contract" in data_footer
    assert "Static pages currently expose 17 hot-output files with no runtime server, authenticated backend, or hidden publication layer." in data_footer
    assert "Route scope" in data_workspace_command
    assert "/data keeps 10 local jumps and 22 global commands one query away from the jump palette." in data_workspace_command
    assert "Anchor spread" in data_workspace_command
    assert "1 top anchor and 9 section anchors map the /data surface from #top to #manifest-notes." in data_workspace_command
    assert "Asset scope" in data_workspace_command
    assert "5 cross-page routes and 17 hot-output files expand the command surface beyond local panel anchors." in data_workspace_command
    assert "Registry shape" in data_html
    assert "Signal anchors" in data_html
    assert "keep the publication command surface self-contained at" in data_html
    assert "Diagnostics, coverage, and manifest JSON dominate the publication registry byte weight." in data_html
    assert "Registry cache" in data_html
    assert "Action posture" in data_html
    assert "of staged provenance one jump away." in data_html
    assert "852 target detail pages remain opt-in, with 852 still not requested in the promoted run." in data_html
    assert "No staged fetch-failure files are indexed in the command surface." in data_html
    assert "No retryable, blocked, or do-not-retry posture is attached to the active manifest." in data_html
    assert "No fetch-failure evidence is currently attached to the active manifest." in data_html
    assert "GET category_summary.csv" in data_html
    assert "GET source_pipeline/snapshots/run_manifest.json" in data_html
    assert "GET source_pipeline/processed/detail_field_coverage.json" in data_html
    assert "11 files" in data_html
    assert "6 JSON" in data_html
    assert "5 CSV" in data_html
    assert "6 files" in data_html
    assert "5 JSON" in data_html
    assert "1 CSV" in data_html
    assert "0 files" in data_html
    assert publication_total_bytes in data_html
    assert staged_total_bytes in data_html
    assert fetch_failure_total_bytes in data_html
    assert "--bg: #05070a" in site_css
    assert ".workstation {" in site_css
    assert ".skip-link {" in site_css
    assert ".chart-detail-grid {" in site_css
    assert ".chart-annotation-rail {" in site_css
    assert ".annotation-rail-grid {" in site_css
    assert ".chart-annotation-rail-standalone {" in site_css
    assert ".chart-annotation-kicker {" in site_css
    assert ".skip-link:focus {" in site_css
    assert ".skip-link:focus-visible," in site_css
    assert ".brand:focus-visible," in site_css
    assert ".nav-link:focus-visible," in site_css
    assert ".rail-command-link:focus-visible," in site_css
    assert ".command-chip:focus-visible," in site_css
    assert ".button:focus-visible {" in site_css
    assert ".command-strip {" in site_css
    assert ".brand-status {" in site_css
    assert ".brand-badge {" in site_css
    assert ".brand-badge strong {" in site_css
    assert ".brand-badge-accent {" in site_css
    assert ".nav-link-label {" in site_css
    assert ".nav-link-meta {" in site_css
    assert ".nav-link-badge {" in site_css
    assert ".nav-link-badge-route {" in site_css
    assert ".nav-link-badge.is-standby {" in site_css
    assert ".command-deck-link {" in site_css
    assert ".command-deck-label {" in site_css
    assert ".command-deck-meta {" in site_css
    assert ".command-deck-badge {" in site_css
    assert ".command-deck-badge-order {" in site_css
    assert ".command-deck-badge-anchor {" in site_css
    assert ".route-registry-link {" in site_css
    assert ".route-registry-label {" in site_css
    assert ".route-registry-meta {" in site_css
    assert ".route-registry-badge {" in site_css
    assert ".route-registry-badge-target {" in site_css
    assert ".route-registry-badge-kind {" in site_css
    assert ".output-registry-link {" in site_css
    assert ".output-registry-label {" in site_css
    assert ".output-registry-meta {" in site_css
    assert ".output-registry-badge {" in site_css
    assert ".output-registry-badge-target {" in site_css
    assert ".output-registry-badge-format {" in site_css
    assert ".output-registry-badge-bytes {" in site_css
    assert ".output-registry-badge-share {" in site_css
    assert ".command-input {" in site_css
    assert ".command-input-wrap:focus-within {" in site_css
    assert ".infographic-grid {" in site_css
    assert ".infographic-card {" in site_css
    assert ".infographic-head {" in site_css
    assert ".infographic-meter {" in site_css
    assert ".infographic-track {" in site_css
    assert ".infographic-fill.tone-red {" in site_css
    assert ".command-shortcuts {" in site_css
    assert ".shortcut-chip {" in site_css
    assert ".shortcut-keys {" in site_css
    assert ".shortcut-chip kbd {" in site_css
    assert ".workspace-command .annotation-rail-grid," in site_css
    assert ".command-surface-rails {" in site_css
    assert "@media (prefers-reduced-motion: reduce)" in site_css
    assert "scroll-behavior: auto;" in site_css
    assert "transition-duration: 0.01ms !important;" in site_css
    assert ".rail-command-group {" in site_css
    assert ".rail-command-group-head {" in site_css
    assert ".rail-command-group-meta {" in site_css
    assert ".rail-command-divider {" in site_css
    assert ".rail-command-divider-label {" in site_css
    assert ".rail-command-divider-count {" in site_css
    assert ".rail-command-divider-file-share {" in site_css
    assert ".rail-command-divider-bytes {" in site_css
    assert ".rail-command-divider-range {" in site_css
    assert ".rail-command-divider-spread {" in site_css
    assert ".rail-command-divider-delta {" in site_css
    assert ".rail-command-divider-gap {" in site_css
    assert ".rail-command-divider-top-share {" in site_css
    assert ".rail-command-divider-smallest-share {" in site_css
    assert ".rail-command-divider-median {" in site_css
    assert ".rail-command-divider-max-median {" in site_css
    assert ".rail-command-divider-average {" in site_css
    assert ".rail-command-divider-share {" in site_css
    assert ".rail-command-group-empty {" in site_css
    assert ".download-summary {" in site_css
    assert ".download-badge {" in site_css
    assert ".download-badge-format {" in site_css
    assert ".section-card.is-focused" in site_css or ".hero.is-focused" in site_css
    assert "const readStoredTarget = () => {" in site_js
    assert "const writeStoredTarget = (target) => {" in site_js
    assert "window.localStorage.getItem(storageKey)" in site_js
    assert "window.localStorage.setItem(storageKey, target)" in site_js
    assert "Some browsers and privacy contexts block storage access entirely." in site_js
    assert "const storedTarget = readStoredTarget();" in site_js
    assert "writeStoredTarget(target);" in site_js
    assert 'const shortcutHelpMessage = "Jump palette focused. Press Enter to jump, Escape to exit search, and [ or ] to cycle local panels.";'
    assert 'input.addEventListener("focus", () => {' in site_js
    assert "setStatus(shortcutHelpMessage);" in site_js
    assert 'input.addEventListener("blur", () => {' in site_js
    assert 'window.matchMedia("(prefers-reduced-motion: reduce)")' in site_js
    assert 'const scrollBehavior = () => (reducedMotionQuery && reducedMotionQuery.matches ? "auto" : "smooth");' in site_js
    assert "scrollPanelIntoView" in site_js
    assert 'event.key === "/"' in site_js
    assert 'event.key === "[" || event.key === "]"' in site_js
    assert 'command.kind === "panel"' in site_js
    assert "new URL(command.target, window.location.href)" in site_js
    assert "window.location.href = absoluteUrl.toString();" in site_js
    assert "matchedCommands" in site_js
    assert 'behavior: "smooth"' not in site_js
    assert "scrollIntoView" in site_js
    for artifact in pipeline_manifest["source_pipeline_diagnostics"]["downloadable_staged_artifacts"]:
        assert artifact["site_path"] in data_html
        assert artifact["sha256"] in data_html
        assert f"{artifact['bytes']:,} bytes" in data_html


def test_build_site_copies_manifest_driven_fetch_failure_downloads(tmp_path: Path) -> None:
    workspace = prepare_workspace(tmp_path)
    failure_dir = workspace / "data" / "fetch_failures"
    failure_dir.mkdir(parents=True, exist_ok=True)
    (failure_dir / "category--ai.html").write_text("<html><body>broken</body></html>", encoding="utf-8")
    (failure_dir / "category--ai.json").write_text(
        json.dumps(
            {
                "source_id": "category--ai",
                "url": "https://trustmrr.com/category/ai",
                "parser_strategy": "trustmrr_category_listing",
                "source_group": "category",
                "recorded_at": "2026-03-27T00:00:00Z",
                "error_type": "HTTPError",
                "message": "HTTP Error 500: server exploded",
                "status_code": 500,
                "robots": {
                    "allowed": True,
                    "effective_delay_seconds": 0.0,
                    "robots_url": "https://trustmrr.com/robots.txt",
                    "status_code": 200,
                },
                "html_snapshot_path": "data/fetch_failures/category--ai.html",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (failure_dir / "category--sales.json").write_text(
        json.dumps(
            {
                "source_id": "category--sales",
                "url": "https://trustmrr.com/category/sales",
                "parser_strategy": "trustmrr_category_listing",
                "source_group": "category",
                "recorded_at": "2026-03-27T00:05:00Z",
                "error_type": "FetchError",
                "message": "Fetching https://trustmrr.com/category/sales is disallowed by robots.txt",
                "status_code": None,
                "robots": {
                    "allowed": False,
                    "effective_delay_seconds": 15.0,
                    "robots_url": "https://trustmrr.com/robots.txt",
                    "status_code": 200,
                },
                "html_snapshot_path": None,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    run_artifact_build(workspace)
    run_site_build(workspace)

    site_root = workspace / "site"
    data_html = (site_root / "data.html").read_text(encoding="utf-8")
    pipeline_manifest = json.loads((site_root / "data" / "pipeline_manifest.json").read_text(encoding="utf-8"))
    fetch_failure_total_bytes = format_byte_count(
        source_pipeline_artifact_total_bytes(pipeline_manifest, "downloadable_fetch_failure_artifacts")
    )
    fetch_failure_format_byte_totals = format_byte_totals(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_link_byte_sizes = byte_sizes_by_site_path(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_link_byte_shares = byte_shares_by_site_path(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_format_byte_ranges = format_byte_ranges(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_format_spread_ratios = format_byte_spread_ratios(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_format_byte_deltas = format_byte_deltas(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_format_share_gaps = format_share_gaps(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_format_top_file_shares = format_top_file_shares(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_format_smallest_file_shares = format_smallest_file_shares(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_format_median_sizes = format_median_byte_sizes(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_format_max_to_median_ratios = format_max_to_median_ratios(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_format_average_sizes = format_average_byte_sizes(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_format_byte_shares = format_byte_shares(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )
    fetch_failure_format_count_shares = format_count_shares(
        list(pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"])
    )

    assert (site_root / "data" / "fetch_failures" / "category--ai.json").exists()
    assert (site_root / "data" / "fetch_failures" / "category--ai.html").exists()
    assert "Fetch failure metadata - AI" in data_html
    assert "Fetch failure HTML snapshot - AI" in data_html
    assert "Fetch failure metadata - Sales" in data_html
    assert "Fetch-failure causes" in data_html
    assert "Fetch-failure timing" in data_html
    assert "Fetch-failure source context" in data_html
    assert "Fetch-failure parser context" in data_html
    assert "Fetch-failure severity" in data_html
    assert "Fetch-failure retryability" in data_html
    assert "Fetch-failure next actions" in data_html
    assert "Affected source labels" in data_html
    assert "Affected source pages" in data_html
    assert "Action artifact rollup" in data_html
    assert "Artifact format counts" in data_html
    assert "Artifact-format source counts" in data_html
    assert "Artifact-format source-count total" in data_html
    assert "Distinct artifact formats" in data_html
    assert "Artifact format" in data_html
    assert "Format source count" in data_html
    assert "Format source labels" in data_html
    assert "Format source pages" in data_html
    assert "Failure context" in data_html
    assert "Artifact summary" in data_html
    assert "Artifact links" in data_html
    assert "Fetch-failure snapshot availability" in data_html
    assert "Fetch-failure delay context" in data_html
    assert "Fetch-failure robots context" in data_html
    assert "HTTPError" in data_html
    assert "FetchError" in data_html
    assert ">500<" in data_html
    assert "0.0s" in data_html
    assert "15.0s" in data_html
    assert "Shortest recorded effective delay" in data_html
    assert "Longest recorded effective delay" in data_html
    assert "2026-03-27T00:00:00Z" in data_html
    assert "2026-03-27T00:05:00Z" in data_html
    assert ">AI<" in data_html
    assert ">Sales<" in data_html
    assert ">category<" in data_html
    assert "trustmrr_category_listing" in data_html
    assert "policy_blocked" in data_html
    assert "server_error" in data_html
    assert "do_not_retry" in data_html
    assert "retryable" in data_html
    assert "respect_robots_policy" in data_html
    assert "retry_after_backoff" in data_html
    assert "HTTP 500" in data_html
    assert "HTTP n/a" in data_html
    assert "Fetch failure metadata - AI" in data_html
    assert "Fetch failure HTML snapshot - AI" in data_html
    assert "Fetch failure metadata - Sales" in data_html
    assert "1 artifact · JSON" in data_html
    assert "2 artifacts · HTML, JSON" in data_html
    assert "JSON: 1" in data_html
    assert "HTML: 1, JSON: 1" in data_html
    assert "JSON: 1 source" in data_html
    assert "HTML: 1 source, JSON: 1 source" in data_html
    assert "1 format-source entry" in data_html
    assert "2 format-source entries" in data_html
    assert "1 distinct artifact format" in data_html
    assert "2 distinct artifact formats" in data_html
    assert data_html.count("https://trustmrr.com/category/ai") >= 5
    assert data_html.count("https://trustmrr.com/category/sales") >= 4
    assert ">available<" in data_html
    assert ">missing<" in data_html
    assert "disallowed" in data_html
    assert "allowed" in data_html
    assert ">200<" in data_html
    assert "data/fetch_failures/category--ai.json" in data_html
    assert "data/fetch_failures/category--ai.html" in data_html
    assert "data/fetch_failures/category--sales.json" in data_html
    assert "GET fetch_failures/category--ai.json" in data_html
    assert "GET fetch_failures/category--ai.html" in data_html
    assert "GET fetch_failures/category--sales.json" in data_html
    assert "Fetch-failure evidence" in data_html
    assert "Failure cache" in data_html
    assert "Snapshot coverage" in data_html
    assert "Recovery posture" in data_html
    assert "3 artifact files cover 2 failed source pages at" in data_html
    assert "HTML captures exist for 1 of 2 failed source pages." in data_html
    assert "Retryable work and robots-blocked failures are both visible before the raw artifact list." in data_html
    assert "These fetch-failure story rails reuse the same staged failure artifact metadata and publication-facing failure counts already published in the bundle" in data_html
    assert "3 files" in data_html
    assert "2 JSON" in data_html
    assert "1 HTML" in data_html
    assert fetch_failure_total_bytes in data_html
    fetch_failure_output_section = extract_hot_output_section(data_html, "Fetch-failure evidence")
    assert "Registry cache" in fetch_failure_output_section
    assert "Action posture" in fetch_failure_output_section
    assert "3 files keep 2 failed source pages one jump away at" in fetch_failure_output_section
    assert fetch_failure_total_bytes in fetch_failure_output_section
    assert "Retryable work and robots-blocked cases are both visible before the artifact list." in fetch_failure_output_section
    assert "JSON files" in fetch_failure_output_section
    assert "HTML files" in fetch_failure_output_section
    assert "Failed sources" in fetch_failure_output_section
    assert "Retryable" in fetch_failure_output_section
    assert "Do not retry" in fetch_failure_output_section
    assert "Robots blocked" in fetch_failure_output_section
    assert 'class="rail-command-link output-registry-link"' in fetch_failure_output_section
    assert 'class="output-registry-badge output-registry-badge-target">data/fetch_failures/category--ai.html<' in fetch_failure_output_section
    assert 'class="output-registry-badge output-registry-badge-target">data/fetch_failures/category--ai.json<' in fetch_failure_output_section
    assert 'class="output-registry-badge output-registry-badge-target">data/fetch_failures/category--sales.json<' in fetch_failure_output_section
    assert 'class="output-registry-badge output-registry-badge-format">HTML<' in fetch_failure_output_section
    assert 'class="output-registry-badge output-registry-badge-format">JSON<' in fetch_failure_output_section
    assert (
        f'class="output-registry-badge output-registry-badge-bytes">'
        f'{fetch_failure_link_byte_sizes["data/fetch_failures/category--ai.html"]}<'
    ) in fetch_failure_output_section
    assert (
        f'class="output-registry-badge output-registry-badge-bytes">'
        f'{fetch_failure_link_byte_sizes["data/fetch_failures/category--ai.json"]}<'
    ) in fetch_failure_output_section
    assert (
        f'class="output-registry-badge output-registry-badge-bytes">'
        f'{fetch_failure_link_byte_sizes["data/fetch_failures/category--sales.json"]}<'
    ) in fetch_failure_output_section
    assert (
        f'class="output-registry-badge output-registry-badge-share">'
        f'{fetch_failure_link_byte_shares["data/fetch_failures/category--ai.html"]}<'
    ) in fetch_failure_output_section
    assert (
        f'class="output-registry-badge output-registry-badge-share">'
        f'{fetch_failure_link_byte_shares["data/fetch_failures/category--ai.json"]}<'
    ) in fetch_failure_output_section
    assert (
        f'class="output-registry-badge output-registry-badge-share">'
        f'{fetch_failure_link_byte_shares["data/fetch_failures/category--sales.json"]}<'
    ) in fetch_failure_output_section
    assert 'rail-command-divider-label">HTML<' in fetch_failure_output_section
    assert 'rail-command-divider-label">JSON<' in fetch_failure_output_section
    assert 'rail-command-divider-count">1<' in fetch_failure_output_section
    assert 'rail-command-divider-count">2<' in fetch_failure_output_section
    assert fetch_failure_format_byte_totals["html"] in fetch_failure_output_section
    assert fetch_failure_format_byte_totals["json"] in fetch_failure_output_section
    assert fetch_failure_format_byte_ranges["html"] in fetch_failure_output_section
    assert fetch_failure_format_byte_ranges["json"] in fetch_failure_output_section
    assert fetch_failure_format_spread_ratios["html"] in fetch_failure_output_section
    assert fetch_failure_format_spread_ratios["json"] in fetch_failure_output_section
    assert fetch_failure_format_byte_deltas["html"] in fetch_failure_output_section
    assert fetch_failure_format_byte_deltas["json"] in fetch_failure_output_section
    assert fetch_failure_format_share_gaps["html"] in fetch_failure_output_section
    assert fetch_failure_format_share_gaps["json"] in fetch_failure_output_section
    assert fetch_failure_format_top_file_shares["html"] in fetch_failure_output_section
    assert fetch_failure_format_top_file_shares["json"] in fetch_failure_output_section
    assert fetch_failure_format_smallest_file_shares["html"] in fetch_failure_output_section
    assert fetch_failure_format_smallest_file_shares["json"] in fetch_failure_output_section
    assert fetch_failure_format_median_sizes["html"] in fetch_failure_output_section
    assert fetch_failure_format_median_sizes["json"] in fetch_failure_output_section
    assert fetch_failure_format_max_to_median_ratios["html"] in fetch_failure_output_section
    assert fetch_failure_format_max_to_median_ratios["json"] in fetch_failure_output_section
    assert fetch_failure_format_average_sizes["html"] in fetch_failure_output_section
    assert fetch_failure_format_average_sizes["json"] in fetch_failure_output_section
    assert fetch_failure_format_byte_shares["html"] in fetch_failure_output_section
    assert fetch_failure_format_byte_shares["json"] in fetch_failure_output_section
    assert fetch_failure_format_count_shares["html"] in fetch_failure_output_section
    assert fetch_failure_format_count_shares["json"] in fetch_failure_output_section
    assert_text_order(
        fetch_failure_output_section,
        [
            'rail-command-divider-label">HTML<',
            "GET fetch_failures/category--ai.html",
            'rail-command-divider-label">JSON<',
            "GET fetch_failures/category--ai.json",
            "GET fetch_failures/category--sales.json",
        ],
    )
    for artifact in pipeline_manifest["source_pipeline_diagnostics"]["downloadable_fetch_failure_artifacts"]:
        assert artifact["site_path"] in data_html
        assert artifact["sha256"] in data_html
        assert f"{artifact['bytes']:,} bytes" in data_html


def test_build_site_is_deterministic_across_rebuilds(tmp_path: Path) -> None:
    workspace = prepare_workspace(tmp_path)

    run_site_build(workspace)
    first_hashes = site_hashes(workspace)

    run_site_build(workspace)
    second_hashes = site_hashes(workspace)

    assert second_hashes == first_hashes
