from __future__ import annotations

from collections import Counter
import csv
import html
import json
from pathlib import Path
import re
import shutil


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CHARTS_DIR = ROOT / "charts"
DOCS_DIR = ROOT / "docs"
SITE_DIR = ROOT / "site"
SITE_ASSETS_DIR = SITE_DIR / "assets"
SITE_CHARTS_DIR = SITE_ASSETS_DIR / "charts"
SITE_DATA_DIR = SITE_DIR / "data"

JSON_EXPORTS = [
    "metrics.json",
    "publication_input.json",
    "validation_report.json",
    "source_coverage_report.json",
    "source_pipeline_diagnostics.json",
    "pipeline_manifest.json",
]
CHART_STEMS = [
    ("category_share_map", "Category share map", "Representation versus visible revenue share, with the outlier-safe zoom panel."),
    ("top_categories_revenue", "Top categories by visible revenue", "The current sample is still dominated by E-commerce and Content Creation."),
    ("category_over_index", "Category over-index", "Revenue share divided by startup share highlights which categories outperform their representation."),
    ("model_mix", "Business-model and GTM composition", "Heuristic model labels show where visible revenue clusters by operating shape and acquisition motion."),
    ("distribution_and_concentration", "Distribution and concentration", "The visible sample is highly top-heavy, with a steep revenue concentration curve."),
]
INLINE_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
INLINE_BOLD_RE = re.compile(r"\*\*(.+?)\*\*")
INLINE_CODE_RE = re.compile(r"`([^`]+)`")


def usd_short(value: float) -> str:
    value = float(value)
    if abs(value) >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if abs(value) >= 1_000:
        return f"${value / 1_000:.1f}k"
    return f"${value:,.0f}"


def pct(value: float, digits: int = 1) -> str:
    return f"{float(value) * 100:.{digits}f}%"


def read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def format_byte_count(byte_count: int) -> str:
    if byte_count == 1:
        return "1 byte"
    return f"{byte_count:,} bytes"


def format_delay_seconds(value: object) -> str:
    if value in (None, ""):
        return "n/a"
    try:
        return f"{float(value):.1f}s"
    except (TypeError, ValueError):
        return html.escape(str(value))


def clean_site_dir() -> None:
    if SITE_DIR.exists():
        shutil.rmtree(SITE_DIR)
    SITE_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)


def source_pipeline_artifact_items(
    source_pipeline_diagnostics: dict[str, object],
    pipeline_manifest: dict[str, object],
    *,
    artifact_key: str,
) -> list[dict[str, object]]:
    manifest_artifacts = pipeline_manifest.get("source_pipeline_diagnostics", {}).get(artifact_key, [])
    diagnostics_artifacts = source_pipeline_diagnostics.get(artifact_key, [])
    selected_artifacts = manifest_artifacts or diagnostics_artifacts
    items: list[dict[str, object]] = []
    for artifact in selected_artifacts:
        if not isinstance(artifact, dict):
            continue
        path = str(artifact.get("path") or "")
        site_path = str(artifact.get("site_path") or "")
        label = str(artifact.get("label") or "")
        description = str(artifact.get("description") or "")
        if not path or not site_path or not label or not description:
            continue
        if not (ROOT / path).exists():
            continue
        item: dict[str, object] = {
            "path": path,
            "site_path": site_path,
            "label": label,
            "description": description,
        }
        artifact_format = str(artifact.get("format") or "")
        if artifact_format:
            item["format"] = artifact_format
        artifact_bytes = artifact.get("bytes")
        if isinstance(artifact_bytes, int):
            item["bytes"] = artifact_bytes
        artifact_sha256 = str(artifact.get("sha256") or "")
        if artifact_sha256:
            item["sha256"] = artifact_sha256
        items.append(item)
    return items


def staged_source_pipeline_download_items(
    source_pipeline_diagnostics: dict[str, object],
    pipeline_manifest: dict[str, object],
) -> list[dict[str, object]]:
    return source_pipeline_artifact_items(
        source_pipeline_diagnostics,
        pipeline_manifest,
        artifact_key="downloadable_staged_artifacts",
    )


def fetch_failure_download_items(
    source_pipeline_diagnostics: dict[str, object],
    pipeline_manifest: dict[str, object],
) -> list[dict[str, object]]:
    return source_pipeline_artifact_items(
        source_pipeline_diagnostics,
        pipeline_manifest,
        artifact_key="downloadable_fetch_failure_artifacts",
    )


def manifest_generated_download_items(pipeline_manifest: dict[str, object]) -> list[dict[str, object]]:
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
        if path in seen_paths or not (ROOT / path).exists():
            continue
        artifact_path = ROOT / path
        items.append(
            {
                "path": path,
                "site_path": path,
                "label": Path(path).name,
                "description": f"Publication bundle artifact copied into the static site: {path}.",
                "format": Path(path).suffix.lstrip(".").lower(),
                "bytes": int(artifact.get("bytes") or artifact_path.stat().st_size),
                "sha256": str(artifact.get("sha256") or ""),
            }
        )
        seen_paths.add(path)

    manifest_path = "data/pipeline_manifest.json"
    if manifest_path not in seen_paths and (ROOT / manifest_path).exists():
        manifest_file = ROOT / manifest_path
        items.append(
            {
                "path": manifest_path,
                "site_path": manifest_path,
                "label": Path(manifest_path).name,
                "description": "Active pipeline manifest for the current static publication bundle.",
                "format": "json",
                "bytes": int(manifest_file.stat().st_size),
                "sha256": "",
            }
        )
    return items


def publication_download_items(
    source_pipeline_diagnostics: dict[str, object],
    pipeline_manifest: dict[str, object],
) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    seen_paths: set[str] = set()
    for artifact in (
        manifest_generated_download_items(pipeline_manifest)
        + staged_source_pipeline_download_items(source_pipeline_diagnostics, pipeline_manifest)
        + fetch_failure_download_items(source_pipeline_diagnostics, pipeline_manifest)
    ):
        site_path = str(artifact.get("site_path") or "")
        if not site_path or site_path in seen_paths:
            continue
        items.append(artifact)
        seen_paths.add(site_path)
    return items


def publication_download_card_metadata(path: str) -> tuple[str, str]:
    metadata = {
        "data/category_summary.csv": (
            "Category summary",
            "Visible revenue, median revenue, startup share, and revenue share by category.",
        ),
        "data/business_model_summary.csv": (
            "Business model summary",
            "Visible-sample counts and revenue totals grouped by heuristic business-model labels.",
        ),
        "data/gtm_model_summary.csv": (
            "GTM model summary",
            "Visible-sample counts and revenue totals grouped by heuristic go-to-market labels.",
        ),
        "data/revenue_band_summary.csv": (
            "Revenue band summary",
            "Visible-sample startup counts and revenue totals grouped into threshold-aware revenue bands.",
        ),
        "data/public_source_pages.csv": (
            "Public source pages",
            "The current public-page source registry that seeds the reproducible listing-page fetch/parse pipeline.",
        ),
        "data/metrics.json": (
            "Top-line metrics",
            "Sample size, revenue concentration, and dominant-category snapshots.",
        ),
        "data/publication_input.json": (
            "Publication input manifest",
            "The active published dataset path plus any live-source promotion provenance.",
        ),
        "data/validation_report.json": (
            "Validation report",
            "Required-column, threshold, duplicate, and warning-level label checks.",
        ),
        "data/source_coverage_report.json": (
            "Source coverage report",
            "Per-source-page startup counts, revenue shares, and category coverage.",
        ),
        "data/source_pipeline_diagnostics.json": (
            "Source-pipeline diagnostics",
            "Promotion provenance, override coverage, duplicate review, per-source parser output counts, and shared detail-field coverage.",
        ),
        "data/pipeline_manifest.json": (
            "Pipeline manifest",
            "Build command, input dataset hash, copied-output inventory, and publication-input context.",
        ),
    }
    return metadata.get(
        path,
        (
            Path(path).name,
            f"Publication bundle artifact copied into the static site: {path}.",
        ),
    )


def count_label(count: int, singular: str) -> str:
    return f"{count:,} {singular}" if count == 1 else f"{count:,} {singular}s"


def artifact_format_label(artifact: dict[str, object]) -> str:
    artifact_format = str(artifact.get("format") or "").strip().lower()
    if artifact_format:
        return artifact_format
    site_path = str(artifact.get("site_path") or artifact.get("path") or "")
    return Path(site_path).suffix.lstrip(".").lower()


def download_badge(text: str, *, tone: str = "neutral") -> str:
    tone_class = "" if tone == "neutral" else f" download-badge-{tone}"
    return f'<span class="download-badge{tone_class}">{html.escape(text)}</span>'


def download_badges_html(artifact: dict[str, object]) -> str:
    badges: list[str] = []
    artifact_format = artifact_format_label(artifact)
    if artifact_format:
        badges.append(download_badge(artifact_format.upper(), tone="format"))
    artifact_bytes = artifact.get("bytes")
    if isinstance(artifact_bytes, int):
        badges.append(download_badge(format_byte_count(artifact_bytes), tone="meta"))
    if not badges:
        return ""
    return f'<div class="download-badges">{"".join(badges)}</div>'


def download_section_summary_html(items: list[dict[str, object]]) -> str:
    format_counts: Counter[str] = Counter()
    total_bytes = 0
    for artifact in items:
        artifact_format = artifact_format_label(artifact)
        if artifact_format:
            format_counts[artifact_format] += 1
        artifact_bytes = artifact.get("bytes")
        if isinstance(artifact_bytes, int):
            total_bytes += artifact_bytes

    badges = [download_badge(count_label(len(items), "file"), tone="count")]
    for artifact_format, count in sorted(format_counts.items(), key=lambda item: (-item[1], item[0])):
        badges.append(download_badge(f"{count:,} {artifact_format.upper()}", tone="format"))
    badges.append(download_badge(format_byte_count(total_bytes), tone="meta"))
    return f'<div class="download-summary">{"".join(badges)}</div>'


def download_format_summary_badges_html(items: list[dict[str, object]]) -> str:
    format_counts: Counter[str] = Counter()
    for artifact in items:
        artifact_format = artifact_format_label(artifact)
        if artifact_format:
            format_counts[artifact_format] += 1
    if not format_counts:
        return ""
    return "".join(
        download_badge(f"{count:,} {artifact_format.upper()}", tone="format")
        for artifact_format, count in sorted(format_counts.items(), key=lambda item: (-item[1], item[0]))
    )


def download_total_bytes_badge_html(items: list[dict[str, object]]) -> str:
    total_bytes = 0
    for artifact in items:
        artifact_bytes = artifact.get("bytes")
        if isinstance(artifact_bytes, int):
            total_bytes += artifact_bytes
    return download_badge(format_byte_count(total_bytes), tone="meta")


def copy_assets(
    *,
    publication_download_items: list[dict[str, object]],
) -> None:
    for stem, _, _ in CHART_STEMS:
        for suffix in (".png", ".svg"):
            shutil.copy2(CHARTS_DIR / f"{stem}{suffix}", SITE_CHARTS_DIR / f"{stem}{suffix}")
    for name in JSON_EXPORTS:
        shutil.copy2(DATA_DIR / name, SITE_DATA_DIR / name)
    for artifact in publication_download_items:
        source_path = ROOT / artifact["path"]
        destination_path = SITE_DIR / artifact["site_path"]
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, destination_path)
    (SITE_DIR / ".nojekyll").write_text("", encoding="utf-8")


def png_dimensions(path: Path) -> tuple[int, int]:
    header = path.read_bytes()[:24]
    if header[:8] != b"\x89PNG\r\n\x1a\n":
        raise ValueError(f"Expected a PNG file: {path}")
    width = int.from_bytes(header[16:20], "big")
    height = int.from_bytes(header[20:24], "big")
    return width, height


def render_inline(text: str) -> str:
    escaped = html.escape(text, quote=False)
    escaped = INLINE_LINK_RE.sub(
        lambda match: (
            f'<a href="{html.escape(match.group(2), quote=True)}">'
            f"{html.escape(match.group(1), quote=False)}</a>"
        ),
        escaped,
    )
    escaped = INLINE_BOLD_RE.sub(r"<strong>\1</strong>", escaped)
    escaped = INLINE_CODE_RE.sub(r"<code>\1</code>", escaped)
    return escaped


def staged_download_provenance_html(artifact: dict[str, object]) -> str:
    metadata_parts: list[str] = []
    artifact_format = str(artifact.get("format") or "")
    if artifact_format:
        metadata_parts.append(artifact_format.upper())
    artifact_bytes = artifact.get("bytes")
    if isinstance(artifact_bytes, int):
        metadata_parts.append(format_byte_count(artifact_bytes))

    provenance = ""
    if metadata_parts:
        provenance += f'<p class="download-provenance">{" · ".join(html.escape(part) for part in metadata_parts)}</p>'

    artifact_sha256 = str(artifact.get("sha256") or "")
    if artifact_sha256:
        provenance += (
            '<p class="download-hash"><strong>SHA256</strong> '
            f'<code>{html.escape(artifact_sha256)}</code></p>'
        )
    return provenance


def download_card_html(
    artifact: dict[str, object],
    *,
    label: str | None = None,
    description: str | None = None,
) -> str:
    card_label = label or str(artifact.get("label") or "")
    card_description = description or str(artifact.get("description") or "")
    site_path = str(artifact.get("site_path") or "")
    return f"""
<article class="download-card">
  <h3>{html.escape(card_label)}</h3>
  <p>{html.escape(card_description)}</p>
  {download_badges_html(artifact)}
  {staged_download_provenance_html(artifact)}
  <a href="{html.escape(site_path, quote=True)}">Download</a>
</article>
"""


def markdown_to_html(markdown_text: str) -> str:
    lines = markdown_text.strip().splitlines()
    parts: list[str] = []
    paragraph: list[str] = []
    unordered_items: list[str] = []
    ordered_items: list[str] = []
    code_lines: list[str] = []
    in_code_block = False

    def flush_paragraph() -> None:
        if paragraph:
            parts.append(f"<p>{render_inline(' '.join(paragraph))}</p>")
            paragraph.clear()

    def flush_lists() -> None:
        if unordered_items:
            items = "".join(f"<li>{render_inline(item)}</li>" for item in unordered_items)
            parts.append(f"<ul>{items}</ul>")
            unordered_items.clear()
        if ordered_items:
            items = "".join(f"<li>{render_inline(item)}</li>" for item in ordered_items)
            parts.append(f"<ol>{items}</ol>")
            ordered_items.clear()

    def flush_code() -> None:
        nonlocal in_code_block
        if in_code_block:
            code = html.escape("\n".join(code_lines), quote=False)
            parts.append(f"<pre><code>{code}</code></pre>")
            code_lines.clear()
            in_code_block = False

    for line in lines:
        stripped = line.rstrip()
        if stripped.startswith("```"):
            flush_paragraph()
            flush_lists()
            if in_code_block:
                flush_code()
            else:
                in_code_block = True
            continue
        if in_code_block:
            code_lines.append(stripped)
            continue
        if not stripped:
            flush_paragraph()
            flush_lists()
            continue
        if stripped.startswith("### "):
            flush_paragraph()
            flush_lists()
            parts.append(f"<h3>{render_inline(stripped[4:])}</h3>")
            continue
        if stripped.startswith("## "):
            flush_paragraph()
            flush_lists()
            parts.append(f"<h2>{render_inline(stripped[3:])}</h2>")
            continue
        if stripped.startswith("# "):
            flush_paragraph()
            flush_lists()
            parts.append(f"<h1>{render_inline(stripped[2:])}</h1>")
            continue
        if stripped.startswith("- "):
            flush_paragraph()
            ordered_items.clear()
            unordered_items.append(stripped[2:].strip())
            continue
        ordered_match = re.match(r"^\d+\. (.+)$", stripped)
        if ordered_match:
            flush_paragraph()
            unordered_items.clear()
            ordered_items.append(ordered_match.group(1).strip())
            continue
        paragraph.append(stripped)

    flush_paragraph()
    flush_lists()
    flush_code()
    return "\n".join(parts)


def status_label(status: str) -> str:
    return {
        "passed": "Passed",
        "passed_with_warnings": "Passed with warnings",
        "failed": "Failed",
    }.get(status, status.replace("_", " ").title())


def status_class(status: str) -> str:
    return {
        "passed": "is-passed",
        "passed_with_warnings": "is-warning",
        "failed": "is-failed",
    }.get(status, "is-neutral")


def shell_tokens(*, active: str, status: str) -> str:
    route_label = {
        "index": "overview",
        "methodology": "methodology",
        "data": "data",
    }.get(active, active)
    return "".join(
        [
            '<span class="shell-pill shell-pill-accent"><strong>surface</strong><span>static pages</span></span>',
            f'<span class="shell-pill {status_class(status)}"><strong>validation</strong><span>{html.escape(status_label(status))}</span></span>',
            f'<span class="shell-pill"><strong>route</strong><span>{html.escape(route_label)}</span></span>',
            '<span class="shell-pill"><strong>mode</strong><span>visible public sample</span></span>',
        ]
    )


def route_key(slug: str) -> str:
    return {
        "index": "/index",
        "methodology": "/methodology",
        "data": "/data",
    }.get(slug, f"/{slug}")


def route_href(slug: str) -> str:
    return {
        "index": "index.html",
        "methodology": "methodology.html",
        "data": "data.html",
    }.get(slug, f"{slug}.html")


def command_item(
    *,
    label: str,
    target: str,
    kind: str,
    query: str,
    terms: str,
) -> dict[str, str]:
    return {
        "label": label,
        "target": target,
        "kind": kind,
        "query": query,
        "terms": terms,
    }


def panel_command_items(*, active: str, command_links: list[tuple[str, str]]) -> list[dict[str, str]]:
    page_route = route_key(active)
    items: list[dict[str, str]] = []
    for label, target in command_links:
        query = page_route if target == "#top" else f"{page_route} {target}"
        items.append(
            command_item(
                label=label,
                target=target,
                kind="panel",
                query=query,
                terms=f"{label} {target} panel section {active}",
            )
        )
    return items


def global_route_command_items() -> list[dict[str, str]]:
    return [
        command_item(
            label="GO / Overview",
            target=route_href("index"),
            kind="route",
            query=route_key("index"),
            terms="overview home index route summary",
        ),
        command_item(
            label="GO / Methodology",
            target=route_href("methodology"),
            kind="route",
            query=route_key("methodology"),
            terms="methodology caveats validation notes route",
        ),
        command_item(
            label="GO / Data",
            target=route_href("data"),
            kind="route",
            query=route_key("data"),
            terms="data downloads diagnostics route provenance",
        ),
        command_item(
            label="GO / Downloads",
            target="data.html#downloads",
            kind="route",
            query="/data #downloads",
            terms="downloads artifacts files json bundle",
        ),
        command_item(
            label="GO / Diagnostics",
            target="data.html#source-pipeline-diagnostics",
            kind="route",
            query="/data #source-pipeline-diagnostics",
            terms="diagnostics provenance source pipeline validation monitor",
        ),
    ]


def global_output_command_items(download_items: list[dict[str, object]]) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for artifact in download_items:
        site_path = str(artifact.get("site_path") or "")
        if not site_path:
            continue
        query = site_path.removeprefix("data/")
        label = f"GET {query}"
        description = str(artifact.get("description") or "")
        human_label = str(artifact.get("label") or "")
        artifact_format = str(artifact.get("format") or "")
        items.append(
            command_item(
                label=label,
                target=site_path,
                kind="asset",
                query=query,
                terms=f"{query} {site_path} {human_label} {description} {artifact_format} download asset",
            )
        )
        if artifact_format:
            items[-1]["format"] = artifact_format
        artifact_bytes = artifact.get("bytes")
        if isinstance(artifact_bytes, int):
            items[-1]["bytes"] = artifact_bytes
    return items


def hot_output_artifact_sort_key(artifact: dict[str, object]) -> tuple[str, str, str, str]:
    site_path = str(artifact.get("site_path") or "")
    query = site_path.removeprefix("data/")
    display_label = str(artifact.get("label") or "")
    return (
        artifact_format_label(artifact),
        query.lower(),
        display_label.lower(),
        site_path.lower(),
    )


def build_output_registry_sections(
    source_pipeline_diagnostics: dict[str, object],
    pipeline_manifest: dict[str, object],
) -> list[dict[str, object]]:
    section_specs = [
        (
            "Publication outputs",
            "Publication outputs",
            "No publication outputs are currently indexed in the static command surface.",
            manifest_generated_download_items(pipeline_manifest),
        ),
        (
            "Staged provenance",
            "Staged provenance",
            "No staged provenance downloads are currently attached to the active manifest.",
            staged_source_pipeline_download_items(source_pipeline_diagnostics, pipeline_manifest),
        ),
        (
            "Fetch-failure evidence",
            "Fetch-failure evidence",
            "No fetch-failure evidence is currently attached to the active manifest.",
            fetch_failure_download_items(source_pipeline_diagnostics, pipeline_manifest),
        ),
    ]
    sections: list[dict[str, object]] = []
    for title, aria_label, empty_message, artifacts in section_specs:
        sorted_artifacts = sorted(artifacts, key=hot_output_artifact_sort_key)
        sections.append(
            {
                "title": title,
                "aria_label": aria_label,
                "empty_message": empty_message,
                "count_label": count_label(len(sorted_artifacts), "file"),
                "format_badges_html": download_format_summary_badges_html(sorted_artifacts),
                "byte_total_badge_html": download_total_bytes_badge_html(sorted_artifacts),
                "items": global_output_command_items(sorted_artifacts),
            }
        )
    return sections


def command_link_markup(item: dict[str, object], *, link_class: str) -> str:
    return (
        f'<a class="{html.escape(link_class, quote=True)}" '
        f'href="{html.escape(str(item["target"]), quote=True)}" '
        f'data-command-label="{html.escape(str(item["label"]), quote=True)}" '
        f'data-command-target="{html.escape(str(item["target"]), quote=True)}" '
        f'data-command-kind="{html.escape(str(item["kind"]), quote=True)}" '
        f'data-command-query="{html.escape(str(item["query"]), quote=True)}" '
        f'data-command-terms="{html.escape(str(item["terms"]), quote=True)}">'
        f'{html.escape(str(item["label"]))}</a>'
    )


def command_links_markup(command_items: list[dict[str, object]], *, link_class: str) -> str:
    return "".join(command_link_markup(item, link_class=link_class) for item in command_items)


def output_registry_command_links_markup(command_items: list[dict[str, object]]) -> str:
    parts: list[str] = []
    format_counts: Counter[str] = Counter(
        str(item.get("format") or "").strip().lower() or "other"
        for item in command_items
    )
    format_bytes: Counter[str] = Counter()
    for item in command_items:
        item_format = str(item.get("format") or "").strip().lower() or "other"
        item_bytes = item.get("bytes")
        if isinstance(item_bytes, int):
            format_bytes[item_format] += item_bytes
    active_format = None
    for item in command_items:
        item_format = str(item.get("format") or "").strip().lower() or "other"
        if item_format != active_format:
            parts.append(
                '<div class="rail-command-divider">'
                f'<span class="rail-command-divider-label">{html.escape(item_format.upper())}</span>'
                f'<span class="rail-command-divider-count">{format_counts[item_format]:,}</span>'
                f'<span class="rail-command-divider-bytes">{html.escape(format_byte_count(format_bytes[item_format]))}</span>'
                "</div>"
            )
            active_format = item_format
        parts.append(command_link_markup(item, link_class="rail-command-link"))
    return "".join(parts)


def output_registry_sections_markup(output_registry_sections: list[dict[str, object]]) -> str:
    parts: list[str] = []
    for section in output_registry_sections:
        title = str(section.get("title") or "")
        aria_label = str(section.get("aria_label") or title)
        empty_message = str(section.get("empty_message") or "")
        count_text = str(section.get("count_label") or "0 files")
        format_badges_html = str(section.get("format_badges_html") or "")
        byte_total_badge_html = str(section.get("byte_total_badge_html") or "")
        items = [
            item
            for item in section.get("items", [])
            if isinstance(item, dict)
        ]
        body_html = (
            f'<nav class="rail-command-links" aria-label="{html.escape(aria_label, quote=True)}">'
            f"{output_registry_command_links_markup(items)}"
            "</nav>"
            if items
            else f'<p class="rail-command-group-empty">{html.escape(empty_message)}</p>'
        )
        parts.append(
            f"""
<div class="rail-command-group">
  <div class="rail-command-group-head">
    <p class="rail-command-group-title">{html.escape(title)}</p>
    <div class="rail-command-group-meta">
      {download_badge(count_text, tone="count")}
      {format_badges_html}
      {byte_total_badge_html}
    </div>
  </div>
  {body_html}
</div>
"""
        )
    return "".join(parts)


def metric_list(items: list[tuple[str, str]]) -> str:
    return (
        '<dl class="metric-list">'
        + "".join(
            (
                "<div>"
                f"<dt>{html.escape(label)}</dt>"
                f"<dd>{html.escape(value)}</dd>"
                "</div>"
            )
            for label, value in items
        )
        + "</dl>"
    )


def rail_module(*, kicker: str, title: str, body_html: str, tone: str = "neutral") -> str:
    tone_class = "" if tone == "neutral" else f" rail-module-{tone}"
    return f"""
<section class="rail-module{tone_class}">
  <p class="rail-kicker">{html.escape(kicker)}</p>
  <h2 class="rail-title">{html.escape(title)}</h2>
  {body_html}
</section>
"""


def page_shell(
    *,
    title: str,
    active: str,
    description: str,
    status: str,
    command_links: list[tuple[str, str]],
    output_registry_sections: list[dict[str, object]],
    monitor_html: str,
    body_html: str,
) -> str:
    local_panel_items = panel_command_items(active=active, command_links=command_links)
    route_registry_items = global_route_command_items()
    output_registry_items = [
        item
        for section in output_registry_sections
        for item in section.get("items", [])
        if isinstance(item, dict)
    ]
    nav_items = [
        ("index", "Overview", "index.html"),
        ("methodology", "Methodology", "methodology.html"),
        ("data", "Data", "data.html"),
    ]
    navigation = "".join(
        (
            f'<a class="nav-link{" is-active" if slug == active else ""}" href="{href}">{label}</a>'
        )
        for slug, label, href in nav_items
    )
    command_bar_links = command_links_markup(local_panel_items, link_class="command-chip")
    command_deck_links = command_links_markup(local_panel_items, link_class="rail-command-link")
    route_registry_links = command_links_markup(route_registry_items, link_class="rail-command-link")
    output_registry_links = output_registry_sections_markup(output_registry_sections)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)} | TrustMRR visible sample</title>
  <meta name="description" content="{html.escape(description, quote=True)}">
  <link rel="stylesheet" href="assets/site.css">
  <script defer src="assets/site.js"></script>
</head>
<body class="page-{active}">
  <div class="page-backdrop"></div>
  <header class="command-strip">
    <div class="site-shell command-strip-inner">
      <a class="brand" href="index.html">
        <span class="brand-kicker">fancyMMR</span>
        <span class="brand-title">TrustMRR visible-sample terminal</span>
      </a>
      <div class="ticker-strip" aria-label="Workspace status">
        {shell_tokens(active=active, status=status)}
      </div>
    </div>
  </header>
  <div class="site-shell workstation">
    <aside class="control-rail" aria-label="Command rail">
      <section class="rail-module">
        <p class="rail-kicker">Route map</p>
        <nav class="nav-links" aria-label="Primary">
          {navigation}
        </nav>
      </section>
      <section class="rail-module">
        <p class="rail-kicker">Command deck</p>
        <nav class="rail-command-links" aria-label="Page panels">
          {command_deck_links}
        </nav>
      </section>
      <section class="rail-module">
        <p class="rail-kicker">Route registry</p>
        <nav class="rail-command-links" aria-label="Cross-page routes">
          {route_registry_links}
        </nav>
      </section>
      <section class="rail-module">
        <p class="rail-kicker">Hot outputs</p>
        {output_registry_links}
      </section>
      <section class="rail-module">
        <p class="rail-kicker">Operating mode</p>
        <p class="rail-copy">Visible public sample only. Static GitHub Pages publication. No runtime server, no hidden backend, no platform-wide claim.</p>
      </section>
    </aside>
    <main class="workspace">
      <section class="workspace-command" data-command-surface data-page-route="{html.escape(route_key(active), quote=True)}">
        <div class="command-entry">
          <label class="command-prompt-label" for="jump-palette">Jump palette</label>
          <div class="command-input-wrap">
            <span class="command-prefix">jump --panel</span>
            <input
              id="jump-palette"
              class="command-input"
              type="text"
              name="jump_palette"
              value="{html.escape(route_key(active), quote=True)}"
              placeholder="{html.escape(route_key(active), quote=True)} #panel"
              autocapitalize="off"
              autocomplete="off"
              spellcheck="false"
              data-command-input
            >
          </div>
          <p class="command-help">Press <code>/</code> or <code>Ctrl+K</code> to focus. Press <code>Enter</code> to jump. Use <code>[</code> and <code>]</code> to cycle local panels. Try <code>/data</code>, <code>/data #downloads</code>, or <code>metrics.json</code>.</p>
        </div>
        <div class="command-bar-links">
          {command_bar_links}
        </div>
        <p class="command-status" aria-live="polite" data-command-status>Ready. {len(local_panel_items):,} local panels and {len(route_registry_items) + len(output_registry_items):,} global commands indexed.</p>
      </section>
      {body_html}
    </main>
    <aside class="monitor-rail" aria-label="Operator monitors">
      <section class="rail-module">
        <p class="rail-kicker">System brief</p>
        <h2 class="rail-title">{html.escape(title)}</h2>
        <p class="rail-copy">{html.escape(description)}</p>
      </section>
      {monitor_html}
      <section class="rail-module">
        <p class="rail-kicker">Build path</p>
        <p class="rail-copy">Regenerate the static operator surface with <code>python src/build_site.py</code>.</p>
      </section>
    </aside>
  </div>
  <footer class="site-shell site-footer">
    <p>This static site is a source-derived visible sample research artifact. It is not a full platform export and is not affiliated with TrustMRR.</p>
    <p>Build inputs live in <code>data/</code>, <code>charts/</code>, <code>docs/</code>, and <code>DATA-NOTICE.md</code>; regenerate the site with <code>python src/build_site.py</code>.</p>
  </footer>
</body>
</html>
"""


def hero_section(*, eyebrow: str, title: str, lede: str, status: str, aside_html: str) -> str:
    return f"""
<section class="hero" id="top">
  <div class="hero-grid">
    <div class="hero-copy">
      <div class="hero-head">
        <div>
          <p class="eyebrow">{html.escape(eyebrow)}</p>
          <h1>{html.escape(title)}</h1>
        </div>
        <span class="panel-code">Primary monitor</span>
      </div>
      <p class="hero-lede">{html.escape(lede)}</p>
      <div class="hero-meta">
        <span class="status-pill {status_class(status)}">{html.escape(status_label(status))}</span>
        <a class="button" href="data.html">Inspect data</a>
        <a class="button button-secondary" href="methodology.html">Read methodology</a>
      </div>
    </div>
    <div class="hero-panel">
      {aside_html}
    </div>
  </div>
</section>
"""


def stat_card(label: str, value: str, note: str) -> str:
    return f"""
<article class="stat-card">
  <p class="stat-label">{html.escape(label)}</p>
  <p class="stat-value">{html.escape(value)}</p>
  <p class="stat-note">{html.escape(note)}</p>
</article>
"""


def section(
    title: str,
    intro: str,
    body_html: str,
    *,
    section_id: str | None = None,
    panel_code: str | None = None,
    panel_tag: str | None = None,
    layout: str = "full",
) -> str:
    section_id_attr = f' id="{html.escape(section_id, quote=True)}"' if section_id else ""
    panel_code_html = f'<p class="panel-kicker">{html.escape(panel_code)}</p>' if panel_code else ""
    panel_tag_html = f'<span class="panel-tag">{html.escape(panel_tag)}</span>' if panel_tag else ""
    return f"""
<section class="section-card layout-{html.escape(layout, quote=True)}"{section_id_attr}>
  <div class="section-head">
    <div class="section-head-copy">
      {panel_code_html}
      <h2>{html.escape(title)}</h2>
      <p>{html.escape(intro)}</p>
    </div>
    {panel_tag_html}
  </div>
  <div class="section-body">
    {body_html}
  </div>
</section>
"""


def render_table(headers: list[str], rows: list[list[str]]) -> str:
    head_html = "".join(f"<th>{html.escape(header)}</th>" for header in headers)
    body_html = "".join(
        "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
        for row in rows
    )
    return f"""
<div class="table-wrap">
  <table>
    <thead><tr>{head_html}</tr></thead>
    <tbody>{body_html}</tbody>
  </table>
</div>
"""


def render_checks(validation_report: dict[str, object]) -> str:
    items = []
    for check in validation_report["checks"]:
        check_state = "check-pass" if check["passed"] else ("check-warn" if check["severity"] == "warning" else "check-fail")
        items.append(
            f"""
<li class="check-item {check_state}">
  <strong>{html.escape(str(check['message']))}</strong>
  <span>{html.escape(str(check['id']))}</span>
</li>
"""
        )
    return f'<ul class="check-list">{"".join(items)}</ul>'


def warning_summary(validation_report: dict[str, object]) -> str:
    warning_bits: list[str] = []
    duplicate_name_count = int(validation_report.get("duplicate_name_count", 0))
    null_counts = validation_report["null_counts"]

    if duplicate_name_count:
        label = "duplicate startup name" if duplicate_name_count == 1 else "duplicate startup names"
        warning_bits.append(f"{duplicate_name_count} {label}")
    if null_counts["biz_model"]:
        warning_bits.append(f"{null_counts['biz_model']} null biz_model values")
    if null_counts["gtm_model"]:
        warning_bits.append(f"{null_counts['gtm_model']} null gtm_model values")

    return ", ".join(warning_bits) if warning_bits else "No outstanding warnings in the current validation layer"


def methodology_warning_snapshot(validation_report: dict[str, object]) -> str:
    duplicate_name_count = int(validation_report.get("duplicate_name_count", 0))
    heuristic_gap_count = int(validation_report["null_counts"]["biz_model"]) + int(validation_report["null_counts"]["gtm_model"])
    duplicate_label = "duplicate name" if duplicate_name_count == 1 else "duplicate names"
    heuristic_label = "heuristic gap" if heuristic_gap_count == 1 else "heuristic gaps"
    return f"{duplicate_name_count} {duplicate_label} / {heuristic_gap_count} {heuristic_label}"


def source_pipeline_warning_summary(source_pipeline_diagnostics: dict[str, object]) -> str:
    warning_ids = [str(value) for value in source_pipeline_diagnostics.get("failing_warning_check_ids", [])]
    return ", ".join(warning_ids) if warning_ids else "none"


def detail_field_population_summary(source_pipeline_diagnostics: dict[str, object]) -> str:
    counts = source_pipeline_diagnostics.get("detail_field_population_counts", {}) or {}
    parse_counts = source_pipeline_diagnostics.get("detail_parse_status_counts", {}) or {}
    return (
        f"{int(counts.get('problem_solved', 0))} problem solved, "
        f"{int(counts.get('pricing_summary', 0))} pricing, "
        f"{int(counts.get('target_audience', 0))} audience, "
        f"{int(counts.get('founder_name', 0))} founder names across "
        f"{int(parse_counts.get('parsed', 0))} parsed detail pages."
    )


def build_index_page(
    metrics: dict[str, object],
    validation_report: dict[str, object],
    category_rows: list[dict[str, str]],
    output_registry_sections: list[dict[str, object]],
) -> str:
    command_links = [
        ("OV.00 Top", "#top"),
        ("OV.01 Snapshot", "#snapshot"),
        ("OV.02 Guardrails", "#scope-guardrails"),
        ("OV.03 Charts", "#main-charts"),
        ("OV.04 Leaders", "#category-leaders"),
    ]
    hero = hero_section(
        eyebrow="Static GitHub Pages bundle",
        title="Visible startup revenue workstation",
        lede="This operator surface packages the current processed TrustMRR visible sample into a dense, fully static Pages publication: summary metrics, charts, methodology, and machine-readable provenance live together with no runtime server.",
        status=str(validation_report["status"]),
        aside_html=f"""
<div class="hero-aside">
  <p class="eyebrow">Current build snapshot</p>
  <p class="hero-aside-value">{int(metrics["sample_size"])} startups</p>
  <p class="hero-aside-note">30 public source pages, deterministic charts, and validation manifests copied directly into the published site output.</p>
  {metric_list([
      ("Visible revenue", usd_short(metrics["total_visible_revenue_usd"])),
      ("Median startup", usd_short(metrics["median_revenue_usd"])),
      ("Top-10 share", pct(metrics["top_10_revenue_share"])),
  ])}
</div>
""",
    )

    monitor_html = "".join(
        [
            rail_module(
                kicker="Revenue lens",
                title=usd_short(metrics["total_visible_revenue_usd"]),
                body_html=(
                    '<p class="rail-copy">Current visible 30-day revenue across the published sample.</p>'
                    + metric_list(
                        [
                            ("Median startup", usd_short(metrics["median_revenue_usd"])),
                            ("Sample size", f"{int(metrics['sample_size']):,} startups"),
                        ]
                    )
                ),
                tone="accent",
            ),
            rail_module(
                kicker="Concentration monitor",
                title=pct(metrics["top_10_revenue_share"]),
                body_html=(
                    '<p class="rail-copy">Top-10 revenue share is the fastest concentration-risk readout in the visible sample.</p>'
                    + metric_list(
                        [
                            ("Dominant category", str(metrics["dominant_category"])),
                            ("Revenue share", pct(metrics["dominant_category_revenue_share"])),
                        ]
                    )
                ),
            ),
            rail_module(
                kicker="Warning posture",
                title=status_label(str(validation_report["status"])),
                body_html=(
                    f'<p class="rail-copy">{html.escape(warning_summary(validation_report))}.</p>'
                ),
                tone="warning" if str(validation_report["status"]) == "passed_with_warnings" else "good",
            ),
        ]
    )

    stats = "".join(
        [
            stat_card(
                "Visible 30-day revenue",
                usd_short(metrics["total_visible_revenue_usd"]),
                "Derived from the current visible sample only.",
            ),
            stat_card(
                "Median startup revenue",
                usd_short(metrics["median_revenue_usd"]),
                "Half the visible sample is at or below this 30-day level.",
            ),
            stat_card(
                "Top-10 concentration",
                pct(metrics["top_10_revenue_share"]),
                "The visible sample is highly top-heavy.",
            ),
            stat_card(
                "Dominant category",
                str(metrics["dominant_category"]),
                f"{pct(metrics['dominant_category_revenue_share'])} of visible revenue with {pct(metrics['dominant_category_startup_share'])} of startups.",
            ),
        ]
    )

    chart_cards = "".join(
        f"""
<article class="chart-card">
  <img src="assets/charts/{stem}.png" alt="{html.escape(title)}" loading="lazy" width="{width}" height="{height}">
  <div class="chart-copy">
    <h3>{html.escape(title)}</h3>
    <p>{html.escape(caption)}</p>
    <a href="assets/charts/{stem}.svg">View SVG</a>
  </div>
</article>
"""
        for stem, title, caption, width, height in [
            (*chart, *png_dimensions(CHARTS_DIR / f"{chart[0]}.png")) for chart in CHART_STEMS
        ]
    )

    top_categories = render_table(
        ["Category", "Visible revenue", "Startup share", "Revenue share", "Performance index"],
        [
            [
                html.escape(row["category"]),
                html.escape(usd_short(float(row["total_revenue"]))),
                html.escape(pct(float(row["startup_share"]))),
                html.escape(pct(float(row["revenue_share"]))),
                html.escape(f"{float(row['performance_index']):.2f}x"),
            ]
            for row in category_rows[:8]
        ],
    )

    sections = [
        section(
            "Snapshot",
            "Top-line metrics and the current validation posture for the visible sample.",
            f'<div class="stat-grid">{stats}</div>',
            section_id="snapshot",
            panel_code="OV.01",
            panel_tag="summary pane",
            layout="compact",
        ),
        section(
            "Scope guardrails",
            "The site keeps the research caveat visible instead of hiding it behind repository docs.",
            f"""
<div class="two-up">
  <article class="callout-card">
    <h3>What this is</h3>
    <p>A source-derived visible public sample built from public pages where <code>Revenue (30d) &gt;= $5,000</code>.</p>
  </article>
  <article class="callout-card">
    <h3>What this is not</h3>
    <p>Not a full platform export, not an official dataset, and not a claim of platform-wide coverage.</p>
  </article>
</div>
<p class="section-note">Validation status: <strong>{html.escape(status_label(str(validation_report["status"])))}</strong>. Warning summary: {html.escape(warning_summary(validation_report))}.</p>
""",
            section_id="scope-guardrails",
            panel_code="OV.02",
            panel_tag="guardrail pane",
            layout="compact",
        ),
        section(
            "Main charts",
            "The published charts are copied into the site so GitHub Pages can serve the bundle directly.",
            f'<div class="chart-grid">{chart_cards}</div>',
            section_id="main-charts",
            panel_code="OV.03",
            panel_tag="chart rack",
        ),
        section(
            "Category leaders",
            "The current sample is concentrated in a small number of categories, with E-commerce far ahead of the rest.",
            top_categories,
            section_id="category-leaders",
            panel_code="OV.04",
            panel_tag="ranking table",
        ),
    ]

    body = hero + f'<div class="panel-stack">{"".join(sections)}</div>'
    return page_shell(
        title="Overview",
        active="index",
        description="Static overview of the TrustMRR visible-sample research bundle.",
        status=str(validation_report["status"]),
        command_links=command_links,
        output_registry_sections=output_registry_sections,
        monitor_html=monitor_html,
        body_html=body,
    )


def build_methodology_page(
    methodology_markdown: str,
    data_notice_markdown: str,
    validation_report: dict[str, object],
    output_registry_sections: list[dict[str, object]],
) -> str:
    command_links = [
        ("MD.00 Top", "#top"),
        ("MD.01 Methodology", "#methodology-panel"),
        ("MD.02 Data notice", "#data-notice"),
        ("MD.03 Validation", "#validation-checks"),
    ]
    hero = hero_section(
        eyebrow="Methodology and caveats",
        title="How the visible sample is defined, validated, and limited",
        lede="This pane keeps the inclusion rule, heuristic-field caveats, and publication limitations visible inside the static bundle so the research surface does not outgrow its evidence.",
        status=str(validation_report["status"]),
        aside_html=f"""
<div class="hero-aside">
  <p class="eyebrow">Warning-only signals</p>
  <p class="hero-aside-value">{html.escape(methodology_warning_snapshot(validation_report))}</p>
  <p class="hero-aside-note">Warning-only checks currently cover duplicate startup names and heuristic label gaps, while missing provenance, threshold violations, and duplicate (name, source_url) pairs still fail the build.</p>
  {metric_list([
      ("Validation", status_label(str(validation_report["status"]))),
      ("Duplicate names", str(int(validation_report.get("duplicate_name_count", 0)))),
      ("Heuristic gaps", str(int(validation_report["null_counts"]["biz_model"]) + int(validation_report["null_counts"]["gtm_model"]))),
  ])}
</div>
""",
    )

    methodology_monitor_html = "".join(
        [
            rail_module(
                kicker="Inclusion rule",
                title=">= $5,000 / 30d",
                body_html=(
                    '<p class="rail-copy">The publication remains a visible public sample derived from public pages above the explicit threshold.</p>'
                ),
                tone="accent",
            ),
            rail_module(
                kicker="Validation posture",
                title=status_label(str(validation_report["status"])),
                body_html=(
                    f'<p class="rail-copy">{html.escape(warning_summary(validation_report))}.</p>'
                ),
                tone="warning" if str(validation_report["status"]) == "passed_with_warnings" else "good",
            ),
            rail_module(
                kicker="Publication caveat",
                title="Not a full export",
                body_html=(
                    '<p class="rail-copy">Methodology, legal caveats, and provenance stay first-class so the static publication remains explicit about what it can and cannot claim.</p>'
                ),
            ),
        ]
    )

    methodology_html = markdown_to_html(methodology_markdown)
    data_notice_html = markdown_to_html(data_notice_markdown)

    sections = [
        section(
            "Methodology",
            "Rendered directly from the repository methodology document to keep the site aligned with the source docs.",
            f'<div class="prose">{methodology_html}</div>',
            section_id="methodology-panel",
            panel_code="MD.01",
            panel_tag="operator notes",
            layout="compact",
        ),
        section(
            "Data notice",
            "Publication caveats from the repository root stay visible in the static site too.",
            f'<div class="prose">{data_notice_html}</div>',
            section_id="data-notice",
            panel_code="MD.02",
            panel_tag="caveat pane",
            layout="compact",
        ),
        section(
            "Validation checks",
            "The seed bundle keeps warnings and failures explicit instead of silently smoothing them away.",
            render_checks(validation_report),
            section_id="validation-checks",
            panel_code="MD.03",
            panel_tag="gate monitor",
        ),
    ]

    body = hero + f'<div class="panel-stack">{"".join(sections)}</div>'
    return page_shell(
        title="Methodology",
        active="methodology",
        description="Methodology and data caveats for the TrustMRR visible-sample static site.",
        status=str(validation_report["status"]),
        command_links=command_links,
        output_registry_sections=output_registry_sections,
        monitor_html=methodology_monitor_html,
        body_html=body,
    )


def build_data_page(
    metrics: dict[str, object],
    publication_input: dict[str, object],
    validation_report: dict[str, object],
    source_coverage_report: dict[str, object],
    source_pipeline_diagnostics: dict[str, object],
    pipeline_manifest: dict[str, object],
    category_rows: list[dict[str, str]],
    revenue_band_rows: list[dict[str, str]],
    output_registry_sections: list[dict[str, object]],
) -> str:
    command_links = [
        ("DT.00 Top", "#top"),
        ("DT.01 Downloads", "#downloads"),
        ("DT.02 Staged bundle", "#staged-bundle"),
        ("DT.03 Fetch failures", "#fetch-failure-snapshots"),
        ("DT.04 Categories", "#top-categories"),
        ("DT.05 Revenue bands", "#revenue-bands"),
        ("DT.06 Source coverage", "#source-coverage"),
        ("DT.07 Diagnostics", "#source-pipeline-diagnostics"),
        ("DT.08 Manifest", "#manifest-notes"),
    ]
    hero = hero_section(
        eyebrow="Data and provenance",
        title="Machine-readable outputs, source coverage, and manifest details",
        lede="This operator pane ships the current JSON outputs alongside human-readable summaries so the published artifact stays inspectable without cloning the repository.",
        status=str(validation_report["status"]),
        aside_html=f"""
<div class="hero-aside">
  <p class="eyebrow">Manifest summary</p>
  <p class="hero-aside-value">{len(pipeline_manifest['generated_outputs'])} generated outputs</p>
  <p class="hero-aside-note">{source_coverage_report['source_page_count']} source pages and {pipeline_manifest['input_dataset']['rows']} visible startups feed the current site build.</p>
  {metric_list([
      ("Publication input", str(publication_input["source_label"])),
      ("Validation", status_label(str(validation_report["status"]))),
      ("Diagnostics", "attached" if source_pipeline_diagnostics["available"] else "seed only"),
  ])}
</div>
""",
    )

    publication_bundle_items = manifest_generated_download_items(pipeline_manifest)
    staged_bundle_items = staged_source_pipeline_download_items(source_pipeline_diagnostics, pipeline_manifest)
    fetch_failure_items = fetch_failure_download_items(source_pipeline_diagnostics, pipeline_manifest)

    if source_pipeline_diagnostics["available"]:
        diagnostics_title = f"{int(source_pipeline_diagnostics['selected_source_count'])}/{int(source_pipeline_diagnostics['expected_source_count'])} sources"
        diagnostics_body = (
            '<p class="rail-copy">Promoted staged diagnostics are attached to the active publication manifest.</p>'
            + metric_list(
                [
                    ("Fetch failures", str(int(source_pipeline_diagnostics["fetch_failure_source_count"]))),
                    ("Detail failures", str(int(source_pipeline_diagnostics["failed_detail_page_count"]))),
                    ("Parsed details", str(int(source_pipeline_diagnostics["parsed_detail_page_count"]))),
                ]
            )
        )
        diagnostics_tone = "good" if str(source_pipeline_diagnostics["validation_status"]) == "passed" else "warning"
    else:
        diagnostics_title = "seed manifest"
        diagnostics_body = (
            f'<p class="rail-copy">{html.escape(str(source_pipeline_diagnostics["message"]))}</p>'
        )
        diagnostics_tone = "neutral"

    data_monitor_html = "".join(
        [
            rail_module(
                kicker="Publication source",
                title=str(publication_input["source_label"]),
                body_html=(
                    '<p class="rail-copy">The published dataset remains URL-addressable and fully static for GitHub Pages.</p>'
                    + metric_list(
                        [
                            ("Rows", f"{int(pipeline_manifest['input_dataset']['rows']):,}"),
                            ("Source pages", str(source_coverage_report["source_page_count"])),
                        ]
                    )
                ),
                tone="accent",
            ),
            rail_module(
                kicker="Download surface",
                title=f"{len(publication_bundle_items)} publication files",
                body_html=(
                    '<p class="rail-copy">Publication outputs, staged provenance files, and fetch-failure snapshots are published as static artifacts.</p>'
                    + metric_list(
                        [
                            ("Publication files", str(len(publication_bundle_items))),
                            ("Staged bundle files", str(len(staged_bundle_items))),
                            ("Fetch-failure files", str(len(fetch_failure_items))),
                        ]
                    )
                ),
            ),
            rail_module(
                kicker="Diagnostics feed",
                title=diagnostics_title,
                body_html=diagnostics_body,
                tone=diagnostics_tone,
            ),
        ]
    )

    downloads = "".join(
        download_card_html(
            artifact,
            label=label,
            description=description,
        )
        for artifact in publication_bundle_items
        for label, description in [publication_download_card_metadata(str(artifact["site_path"]))]
    )
    staged_bundle_downloads = "".join(
        download_card_html(artifact)
        for artifact in staged_bundle_items
    )
    fetch_failure_downloads = "".join(
        download_card_html(artifact)
        for artifact in fetch_failure_items
    )

    category_table = render_table(
        ["Category", "Visible revenue", "Median revenue", "Revenue share"],
        [
            [
                html.escape(row["category"]),
                html.escape(usd_short(float(row["total_revenue"]))),
                html.escape(usd_short(float(row["median_revenue"]))),
                html.escape(pct(float(row["revenue_share"]))),
            ]
            for row in category_rows[:10]
        ],
    )

    band_table = render_table(
        ["Revenue band", "Startup count", "Visible revenue", "Revenue share"],
        [
            [
                html.escape(row["revenue_band"]),
                html.escape(f"{int(float(row['startup_count'])):,}"),
                html.escape(usd_short(float(row["total_revenue"]))),
                html.escape(pct(float(row["revenue_share"]))),
            ]
            for row in revenue_band_rows
        ],
    )

    source_table = render_table(
        ["Source page", "Startups", "Visible revenue", "Revenue share"],
        [
            [
                f'<a href="{html.escape(row["source_url"], quote=True)}">{html.escape(row["source_url"])}</a>',
                html.escape(f"{int(row['startup_count']):,}"),
                html.escape(usd_short(float(row["total_revenue_usd"]))),
                html.escape(pct(float(row["revenue_share"]))),
            ]
            for row in source_coverage_report["source_pages"][:10]
        ],
    )

    if source_pipeline_diagnostics["available"]:
        diagnostics_cards = f"""
<div class="card-grid">
  <article class="callout-card">
    <h3>Promotion cutover</h3>
    <p>{html.escape(str(source_pipeline_diagnostics['promoted_at'] or 'n/a'))}</p>
  </article>
  <article class="callout-card">
    <h3>Registry coverage</h3>
    <p>{html.escape(str(source_pipeline_diagnostics['selected_source_count']))} selected of {html.escape(str(source_pipeline_diagnostics['expected_source_count']))} expected sources.</p>
  </article>
  <article class="callout-card">
    <h3>Override coverage</h3>
    <p>{html.escape(str(source_pipeline_diagnostics['fully_mapped_visible_row_count']))} fully mapped visible rows with {html.escape(str(source_pipeline_diagnostics['alias_resolved_visible_row_count']))} alias-resolved cases.</p>
  </article>
  <article class="callout-card">
    <h3>Warning-only staged checks</h3>
    <p>{html.escape(source_pipeline_warning_summary(source_pipeline_diagnostics))}</p>
  </article>
  <article class="callout-card">
    <h3>Detail-page staging</h3>
    <p>{html.escape(str(source_pipeline_diagnostics['parsed_detail_page_count']))} parsed, {html.escape(str(source_pipeline_diagnostics['failed_detail_page_count']))} failed, {html.escape(str(source_pipeline_diagnostics['fetched_detail_page_count']))} fetched across {html.escape(str(source_pipeline_diagnostics['detail_page_target_count']))} target detail pages.</p>
  </article>
  <article class="callout-card">
    <h3>Fetch failures</h3>
    <p>{html.escape(str(source_pipeline_diagnostics['fetch_failure_source_count']))} source pages currently have staged fetch-failure snapshots.</p>
  </article>
  <article class="callout-card">
    <h3>Detail-field coverage</h3>
    <p>{html.escape(detail_field_population_summary(source_pipeline_diagnostics))}</p>
  </article>
</div>
"""
        diagnostics_table = render_table(
            ["Source page", "Parser", "Parsed cards", "Visible rows", "Failed details"],
            [
                [
                    f'<a href="{html.escape(row["source_url"], quote=True)}">{html.escape(row["source_url"])}</a>',
                    html.escape(str(row.get("parser_strategy") or "unknown")),
                    html.escape(f"{int(row['parsed_card_count']):,}"),
                    html.escape(f"{int(row['visible_sample_row_count']):,}"),
                    html.escape(f"{int(row['failed_detail_page_count']):,}"),
                ]
                for row in source_pipeline_diagnostics["source_pages"][:10]
            ],
        )
        failure_sources = source_pipeline_diagnostics.get("detail_parse_failure_sources", [])
        if failure_sources:
            diagnostics_failure_section = render_table(
                ["Source page", "Failed details", "Parsed details", "Fetched details", "Target details"],
                [
                    [
                        f'<a href="{html.escape(row["source_url"], quote=True)}">{html.escape(row["source_url"])}</a>',
                        html.escape(f"{int(row['failed_detail_page_count']):,}"),
                        html.escape(f"{int(row['parsed_detail_page_count']):,}"),
                        html.escape(f"{int(row['fetched_detail_page_count']):,}"),
                        html.escape(f"{int(row['detail_page_target_count']):,}"),
                    ]
                    for row in failure_sources[:10]
                ],
            )
        else:
            diagnostics_failure_section = (
                '<p class="section-note">No source pages in the active manifest currently report staged detail parse failures.</p>'
            )
        fetch_failure_sources = source_pipeline_diagnostics.get("fetch_failure_sources", [])
        if fetch_failure_sources:
            diagnostics_fetch_failure_section = render_table(
                [
                    "Source page",
                    "Recorded at",
                    "Robots",
                    "robots.txt",
                    "Delay",
                    "Status",
                    "Error",
                    "Severity",
                    "Retryability",
                    "Next action",
                    "HTML snapshot",
                    "Message",
                ],
                [
                    [
                        f'<a href="{html.escape(row["source_url"], quote=True)}">{html.escape(row["source_url"])}</a>',
                        html.escape(str(row.get("recorded_at") or "n/a")),
                        html.escape(str(row.get("robots_policy") or "unknown")),
                        html.escape(
                            str(row.get("robots_status_code") if row.get("robots_status_code") is not None else "n/a")
                        ),
                        html.escape(format_delay_seconds(row.get("robots_effective_delay_seconds"))),
                        html.escape(str(row.get("status_code") if row.get("status_code") is not None else "n/a")),
                        html.escape(str(row.get("error_type") or "unknown")),
                        html.escape(str(row.get("failure_severity") or "unknown")),
                        html.escape(str(row.get("failure_retryability") or "unknown")),
                        html.escape(str(row.get("failure_next_action") or "unknown")),
                        html.escape("yes" if row.get("has_html_snapshot") else "no"),
                        html.escape(str(row.get("message") or "")),
                    ]
                    for row in fetch_failure_sources[:10]
                ],
            )
        else:
            diagnostics_fetch_failure_section = (
                '<p class="section-note">No staged source fetch failures are currently recorded for the active manifest.</p>'
            )
        fetch_failure_earliest_recorded_at = source_pipeline_diagnostics.get("fetch_failure_earliest_recorded_at")
        fetch_failure_latest_recorded_at = source_pipeline_diagnostics.get("fetch_failure_latest_recorded_at")
        if fetch_failure_sources and fetch_failure_earliest_recorded_at and fetch_failure_latest_recorded_at:
            diagnostics_fetch_failure_timing_section = (
                "<h3>Fetch-failure timing</h3>"
                f'<p class="section-note">Earliest recorded fetch failure: <strong>{html.escape(str(fetch_failure_earliest_recorded_at))}</strong>. '
                f'Latest recorded fetch failure: <strong>{html.escape(str(fetch_failure_latest_recorded_at))}</strong>.</p>'
            )
        else:
            diagnostics_fetch_failure_timing_section = (
                "<h3>Fetch-failure timing</h3>"
                '<p class="section-note">No staged fetch-failure timing is currently recorded for the active manifest.</p>'
            )
        fetch_failure_source_label_counts = source_pipeline_diagnostics.get("fetch_failure_source_label_counts", {}) or {}
        fetch_failure_source_group_counts = source_pipeline_diagnostics.get("fetch_failure_source_group_counts", {}) or {}
        if fetch_failure_sources:
            diagnostics_fetch_failure_source_context_section = (
                "<h3>Fetch-failure source context</h3>"
                + render_table(
                    ["Source label", "Affected sources"],
                    [
                        [
                            html.escape(str(source_label)),
                            html.escape(f"{int(count):,}"),
                        ]
                        for source_label, count in fetch_failure_source_label_counts.items()
                    ],
                )
                + render_table(
                    ["Source group", "Affected sources"],
                    [
                        [
                            html.escape(str(source_group)),
                            html.escape(f"{int(count):,}"),
                        ]
                        for source_group, count in fetch_failure_source_group_counts.items()
                    ],
                )
            )
        else:
            diagnostics_fetch_failure_source_context_section = (
                "<h3>Fetch-failure source context</h3>"
                '<p class="section-note">No staged fetch-failure source-label context is currently recorded for the active manifest.</p>'
            )
        fetch_failure_parser_strategy_counts = (
            source_pipeline_diagnostics.get("fetch_failure_parser_strategy_counts", {}) or {}
        )
        if fetch_failure_sources:
            diagnostics_fetch_failure_parser_context_section = (
                "<h3>Fetch-failure parser context</h3>"
                + render_table(
                    ["Parser strategy", "Affected sources"],
                    [
                        [
                            html.escape(str(parser_strategy)),
                            html.escape(f"{int(count):,}"),
                        ]
                        for parser_strategy, count in fetch_failure_parser_strategy_counts.items()
                    ],
                )
            )
        else:
            diagnostics_fetch_failure_parser_context_section = (
                "<h3>Fetch-failure parser context</h3>"
                '<p class="section-note">No staged fetch-failure parser-strategy context is currently recorded for the active manifest.</p>'
            )
        fetch_failure_severity_counts = source_pipeline_diagnostics.get("fetch_failure_severity_counts", {}) or {}
        if fetch_failure_sources:
            diagnostics_fetch_failure_severity_section = (
                "<h3>Fetch-failure severity</h3>"
                + render_table(
                    ["Severity class", "Affected sources"],
                    [
                        [
                            html.escape(str(severity)),
                            html.escape(f"{int(count):,}"),
                        ]
                        for severity, count in fetch_failure_severity_counts.items()
                    ],
                )
            )
        else:
            diagnostics_fetch_failure_severity_section = (
                "<h3>Fetch-failure severity</h3>"
                '<p class="section-note">No staged fetch-failure severity context is currently recorded for the active manifest.</p>'
            )
        fetch_failure_retryability_counts = (
            source_pipeline_diagnostics.get("fetch_failure_retryability_counts", {}) or {}
        )
        if fetch_failure_sources:
            diagnostics_fetch_failure_retryability_section = (
                "<h3>Fetch-failure retryability</h3>"
                + render_table(
                    ["Retryability hint", "Affected sources"],
                    [
                        [
                            html.escape(str(retryability)),
                            html.escape(f"{int(count):,}"),
                        ]
                        for retryability, count in fetch_failure_retryability_counts.items()
                    ],
                )
            )
        else:
            diagnostics_fetch_failure_retryability_section = (
                "<h3>Fetch-failure retryability</h3>"
                '<p class="section-note">No staged fetch-failure retryability context is currently recorded for the active manifest.</p>'
            )
        fetch_failure_next_action_counts = (
            source_pipeline_diagnostics.get("fetch_failure_next_action_counts", {}) or {}
        )
        fetch_failure_next_action_source_lists = (
            source_pipeline_diagnostics.get("fetch_failure_next_action_source_lists", []) or []
        )
        fetch_failure_next_action_artifact_rollups = (
            source_pipeline_diagnostics.get("fetch_failure_next_action_artifact_rollups", []) or []
        )
        if fetch_failure_sources:
            diagnostics_fetch_failure_next_action_section = (
                "<h3>Fetch-failure next actions</h3>"
                + render_table(
                    ["Recommended action", "Affected sources"],
                    [
                        [
                            html.escape(str(next_action)),
                            html.escape(f"{int(count):,}"),
                        ]
                        for next_action, count in fetch_failure_next_action_counts.items()
                    ],
                )
                + render_table(
                    [
                        "Recommended action",
                        "Affected sources",
                        "Action artifact rollup",
                        "Artifact format counts",
                        "Artifact-format source counts",
                        "Artifact-format source-count total",
                        "Distinct artifact formats",
                    ],
                    [
                        [
                            html.escape(str(rollup.get("failure_next_action") or "unknown")),
                            html.escape(f"{int(rollup.get('source_count') or 0):,}"),
                            html.escape(
                                str(rollup.get("artifact_summary") or "No staged fetch-failure artifact rollup")
                            ),
                            html.escape(
                                str(
                                    rollup.get("artifact_format_count_summary")
                                    or "No staged fetch-failure artifact format counts"
                                )
                            ),
                            html.escape(
                                str(
                                    rollup.get("artifact_format_source_count_summary")
                                    or "No staged fetch-failure artifact-format source counts"
                                )
                            ),
                            html.escape(
                                str(
                                    rollup.get("artifact_format_source_count_total_summary")
                                    or "No staged fetch-failure artifact-format source-count total"
                                )
                            ),
                            html.escape(
                                str(
                                    rollup.get("artifact_format_distinct_count_summary")
                                    or "No staged fetch-failure distinct artifact-format count"
                                )
                            ),
                        ]
                        for rollup in fetch_failure_next_action_artifact_rollups
                    ],
                )
                + render_table(
                    [
                        "Recommended action",
                        "Artifact format",
                        "Format source count",
                        "Format source labels",
                        "Format source pages",
                    ],
                    [
                        [
                            html.escape(str(rollup.get("failure_next_action") or "unknown")),
                            html.escape(str(format_group.get("format") or "unknown").upper()),
                            html.escape(f"{int(format_group.get('source_count') or 0):,}"),
                            "<br>".join(
                                html.escape(
                                    str(source.get("source_label") or source.get("source_id") or "unknown")
                                )
                                for source in format_group.get("sources", [])
                            )
                            or html.escape("n/a"),
                            "<br>".join(
                                (
                                    f'<a href="{html.escape(str(source.get("source_url") or ""), quote=True)}">'
                                    f'{html.escape(str(source.get("source_url") or source.get("source_id") or "unknown"))}</a>'
                                )
                                if str(source.get("source_url") or "")
                                else html.escape(str(source.get("source_id") or "unknown"))
                                for source in format_group.get("sources", [])
                            )
                            or html.escape("n/a"),
                        ]
                        for rollup in fetch_failure_next_action_artifact_rollups
                        for format_group in rollup.get("artifact_format_source_lists", [])
                    ],
                )
                + render_table(
                    ["Recommended action", "Affected source labels", "Affected source pages"],
                    [
                        [
                            html.escape(str(group.get("failure_next_action") or "unknown")),
                            "<br>".join(
                                html.escape(str(source.get("source_label") or source.get("source_id") or "unknown"))
                                for source in group.get("sources", [])
                            )
                            or html.escape("n/a"),
                            "<br>".join(
                                (
                                    f'<a href="{html.escape(str(source.get("source_url") or ""), quote=True)}">'
                                    f'{html.escape(str(source.get("source_url") or source.get("source_id") or "unknown"))}</a>'
                                )
                                if str(source.get("source_url") or "")
                                else html.escape(str(source.get("source_id") or "unknown"))
                                for source in group.get("sources", [])
                            )
                            or html.escape("n/a"),
                        ]
                        for group in fetch_failure_next_action_source_lists
                    ],
                )
                + render_table(
                    ["Recommended action", "Source label", "Source page", "Failure context", "Artifact summary", "Artifact links"],
                    [
                        [
                            html.escape(str(group.get("failure_next_action") or "unknown")),
                            html.escape(str(source.get("source_label") or source.get("source_id") or "unknown")),
                            (
                                f'<a href="{html.escape(str(source.get("source_url") or ""), quote=True)}">'
                                f'{html.escape(str(source.get("source_url") or source.get("source_id") or "unknown"))}</a>'
                            )
                            if str(source.get("source_url") or "")
                            else html.escape(str(source.get("source_id") or "unknown")),
                            html.escape(str(source.get("failure_context_summary") or "unknown")),
                            html.escape(str(source.get("artifact_summary") or "No staged fetch-failure artifact summary")),
                            "<br>".join(
                                f'<a href="{html.escape(str(artifact.get("site_path") or ""), quote=True)}">'
                                f'{html.escape(str(artifact.get("label") or artifact.get("path") or "artifact"))}</a>'
                                for artifact in source.get("artifact_links", [])
                                if str(artifact.get("site_path") or "")
                            )
                            or html.escape("No staged fetch-failure artifact links"),
                        ]
                        for group in fetch_failure_next_action_source_lists
                        for source in group.get("sources", [])
                    ],
                )
            )
        else:
            diagnostics_fetch_failure_next_action_section = (
                "<h3>Fetch-failure next actions</h3>"
                '<p class="section-note">No staged fetch-failure next-action recommendations, source lists, source details, artifact links, artifact summaries, artifact rollups, artifact format counts, artifact-format source lists, artifact-format source-count summaries, artifact-format source-count totals, or distinct artifact-format counts are currently recorded for the active manifest.</p>'
            )
        fetch_failure_html_snapshot_availability_counts = (
            source_pipeline_diagnostics.get("fetch_failure_html_snapshot_availability_counts", {}) or {}
        )
        if fetch_failure_sources:
            diagnostics_fetch_failure_snapshot_context_section = (
                "<h3>Fetch-failure snapshot availability</h3>"
                + render_table(
                    ["HTML snapshot", "Affected sources"],
                    [
                        [
                            html.escape(str(snapshot_availability)),
                            html.escape(f"{int(count):,}"),
                        ]
                        for snapshot_availability, count in fetch_failure_html_snapshot_availability_counts.items()
                    ],
                )
            )
        else:
            diagnostics_fetch_failure_snapshot_context_section = (
                "<h3>Fetch-failure snapshot availability</h3>"
                '<p class="section-note">No staged fetch-failure snapshot-availability context is currently recorded for the active manifest.</p>'
            )
        fetch_failure_robots_policy_counts = source_pipeline_diagnostics.get("fetch_failure_robots_policy_counts", {}) or {}
        fetch_failure_robots_status_code_counts = (
            source_pipeline_diagnostics.get("fetch_failure_robots_status_code_counts", {}) or {}
        )
        fetch_failure_effective_delay_seconds_counts = (
            source_pipeline_diagnostics.get("fetch_failure_effective_delay_seconds_counts", {}) or {}
        )
        fetch_failure_min_effective_delay_seconds = source_pipeline_diagnostics.get(
            "fetch_failure_min_effective_delay_seconds"
        )
        fetch_failure_max_effective_delay_seconds = source_pipeline_diagnostics.get(
            "fetch_failure_max_effective_delay_seconds"
        )
        if fetch_failure_sources:
            delay_note = ""
            if (
                fetch_failure_min_effective_delay_seconds is not None
                and fetch_failure_max_effective_delay_seconds is not None
            ):
                delay_note = (
                    f'<p class="section-note">Shortest recorded effective delay: '
                    f'<strong>{html.escape(format_delay_seconds(fetch_failure_min_effective_delay_seconds))}</strong>. '
                    f'Longest recorded effective delay: '
                    f'<strong>{html.escape(format_delay_seconds(fetch_failure_max_effective_delay_seconds))}</strong>.</p>'
                )
            diagnostics_fetch_failure_delay_section = (
                "<h3>Fetch-failure delay context</h3>"
                + delay_note
                + render_table(
                    ["Effective delay", "Affected sources"],
                    [
                        [
                            html.escape(
                                "n/a" if str(delay_seconds) == "n/a" else format_delay_seconds(delay_seconds)
                            ),
                            html.escape(f"{int(count):,}"),
                        ]
                        for delay_seconds, count in fetch_failure_effective_delay_seconds_counts.items()
                    ],
                )
            )
        else:
            diagnostics_fetch_failure_delay_section = (
                "<h3>Fetch-failure delay context</h3>"
                '<p class="section-note">No staged fetch-failure delay context is currently recorded for the active manifest.</p>'
            )
        if fetch_failure_sources:
            diagnostics_fetch_failure_robots_section = (
                "<h3>Fetch-failure robots context</h3>"
                + render_table(
                    ["Robots policy", "Affected sources"],
                    [
                        [
                            html.escape(str(policy)),
                            html.escape(f"{int(count):,}"),
                        ]
                        for policy, count in fetch_failure_robots_policy_counts.items()
                    ],
                )
                + render_table(
                    ["robots.txt status", "Affected sources"],
                    [
                        [
                            html.escape(str(status_code)),
                            html.escape(f"{int(count):,}"),
                        ]
                        for status_code, count in fetch_failure_robots_status_code_counts.items()
                    ],
                )
            )
        else:
            diagnostics_fetch_failure_robots_section = (
                "<h3>Fetch-failure robots context</h3>"
                '<p class="section-note">No staged fetch-failure robots context is currently recorded for the active manifest.</p>'
            )
        fetch_failure_error_type_counts = source_pipeline_diagnostics.get("fetch_failure_error_type_counts", {}) or {}
        fetch_failure_status_code_counts = source_pipeline_diagnostics.get("fetch_failure_status_code_counts", {}) or {}
        if fetch_failure_sources:
            diagnostics_fetch_failure_breakdown_section = (
                "<h3>Fetch-failure causes</h3>"
                + render_table(
                    ["Error type", "Affected sources"],
                    [
                        [
                            html.escape(str(error_type)),
                            html.escape(f"{int(count):,}"),
                        ]
                        for error_type, count in fetch_failure_error_type_counts.items()
                    ],
                )
                + render_table(
                    ["Status code", "Affected sources"],
                    [
                        [
                            html.escape(str(status_code)),
                            html.escape(f"{int(count):,}"),
                        ]
                        for status_code, count in fetch_failure_status_code_counts.items()
                    ],
                )
            )
        else:
            diagnostics_fetch_failure_breakdown_section = (
                "<h3>Fetch-failure causes</h3>"
                '<p class="section-note">No staged fetch-failure causes are currently recorded for the active manifest.</p>'
            )
        coverage_rows = [
            row
            for row in sorted(
                source_pipeline_diagnostics["source_pages"],
                key=lambda row: (
                    -int(row.get("parsed_detail_page_count", 0)),
                    -int((row.get("detail_field_population_counts", {}) or {}).get("problem_solved", 0)),
                    -int((row.get("detail_field_population_counts", {}) or {}).get("pricing_summary", 0)),
                    str(row.get("source_id") or ""),
                ),
            )
            if int(row.get("parsed_detail_page_count", 0)) > 0
            or any(int(value) > 0 for value in (row.get("detail_field_population_counts", {}) or {}).values())
        ]
        if coverage_rows:
            diagnostics_coverage_section = render_table(
                ["Source page", "Parsed details", "Problem solved", "Pricing", "Audience", "Founder names"],
                [
                    [
                        f'<a href="{html.escape(row["source_url"], quote=True)}">{html.escape(row["source_url"])}</a>',
                        html.escape(f"{int(row['parsed_detail_page_count']):,}"),
                        html.escape(
                            f"{int((row.get('detail_field_population_counts', {}) or {}).get('problem_solved', 0)):,}"
                        ),
                        html.escape(
                            f"{int((row.get('detail_field_population_counts', {}) or {}).get('pricing_summary', 0)):,}"
                        ),
                        html.escape(
                            f"{int((row.get('detail_field_population_counts', {}) or {}).get('target_audience', 0)):,}"
                        ),
                        html.escape(
                            f"{int((row.get('detail_field_population_counts', {}) or {}).get('founder_name', 0)):,}"
                        ),
                    ]
                    for row in coverage_rows[:10]
                ],
            )
        else:
            diagnostics_coverage_section = (
                '<p class="section-note">No staged detail rows in the active manifest currently populate the shared detail fields.</p>'
            )
        diagnostics_section = section(
            "Source-pipeline diagnostics",
            "The promoted bundle keeps the staged parser/override/duplicate provenance visible instead of burying it inside the repo-only staging directory.",
            diagnostics_cards
            + diagnostics_table
            + diagnostics_failure_section
            + diagnostics_fetch_failure_section
            + diagnostics_fetch_failure_timing_section
            + diagnostics_fetch_failure_source_context_section
            + diagnostics_fetch_failure_parser_context_section
            + diagnostics_fetch_failure_severity_section
            + diagnostics_fetch_failure_retryability_section
            + diagnostics_fetch_failure_next_action_section
            + diagnostics_fetch_failure_snapshot_context_section
            + diagnostics_fetch_failure_delay_section
            + diagnostics_fetch_failure_robots_section
            + diagnostics_fetch_failure_breakdown_section
            + diagnostics_coverage_section
            + (
                f'<p class="section-note">Staged validation: <strong>{html.escape(status_label(str(source_pipeline_diagnostics["validation_status"])))}</strong>. '
                f'Run-manifest validation: <strong>{html.escape(status_label(str(source_pipeline_diagnostics["run_manifest_validation_status"])))}</strong>. '
                f'Suspicious duplicate groups: <strong>{html.escape(str(source_pipeline_diagnostics["suspicious_duplicate_group_count"]))}</strong>. '
                f'Fetch failures: <strong>{html.escape(str(source_pipeline_diagnostics["fetch_failure_source_count"]))}</strong> source pages. '
                f'Detail parse failures: <strong>{html.escape(str(source_pipeline_diagnostics["failed_detail_page_count"]))}</strong> across '
                f'<strong>{html.escape(str(source_pipeline_diagnostics["detail_parse_failure_source_count"]))}</strong> source pages.</p>'
            ),
            section_id="source-pipeline-diagnostics",
            panel_code="DT.07",
            panel_tag="provenance monitor",
        )
    else:
        diagnostics_section = section(
            "Source-pipeline diagnostics",
            "The publication build records whether live-source promotion diagnostics are attached to the active dataset.",
            f"""
<div class="two-up">
  <article class="callout-card">
    <h3>Diagnostics availability</h3>
    <p>{html.escape(str(source_pipeline_diagnostics['message']))}</p>
  </article>
  <article class="callout-card">
    <h3>Current publication source</h3>
    <p>{html.escape(str(publication_input['source_label']))}</p>
  </article>
</div>
""",
            section_id="source-pipeline-diagnostics",
            panel_code="DT.07",
            panel_tag="provenance monitor",
        )

    manifest_note = (
        f"Publication input: {publication_input['dataset_path']} ({publication_input['dataset_kind']})."
        f" "
        f"Input dataset hash: {pipeline_manifest['input_dataset']['sha256']}."
        f" Validation status: {status_label(str(validation_report['status']))}."
    )

    sections = [
        section(
            "Downloads",
            "Manifest-driven publication outputs are copied into the site so the published Pages bundle stays inspectable on its own.",
            download_section_summary_html(publication_bundle_items) + f'<div class="card-grid">{downloads}</div>',
            section_id="downloads",
            panel_code="DT.01",
            panel_tag="core outputs",
            layout="compact",
        ),
        section(
            "Staged Bundle",
            "The active promoted staged source-pipeline bundle is exposed as a separate manifest-driven provenance surface, distinct from the promoted dataset contract.",
            (
                download_section_summary_html(staged_bundle_items) + f'<div class="card-grid">{staged_bundle_downloads}</div>'
                if staged_bundle_items
                else (
                    download_section_summary_html(staged_bundle_items)
                    + '<p class="section-note">No staged source-pipeline bundle downloads are attached to the active publication manifest.</p>'
                )
            ),
            section_id="staged-bundle",
            panel_code="DT.02",
            panel_tag="staged provenance",
            layout="compact",
        ),
        section(
            "Fetch Failure Snapshots",
            "When staged source fetch failures exist, their raw snapshot artifacts are exposed here as manifest-driven downloads separate from the promoted dataset contract.",
            (
                download_section_summary_html(fetch_failure_items) + f'<div class="card-grid">{fetch_failure_downloads}</div>'
                if fetch_failure_items
                else (
                    download_section_summary_html(fetch_failure_items)
                    + '<p class="section-note">No staged fetch-failure snapshot downloads are currently attached to the active manifest.</p>'
                )
            ),
            section_id="fetch-failure-snapshots",
            panel_code="DT.03",
            panel_tag="failure cache",
            layout="compact",
        ),
        section(
            "Top categories",
            "Category revenue concentration remains the clearest summary of the current sample shape.",
            category_table,
            section_id="top-categories",
            panel_code="DT.04",
            panel_tag="category board",
            layout="compact",
        ),
        section(
            "Revenue bands",
            "Most visible startups sit below $50k in 30-day revenue even though the total revenue is dominated by the top tail.",
            band_table,
            section_id="revenue-bands",
            panel_code="DT.05",
            panel_tag="distribution board",
            layout="compact",
        ),
        section(
            "Source coverage",
            "The source coverage report keeps the public-page footprint explicit, with links back to the source pages.",
            source_table,
            section_id="source-coverage",
            panel_code="DT.06",
            panel_tag="source monitor",
            layout="compact",
        ),
        diagnostics_section,
        section(
            "Manifest notes",
            "The site build depends on the processed research outputs already generated in the repository root.",
            f"""
<div class="two-up">
  <article class="callout-card">
    <h3>Sample size</h3>
    <p>{html.escape(f"{metrics['sample_size']:,}")} startups from {html.escape(str(source_coverage_report['source_page_count']))} source pages.</p>
  </article>
  <article class="callout-card">
    <h3>Build provenance</h3>
    <p>{html.escape(manifest_note)}</p>
  </article>
</div>
""",
            section_id="manifest-notes",
            panel_code="DT.08",
            panel_tag="build notes",
            layout="compact",
        ),
    ]

    body = hero + f'<div class="panel-stack">{"".join(sections)}</div>'
    return page_shell(
        title="Data",
        active="data",
        description="Download links and source coverage for the TrustMRR visible-sample static site.",
        status=str(validation_report["status"]),
        command_links=command_links,
        output_registry_sections=output_registry_sections,
        monitor_html=data_monitor_html,
        body_html=body,
    )


def build_stylesheet() -> str:
    return """\
:root {
  --bg: #05070a;
  --bg-elevated: #0b0f13;
  --bg-panel: #11161d;
  --bg-panel-strong: #171d26;
  --ink: #f4ead7;
  --ink-soft: #b6ae9d;
  --ink-dim: #827c70;
  --line: rgba(246, 165, 58, 0.18);
  --line-strong: rgba(246, 165, 58, 0.38);
  --accent: #f6a53a;
  --accent-soft: rgba(246, 165, 58, 0.14);
  --cyan: #62c9d6;
  --cyan-soft: rgba(98, 201, 214, 0.14);
  --green: #83d486;
  --green-soft: rgba(131, 212, 134, 0.14);
  --red: #ff7b69;
  --red-soft: rgba(255, 123, 105, 0.14);
  --warning: #ffbf61;
  --warning-soft: rgba(255, 191, 97, 0.14);
  --shadow: 0 24px 64px rgba(0, 0, 0, 0.42);
  --radius-lg: 18px;
  --radius-md: 12px;
  --mono: "IBM Plex Mono", "JetBrains Mono", "SFMono-Regular", Consolas, "Liberation Mono", monospace;
  --sans: "Aptos", "Segoe UI Variable", "Segoe UI", sans-serif;
}

* {
  box-sizing: border-box;
}

html {
  scroll-behavior: smooth;
}

body {
  margin: 0;
  color: var(--ink);
  background:
    radial-gradient(circle at top left, rgba(246, 165, 58, 0.12), transparent 30%),
    radial-gradient(circle at top right, rgba(98, 201, 214, 0.08), transparent 22%),
    linear-gradient(180deg, #05070a 0%, #0a0e13 48%, #05070a 100%);
  font-family: var(--mono);
  line-height: 1.55;
}

.page-backdrop {
  position: fixed;
  inset: 0;
  pointer-events: none;
  background-image:
    linear-gradient(rgba(246, 165, 58, 0.03) 1px, transparent 1px),
    linear-gradient(90deg, rgba(246, 165, 58, 0.025) 1px, transparent 1px),
    linear-gradient(180deg, rgba(255, 255, 255, 0.025) 0%, transparent 18%, rgba(0, 0, 0, 0.06) 100%);
  background-size: 28px 28px, 28px 28px, 100% 6px;
  mask-image: linear-gradient(180deg, rgba(0, 0, 0, 0.82), rgba(0, 0, 0, 0.4));
  opacity: 0.8;
}

.site-shell {
  width: min(1480px, calc(100vw - 32px));
  margin: 0 auto;
}

.command-strip {
  position: sticky;
  top: 0;
  z-index: 20;
  backdrop-filter: blur(16px);
  background: rgba(5, 7, 10, 0.92);
  border-bottom: 1px solid var(--line-strong);
}

.command-strip-inner {
  display: flex;
  gap: 18px;
  align-items: center;
  justify-content: space-between;
  padding: 12px 0;
}

.brand {
  display: inline-flex;
  flex-direction: column;
  gap: 4px;
  color: inherit;
  text-decoration: none;
}

.brand-kicker,
.eyebrow,
.stat-label,
.panel-kicker,
.rail-kicker,
.command-prompt-label {
  letter-spacing: 0.18em;
  text-transform: uppercase;
  font-size: 0.72rem;
  color: var(--accent);
  font-weight: 700;
}

.brand-title {
  font-size: 0.96rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.ticker-strip,
.command-bar-links {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}

.shell-pill {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  min-height: 34px;
  padding: 0 12px;
  border: 1px solid var(--line);
  border-radius: 999px;
  background: rgba(17, 22, 29, 0.92);
  color: var(--ink-soft);
  font-size: 0.76rem;
  text-transform: uppercase;
  letter-spacing: 0.08em;
}

.shell-pill strong {
  color: var(--ink);
  font-size: 0.7rem;
}

.shell-pill-accent {
  border-color: var(--line-strong);
  background: var(--accent-soft);
}

.shell-pill.is-passed {
  color: var(--green);
  background: var(--green-soft);
  border-color: rgba(131, 212, 134, 0.36);
}

.shell-pill.is-warning {
  color: var(--warning);
  background: var(--warning-soft);
  border-color: rgba(255, 191, 97, 0.36);
}

.shell-pill.is-failed {
  color: var(--red);
  background: var(--red-soft);
  border-color: rgba(255, 123, 105, 0.36);
}

.nav-links {
  display: grid;
  gap: 8px;
}

.rail-command-links {
  display: grid;
  gap: 8px;
}

.rail-command-group {
  display: grid;
  gap: 10px;
}

.rail-command-group + .rail-command-group {
  padding-top: 10px;
  border-top: 1px dashed rgba(246, 165, 58, 0.16);
}

.rail-command-group-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 10px;
  flex-wrap: wrap;
}

.rail-command-group-meta {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 8px;
}

.rail-command-divider {
  display: flex;
  align-items: center;
  gap: 10px;
  margin: 2px 0 0;
  color: var(--ink-dim);
}

.rail-command-divider::before,
.rail-command-divider::after {
  content: "";
  flex: 1 1 auto;
  border-top: 1px dashed rgba(98, 201, 214, 0.2);
}

.rail-command-divider-label {
  font-size: 0.65rem;
  letter-spacing: 0.14em;
  text-transform: uppercase;
  color: var(--cyan);
}

.rail-command-divider-count {
  font-size: 0.64rem;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--ink-soft);
}

.rail-command-divider-bytes {
  font-size: 0.64rem;
  letter-spacing: 0.08em;
  color: var(--ink-dim);
}

.rail-command-group-title,
.rail-command-group-empty {
  margin: 0;
}

.rail-command-group-title {
  color: var(--ink-soft);
  text-transform: uppercase;
  letter-spacing: 0.12em;
  font-size: 0.68rem;
}

.rail-command-group-empty {
  color: var(--ink-dim);
  font-size: 0.76rem;
}

.nav-link,
.rail-command-link,
.command-chip {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-height: 42px;
  padding: 10px 14px;
  border-radius: 999px;
  color: var(--ink-soft);
  text-decoration: none;
  border: 1px solid var(--line);
  background: rgba(17, 22, 29, 0.92);
  transition: 180ms ease;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-size: 0.78rem;
}

.nav-link:hover,
.nav-link.is-active,
.rail-command-link:hover,
.command-chip:hover {
  color: var(--ink);
  background: var(--accent-soft);
  border-color: var(--line-strong);
}

.workstation {
  display: grid;
  grid-template-columns: 240px minmax(0, 1fr) 290px;
  gap: 18px;
  padding: 18px 0 0;
  align-items: start;
}

.control-rail,
.monitor-rail {
  position: sticky;
  top: 74px;
  display: grid;
  gap: 14px;
}

.workspace {
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 18px;
}

.rail-module,
.workspace-command,
.hero,
.section-card,
.site-footer {
  background: linear-gradient(180deg, rgba(23, 29, 38, 0.95), rgba(11, 15, 19, 0.98));
  border: 1px solid var(--line);
  box-shadow: var(--shadow);
  border-radius: var(--radius-lg);
  position: relative;
  overflow: hidden;
}

.rail-module::before,
.workspace-command::before,
.hero::before,
.section-card::before,
.site-footer::before,
.table-wrap::before,
.stat-card::before,
.callout-card::before,
.download-card::before,
.chart-card::before,
.check-item::before {
  content: "";
  position: absolute;
  inset: 0 auto auto 0;
  width: 100%;
  height: 2px;
  background: linear-gradient(90deg, rgba(246, 165, 58, 0.85), rgba(98, 201, 214, 0.28), transparent 92%);
  pointer-events: none;
}

.rail-module,
.workspace-command,
.site-footer {
  padding: 16px 18px;
}

.rail-module-accent {
  border-color: rgba(246, 165, 58, 0.36);
}

.rail-module-warning {
  border-color: rgba(255, 191, 97, 0.32);
}

.rail-module-good {
  border-color: rgba(131, 212, 134, 0.32);
}

.rail-title {
  margin: 6px 0 10px;
  font-size: 1rem;
}

.rail-copy,
.hero-aside-note,
.section-head p,
.site-footer p,
.chart-copy p,
.callout-card p,
.download-card p,
.hero-lede,
.stat-note,
.prose p,
.prose li,
.check-item span,
.metric-list dt {
  color: var(--ink-soft);
}

.metric-list {
  margin: 14px 0 0;
  display: grid;
  gap: 8px;
}

.metric-list div {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 12px;
  padding-top: 8px;
  border-top: 1px dashed rgba(246, 165, 58, 0.18);
}

.metric-list dt {
  font-size: 0.72rem;
  text-transform: uppercase;
  letter-spacing: 0.14em;
}

.metric-list dd {
  margin: 0;
  color: var(--ink);
  text-align: right;
}

.workspace-command {
  display: flex;
  gap: 14px;
  align-items: center;
  justify-content: space-between;
}

.command-prompt {
  display: inline-flex;
  align-items: center;
  gap: 10px;
  color: var(--ink-soft);
}

.command-entry {
  min-width: min(100%, 460px);
  display: grid;
  gap: 8px;
}

.command-input-wrap {
  display: flex;
  align-items: center;
  gap: 10px;
  min-height: 56px;
  padding: 10px 14px;
  border: 1px solid rgba(246, 165, 58, 0.24);
  border-radius: 12px;
  background: rgba(246, 165, 58, 0.08);
}

.command-prefix {
  color: var(--accent);
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-size: 0.76rem;
  white-space: nowrap;
}

.command-input {
  width: 100%;
  min-width: 0;
  border: 0;
  outline: 0;
  background: transparent;
  color: var(--ink);
  font: inherit;
}

.command-input::placeholder {
  color: var(--ink-dim);
}

.command-help,
.command-status {
  margin: 0;
  color: var(--ink-soft);
  font-size: 0.8rem;
}

.command-status {
  text-transform: uppercase;
  letter-spacing: 0.08em;
}

.command-chip.is-muted,
.rail-command-link.is-muted {
  opacity: 0.38;
}

.command-chip.is-targeted,
.rail-command-link.is-targeted {
  color: var(--ink);
  background: rgba(98, 201, 214, 0.12);
  border-color: rgba(98, 201, 214, 0.34);
}

.hero,
.section-card {
  scroll-margin-top: 92px;
}

.hero.is-focused,
.section-card.is-focused {
  border-color: rgba(98, 201, 214, 0.42);
  box-shadow:
    0 0 0 1px rgba(98, 201, 214, 0.24),
    var(--shadow);
}

.hero {
  padding: 24px;
}

.hero-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.7fr) minmax(280px, 0.9fr);
  gap: 18px;
  align-items: stretch;
}

.hero-head {
  display: flex;
  gap: 16px;
  align-items: flex-start;
  justify-content: space-between;
  margin-bottom: 14px;
}

.panel-code {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  color: var(--ink-soft);
  text-transform: uppercase;
  letter-spacing: 0.12em;
  font-size: 0.74rem;
  white-space: nowrap;
}

.panel-code::before {
  content: "";
  width: 8px;
  height: 8px;
  border-radius: 999px;
  background: var(--accent);
  box-shadow: 0 0 0 4px rgba(246, 165, 58, 0.12);
}

.hero-panel {
  padding: 18px;
  border: 1px solid rgba(246, 165, 58, 0.2);
  border-radius: var(--radius-md);
  background: linear-gradient(180deg, rgba(246, 165, 58, 0.08), rgba(11, 15, 19, 0.4));
}

.hero-aside {
  display: grid;
  gap: 10px;
}

.hero-aside p {
  margin: 0;
}

h1,
h2,
h3 {
  margin: 0 0 12px;
  line-height: 1.05;
  font-family: var(--sans);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

h1 {
  font-size: clamp(2.2rem, 4vw, 4.4rem);
  max-width: 13ch;
}

h2 {
  font-size: clamp(1.2rem, 1.7vw, 1.75rem);
}

h3 {
  font-size: 1rem;
}

.hero-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  align-items: center;
  margin-top: 22px;
}

.button {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-height: 44px;
  padding: 0 18px;
  border-radius: 999px;
  text-decoration: none;
  color: #140f06;
  background: linear-gradient(135deg, #f6a53a 0%, #ffbf61 100%);
  border: 1px solid rgba(255, 191, 97, 0.52);
  box-shadow: 0 10px 22px rgba(246, 165, 58, 0.18);
  text-transform: uppercase;
  letter-spacing: 0.06em;
  font-size: 0.82rem;
  font-weight: 700;
}

.button.button-secondary {
  color: var(--ink);
  background: rgba(17, 22, 29, 0.92);
  box-shadow: none;
  border: 1px solid rgba(98, 201, 214, 0.3);
}

.status-pill {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  min-height: 36px;
  padding: 0 14px;
  border-radius: 999px;
  font-size: 0.78rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  border: 1px solid currentColor;
}

.status-pill.is-passed {
  color: var(--green);
  background: var(--green-soft);
}

.status-pill.is-warning {
  color: var(--amber);
  background: var(--amber-soft);
}

.status-pill.is-failed {
  color: var(--red);
  background: var(--red-soft);
}

.hero-aside-value,
.stat-value {
  margin: 0;
  font-size: clamp(1.8rem, 2.2vw, 2.7rem);
  font-weight: 800;
  font-family: var(--sans);
  letter-spacing: 0.03em;
}

.section-card {
  grid-column: 1 / -1;
  padding: 22px;
}

.section-card.layout-compact {
  grid-column: span 6;
}

.panel-stack {
  display: grid;
  grid-template-columns: repeat(12, minmax(0, 1fr));
  gap: 18px;
}

.section-head {
  display: flex;
  gap: 14px;
  align-items: flex-start;
  justify-content: space-between;
  margin-bottom: 18px;
  padding-bottom: 14px;
  border-bottom: 1px solid rgba(246, 165, 58, 0.16);
}

.section-head-copy {
  display: grid;
  gap: 6px;
}

.panel-tag {
  display: inline-flex;
  align-items: center;
  min-height: 30px;
  padding: 0 10px;
  border-radius: 999px;
  border: 1px solid rgba(98, 201, 214, 0.24);
  background: rgba(98, 201, 214, 0.08);
  color: var(--cyan);
  text-transform: uppercase;
  letter-spacing: 0.1em;
  font-size: 0.72rem;
  white-space: nowrap;
}

.stat-grid,
.card-grid,
.chart-grid,
.two-up {
  display: grid;
  gap: 14px;
}

.stat-grid {
  grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
}

.card-grid,
.two-up {
  grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
}

.chart-grid {
  grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
}

.stat-card,
.callout-card,
.download-card,
.chart-card,
.table-wrap,
.check-item {
  position: relative;
  background: linear-gradient(180deg, rgba(23, 29, 38, 0.88), rgba(11, 15, 19, 0.96));
  border: 1px solid rgba(246, 165, 58, 0.16);
  border-radius: var(--radius-md);
  overflow: hidden;
}

.stat-card,
.callout-card,
.download-card {
  padding: 18px;
}

.chart-card img {
  display: block;
  width: 100%;
  height: auto;
  background: #f7f3ea;
  border-bottom: 1px solid rgba(246, 165, 58, 0.16);
}

.chart-copy {
  padding: 16px 18px 18px;
}

.chart-copy a,
.download-card a,
.table-wrap a,
.prose a {
  color: var(--cyan);
  text-decoration-thickness: 0.08em;
  text-underline-offset: 0.14em;
  overflow-wrap: anywhere;
}

.section-note {
  margin-top: 16px;
  padding: 14px 16px;
  border-radius: 12px;
  border: 1px solid rgba(98, 201, 214, 0.22);
  border-left: 3px solid var(--cyan);
  background: rgba(98, 201, 214, 0.08);
}

.download-summary,
.download-badges {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.download-summary {
  margin-bottom: 16px;
}

.download-badge {
  display: inline-flex;
  align-items: center;
  min-height: 28px;
  padding: 0 10px;
  border-radius: 999px;
  border: 1px solid rgba(246, 165, 58, 0.18);
  background: rgba(246, 165, 58, 0.08);
  color: var(--ink);
  text-transform: uppercase;
  letter-spacing: 0.08em;
  font-size: 0.68rem;
  white-space: nowrap;
}

.download-badge-count {
  border-color: rgba(246, 165, 58, 0.34);
  color: var(--accent);
}

.download-badge-format {
  border-color: rgba(98, 201, 214, 0.28);
  background: rgba(98, 201, 214, 0.08);
  color: var(--cyan);
}

.download-badge-meta {
  border-color: rgba(244, 234, 215, 0.12);
  background: rgba(244, 234, 215, 0.05);
  color: var(--ink-soft);
}

.download-provenance,
.download-hash {
  margin: 12px 0 0;
  color: var(--ink-soft);
  font-size: 0.8rem;
}

.download-hash code {
  font-family: var(--mono);
  overflow-wrap: anywhere;
}

.table-wrap {
  overflow-x: auto;
}

table {
  width: 100%;
  border-collapse: collapse;
  min-width: 640px;
}

th,
td {
  padding: 12px 14px;
  text-align: left;
  border-bottom: 1px solid rgba(246, 165, 58, 0.12);
  vertical-align: top;
}

th {
  font-size: 0.74rem;
  letter-spacing: 0.14em;
  text-transform: uppercase;
  color: var(--accent);
  background: rgba(246, 165, 58, 0.06);
}

tbody tr:nth-child(odd) {
  background: rgba(255, 255, 255, 0.01);
}

tbody tr:hover {
  background: rgba(246, 165, 58, 0.05);
}

tbody tr:last-child td {
  border-bottom: 0;
}

.check-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: grid;
  gap: 10px;
}

.check-item {
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding: 14px 16px;
}

.check-item.check-pass {
  border-left: 4px solid var(--green);
}

.check-item.check-warn {
  border-left: 4px solid var(--warning);
}

.check-item.check-fail {
  border-left: 4px solid var(--red);
}

.prose h1,
.prose h2,
.prose h3 {
  margin-top: 1.4em;
}

.prose h1:first-child,
.prose h2:first-child,
.prose h3:first-child {
  margin-top: 0;
}

.prose ul,
.prose ol {
  padding-left: 1.3rem;
}

.prose pre {
  overflow-x: auto;
  padding: 16px;
  border-radius: 12px;
  background: #05070a;
  color: #f8fafc;
  border: 1px solid rgba(246, 165, 58, 0.18);
}

code {
  font-family: var(--mono);
  font-size: 0.94em;
  background: rgba(246, 165, 58, 0.1);
  padding: 0.12em 0.34em;
  border-radius: 0.34em;
}

pre code {
  padding: 0;
  background: transparent;
}

.site-footer {
  margin: 18px auto 28px;
}

@media (max-width: 1240px) {
  .workstation {
    grid-template-columns: 220px minmax(0, 1fr);
  }

  .monitor-rail {
    position: static;
    grid-column: 1 / -1;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  }
}

@media (max-width: 980px) {
  .workstation {
    grid-template-columns: 1fr;
  }

  .control-rail,
  .monitor-rail {
    position: static;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  }

  .hero-grid {
    grid-template-columns: 1fr;
  }

  .section-card.layout-compact {
    grid-column: 1 / -1;
  }
}

@media (max-width: 680px) {
  .command-strip-inner,
  .workspace-command,
  .hero-head,
  .section-head {
    flex-direction: column;
    align-items: flex-start;
  }

  .site-shell {
    width: min(100vw - 18px, 1480px);
  }

  .hero,
  .section-card,
  .site-footer,
  .rail-module,
  .workspace-command {
    padding: 16px;
  }

  .ticker-strip,
  .command-bar-links,
  .rail-command-links,
  .command-entry {
    width: 100%;
  }

  .nav-link,
  .rail-command-link,
  .command-chip {
    width: 100%;
  }

  .command-input-wrap {
    flex-direction: column;
    align-items: flex-start;
  }

  h1 {
    max-width: none;
  }
}
"""


def build_script() -> str:
    return """\
(() => {
  const surface = document.querySelector("[data-command-surface]");
  if (!surface) {
    return;
  }

  const input = surface.querySelector("[data-command-input]");
  const status = surface.querySelector("[data-command-status]");
  if (!(input instanceof HTMLInputElement) || !status) {
    return;
  }

  const route = surface.getAttribute("data-page-route") || window.location.pathname || "/";
  const storageKey = `fancymmr:last-panel:${route}`;
  const commandNodes = Array.from(document.querySelectorAll("[data-command-target]"));
  const commandMap = new Map();

  for (const node of commandNodes) {
    const target = node.getAttribute("data-command-target") || "";
    const label = node.getAttribute("data-command-label") || node.textContent || target;
    const kind = node.getAttribute("data-command-kind") || (target.startsWith("#") ? "panel" : "route");
    const queryValue = (node.getAttribute("data-command-query") || target).trim();
    const extraTerms = node.getAttribute("data-command-terms") || "";
    if (!target) {
      continue;
    }
    const key = `${kind}::${target}`;
    if (!commandMap.has(key)) {
      const panel = kind === "panel" && target.startsWith("#") ? document.querySelector(target) : null;
      const titleNode = panel instanceof HTMLElement ? panel.querySelector("h1, h2, h3") : null;
      const title = (titleNode && titleNode.textContent ? titleNode.textContent : label).trim();
      commandMap.set(key, {
        key,
        kind,
        target,
        label: label.trim(),
        queryValue,
        title,
        panel,
        elements: [],
        terms: `${label} ${queryValue} ${target} ${title} ${extraTerms} ${kind}`.toLowerCase(),
      });
    }
    commandMap.get(key).elements.push(node);
  }

  const commands = Array.from(commandMap.values());
  const panels = commands.filter((command) => command.kind === "panel");
  const panelMap = new Map(panels.map((panel) => [panel.target, panel]));
  const panelCount = panels.length;
  const globalCommandCount = commands.length - panelCount;
  let lastMatches = panels.length ? panels : commands;

  const isEditableTarget = (eventTarget) => {
    if (!(eventTarget instanceof HTMLElement)) {
      return false;
    }
    return (
      eventTarget === input ||
      eventTarget.tagName === "INPUT" ||
      eventTarget.tagName === "TEXTAREA" ||
      eventTarget.tagName === "SELECT" ||
      eventTarget.isContentEditable
    );
  };

  const readyMessage = () => {
    const panelLabel = `${panelCount} local panel${panelCount === 1 ? "" : "s"}`;
    const globalLabel = `${globalCommandCount} global command${globalCommandCount === 1 ? "" : "s"}`;
    return `Ready. ${panelLabel} and ${globalLabel} indexed.`;
  };

  const currentTarget = () => {
    const hash = window.location.hash || "#top";
    return panelMap.has(hash) ? hash : "#top";
  };

  const setStatus = (message) => {
    status.textContent = message;
  };

  const normalizedCommandQuery = (rawValue) => {
    const raw = rawValue.trim().toLowerCase();
    if (!raw) {
      return "";
    }
    const normalizedRoute = route.trim().toLowerCase();
    if (raw === normalizedRoute) {
      return "";
    }
    if (raw.startsWith(`${normalizedRoute} `)) {
      return raw.slice(normalizedRoute.length).trim();
    }
    return raw;
  };

  const syncInputValue = (target) => {
    if (document.activeElement === input) {
      return;
    }
    input.value = target === "#top" ? route : `${route} ${target}`;
  };

  const highlightPanel = (target) => {
    for (const panel of panels) {
      const isTarget = panel.target === target;
      for (const element of panel.elements) {
        element.classList.toggle("is-targeted", isTarget);
      }
      if (panel.panel) {
        panel.panel.classList.toggle("is-focused", isTarget);
      }
    }
    syncInputValue(target);
    const activePanel = panelMap.get(target);
    if (activePanel) {
      window.localStorage.setItem(storageKey, target);
      const index = panels.findIndex((panel) => panel.target === target);
      setStatus(`Focused ${activePanel.label}. Panel ${index + 1} of ${panelCount}.`);
    } else {
      setStatus(readyMessage());
    }
  };

  const matchedCommands = (query) => {
    const normalized = normalizedCommandQuery(query);
    if (!normalized) {
      return panels.length ? panels : commands;
    }
    return commands.filter((command) => command.terms.includes(normalized));
  };

  const renderMatches = (matches, query) => {
    lastMatches = matches.length ? matches : (panels.length ? panels : commands);
    const normalized = normalizedCommandQuery(query);
    if (!normalized) {
      for (const command of commands) {
        for (const element of command.elements) {
          element.classList.remove("is-muted");
        }
      }
      highlightPanel(currentTarget());
      return;
    }

    for (const command of commands) {
      const isMatch = matches.includes(command);
      for (const element of command.elements) {
        element.classList.toggle("is-muted", !isMatch);
      }
    }

    if (matches.length) {
      setStatus(`${matches.length} command match${matches.length === 1 ? "" : "es"} for "${normalized}". Press Enter to jump.`);
    } else {
      setStatus(`No command matches for "${normalized}".`);
    }
  };

  const navigateTo = (command) => {
    if (!command) {
      return;
    }
    if (command.kind === "panel") {
      if (window.location.hash === command.target) {
        highlightPanel(command.target);
        command.panel?.scrollIntoView({ behavior: "smooth", block: "start" });
        return;
      }
      window.location.hash = command.target;
      return;
    }

    const absoluteUrl = new URL(command.target, window.location.href);
    if (
      absoluteUrl.origin === window.location.origin
      && absoluteUrl.pathname === window.location.pathname
      && absoluteUrl.hash
      && panelMap.has(absoluteUrl.hash)
    ) {
      if (window.location.hash === absoluteUrl.hash) {
        highlightPanel(absoluteUrl.hash);
        panelMap.get(absoluteUrl.hash)?.panel?.scrollIntoView({ behavior: "smooth", block: "start" });
      } else {
        window.location.hash = absoluteUrl.hash;
      }
      return;
    }

    window.location.href = absoluteUrl.toString();
  };

  const cyclePanels = (direction) => {
    if (!panelCount) {
      return;
    }
    const activeTarget = currentTarget();
    const currentIndex = Math.max(
      0,
      panels.findIndex((panel) => panel.target === activeTarget),
    );
    const nextIndex = (currentIndex + direction + panelCount) % panelCount;
    navigateTo(panels[nextIndex]);
  };

  input.addEventListener("input", () => {
    renderMatches(matchedCommands(input.value), input.value);
  });

  input.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      navigateTo(lastMatches[0] || panelMap.get(currentTarget()) || commands[0]);
      return;
    }
    if (event.key === "Escape") {
      input.blur();
      input.value = route;
      renderMatches(panels.length ? panels : commands, "");
    }
  });

  window.addEventListener("hashchange", () => {
    highlightPanel(currentTarget());
    renderMatches(matchedCommands(input.value), input.value);
  });

  document.addEventListener("keydown", (event) => {
    if ((event.key === "/" && !event.metaKey && !event.ctrlKey && !event.altKey) || ((event.key === "k" || event.key === "K") && event.ctrlKey)) {
      if (isEditableTarget(event.target)) {
        return;
      }
      event.preventDefault();
      input.focus();
      input.select();
      return;
    }

    if ((event.key === "[" || event.key === "]") && !isEditableTarget(event.target)) {
      event.preventDefault();
      cyclePanels(event.key === "]" ? 1 : -1);
    }
  });

  const initialTarget = currentTarget();
  const storedTarget = window.localStorage.getItem(storageKey);
  syncInputValue(initialTarget);
  if (!window.location.hash && storedTarget && panelMap.has(storedTarget)) {
    setStatus(`${readyMessage()} Last panel: ${panelMap.get(storedTarget).label}.`);
  } else {
    setStatus(readyMessage());
  }
  highlightPanel(initialTarget);
})();
"""


def write_site_pages() -> None:
    metrics = read_json(DATA_DIR / "metrics.json")
    publication_input = read_json(DATA_DIR / "publication_input.json")
    validation_report = read_json(DATA_DIR / "validation_report.json")
    source_coverage_report = read_json(DATA_DIR / "source_coverage_report.json")
    source_pipeline_diagnostics = read_json(DATA_DIR / "source_pipeline_diagnostics.json")
    pipeline_manifest = read_json(DATA_DIR / "pipeline_manifest.json")
    output_registry_sections = build_output_registry_sections(
        source_pipeline_diagnostics,
        pipeline_manifest,
    )
    category_rows = read_csv_rows(DATA_DIR / "category_summary.csv")
    revenue_band_rows = read_csv_rows(DATA_DIR / "revenue_band_summary.csv")
    methodology_markdown = (DOCS_DIR / "methodology.md").read_text(encoding="utf-8")
    data_notice_markdown = (ROOT / "DATA-NOTICE.md").read_text(encoding="utf-8")

    pages = {
        "index.html": build_index_page(metrics, validation_report, category_rows, output_registry_sections),
        "methodology.html": build_methodology_page(
            methodology_markdown,
            data_notice_markdown,
            validation_report,
            output_registry_sections,
        ),
        "data.html": build_data_page(
            metrics,
            publication_input,
            validation_report,
            source_coverage_report,
            source_pipeline_diagnostics,
            pipeline_manifest,
            category_rows,
            revenue_band_rows,
            output_registry_sections,
        ),
    }
    for filename, content in pages.items():
        (SITE_DIR / filename).write_text(content, encoding="utf-8")
    (SITE_ASSETS_DIR / "site.css").write_text(build_stylesheet(), encoding="utf-8")
    (SITE_ASSETS_DIR / "site.js").write_text(build_script(), encoding="utf-8")


def build_site() -> None:
    source_pipeline_diagnostics = read_json(DATA_DIR / "source_pipeline_diagnostics.json")
    pipeline_manifest = read_json(DATA_DIR / "pipeline_manifest.json")
    all_download_items = publication_download_items(source_pipeline_diagnostics, pipeline_manifest)
    clean_site_dir()
    copy_assets(publication_download_items=all_download_items)
    write_site_pages()
