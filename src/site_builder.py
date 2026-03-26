from __future__ import annotations

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


def clean_site_dir() -> None:
    if SITE_DIR.exists():
        shutil.rmtree(SITE_DIR)
    SITE_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)


def staged_detail_download_items(
    source_pipeline_diagnostics: dict[str, object],
    pipeline_manifest: dict[str, object],
) -> list[dict[str, str]]:
    manifest_artifacts = pipeline_manifest.get("source_pipeline_diagnostics", {}).get(
        "downloadable_staged_artifacts", []
    )
    diagnostics_artifacts = source_pipeline_diagnostics.get("downloadable_staged_artifacts", [])
    selected_artifacts = manifest_artifacts or diagnostics_artifacts
    items: list[dict[str, str]] = []
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
        items.append(
            {
                "path": path,
                "site_path": site_path,
                "label": label,
                "description": description,
            }
        )
    return items


def copy_assets(
    *,
    staged_download_items: list[dict[str, str]],
) -> None:
    for stem, _, _ in CHART_STEMS:
        for suffix in (".png", ".svg"):
            shutil.copy2(CHARTS_DIR / f"{stem}{suffix}", SITE_CHARTS_DIR / f"{stem}{suffix}")
    for name in JSON_EXPORTS:
        shutil.copy2(DATA_DIR / name, SITE_DATA_DIR / name)
    for artifact in staged_download_items:
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


def page_shell(*, title: str, active: str, description: str, body_html: str) -> str:
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
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)} | TrustMRR visible sample</title>
  <meta name="description" content="{html.escape(description, quote=True)}">
  <link rel="stylesheet" href="assets/site.css">
</head>
<body class="page-{active}">
  <div class="page-backdrop"></div>
  <header class="topbar">
    <div class="site-shell topbar-inner">
      <a class="brand" href="index.html">
        <span class="brand-kicker">fancyMMR</span>
        <span class="brand-title">TrustMRR visible-sample research</span>
      </a>
      <nav class="nav-links" aria-label="Primary">
        {navigation}
      </nav>
    </div>
  </header>
  <main class="site-shell">
    {body_html}
  </main>
  <footer class="site-shell site-footer">
    <p>This static site is a source-derived visible sample research artifact. It is not a full platform export and is not affiliated with TrustMRR.</p>
    <p>Build inputs live in <code>data/</code>, <code>charts/</code>, <code>docs/</code>, and <code>DATA-NOTICE.md</code>; regenerate the site with <code>python src/build_site.py</code>.</p>
  </footer>
</body>
</html>
"""


def hero_section(*, eyebrow: str, title: str, lede: str, status: str, aside_html: str) -> str:
    return f"""
<section class="hero">
  <div class="hero-copy">
    <p class="eyebrow">{html.escape(eyebrow)}</p>
    <h1>{html.escape(title)}</h1>
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


def section(title: str, intro: str, body_html: str) -> str:
    return f"""
<section class="section-card">
  <div class="section-head">
    <h2>{html.escape(title)}</h2>
    <p>{html.escape(intro)}</p>
  </div>
  {body_html}
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
) -> str:
    hero = hero_section(
        eyebrow="Static GitHub Pages bundle",
        title="Visible startup revenue research, published as a fully static site",
        lede="This site packages the current processed TrustMRR visible sample into a Pages-friendly publication flow: summary metrics, charts, methodology, and machine-readable provenance live together with no runtime server.",
        status=str(validation_report["status"]),
        aside_html=f"""
<div class="hero-aside">
  <p class="eyebrow">Current build snapshot</p>
  <p class="hero-aside-value">{int(metrics["sample_size"])} startups</p>
  <p class="hero-aside-note">30 public source pages, deterministic charts, and validation/manifests copied into the published site output.</p>
</div>
""",
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
        ),
        section(
            "Main charts",
            "The published charts are copied into the site so GitHub Pages can serve the bundle directly.",
            f'<div class="chart-grid">{chart_cards}</div>',
        ),
        section(
            "Category leaders",
            "The current sample is concentrated in a small number of categories, with E-commerce far ahead of the rest.",
            top_categories,
        ),
    ]

    body = hero + "".join(sections)
    return page_shell(
        title="Overview",
        active="index",
        description="Static overview of the TrustMRR visible-sample research bundle.",
        body_html=body,
    )


def build_methodology_page(
    methodology_markdown: str,
    data_notice_markdown: str,
    validation_report: dict[str, object],
) -> str:
    hero = hero_section(
        eyebrow="Methodology and caveats",
        title="How the visible sample is defined, validated, and limited",
        lede="The methodology page keeps the inclusion rule, heuristic-field caveats, and publication limitations visible in the site itself so the static bundle does not outgrow its evidence.",
        status=str(validation_report["status"]),
        aside_html=f"""
<div class="hero-aside">
  <p class="eyebrow">Warning-only signals</p>
  <p class="hero-aside-value">{html.escape(methodology_warning_snapshot(validation_report))}</p>
  <p class="hero-aside-note">Warning-only checks currently cover duplicate startup names and heuristic label gaps, while missing provenance, threshold violations, and duplicate (name, source_url) pairs still fail the build.</p>
</div>
""",
    )

    methodology_html = markdown_to_html(methodology_markdown)
    data_notice_html = markdown_to_html(data_notice_markdown)

    sections = [
        section(
            "Methodology",
            "Rendered directly from the repository methodology document to keep the site aligned with the source docs.",
            f'<div class="prose">{methodology_html}</div>',
        ),
        section(
            "Data notice",
            "Publication caveats from the repository root stay visible in the static site too.",
            f'<div class="prose">{data_notice_html}</div>',
        ),
        section(
            "Validation checks",
            "The seed bundle keeps warnings and failures explicit instead of silently smoothing them away.",
            render_checks(validation_report),
        ),
    ]

    body = hero + "".join(sections)
    return page_shell(
        title="Methodology",
        active="methodology",
        description="Methodology and data caveats for the TrustMRR visible-sample static site.",
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
) -> str:
    hero = hero_section(
        eyebrow="Data and provenance",
        title="Machine-readable outputs, source coverage, and manifest details",
        lede="The site ships the current JSON outputs alongside a human-readable summary so the published artifact stays inspectable without cloning the repository.",
        status=str(validation_report["status"]),
        aside_html=f"""
<div class="hero-aside">
  <p class="eyebrow">Manifest summary</p>
  <p class="hero-aside-value">{len(pipeline_manifest['generated_outputs'])} generated outputs</p>
  <p class="hero-aside-note">{source_coverage_report['source_page_count']} source pages and {pipeline_manifest['input_dataset']['rows']} visible startups feed the current site build.</p>
</div>
""",
    )

    download_items = [
        ("data/metrics.json", "Top-line metrics", "Sample size, revenue concentration, and dominant-category snapshots."),
        (
            "data/publication_input.json",
            "Publication input manifest",
            "The active published dataset path plus any live-source promotion provenance.",
        ),
        ("data/validation_report.json", "Validation report", "Required-column, threshold, duplicate, and warning-level label checks."),
        ("data/source_coverage_report.json", "Source coverage report", "Per-source-page startup counts, revenue shares, and category coverage."),
        (
            "data/source_pipeline_diagnostics.json",
            "Source-pipeline diagnostics",
            "Promotion provenance, override coverage, duplicate review, per-source parser output counts, and shared detail-field coverage.",
        ),
        ("data/pipeline_manifest.json", "Pipeline manifest", "Build command, input dataset hash, and copied-output inventory."),
    ]
    download_items.extend(
        (
            artifact["site_path"],
            artifact["label"],
            artifact["description"],
        )
        for artifact in staged_detail_download_items(source_pipeline_diagnostics, pipeline_manifest)
    )

    downloads = "".join(
        f"""
<article class="download-card">
  <h3>{html.escape(name)}</h3>
  <p>{html.escape(description)}</p>
  <a href="{html.escape(filename, quote=True)}">Download</a>
</article>
"""
        for filename, name, description in download_items
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
            + diagnostics_coverage_section
            + (
                f'<p class="section-note">Staged validation: <strong>{html.escape(status_label(str(source_pipeline_diagnostics["validation_status"])))}</strong>. '
                f'Run-manifest validation: <strong>{html.escape(status_label(str(source_pipeline_diagnostics["run_manifest_validation_status"])))}</strong>. '
                f'Suspicious duplicate groups: <strong>{html.escape(str(source_pipeline_diagnostics["suspicious_duplicate_group_count"]))}</strong>. '
                f'Detail parse failures: <strong>{html.escape(str(source_pipeline_diagnostics["failed_detail_page_count"]))}</strong> across '
                f'<strong>{html.escape(str(source_pipeline_diagnostics["detail_parse_failure_source_count"]))}</strong> source pages.</p>'
            ),
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
            "Core JSON outputs and selected staged provenance artifacts are copied into the site so the published Pages bundle stays inspectable on its own.",
            f'<div class="card-grid">{downloads}</div>',
        ),
        section(
            "Top categories",
            "Category revenue concentration remains the clearest summary of the current sample shape.",
            category_table,
        ),
        section(
            "Revenue bands",
            "Most visible startups sit below $50k in 30-day revenue even though the total revenue is dominated by the top tail.",
            band_table,
        ),
        section(
            "Source coverage",
            "The source coverage report keeps the public-page footprint explicit, with links back to the source pages.",
            source_table,
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
        ),
    ]

    body = hero + "".join(sections)
    return page_shell(
        title="Data",
        active="data",
        description="Download links and source coverage for the TrustMRR visible-sample static site.",
        body_html=body,
    )


def build_stylesheet() -> str:
    return """\
:root {
  --paper: #f5efe3;
  --paper-strong: #fffaf0;
  --ink: #172033;
  --muted: #5b6578;
  --line: rgba(23, 32, 51, 0.12);
  --blue: #1d4ed8;
  --blue-soft: rgba(29, 78, 216, 0.12);
  --amber: #b45309;
  --amber-soft: rgba(180, 83, 9, 0.14);
  --green: #166534;
  --green-soft: rgba(22, 101, 52, 0.14);
  --red: #b91c1c;
  --red-soft: rgba(185, 28, 28, 0.14);
  --shadow: 0 18px 40px rgba(23, 32, 51, 0.09);
  --radius-lg: 28px;
  --radius-md: 18px;
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
    radial-gradient(circle at top left, rgba(29, 78, 216, 0.18), transparent 32%),
    radial-gradient(circle at top right, rgba(180, 83, 9, 0.14), transparent 26%),
    linear-gradient(180deg, #f8f3e8 0%, #f2eadc 100%);
  font-family: "Avenir Next", "Segoe UI Variable", "Segoe UI", sans-serif;
  line-height: 1.6;
}

.page-backdrop {
  position: fixed;
  inset: 0;
  pointer-events: none;
  background-image:
    linear-gradient(rgba(23, 32, 51, 0.03) 1px, transparent 1px),
    linear-gradient(90deg, rgba(23, 32, 51, 0.03) 1px, transparent 1px);
  background-size: 24px 24px;
  mask-image: linear-gradient(180deg, rgba(0, 0, 0, 0.4), transparent 90%);
}

.site-shell {
  width: min(1180px, calc(100vw - 32px));
  margin: 0 auto;
}

.topbar {
  position: sticky;
  top: 0;
  z-index: 10;
  backdrop-filter: blur(18px);
  background: rgba(245, 239, 227, 0.82);
  border-bottom: 1px solid rgba(23, 32, 51, 0.08);
}

.topbar-inner {
  display: flex;
  gap: 18px;
  align-items: center;
  justify-content: space-between;
  padding: 18px 0;
}

.brand {
  display: inline-flex;
  flex-direction: column;
  gap: 2px;
  color: inherit;
  text-decoration: none;
}

.brand-kicker,
.eyebrow,
.stat-label {
  letter-spacing: 0.12em;
  text-transform: uppercase;
  font-size: 0.74rem;
  color: var(--amber);
  font-weight: 700;
}

.brand-title {
  font-size: 0.98rem;
  font-weight: 700;
}

.nav-links {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}

.nav-link {
  padding: 10px 14px;
  border-radius: 999px;
  color: var(--muted);
  text-decoration: none;
  border: 1px solid transparent;
  transition: 180ms ease;
}

.nav-link:hover,
.nav-link.is-active {
  color: var(--ink);
  background: rgba(255, 255, 255, 0.55);
  border-color: rgba(23, 32, 51, 0.08);
}

.hero {
  display: grid;
  grid-template-columns: minmax(0, 1.7fr) minmax(300px, 0.9fr);
  gap: 22px;
  padding: 42px 0 26px;
}

.hero-copy,
.hero-panel,
.section-card,
.site-footer {
  background: rgba(255, 250, 240, 0.82);
  border: 1px solid var(--line);
  box-shadow: var(--shadow);
  border-radius: var(--radius-lg);
}

.hero-copy {
  padding: 36px;
}

.hero-panel {
  padding: 28px;
  align-self: stretch;
}

h1,
h2,
h3 {
  margin: 0 0 12px;
  line-height: 1.08;
  font-family: "Georgia", "Iowan Old Style", "Palatino Linotype", serif;
}

h1 {
  font-size: clamp(2.4rem, 4vw, 4.8rem);
  max-width: 12ch;
}

h2 {
  font-size: clamp(1.6rem, 2vw, 2.2rem);
}

h3 {
  font-size: 1.15rem;
}

.hero-lede,
.section-head p,
.site-footer p,
.chart-copy p,
.callout-card p,
.download-card p,
.hero-aside-note,
.stat-note,
.prose p,
.prose li {
  color: var(--muted);
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
  color: white;
  background: linear-gradient(135deg, #1d4ed8 0%, #1e40af 100%);
  box-shadow: 0 10px 18px rgba(29, 78, 216, 0.18);
}

.button.button-secondary {
  color: var(--ink);
  background: rgba(255, 255, 255, 0.6);
  box-shadow: none;
  border: 1px solid rgba(23, 32, 51, 0.08);
}

.status-pill {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  min-height: 36px;
  padding: 0 14px;
  border-radius: 999px;
  font-size: 0.92rem;
  font-weight: 700;
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
  font-size: clamp(1.8rem, 2vw, 2.8rem);
  font-weight: 800;
}

.section-card {
  padding: 28px;
  margin-bottom: 24px;
}

.section-head {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin-bottom: 20px;
}

.stat-grid,
.card-grid,
.chart-grid,
.two-up {
  display: grid;
  gap: 16px;
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
.chart-card {
  background: rgba(255, 255, 255, 0.68);
  border: 1px solid rgba(23, 32, 51, 0.08);
  border-radius: var(--radius-md);
  overflow: hidden;
}

.stat-card,
.callout-card,
.download-card {
  padding: 20px;
}

.chart-card img {
  display: block;
  width: 100%;
  height: auto;
  background: white;
}

.chart-copy {
  padding: 18px 18px 20px;
}

.chart-copy a,
.download-card a,
.table-wrap a,
.prose a {
  color: var(--blue);
  text-decoration-thickness: 0.08em;
  text-underline-offset: 0.14em;
}

.section-note {
  margin-top: 16px;
  padding: 14px 16px;
  border-radius: 16px;
  background: rgba(29, 78, 216, 0.08);
}

.table-wrap {
  overflow-x: auto;
  border-radius: var(--radius-md);
  border: 1px solid rgba(23, 32, 51, 0.08);
  background: rgba(255, 255, 255, 0.78);
}

table {
  width: 100%;
  border-collapse: collapse;
  min-width: 640px;
}

th,
td {
  padding: 14px 16px;
  text-align: left;
  border-bottom: 1px solid rgba(23, 32, 51, 0.08);
}

th {
  font-size: 0.82rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--muted);
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
  border-radius: 16px;
  border: 1px solid rgba(23, 32, 51, 0.08);
  background: rgba(255, 255, 255, 0.78);
}

.check-item span {
  color: var(--muted);
  font-size: 0.92rem;
}

.check-item.check-pass {
  border-left: 6px solid var(--green);
}

.check-item.check-warn {
  border-left: 6px solid var(--amber);
}

.check-item.check-fail {
  border-left: 6px solid var(--red);
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
  border-radius: 16px;
  background: #111827;
  color: #f8fafc;
}

.prose code {
  font-family: "IBM Plex Mono", "SFMono-Regular", Consolas, monospace;
}

.site-footer {
  padding: 20px 22px 26px;
  margin: 0 0 28px;
}

@media (max-width: 880px) {
  .hero {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 680px) {
  .topbar-inner {
    flex-direction: column;
    align-items: flex-start;
  }

  .site-shell {
    width: min(100vw - 20px, 1180px);
  }

  .hero-copy,
  .hero-panel,
  .section-card,
  .site-footer {
    padding: 22px;
    border-radius: 22px;
  }

  h1 {
    max-width: none;
  }
}
"""


def write_site_pages() -> None:
    metrics = read_json(DATA_DIR / "metrics.json")
    publication_input = read_json(DATA_DIR / "publication_input.json")
    validation_report = read_json(DATA_DIR / "validation_report.json")
    source_coverage_report = read_json(DATA_DIR / "source_coverage_report.json")
    source_pipeline_diagnostics = read_json(DATA_DIR / "source_pipeline_diagnostics.json")
    pipeline_manifest = read_json(DATA_DIR / "pipeline_manifest.json")
    category_rows = read_csv_rows(DATA_DIR / "category_summary.csv")
    revenue_band_rows = read_csv_rows(DATA_DIR / "revenue_band_summary.csv")
    methodology_markdown = (DOCS_DIR / "methodology.md").read_text(encoding="utf-8")
    data_notice_markdown = (ROOT / "DATA-NOTICE.md").read_text(encoding="utf-8")

    pages = {
        "index.html": build_index_page(metrics, validation_report, category_rows),
        "methodology.html": build_methodology_page(methodology_markdown, data_notice_markdown, validation_report),
        "data.html": build_data_page(
            metrics,
            publication_input,
            validation_report,
            source_coverage_report,
            source_pipeline_diagnostics,
            pipeline_manifest,
            category_rows,
            revenue_band_rows,
        ),
    }
    for filename, content in pages.items():
        (SITE_DIR / filename).write_text(content, encoding="utf-8")
    (SITE_ASSETS_DIR / "site.css").write_text(build_stylesheet(), encoding="utf-8")


def build_site() -> None:
    source_pipeline_diagnostics = read_json(DATA_DIR / "source_pipeline_diagnostics.json")
    pipeline_manifest = read_json(DATA_DIR / "pipeline_manifest.json")
    staged_download_items = staged_detail_download_items(source_pipeline_diagnostics, pipeline_manifest)
    clean_site_dir()
    copy_assets(staged_download_items=staged_download_items)
    write_site_pages()
