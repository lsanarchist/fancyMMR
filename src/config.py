from __future__ import annotations

import csv
from dataclasses import dataclass
import json
from pathlib import Path
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
PUBLIC_SOURCE_PAGES_CSV = DATA_DIR / "public_source_pages.csv"
SOURCE_COVERAGE_REPORT_JSON = DATA_DIR / "source_coverage_report.json"


@dataclass(frozen=True)
class FetchPaths:
    root: Path
    data_dir: Path
    cache_dir: Path
    failure_snapshot_dir: Path


@dataclass(frozen=True)
class FetchPolicy:
    http_user_agent: str
    robots_user_agent: str
    timeout_seconds: float
    min_delay_seconds: float
    max_retries: int
    retry_backoff_seconds: float
    cache_ttl_seconds: int
    respect_robots_txt: bool


@dataclass(frozen=True)
class SourceConfig:
    source_id: str
    url: str
    parser_strategy: str
    category_slug: str
    category_label: str
    source_group: str


DEFAULT_FETCH_PATHS = FetchPaths(
    root=ROOT,
    data_dir=DATA_DIR,
    cache_dir=DATA_DIR / "fetch_cache",
    failure_snapshot_dir=DATA_DIR / "fetch_failures",
)
DEFAULT_FETCH_POLICY = FetchPolicy(
    http_user_agent="fancyMMRResearchBot/0.1 (+public-visible-sample build)",
    robots_user_agent="fancymmr-research-bot",
    timeout_seconds=20.0,
    min_delay_seconds=2.0,
    max_retries=2,
    retry_backoff_seconds=2.0,
    cache_ttl_seconds=86_400,
    respect_robots_txt=True,
)


def slugify_source_url(url: str) -> str:
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        return parsed.netloc.replace(".", "-")
    return "--".join(parts)


def infer_parser_strategy(url: str) -> str:
    if "/special-category/" in url:
        return "trustmrr_special_category_listing"
    if "/category/" in url:
        return "trustmrr_category_listing"
    return "trustmrr_public_html_page"


def infer_source_group(url: str) -> str:
    if "/special-category/" in url:
        return "special-category"
    if "/category/" in url:
        return "category"
    return "page"


def infer_category_slug(url: str) -> str:
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    return parts[-1] if parts else parsed.netloc.replace(".", "-")


def fallback_label_from_slug(slug: str) -> str:
    return slug.replace("-", " ").replace("_", " ").title()


def load_source_label_map(source_coverage_report_json: Path | None = None) -> dict[str, str]:
    coverage_report_path = SOURCE_COVERAGE_REPORT_JSON if source_coverage_report_json is None else source_coverage_report_json
    if not coverage_report_path.exists():
        return {}
    coverage_report = json.loads(coverage_report_path.read_text(encoding="utf-8"))
    label_map: dict[str, str] = {}
    for row in coverage_report.get("source_pages", []):
        categories = row.get("categories") or []
        if categories:
            label_map[row["source_url"]] = " / ".join(categories)
    return label_map


def load_source_registry(
    *,
    public_source_pages_csv: Path | None = None,
    source_coverage_report_json: Path | None = None,
) -> list[SourceConfig]:
    source_pages_path = PUBLIC_SOURCE_PAGES_CSV if public_source_pages_csv is None else public_source_pages_csv
    label_map = load_source_label_map(source_coverage_report_json)
    with source_pages_path.open(encoding="utf-8", newline="") as handle:
        urls = sorted({row["source_url"].strip() for row in csv.DictReader(handle) if row.get("source_url")})

    registry = [
        SourceConfig(
            source_id=slugify_source_url(url),
            url=url,
            parser_strategy=infer_parser_strategy(url),
            category_slug=infer_category_slug(url),
            category_label=label_map.get(url, fallback_label_from_slug(infer_category_slug(url))),
            source_group=infer_source_group(url),
        )
        for url in urls
    ]
    return registry
