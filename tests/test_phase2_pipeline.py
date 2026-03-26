from __future__ import annotations

import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from src.build_all import PipelinePaths, run_pipeline
from src.config import FetchPaths, SourceConfig
from src.fetch import FetchResult
from src.normalize import (
    build_override_key,
    build_visible_sample_rows,
    normalize_parsed_cards,
    parse_money_value,
    slugify_startup_name,
)
from src.parse import ParsedStartupCard, parse_source_html
from src.validate import build_suspicious_duplicates_report, validate_normalized_rows


FIXTURES = ROOT / "tests" / "fixtures"


def read_fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def category_source() -> SourceConfig:
    return SourceConfig(
        source_id="category--ai",
        url="https://trustmrr.com/category/ai",
        parser_strategy="trustmrr_category_listing",
        category_slug="ai",
        category_label="Artificial Intelligence",
        source_group="category",
    )


def special_source() -> SourceConfig:
    return SourceConfig(
        source_id="special-category--openclaw",
        url="https://trustmrr.com/special-category/openclaw",
        parser_strategy="trustmrr_special_category_listing",
        category_slug="openclaw",
        category_label="OpenClaw (special)",
        source_group="special-category",
    )


def iot_source() -> SourceConfig:
    return SourceConfig(
        source_id="category--iot-hardware",
        url="https://trustmrr.com/category/iot-hardware",
        parser_strategy="trustmrr_category_listing",
        category_slug="iot-hardware",
        category_label="IoT & Hardware",
        source_group="category",
    )


def test_parse_source_html_extracts_category_cards_from_fixture() -> None:
    cards = parse_source_html(category_source(), read_fixture("category_ai_fixture.html"))

    assert len(cards) == 2
    assert cards[0].name == "Rezi"
    assert cards[0].badge == "FOR SALE"
    assert cards[0].revenue_30d_text == "$273k"
    assert cards[0].detail_url == "https://trustmrr.com/startup/rezi"
    assert cards[1].name == "Think again"
    assert cards[1].revenue_30d_text == "$0"


def test_parse_source_html_extracts_special_category_cards_from_fixture() -> None:
    cards = parse_source_html(special_source(), read_fixture("special_openclaw_fixture.html"))

    assert len(cards) == 1
    assert cards[0].name == "Claw Mart"
    assert cards[0].category_label == "OpenClaw (special)"
    assert cards[0].mrr_text == "$4.8k"


def test_normalize_parsed_cards_converts_metrics_and_thresholds() -> None:
    cards = parse_source_html(category_source(), read_fixture("category_ai_fixture.html"))
    rows = normalize_parsed_cards(cards, scraped_at="2026-03-26T19:00:00Z")
    visible_rows = build_visible_sample_rows(rows)

    assert parse_money_value("$8.7M") == 8_700_000
    assert slugify_startup_name("Vid.AI") == "vid-ai"
    assert rows[0]["revenue_30d"] == 273_000
    assert rows[0]["canonical_slug"] == "rezi"
    assert rows[0]["revenue_band"] == "$100k–$500k"
    assert rows[0]["biz_model"] == "Software / SaaS"
    assert rows[0]["gtm_model"] == "PLG / inbound software"
    assert rows[0]["heuristic_override_key"] == build_override_key(category_source().url, "rezi")
    assert rows[0]["heuristic_override_source"] == "tracked_override"
    assert rows[0]["included_in_visible_sample"] is True
    assert rows[1]["revenue_30d"] == 0
    assert rows[1]["revenue_band"] is None
    assert rows[1]["included_in_visible_sample"] is False
    assert len(visible_rows) == 1
    assert visible_rows[0]["name"] == "Rezi"


def test_normalize_parsed_cards_applies_alias_backed_override() -> None:
    cards = [
        ParsedStartupCard(
            source_id="category--ai",
            source_url="https://trustmrr.com/category/ai",
            parser_strategy="trustmrr_category_listing",
            source_group="category",
            category_label="AI",
            position=1,
            detail_path="/startup/parakeet-chat",
            detail_url="https://trustmrr.com/startup/parakeet-chat",
            name="Parakeet Chat",
            description="SMS, Chat and AI for inmates.",
            revenue_30d_text="$27k",
            mrr_text="$31k",
            total_revenue_text="$1.5M",
            badge="FOR SALE",
        )
    ]

    row = normalize_parsed_cards(cards, scraped_at="2026-03-26T19:00:00Z")[0]

    assert row["canonical_slug"] == "parakeet"
    assert row["canonical_slug_source"] == "alias"
    assert row["biz_model"] == "Software / SaaS"
    assert row["gtm_model"] == "PLG / inbound software"


def test_normalize_parsed_cards_applies_manual_override_for_roofclaw() -> None:
    cards = parse_source_html(iot_source(), read_fixture("category_iot_hardware_fixture.html"))

    row = normalize_parsed_cards(cards, scraped_at="2026-03-26T19:00:00Z")[0]

    assert row["canonical_slug"] == "roofclaw"
    assert row["canonical_slug_source"] == "name_slug"
    assert row["biz_model"] == "Services / agency / lead-gen"
    assert row["gtm_model"] == "Sales-led / outbound / SEO"
    assert row["heuristic_override_key"] == build_override_key(iot_source().url, "roofclaw")


def test_normalize_parsed_cards_applies_direct_override_key() -> None:
    cards = [
        ParsedStartupCard(
            source_id="category--ai",
            source_url="https://trustmrr.com/category/ai",
            parser_strategy="trustmrr_category_listing",
            source_group="category",
            category_label="AI",
            position=1,
            detail_path="/startup/ai-interview-copilot",
            detail_url="https://trustmrr.com/startup/ai-interview-copilot",
            name="AI Interview Copilot",
            description="Real time interview insights for job seekers.",
            revenue_30d_text="$38k",
            mrr_text="$40k",
            total_revenue_text="$823k",
            badge="FOR SALE",
        )
    ]

    row = normalize_parsed_cards(cards, scraped_at="2026-03-26T19:00:00Z")[0]

    assert row["canonical_slug"] == "ai-interview-copilot"
    assert row["biz_model"] == "Software / SaaS"
    assert row["gtm_model"] == "PLG / inbound software"
    assert row["heuristic_override_source"] == "tracked_override"


def test_validate_normalized_rows_reports_suspicious_duplicate_candidates() -> None:
    normalized_rows = [
        {
            "name": "Rezi",
            "canonical_slug": "rezi",
            "category": "AI",
            "revenue_30d": 273000,
            "biz_model": "Software / SaaS",
            "gtm_model": "PLG / inbound software",
            "source_url": "https://trustmrr.com/category/ai",
            "scraped_at": "2026-03-26T19:00:00Z",
            "detail_url": "https://trustmrr.com/startup/rezi",
            "included_in_visible_sample": True,
        },
        {
            "name": "Rezi",
            "canonical_slug": "rezi",
            "category": "Productivity",
            "revenue_30d": 50000,
            "biz_model": "Software / SaaS",
            "gtm_model": "PLG / inbound software",
            "source_url": "https://trustmrr.com/category/productivity",
            "scraped_at": "2026-03-26T19:00:00Z",
            "detail_url": "https://trustmrr.com/startup/rezi-pro",
            "included_in_visible_sample": True,
        },
    ]

    suspicious_duplicates = build_suspicious_duplicates_report(normalized_rows)
    report = validate_normalized_rows(
        normalized_rows,
        suspicious_duplicates_report=suspicious_duplicates,
    )
    checks = {check["id"]: check for check in report["checks"]}

    assert suspicious_duplicates["group_count"] == 1
    assert suspicious_duplicates["groups"][0]["canonical_slug"] == "rezi"
    assert report["suspicious_duplicate_group_count"] == 1
    assert report["status"] == "passed_with_warnings"
    assert not checks["suspicious_canonical_slug_duplicates"]["passed"]


def test_run_pipeline_writes_staged_outputs_from_fixture_fetch(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    data_dir = workspace / "data"
    data_dir.mkdir()

    fetch_paths = FetchPaths(
        root=workspace,
        data_dir=data_dir,
        cache_dir=data_dir / "fetch_cache",
        failure_snapshot_dir=data_dir / "fetch_failures",
    )
    pipeline_paths = PipelinePaths(
        root=workspace,
        pipeline_dir=data_dir / "source_pipeline",
        raw_dir=data_dir / "source_pipeline" / "raw",
        interim_dir=data_dir / "source_pipeline" / "interim",
        processed_dir=data_dir / "source_pipeline" / "processed",
        snapshots_dir=data_dir / "source_pipeline" / "snapshots",
    )
    source = category_source()

    def fake_fetcher(
        selected_source: SourceConfig,
        *,
        paths: FetchPaths,
        force: bool = False,
        last_live_fetch_at: float | None = None,
    ) -> tuple[FetchResult, float | None]:
        paths.cache_dir.mkdir(parents=True, exist_ok=True)
        paths.failure_snapshot_dir.mkdir(parents=True, exist_ok=True)
        html_path = paths.cache_dir / f"{selected_source.source_id}.html"
        meta_path = paths.cache_dir / f"{selected_source.source_id}.json"
        html_path.write_text(read_fixture("category_ai_fixture.html"), encoding="utf-8")
        record = FetchResult(
            source_id=selected_source.source_id,
            url=selected_source.url,
            final_url=selected_source.url,
            parser_strategy=selected_source.parser_strategy,
            source_group=selected_source.source_group,
            category_label=selected_source.category_label,
            content_type="text/html",
            body_sha256="fixture",
            bytes_written=html_path.stat().st_size,
            cache_html_path=html_path.relative_to(paths.root).as_posix(),
            cache_meta_path=meta_path.relative_to(paths.root).as_posix(),
            robots={"allowed": True, "effective_delay_seconds": 0.0},
            cached=False,
        )
        meta_path.write_text(json.dumps(record.__dict__, indent=2, sort_keys=True), encoding="utf-8")
        return record, last_live_fetch_at

    summary = run_pipeline(
        source_registry=[source],
        fetch_paths=fetch_paths,
        pipeline_paths=pipeline_paths,
        fetcher=fake_fetcher,
    )

    assert summary["selected_source_count"] == 1
    assert summary["normalized_row_count"] == 2
    assert summary["visible_sample_row_count"] == 1
    assert summary["validation_status"] == "passed"
    assert summary["fully_mapped_visible_row_count"] == 1
    assert summary["suspicious_duplicate_group_count"] == 0

    assert (pipeline_paths.raw_dir / f"{source.source_id}.html").exists()
    assert (pipeline_paths.raw_dir / f"{source.source_id}.json").exists()
    assert (pipeline_paths.interim_dir / f"{source.source_id}.json").exists()
    assert (pipeline_paths.processed_dir / "normalized_rows.csv").exists()
    assert (pipeline_paths.processed_dir / "visible_sample_rows.csv").exists()
    assert (pipeline_paths.processed_dir / "heuristic_override_report.json").exists()
    assert (pipeline_paths.processed_dir / "suspicious_duplicates.json").exists()
    assert (pipeline_paths.processed_dir / "validation_report.json").exists()
    assert (pipeline_paths.snapshots_dir / "run_manifest.json").exists()
