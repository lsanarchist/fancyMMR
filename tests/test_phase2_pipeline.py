from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from src.build_all import PipelinePaths, build_parser, run_pipeline
from src.config import FetchPaths, SourceConfig
from src.fetch import FetchResult
from src.normalize import (
    build_override_key,
    build_visible_sample_rows,
    dedupe_normalized_rows,
    normalize_parsed_cards,
    parse_money_value,
    slugify_startup_name,
)
from src.parse import ParsedStartupCard, parse_source_html, parse_startup_detail_html
from src.validate import build_suspicious_duplicates_report, validate_normalized_rows


FIXTURES = ROOT / "tests" / "fixtures"


def read_fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def write_fetch_fixture_result(
    selected_source: SourceConfig,
    *,
    paths: FetchPaths,
    html_fixture: str,
) -> FetchResult:
    paths.cache_dir.mkdir(parents=True, exist_ok=True)
    paths.failure_snapshot_dir.mkdir(parents=True, exist_ok=True)
    html_path = paths.cache_dir / f"{selected_source.source_id}.html"
    meta_path = paths.cache_dir / f"{selected_source.source_id}.json"
    html_path.write_text(html_fixture, encoding="utf-8")
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
    return record


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


def ecommerce_source() -> SourceConfig:
    return SourceConfig(
        source_id="category--ecommerce",
        url="https://trustmrr.com/category/ecommerce",
        parser_strategy="trustmrr_category_listing",
        category_slug="ecommerce",
        category_label="E-commerce",
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


def test_parse_source_html_accepts_gmv_as_third_metric_label() -> None:
    cards = parse_source_html(ecommerce_source(), read_fixture("category_ecommerce_gmv_fixture.html"))

    assert len(cards) == 2
    assert cards[0].name == "Gumroad"
    assert cards[0].total_revenue_label == "GMV"
    assert cards[0].total_revenue_text == "$878.6M"
    assert cards[1].name == "easytools"
    assert cards[1].total_revenue_label == "GMV"


def test_parse_startup_detail_html_extracts_labeled_fields_from_fixture() -> None:
    card = parse_source_html(category_source(), read_fixture("category_ai_fixture.html"))[0]

    detail = parse_startup_detail_html(card, read_fixture("startup_rezi_detail_fixture.html"))

    assert detail.problem_solved == (
        "Streamlines resume creation and optimization to improve chances of getting interview callbacks."
    )
    assert detail.pricing_summary == "Free: $0/mo, Pro: $29/mo, Lifetime: $149 one-time"
    assert detail.target_audience == "Job Seekers"
    assert detail.business_detail_badges == ("B2C", "~4,005,400 users")
    assert detail.founder_name == "Jacob Jacquet"
    assert detail.founder_role == "Founder of Rezi"
    assert detail.founder_quote == "Open to conversations to better understand market demand."
    assert detail.product_updates_heading_present is True


def test_parse_startup_detail_html_extracts_shared_fields_from_second_fixture_shape() -> None:
    card = ParsedStartupCard(
        source_id="category--mobile-apps",
        source_url="https://trustmrr.com/category/mobile-apps",
        parser_strategy="trustmrr_category_listing",
        source_group="category",
        category_label="Mobile Apps",
        position=1,
        detail_path="/startup/flibbo-ai",
        detail_url="https://trustmrr.com/startup/flibbo-ai",
        name="Flibbo",
        description="AI-powered assistant for posts, visuals, and music.",
        revenue_30d_text="$6.6k",
        mrr_text="$6.2k",
        total_revenue_text="$72k",
    )

    detail = parse_startup_detail_html(card, read_fixture("startup_flibbo_ai_detail_fixture.html"))

    assert detail.problem_solved == (
        "Effortlessly create viral content with AI-generated post ideas, realistic visuals, and original music."
    )
    assert detail.pricing_summary == "Free · In-App Purchases"
    assert detail.target_audience == "Content creators, influencers, and businesses"
    assert detail.business_detail_badges == ("B2C", "~10,000 users")
    assert detail.founder_name == "Flibbo"
    assert detail.founder_role == "Founder of flibbo AI"
    assert detail.founder_quote == (
        "Flibbo is an all in one AI creation platform with image, video, music, prompts, tutorials, and a social feed."
    )
    assert detail.product_updates_heading_present is True


def test_parse_startup_detail_html_fails_loudly_when_required_label_is_missing() -> None:
    card = parse_source_html(category_source(), read_fixture("category_ai_fixture.html"))[0]
    invalid_html = read_fixture("startup_rezi_detail_fixture.html").replace("Pricing", "Price ladder", 1)

    with pytest.raises(ValueError, match="Pricing"):
        parse_startup_detail_html(card, invalid_html)


def test_build_all_parser_accepts_optional_detail_fetch_flags() -> None:
    args = build_parser().parse_args(["--fetch-details", "--detail-limit-per-source", "2"])

    assert args.fetch_details is True
    assert args.detail_limit_per_source == 2


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
    assert rows[0]["total_revenue_label"] == "Total"
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


def test_normalize_parsed_cards_applies_detail_slug_alias_for_generic_name() -> None:
    cards = [
        ParsedStartupCard(
            source_id="category--legal",
            source_url="https://trustmrr.com/category/legal",
            parser_strategy="trustmrr_category_listing",
            source_group="category",
            category_label="Legal",
            position=1,
            detail_path="/startup/hidden-venture-12345",
            detail_url="https://trustmrr.com/startup/hidden-venture-12345",
            name="Hidden Business",
            description="Launch support for founders expanding to the US.",
            revenue_30d_text="$55k",
            mrr_text="$55k",
            total_revenue_text="$660k",
            badge="FOR SALE",
        )
    ]

    row = normalize_parsed_cards(cards, scraped_at="2026-03-26T19:00:00Z")[0]

    assert row["canonical_slug"] == "hidden-business-launch-your-us-business"
    assert row["canonical_slug_source"] == "alias"
    assert row["biz_model"] == "Services / agency / lead-gen"
    assert row["gtm_model"] == "Sales-led / outbound / SEO"
    assert row["heuristic_override_key"] == build_override_key(
        "https://trustmrr.com/category/legal",
        "hidden-business-launch-your-us-business",
    )


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


def test_normalize_parsed_cards_disambiguates_same_source_name_collision_with_detail_slug() -> None:
    cards = [
        ParsedStartupCard(
            source_id="category--design-tools",
            source_url="https://trustmrr.com/category/design-tools",
            parser_strategy="trustmrr_category_listing",
            source_group="category",
            category_label="Design Tools",
            position=1,
            detail_path="/startup/sleek",
            detail_url="https://trustmrr.com/startup/sleek",
            name="Sleek",
            description="First design workflow tool.",
            revenue_30d_text="$25k",
            mrr_text="$27k",
            total_revenue_text="$1.2M",
            badge="FOR SALE",
        ),
        ParsedStartupCard(
            source_id="category--design-tools",
            source_url="https://trustmrr.com/category/design-tools",
            parser_strategy="trustmrr_category_listing",
            source_group="category",
            category_label="Design Tools",
            position=2,
            detail_path="/startup/sleek-1",
            detail_url="https://trustmrr.com/startup/sleek-1",
            name="Sleek",
            description="Second design workflow tool.",
            revenue_30d_text="$7.8k",
            mrr_text="$8.1k",
            total_revenue_text="$210k",
            badge="FOR SALE",
        ),
    ]

    rows = normalize_parsed_cards(cards, scraped_at="2026-03-26T19:00:00Z")

    assert rows[0]["canonical_slug"] == "sleek"
    assert rows[0]["biz_model"] == "Software / SaaS"
    assert rows[1]["canonical_slug"] == "sleek-1"
    assert rows[1]["canonical_slug_source"] == "detail_slug_collision"
    assert rows[1]["biz_model"] == "Software / SaaS"
    assert rows[1]["gtm_model"] == "PLG / inbound software"


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


def test_build_suspicious_duplicates_report_ignores_non_visible_collisions() -> None:
    normalized_rows = [
        {
            "name": "Private Enterprise",
            "canonical_slug": "private-enterprise",
            "category": "Marketing",
            "revenue_30d": 41000,
            "biz_model": "Services / agency / lead-gen",
            "gtm_model": "Sales-led / outbound / SEO",
            "source_url": "https://trustmrr.com/category/marketing",
            "scraped_at": "2026-03-26T19:00:00Z",
            "detail_url": "https://trustmrr.com/startup/hype-social-media-strategy",
            "included_in_visible_sample": True,
        },
        {
            "name": "Private Enterprise",
            "canonical_slug": "private-enterprise",
            "category": "Real Estate",
            "revenue_30d": 186,
            "biz_model": "",
            "gtm_model": "",
            "source_url": "https://trustmrr.com/category/real-estate",
            "scraped_at": "2026-03-26T19:00:00Z",
            "detail_url": "https://trustmrr.com/startup/private-enterprise",
            "included_in_visible_sample": False,
        },
    ]

    report = build_suspicious_duplicates_report(normalized_rows)

    assert report["group_count"] == 0
    assert report["row_count"] == 0


def test_dedupe_normalized_rows_prefers_primary_category_over_special_category_for_same_detail_url() -> None:
    normalized_rows = [
        {
            "name": "Roofclaw",
            "canonical_slug": "roofclaw",
            "category": "OpenClaw (special)",
            "revenue_30d": 55000,
            "biz_model": "Services / agency / lead-gen",
            "gtm_model": "Sales-led / outbound / SEO",
            "source_url": "https://trustmrr.com/special-category/openclaw",
            "source_group": "special-category",
            "scraped_at": "2026-03-26T19:00:00Z",
            "detail_url": "https://trustmrr.com/startup/roofclaw",
            "included_in_visible_sample": True,
            "position": 9,
        },
        {
            "name": "Roofclaw",
            "canonical_slug": "roofclaw",
            "category": "IoT & Hardware",
            "revenue_30d": 55000,
            "biz_model": "Services / agency / lead-gen",
            "gtm_model": "Sales-led / outbound / SEO",
            "source_url": "https://trustmrr.com/category/iot-hardware",
            "source_group": "category",
            "scraped_at": "2026-03-26T19:00:00Z",
            "detail_url": "https://trustmrr.com/startup/roofclaw",
            "included_in_visible_sample": True,
            "position": 1,
        },
    ]

    deduped_rows = dedupe_normalized_rows(normalized_rows)

    assert len(deduped_rows) == 1
    assert deduped_rows[0]["source_url"] == "https://trustmrr.com/category/iot-hardware"
    assert deduped_rows[0]["category"] == "IoT & Hardware"


def test_validate_normalized_rows_treats_duplicate_name_source_pairs_as_warning() -> None:
    normalized_rows = [
        {
            "name": "Private Enterprise",
            "canonical_slug": "private-enterprise",
            "category": "Mobile Apps",
            "revenue_30d": 32000,
            "biz_model": "Software / SaaS",
            "gtm_model": "PLG / inbound software",
            "source_url": "https://trustmrr.com/category/mobile-apps",
            "scraped_at": "2026-03-26T19:00:00Z",
            "detail_url": "https://trustmrr.com/startup/private-enterprise-6",
            "included_in_visible_sample": True,
        },
        {
            "name": "Private Enterprise",
            "canonical_slug": "private-enterprise",
            "category": "Mobile Apps",
            "revenue_30d": 44000,
            "biz_model": "Software / SaaS",
            "gtm_model": "PLG / inbound software",
            "source_url": "https://trustmrr.com/category/mobile-apps",
            "scraped_at": "2026-03-26T19:00:00Z",
            "detail_url": "https://trustmrr.com/startup/private-enterprise-10",
            "included_in_visible_sample": True,
        },
    ]

    report = validate_normalized_rows(normalized_rows)
    checks = {check["id"]: check for check in report["checks"]}

    assert report["status"] == "passed_with_warnings"
    assert report["duplicate_name_source_url_count"] == 1
    assert checks["visible_name_source_pairs_unique"]["severity"] == "warning"
    assert not checks["visible_name_source_pairs_unique"]["passed"]


def test_validate_normalized_rows_accepts_disambiguated_name_source_pairs() -> None:
    normalized_rows = [
        {
            "name": "Private Enterprise",
            "canonical_slug": "private-enterprise-mobile-apps",
            "category": "Mobile Apps",
            "revenue_30d": 32000,
            "biz_model": "Consumer app / subscription",
            "gtm_model": "App-store / paid social / consumer",
            "source_url": "https://trustmrr.com/category/mobile-apps",
            "scraped_at": "2026-03-26T19:00:00Z",
            "detail_url": "https://trustmrr.com/startup/private-enterprise-6",
            "included_in_visible_sample": True,
        },
        {
            "name": "Private Enterprise",
            "canonical_slug": "private-enterprise-health-wellness-app",
            "category": "Mobile Apps",
            "revenue_30d": 44000,
            "biz_model": "Consumer app / subscription",
            "gtm_model": "App-store / paid social / consumer",
            "source_url": "https://trustmrr.com/category/mobile-apps",
            "scraped_at": "2026-03-26T19:00:00Z",
            "detail_url": "https://trustmrr.com/startup/private-enterprise-10",
            "included_in_visible_sample": True,
        },
    ]

    report = validate_normalized_rows(normalized_rows)
    checks = {check["id"]: check for check in report["checks"]}

    assert report["status"] == "passed"
    assert report["duplicate_name_source_url_count"] == 0
    assert checks["visible_name_source_pairs_unique"]["passed"]


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
        return (
            write_fetch_fixture_result(
                selected_source,
                paths=paths,
                html_fixture=read_fixture("category_ai_fixture.html"),
            ),
            last_live_fetch_at,
        )

    def detail_page_html_resolver(card: ParsedStartupCard) -> str | None:
        if card.detail_url.endswith("/rezi"):
            return read_fixture("startup_rezi_detail_fixture.html")
        return None

    summary = run_pipeline(
        source_registry=[source],
        fetch_paths=fetch_paths,
        pipeline_paths=pipeline_paths,
        fetcher=fake_fetcher,
        detail_page_html_resolver=detail_page_html_resolver,
    )

    assert summary["selected_source_count"] == 1
    assert summary["detail_page_target_count"] == 2
    assert summary["fetched_detail_page_count"] == 0
    assert summary["parsed_detail_page_count"] == 1
    assert summary["normalized_row_count"] == 2
    assert summary["visible_sample_row_count"] == 1
    assert summary["validation_status"] == "passed"
    assert summary["fully_mapped_visible_row_count"] == 1
    assert summary["suspicious_duplicate_group_count"] == 0

    assert (pipeline_paths.raw_dir / f"{source.source_id}.html").exists()
    assert (pipeline_paths.raw_dir / f"{source.source_id}.json").exists()
    assert (pipeline_paths.interim_dir / f"{source.source_id}.json").exists()
    assert (pipeline_paths.interim_dir / f"{source.source_id}.detail_pages.json").exists()
    assert (pipeline_paths.processed_dir / "normalized_rows.csv").exists()
    assert (pipeline_paths.processed_dir / "visible_sample_rows.csv").exists()
    assert (pipeline_paths.processed_dir / "heuristic_override_report.json").exists()
    assert (pipeline_paths.processed_dir / "suspicious_duplicates.json").exists()
    assert (pipeline_paths.processed_dir / "validation_report.json").exists()
    assert (pipeline_paths.snapshots_dir / "run_manifest.json").exists()

    detail_scaffolds = json.loads(
        (pipeline_paths.interim_dir / f"{source.source_id}.detail_pages.json").read_text(encoding="utf-8")
    )
    snapshot_manifest = json.loads(
        (pipeline_paths.snapshots_dir / "run_manifest.json").read_text(encoding="utf-8")
    )
    assert detail_scaffolds["detail_page_target_count"] == 2
    assert detail_scaffolds["parsed_detail_page_count"] == 1
    assert detail_scaffolds["detail_pages"][0]["detail_parse_status"] == "parsed"
    assert detail_scaffolds["detail_pages"][0]["detail_slug"] == "rezi"
    assert detail_scaffolds["detail_pages"][0]["extracted_detail"]["target_audience"] == "Job Seekers"
    assert detail_scaffolds["detail_pages"][1]["detail_parse_status"] == "html_not_supplied"
    assert detail_scaffolds["detail_pages"][1]["extracted_detail"] is None
    assert snapshot_manifest["detail_page_target_count"] == 2
    assert snapshot_manifest["fetched_detail_page_count"] == 0
    assert snapshot_manifest["parsed_detail_page_count"] == 1
    assert snapshot_manifest["per_source_outputs"][0]["detail_scaffold_path"].endswith(
        "data/source_pipeline/interim/category--ai.detail_pages.json"
    )


def test_run_pipeline_fetches_and_stages_detail_pages_when_requested(tmp_path: Path) -> None:
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
    detail_fetch_urls: list[str] = []

    def fake_fetcher(
        selected_source: SourceConfig,
        *,
        paths: FetchPaths,
        force: bool = False,
        last_live_fetch_at: float | None = None,
    ) -> tuple[FetchResult, float | None]:
        return (
            write_fetch_fixture_result(
                selected_source,
                paths=paths,
                html_fixture=read_fixture("category_ai_fixture.html"),
            ),
            last_live_fetch_at,
        )

    def fake_detail_fetcher(
        selected_source: SourceConfig,
        *,
        paths: FetchPaths,
        force: bool = False,
        last_live_fetch_at: float | None = None,
    ) -> tuple[FetchResult, float | None]:
        detail_fetch_urls.append(selected_source.url)
        assert selected_source.parser_strategy == "trustmrr_startup_detail"
        return (
            write_fetch_fixture_result(
                selected_source,
                paths=paths,
                html_fixture=read_fixture("startup_rezi_detail_fixture.html"),
            ),
            last_live_fetch_at,
        )

    summary = run_pipeline(
        source_registry=[source],
        fetch_paths=fetch_paths,
        pipeline_paths=pipeline_paths,
        fetcher=fake_fetcher,
        fetch_detail_pages=True,
        detail_page_limit_per_source=1,
        detail_page_fetcher=fake_detail_fetcher,
    )

    assert detail_fetch_urls == ["https://trustmrr.com/startup/rezi"]
    assert summary["selected_source_count"] == 1
    assert summary["detail_page_target_count"] == 2
    assert summary["fetched_detail_page_count"] == 1
    assert summary["parsed_detail_page_count"] == 1
    assert summary["validation_status"] == "passed"

    detail_scaffolds = json.loads(
        (pipeline_paths.interim_dir / f"{source.source_id}.detail_pages.json").read_text(encoding="utf-8")
    )
    snapshot_manifest = json.loads(
        (pipeline_paths.snapshots_dir / "run_manifest.json").read_text(encoding="utf-8")
    )

    assert (pipeline_paths.raw_dir / "details" / "category--ai--detail--rezi.html").exists()
    assert (pipeline_paths.raw_dir / "details" / "category--ai--detail--rezi.json").exists()
    assert detail_scaffolds["fetched_detail_page_count"] == 1
    assert detail_scaffolds["parsed_detail_page_count"] == 1
    assert detail_scaffolds["detail_pages"][0]["detail_parse_status"] == "parsed"
    assert detail_scaffolds["detail_pages"][0]["detail_html_source"] == "fetched_html"
    assert detail_scaffolds["detail_pages"][0]["detail_fetch_source_id"] == "category--ai--detail--rezi"
    assert detail_scaffolds["detail_pages"][0]["detail_fetch_cached"] is False
    assert detail_scaffolds["detail_pages"][0]["detail_raw_html_path"].endswith(
        "data/source_pipeline/raw/details/category--ai--detail--rezi.html"
    )
    assert detail_scaffolds["detail_pages"][1]["detail_parse_status"] == "skipped_by_limit"
    assert detail_scaffolds["detail_pages"][1]["detail_html_source"] == "fetch_skipped_by_limit"
    assert detail_scaffolds["detail_pages"][1]["detail_raw_html_path"] is None
    assert snapshot_manifest["detail_page_target_count"] == 2
    assert snapshot_manifest["fetched_detail_page_count"] == 1
    assert snapshot_manifest["parsed_detail_page_count"] == 1
    assert snapshot_manifest["per_source_outputs"][0]["fetched_detail_page_count"] == 1
