from __future__ import annotations

from decimal import Decimal, InvalidOperation
from functools import lru_cache
import json
from pathlib import Path
import re
import unicodedata

from .fancymmr_build.config import MIN_REVENUE_30D, REVENUE_BINS, REVENUE_LABELS
from .parse import ParsedStartupCard


NORMALIZED_ROW_FIELDS = [
    "name",
    "canonical_slug",
    "canonical_slug_source",
    "detail_slug",
    "category",
    "biz_model",
    "gtm_model",
    "revenue_30d",
    "revenue_30d_display",
    "mrr",
    "mrr_display",
    "total_revenue",
    "total_revenue_display",
    "revenue_band",
    "included_in_visible_sample",
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
    "scraped_at",
]
VISIBLE_SAMPLE_ROW_FIELDS = [
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
]

ROOT = Path(__file__).resolve().parents[1]
OVERRIDES_DIR = ROOT / "data" / "source_pipeline" / "overrides"
BIZ_MODEL_OVERRIDES_PATH = OVERRIDES_DIR / "biz_model_overrides.json"
GTM_MODEL_OVERRIDES_PATH = OVERRIDES_DIR / "gtm_model_overrides.json"
CANONICAL_SLUG_ALIASES_PATH = OVERRIDES_DIR / "canonical_slug_aliases.json"

_MONEY_PATTERN = re.compile(r"^\$?\s*([0-9][0-9,]*(?:\.\d+)?)\s*([kKmMbB]?)$")
_SUFFIX_MULTIPLIERS = {
    "": 1,
    "k": 1_000,
    "m": 1_000_000,
    "b": 1_000_000_000,
}


def slugify_startup_name(value: str) -> str:
    ascii_value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_value.casefold()).strip("-")
    return slug or "unknown"


def detail_slug_from_path(detail_path: str) -> str:
    segment = detail_path.rsplit("/", 1)[-1] if detail_path else ""
    return slugify_startup_name(segment)


def build_override_key(source_url: str, canonical_slug: str) -> str:
    return f"{source_url}::{canonical_slug}"


def _load_override_payload(path: Path, payload_key: str) -> dict[str, str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {str(key): str(value) for key, value in payload.get(payload_key, {}).items()}


@lru_cache(maxsize=1)
def load_canonical_slug_aliases() -> dict[str, str]:
    return _load_override_payload(CANONICAL_SLUG_ALIASES_PATH, "aliases")


@lru_cache(maxsize=1)
def load_heuristic_override_maps() -> dict[str, dict[str, str]]:
    return {
        "biz_model": _load_override_payload(BIZ_MODEL_OVERRIDES_PATH, "overrides"),
        "gtm_model": _load_override_payload(GTM_MODEL_OVERRIDES_PATH, "overrides"),
    }


def resolve_canonical_slug(*, source_url: str, name: str, detail_path: str) -> tuple[str, str, str]:
    name_slug = slugify_startup_name(name)
    detail_slug = detail_slug_from_path(detail_path)
    aliases = load_canonical_slug_aliases()

    for observed_slug in (name_slug, detail_slug):
        alias = aliases.get(build_override_key(source_url, observed_slug))
        if alias:
            return alias, detail_slug, "alias"

    return name_slug, detail_slug, "name_slug"


def parse_money_value(value: str) -> int:
    normalized = value.strip()
    match = _MONEY_PATTERN.fullmatch(normalized)
    if not match:
        raise ValueError(f"Unsupported money value: {value!r}")

    amount_text, suffix = match.groups()
    try:
        amount = Decimal(amount_text.replace(",", ""))
    except InvalidOperation as error:
        raise ValueError(f"Unsupported money value: {value!r}") from error

    multiplier = _SUFFIX_MULTIPLIERS[suffix.lower()]
    return int(amount * multiplier)


def revenue_band_for_value(revenue_30d: int) -> str | None:
    if revenue_30d < MIN_REVENUE_30D:
        return None

    for lower, upper, label in zip(REVENUE_BINS[:-1], REVENUE_BINS[1:], REVENUE_LABELS):
        if lower <= revenue_30d < upper:
            return label

    return REVENUE_LABELS[-1]


def build_heuristic_override_report(normalized_rows: list[dict[str, object]]) -> dict[str, object]:
    visible_rows = [row for row in normalized_rows if row["included_in_visible_sample"]]
    unmapped_visible_rows = [
        {
            "name": row["name"],
            "canonical_slug": row["canonical_slug"],
            "source_url": row["source_url"],
            "detail_url": row["detail_url"],
        }
        for row in visible_rows
        if not row["biz_model"] or not row["gtm_model"]
    ]

    return {
        "schema_version": 1,
        "override_key_format": "<source_url>::<canonical_slug>",
        "override_files": [
            "data/source_pipeline/overrides/biz_model_overrides.json",
            "data/source_pipeline/overrides/gtm_model_overrides.json",
            "data/source_pipeline/overrides/canonical_slug_aliases.json",
        ],
        "total_row_count": len(normalized_rows),
        "visible_sample_row_count": len(visible_rows),
        "biz_model_override_count": sum(1 for row in visible_rows if row["biz_model"]),
        "gtm_model_override_count": sum(1 for row in visible_rows if row["gtm_model"]),
        "fully_mapped_visible_row_count": sum(
            1 for row in visible_rows if row["biz_model"] and row["gtm_model"]
        ),
        "alias_resolved_visible_row_count": sum(
            1 for row in visible_rows if row["canonical_slug_source"] == "alias"
        ),
        "unmapped_visible_row_count": len(unmapped_visible_rows),
        "unmapped_visible_rows": sorted(
            unmapped_visible_rows,
            key=lambda row: (row["source_url"], row["canonical_slug"], row["name"]),
        ),
    }


def normalize_parsed_cards(cards: list[ParsedStartupCard], *, scraped_at: str) -> list[dict[str, object]]:
    override_maps = load_heuristic_override_maps()
    normalized_rows: list[dict[str, object]] = []
    for card in cards:
        revenue_30d = parse_money_value(card.revenue_30d_text)
        mrr = parse_money_value(card.mrr_text)
        total_revenue = parse_money_value(card.total_revenue_text)
        included = revenue_30d >= MIN_REVENUE_30D
        canonical_slug, detail_slug, canonical_slug_source = resolve_canonical_slug(
            source_url=card.source_url,
            name=card.name,
            detail_path=card.detail_path,
        )
        heuristic_override_key = build_override_key(card.source_url, canonical_slug)
        biz_model = override_maps["biz_model"].get(heuristic_override_key, "")
        gtm_model = override_maps["gtm_model"].get(heuristic_override_key, "")

        normalized_rows.append(
            {
                "name": card.name,
                "canonical_slug": canonical_slug,
                "canonical_slug_source": canonical_slug_source,
                "detail_slug": detail_slug,
                "category": card.category_label,
                "biz_model": biz_model,
                "gtm_model": gtm_model,
                "revenue_30d": revenue_30d,
                "revenue_30d_display": card.revenue_30d_text,
                "mrr": mrr,
                "mrr_display": card.mrr_text,
                "total_revenue": total_revenue,
                "total_revenue_display": card.total_revenue_text,
                "revenue_band": revenue_band_for_value(revenue_30d),
                "included_in_visible_sample": included,
                "source_url": card.source_url,
                "source_id": card.source_id,
                "source_group": card.source_group,
                "parser_strategy": card.parser_strategy,
                "detail_url": card.detail_url,
                "detail_path": card.detail_path,
                "position": card.position,
                "badge": card.badge or "",
                "description": card.description,
                "heuristic_override_key": heuristic_override_key,
                "heuristic_override_source": "tracked_override" if biz_model or gtm_model else "",
                "scraped_at": scraped_at,
            }
        )

    return normalized_rows


def build_visible_sample_rows(normalized_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    visible_rows: list[dict[str, object]] = []
    for row in normalized_rows:
        if not row["included_in_visible_sample"]:
            continue
        visible_rows.append({field: row[field] for field in VISIBLE_SAMPLE_ROW_FIELDS})
    return visible_rows
