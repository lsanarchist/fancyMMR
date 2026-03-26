from __future__ import annotations

from dataclasses import asdict, dataclass, field
from html.parser import HTMLParser

from .config import SourceConfig


METRIC_LABEL_ALIASES = {
    "revenue_30d": ("Revenue (30d)",),
    "mrr": ("MRR",),
    "total_revenue": ("Total", "Total revenue", "GMV"),
}
STARTUP_CARD_CLASS_TOKENS = ("flex", "flex-col", "rounded-lg")
TRUSTMRR_BASE_URL = "https://trustmrr.com"


def _normalize_text(value: str) -> str:
    return " ".join(value.split())


@dataclass(frozen=True)
class ParsedStartupCard:
    source_id: str
    source_url: str
    parser_strategy: str
    source_group: str
    category_label: str
    position: int
    detail_path: str
    detail_url: str
    name: str
    description: str
    revenue_30d_text: str
    mrr_text: str
    total_revenue_text: str
    total_revenue_label: str = "Total"
    badge: str | None = None


@dataclass
class _CardBuilder:
    source: SourceConfig
    position: int
    detail_path: str
    name_text: str | None = None
    image_alt: str | None = None
    badge_text: str | None = None
    paragraph_blocks: list[str] = field(default_factory=list)


class _StartupCardHTMLParser(HTMLParser):
    def __init__(self, source: SourceConfig) -> None:
        super().__init__(convert_charrefs=True)
        self.source = source
        self.cards: list[ParsedStartupCard] = []
        self.current_card: _CardBuilder | None = None
        self.card_counter = 0
        self._name_parts: list[str] | None = None
        self._paragraph_parts: list[str] | None = None
        self._badge_parts: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = {key: value for key, value in attrs if value is not None}

        if tag == "a":
            href = attrs_dict.get("href", "")
            class_attr = attrs_dict.get("class", "")
            if href.startswith("/startup/") and all(token in class_attr for token in STARTUP_CARD_CLASS_TOKENS):
                self.card_counter += 1
                self.current_card = _CardBuilder(
                    source=self.source,
                    position=self.card_counter,
                    detail_path=href,
                )
                self._name_parts = None
                self._paragraph_parts = None
                self._badge_parts = None
            return

        if self.current_card is None:
            return

        if tag == "img" and attrs_dict.get("alt"):
            self.current_card.image_alt = _normalize_text(attrs_dict["alt"])
        elif tag == "h3":
            self._name_parts = []
        elif tag == "p":
            self._paragraph_parts = []
        elif tag == "div":
            class_attr = attrs_dict.get("class", "")
            if "rounded-bl-lg" in class_attr and "font-bold" in class_attr:
                self._badge_parts = []

    def handle_data(self, data: str) -> None:
        text = _normalize_text(data)
        if not text:
            return

        if self._name_parts is not None:
            self._name_parts.append(text)
        elif self._paragraph_parts is not None:
            self._paragraph_parts.append(text)
        elif self._badge_parts is not None:
            self._badge_parts.append(text)

    def handle_endtag(self, tag: str) -> None:
        if tag == "h3" and self._name_parts is not None and self.current_card is not None:
            name_text = _normalize_text(" ".join(self._name_parts))
            if name_text:
                self.current_card.name_text = name_text
            self._name_parts = None
            return

        if tag == "p" and self._paragraph_parts is not None and self.current_card is not None:
            paragraph = _normalize_text(" ".join(self._paragraph_parts))
            if paragraph:
                self.current_card.paragraph_blocks.append(paragraph)
            self._paragraph_parts = None
            return

        if tag == "div" and self._badge_parts is not None and self.current_card is not None:
            badge_text = _normalize_text(" ".join(self._badge_parts))
            if badge_text:
                self.current_card.badge_text = badge_text
            self._badge_parts = None
            return

        if tag == "a" and self.current_card is not None:
            self.cards.append(_finalize_card(self.current_card))
            self.current_card = None
            self._name_parts = None
            self._paragraph_parts = None
            self._badge_parts = None


def _finalize_card(builder: _CardBuilder) -> ParsedStartupCard:
    metrics: dict[str, tuple[str, str]] = {}
    description_parts: list[str] = []
    index = 0
    while index < len(builder.paragraph_blocks):
        block = builder.paragraph_blocks[index]
        canonical_metric_key = _metric_key_for_label(block)
        if canonical_metric_key is not None:
            if index + 1 >= len(builder.paragraph_blocks):
                raise ValueError(
                    f"Missing metric value after {block!r} for {builder.detail_path} on {builder.source.url}"
                )
            metrics[canonical_metric_key] = (block, builder.paragraph_blocks[index + 1])
            index += 2
            continue
        description_parts.append(block)
        index += 1

    missing_metric_keys = [
        metric_key for metric_key in METRIC_LABEL_ALIASES if metric_key not in metrics
    ]
    if missing_metric_keys:
        missing_metric_labels = [METRIC_LABEL_ALIASES[key][0] for key in missing_metric_keys]
        raise ValueError(
            f"Missing metric labels {missing_metric_labels!r} for {builder.detail_path} on {builder.source.url}"
        )

    name = builder.name_text or builder.image_alt
    if not name:
        raise ValueError(f"Missing startup name for {builder.detail_path} on {builder.source.url}")

    return ParsedStartupCard(
        source_id=builder.source.source_id,
        source_url=builder.source.url,
        parser_strategy=builder.source.parser_strategy,
        source_group=builder.source.source_group,
        category_label=builder.source.category_label,
        position=builder.position,
        detail_path=builder.detail_path,
        detail_url=f"{TRUSTMRR_BASE_URL}{builder.detail_path}",
        name=name,
        description=" ".join(description_parts).strip(),
        revenue_30d_text=metrics["revenue_30d"][1],
        mrr_text=metrics["mrr"][1],
        total_revenue_text=metrics["total_revenue"][1],
        total_revenue_label=metrics["total_revenue"][0],
        badge=builder.badge_text,
    )


def _metric_key_for_label(label: str) -> str | None:
    for metric_key, aliases in METRIC_LABEL_ALIASES.items():
        if label in aliases:
            return metric_key
    return None


def parse_source_html(source: SourceConfig, html: str) -> list[ParsedStartupCard]:
    parser = _StartupCardHTMLParser(source)
    parser.feed(html)
    parser.close()
    if not parser.cards:
        raise ValueError(f"No startup cards were parsed from {source.url}")
    return parser.cards


def parsed_cards_as_dicts(cards: list[ParsedStartupCard]) -> list[dict[str, object]]:
    return [asdict(card) for card in cards]
