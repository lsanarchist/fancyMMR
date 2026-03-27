from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import html
from typing import Callable


CommandItem = dict[str, object]


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


def output_registry_item_format(item: CommandItem) -> str:
    return str(item.get("format") or "").strip().lower() or "other"


@dataclass(frozen=True)
class OutputRegistryFormatSummary:
    item_format: str
    item_count: int
    count_share: str
    total_bytes: str
    byte_range: str
    spread_ratio: str
    byte_delta: str
    share_gap: str
    top_share: str
    smallest_share: str
    median_size: str
    max_to_median_ratio: str
    average_size: str
    byte_share: str


def output_registry_format_summaries(
    command_items: list[CommandItem],
) -> dict[str, OutputRegistryFormatSummary]:
    format_order: list[str] = []
    format_counts: Counter[str] = Counter()
    format_bytes: Counter[str] = Counter()
    format_byte_values: dict[str, list[int]] = {}
    format_min_bytes: dict[str, int] = {}
    format_max_bytes: dict[str, int] = {}

    for item in command_items:
        item_format = output_registry_item_format(item)
        if item_format not in format_counts:
            format_order.append(item_format)
        format_counts[item_format] += 1
        item_bytes = item.get("bytes")
        if not isinstance(item_bytes, int):
            continue
        format_bytes[item_format] += item_bytes
        format_byte_values.setdefault(item_format, []).append(item_bytes)
        if item_format not in format_min_bytes or item_bytes < format_min_bytes[item_format]:
            format_min_bytes[item_format] = item_bytes
        if item_format not in format_max_bytes or item_bytes > format_max_bytes[item_format]:
            format_max_bytes[item_format] = item_bytes

    section_item_count = sum(format_counts.values())
    section_total_bytes = sum(format_bytes.values())
    summaries: dict[str, OutputRegistryFormatSummary] = {}
    for item_format in format_order:
        total_bytes = format_bytes[item_format]
        byte_values = format_byte_values.get(item_format, [])
        median_bytes = median_byte_value(byte_values)
        summaries[item_format] = OutputRegistryFormatSummary(
            item_format=item_format,
            item_count=format_counts[item_format],
            count_share=format_count_share(format_counts[item_format], section_item_count),
            total_bytes=format_byte_count(total_bytes),
            byte_range=format_byte_range(format_min_bytes.get(item_format), format_max_bytes.get(item_format)),
            spread_ratio=format_byte_spread_ratio(format_min_bytes.get(item_format), format_max_bytes.get(item_format)),
            byte_delta=format_byte_delta(format_min_bytes.get(item_format), format_max_bytes.get(item_format)),
            share_gap=format_share_gap(
                format_min_bytes.get(item_format),
                format_max_bytes.get(item_format),
                total_bytes,
            ),
            top_share=format_top_file_share(format_max_bytes.get(item_format), total_bytes),
            smallest_share=format_smallest_file_share(format_min_bytes.get(item_format), total_bytes),
            median_size=format_median_byte_count(byte_values),
            max_to_median_ratio=format_max_to_median_ratio(
                format_max_bytes.get(item_format),
                median_bytes,
            ),
            average_size=format_average_byte_count(total_bytes, format_counts[item_format]),
            byte_share=format_byte_share(total_bytes, section_total_bytes),
        )
    return summaries


def output_registry_format_divider_markup(summary: OutputRegistryFormatSummary) -> str:
    return (
        '<div class="rail-command-divider">'
        f'<span class="rail-command-divider-label">{html.escape(summary.item_format.upper())}</span>'
        f'<span class="rail-command-divider-count">{summary.item_count:,}</span>'
        f'<span class="rail-command-divider-file-share">{html.escape(summary.count_share)}</span>'
        f'<span class="rail-command-divider-bytes">{html.escape(summary.total_bytes)}</span>'
        f'<span class="rail-command-divider-range">{html.escape(summary.byte_range)}</span>'
        f'<span class="rail-command-divider-spread">{html.escape(summary.spread_ratio)}</span>'
        f'<span class="rail-command-divider-delta">{html.escape(summary.byte_delta)}</span>'
        f'<span class="rail-command-divider-gap">{html.escape(summary.share_gap)}</span>'
        f'<span class="rail-command-divider-top-share">{html.escape(summary.top_share)}</span>'
        f'<span class="rail-command-divider-smallest-share">{html.escape(summary.smallest_share)}</span>'
        f'<span class="rail-command-divider-median">{html.escape(summary.median_size)}</span>'
        f'<span class="rail-command-divider-max-median">{html.escape(summary.max_to_median_ratio)}</span>'
        f'<span class="rail-command-divider-average">{html.escape(summary.average_size)}</span>'
        f'<span class="rail-command-divider-share">{html.escape(summary.byte_share)}</span>'
        "</div>"
    )


def output_registry_command_links_markup(
    command_items: list[CommandItem],
    *,
    link_renderer: Callable[[CommandItem], str],
) -> str:
    summaries = output_registry_format_summaries(command_items)
    parts: list[str] = []
    active_format = None
    for item in command_items:
        item_format = output_registry_item_format(item)
        if item_format != active_format:
            summary = summaries.get(item_format)
            if summary is not None:
                parts.append(output_registry_format_divider_markup(summary))
            active_format = item_format
        parts.append(link_renderer(item))
    return "".join(parts)
