from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
import time
from pathlib import Path
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.robotparser import RobotFileParser

from .config import DEFAULT_FETCH_PATHS, DEFAULT_FETCH_POLICY, FetchPaths, FetchPolicy, SourceConfig, load_source_registry


@dataclass(frozen=True)
class FetchResult:
    source_id: str
    url: str
    final_url: str
    parser_strategy: str
    source_group: str
    category_label: str
    content_type: str
    body_sha256: str
    bytes_written: int
    cache_html_path: str
    cache_meta_path: str
    robots: dict[str, object]
    cached: bool


class FetchError(RuntimeError):
    """Raised when the polite fetch layer cannot safely fetch a source page."""


def _ensure_fetch_dirs(paths: FetchPaths) -> None:
    paths.cache_dir.mkdir(parents=True, exist_ok=True)
    paths.failure_snapshot_dir.mkdir(parents=True, exist_ok=True)


def _cache_html_path(paths: FetchPaths, source: SourceConfig) -> Path:
    return paths.cache_dir / f"{source.source_id}.html"


def _cache_meta_path(paths: FetchPaths, source: SourceConfig) -> Path:
    return paths.cache_dir / f"{source.source_id}.json"


def _failure_html_path(paths: FetchPaths, source: SourceConfig) -> Path:
    return paths.failure_snapshot_dir / f"{source.source_id}.html"


def _failure_meta_path(paths: FetchPaths, source: SourceConfig) -> Path:
    return paths.failure_snapshot_dir / f"{source.source_id}.json"


def _build_headers(policy: FetchPolicy, *, accept: str) -> dict[str, str]:
    return {
        "User-Agent": policy.http_user_agent,
        "Accept": accept,
    }


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _relative_to_root(path: Path, paths: FetchPaths) -> str:
    return path.relative_to(paths.root).as_posix()


def _robots_url(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}/robots.txt"


def _request_rate_delay(request_rate) -> float:
    if request_rate is None or not getattr(request_rate, "requests", 0):
        return 0.0
    return float(request_rate.seconds) / float(request_rate.requests)


def load_cached_result(source: SourceConfig, *, paths: FetchPaths = DEFAULT_FETCH_PATHS, policy: FetchPolicy = DEFAULT_FETCH_POLICY) -> FetchResult | None:
    html_path = _cache_html_path(paths, source)
    meta_path = _cache_meta_path(paths, source)
    if not html_path.exists() or not meta_path.exists():
        return None

    age_seconds = max(0.0, time.time() - html_path.stat().st_mtime)
    if age_seconds > policy.cache_ttl_seconds:
        return None

    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    return FetchResult(**{**metadata, "cached": True})


def write_cached_result(
    source: SourceConfig,
    *,
    body: bytes,
    final_url: str,
    content_type: str,
    robots: dict[str, object],
    paths: FetchPaths = DEFAULT_FETCH_PATHS,
) -> FetchResult:
    html_path = _cache_html_path(paths, source)
    meta_path = _cache_meta_path(paths, source)
    html_path.write_bytes(body)
    record = FetchResult(
        source_id=source.source_id,
        url=source.url,
        final_url=final_url,
        parser_strategy=source.parser_strategy,
        source_group=source.source_group,
        category_label=source.category_label,
        content_type=content_type,
        body_sha256=_sha256_bytes(body),
        bytes_written=len(body),
        cache_html_path=_relative_to_root(html_path, paths),
        cache_meta_path=_relative_to_root(meta_path, paths),
        robots=robots,
        cached=False,
    )
    meta_path.write_text(json.dumps(record.__dict__, indent=2, sort_keys=True), encoding="utf-8")
    return record


def save_failure_snapshot(
    source: SourceConfig,
    *,
    error: Exception,
    paths: FetchPaths = DEFAULT_FETCH_PATHS,
    response_body: bytes | None = None,
    response_status: int | None = None,
    robots: dict[str, object] | None = None,
) -> dict[str, object]:
    _ensure_fetch_dirs(paths)
    html_path = _failure_html_path(paths, source)
    meta_path = _failure_meta_path(paths, source)
    html_snapshot_path: str | None = None
    if response_body is not None:
        html_path.write_text(response_body.decode("utf-8", errors="replace"), encoding="utf-8")
        html_snapshot_path = _relative_to_root(html_path, paths)

    snapshot = {
        "source_id": source.source_id,
        "url": source.url,
        "parser_strategy": source.parser_strategy,
        "source_group": source.source_group,
        "error_type": error.__class__.__name__,
        "message": str(error),
        "status_code": response_status,
        "robots": robots,
        "html_snapshot_path": html_snapshot_path,
    }
    meta_path.write_text(json.dumps(snapshot, indent=2, sort_keys=True), encoding="utf-8")
    return snapshot


def resolve_robots_rules(
    source: SourceConfig,
    *,
    policy: FetchPolicy = DEFAULT_FETCH_POLICY,
    opener: Callable = urlopen,
) -> dict[str, object]:
    robots_url = _robots_url(source.url)
    parser = RobotFileParser()
    parser.set_url(robots_url)
    status_code: int | None = None

    try:
        request = Request(robots_url, headers=_build_headers(policy, accept="text/plain,*/*;q=0.8"))
        with opener(request, timeout=policy.timeout_seconds) as response:
            status_code = getattr(response, "status", 200)
            body = response.read().decode("utf-8", errors="replace")
        parser.parse(body.splitlines())
    except HTTPError as error:
        status_code = error.code
        if error.code == 404:
            parser.parse([])
        else:
            raise FetchError(f"Unable to read robots.txt for {source.url}: HTTP {error.code}") from error
    except URLError as error:
        raise FetchError(f"Unable to read robots.txt for {source.url}: {error.reason}") from error

    allowed = True
    if policy.respect_robots_txt:
        allowed = parser.can_fetch(policy.robots_user_agent, source.url)

    crawl_delay = parser.crawl_delay(policy.robots_user_agent)
    request_rate = parser.request_rate(policy.robots_user_agent)
    effective_delay = max(policy.min_delay_seconds, float(crawl_delay or 0), _request_rate_delay(request_rate))

    return {
        "robots_url": robots_url,
        "status_code": status_code,
        "allowed": allowed,
        "crawl_delay_seconds": crawl_delay,
        "request_rate": None
        if request_rate is None
        else {
            "requests": request_rate.requests,
            "seconds": request_rate.seconds,
        },
        "effective_delay_seconds": effective_delay,
    }


def fetch_source(
    source: SourceConfig,
    *,
    paths: FetchPaths = DEFAULT_FETCH_PATHS,
    policy: FetchPolicy = DEFAULT_FETCH_POLICY,
    force: bool = False,
    opener: Callable = urlopen,
    robots_resolver: Callable[..., dict[str, object]] = resolve_robots_rules,
    sleeper: Callable[[float], None] = time.sleep,
    last_live_fetch_at: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
) -> tuple[FetchResult, float | None]:
    _ensure_fetch_dirs(paths)

    if not force:
        cached = load_cached_result(source, paths=paths, policy=policy)
        if cached is not None:
            return cached, last_live_fetch_at

    robots = robots_resolver(source, policy=policy, opener=opener)
    if not robots["allowed"]:
        snapshot = save_failure_snapshot(
            source,
            error=FetchError(f"Fetching {source.url} is disallowed by robots.txt"),
            paths=paths,
            robots=robots,
        )
        raise FetchError(f"Fetching {source.url} is disallowed by robots.txt; snapshot={snapshot['html_snapshot_path']}")

    if last_live_fetch_at is not None:
        remaining_delay = float(robots["effective_delay_seconds"]) - (monotonic() - last_live_fetch_at)
        if remaining_delay > 0:
            sleeper(remaining_delay)

    request = Request(
        source.url,
        headers=_build_headers(policy, accept="text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
    )

    last_error: Exception | None = None
    for attempt in range(policy.max_retries + 1):
        started_at = monotonic()
        try:
            with opener(request, timeout=policy.timeout_seconds) as response:
                body = response.read()
                final_url = response.geturl()
                content_type = response.headers.get_content_type()
            result = write_cached_result(
                source,
                body=body,
                final_url=final_url,
                content_type=content_type,
                robots=robots,
                paths=paths,
            )
            return result, started_at
        except HTTPError as error:
            body = error.read()
            save_failure_snapshot(
                source,
                error=error,
                paths=paths,
                response_body=body or None,
                response_status=error.code,
                robots=robots,
            )
            last_error = error
        except URLError as error:
            save_failure_snapshot(source, error=error, paths=paths, robots=robots)
            last_error = error

        if attempt < policy.max_retries:
            sleeper(policy.retry_backoff_seconds * (attempt + 1))

    if last_error is None:
        raise FetchError(f"Fetching {source.url} failed without an explicit error")
    raise FetchError(f"Unable to fetch {source.url}: {last_error}") from last_error


def select_sources(
    *,
    source_id: str | None = None,
    limit: int | None = None,
) -> list[SourceConfig]:
    registry = load_source_registry()
    if source_id is not None:
        registry = [source for source in registry if source.source_id == source_id]
    if limit is not None:
        registry = registry[:limit]
    return registry


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m src.fetch",
        description="Polite public-page fetch tooling for the current TrustMRR visible-sample source registry.",
    )
    subparsers = parser.add_subparsers(dest="command")

    list_parser = subparsers.add_parser("list", help="List the current seed source registry")
    list_parser.add_argument("--limit", type=int, default=None, help="Only print the first N registry entries")

    fetch_parser = subparsers.add_parser("fetch", help="Fetch one or more source pages into the local cache")
    fetch_parser.add_argument("--source-id", default=None, help="Fetch one specific source id")
    fetch_parser.add_argument("--limit", type=int, default=None, help="Only fetch the first N sources")
    fetch_parser.add_argument("--force", action="store_true", help="Ignore fresh cache entries and refetch")
    fetch_parser.add_argument("--dry-run", action="store_true", help="Print the selected sources without fetching them")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command in {None, "list"}:
        for source in select_sources(limit=getattr(args, "limit", None)):
            print(f"{source.source_id}\t{source.source_group}\t{source.parser_strategy}\t{source.url}")
        return 0

    if args.command == "fetch":
        sources = select_sources(source_id=args.source_id, limit=args.limit)
        if not sources:
            raise SystemExit("No sources matched the requested selection")
        if args.dry_run:
            for source in sources:
                print(f"dry-run\t{source.source_id}\t{source.url}")
            return 0

        last_live_fetch_at: float | None = None
        for source in sources:
            result, last_live_fetch_at = fetch_source(source, force=args.force, last_live_fetch_at=last_live_fetch_at)
            print(
                json.dumps(
                    {
                        "source_id": result.source_id,
                        "cached": result.cached,
                        "cache_html_path": result.cache_html_path,
                        "cache_meta_path": result.cache_meta_path,
                        "body_sha256": result.body_sha256,
                    },
                    sort_keys=True,
                )
            )
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
