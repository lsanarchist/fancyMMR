from __future__ import annotations

import io
import json
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.error import HTTPError

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import DEFAULT_FETCH_POLICY, FetchPaths, SourceConfig, load_source_registry
from src.fetch import FetchError, fetch_source, save_failure_snapshot


class FakeResponse:
    def __init__(self, url: str, body: bytes, content_type: str = "text/html", status: int = 200) -> None:
        self._url = url
        self._body = body
        self.status = status
        self.headers = self
        self._content_type = content_type

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def read(self) -> bytes:
        return self._body

    def geturl(self) -> str:
        return self._url

    def get_content_type(self) -> str:
        return self._content_type


def test_source_registry_is_seeded_from_current_public_source_pages() -> None:
    registry = load_source_registry()

    assert len(registry) == 30
    assert registry[0].url == "https://trustmrr.com/category/ai"
    assert registry[-1].source_group in {"category", "special-category"}
    assert any(source.parser_strategy == "trustmrr_special_category_listing" for source in registry)
    assert any(source.category_label == "E-commerce" for source in registry)


def test_fetch_source_caches_successful_html_response(tmp_path: Path) -> None:
    paths = FetchPaths(
        root=tmp_path,
        data_dir=tmp_path / "data",
        cache_dir=tmp_path / "data" / "fetch_cache",
        failure_snapshot_dir=tmp_path / "data" / "fetch_failures",
    )
    source = SourceConfig(
        source_id="category--ai",
        url="https://example.com/category/ai",
        parser_strategy="trustmrr_category_listing",
        category_slug="ai",
        category_label="AI",
        source_group="category",
    )
    calls = {"count": 0}

    def fake_opener(request, timeout):
        calls["count"] += 1
        return FakeResponse(request.full_url, b"<html>ok</html>")

    robots = {
        "robots_url": "https://example.com/robots.txt",
        "status_code": 200,
        "allowed": True,
        "crawl_delay_seconds": None,
        "request_rate": None,
        "effective_delay_seconds": 0.0,
    }

    first_result, last_fetch_at = fetch_source(
        source,
        paths=paths,
        policy=DEFAULT_FETCH_POLICY,
        opener=fake_opener,
        robots_resolver=lambda *args, **kwargs: robots,
        sleeper=lambda _: None,
        monotonic=lambda: 10.0,
    )
    second_result, _ = fetch_source(
        source,
        paths=paths,
        policy=DEFAULT_FETCH_POLICY,
        opener=lambda *args, **kwargs: pytest.fail("cache hit should not refetch"),
        robots_resolver=lambda *args, **kwargs: pytest.fail("cache hit should not re-read robots"),
        sleeper=lambda _: None,
        last_live_fetch_at=last_fetch_at,
        monotonic=lambda: 11.0,
    )

    assert calls["count"] == 1
    assert first_result.cached is False
    assert second_result.cached is True
    assert (tmp_path / first_result.cache_html_path).exists()
    assert (tmp_path / first_result.cache_meta_path).exists()


def test_fetch_source_saves_html_snapshot_on_http_error(tmp_path: Path) -> None:
    paths = FetchPaths(
        root=tmp_path,
        data_dir=tmp_path / "data",
        cache_dir=tmp_path / "data" / "fetch_cache",
        failure_snapshot_dir=tmp_path / "data" / "fetch_failures",
    )
    source = SourceConfig(
        source_id="category--ai",
        url="https://example.com/category/ai",
        parser_strategy="trustmrr_category_listing",
        category_slug="ai",
        category_label="AI",
        source_group="category",
    )

    def fake_opener(request, timeout):
        raise HTTPError(
            request.full_url,
            500,
            "server exploded",
            hdrs=None,
            fp=io.BytesIO(b"<html>broken</html>"),
        )

    robots = {
        "robots_url": "https://example.com/robots.txt",
        "status_code": 200,
        "allowed": True,
        "crawl_delay_seconds": None,
        "request_rate": None,
        "effective_delay_seconds": 0.0,
    }

    with pytest.raises(FetchError):
        fetch_source(
            source,
            paths=paths,
            policy=DEFAULT_FETCH_POLICY,
            opener=fake_opener,
            robots_resolver=lambda *args, **kwargs: robots,
            sleeper=lambda _: None,
            monotonic=lambda: 10.0,
        )

    failure_meta = tmp_path / "data" / "fetch_failures" / "category--ai.json"
    failure_html = tmp_path / "data" / "fetch_failures" / "category--ai.html"
    assert failure_meta.exists()
    assert failure_html.exists()
    snapshot = json.loads(failure_meta.read_text(encoding="utf-8"))
    assert snapshot["recorded_at"].endswith("Z")
    assert snapshot["status_code"] == 500
    assert snapshot["html_snapshot_path"] == "data/fetch_failures/category--ai.html"
    assert "server exploded" in snapshot["message"]
    assert "broken" in failure_html.read_text(encoding="utf-8")


def test_save_failure_snapshot_without_html_body_writes_metadata(tmp_path: Path) -> None:
    paths = FetchPaths(
        root=tmp_path,
        data_dir=tmp_path / "data",
        cache_dir=tmp_path / "data" / "fetch_cache",
        failure_snapshot_dir=tmp_path / "data" / "fetch_failures",
    )
    source = SourceConfig(
        source_id="special-category--openclaw",
        url="https://example.com/special-category/openclaw",
        parser_strategy="trustmrr_special_category_listing",
        category_slug="openclaw",
        category_label="OpenClaw (special)",
        source_group="special-category",
    )

    snapshot = save_failure_snapshot(
        source,
        error=RuntimeError("boom"),
        paths=paths,
        robots={"allowed": False},
        recorded_at="2026-03-27T00:00:00Z",
    )

    assert snapshot["html_snapshot_path"] is None
    assert snapshot["recorded_at"] == "2026-03-27T00:00:00Z"
    failure_meta = tmp_path / "data" / "fetch_failures" / "special-category--openclaw.json"
    assert failure_meta.exists()


def test_fetch_cli_help_runs() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "src.fetch", "--help"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "Polite public-page fetch tooling" in result.stdout
