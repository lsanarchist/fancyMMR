from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read_text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_publication_docs_keep_split_license_and_release_contract() -> None:
    readme = read_text("README.md")
    methodology = read_text("docs/methodology.md")
    checklist = read_text("RELEASE_CHECKLIST.md")
    changelog = read_text("CHANGELOG.md")
    data_notice = read_text("DATA-NOTICE.md")

    assert "top-level detected `LICENSE` file" in readme
    assert "GitHub license detection expects a standard root license file" in readme
    assert "docs/source_pipeline_overrides.md" in methodology
    assert "does **not** ship a blanket root `LICENSE` file" in methodology
    assert "Keep the deliberate no-root-`LICENSE` decision" in checklist
    assert "Review the draft release notes and published archive contents before you publish." in checklist
    assert "does not publish a blanket root `LICENSE` file" in changelog
    assert "not a full platform export" in readme
    assert "full platform export" in methodology
    assert "does not relicense the source-derived data bundle" in data_notice
