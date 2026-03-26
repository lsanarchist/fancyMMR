from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read_text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_build_workflow_runs_repo_ci_commands_on_python_3_12() -> None:
    workflow = read_text(".github/workflows/build.yml")

    assert "pull_request:" in workflow
    assert "workflow_dispatch:" in workflow
    assert "uses: actions/checkout@v5" in workflow
    assert "uses: actions/setup-python@v5" in workflow
    assert 'python-version: "3.12"' in workflow
    assert "cache: pip" in workflow
    assert "python -m pip install --upgrade pip" in workflow
    assert "pip install -r requirements.txt" in workflow
    assert "run: python -m pytest" in workflow
    assert "run: python src/build_artifacts.py" in workflow
    assert "run: python src/build_site.py" in workflow


def test_pages_workflow_uses_custom_pages_actions_with_deploy_permissions() -> None:
    workflow = read_text(".github/workflows/pages.yml")

    assert "workflow_dispatch:" in workflow
    assert "uses: actions/configure-pages@v5" in workflow
    assert "uses: actions/upload-pages-artifact@v4" in workflow
    assert "path: ./site" in workflow
    assert "uses: actions/deploy-pages@v4" in workflow
    assert "name: github-pages" in workflow
    assert "url: ${{ steps.deployment.outputs.page_url }}" in workflow
    assert "permissions:" in workflow
    assert "contents: read" in workflow
    assert "pages: write" in workflow
    assert "id-token: write" in workflow
    assert "needs: build" in workflow
    assert "group: pages" in workflow
    assert "cancel-in-progress: true" in workflow


def test_generated_readme_documents_ci_and_pages_contract() -> None:
    readme = read_text("README.md")

    assert "Local CI-equivalent commands:" in readme
    assert "python -m pytest" in readme
    assert "python src/build_artifacts.py" in readme
    assert "python src/build_site.py" in readme
    assert "Python 3.12" in readme
    assert ".github/workflows/build.yml" in readme
    assert ".github/workflows/pages.yml" in readme
    assert "GitHub Actions" in readme
