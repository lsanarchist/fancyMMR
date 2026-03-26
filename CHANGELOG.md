# Changelog

## Unreleased
- Added deterministic GitHub Actions rebuild and GitHub Pages deployment workflows for the static `site/` bundle.
- Added a staged public-source fetch -> parse -> normalize -> validate pipeline under `data/source_pipeline/`, including tracked heuristic overrides and duplicate-review reporting.
- Finalized the publication contract: `LICENSE-CODE-MIT.txt` covers original code/docs, `DATA-NOTICE.md` stays attached to the source-derived data bundle and generated artifacts, and the repo intentionally does not publish a blanket root `LICENSE` file.

## v1.0.0
- Rebuilt the category share chart as a two-panel figure to fix the original outlier-driven label collision.
- Added publication-grade PNG and SVG chart exports for GitHub.
- Added processed summary tables for categories, business models, GTM models, and revenue bands.
- Added methodology, data dictionary, notice, release checklist, and a reproducible build script.
