# Methodology

## Scope

This repository packages a **visible public sample** of startups from public TrustMRR pages where `Revenue (30d) >= $5,000`. It is intended for research, not as an official platform export or a full platform export.

## Inclusion rule

A startup is included when a visible public row in the source sample showed a 30-day revenue value of at least $5,000.

## Metrics

- **Startup share** = category startup count / total visible startups
- **Revenue share** = category visible revenue / total visible revenue
- **Performance index** = revenue share / startup share
- **Revenue concentration curve** sorts startups by `revenue_30d` descending and plots cumulative revenue captured by the top X% of startups

## Derived labels

Two columns are heuristic and should be treated as analyst judgments rather than first-party platform taxonomy:

- `biz_model`
- `gtm_model`

For the staged live source pipeline, deterministic override maps and alias rules live under `data/source_pipeline/overrides/`; see `docs/source_pipeline_overrides.md`.

## Current pipeline shape

- `python src/build_all.py --limit 1` fetches and stages live public-page outputs under `data/source_pipeline/` for smoke verification
- `python src/build_artifacts.py` rebuilds the currently published analytics bundle from the checked-in `data/visible_sample.csv` seed dataset
- `python src/build_site.py` turns the published analytics bundle into the static `site/` output that GitHub Pages serves without a runtime server
- `.github/workflows/build.yml` and `.github/workflows/pages.yml` pin Python 3.12 and run the deterministic local publication path on GitHub-hosted runners

## Validation and provenance artifacts

The published bundle keeps machine-readable validation and provenance outputs alongside the charts and summary tables:

- `data/validation_report.json`
- `data/source_coverage_report.json`
- `data/pipeline_manifest.json`

Those files describe the current seed-bundle validation status, the visible source-page coverage, and the generated outputs that belong to the current publication build.

## Publication and licensing shape

- `LICENSE-CODE-MIT.txt` covers the original code and original documentation in this repository
- `DATA-NOTICE.md` remains the publication notice for the source-derived data bundle and generated artifacts such as CSV/JSON outputs, charts, and the static site
- The repository intentionally keeps those scopes separate instead of presenting the mixed-content bundle as one blanket open-source data license
- The repository intentionally does **not** ship a blanket root `LICENSE` file, because a single detected license badge on GitHub would overstate the rights granted for the mixed code/data bundle
- Any public republication should preserve the visible-sample framing, the methodology caveats, and the data notice together

## Limitations

1. This is not a full database export and does not claim platform-wide coverage.
2. Values reflect the visible sample captured for this research bundle.
3. Category/business-model/GTM labels can simplify messy real-world companies.
4. Some source pages may have changed since the sample was assembled.
5. Any public republication should preserve the caveat that this is a **source-derived visible sample**.

## Rebuild

From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/build_artifacts.py
python src/build_site.py
python src/build_all.py --limit 1
```
