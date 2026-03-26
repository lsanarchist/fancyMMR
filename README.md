# TrustMRR visible-sample research

Independent, GitHub-ready packaging of a **visible public sample** of startups with `Revenue (30d) >= $5,000`.

## What is in scope

- Startup-level visible sample
- Category, business-model, GTM, and revenue-band summaries
- Publication-grade charts in both PNG and SVG
- Reproducible build script
- Methodology, data notes, and release checklist

## Key takeaways

- Visible sample size: **229 startups**
- Total visible 30-day revenue: **$24.30M**
- Median visible 30-day revenue: **$19.0k**
- Top 10 startups capture **70.4%** of visible revenue
- The largest category is **E-commerce**, accounting for **45.8%** of visible revenue with **5.2%** of visible startups

## Main charts

### Category share map
![Category share map](charts/category_share_map.png)

### Top categories by visible revenue
![Top categories by revenue](charts/top_categories_revenue.png)

### Category over-index vs representation
![Category over-index](charts/category_over_index.png)

### Business-model and GTM composition
![Business-model and GTM composition](charts/model_mix.png)

### Distribution and concentration
![Distribution and concentration](charts/distribution_and_concentration.png)

## Repository layout

```text
.
├── charts/
├── data/
├── docs/
│   └── methodology.md
├── src/
│   └── build_artifacts.py
├── CHANGELOG.md
├── DATA-NOTICE.md
├── LICENSE-CODE-MIT.txt
├── RELEASE_CHECKLIST.md
└── requirements.txt
```

## Method note

This repository is based on a **source-derived visible sample**, not a full platform export. The `biz_model` and `gtm_model` fields are heuristic labels. See [docs/methodology.md](docs/methodology.md) for details.

## Rebuild

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/build_artifacts.py
```

## Publishing note

I did **not** hard-code a final repository license into this bundle because the repo mixes original code/docs with source-derived data. Use:

- `LICENSE-CODE-MIT.txt` as a starting point for code/docs if that is your final choice
- `DATA-NOTICE.md` to preserve the scope and attribution caveat
- `RELEASE_CHECKLIST.md` before you publish
