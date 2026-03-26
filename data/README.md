# Data files

This directory contains the processed visible sample used in the charts and summaries.

## Files

- `visible_sample.csv` — startup-level visible sample. Columns:
  - `name`
  - `category`
  - `revenue_30d` (USD)
  - `biz_model` (heuristic)
  - `gtm_model` (heuristic)
  - `source_url`
  - `revenue_band` (derived during packaging)

- `category_summary.csv` — category-level aggregation with:
  - `startup_count`
  - `total_revenue`
  - `median_revenue`
  - `startup_share`
  - `revenue_share`
  - `performance_index` = revenue_share / startup_share

- `business_model_summary.csv` — startup count, total revenue, median revenue, startup share, and revenue share by `biz_model`.

- `gtm_model_summary.csv` — startup count, total revenue, median revenue, startup share, and revenue share by `gtm_model`.

- `revenue_band_summary.csv` — count and visible revenue split across revenue bands using left-inclusive buckets:
  - [5k, 10k)
  - [10k, 50k)
  - [50k, 100k)
  - [100k, 500k)
  - [500k, 1M)
  - [1M, +inf)

- `public_source_pages.csv` — unique public source pages referenced by the visible sample.

- `metrics.json` — top-line metrics used in the README.

## Important scope note

This is a **visible public sample**, not a full platform export. It should be treated as a derived research dataset built from public pages.
