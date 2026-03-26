# Source Pipeline Overrides

## Purpose

The phase-2 source pipeline emits `biz_model` and `gtm_model` as analyst heuristics, not first-party truth. To keep those judgments reproducible, the live pipeline resolves them from tracked override files instead of scattering one-off label logic through the parser.

## Files

- `data/source_pipeline/overrides/biz_model_overrides.json`
- `data/source_pipeline/overrides/gtm_model_overrides.json`
- `data/source_pipeline/overrides/canonical_slug_aliases.json`

## Key format

Override keys use:

`<source_url>::<canonical_slug>`

Example:

`https://trustmrr.com/category/ai::ai-interview-copilot`

## Canonical slug rules

1. Start from the normalized startup name slug.
2. If `canonical_slug_aliases.json` defines an alias for the observed slug on that source page, use the alias instead.
3. Keep the raw detail-page slug separately for debugging and drift review.

This matches the current TrustMRR listing pattern where cards link to `/startup/<slug>` detail pages on pages such as `https://trustmrr.com/category/ai` and `https://trustmrr.com/special-category/openclaw`.

## Duplicate review

The pipeline writes `data/source_pipeline/processed/suspicious_duplicates.json` whenever the same canonical slug appears on multiple distinct detail URLs. That report is warning-level review material, not silent dedupe.

## Current scope

These override files only affect the staged phase-2 outputs under `data/source_pipeline/`. The published charts, README, and Pages site still use the checked-in seed bundle until a later task explicitly promotes the live pipeline output.
