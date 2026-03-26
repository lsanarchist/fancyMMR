# Source-Pipeline Overrides

These files make the phase-2 heuristic layer explicit and reproducible.

## Files

- `biz_model_overrides.json`
  Canonical-slug keyed `biz_model` assignments.
- `gtm_model_overrides.json`
  Canonical-slug keyed `gtm_model` assignments.
- `canonical_slug_aliases.json`
  Maps observed live slugs back to the canonical slug used by the checked-in visible sample.

## Key format

Overrides use:

`<source_url>::<canonical_slug>`

This keeps the initial mapping deterministic even before later dedupe/merge work decides how to reconcile the same startup across multiple source pages.

## Resolution order

1. Build a canonical slug from the startup name.
2. Build a detail-path slug from the parsed startup URL.
3. Apply any alias entry that remaps an observed slug to the canonical seed slug.
4. Look up explicit `biz_model` and `gtm_model` overrides.
5. If a visible row is still unmapped, keep the row and surface a validation warning instead of silently guessing.

## Duplicate policy

Duplicate handling is warning-only in phase 2.

- If the same canonical slug appears across multiple detail URLs, the pipeline writes `processed/suspicious_duplicates.json`.
- Those rows are not merged automatically yet.
- A later task can decide whether they should collapse into one visible-sample record.
