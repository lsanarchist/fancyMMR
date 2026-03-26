# Source-Pipeline Overrides

## Purpose

The phase-2 source pipeline stages live public-page rows under `data/source_pipeline/` before those rows replace the published seed bundle. Two heuristic fields remain analyst judgments rather than first-party source data:

- `biz_model`
- `gtm_model`

To keep those labels deterministic, the repo stores tracked override maps instead of inferring them ad hoc during each run.

## Key format

Override keys use:

```text
<source_url>::<canonical_slug>
```

- `source_url` is the public TrustMRR listing page that surfaced the startup
- `canonical_slug` is a normalized startup-name slug built from Unicode-normalized, ASCII-folded, case-folded text with non-alphanumeric runs collapsed to `-`

Example:

```text
https://trustmrr.com/category/ai::rezi
```

## Files

- `data/source_pipeline/overrides/biz_model_overrides.json`
- `data/source_pipeline/overrides/gtm_model_overrides.json`
- `data/source_pipeline/overrides/canonical_slug_aliases.json`

The alias file exists for explicit name drift cases where the current live listing name no longer matches the seed-bundle name exactly. For example, `Parakeet Chat` can resolve to the older canonical slug `parakeet`.

## Update rules

1. Prefer exact key matches first.
2. Add aliases only for observed, reviewable name drift.
3. Keep overrides deterministic and reviewable; do not add fuzzy matching at runtime.
4. If a live row remains unmapped, surface it in the staged override report instead of guessing.

## Duplicate handling

The source pipeline also treats canonical-slug collisions across multiple detail URLs as suspicious duplicate candidates. Those collisions are surfaced in staged reports for review, not silently merged.
