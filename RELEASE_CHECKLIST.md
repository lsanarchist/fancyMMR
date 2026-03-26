# Release checklist

Use this checklist before drafting or publishing a tagged GitHub release.

## Deterministic build status
- [ ] Run `python -m pytest`.
- [ ] If you intend to publish a live-source-backed bundle, run `python src/promote_live_bundle.py --dry-run` first and confirm the promotion gates stay clean for a full staged run across `data/public_source_pages.csv` rather than a `--limit 1` smoke.
- [ ] If you intentionally want the published bundle to switch to the staged live-source dataset, run `python src/promote_live_bundle.py` before the rebuild commands below.
- [ ] Run `python src/build_artifacts.py`.
- [ ] Run `python src/build_site.py`.
- [ ] Optionally run `python src/build_all.py --limit 1` if you want the release notes to mention the latest staged live-source smoke.

## Hosted automation
- [ ] Confirm `.github/workflows/build.yml` is green on the release commit.
- [ ] Confirm `.github/workflows/pages.yml` is green on `main` after the release commit lands.
- [ ] Confirm the repository Pages source is set to **GitHub Actions**.

## Research quality
- [ ] Re-read `README.md` and confirm the positioning still matches the story you want to publish.
- [ ] Re-read `docs/methodology.md` and confirm the current pipeline status section still matches repo reality.
- [ ] Re-check the caveat that this is a **visible public sample**, not a full platform export.
- [ ] Spot-check a few `source_url` links from the dataset currently named in `data/publication_input.json`.

## Legal / attribution
- [ ] Confirm `LICENSE-CODE-MIT.txt` has the copyright holder and year you want for original code/docs.
- [ ] Review `DATA-NOTICE.md` and keep it if you publish source-derived data or generated artifacts.
- [ ] Keep the split publication notice explicit: code/docs under `LICENSE-CODE-MIT.txt`, source-derived data under `DATA-NOTICE.md`.
- [ ] Keep the deliberate no-root-`LICENSE` decision unless you intentionally want GitHub to present the whole repository as one detected license.
- [ ] Confirm you are comfortable publicly redistributing the processed sample.

## Release notes and GitHub polish
- [ ] Update `CHANGELOG.md` so the top section matches what the release is actually shipping.
- [ ] Add a short repository description.
- [ ] Set a social preview image. Suggested: `charts/category_share_map.png`.
- [ ] Pin the methodology link near the top of the README if you want extra rigor.
- [ ] Draft the GitHub release from the tag you want to publish.
- [ ] Review the draft release notes and published archive contents before you publish.
- [ ] Use generated release notes or write notes that match the top section of `CHANGELOG.md`.

## Optional cleanup
- [ ] Remove any charts or files you do not want in the public repo.
- [ ] Add your own author / contact details.
