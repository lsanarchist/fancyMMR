# Methodology

## Scope

This repository packages a **visible public sample** of startups from public TrustMRR pages where `Revenue (30d) >= $5,000`. It is intended for research, not as an official platform export.

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
```
