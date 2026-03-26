from __future__ import annotations

from fancymmr_build.aggregate import load_visible_sample, summarize_visible_sample, write_summary_outputs
from fancymmr_build.charts import configure_matplotlib, render_all_charts
from fancymmr_build.readme_builder import write_readme
from fancymmr_build.validation import write_pipeline_manifest, write_validation_outputs


def main() -> None:
    configure_matplotlib()
    visible_sample = load_visible_sample()
    summary = summarize_visible_sample(visible_sample)
    metrics = write_summary_outputs(summary)
    validation_report, source_coverage_report, source_pipeline_diagnostics_report = write_validation_outputs(summary)
    render_all_charts(summary)
    write_readme(metrics)
    write_pipeline_manifest(
        summary,
        metrics,
        validation_report,
        source_coverage_report,
        source_pipeline_diagnostics_report,
    )


if __name__ == "__main__":
    main()
