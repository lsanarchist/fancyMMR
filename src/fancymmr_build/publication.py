from __future__ import annotations

import json
from pathlib import Path

from .config import BUILD_PATHS, DEFAULT_PUBLICATION_DATASET, PUBLICATION_INPUT_MANIFEST
from .schemas import PublicationInput


PUBLICATION_DATASET_COLUMNS = [
    "name",
    "category",
    "revenue_30d",
    "biz_model",
    "gtm_model",
    "source_url",
    "revenue_band",
]


def default_publication_input_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "dataset_path": DEFAULT_PUBLICATION_DATASET,
        "dataset_kind": "seed_visible_sample",
        "source_label": "Checked-in seed visible-sample dataset",
        "expected_source_count": None,
        "selected_source_count": None,
        "promoted_from_visible_rows_path": None,
        "source_pipeline_validation_report_path": None,
        "source_pipeline_override_report_path": None,
        "source_pipeline_duplicates_report_path": None,
        "source_pipeline_run_manifest_path": None,
        "promoted_at": None,
        "staged_visible_rows_sha256": None,
        "promoted_dataset_sha256": None,
        "promotion_gate_summary": None,
    }


def publication_input_manifest_path(root: Path | None = None) -> Path:
    resolved_root = BUILD_PATHS.root if root is None else root
    return resolved_root / PUBLICATION_INPUT_MANIFEST


def read_publication_input(root: Path | None = None) -> PublicationInput:
    resolved_root = BUILD_PATHS.root if root is None else root
    manifest_path = publication_input_manifest_path(resolved_root)
    payload = default_publication_input_payload()
    if manifest_path.exists():
        payload.update(json.loads(manifest_path.read_text(encoding="utf-8")))
    else:
        write_publication_input_payload(resolved_root, payload)

    dataset_path_str = str(payload["dataset_path"])
    return PublicationInput(
        manifest_path=manifest_path,
        dataset_path=resolved_root / dataset_path_str,
        dataset_path_str=dataset_path_str,
        dataset_kind=str(payload["dataset_kind"]),
        source_label=str(payload["source_label"]),
        expected_source_count=_optional_int(payload.get("expected_source_count")),
        selected_source_count=_optional_int(payload.get("selected_source_count")),
        promoted_from_visible_rows_path=_optional_string(payload.get("promoted_from_visible_rows_path")),
        source_pipeline_validation_report_path=_optional_string(
            payload.get("source_pipeline_validation_report_path")
        ),
        source_pipeline_override_report_path=_optional_string(
            payload.get("source_pipeline_override_report_path")
        ),
        source_pipeline_duplicates_report_path=_optional_string(
            payload.get("source_pipeline_duplicates_report_path")
        ),
        source_pipeline_run_manifest_path=_optional_string(payload.get("source_pipeline_run_manifest_path")),
        promoted_at=_optional_string(payload.get("promoted_at")),
        staged_visible_rows_sha256=_optional_string(payload.get("staged_visible_rows_sha256")),
        promoted_dataset_sha256=_optional_string(payload.get("promoted_dataset_sha256")),
        promotion_gate_summary=_optional_mapping(payload.get("promotion_gate_summary")),
    )


def write_publication_input_payload(root: Path, payload: dict[str, object]) -> Path:
    manifest_path = publication_input_manifest_path(root)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return manifest_path


def _optional_mapping(value: object) -> dict[str, object] | None:
    if value is None:
        return None
    return dict(value)


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)
