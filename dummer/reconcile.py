from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Dict, List

SummaryGroupComponent = tuple[str, int]


@dataclass(frozen=True)
class ReconcileItem:
    rel_dir: str
    local_count: int
    processed_count: int | None


@dataclass(frozen=True)
class ReconcileResult:
    pending: List[ReconcileItem]
    drift: List[ReconcileItem]


def reconcile(local_inventory: Dict[str, int], processed_inventory: Dict[str, int]) -> ReconcileResult:
    pending: List[ReconcileItem] = []
    drift: List[ReconcileItem] = []
    for rel_dir in sorted(local_inventory.keys(), key=lambda p: (-len(PurePosixPath(p).parts), p)):
        local_count = local_inventory[rel_dir]
        processed_count = processed_inventory.get(rel_dir)
        if processed_count is None or processed_count < local_count:
            pending.append(
                ReconcileItem(
                    rel_dir=rel_dir,
                    local_count=local_count,
                    processed_count=processed_count,
                )
            )
        elif processed_count > local_count:
            drift.append(
                ReconcileItem(
                    rel_dir=rel_dir,
                    local_count=local_count,
                    processed_count=processed_count,
                )
            )
    return ReconcileResult(pending=pending, drift=drift)


def _path_parts(rel_dir: str) -> tuple[str, ...]:
    return tuple(p for p in PurePosixPath(rel_dir).parts if p and p != "/")


def _normalize_group_components(
    group_components: tuple[SummaryGroupComponent, ...] | None,
    group_component_indices: tuple[int, ...],
) -> tuple[SummaryGroupComponent, ...]:
    if group_components is not None:
        return group_components
    return tuple((f"component_{idx}", idx) for idx in group_component_indices)


def _component_at(parts: tuple[str, ...], raw_idx: int) -> str:
    idx = raw_idx if raw_idx >= 0 else len(parts) + raw_idx
    if idx < 0 or idx >= len(parts):
        return ""
    return parts[idx]


def _extract_summary_key(
    rel_dir: str,
    summary_anchor_component: int | None,
    group_components: tuple[SummaryGroupComponent, ...],
) -> tuple[str, tuple[str, ...]]:
    parts = _path_parts(rel_dir)
    anchor = "all" if summary_anchor_component is None else _component_at(parts, summary_anchor_component)
    group_values = tuple(_component_at(parts, raw_idx) for _label, raw_idx in group_components)
    return anchor, group_values


def summarize_reconciliation(
    local_inventory: Dict[str, int],
    processed_inventory: Dict[str, int],
    *,
    summary_anchor_component: int | None = None,
    group_component_indices: tuple[int, ...] = (),
    group_components: tuple[SummaryGroupComponent, ...] | None = None,
) -> List[Dict[str, Any]]:
    normalized_group_components = _normalize_group_components(group_components, group_component_indices)
    grouped: dict[tuple[str, tuple[str, ...]], dict[str, int]] = defaultdict(
        lambda: {
            "processed_directories": 0,
            "total_directories": 0,
            "processed_files": 0,
            "total_files": 0,
        }
    )

    for rel_dir, local_count in local_inventory.items():
        key = _extract_summary_key(rel_dir, summary_anchor_component, normalized_group_components)

        processed_count = processed_inventory.get(rel_dir, 0)
        processed_files = max(0, min(processed_count, local_count))

        bucket = grouped[key]
        bucket["total_directories"] += 1
        bucket["total_files"] += local_count
        bucket["processed_files"] += processed_files
        if processed_count == local_count:
            bucket["processed_directories"] += 1

    summary: List[Dict[str, Any]] = []
    for anchor, group_values in sorted(grouped):
        bucket = grouped[(anchor, group_values)]
        total_directories = bucket["total_directories"]
        processed_directories = bucket["processed_directories"]
        total_files = bucket["total_files"]
        processed_files = bucket["processed_files"]
        bucket_summary: Dict[str, Any] = {
            "anchor": anchor,
            "directories": {
                "processed": processed_directories,
                "total": total_directories,
                "missing": total_directories - processed_directories,
            },
            "files": {
                "processed": processed_files,
                "total": total_files,
                "missing": total_files - processed_files,
            },
        }
        if normalized_group_components:
            bucket_summary["group"] = {
                label: value for (label, _idx), value in zip(normalized_group_components, group_values)
            }
        summary.append(bucket_summary)

    return summary
