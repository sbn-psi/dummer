#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from dummer.console import TimestampedArgumentParser, log
from dummer.reconcile import SummaryGroupComponent, reconcile, summarize_reconciliation
from dummer.state import read_state_file


def _parse_group_components(raw: str | None) -> tuple[SummaryGroupComponent, ...]:
    if not raw:
        return ()
    components: list[SummaryGroupComponent] = []
    for item in raw.split(","):
        stripped = item.strip()
        if not stripped:
            continue
        if ":" in stripped:
            label, index_raw = stripped.split(":", 1)
            label = label.strip()
            if not label:
                raise ValueError("missing group label")
            components.append((label, int(index_raw.strip())))
        else:
            index = int(stripped)
            components.append((f"component_{index}", index))
    return tuple(components)


def _summarize(
    local: dict[str, int],
    processed: dict[str, int],
    summary_anchor_component: int | None,
    group_components: tuple[SummaryGroupComponent, ...],
) -> None:
    for bucket in summarize_reconciliation(
        local,
        processed,
        summary_anchor_component=summary_anchor_component,
        group_components=group_components,
    ):
        group = bucket.get("group", {})
        group_text = "".join(f"\t{name}={value}," for name, value in group.items())
        log(
            f"anchor={bucket['anchor']},{group_text}"
            f"\tdirectories_processed={bucket['directories']['processed']},"
            f"\tdirectories_total={bucket['directories']['total']},"
            f"\tdirectories_missing={bucket['directories']['missing']},"
            f"\tfiles_processed={bucket['files']['processed']},"
            f"\tfiles_total={bucket['files']['total']},"
            f"\tfiles_missing={bucket['files']['missing']}"
        )


def main() -> int:
    p = TimestampedArgumentParser(description="Compare local and processed inventories")
    p.add_argument("--local-state", required=True)
    p.add_argument("--processed-state", required=True)
    p.add_argument(
        "--summary",
        action="store_true",
        help="Show summary counts instead of listing every pending directory",
    )
    p.add_argument(
        "--summary-anchor-component",
        type=int,
        default=None,
        help="Optional path component index used as the summary anchor",
    )
    p.add_argument(
        "--summary-group-components",
        default=None,
        help="Optional comma-separated path component indices or label:index pairs, e.g. 0,1 or collection:0,instrument:1",
    )
    ns = p.parse_args()

    local_state = Path(ns.local_state)
    processed_state = Path(ns.processed_state)

    if not local_state.exists():
        log(f"Error: local state file not found: {local_state}")
        return 1

    if not processed_state.exists():
        log(f"Error: processed state file not found: {processed_state}")
        return 1

    local = read_state_file(local_state)
    processed = read_state_file(processed_state)
    result = reconcile(local, processed)

    if ns.summary:
        try:
            group_components = _parse_group_components(ns.summary_group_components)
        except ValueError:
            log("Error: --summary-group-components must be a comma-separated list of integers or label:index pairs")
            return 1
        _summarize(local, processed, ns.summary_anchor_component, group_components)
    else:
        for item in result.pending:
            done = "none" if item.processed_count is None else str(item.processed_count)
            log(f"{item.rel_dir},\tlocal={item.local_count},\tprocessed={done}")
        for item in result.drift:
            log(f"DRIFT {item.rel_dir},\tlocal={item.local_count},\tprocessed={item.processed_count}")

    log(f"pending={len(result.pending)}")
    log(f"drift={len(result.drift)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
