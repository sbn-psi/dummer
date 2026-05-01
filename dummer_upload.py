#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dummer.args import add_summary_args, add_upload_args
from dummer.console import TimestampedArgumentParser, log
from dummer.runtime_config import DEFAULTS, UPLOAD_RUNTIME_KEYS, load_config_defaults, log_effective_configuration
from dummer.workflow import (
    build_upload_options_from_namespace,
    filter_reconcile_for_path_component,
    load_reconcile_artifacts,
    upload_from_reconcile_result,
)


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    defaults = load_config_defaults(args)
    p = TimestampedArgumentParser(description="Upload pending directories from local and processed state files")
    p.add_argument("--local-path", default=defaults["local_path"], help="Root path containing the local files to upload")
    p.add_argument("--local-state", default=defaults["local_state"])
    p.add_argument("--processed-state", default=defaults["processed_state"])
    p.add_argument("--path-filter", default=defaults["path_filter"], help="Optional path component filter (e.g. 2026)")
    add_summary_args(p, defaults)
    add_upload_args(p, defaults)
    ns = p.parse_args(args)
    if not ns.local_state:
        p.error("--local-state is required unless DUMMER_LOCAL_STATE is set")
    if not ns.processed_state:
        p.error("--processed-state is required unless DUMMER_PROCESSED_STATE is set")
    log_effective_configuration(
        ns,
        (
            "local_state",
            "processed_state",
            "path_filter",
            "summary_anchor_component",
            *UPLOAD_RUNTIME_KEYS,
            "threads",
            "script_dir",
            "log_level",
            "interactive",
            "max_dirs",
            "loop",
        ),
    )

    local_state = Path(ns.local_state)
    processed_state = Path(ns.processed_state)
    if not local_state.exists():
        p.error(f"Local state file not found: {local_state}")
    artifacts = load_reconcile_artifacts(
        local_state,
        processed_state,
        summary_anchor_component=ns.summary_anchor_component,
    )
    reconciled, summary = filter_reconcile_for_path_component(artifacts.result, artifacts.summary, ns.path_filter)
    if ns.path_filter:
        log(f"Applied path filter {ns.path_filter}; {len(reconciled.pending)} pending directories match.")
    log(f"Reconcile step found {len(reconciled.pending)} pending directories.")

    try:
        return upload_from_reconcile_result(
            reconciled,
            processed_state,
            build_upload_options_from_namespace(ns),
            script_name="dummer_upload",
            reconciliation_summary=summary,
        )
    except ValueError as exc:
        p.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
