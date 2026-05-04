from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .args import add_path_filter_args, add_summary_args, add_upload_args
from .console import TimestampedArgumentParser, log
from .runtime_config import DEFAULTS, load_config_defaults, log_effective_configuration
from .workflow import (
    InventoryBuildSpec,
    build_inventory_state,
    build_upload_options_from_namespace,
    ensure_dir,
    filter_reconcile_for_path_component,
    load_reconcile_artifacts,
    upload_from_reconcile_result,
)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    defaults = load_config_defaults(argv)
    _clear_source_defaults_when_cli_source_is_set(
        defaults,
        argv,
        (
            ("--local-state", "local_state"),
            ("--local-manifest", "local_manifest"),
            ("--inventory-manifest", "local_manifest"),
        ),
    )
    _clear_source_defaults_when_cli_source_is_set(
        defaults,
        argv,
        (
            ("--processed-state", "processed_state"),
            ("--processed-manifest", "processed_manifest"),
            ("--processed-crawl", "processed_crawl"),
            ("--processed-s3-bucket", "processed_s3_bucket"),
        ),
    )
    p = TimestampedArgumentParser(
        prog="dummer",
        description="Build inventory state, reconcile it, and upload pending directories with DUM.",
    )
    p.add_argument("--local-state", default=defaults["local_state"], help="Reuse an existing local dir/count state file")
    p.add_argument("--local-manifest", "--inventory-manifest", dest="local_manifest", default=defaults["local_manifest"], help="Build local state from a file manifest")
    p.add_argument("--local-path", default=defaults["local_path"], help="Root path containing the local files to inventory and upload")
    p.add_argument("--local-root", "--inventory-manifest-root", dest="local_root", default=defaults["local_root"])
    p.add_argument("--processed-state", default=defaults["processed_state"], help="Reuse an existing processed dir/count state file")
    p.add_argument("--processed-manifest", default=defaults["processed_manifest"], help="Build processed state from a file manifest")
    p.add_argument("--processed-crawl", default=defaults["processed_crawl"], help="Build processed state by crawling this filesystem path")
    p.add_argument("--processed-root", default=defaults["processed_root"])
    p.add_argument("--processed-s3-bucket", default=defaults["processed_s3_bucket"], help="Build processed state by listing this public S3 bucket")
    p.add_argument("--processed-s3-prefix", default=defaults["processed_s3_prefix"])
    p.add_argument("--processed-s3-region", default=defaults["processed_s3_region"])
    p.add_argument("--processed-s3-known-dirs-file", default=defaults["processed_s3_known_dirs_file"])
    p.add_argument("--processed-s3-known-dirs-workers", type=int, default=defaults["processed_s3_known_dirs_workers"])
    p.add_argument(
        "--processed-s3-resume-from-state",
        action="store_true",
        default=defaults["processed_s3_resume_from_state"],
        help="Resume from the existing processed S3 state file by redoing the last path-component cluster",
    )
    p.add_argument(
        "--processed-s3-resume-cluster-depth",
        type=int,
        default=defaults["processed_s3_resume_cluster_depth"],
        help="Path component depth used as the S3 resume cluster boundary",
    )
    p.add_argument("--processed-s3-max-retries", type=int, default=defaults["processed_s3_max_retries"])
    p.add_argument(
        "--processed-s3-retry-delay-seconds",
        type=float,
        default=defaults["processed_s3_retry_delay_seconds"],
    )

    add_path_filter_args(p, defaults)
    add_summary_args(p, defaults)
    add_upload_args(p, defaults)

    return p.parse_args(argv)


def _clear_source_defaults_when_cli_source_is_set(
    defaults: dict[str, object],
    argv: list[str],
    options: tuple[tuple[str, str], ...],
) -> None:
    cli_selected = any(
        arg == flag or arg.startswith(f"{flag}=")
        for arg in argv
        for flag, _dest in options
    )
    if not cli_selected:
        return
    for _flag, dest in options:
        defaults[dest] = None


def _selected_source(ns: argparse.Namespace, names: tuple[str, ...], label: str) -> str:
    selected = [name for name in names if getattr(ns, name)]
    if len(selected) != 1:
        joined = ", ".join(f"--{name.replace('_', '-')}" for name in names)
        raise ValueError(f"Choose exactly one {label} source: {joined}")
    return selected[0]


def _selected_local_source(ns: argparse.Namespace) -> str:
    if ns.local_state and ns.local_manifest:
        raise ValueError("Choose only one prepared local inventory source: --local-state or --local-manifest")
    if ns.local_state:
        return "local_state"
    if ns.local_manifest:
        return "local_manifest"
    if ns.local_path:
        return "local_path"
    raise ValueError("Set --local-path, or provide --local-state/--local-manifest to reuse prepared local inventory")


def _generated_state_path(script_dir: Path, source: str, side: str) -> Path:
    if side == "local":
        return script_dir / "local_dirs.txt"
    if source == "processed_manifest":
        return script_dir / "processed_parse_dirs.txt"
    return script_dir / "processed_s3_dirs.txt"


def _local_build_spec(ns: argparse.Namespace, local_state: Path) -> InventoryBuildSpec:
    mode = "parse" if ns.local_manifest else "crawl"
    return InventoryBuildSpec(
        mode=mode,
        out_path=local_state,
        bundle_path=ns.local_path,
        manifest_path=ns.local_manifest,
        root=ns.local_root,
        path_filter_depth=ns.path_filter_depth,
    )


def _processed_build_spec(ns: argparse.Namespace, processed_state: Path) -> InventoryBuildSpec:
    if ns.processed_manifest:
        mode = "parse"
    elif ns.processed_crawl:
        mode = "crawl"
    else:
        mode = "s3"
    return InventoryBuildSpec(
        mode=mode,
        out_path=processed_state,
        bundle_path=ns.processed_crawl,
        manifest_path=ns.processed_manifest,
        root=ns.processed_root,
        s3_bucket=ns.processed_s3_bucket,
        s3_prefix=ns.processed_s3_prefix,
        s3_region=ns.processed_s3_region,
        s3_resume_from_state=ns.processed_s3_resume_from_state,
        s3_max_retries=ns.processed_s3_max_retries,
        s3_retry_delay_seconds=ns.processed_s3_retry_delay_seconds,
        s3_known_dirs_file=ns.processed_s3_known_dirs_file,
        s3_known_dirs_workers=ns.processed_s3_known_dirs_workers,
        s3_resume_cluster_depth=ns.processed_s3_resume_cluster_depth,
        path_filter_depth=ns.path_filter_depth,
    )


def main(argv: list[str] | None = None) -> int:
    ns = _parse_args(sys.argv[1:] if argv is None else argv)
    log_effective_configuration(ns)

    try:
        local_source = _selected_local_source(ns)
        processed_source = _selected_source(
            ns,
            ("processed_state", "processed_manifest", "processed_crawl", "processed_s3_bucket"),
            "processed",
        )
    except ValueError as exc:
        log(f"Error: {exc}")
        return 1

    script_dir = Path(ns.script_dir)
    ensure_dir(script_dir, "script directory")

    local_state = (
        Path(ns.local_state)
        if local_source == "local_state"
        else _generated_state_path(script_dir, local_source, "local")
    )
    processed_state = (
        Path(ns.processed_state)
        if processed_source == "processed_state"
        else _generated_state_path(script_dir, processed_source, "processed")
    )

    if local_source != "local_state":
        try:
            count = build_inventory_state(_local_build_spec(ns, local_state), ns.path_filter)
        except (FileNotFoundError, RuntimeError, ValueError) as exc:
            log(f"Error: {exc}")
            return 1
        log(f"Local inventory refreshed: {count} directories written to {local_state}")

    if processed_source == "processed_state":
        if not processed_state.exists():
            log(f"Processed state file not found at {processed_state}; reconcile will treat it as empty.")
    else:
        try:
            count = build_inventory_state(_processed_build_spec(ns, processed_state), ns.path_filter)
        except (FileNotFoundError, RuntimeError, ValueError) as exc:
            log(f"Error: {exc}")
            return 1
        log(f"Processed inventory refreshed: {count} directories written to {processed_state}")

    artifacts = load_reconcile_artifacts(
        local_state,
        processed_state,
        summary_anchor_component=ns.summary_anchor_component,
    )
    reconciled, summary = filter_reconcile_for_path_component(artifacts.result, artifacts.summary, ns.path_filter)
    if ns.path_filter:
        log(f"Applied path filter {ns.path_filter}; {len(reconciled.pending)} pending directories match.")
    log(f"Reconcile step found {len(reconciled.pending)} pending directories.")

    options = build_upload_options_from_namespace(ns)
    try:
        return upload_from_reconcile_result(
            reconciled,
            processed_state,
            options,
            script_name="dummer",
            reconciliation_summary=summary,
            summary_anchor_component=ns.summary_anchor_component,
        )
    except ValueError as exc:
        log(f"Error: {exc}")
        return 1
