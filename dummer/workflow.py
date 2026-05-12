from __future__ import annotations

import shlex
from copy import deepcopy
from argparse import Namespace
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from .console import log, write_raw
from .dum import build_command, build_direct_file_only_exclude_patterns, execute_command, parse_ingress_report
from .inventory import (
    crawl_inventory_to_state_file_with_options,
    parse_inventory_manifest_to_state_file,
    public_s3_inventory_to_state_file,
)
from .reconcile import ReconcileItem, ReconcileResult, reconcile, summarize_reconciliation
from .reporting import PipelineReportWriter
from .state import read_state_file, set_state_entry
from .utils import report_safe_name, sanitize_path


@dataclass(frozen=True)
class InventoryBuildSpec:
    mode: str
    out_path: Path
    bundle_path: str | None = None
    manifest_path: str | None = None
    root: str | None = None
    s3_bucket: str | None = None
    s3_prefix: str | None = None
    s3_region: str | None = None
    s3_resume_from_state: bool = False
    s3_max_retries: int = 10
    s3_retry_delay_seconds: float = 2.0
    s3_known_dirs_file: str | None = None
    s3_known_dirs_workers: int = 10
    s3_resume_cluster_depth: int = 2
    path_filter_depth: int | None = None
    crawl_min_depth: int | None = None
    crawl_max_depth: int | None = None


@dataclass(frozen=True)
class UploadOptions:
    local_path: str | None
    bundle: str | None
    prefix: str | None
    config: str
    name: str
    dum_binary: str
    threads: int
    report_dir: Path | None
    pipeline_report_dir: Path | None
    log_level: str
    max_dirs: int
    loop: bool
    interactive: bool = False


@dataclass(frozen=True)
class ReconcileArtifacts:
    local_inventory: dict[str, int]
    processed_inventory: dict[str, int]
    result: ReconcileResult
    summary: list[dict[str, Any]]


def ensure_dir(path: Path, label: str) -> None:
    path.mkdir(parents=True, exist_ok=True)
    if not path.is_dir():
        log(f"Error: Could not create {label} '{path}'")
        raise SystemExit(1)


def build_inventory_state(spec: InventoryBuildSpec, path_filter: str | None = None) -> int:
    if spec.mode == "crawl":
        if not spec.bundle_path:
            raise ValueError("A crawl path is required when mode is crawl")
        return crawl_inventory_to_state_file_with_options(
            spec.bundle_path,
            spec.out_path,
            path_filter=path_filter,
            path_filter_depth=spec.path_filter_depth,
            crawl_min_depth=spec.crawl_min_depth,
            crawl_max_depth=spec.crawl_max_depth,
        )

    if spec.mode == "parse":
        if not spec.manifest_path:
            raise ValueError("--manifest is required when mode is parse")
        return parse_inventory_manifest_to_state_file(
            spec.manifest_path,
            spec.out_path,
            path_filter,
            spec.root,
        )

    if spec.mode == "s3":
        if not spec.s3_bucket:
            raise ValueError("--s3-bucket is required when mode is s3")
        return public_s3_inventory_to_state_file(
            spec.s3_bucket,
            spec.out_path,
            prefix=spec.s3_prefix,
            path_filter=path_filter,
            bundle_root=spec.root,
            region=spec.s3_region,
            resume_from_state=spec.s3_resume_from_state,
            max_retries=spec.s3_max_retries,
            retry_delay_seconds=spec.s3_retry_delay_seconds,
            known_dirs_file=spec.s3_known_dirs_file,
            known_dirs_workers=spec.s3_known_dirs_workers,
            resume_cluster_depth=spec.s3_resume_cluster_depth,
        )

    raise ValueError(f"Unsupported inventory mode: {spec.mode}")


def load_reconcile_result(local_state: Path, processed_state: Path) -> ReconcileResult:
    return load_reconcile_artifacts(local_state, processed_state).result


def load_reconcile_artifacts(
    local_state: Path,
    processed_state: Path,
    *,
    summary_anchor_component: int | None = None,
) -> ReconcileArtifacts:
    local_inventory = read_state_file(local_state)
    processed_inventory = read_state_file(processed_state)
    result = reconcile(local_inventory, processed_inventory)
    summary = summarize_reconciliation(
        local_inventory,
        processed_inventory,
        summary_anchor_component=summary_anchor_component,
    )
    return ReconcileArtifacts(
        local_inventory=local_inventory,
        processed_inventory=processed_inventory,
        result=result,
        summary=summary,
    )


def filter_reconcile_for_path_component(
    reconciled: ReconcileResult,
    summary: list[dict[str, Any]],
    path_filter: str | None,
) -> tuple[ReconcileResult, list[dict[str, Any]]]:
    if not path_filter:
        return reconciled, summary

    filtered = ReconcileResult(
        pending=[item for item in reconciled.pending if path_filter in Path(item.rel_dir).parts],
        drift=[item for item in reconciled.drift if path_filter in Path(item.rel_dir).parts],
    )
    filtered_summary = [bucket for bucket in summary if bucket.get("anchor") == path_filter]
    if not filtered_summary:
        filtered_summary = summary
    return filtered, filtered_summary


def _drift_report_items(reconciled: ReconcileResult) -> list[dict[str, Any]]:
    return [
        {
            "rel_dir": item.rel_dir,
            "local_count": item.local_count,
            "processed_count": item.processed_count,
            "extra_processed_files": (item.processed_count or 0) - item.local_count,
        }
        for item in reconciled.drift
    ]


def _increment_completed_directory_in_summary(
    summary: list[dict[str, Any]],
    item: ReconcileItem,
    *,
    summary_anchor_component: int | None = None,
) -> None:
    if not summary:
        return

    anchor = "all"
    if summary_anchor_component is not None:
        parts = tuple(p for p in PurePosixPath(item.rel_dir).parts if p and p != "/")
        idx = summary_anchor_component if summary_anchor_component >= 0 else len(parts) + summary_anchor_component
        anchor = parts[idx] if 0 <= idx < len(parts) else ""

    old_processed_files = max(0, min(item.processed_count or 0, item.local_count))
    processed_file_delta = item.local_count - old_processed_files

    for bucket in summary:
        if bucket.get("anchor") != anchor:
            continue
        directories = bucket.get("directories", {})
        files = bucket.get("files", {})
        directories["processed"] = directories.get("processed", 0) + 1
        directories["missing"] = max(0, directories.get("missing", 0) - 1)
        files["processed"] = files.get("processed", 0) + processed_file_delta
        files["missing"] = max(0, files.get("missing", 0) - processed_file_delta)
        return


def _increment_processed_files_in_summary(
    summary: list[dict[str, Any]],
    item: ReconcileItem,
    new_processed_count: int,
    *,
    summary_anchor_component: int | None = None,
) -> None:
    if not summary:
        return

    anchor = "all"
    if summary_anchor_component is not None:
        parts = tuple(p for p in PurePosixPath(item.rel_dir).parts if p and p != "/")
        idx = summary_anchor_component if summary_anchor_component >= 0 else len(parts) + summary_anchor_component
        anchor = parts[idx] if 0 <= idx < len(parts) else ""

    old_processed_files = max(0, min(item.processed_count or 0, item.local_count))
    processed_file_delta = max(0, min(new_processed_count, item.local_count) - old_processed_files)
    if processed_file_delta == 0:
        return

    for bucket in summary:
        if bucket.get("anchor") != anchor:
            continue
        files = bucket.get("files", {})
        files["processed"] = files.get("processed", 0) + processed_file_delta
        files["missing"] = max(0, files.get("missing", 0) - processed_file_delta)
        return


def build_upload_options_from_namespace(ns: Namespace) -> UploadOptions:
    return UploadOptions(
        local_path=getattr(ns, "local_path", None),
        bundle=ns.bundle,
        prefix=ns.prefix,
        config=ns.config,
        name=ns.name,
        dum_binary=ns.dum_binary,
        threads=ns.threads,
        report_dir=Path(ns.report_dir) if ns.report_dir else None,
        pipeline_report_dir=Path(ns.pipeline_report_dir) if ns.pipeline_report_dir else None,
        log_level=ns.log_level,
        max_dirs=ns.max_dirs,
        loop=ns.loop,
        interactive=ns.interactive,
    )


def upload_from_reconcile_result(
    reconciled: ReconcileResult,
    processed_state: Path,
    options: UploadOptions,
    script_name: str = "dummer",
    reconciliation_summary: list[dict[str, Any]] | None = None,
    summary_anchor_component: int | None = None,
) -> int:
    current_reconciliation_summary = deepcopy(reconciliation_summary or [])
    if not reconciled.pending:
        log("No new, unprocessed directories found to process.")
        if reconciled.drift:
            log(f"Reconcile drift found {len(reconciled.drift)} directories with processed_count > local_count.")
        if options.pipeline_report_dir:
            ensure_dir(options.pipeline_report_dir, "pipeline report directory")
            writer = PipelineReportWriter(options.pipeline_report_dir, script_name)
            ctx = writer.new_context()
            ctx.status = "no_work_found"
            ctx.reconciliation_summary = deepcopy(current_reconciliation_summary)
            ctx.reconciliation_drift = _drift_report_items(reconciled)
            writer.write(ctx)
        return 0

    missing = []
    if not options.local_path:
        missing.append("--local-path / DUMMER_LOCAL_PATH")
    if not options.config:
        missing.append("--config / DUMMER_CONFIG")
    if not options.name:
        missing.append("--name / DUMMER_NAME")
    if not options.dum_binary:
        missing.append("--dum-binary / DUMMER_DUM_BINARY")
    if not options.report_dir:
        missing.append("--report-dir / DUMMER_REPORT_DIR")
    if not options.pipeline_report_dir:
        missing.append("--pipeline-report-dir / DUMMER_PIPELINE_REPORT_DIR")
    if missing:
        raise ValueError("Missing required upload configuration: " + ", ".join(missing))

    ensure_dir(options.report_dir, "report directory")
    ensure_dir(options.pipeline_report_dir, "pipeline report directory")
    writer = PipelineReportWriter(options.pipeline_report_dir, script_name)
    if reconciled.drift:
        log(f"Reconcile drift found {len(reconciled.drift)} directories with processed_count > local_count.")

    limit = len(reconciled.pending) if options.loop else max(1, options.max_dirs)
    partial_failures = 0

    for item in reconciled.pending[:limit]:
        ctx = writer.new_context()
        ctx.directory_processed = item.rel_dir
        ctx.reconciliation_summary = deepcopy(current_reconciliation_summary)
        ctx.reconciliation_drift = _drift_report_items(reconciled)

        log(f"Found candidate directory to process from reconcile step: {item.rel_dir}")

        full_path = sanitize_path(str(Path(options.local_path) / item.rel_dir))
        dum_prefix = sanitize_path(options.prefix or str(Path(options.local_path).parent))
        report_name = report_safe_name(item.rel_dir)
        report_path = options.report_dir / f"{report_name}.json"
        suffix = 1
        while report_path.exists():
            report_path = options.report_dir / f"{report_name}_{suffix}.json"
            suffix += 1

        ctx.pds_ingress_client_report_path = str(report_path)
        exclude_patterns = build_direct_file_only_exclude_patterns(
            full_path=full_path,
            bundle_prefix=dum_prefix,
        )

        command = build_command(
            dum_binary=options.dum_binary,
            log_level=options.log_level,
            bundle_prefix=dum_prefix,
            config_file=options.config,
            name_param=options.name,
            full_path=full_path,
            num_threads=options.threads,
            report_path=str(report_path),
            exclude_patterns=exclude_patterns,
        )
        ctx.command_executed = " ".join(shlex.quote(x) for x in command)

        log("Executing command:")
        log(ctx.command_executed)
        exit_code, output = execute_command(command, interactive=options.interactive)
        ctx.command_exit_code = exit_code
        if options.interactive:
            write_raw("\n")

        if exit_code == 0:
            log("Command finished successfully.")
            log("Validating upload success by parsing report...")
            validation = parse_ingress_report(report_path, expected_total_files=item.local_count)
            if validation:
                set_state_entry(processed_state, item.rel_dir, item.local_count)
                _increment_completed_directory_in_summary(
                    current_reconciliation_summary,
                    item,
                    summary_anchor_component=summary_anchor_component,
                )
                ctx.status = "processing_succeeded"
                ctx.reconciliation_summary = deepcopy(current_reconciliation_summary)
                log("Processing completed successfully.")
                writer.write(ctx)
                continue

            partial_processed_count = 0
            if not isinstance(validation, bool):
                partial_processed_count = min(validation.processed_count, item.local_count)
            previous_processed_count = item.processed_count or 0
            if 0 < partial_processed_count < item.local_count and partial_processed_count > previous_processed_count:
                set_state_entry(processed_state, item.rel_dir, partial_processed_count)
                _increment_processed_files_in_summary(
                    current_reconciliation_summary,
                    item,
                    partial_processed_count,
                    summary_anchor_component=summary_anchor_component,
                )
                ctx.reconciliation_summary = deepcopy(current_reconciliation_summary)
                partial_failures += 1
                ctx.status = "partial_processed"
                log(
                    "Partial upload progress recorded: "
                    f"processed={partial_processed_count}, local={item.local_count}. "
                    "Directory remains pending for a future reconcile."
                )
                writer.write(ctx)
                if options.loop:
                    continue
                return 1

            log("Error: Upload validation failed. Directory will be retried on next run.")
            ctx.status = "validation_failed"
            log("Validation failed - will retry on next run.")
            writer.write(ctx)
            return 1

        log(f"Error: Command failed with exit code {exit_code}")
        if output:
            if options.interactive:
                log("Command output was streamed above.")
            else:
                log("Command output:")
                write_raw(output if output.endswith("\n") else f"{output}\n")
        log(f"Directory '{item.rel_dir}' will be retried on the next run.")
        ctx.status = "processing_failed"
        log("Processing failed - will retry on next run.")
        writer.write(ctx)
        return 1

    if options.loop:
        if partial_failures:
            log(f"Completed loop with {partial_failures} partial directorie(s); pending work remains for next run.")
            return 1
        log("No new, unprocessed directories found to process.")
        ctx = writer.new_context()
        ctx.status = "no_work_found"
        ctx.reconciliation_summary = deepcopy(current_reconciliation_summary)
        ctx.reconciliation_drift = _drift_report_items(reconciled)
        writer.write(ctx)
    else:
        log(f"Processed {min(limit, len(reconciled.pending))} pending directorie(s) this run.")
    return 0
