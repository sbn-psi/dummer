#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from dummer.args import add_path_filter_args
from dummer.console import TimestampedArgumentParser, log
from dummer.workflow import InventoryBuildSpec, build_inventory_state


def _build_local_spec(ns: argparse.Namespace) -> InventoryBuildSpec:
    mode = "parse" if ns.local_manifest else "crawl"
    return InventoryBuildSpec(
        mode=mode,
        out_path=Path(ns.local_out),
        bundle_path=ns.local_path,
        manifest_path=ns.local_manifest,
        root=ns.local_root,
        path_filter_depth=ns.path_filter_depth,
        crawl_min_depth=ns.crawl_min_depth,
        crawl_max_depth=ns.crawl_max_depth,
        include_hidden=ns.include_hidden,
    )


def _build_processed_spec(ns: argparse.Namespace) -> InventoryBuildSpec:
    if ns.processed_manifest:
        mode = "parse"
    elif ns.processed_crawl:
        mode = "crawl"
    else:
        mode = "s3"
    return InventoryBuildSpec(
        mode=mode,
        out_path=Path(ns.processed_out),
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
        crawl_min_depth=ns.crawl_min_depth,
        crawl_max_depth=ns.crawl_max_depth,
        include_hidden=ns.include_hidden,
    )


def main() -> int:
    p = TimestampedArgumentParser(description="Build local and/or processed inventory state files for dummer")
    add_path_filter_args(p, {})

    p.add_argument("--local-out", default=None, help="Write the local inventory state file to this path")
    p.add_argument(
        "--local-manifest",
        default=None,
        help="Gzipped or plain-text file manifest for local parse mode",
    )
    p.add_argument("--local-path", default=None, help="Root path containing local files to inventory")
    p.add_argument(
        "--local-root",
        default=None,
        help="Leading manifest path prefix to strip before counting local parent directories",
    )
    p.add_argument("--processed-out", default=None, help="Write the processed inventory state file to this path")
    p.add_argument("--processed-crawl", default=None, help="Filesystem path to crawl for processed inventory")
    p.add_argument("--processed-manifest", default=None, help="Manifest file for processed parse mode")
    p.add_argument(
        "--processed-root",
        default=None,
        help="Leading path prefix to strip before counting processed parent directories",
    )
    p.add_argument("--processed-s3-bucket", default=None, help="Public S3 bucket to inspect for processed state")
    p.add_argument("--processed-s3-prefix", default=None, help="Directory-style S3 key prefix to list")
    p.add_argument("--processed-s3-region", default=None, help="Bucket region for the public S3 endpoint")
    p.add_argument(
        "--processed-s3-known-dirs-file",
        default=None,
        help="Known processed directories to count via exact-prefix S3 requests instead of a full prefix scan",
    )
    p.add_argument(
        "--processed-s3-known-dirs-workers",
        type=int,
        default=10,
        help="Concurrent workers for exact-prefix S3 counts when --processed-s3-known-dirs-file is provided",
    )
    p.add_argument(
        "--processed-s3-resume-from-state",
        action="store_true",
        help="Resume from the existing processed S3 state file by redoing the last path-component cluster",
    )
    p.add_argument(
        "--processed-s3-resume-cluster-depth",
        type=int,
        default=2,
        help="Path component depth used as the S3 resume cluster boundary",
    )
    p.add_argument("--processed-s3-max-retries", type=int, default=10)
    p.add_argument("--processed-s3-retry-delay-seconds", type=float, default=2.0)
    ns = p.parse_args()

    if not ns.local_out and not ns.processed_out:
        p.error("At least one of --local-out or --processed-out is required")

    if ns.local_out:
        if bool(ns.local_manifest) == bool(ns.local_path):
            p.error("When --local-out is set, choose exactly one local inventory input: --local-manifest or --local-path")
        try:
            count = build_inventory_state(_build_local_spec(ns), ns.path_filter)
        except (FileNotFoundError, RuntimeError, ValueError) as exc:
            log(f"Error: {exc}")
            return 1
        log(f"Local inventory wrote {count} directories to {ns.local_out}")

    if ns.processed_out:
        processed_sources = [ns.processed_manifest, ns.processed_crawl, ns.processed_s3_bucket]
        if sum(1 for source in processed_sources if source) != 1:
            p.error(
                "When --processed-out is set, choose exactly one processed source: "
                "--processed-manifest, --processed-crawl, or --processed-s3-bucket"
            )
        try:
            count = build_inventory_state(_build_processed_spec(ns), ns.path_filter)
        except (FileNotFoundError, RuntimeError, ValueError) as exc:
            log(f"Error: {exc}")
            return 1
        log(f"Processed inventory wrote {count} directories to {ns.processed_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
