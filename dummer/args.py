from __future__ import annotations

import argparse
from typing import Mapping


def add_path_filter_args(parser: argparse.ArgumentParser, defaults: Mapping[str, object]) -> None:
    parser.add_argument("--path-filter", default=defaults.get("path_filter"), help="Optional path component filter (e.g. 2026)")
    parser.add_argument(
        "--path-filter-depth",
        type=int,
        default=defaults.get("path_filter_depth"),
        help="Optional path component depth for crawl-mode sibling pruning",
    )
    parser.add_argument(
        "--crawl-min-depth",
        type=int,
        default=defaults.get("crawl_min_depth"),
        help="Optional minimum relative directory depth for filesystem crawl inventory",
    )
    parser.add_argument(
        "--crawl-max-depth",
        type=int,
        default=defaults.get("crawl_max_depth"),
        help="Optional maximum relative directory depth for filesystem crawl inventory",
    )


def add_summary_args(parser: argparse.ArgumentParser, defaults: Mapping[str, object]) -> None:
    parser.add_argument(
        "--summary-anchor-component",
        type=int,
        default=defaults.get("summary_anchor_component"),
        help="Optional path component index used as the reconciliation summary anchor",
    )


def add_upload_args(parser: argparse.ArgumentParser, defaults: Mapping[str, object]) -> None:
    parser.add_argument("--bundle", default=defaults.get("bundle"))
    parser.add_argument("--prefix", default=defaults.get("prefix"))
    parser.add_argument("--config", default=defaults.get("config"))
    parser.add_argument("--name", default=defaults.get("name"))
    parser.add_argument("--dum-binary", default=defaults.get("dum_binary"))
    parser.add_argument("--threads", type=int, default=defaults.get("threads"))
    parser.add_argument("--report-dir", default=defaults.get("report_dir"))
    parser.add_argument("--pipeline-report-dir", default=defaults.get("pipeline_report_dir"))
    parser.add_argument("--script-dir", default=defaults.get("script_dir"))
    parser.add_argument("--log-level", default=defaults.get("log_level"))
    parser.add_argument(
        "--interactive",
        action="store_true",
        default=defaults.get("interactive"),
        help="Run DUM on a pseudo-terminal and stream its live interactive output",
    )
    parser.add_argument(
        "--direct-file-list-upload",
        action="store_true",
        default=defaults.get("direct_file_list_upload"),
        help="Pass each pending directory's direct files to DUM instead of the directory path",
    )
    parser.add_argument(
        "--direct-file-list-batch-size",
        type=int,
        default=defaults.get("direct_file_list_batch_size"),
        help="Maximum number of direct file paths to pass to DUM per command in direct file-list mode",
    )
    parser.add_argument("--max-dirs", type=int, default=defaults.get("max_dirs"), help="How many pending dirs to process this run")
    parser.add_argument(
        "-L",
        "--loop",
        action="store_true",
        default=defaults.get("loop"),
        help="Process all pending dirs until failure or exhaustion",
    )
