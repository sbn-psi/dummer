from __future__ import annotations

import argparse
import os
from pathlib import Path

from .console import log


DEFAULTS: dict[str, object] = {
    "local_path": None,
    "local_manifest": None,
    "local_root": None,
    "processed_crawl": None,
    "processed_manifest": None,
    "processed_root": None,
    "processed_s3_bucket": None,
    "processed_s3_prefix": None,
    "processed_s3_region": None,
    "processed_s3_known_dirs_file": None,
    "processed_s3_known_dirs_workers": 10,
    "processed_s3_resume_from_state": False,
    "processed_s3_resume_cluster_depth": 2,
    "processed_s3_max_retries": 10,
    "processed_s3_retry_delay_seconds": 2.0,
    "path_filter": None,
    "path_filter_depth": None,
    "crawl_min_depth": None,
    "crawl_max_depth": None,
    "include_hidden": False,
    "summary_anchor_component": None,
    "bundle": None,
    "prefix": None,
    "config": None,
    "name": None,
    "dum_binary": "/usr/local/bin/pds-ingress-client",
    "threads": 12,
    "report_dir": None,
    "pipeline_report_dir": None,
    "dum_manifest_store": None,
    "script_dir": ".",
    "log_level": "warn",
    "interactive": False,
    "direct_file_list_upload": False,
    "direct_file_list_batch_size": 500,
    "local_state": None,
    "processed_state": None,
    "max_dirs": 1,
    "loop": False,
}

ENV_VAR_NAMES: dict[str, str] = {
    "local_path": "DUMMER_LOCAL_PATH",
    "local_manifest": "DUMMER_LOCAL_MANIFEST",
    "local_root": "DUMMER_LOCAL_ROOT",
    "processed_crawl": "DUMMER_PROCESSED_CRAWL",
    "processed_manifest": "DUMMER_PROCESSED_MANIFEST",
    "processed_root": "DUMMER_PROCESSED_ROOT",
    "processed_s3_bucket": "DUMMER_PROCESSED_S3_BUCKET",
    "processed_s3_prefix": "DUMMER_PROCESSED_S3_PREFIX",
    "processed_s3_region": "DUMMER_PROCESSED_S3_REGION",
    "processed_s3_known_dirs_file": "DUMMER_PROCESSED_S3_KNOWN_DIRS_FILE",
    "processed_s3_known_dirs_workers": "DUMMER_PROCESSED_S3_KNOWN_DIRS_WORKERS",
    "processed_s3_resume_from_state": "DUMMER_PROCESSED_S3_RESUME_FROM_STATE",
    "processed_s3_resume_cluster_depth": "DUMMER_PROCESSED_S3_RESUME_CLUSTER_DEPTH",
    "processed_s3_max_retries": "DUMMER_PROCESSED_S3_MAX_RETRIES",
    "processed_s3_retry_delay_seconds": "DUMMER_PROCESSED_S3_RETRY_DELAY_SECONDS",
    "path_filter": "DUMMER_PATH_FILTER",
    "path_filter_depth": "DUMMER_PATH_FILTER_DEPTH",
    "crawl_min_depth": "DUMMER_CRAWL_MIN_DEPTH",
    "crawl_max_depth": "DUMMER_CRAWL_MAX_DEPTH",
    "include_hidden": "DUMMER_INCLUDE_HIDDEN",
    "summary_anchor_component": "DUMMER_SUMMARY_ANCHOR_COMPONENT",
    "bundle": "DUMMER_BUNDLE",
    "prefix": "DUMMER_PREFIX",
    "config": "DUMMER_CONFIG",
    "name": "DUMMER_NAME",
    "dum_binary": "DUMMER_DUM_BINARY",
    "threads": "DUMMER_THREADS",
    "report_dir": "DUMMER_REPORT_DIR",
    "pipeline_report_dir": "DUMMER_PIPELINE_REPORT_DIR",
    "dum_manifest_store": "DUMMER_DUM_MANIFEST_STORE",
    "script_dir": "DUMMER_SCRIPT_DIR",
    "log_level": "DUMMER_LOG_LEVEL",
    "interactive": "DUMMER_INTERACTIVE",
    "direct_file_list_upload": "DUMMER_DIRECT_FILE_LIST_UPLOAD",
    "direct_file_list_batch_size": "DUMMER_DIRECT_FILE_LIST_BATCH_SIZE",
    "local_state": "DUMMER_LOCAL_STATE",
    "processed_state": "DUMMER_PROCESSED_STATE",
    "max_dirs": "DUMMER_MAX_DIRS",
    "loop": "DUMMER_LOOP",
}

UPLOAD_RUNTIME_KEYS = (
    "local_path",
    "prefix",
    "bundle",
    "config",
    "name",
    "dum_binary",
    "report_dir",
    "pipeline_report_dir",
)


def parse_bool(raw: str) -> bool:
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"Could not parse boolean value '{raw}'")


def coerce_config_value(dest: str, raw: str) -> object:
    if dest in {
        "processed_s3_resume_from_state",
        "interactive",
        "direct_file_list_upload",
        "include_hidden",
        "loop",
    }:
        return parse_bool(raw)
    if dest in {
        "processed_s3_known_dirs_workers",
        "processed_s3_resume_cluster_depth",
        "path_filter_depth",
        "crawl_min_depth",
        "crawl_max_depth",
        "summary_anchor_component",
        "threads",
        "max_dirs",
        "direct_file_list_batch_size",
        "processed_s3_max_retries",
    }:
        return int(raw)
    if dest == "processed_s3_retry_delay_seconds":
        return float(raw)
    value = raw.strip()
    return value or None


def read_dotenv(dotenv_path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not dotenv_path.is_file():
        return values

    for line in dotenv_path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def resolve_dotenv_path(argv: list[str]) -> Path | None:
    cli_script_dir = None
    for idx, arg in enumerate(argv):
        if arg == "--script-dir" and idx + 1 < len(argv):
            cli_script_dir = Path(argv[idx + 1])
            break

    candidates: list[Path] = []
    if cli_script_dir is not None:
        candidates.append(cli_script_dir / ".env")
    candidates.append(Path.cwd() / ".env")
    default_script_dir = Path(str(DEFAULTS["script_dir"]))
    if default_script_dir != Path.cwd():
        candidates.append(default_script_dir / ".env")

    seen: set[Path] = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except FileNotFoundError:
            resolved = candidate.absolute()
        if resolved in seen:
            continue
        seen.add(resolved)
        if candidate.is_file():
            return candidate
    return None


def load_config_defaults(argv: list[str]) -> dict[str, object]:
    defaults = dict(DEFAULTS)

    dotenv_values: dict[str, str] = {}
    dotenv_path = resolve_dotenv_path(argv)
    if dotenv_path is not None:
        dotenv_values = read_dotenv(dotenv_path)

    for dest, env_name in ENV_VAR_NAMES.items():
        raw = os.environ.get(env_name)
        if raw is None and dotenv_values:
            raw = dotenv_values.get(env_name)
        if raw is None:
            continue
        defaults[dest] = coerce_config_value(dest, raw)

    return defaults


def log_effective_configuration(ns: argparse.Namespace, keys: tuple[str, ...] | None = None) -> None:
    log("Resolved configuration:")
    for dest in keys or tuple(DEFAULTS):
        value = getattr(ns, dest)
        log(f"  --{dest.replace('_', '-')}: {value!r}")


def validate_required_runtime_settings(ns: argparse.Namespace) -> None:
    required = {
        "local_path": "--local-path / DUMMER_LOCAL_PATH",
        "config": "--config / DUMMER_CONFIG",
        "name": "--name / DUMMER_NAME",
        "dum_binary": "--dum-binary / DUMMER_DUM_BINARY",
        "report_dir": "--report-dir / DUMMER_REPORT_DIR",
        "pipeline_report_dir": "--pipeline-report-dir / DUMMER_PIPELINE_REPORT_DIR",
    }
    missing = [label for attr, label in required.items() if not getattr(ns, attr)]
    if missing:
        raise ValueError("Missing required configuration: " + ", ".join(missing))
