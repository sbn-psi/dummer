from __future__ import annotations

import argparse
import json
import random
import secrets
import time
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Callable, Iterable, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
from xml.etree import ElementTree

from .console import ProgressHeartbeat, log
from .inventory import _candidate_rel_dir_from_path, _normalize_s3_prefix, _public_s3_base_url
from .runtime_config import DEFAULTS, load_config_defaults, log_effective_configuration
from .state import read_state_file
from .utils import now_iso8601_utc, report_safe_name, sanitize_path


DEFAULT_COMPARE_CHUNK_BYTES = 1024 * 1024


@dataclass(frozen=True)
class IntegrityOptions:
    enabled: bool = False
    run_probability: float = 1.0
    max_dirs: int | None = None
    max_files: int | None = None
    rel_dirs: tuple[str, ...] = ()
    report_dir: Path | None = None
    compare_chunk_bytes: int = DEFAULT_COMPARE_CHUNK_BYTES
    max_retries: int = 3
    retry_delay_seconds: float = 2.0


@dataclass(frozen=True)
class PublicS3Object:
    key: str


@dataclass
class FileCompareResult:
    rel_path: str
    s3_key: str
    status: str
    bytes_compared: int = 0
    error: str = ""


@dataclass
class DirectoryCompareResult:
    rel_dir: str
    status: str
    local_file_count: int = 0
    s3_file_count: int = 0
    checked_file_count: int = 0
    mismatches: list[FileCompareResult] = field(default_factory=list)
    missing_s3_files: list[str] = field(default_factory=list)
    extra_s3_keys: list[str] = field(default_factory=list)


@dataclass
class IntegrityRunResult:
    status: str
    report_path: Path | None = None
    directories_considered: int = 0
    directories_selected: int = 0
    files_checked: int = 0
    failures: int = 0


class RandomSource(Protocol):
    def random(self) -> float: ...

    def shuffle(self, x: list[str]) -> None: ...


def _parse_rel_dirs(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ()
    parts = []
    for item in raw.replace("\n", ",").split(","):
        cleaned = item.strip().strip("/")
        if cleaned:
            parts.append(cleaned)
    return tuple(parts)


def _positive_int_or_none(raw: object) -> int | None:
    if raw in (None, ""):
        return None
    value = int(raw)
    if value < 1:
        raise ValueError("value must be 1 or greater")
    return value


def _nonnegative_float(raw: object) -> float:
    value = float(raw)
    if value < 0.0 or value > 1.0:
        raise ValueError("integrity run probability must be between 0.0 and 1.0")
    return value


def build_integrity_options_from_namespace(ns: argparse.Namespace) -> IntegrityOptions:
    raw_dirs = getattr(ns, "integrity_rel_dirs", None)
    rel_dirs = tuple(raw_dirs or ()) or _parse_rel_dirs(getattr(ns, "integrity_dirs", None))
    report_dir = (
        getattr(ns, "integrity_report_dir", None)
        or getattr(ns, "pipeline_report_dir", None)
        or str(Path(getattr(ns, "script_dir", ".")) / "integrity_reports")
    )
    return IntegrityOptions(
        enabled=bool(getattr(ns, "integrity_check", False)),
        run_probability=_nonnegative_float(getattr(ns, "integrity_run_probability", 1.0)),
        max_dirs=_positive_int_or_none(getattr(ns, "integrity_max_dirs", None)),
        max_files=_positive_int_or_none(getattr(ns, "integrity_max_files", None)),
        rel_dirs=rel_dirs,
        report_dir=Path(report_dir) if report_dir else None,
        compare_chunk_bytes=int(getattr(ns, "integrity_compare_chunk_bytes", DEFAULT_COMPARE_CHUNK_BYTES)),
        max_retries=int(getattr(ns, "integrity_max_retries", 3)),
        retry_delay_seconds=float(getattr(ns, "integrity_retry_delay_seconds", 2.0)),
    )


def eligible_integrity_dirs(
    local_inventory: dict[str, int],
    processed_inventory: dict[str, int],
    *,
    requested_dirs: Iterable[str] = (),
) -> list[str]:
    requested = {item.strip().strip("/") or "." for item in requested_dirs}
    candidates = []
    for rel_dir, local_count in local_inventory.items():
        if local_count < 1:
            continue
        if requested and rel_dir not in requested:
            continue
        if processed_inventory.get(rel_dir) == local_count:
            candidates.append(rel_dir)
    return sorted(candidates)


def _random_source() -> RandomSource:
    return random.SystemRandom()


def _limited_sample(items: list[str], limit: int | None, rng: RandomSource) -> list[str]:
    shuffled = list(items)
    rng.shuffle(shuffled)
    if limit is not None:
        shuffled = shuffled[:limit]
    return sorted(shuffled)


def _s3_directory_prefix(rel_dir: str, *, processed_s3_prefix: str | None, processed_root: str | None) -> str:
    base = _normalize_s3_prefix(processed_root) or _normalize_s3_prefix(processed_s3_prefix) or ""
    rel = "" if rel_dir == "." else _normalize_s3_prefix(rel_dir)
    if rel:
        return f"{base}{rel}"
    return base


def _public_s3_list_root(
    bucket: str,
    *,
    prefix: str,
    region: str | None,
    max_retries: int,
    retry_delay_seconds: float,
    continuation_token: str | None = None,
) -> ElementTree.Element:
    base_url = _public_s3_base_url(bucket, region=region)
    params = {"list-type": "2", "prefix": prefix}
    if continuation_token:
        params["continuation-token"] = continuation_token
    url = f"{base_url}/?{urlencode(params)}"

    attempt = 0
    while True:
        try:
            with urlopen(url, timeout=30) as response:
                return ElementTree.fromstring(response.read())
        except HTTPError as exc:
            attempt += 1
            if exc.code in {429, 500, 502, 503, 504} and attempt <= max_retries:
                time.sleep(retry_delay_seconds * (2 ** (attempt - 1)))
                continue
            raise RuntimeError(f"Public S3 listing failed for bucket '{bucket}': HTTP {exc.code}") from exc
        except URLError as exc:
            attempt += 1
            if attempt <= max_retries:
                time.sleep(retry_delay_seconds * (2 ** (attempt - 1)))
                continue
            raise RuntimeError(f"Public S3 listing failed for bucket '{bucket}': {exc.reason}") from exc


def list_public_s3_direct_files(
    *,
    bucket: str,
    rel_dir: str,
    processed_s3_prefix: str | None,
    processed_root: str | None,
    region: str | None,
    max_retries: int,
    retry_delay_seconds: float,
) -> list[PublicS3Object]:
    prefix = _s3_directory_prefix(rel_dir, processed_s3_prefix=processed_s3_prefix, processed_root=processed_root)
    token: str | None = None
    objects: list[PublicS3Object] = []
    while True:
        root = _public_s3_list_root(
            bucket,
            prefix=prefix,
            region=region,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
            continuation_token=token,
        )
        for item in root.findall("{*}Contents"):
            key = item.findtext("{*}Key")
            if not key or key.endswith("/"):
                continue
            if _candidate_rel_dir_from_path(key, bundle_root=processed_root, include_hidden=True) == rel_dir:
                objects.append(PublicS3Object(key=key))
        is_truncated = (root.findtext("{*}IsTruncated") or "").strip().lower() == "true"
        token = root.findtext("{*}NextContinuationToken")
        if not is_truncated or not token:
            break
    return sorted(objects, key=lambda item: item.key)


def _local_direct_files(local_root: str, rel_dir: str, *, include_hidden: bool) -> dict[str, Path]:
    base = Path(sanitize_path(local_root))
    directory = base if rel_dir == "." else base / rel_dir
    if not directory.is_dir():
        raise FileNotFoundError(f"Local integrity directory does not exist: {directory}")
    files: dict[str, Path] = {}
    for child in sorted(directory.iterdir()):
        if not child.is_file():
            continue
        if not include_hidden and child.name.startswith("."):
            continue
        rel_path = child.name if rel_dir == "." else PurePosixPath(rel_dir, child.name).as_posix()
        files[rel_path] = child
    return files


def _rel_path_from_s3_key(key: str, processed_root: str | None, processed_s3_prefix: str | None) -> str:
    parts = tuple(p for p in PurePosixPath(key).parts if p and p != "/")
    root_parts = tuple(p for p in PurePosixPath(processed_root or processed_s3_prefix or "").parts if p and p != "/")
    if root_parts and parts[: len(root_parts)] == root_parts:
        parts = parts[len(root_parts) :]
    return PurePosixPath(*parts).as_posix() if parts else "."


def _open_public_s3_object(bucket: str, key: str, region: str | None) -> BinaryIO:
    base_url = _public_s3_base_url(bucket, region=region)
    url = f"{base_url}/{quote(key, safe='/')}"
    request = Request(url, method="GET")
    return urlopen(request, timeout=60)


def _compare_streams(local_file: BinaryIO, remote_file: BinaryIO, chunk_size: int) -> tuple[bool, int]:
    compared = 0
    while True:
        local_chunk = local_file.read(chunk_size)
        remote_chunk = remote_file.read(chunk_size)
        compared += min(len(local_chunk), len(remote_chunk))
        if local_chunk != remote_chunk:
            return False, compared
        if not local_chunk:
            return True, compared


def compare_file_bytes(
    *,
    local_path: Path,
    bucket: str,
    key: str,
    region: str | None,
    chunk_size: int,
    opener: Callable[[str, str, str | None], BinaryIO] = _open_public_s3_object,
) -> FileCompareResult:
    rel_path = local_path.name
    try:
        with local_path.open("rb") as local_file:
            with opener(bucket, key, region) as remote_file:
                matched, compared = _compare_streams(local_file, remote_file, chunk_size)
        return FileCompareResult(
            rel_path=rel_path,
            s3_key=key,
            status="matched" if matched else "mismatch",
            bytes_compared=compared,
        )
    except Exception as exc:
        return FileCompareResult(rel_path=rel_path, s3_key=key, status="error", error=str(exc))


def compare_directory(
    *,
    local_root: str,
    rel_dir: str,
    bucket: str,
    processed_s3_prefix: str | None,
    processed_root: str | None,
    region: str | None,
    include_hidden: bool,
    max_files: int | None,
    rng: RandomSource,
    options: IntegrityOptions,
    opener: Callable[[str, str, str | None], BinaryIO] = _open_public_s3_object,
) -> DirectoryCompareResult:
    local_files = _local_direct_files(local_root, rel_dir, include_hidden=include_hidden)
    log(f"Integrity check listing S3 objects for directory '{rel_dir}'")
    s3_objects = list_public_s3_direct_files(
        bucket=bucket,
        rel_dir=rel_dir,
        processed_s3_prefix=processed_s3_prefix,
        processed_root=processed_root,
        region=region,
        max_retries=options.max_retries,
        retry_delay_seconds=options.retry_delay_seconds,
    )
    s3_by_rel_path = {_rel_path_from_s3_key(item.key, processed_root, processed_s3_prefix): item for item in s3_objects}
    local_names = set(local_files)
    s3_names = set(s3_by_rel_path)
    missing = sorted(local_names - s3_names)
    extra = sorted(s3_by_rel_path[name].key for name in s3_names - local_names)

    comparable = sorted(local_names & s3_names)
    comparable = _limited_sample(comparable, max_files, rng)
    log(
        "Integrity check comparing directory "
        f"'{rel_dir}': local_files={len(local_files)}, s3_files={len(s3_objects)}, "
        f"files_to_compare={len(comparable)}, missing_s3={len(missing)}, extra_s3={len(extra)}"
    )
    mismatches: list[FileCompareResult] = []
    heartbeat = ProgressHeartbeat(f"Integrity directory {rel_dir}")
    for index, rel_path in enumerate(comparable, start=1):
        result = compare_file_bytes(
            local_path=local_files[rel_path],
            bucket=bucket,
            key=s3_by_rel_path[rel_path].key,
            region=region,
            chunk_size=options.compare_chunk_bytes,
            opener=opener,
        )
        if result.rel_path == local_files[rel_path].name:
            result.rel_path = rel_path
        if result.status != "matched":
            mismatches.append(result)
            log(
                "Integrity mismatch detected: "
                f"directory='{rel_dir}', file='{rel_path}', status={result.status}, bytes_compared={result.bytes_compared}"
            )
        heartbeat.notify_activity()
        heartbeat.maybe_emit(f"files_compared={index}/{len(comparable)}, mismatches={len(mismatches)}")

    status = "matched"
    if missing or extra or mismatches:
        status = "failed"

    return DirectoryCompareResult(
        rel_dir=rel_dir,
        status=status,
        local_file_count=len(local_files),
        s3_file_count=len(s3_objects),
        checked_file_count=len(comparable),
        mismatches=mismatches,
        missing_s3_files=missing,
        extra_s3_keys=extra,
    )


def _write_integrity_report(
    *,
    report_dir: Path,
    started_at: str,
    status: str,
    selected_dirs: list[str],
    directory_results: list[DirectoryCompareResult],
    skipped_reason: str = "",
) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    filename = f"integrity_report_{started_at}_{secrets.token_hex(4)}.json"
    path = report_dir / filename
    payload = {
        "run_start_time": started_at,
        "run_end_time": now_iso8601_utc(),
        "status": status,
        "skipped_reason": skipped_reason,
        "downloaded_to_disk": False,
        "selected_directories": selected_dirs,
        "directories": [
            {
                "rel_dir": item.rel_dir,
                "status": item.status,
                "local_file_count": item.local_file_count,
                "s3_file_count": item.s3_file_count,
                "checked_file_count": item.checked_file_count,
                "missing_s3_files": item.missing_s3_files,
                "extra_s3_keys": item.extra_s3_keys,
                "mismatches": [
                    {
                        "rel_path": mismatch.rel_path,
                        "s3_key": mismatch.s3_key,
                        "status": mismatch.status,
                        "bytes_compared": mismatch.bytes_compared,
                        "error": mismatch.error,
                    }
                    for mismatch in item.mismatches
                ],
            }
            for item in directory_results
        ],
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    log(f"Integrity report generated at: {path}")
    return path


def run_integrity_check(
    *,
    local_path: str,
    local_inventory: dict[str, int],
    processed_inventory: dict[str, int],
    processed_s3_bucket: str | None,
    processed_s3_prefix: str | None,
    processed_root: str | None,
    processed_s3_region: str | None,
    include_hidden: bool,
    options: IntegrityOptions,
    opener: Callable[[str, str, str | None], BinaryIO] = _open_public_s3_object,
) -> IntegrityRunResult:
    if not options.enabled:
        return IntegrityRunResult(status="disabled")
    if not processed_s3_bucket:
        raise ValueError("Integrity check requires --processed-s3-bucket / DUMMER_PROCESSED_S3_BUCKET")
    if not local_path:
        raise ValueError("Integrity check requires --local-path / DUMMER_LOCAL_PATH")
    if not options.report_dir:
        raise ValueError("Integrity check requires --integrity-report-dir or --pipeline-report-dir")
    if options.compare_chunk_bytes < 1:
        raise ValueError("--integrity-compare-chunk-bytes must be 1 or greater")

    rng = _random_source()
    started_at = now_iso8601_utc()
    log("Integrity check stage starting.")
    log(
        "Integrity check configuration: "
        f"run_probability={options.run_probability}, max_dirs={options.max_dirs}, "
        f"max_files={options.max_files}, requested_dirs={list(options.rel_dirs) or 'any'}"
    )
    candidates = eligible_integrity_dirs(
        local_inventory,
        processed_inventory,
        requested_dirs=options.rel_dirs,
    )
    gate_value = rng.random()
    log(
        "Integrity check probability decision: "
        f"random_value={gate_value:.6f}, threshold={options.run_probability}, "
        f"eligible_directories={len(candidates)}"
    )
    if gate_value >= options.run_probability:
        log("Integrity check skipped by probability gate.")
        report_path = _write_integrity_report(
            report_dir=options.report_dir,
            started_at=started_at,
            status="skipped",
            selected_dirs=[],
            directory_results=[],
            skipped_reason=f"probability gate did not run at {options.run_probability}",
        )
        return IntegrityRunResult(status="skipped", report_path=report_path, directories_considered=len(candidates))

    selected_dirs = _limited_sample(candidates, options.max_dirs, rng)
    log(
        "Integrity check selected directories: "
        f"selected={len(selected_dirs)}, eligible={len(candidates)}, directories={selected_dirs}"
    )
    files_remaining = options.max_files
    directory_results: list[DirectoryCompareResult] = []
    for dir_index, rel_dir in enumerate(selected_dirs, start=1):
        log(f"Integrity check starting directory {dir_index}/{len(selected_dirs)}: {rel_dir}")
        per_dir_file_limit = files_remaining
        result = compare_directory(
            local_root=local_path,
            rel_dir=rel_dir,
            bucket=processed_s3_bucket,
            processed_s3_prefix=processed_s3_prefix,
            processed_root=processed_root,
            region=processed_s3_region,
            include_hidden=include_hidden,
            max_files=per_dir_file_limit,
            rng=rng,
            options=options,
            opener=opener,
        )
        directory_results.append(result)
        log(
            "Integrity check finished directory "
            f"{dir_index}/{len(selected_dirs)}: {rel_dir}, status={result.status}, "
            f"checked_files={result.checked_file_count}, missing_s3={len(result.missing_s3_files)}, "
            f"extra_s3={len(result.extra_s3_keys)}, mismatches={len(result.mismatches)}"
        )
        if files_remaining is not None:
            files_remaining = max(0, files_remaining - result.checked_file_count)
            if files_remaining == 0:
                log("Integrity check reached --integrity-max-files limit.")
                break

    failures = sum(1 for item in directory_results if item.status != "matched")
    status = "succeeded" if failures == 0 else "failed"
    report_path = _write_integrity_report(
        report_dir=options.report_dir,
        started_at=started_at,
        status=status,
        selected_dirs=selected_dirs,
        directory_results=directory_results,
    )
    return IntegrityRunResult(
        status=status,
        report_path=report_path,
        directories_considered=len(candidates),
        directories_selected=len(directory_results),
        files_checked=sum(item.checked_file_count for item in directory_results),
        failures=failures,
    )


def add_integrity_args(parser: argparse.ArgumentParser, defaults: dict[str, object]) -> None:
    parser.add_argument("--integrity-check", action="store_true", default=defaults.get("integrity_check"))
    parser.add_argument("--integrity-run-probability", type=float, default=defaults.get("integrity_run_probability"))
    parser.add_argument("--integrity-max-dirs", type=int, default=defaults.get("integrity_max_dirs"))
    parser.add_argument("--integrity-max-files", type=int, default=defaults.get("integrity_max_files"))
    parser.add_argument("--integrity-dir", dest="integrity_rel_dirs", action="append", default=None)
    parser.add_argument("--integrity-dirs", default=defaults.get("integrity_dirs"))
    parser.add_argument("--integrity-report-dir", default=defaults.get("integrity_report_dir"))
    parser.add_argument(
        "--integrity-compare-chunk-bytes",
        type=int,
        default=defaults.get("integrity_compare_chunk_bytes"),
    )
    parser.add_argument("--integrity-max-retries", type=int, default=defaults.get("integrity_max_retries"))
    parser.add_argument(
        "--integrity-retry-delay-seconds",
        type=float,
        default=defaults.get("integrity_retry_delay_seconds"),
    )


def _parse_standalone_args(argv: list[str] | None) -> argparse.Namespace:
    defaults = load_config_defaults([] if argv is None else argv)
    parser = argparse.ArgumentParser(description="Run public-S3 byte-for-byte Dummer integrity checks.")
    parser.add_argument("--local-path", default=defaults["local_path"], required=not bool(defaults["local_path"]))
    parser.add_argument("--local-state", default=defaults["local_state"])
    parser.add_argument("--processed-state", default=defaults["processed_state"])
    parser.add_argument("--processed-s3-bucket", default=defaults["processed_s3_bucket"])
    parser.add_argument("--processed-s3-prefix", default=defaults["processed_s3_prefix"])
    parser.add_argument("--processed-root", default=defaults["processed_root"])
    parser.add_argument("--processed-s3-region", default=defaults["processed_s3_region"])
    parser.add_argument("--include-hidden", action="store_true", default=defaults["include_hidden"])
    parser.add_argument("--script-dir", default=defaults["script_dir"])
    add_integrity_args(parser, defaults)
    ns = parser.parse_args(argv)
    ns.integrity_check = True
    return ns


def standalone_main(argv: list[str] | None = None) -> int:
    ns = _parse_standalone_args(argv)
    log_effective_configuration(ns, keys=tuple(k for k in DEFAULTS if hasattr(ns, k)))
    script_dir = Path(ns.script_dir)
    local_state = Path(ns.local_state) if ns.local_state else script_dir / "local_dirs.txt"
    processed_state = Path(ns.processed_state) if ns.processed_state else script_dir / "processed_s3_dirs.txt"
    local_inventory = read_state_file(local_state)
    processed_inventory = read_state_file(processed_state)
    options = build_integrity_options_from_namespace(ns)
    try:
        result = run_integrity_check(
            local_path=ns.local_path,
            local_inventory=local_inventory,
            processed_inventory=processed_inventory,
            processed_s3_bucket=ns.processed_s3_bucket,
            processed_s3_prefix=ns.processed_s3_prefix,
            processed_root=ns.processed_root,
            processed_s3_region=ns.processed_s3_region,
            include_hidden=ns.include_hidden,
            options=options,
        )
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        log(f"Error: {exc}")
        return 1
    return 0 if result.status in {"succeeded", "skipped", "disabled"} else 1
