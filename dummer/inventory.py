from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import gzip
import os
import time
from dataclasses import dataclass
from pathlib import Path
from pathlib import PurePosixPath
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen
from xml.etree import ElementTree

from .console import ProgressHeartbeat, log
from .state import IncrementalStateSnapshotWriter, read_state_file
from .utils import sanitize_path


def _contains_path_component(rel_path: str, component: str) -> bool:
    parts = [p for p in rel_path.split("/") if p]
    return component in parts


def _path_contains_component(raw_path: str, component: str) -> bool:
    cleaned = raw_path.strip("/")
    token = f"/{component}/"
    return (
        cleaned == component
        or cleaned.startswith(f"{component}/")
        or cleaned.endswith(f"/{component}")
        or token in f"/{cleaned}/"
    )


def _should_prune_path_filter_sibling(path_parts: Iterable[str], path_filter: str | None, path_filter_depth: int | None) -> bool:
    if path_filter is None or path_filter_depth is None:
        return False
    parts = tuple(path_parts)
    if len(parts) <= path_filter_depth:
        return False
    return parts[path_filter_depth] != path_filter


def _manifest_root_parts(bundle_root: str | None) -> tuple[str, ...]:
    if not bundle_root:
        return ()
    return tuple(p for p in PurePosixPath(sanitize_path(bundle_root)).parts if p and p != "/")


def _normalize_s3_prefix(prefix: str | None) -> str | None:
    if prefix is None:
        return None
    cleaned = prefix.lstrip("/")
    if cleaned and not cleaned.endswith("/"):
        cleaned = f"{cleaned}/"
    return cleaned


def _normalize_start_after_key(start_after: str | None) -> str | None:
    if not start_after:
        return None
    return start_after.lstrip("/")


def _s3_prefix_parent(prefix: str | None) -> str | None:
    normalized = _normalize_s3_prefix(prefix)
    if not normalized:
        return None
    parts = [part for part in normalized.rstrip("/").split("/") if part]
    if len(parts) <= 1:
        return ""
    return "/".join(parts[:-1]) + "/"


def _s3_prefix_leaf(prefix: str | None) -> str | None:
    normalized = _normalize_s3_prefix(prefix)
    if not normalized:
        return None
    parts = [part for part in normalized.rstrip("/").split("/") if part]
    if not parts:
        return None
    return parts[-1]


def _relative_dir_from_manifest_path(raw_path: str, bundle_root: str | None) -> str | None:
    parts = tuple(p for p in PurePosixPath(raw_path).parts if p and p != "/")
    if len(parts) < 2:
        return None

    root_parts = _manifest_root_parts(bundle_root)
    if root_parts:
        if parts[: len(root_parts)] != root_parts:
            return None
        rel_parts = parts[len(root_parts) : -1]
    else:
        rel_parts = parts[1:-1]

    if not rel_parts:
        return "."

    return PurePosixPath(*rel_parts).as_posix()


def _add_candidate_path_to_counts(
    counts: dict[str, int],
    raw_path: str,
    path_filter: str | None = None,
    bundle_root: str | None = None,
) -> bool:
    rel_dir = _candidate_rel_dir_from_path(raw_path, path_filter=path_filter, bundle_root=bundle_root)
    if not rel_dir:
        return False
    counts[rel_dir] = counts.get(rel_dir, 0) + 1
    return True


def _increment_writer_from_candidate_path(
    writer: IncrementalStateSnapshotWriter,
    raw_path: str,
    path_filter: str | None = None,
    bundle_root: str | None = None,
) -> bool:
    rel_dir = _candidate_rel_dir_from_path(raw_path, path_filter=path_filter, bundle_root=bundle_root)
    if not rel_dir:
        return False
    writer.increment(rel_dir)
    return True


def _candidate_rel_dir_from_path(
    raw_path: str,
    path_filter: str | None = None,
    bundle_root: str | None = None,
) -> str | None:
    candidate = raw_path.strip()
    if not candidate or candidate.endswith("/"):
        return None

    rel_dir = _relative_dir_from_manifest_path(candidate, bundle_root)
    if rel_dir is None:
        return None
    if path_filter and not _contains_path_component(rel_dir, path_filter):
        return None
    return rel_dir


def _relative_dir_from_directory_path(raw_path: str, bundle_root: str | None) -> str | None:
    cleaned = raw_path.strip().strip("/")
    if not cleaned:
        return None
    return _relative_dir_from_manifest_path(f"{cleaned}/.__dir__", bundle_root)


def _read_ordered_state_entries(path: Path) -> list[tuple[str, int]]:
    entries: list[tuple[str, int]] = []
    if not path.exists():
        return entries

    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#"):
            continue
        parts = raw.rsplit("\t", 1)
        if len(parts) != 2:
            continue
        rel_dir, count_raw = parts[0].strip(), parts[1].strip()
        if not rel_dir:
            continue
        try:
            entries.append((rel_dir, int(count_raw)))
        except ValueError:
            continue

    return entries


def _cluster_prefix(rel_dir: str, depth: int = 2) -> str | None:
    parts = tuple(p for p in PurePosixPath(rel_dir).parts if p and p != "/")
    if len(parts) < depth:
        return None
    return PurePosixPath(*parts[:depth]).as_posix()


def _resume_trusted_counts_and_cluster(path: Path, cluster_depth: int = 2) -> tuple[dict[str, int], str | None]:
    entries = _read_ordered_state_entries(path)
    if not entries:
        return {}, None

    cluster = _cluster_prefix(entries[-1][0], depth=cluster_depth)
    if not cluster:
        return {}, None

    trusted: dict[str, int] = {}
    cluster_prefix = f"{cluster}/"
    for rel_dir, count in entries:
        if rel_dir == cluster or rel_dir.startswith(cluster_prefix):
            continue
        trusted[rel_dir] = count

    return trusted, cluster


def _resume_start_after_from_cluster(cluster: str, bundle_root: str | None) -> str:
    root = _normalize_s3_prefix(bundle_root)
    if not root:
        raise ValueError("--processed-root is required when resuming S3 inventory from an existing state file")
    cluster_prefix = _normalize_s3_prefix(cluster)
    assert cluster_prefix is not None
    return f"{root}{cluster_prefix}"


def _inventory_counts_from_paths(
    paths: Iterable[str],
    path_filter: str | None = None,
    bundle_root: str | None = None,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for raw in paths:
        _add_candidate_path_to_counts(counts, raw, path_filter=path_filter, bundle_root=bundle_root)
    return counts


@dataclass(frozen=True)
class KnownS3Directory:
    rel_dir: str
    s3_prefix: str


def _normalize_known_s3_directory(raw_dir: str, bucket_prefix: str | None, bundle_root: str | None) -> KnownS3Directory | None:
    cleaned = raw_dir.strip().strip("/")
    if not cleaned:
        return None

    normalized_bucket_prefix = _normalize_s3_prefix(bucket_prefix)
    bucket_prefix_no_slash = normalized_bucket_prefix.rstrip("/") if normalized_bucket_prefix else None
    normalized_bundle_root = _normalize_s3_prefix(bundle_root)
    bundle_root_no_slash = normalized_bundle_root.rstrip("/") if normalized_bundle_root else None
    bucket_parent = _s3_prefix_parent(bucket_prefix)
    bucket_leaf = _s3_prefix_leaf(bucket_prefix)

    if bundle_root_no_slash and (cleaned == bundle_root_no_slash or cleaned.startswith(f"{bundle_root_no_slash}/")):
        s3_dir = cleaned
    elif bucket_prefix_no_slash and (cleaned == bucket_prefix_no_slash or cleaned.startswith(f"{bucket_prefix_no_slash}/")):
        s3_dir = cleaned
    elif bucket_leaf and (cleaned == bucket_leaf or cleaned.startswith(f"{bucket_leaf}/")) and bucket_parent is not None:
        s3_dir = f"{bucket_parent}{cleaned}"
    elif normalized_bucket_prefix:
        s3_dir = f"{normalized_bucket_prefix}{cleaned}"
    else:
        s3_dir = cleaned

    rel_dir = _relative_dir_from_directory_path(s3_dir, bundle_root)
    if not rel_dir:
        rel_dir = _relative_dir_from_directory_path(cleaned, bundle_root)
    if not rel_dir:
        return None

    return KnownS3Directory(rel_dir=rel_dir, s3_prefix=_normalize_s3_prefix(s3_dir) or "")


def _load_known_s3_directories(
    known_dirs_file: str,
    *,
    bucket_prefix: str | None,
    bundle_root: str | None,
    path_filter: str | None = None,
) -> list[KnownS3Directory]:
    source = Path(sanitize_path(known_dirs_file))
    if not source.is_file():
        raise FileNotFoundError(f"Known directory list does not exist: {source}")

    directories: list[KnownS3Directory] = []
    seen_rel_dirs: set[str] = set()
    for line in source.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#"):
            continue
        entry = _normalize_known_s3_directory(raw.split("\t", 1)[0], bucket_prefix, bundle_root)
        if not entry:
            continue
        if path_filter and not _contains_path_component(entry.rel_dir, path_filter):
            continue
        if entry.rel_dir in seen_rel_dirs:
            continue
        directories.append(entry)
        seen_rel_dirs.add(entry.rel_dir)
    return directories


def write_inventory_to_state_file(counts: dict[str, int], out_path: Path) -> int:
    writer = IncrementalStateSnapshotWriter(out_path)
    for rel_dir in sorted(counts):
        writer.set_count(rel_dir, counts[rel_dir])
    writer.maybe_flush(force=True)
    return len(writer.counts)


def _public_s3_base_url(bucket: str, region: str | None = None, endpoint_url: str | None = None) -> str:
    if endpoint_url:
        return endpoint_url.rstrip("/")
    if region:
        return f"https://{bucket}.s3.{region}.amazonaws.com"
    return f"https://{bucket}.s3.amazonaws.com"


def _public_s3_list_root(
    bucket: str,
    *,
    prefix: str | None = None,
    delimiter: str | None = None,
    start_after: str | None = None,
    continuation_token: str | None = None,
    region: str | None = None,
    endpoint_url: str | None = None,
    max_retries: int = 10,
    retry_delay_seconds: float = 2.0,
):
    normalized_prefix = _normalize_s3_prefix(prefix)
    normalized_start_after = _normalize_start_after_key(start_after)
    base_url = _public_s3_base_url(bucket, region=region, endpoint_url=endpoint_url)

    params = {"list-type": "2"}
    if normalized_prefix:
        params["prefix"] = normalized_prefix
    if delimiter:
        params["delimiter"] = delimiter
    if continuation_token:
        params["continuation-token"] = continuation_token
    elif normalized_start_after:
        params["start-after"] = normalized_start_after

    url = f"{base_url}/?{urlencode(params)}"
    attempt = 0
    while True:
        try:
            with urlopen(url, timeout=30) as response:
                payload = response.read()
            return ElementTree.fromstring(payload)
        except HTTPError as exc:
            attempt += 1
            should_retry = exc.code in {429, 500, 502, 503, 504} and attempt <= max_retries
            if should_retry:
                delay = retry_delay_seconds * (2 ** (attempt - 1))
                log(
                    f"S3 listing request failed with HTTP {exc.code}; retrying in {delay:.1f}s "
                    f"(attempt {attempt}/{max_retries})"
                )
                time.sleep(delay)
                continue
            log(
                f"S3 listing request failed with HTTP {exc.code} and will not be retried "
                f"(attempt {attempt}/{max_retries})"
            )
            raise RuntimeError(f"Public S3 listing failed for bucket '{bucket}': HTTP {exc.code}") from exc
        except URLError as exc:
            attempt += 1
            if attempt <= max_retries:
                delay = retry_delay_seconds * (2 ** (attempt - 1))
                log(
                    f"S3 listing request failed with network error '{exc.reason}'; retrying in {delay:.1f}s "
                    f"(attempt {attempt}/{max_retries})"
                )
                time.sleep(delay)
                continue
            log(
                f"S3 listing request failed with network error '{exc.reason}' and will not be retried "
                f"(attempt {attempt}/{max_retries})"
            )
            raise RuntimeError(f"Public S3 listing failed for bucket '{bucket}': {exc.reason}") from exc


def _public_s3_page_keys(root: ElementTree.Element) -> list[str]:
    keys: list[str] = []
    for item in root.findall("{*}Contents"):
        key = item.findtext("{*}Key")
        if key:
            keys.append(key)
    return keys


def _count_public_s3_objects_for_prefix(
    bucket: str,
    *,
    prefix: str,
    rel_dir: str | None = None,
    bundle_root: str | None = None,
    region: str | None = None,
    endpoint_url: str | None = None,
    max_retries: int = 10,
    retry_delay_seconds: float = 2.0,
) -> tuple[int, int]:
    continuation_token: str | None = None
    object_count = 0
    page_count = 0
    # Delimiter mode keeps S3 from descending below the requested directory prefix.
    delimiter = "/" if rel_dir is not None else None

    while True:
        root = _public_s3_list_root(
            bucket,
            prefix=prefix,
            delimiter=delimiter,
            continuation_token=continuation_token,
            region=region,
            endpoint_url=endpoint_url,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
        )
        keys = _public_s3_page_keys(root)
        if rel_dir is None:
            object_count += sum(1 for key in keys if not key.endswith("/"))
        else:
            object_count += sum(1 for key in keys if _candidate_rel_dir_from_path(key, bundle_root=bundle_root) == rel_dir)
        page_count += 1

        is_truncated = (root.findtext("{*}IsTruncated") or "").strip().lower() == "true"
        continuation_token = root.findtext("{*}NextContinuationToken")
        if not is_truncated or not continuation_token:
            break

    return object_count, page_count


def iter_public_s3_keys(
    bucket: str,
    prefix: str | None = None,
    start_after: str | None = None,
    region: str | None = None,
    endpoint_url: str | None = None,
    max_retries: int = 10,
    retry_delay_seconds: float = 2.0,
):
    """
    Yield object keys from a public bucket using anonymous ListObjectsV2 requests.
    The bucket must allow public ListBucket access; object-read access alone is not enough.
    """
    token: str | None = None
    while True:
        root = _public_s3_list_root(
            bucket,
            prefix=prefix,
            start_after=start_after if token is None else None,
            continuation_token=token,
            region=region,
            endpoint_url=endpoint_url,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
        )
        for key in _public_s3_page_keys(root):
            yield key

        is_truncated = (root.findtext("{*}IsTruncated") or "").strip().lower() == "true"
        token = root.findtext("{*}NextContinuationToken")
        if not is_truncated or not token:
            break


def build_inventory_from_public_s3(
    bucket: str,
    prefix: str | None = None,
    path_filter: str | None = None,
    bundle_root: str | None = None,
    region: str | None = None,
    endpoint_url: str | None = None,
    start_after: str | None = None,
    max_retries: int = 10,
    retry_delay_seconds: float = 2.0,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    objects_seen = 0
    objects_matched = 0
    heartbeat = ProgressHeartbeat(f"S3 inventory for bucket {bucket}")
    log(f"Starting public S3 inventory build from bucket '{bucket}'")

    for key in iter_public_s3_keys(
        bucket=bucket,
        prefix=prefix,
        start_after=start_after,
        region=region,
        endpoint_url=endpoint_url,
        max_retries=max_retries,
        retry_delay_seconds=retry_delay_seconds,
    ):
        objects_seen += 1
        if _add_candidate_path_to_counts(counts, key, path_filter=path_filter, bundle_root=bundle_root):
            objects_matched += 1
        heartbeat.notify_activity()
        heartbeat.maybe_emit(f"objects_seen={objects_seen}, matched={objects_matched}, directories={len(counts)}")

    return counts


def public_s3_inventory_to_state_file(
    bucket: str,
    out_path: Path,
    prefix: str | None = None,
    path_filter: str | None = None,
    bundle_root: str | None = None,
    region: str | None = None,
    endpoint_url: str | None = None,
    resume_from_state: bool = False,
    max_retries: int = 10,
    retry_delay_seconds: float = 2.0,
    known_dirs_file: str | None = None,
    known_dirs_workers: int = 10,
    resume_cluster_depth: int = 2,
) -> int:
    if known_dirs_file:
        return _public_s3_inventory_from_known_dirs_to_state_file(
            bucket,
            out_path,
            known_dirs_file=known_dirs_file,
            prefix=prefix,
            path_filter=path_filter,
            bundle_root=bundle_root,
            region=region,
            endpoint_url=endpoint_url,
            resume_from_state=resume_from_state,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
            workers=known_dirs_workers,
            resume_cluster_depth=resume_cluster_depth,
        )

    objects_seen = 0
    objects_matched = 0
    writer = IncrementalStateSnapshotWriter(out_path)
    heartbeat = ProgressHeartbeat(f"S3 inventory for bucket {bucket}")
    log(f"Starting public S3 inventory build from bucket '{bucket}'")
    start_after: str | None = None

    if resume_from_state and out_path.exists():
        trusted_counts, resume_cluster = _resume_trusted_counts_and_cluster(out_path, cluster_depth=resume_cluster_depth)
        if resume_cluster:
            start_after = _resume_start_after_from_cluster(resume_cluster, bundle_root)
            for rel_dir in sorted(trusted_counts):
                writer.set_count(rel_dir, trusted_counts[rel_dir])
            writer.maybe_flush(force=True)
            log(
                f"Resuming S3 inventory from cluster '{resume_cluster}'; "
                f"trusted_directories={len(trusted_counts)}, start_after='{start_after}'"
            )
        else:
            log("Resume requested but no valid prior S3 cluster was found; rebuilding from the beginning.")

    for key in iter_public_s3_keys(
        bucket=bucket,
        prefix=prefix,
        start_after=start_after,
        region=region,
        endpoint_url=endpoint_url,
        max_retries=max_retries,
        retry_delay_seconds=retry_delay_seconds,
    ):
        objects_seen += 1
        if _increment_writer_from_candidate_path(writer, key, path_filter=path_filter, bundle_root=bundle_root):
            objects_matched += 1
        writer.maybe_flush()
        heartbeat.notify_activity()
        heartbeat.maybe_emit(
            f"objects_seen={objects_seen}, matched={objects_matched}, directories_written={len(writer.counts)}, "
            f"snapshots={writer.snapshots_written}"
        )

    writer.maybe_flush(force=True)
    log(
        f"Completed public S3 inventory build: objects_seen={objects_seen}, "
        f"matched={objects_matched}, directories_written={len(writer.counts)}, "
        f"snapshots={writer.snapshots_written}, out={out_path}"
    )
    return len(writer.counts)


def _public_s3_inventory_from_known_dirs_to_state_file(
    bucket: str,
    out_path: Path,
    *,
    known_dirs_file: str,
    prefix: str | None = None,
    path_filter: str | None = None,
    bundle_root: str | None = None,
    region: str | None = None,
    endpoint_url: str | None = None,
    resume_from_state: bool = False,
    max_retries: int = 10,
    retry_delay_seconds: float = 2.0,
    workers: int = 10,
    resume_cluster_depth: int = 2,
) -> int:
    known_dirs = _load_known_s3_directories(
        known_dirs_file,
        bucket_prefix=prefix,
        bundle_root=bundle_root,
        path_filter=path_filter,
    )
    writer = IncrementalStateSnapshotWriter(out_path)
    heartbeat = ProgressHeartbeat(f"S3 known-dir inventory for bucket {bucket}")
    log(
        f"Starting public S3 inventory build from known directories in '{known_dirs_file}' "
        f"(directories={len(known_dirs)}, workers={max(1, workers)})"
    )

    if resume_from_state and out_path.exists():
        trusted_counts, resume_cluster = _resume_trusted_counts_and_cluster(out_path, cluster_depth=resume_cluster_depth)
        if trusted_counts:
            for rel_dir in sorted(trusted_counts):
                writer.set_count(rel_dir, trusted_counts[rel_dir])
            writer.maybe_flush(force=True)
            known_dirs = [entry for entry in known_dirs if entry.rel_dir not in trusted_counts]
            if resume_cluster:
                log(
                    f"Resuming known-directory S3 inventory from cluster '{resume_cluster}'; "
                    f"trusted_directories={len(trusted_counts)}, remaining_directories={len(known_dirs)}"
                )
            else:
                log(
                    f"Resuming known-directory S3 inventory from existing state; "
                    f"trusted_directories={len(trusted_counts)}, remaining_directories={len(known_dirs)}"
                )
        elif out_path.exists():
            exact_counts = read_state_file(out_path)
            if exact_counts:
                for rel_dir in sorted(exact_counts):
                    writer.set_count(rel_dir, exact_counts[rel_dir])
                writer.maybe_flush(force=True)
                known_dirs = [entry for entry in known_dirs if entry.rel_dir not in exact_counts]
                log(
                    f"Resume requested but no valid cluster boundary was found; "
                    f"trusting exact existing directories={len(exact_counts)}, remaining_directories={len(known_dirs)}"
                )

    directories_completed = 0
    directories_with_objects = 0
    objects_seen = 0
    pages_seen = 0
    zero_count_directories = 0
    started = time.monotonic()

    def count_known_dir(entry: KnownS3Directory) -> tuple[KnownS3Directory, int, int]:
        object_count, page_count = _count_public_s3_objects_for_prefix(
            bucket,
            prefix=entry.s3_prefix,
            rel_dir=entry.rel_dir,
            bundle_root=bundle_root,
            region=region,
            endpoint_url=endpoint_url,
            max_retries=max_retries,
            retry_delay_seconds=retry_delay_seconds,
        )
        return entry, object_count, page_count

    if workers <= 1:
        results: Iterable[tuple[KnownS3Directory, int, int]] = (count_known_dir(entry) for entry in known_dirs)
    else:
        executor = ThreadPoolExecutor(max_workers=max(1, workers))
        futures = [executor.submit(count_known_dir, entry) for entry in known_dirs]
        results = (future.result() for future in as_completed(futures))

    try:
        for entry, object_count, page_count in results:
            directories_completed += 1
            objects_seen += object_count
            pages_seen += page_count
            if object_count > 0:
                writer.set_count(entry.rel_dir, object_count)
                directories_with_objects += 1
            else:
                zero_count_directories += 1
            writer.maybe_flush()
            heartbeat.notify_activity()
            heartbeat.maybe_emit(
                f"directories_completed={directories_completed}/{len(known_dirs)}, "
                f"directories_written={directories_with_objects}, zero_count={zero_count_directories}, "
                f"objects_seen={objects_seen}, pages={pages_seen}, snapshots={writer.snapshots_written}"
            )
    finally:
        if workers > 1:
            executor.shutdown(wait=True)

    writer.maybe_flush(force=True)
    log(
        f"Completed public S3 known-directory inventory build: elapsed={time.monotonic() - started:.1f}s, "
        f"directories_completed={directories_completed}, directories_written={directories_with_objects}, "
        f"zero_count={zero_count_directories}, objects_seen={objects_seen}, pages={pages_seen}, "
        f"snapshots={writer.snapshots_written}, out={out_path}"
    )
    return len(writer.counts)


def crawl_inventory_to_state_file(bundle_path: str, out_path: Path, path_filter: str | None = None) -> int:
    """
    Stream direct-file directory inventory directly to disk.
    Returns the number of written entries.
    """
    return crawl_inventory_to_state_file_with_options(bundle_path, out_path, path_filter=path_filter)


def crawl_inventory_to_state_file_with_options(
    bundle_path: str,
    out_path: Path,
    path_filter: str | None = None,
    path_filter_depth: int | None = None,
    crawl_min_depth: int | None = None,
    crawl_max_depth: int | None = None,
) -> int:
    """
    Stream direct-file directory inventory directly to disk.
    When path_filter_depth is provided, sibling subtrees whose component at
    that depth cannot match path_filter are skipped early.
    Crawl depth is counted from the crawl root's children: direct child
    directories are depth 1, grandchildren are depth 2.
    Returns the number of written entries.
    """
    if crawl_min_depth is not None and crawl_min_depth < 0:
        raise ValueError("--crawl-min-depth must be 0 or greater")
    if crawl_max_depth is not None and crawl_max_depth < 0:
        raise ValueError("--crawl-max-depth must be 0 or greater")
    if crawl_min_depth is not None and crawl_max_depth is not None and crawl_min_depth > crawl_max_depth:
        raise ValueError("--crawl-min-depth cannot be greater than --crawl-max-depth")

    base = Path(sanitize_path(bundle_path))
    if not base.is_dir():
        raise FileNotFoundError(f"Base directory does not exist: {base}")

    written = 0
    scanned = 0
    writer = IncrementalStateSnapshotWriter(out_path)
    heartbeat = ProgressHeartbeat(f"Crawl inventory for {base}")
    log(f"Starting crawl inventory from '{base}'")

    for root, dirs, files in os.walk(base, topdown=True):
        root_path = Path(root)
        scanned += 1
        if root_path == base:
            rel_parts: tuple[str, ...] = ()
        else:
            rel_parts = root_path.relative_to(base).parts
        depth = len(rel_parts)

        if crawl_max_depth is not None and depth >= crawl_max_depth:
            dirs[:] = []

        if _should_prune_path_filter_sibling(rel_parts, path_filter, path_filter_depth):
            dirs[:] = []
            writer.maybe_flush()
            heartbeat.notify_activity()
            heartbeat.maybe_emit(
                f"directories_scanned={scanned}, directories_written={written}, snapshots={writer.snapshots_written}"
            )
            continue

        if path_filter and path_filter not in rel_parts:
            writer.maybe_flush()
            heartbeat.notify_activity()
            heartbeat.maybe_emit(
                f"directories_scanned={scanned}, directories_written={written}, snapshots={writer.snapshots_written}"
            )
            continue

        if crawl_min_depth is not None and depth < crawl_min_depth:
            writer.maybe_flush()
            heartbeat.notify_activity()
            heartbeat.maybe_emit(
                f"directories_scanned={scanned}, directories_written={written}, snapshots={writer.snapshots_written}"
            )
            continue

        rel = "." if root_path == base else root_path.relative_to(base).as_posix()
        if not files:
            writer.maybe_flush()
            heartbeat.notify_activity()
            heartbeat.maybe_emit(
                f"directories_scanned={scanned}, directories_written={written}, snapshots={writer.snapshots_written}"
            )
            continue

        writer.set_count(rel, len(files))
        writer.maybe_flush()
        written += 1
        heartbeat.notify_activity()
        heartbeat.maybe_emit(
            f"directories_scanned={scanned}, directories_written={written}, snapshots={writer.snapshots_written}"
        )

    writer.maybe_flush(force=True)
    log(
        f"Completed crawl inventory build: directories_scanned={scanned}, "
        f"directories_written={written}, snapshots={writer.snapshots_written}, out={out_path}"
    )
    return written


def parse_inventory_manifest_to_state_file(
    manifest_path: str,
    out_path: Path,
    path_filter: str | None = None,
    bundle_root: str | None = None,
) -> int:
    """
    Build directory inventory from a newline-delimited manifest of file paths.
    If bundle_root is omitted, the first path component is treated as the bundle root.
    Returns the number of written directory entries.
    """
    source = Path(sanitize_path(manifest_path))
    if not source.is_file():
        raise FileNotFoundError(f"Manifest file does not exist: {source}")

    opener = gzip.open if source.suffix == ".gz" else open
    lines_read = 0
    matched = 0
    writer = IncrementalStateSnapshotWriter(out_path)
    heartbeat = ProgressHeartbeat(f"Manifest inventory for {source}")
    log(f"Starting manifest inventory parse from '{source}'")

    with opener(source, "rt", encoding="utf-8") as fh:
        for line in fh:
            lines_read += 1
            if _increment_writer_from_candidate_path(writer, line, path_filter=path_filter, bundle_root=bundle_root):
                matched += 1
            writer.maybe_flush()
            heartbeat.notify_activity()
            heartbeat.maybe_emit(
                f"lines_read={lines_read}, matched={matched}, directories={len(writer.counts)}, "
                f"snapshots={writer.snapshots_written}"
            )

    writer.maybe_flush(force=True)
    log(
        f"Completed manifest inventory parse: lines_read={lines_read}, matched={matched}, "
        f"directories={len(writer.counts)}, snapshots={writer.snapshots_written}, out={out_path}"
    )
    return len(writer.counts)
