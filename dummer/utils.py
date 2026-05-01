from __future__ import annotations

from pathlib import Path


def sanitize_path(path: str) -> str:
    """Mimic shell sanitize_path: strip trailing slash and collapse duplicate slashes."""
    if not path:
        return path
    path = path.rstrip("/")
    while "//" in path:
        path = path.replace("//", "/")
    return path


def path_depth(path: str) -> int:
    return len([p for p in Path(path).parts if p and p != "/"])


def now_iso8601_utc() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def report_safe_name(rel_dir: str) -> str:
    return rel_dir.replace("/", "_")
