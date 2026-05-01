from __future__ import annotations

from time import monotonic
from pathlib import Path
from typing import Dict


def read_state_file(path: Path) -> Dict[str, int]:
    result: Dict[str, int] = {}
    if not path.exists():
        return result
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
            result[rel_dir] = int(count_raw)
        except ValueError:
            continue
    return result


def write_state_file(path: Path, counts: Dict[str, int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    with tmp_path.open("w", encoding="utf-8") as out:
        for rel_dir in sorted(counts):
            out.write(f"{rel_dir}\t{counts[rel_dir]}\n")
    tmp_path.replace(path)


def set_state_entry(path: Path, rel_dir: str, file_count: int) -> None:
    counts = read_state_file(path)
    counts[rel_dir] = file_count
    write_state_file(path, counts)


class IncrementalStateSnapshotWriter:
    def __init__(
        self,
        path: Path,
        *,
        flush_interval_seconds: float = 120.0,
        flush_interval_changes: int = 100000,
    ) -> None:
        self.path = path
        self.flush_interval_seconds = flush_interval_seconds
        self.flush_interval_changes = flush_interval_changes
        self.counts: Dict[str, int] = {}
        self.pending_changes = 0
        self.snapshots_written = 0
        self._last_flush = monotonic()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def increment(self, rel_dir: str, amount: int = 1) -> None:
        self.counts[rel_dir] = self.counts.get(rel_dir, 0) + amount
        self.pending_changes += 1

    def set_count(self, rel_dir: str, count: int) -> None:
        if self.counts.get(rel_dir) == count:
            return
        self.counts[rel_dir] = count
        self.pending_changes += 1

    def maybe_flush(self, *, force: bool = False) -> bool:
        if not self.pending_changes and not force:
            return False

        now = monotonic()
        should_flush = (
            force
            or self.snapshots_written == 0
            or self.pending_changes >= self.flush_interval_changes
            or now - self._last_flush >= self.flush_interval_seconds
        )
        if not should_flush:
            return False

        write_state_file(self.path, self.counts)

        self.pending_changes = 0
        self.snapshots_written += 1
        self._last_flush = now
        return True
