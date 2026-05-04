from __future__ import annotations

import json
import os
import pty
import selectors
import shutil
import subprocess
import termios
import fcntl
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Tuple

from .console import ProgressHeartbeat, log, write_raw
from .utils import sanitize_path


@dataclass(frozen=True)
class IngressReportValidation:
    valid: bool
    total_uploaded: int = 0
    total_skipped: int = 0
    total_failed: int = 0
    total_unprocessed: int = 0
    total_files: int = 0

    def __bool__(self) -> bool:
        return self.valid

    @property
    def processed_count(self) -> int:
        return self.total_uploaded + self.total_skipped


def parse_ingress_report(report_file: Path, *, expected_total_files: int | None = None) -> IngressReportValidation:
    if not report_file.is_file():
        log(f"Error: Report file not found: {report_file}")
        return IngressReportValidation(valid=False)

    try:
        parsed = json.loads(report_file.read_text(encoding="utf-8"))
    except OSError as exc:
        log(f"Error: Could not read report file: {exc}")
        return IngressReportValidation(valid=False)
    except json.JSONDecodeError as exc:
        log(f"Error: Could not parse report JSON: {exc}")
        log("Parsed: Uploaded='' Skipped='' Failed='' Unprocessed='' Files=''")
        return IngressReportValidation(valid=False)

    def val(key: str) -> int:
        raw = parsed.get(key, 0)
        if isinstance(raw, bool):
            raise ValueError(f"{key} must be an integer, not a boolean")
        if isinstance(raw, int):
            return int(raw)
        if isinstance(raw, float):
            raise ValueError(f"{key} must be an integer, not a float")
        raw_s = str(raw).strip()
        if not raw_s.isdigit():
            raise ValueError(f"{key} must be an integer")
        return int(raw_s)

    try:
        total_failed = val("Total Failed")
        total_files = val("Total Files")
        total_uploaded = val("Total Uploaded")
        total_skipped = val("Total Skipped")
        total_unprocessed = val("Total Unprocessed")
    except (TypeError, ValueError) as exc:
        log(f"Error: Could not parse required numeric fields from report file: {exc}")
        log(
            "Parsed: Uploaded='{}' Skipped='{}' Failed='{}' Unprocessed='{}' Files='{}'".format(
                parsed.get("Total Uploaded", ""),
                parsed.get("Total Skipped", ""),
                parsed.get("Total Failed", ""),
                parsed.get("Total Unprocessed", ""),
                parsed.get("Total Files", ""),
            )
        )
        return IngressReportValidation(valid=False)

    result = IngressReportValidation(
        valid=False,
        total_uploaded=total_uploaded,
        total_skipped=total_skipped,
        total_failed=total_failed,
        total_unprocessed=total_unprocessed,
        total_files=total_files,
    )

    log(
        f"Report totals - Uploaded: {total_uploaded}, Skipped: {total_skipped}, "
        f"Failed: {total_failed}, Unprocessed: {total_unprocessed}, Files: {total_files}"
    )

    accounted = total_uploaded + total_skipped + total_failed + total_unprocessed
    if accounted != total_files:
        log(f"Upload validation failed: totals mismatch (accounted={accounted}, files={total_files})")
        return result

    if expected_total_files is not None and total_files != expected_total_files:
        log(
            "Upload validation failed: report file count does not match local inventory "
            f"(report_files={total_files}, local_files={expected_total_files})"
        )
        return result

    if total_failed == 0 and total_unprocessed == 0 and total_files > 0:
        log("Upload validation successful")
        return IngressReportValidation(
            valid=True,
            total_uploaded=total_uploaded,
            total_skipped=total_skipped,
            total_failed=total_failed,
            total_unprocessed=total_unprocessed,
            total_files=total_files,
        )

    log(f"Upload validation failed: failed={total_failed}, unprocessed={total_unprocessed}")
    return result


def build_command(
    *,
    dum_binary: str = "/usr/local/bin/pds-ingress-client",
    log_level: str,
    bundle_prefix: str,
    config_file: str,
    name_param: str,
    full_path: str,
    num_threads: int,
    report_path: str,
    exclude_patterns: Iterable[str] = (),
) -> list[str]:
    command = [
        dum_binary,
        "--log-level",
        log_level,
        "--prefix",
        f"{bundle_prefix}/",
        "-c",
        config_file,
        "-n",
        name_param,
        full_path,
        "--num-threads",
        str(num_threads),
        "--report-path",
        report_path,
    ]
    for pattern in exclude_patterns:
        command.extend(["--exclude", pattern])
    return command


def build_direct_file_only_exclude_patterns(full_path: str, bundle_prefix: str) -> list[str]:
    normalized_full_path = sanitize_path(full_path)
    normalized_prefix = sanitize_path(bundle_prefix)

    patterns = [f"{normalized_full_path}/*/*"]
    prefix_with_sep = f"{normalized_prefix}/"
    if normalized_full_path.startswith(prefix_with_sep):
        trimmed = normalized_full_path[len(prefix_with_sep) :]
        if trimmed:
            patterns.append(f"{trimmed}/*/*")

    deduped: list[str] = []
    seen: set[str] = set()
    for pattern in patterns:
        if pattern in seen:
            continue
        deduped.append(pattern)
        seen.add(pattern)
    return deduped


def _execute_command_captured(command: list[str]) -> Tuple[int, str]:
    try:
        completed = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        return 127, str(exc)

    return completed.returncode, completed.stdout


def _execute_command_interactive(command: list[str]) -> Tuple[int, str]:
    master_fd, slave_fd = pty.openpty()
    size = shutil.get_terminal_size(fallback=(120, 40))
    winsize = struct.pack("HHHH", size.lines, size.columns, 0, 0)
    fcntl.ioctl(slave_fd, termios.TIOCSWINSZ, winsize)
    child_env = os.environ.copy()
    if not child_env.get("TERM") or child_env["TERM"] == "dumb":
        child_env["TERM"] = "xterm-256color"
    if not child_env.get("COLORTERM"):
        child_env["COLORTERM"] = "truecolor"

    try:
        proc = subprocess.Popen(
            command,
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            env=child_env,
            text=False,
            close_fds=True,
        )
    except FileNotFoundError as exc:
        os.close(slave_fd)
        os.close(master_fd)
        return 127, str(exc)
    except Exception:
        os.close(slave_fd)
        os.close(master_fd)
        raise
    os.close(slave_fd)

    selector = selectors.DefaultSelector()
    selector.register(master_fd, selectors.EVENT_READ)
    output_chunks: list[str] = []

    try:
        while True:
            events = selector.select(timeout=1.0)
            if events:
                for key, _ in events:
                    try:
                        chunk = os.read(key.fd, 4096)
                    except OSError:
                        chunk = b""
                    if not chunk:
                        selector.unregister(key.fd)
                        continue
                    text = chunk.decode("utf-8", errors="replace")
                    output_chunks.append(text)
                    write_raw(text)

            if proc.poll() is not None and not selector.get_map():
                break
    except Exception:
        if proc.poll() is None:
            proc.terminate()
        raise
    finally:
        selector.close()
        os.close(master_fd)

    return proc.wait(), "".join(output_chunks)


def execute_command(command: list[str], *, interactive: bool = False) -> Tuple[int, str]:
    if interactive:
        return _execute_command_interactive(command)
    return _execute_command_captured(command)
