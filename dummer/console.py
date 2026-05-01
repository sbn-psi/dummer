from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from typing import TextIO


def timestamp_now() -> str:
    return datetime.now().astimezone().replace(microsecond=0).isoformat()


def log(message: str, *, stream: TextIO | None = None) -> None:
    target = stream or sys.stdout
    print(f"[{timestamp_now()}] {message}", file=target, flush=True)


def write_raw(text: str, *, stream: TextIO | None = None) -> None:
    if not text:
        return
    target = stream or sys.stdout
    print(text, file=target, end="", flush=True)


def log_lines(text: str, *, stream: TextIO | None = None) -> None:
    if not text:
        return
    for line in text.splitlines():
        log(line, stream=stream)


class ProgressHeartbeat:
    def __init__(self, label: str, interval_seconds: float = 30.0, stream: TextIO | None = None) -> None:
        self.label = label
        self.interval_seconds = interval_seconds
        self.stream = stream
        self._last_activity = time.monotonic()
        self._last_emit = self._last_activity

    def notify_activity(self) -> None:
        self._last_activity = time.monotonic()

    def maybe_emit(self, detail: str | None = None, *, force: bool = False) -> None:
        now = time.monotonic()
        has_activity = self._last_activity > self._last_emit
        if force or (has_activity and now - self._last_emit >= self.interval_seconds):
            message = f"{self.label} heartbeat"
            if detail:
                message = f"{message}: {detail}"
            log(message, stream=self.stream)
            self._last_emit = now


class TimestampedArgumentParser(argparse.ArgumentParser):
    def _print_message(self, message: str, file: TextIO | None = None) -> None:
        if not message:
            return
        log_lines(message.rstrip("\n"), stream=file or sys.stderr)
