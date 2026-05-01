from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .console import log
from .utils import now_iso8601_utc


@dataclass
class RunContext:
    run_start_time: str
    status: str = "starting"
    log_file_scanned: str = ""
    directory_processed: str = ""
    command_executed: str = ""
    command_exit_code: int = -1
    pds_ingress_client_report_path: str = ""
    reconciliation_summary: list[dict[str, Any]] = field(default_factory=list)
    reconciliation_drift: list[dict[str, Any]] = field(default_factory=list)


class PipelineReportWriter:
    def __init__(self, output_dir: Path, script_name: str) -> None:
        self.output_dir = output_dir
        self.script_name = script_name
        self.counter = 0

    def new_context(self) -> RunContext:
        return RunContext(run_start_time=now_iso8601_utc())

    def write(self, ctx: RunContext) -> Path:
        self.counter += 1
        self.output_dir.mkdir(parents=True, exist_ok=True)
        if ctx.status == "starting":
            ctx.status = "no_work_found"

        run_end_time = now_iso8601_utc()
        filename = f"run_report_{self.script_name}_{ctx.run_start_time}_{self.counter}.json"
        path = self.output_dir / filename

        payload = {
            "run_start_time": ctx.run_start_time,
            "run_end_time": run_end_time,
            "status": ctx.status,
            "log_file_scanned": ctx.log_file_scanned,
            "directory_processed": ctx.directory_processed,
            "command_executed": ctx.command_executed,
            "command_exit_code": ctx.command_exit_code,
            "pds_ingress_client_report_path": ctx.pds_ingress_client_report_path,
            "reconciliation_summary": ctx.reconciliation_summary,
            "reconciliation_drift": ctx.reconciliation_drift,
        }
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        log(f"Pipeline run report generated at: {path}")
        return path
