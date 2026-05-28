from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from dummer.reconcile import ReconcileItem, ReconcileResult
from dummer.state import read_state_file
from dummer.dum import build_command, command_size_bytes
from dummer.workflow import (
    UploadOptions,
    _command_size_checked_file_batches,
    _truncated_direct_file_list_command,
    upload_from_reconcile_result,
)


class WorkflowUploadTests(unittest.TestCase):
    def test_max_dirs_processes_multiple_successes_without_loop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            processed_state = base / "processed.txt"
            report_dir = base / "reports"
            pipeline_dir = base / "pipeline"
            report_dir.mkdir()
            pipeline_dir.mkdir()

            options = UploadOptions(
                local_path="/data/bundle",
                bundle="bundle",
                prefix="/data",
                config="/conf.ini",
                name="name",
                dum_binary="/bin/echo",
                threads=1,
                report_dir=report_dir,
                pipeline_report_dir=pipeline_dir,
                log_level="warn",
                max_dirs=2,
                loop=False,
            )
            reconciled = ReconcileResult(
                pending=[
                    ReconcileItem("a/2026/one", 1, None),
                    ReconcileItem("a/2026/two", 2, None),
                    ReconcileItem("a/2026/three", 3, None),
                ],
                drift=[],
            )

            with patch("dummer.workflow.execute_command", return_value=(0, "")):
                with patch("dummer.workflow.parse_ingress_report", return_value=True):
                    exit_code = upload_from_reconcile_result(reconciled, processed_state, options)

            self.assertEqual(exit_code, 0)
            self.assertEqual(
                read_state_file(processed_state),
                {
                    "a/2026/one": 1,
                    "a/2026/two": 2,
                },
            )

    def test_validation_failure_does_not_update_processed_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            processed_state = base / "processed.txt"
            report_dir = base / "reports"
            pipeline_dir = base / "pipeline"
            report_dir.mkdir()
            pipeline_dir.mkdir()

            options = UploadOptions(
                local_path="/data/bundle",
                bundle="bundle",
                prefix="/data",
                config="/conf.ini",
                name="name",
                dum_binary="/bin/echo",
                threads=1,
                report_dir=report_dir,
                pipeline_report_dir=pipeline_dir,
                log_level="warn",
                max_dirs=1,
                loop=False,
            )
            reconciled = ReconcileResult(
                pending=[ReconcileItem("a/2026/one", 1, None)],
                drift=[],
            )

            with patch("dummer.workflow.execute_command", return_value=(0, "")):
                with patch("dummer.workflow.parse_ingress_report", return_value=False):
                    with patch("dummer.workflow.set_state_entry") as set_state_entry:
                        exit_code = upload_from_reconcile_result(reconciled, processed_state, options)

            self.assertEqual(exit_code, 1)
            set_state_entry.assert_not_called()
            self.assertEqual(read_state_file(processed_state), {})

    def test_successive_uploads_rewrite_sorted_deduped_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            processed_state = base / "processed.txt"
            processed_state.write_text("z/old\t9\na/old\t1\nz/old\t2\n", encoding="utf-8")
            report_dir = base / "reports"
            pipeline_dir = base / "pipeline"
            report_dir.mkdir()
            pipeline_dir.mkdir()

            options = UploadOptions(
                local_path="/data/bundle",
                bundle="bundle",
                prefix="/data",
                config="/conf.ini",
                name="name",
                dum_binary="/bin/echo",
                threads=1,
                report_dir=report_dir,
                pipeline_report_dir=pipeline_dir,
                log_level="warn",
                max_dirs=2,
                loop=False,
            )
            reconciled = ReconcileResult(
                pending=[
                    ReconcileItem("b/new", 3, None),
                    ReconcileItem("a/old", 4, 1),
                ],
                drift=[],
            )

            with patch("dummer.workflow.execute_command", return_value=(0, "")):
                with patch("dummer.workflow.parse_ingress_report", return_value=True):
                    exit_code = upload_from_reconcile_result(reconciled, processed_state, options)

            self.assertEqual(exit_code, 0)
            self.assertEqual(
                processed_state.read_text(encoding="utf-8").splitlines(),
                [
                    "a/old\t4",
                    "b/new\t3",
                    "z/old\t2",
                ],
            )

    def test_direct_file_list_upload_batches_direct_files_without_excludes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            local_root = base / "bundle"
            target = local_root / "a" / "2026" / "one"
            child = target / "child"
            child.mkdir(parents=True)
            for name in ("one.dat", "two.dat", "three.dat"):
                (target / name).write_text(name, encoding="utf-8")
            (child / "nested.dat").write_text("nested", encoding="utf-8")
            processed_state = base / "processed.txt"
            report_dir = base / "reports"
            pipeline_dir = base / "pipeline"
            report_dir.mkdir()
            pipeline_dir.mkdir()

            options = UploadOptions(
                local_path=str(local_root),
                bundle="bundle",
                prefix=str(base),
                config="/conf.ini",
                name="name",
                dum_binary="/bin/echo",
                threads=1,
                report_dir=report_dir,
                pipeline_report_dir=pipeline_dir,
                log_level="warn",
                max_dirs=1,
                loop=False,
                direct_file_list_upload=True,
                direct_file_list_batch_size=2,
            )
            reconciled = ReconcileResult(
                pending=[ReconcileItem("a/2026/one", 3, None)],
                drift=[],
            )

            commands: list[list[str]] = []

            def _capture_command(command, **_kwargs):
                commands.append(command)
                return (0, "")

            with patch("dummer.workflow.execute_command", side_effect=_capture_command):
                with patch("dummer.workflow.parse_ingress_report", return_value=True) as parse_report:
                    exit_code = upload_from_reconcile_result(reconciled, processed_state, options)

            self.assertEqual(exit_code, 0)
            self.assertEqual(len(commands), 2)
            self.assertIn(str(target / "one.dat"), commands[0])
            self.assertIn(str(target / "three.dat"), commands[0])
            self.assertIn(str(target / "two.dat"), commands[1])
            self.assertNotIn(str(target), commands[0] + commands[1])
            self.assertNotIn(str(child / "nested.dat"), commands[0] + commands[1])
            self.assertNotIn("--exclude", commands[0] + commands[1])
            self.assertEqual([call.kwargs["expected_total_files"] for call in parse_report.mock_calls], [2, 1])
            self.assertEqual(read_state_file(processed_state), {"a/2026/one": 3})

    def test_direct_file_list_batches_respect_runtime_command_size_limit(self) -> None:
        file_paths = [
            "/data/bundle/one.dat",
            "/data/bundle/two.dat",
            "/data/bundle/three.dat",
        ]
        max_command_bytes = (
            max(
                command_size_bytes(
                    build_command(
                        dum_binary="/bin/echo",
                        log_level="warn",
                        bundle_prefix="/data",
                        config_file="/conf.ini",
                        name_param="name",
                        full_path=[file_path],
                        num_threads=1,
                        report_path="/tmp/report.json",
                    )
                )
                for file_path in file_paths
            )
            + 1
        )

        batches, size_limited = _command_size_checked_file_batches(
            file_paths=file_paths,
            max_files_per_batch=500,
            max_command_bytes=max_command_bytes,
            dum_binary="/bin/echo",
            log_level="warn",
            bundle_prefix="/data",
            config_file="/conf.ini",
            name_param="name",
            num_threads=1,
            report_path="/tmp/report.json",
        )

        self.assertTrue(size_limited)
        self.assertEqual(
            batches,
            [
                ["/data/bundle/one.dat"],
                ["/data/bundle/two.dat"],
                ["/data/bundle/three.dat"],
            ],
        )

    def test_direct_file_list_command_display_truncates_only_file_list(self) -> None:
        file_paths = [f"/data/bundle/file_{idx}.dat" for idx in range(8)]
        command = build_command(
            dum_binary="/bin/echo",
            log_level="warn",
            bundle_prefix="/data",
            config_file="/conf.ini",
            name_param="name",
            full_path=file_paths,
            num_threads=1,
            report_path="/tmp/report.json",
        )

        display = _truncated_direct_file_list_command(command, file_paths, keep=2)

        self.assertIn("/bin/echo --log-level warn --prefix /data/ -c /conf.ini -n name", display)
        self.assertIn("/data/bundle/file_0.dat", display)
        self.assertIn("/data/bundle/file_1.dat", display)
        self.assertIn("[... 4 file paths omitted ...]", display)
        self.assertIn("/data/bundle/file_6.dat", display)
        self.assertIn("/data/bundle/file_7.dat", display)
        self.assertIn("--num-threads 1 --report-path /tmp/report.json", display)
        self.assertNotIn("/data/bundle/file_2.dat", display)
        self.assertNotIn("/data/bundle/file_5.dat", display)

    def test_direct_file_list_upload_logs_truncated_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            local_root = base / "bundle"
            target = local_root / "a" / "2026" / "one"
            target.mkdir(parents=True)
            file_paths = []
            for idx in range(8):
                path = target / f"file_{idx}.dat"
                path.write_text(f"file {idx}", encoding="utf-8")
                file_paths.append(path)
            processed_state = base / "processed.txt"
            report_dir = base / "reports"
            pipeline_dir = base / "pipeline"
            report_dir.mkdir()
            pipeline_dir.mkdir()

            options = UploadOptions(
                local_path=str(local_root),
                bundle="bundle",
                prefix=str(base),
                config="/conf.ini",
                name="name",
                dum_binary="/bin/echo",
                threads=1,
                report_dir=report_dir,
                pipeline_report_dir=pipeline_dir,
                log_level="warn",
                max_dirs=1,
                loop=False,
                direct_file_list_upload=True,
                direct_file_list_batch_size=20,
            )
            reconciled = ReconcileResult(
                pending=[ReconcileItem("a/2026/one", 8, None)],
                drift=[],
            )

            buffer = io.StringIO()
            with redirect_stdout(buffer):
                with patch("dummer.workflow.execute_command", return_value=(0, "")):
                    with patch("dummer.workflow.parse_ingress_report", return_value=True):
                        exit_code = upload_from_reconcile_result(reconciled, processed_state, options)

            output = buffer.getvalue()
            self.assertEqual(exit_code, 0)
            self.assertIn(str(file_paths[0]), output)
            self.assertIn(str(file_paths[2]), output)
            self.assertIn("[... 2 file paths omitted ...]", output)
            self.assertIn(str(file_paths[5]), output)
            self.assertIn(str(file_paths[7]), output)
            self.assertIn("--num-threads 1 --report-path", output)
            self.assertNotIn(str(file_paths[3]), output)
            self.assertNotIn(str(file_paths[4]), output)
            self.assertNotIn("Command size:", output)
            reports = list(pipeline_dir.glob("run_report_*.json"))
            self.assertEqual(len(reports), 1)
            payload = json.loads(reports[0].read_text(encoding="utf-8"))
            self.assertIn(str(file_paths[3]), payload["command_executed"])
            self.assertIn(str(file_paths[4]), payload["command_executed"])
            self.assertNotIn("[... 2 file paths omitted ...]", payload["command_executed"])

    def test_successful_upload_advances_reconciliation_summary_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            processed_state = base / "processed.txt"
            report_dir = base / "reports"
            pipeline_dir = base / "pipeline"
            report_dir.mkdir()
            pipeline_dir.mkdir()

            options = UploadOptions(
                local_path="/data/bundle",
                bundle="bundle",
                prefix="/data",
                config="/conf.ini",
                name="name",
                dum_binary="/bin/echo",
                threads=1,
                report_dir=report_dir,
                pipeline_report_dir=pipeline_dir,
                log_level="warn",
                max_dirs=1,
                loop=False,
            )
            reconciled = ReconcileResult(
                pending=[ReconcileItem("collection/703/2026/26Apr30", 4, 1)],
                drift=[],
            )
            summary = [
                {
                    "anchor": "2026",
                    "directories": {"processed": 2, "total": 3, "missing": 1},
                    "files": {"processed": 11, "total": 14, "missing": 3},
                }
            ]

            with patch("dummer.workflow.execute_command", return_value=(0, "")):
                with patch("dummer.workflow.parse_ingress_report", return_value=True):
                    exit_code = upload_from_reconcile_result(
                        reconciled,
                        processed_state,
                        options,
                        reconciliation_summary=summary,
                        summary_anchor_component=2,
                    )

            self.assertEqual(exit_code, 0)
            reports = list(pipeline_dir.glob("run_report_*.json"))
            self.assertEqual(len(reports), 1)
            payload = json.loads(reports[0].read_text(encoding="utf-8"))
            self.assertEqual(
                payload["reconciliation_summary"],
                [
                    {
                        "anchor": "2026",
                        "directories": {"processed": 3, "total": 3, "missing": 0},
                        "files": {"processed": 14, "total": 14, "missing": 0},
                    }
                ],
            )

    def test_loop_records_partial_progress_and_continues(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            processed_state = base / "processed.txt"
            report_dir = base / "reports"
            pipeline_dir = base / "pipeline"
            report_dir.mkdir()
            pipeline_dir.mkdir()

            options = UploadOptions(
                local_path="/data/bundle",
                bundle="bundle",
                prefix="/data",
                config="/conf.ini",
                name="name",
                dum_binary="/bin/echo",
                threads=1,
                report_dir=report_dir,
                pipeline_report_dir=pipeline_dir,
                log_level="warn",
                max_dirs=1,
                loop=True,
            )
            reconciled = ReconcileResult(
                pending=[
                    ReconcileItem("collection/703/2026/partial", 5, None),
                    ReconcileItem("collection/703/2026/done", 2, None),
                ],
                drift=[],
            )
            summary = [
                {
                    "anchor": "2026",
                    "directories": {"processed": 0, "total": 2, "missing": 2},
                    "files": {"processed": 0, "total": 7, "missing": 7},
                }
            ]

            def _write_report(command, *, interactive=False):
                report_path = Path(command[command.index("--report-path") + 1])
                if any("partial" in part for part in command):
                    payload = {
                        "Total Uploaded": 3,
                        "Total Skipped": 0,
                        "Total Failed": 0,
                        "Total Unprocessed": 0,
                        "Total Files": 3,
                    }
                else:
                    payload = {
                        "Total Uploaded": 2,
                        "Total Skipped": 0,
                        "Total Failed": 0,
                        "Total Unprocessed": 0,
                        "Total Files": 2,
                    }
                report_path.write_text(json.dumps(payload), encoding="utf-8")
                return 0, ""

            with patch("dummer.workflow.execute_command", side_effect=_write_report):
                exit_code = upload_from_reconcile_result(
                    reconciled,
                    processed_state,
                    options,
                    reconciliation_summary=summary,
                    summary_anchor_component=2,
                )

            self.assertEqual(exit_code, 1)
            self.assertEqual(
                read_state_file(processed_state),
                {
                    "collection/703/2026/done": 2,
                    "collection/703/2026/partial": 3,
                },
            )
            reports = sorted(pipeline_dir.glob("run_report_*.json"))
            self.assertEqual(len(reports), 2)
            partial_payload = json.loads(reports[0].read_text(encoding="utf-8"))
            done_payload = json.loads(reports[1].read_text(encoding="utf-8"))
            self.assertEqual(partial_payload["status"], "partial_processed")
            self.assertEqual(done_payload["status"], "processing_succeeded")
            self.assertEqual(
                done_payload["reconciliation_summary"],
                [
                    {
                        "anchor": "2026",
                        "directories": {"processed": 1, "total": 2, "missing": 1},
                        "files": {"processed": 5, "total": 7, "missing": 2},
                    }
                ],
            )

    def test_drift_is_written_to_no_work_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            pipeline_dir = base / "pipeline"
            options = UploadOptions(
                local_path=None,
                bundle="",
                prefix="",
                config="",
                name="",
                dum_binary="",
                threads=1,
                report_dir=None,
                pipeline_report_dir=pipeline_dir,
                log_level="warn",
                max_dirs=1,
                loop=False,
            )
            reconciled = ReconcileResult(
                pending=[],
                drift=[ReconcileItem("a/drift", 1, 3)],
            )

            exit_code = upload_from_reconcile_result(reconciled, base / "processed.txt", options)

            self.assertEqual(exit_code, 0)
            reports = list(pipeline_dir.glob("run_report_*.json"))
            self.assertEqual(len(reports), 1)
            report_text = reports[0].read_text(encoding="utf-8")
            self.assertIn('"rel_dir": "a/drift"', report_text)
            self.assertIn('"extra_processed_files": 2', report_text)

    def test_no_pending_work_does_not_require_upload_runtime_settings(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            options = UploadOptions(
                local_path=None,
                bundle="",
                prefix="",
                config="",
                name="",
                dum_binary="",
                threads=1,
                report_dir=None,
                pipeline_report_dir=None,
                log_level="warn",
                max_dirs=1,
                loop=False,
            )

            exit_code = upload_from_reconcile_result(
                ReconcileResult(pending=[], drift=[]),
                Path(tmp) / "processed.txt",
                options,
            )

            self.assertEqual(exit_code, 0)
