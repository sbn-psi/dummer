from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dummer.reconcile import ReconcileItem, ReconcileResult
from dummer.state import read_state_file
from dummer.workflow import UploadOptions, upload_from_reconcile_result


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
