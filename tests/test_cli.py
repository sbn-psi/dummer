from __future__ import annotations

import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from dummer import cli
from dummer.reconcile import ReconcileItem, ReconcileResult
from dummer.runtime_config import DEFAULTS
from dummer.workflow import ReconcileArtifacts


class CliConfigTests(unittest.TestCase):
    def test_parse_args_prefers_cli_over_env_and_dotenv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            script_dir = Path(tmp)
            dotenv_path = script_dir / ".env"
            dotenv_path.write_text(
                "\n".join(
                    [
                        "DUMMER_PATH_FILTER=2024",
                        "DUMMER_THREADS=7",
                        "DUMMER_BUNDLE=dotenv.bundle",
                    ]
                ),
                encoding="utf-8",
            )

            env_updates = {
                "DUMMER_PATH_FILTER": "2025",
                "DUMMER_THREADS": "11",
                "DUMMER_BUNDLE": "env.bundle",
            }

            with patch.dict(os.environ, env_updates, clear=False):
                ns = cli._parse_args(
                    [
                        "--script-dir",
                        str(script_dir),
                        "--path-filter",
                        "2026",
                        "--threads",
                        "13",
                    ]
                )

            self.assertEqual(ns.path_filter, "2026")
            self.assertEqual(ns.threads, 13)
            self.assertEqual(ns.bundle, "env.bundle")

    def test_parse_args_uses_dotenv_when_env_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            script_dir = Path(tmp)
            dotenv_path = script_dir / ".env"
            dotenv_path.write_text(
                "\n".join(
                    [
                        "DUMMER_PATH_FILTER=2026",
                        "DUMMER_PATH_FILTER_DEPTH=2",
                        "DUMMER_SUMMARY_ANCHOR_COMPONENT=2",
                        "DUMMER_THREADS=9",
                        "DUMMER_PROCESSED_S3_RESUME_FROM_STATE=true",
                        "DUMMER_DUM_BINARY=/opt/pds-ingress-client",
                    ]
                ),
                encoding="utf-8",
            )

            with patch.dict(os.environ, {}, clear=True):
                ns = cli._parse_args(["--script-dir", str(script_dir)])

            self.assertEqual(ns.path_filter, "2026")
            self.assertEqual(ns.path_filter_depth, 2)
            self.assertEqual(ns.summary_anchor_component, 2)
            self.assertEqual(ns.threads, 9)
            self.assertTrue(ns.processed_s3_resume_from_state)
            self.assertEqual(ns.dum_binary, "/opt/pds-ingress-client")

    def test_cli_source_overrides_processed_source_from_env_and_keeps_local_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            script_dir = Path(tmp)
            local_path = script_dir / "local-path"
            dotenv_path = script_dir / ".env"
            dotenv_path.write_text(
                "\n".join(
                    [
                        f"DUMMER_LOCAL_PATH={local_path}",
                        f"DUMMER_PROCESSED_S3_BUCKET=example-bucket",
                    ]
                ),
                encoding="utf-8",
            )
            local_state = script_dir / "local.txt"
            processed_state = script_dir / "processed.txt"

            with patch.dict(os.environ, {}, clear=True):
                ns = cli._parse_args(
                    [
                        "--script-dir",
                        str(script_dir),
                        "--local-state",
                        str(local_state),
                        "--processed-state",
                        str(processed_state),
                    ]
                )

            self.assertEqual(ns.local_state, str(local_state))
            self.assertEqual(ns.local_path, str(local_path))
            self.assertEqual(ns.processed_state, str(processed_state))
            self.assertIsNone(ns.processed_s3_bucket)

    def test_local_state_wins_over_configured_local_path_for_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            script_dir = Path(tmp)
            local_path = script_dir / "local-path"
            local_path.mkdir()
            dotenv_path = script_dir / ".env"
            dotenv_path.write_text(f"DUMMER_LOCAL_PATH={local_path}\n", encoding="utf-8")
            local_state = script_dir / "local_state.txt"
            processed_state = script_dir / "processed_state.txt"
            local_state.write_text("a\t1\n", encoding="utf-8")
            processed_state.write_text("a\t1\n", encoding="utf-8")

            buffer = io.StringIO()
            with patch.dict(os.environ, {}, clear=True):
                with redirect_stdout(buffer):
                    exit_code = cli.main(
                        [
                            "--script-dir",
                            str(script_dir),
                            "--local-state",
                            str(local_state),
                            "--processed-state",
                            str(processed_state),
                            "--pipeline-report-dir",
                            "",
                        ]
                    )

            self.assertEqual(exit_code, 0)
            self.assertNotIn("Local inventory refreshed", buffer.getvalue())

    def test_main_logs_resolved_values_at_startup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            script_dir = base / "script"
            report_dir = base / "reports"
            pipeline_report_dir = base / "pipeline"
            script_dir.mkdir()
            report_dir.mkdir()
            pipeline_report_dir.mkdir()

            local_state = script_dir / "local_state.txt"
            processed_state = script_dir / "processed_state.txt"
            local_state.write_text("collection/703/2026/26Apr30\t1\n", encoding="utf-8")
            processed_state.write_text("collection/703/2026/26Apr30\t1\n", encoding="utf-8")

            artifacts = ReconcileArtifacts(
                local_inventory={"collection/703/2026/26Apr30": 1},
                processed_inventory={"collection/703/2026/26Apr30": 1},
                result=cli.load_reconcile_artifacts(local_state, processed_state).result,
                summary=[],
            )

            buffer = io.StringIO()
            with redirect_stdout(buffer):
                with patch("dummer.cli.load_reconcile_artifacts", return_value=artifacts):
                    with patch("dummer.cli.upload_from_reconcile_result", return_value=0):
                        exit_code = cli.main(
                            [
                                "--script-dir",
                                str(script_dir),
                                "--local-state",
                                str(local_state),
                                "--processed-state",
                                str(processed_state),
                                "--report-dir",
                                str(report_dir),
                                "--pipeline-report-dir",
                                str(pipeline_report_dir),
                                "--bundle",
                                "gbo.ast.catalina.survey",
                                "--prefix",
                                "/dsk8/catalina",
                                "--config",
                                "/home/dum/conf.default.ini",
                                "--name",
                                "sbn",
                                "--path-filter",
                                "2026",
                            ]
                        )

            output = buffer.getvalue()
            self.assertEqual(exit_code, 0)
            self.assertIn("Resolved configuration:", output)
            self.assertIn("--path-filter: '2026'", output)
            self.assertIn(f"--script-dir: '{script_dir}'", output)

    def test_main_allows_no_upload_work_when_upload_runtime_settings_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            script_dir = base / "script"
            script_dir.mkdir()
            local_state = script_dir / "local_state.txt"
            processed_state = script_dir / "processed_state.txt"
            local_state.write_text("collection/703/2026/26Apr30\t1\n", encoding="utf-8")
            processed_state.write_text("collection/703/2026/26Apr30\t1\n", encoding="utf-8")

            buffer = io.StringIO()
            with patch.dict(os.environ, {}, clear=True):
                with redirect_stdout(buffer):
                    exit_code = cli.main(
                        [
                            "--script-dir",
                            str(script_dir),
                            "--local-state",
                            str(local_state),
                            "--processed-state",
                            str(processed_state),
                            "--pipeline-report-dir",
                            "",
                        ]
                    )

        output = buffer.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertIn("No new, unprocessed directories found to process.", output)

    def test_main_applies_path_filter_before_upload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            script_dir = base / "script"
            report_dir = base / "reports"
            pipeline_report_dir = base / "pipeline"
            script_dir.mkdir()
            report_dir.mkdir()
            pipeline_report_dir.mkdir()

            local_state = script_dir / "local_state.txt"
            processed_state = script_dir / "processed_state.txt"
            local_state.write_text("", encoding="utf-8")
            processed_state.write_text("a\t1\n", encoding="utf-8")

            artifacts = ReconcileArtifacts(
                local_inventory={},
                processed_inventory={},
                result=ReconcileResult(
                    pending=[
                        ReconcileItem("collection/703/2026/26Apr30", 1, None),
                        ReconcileItem("collection/703/2025/25Apr30", 1, None),
                    ],
                    drift=[],
                ),
                summary=[
                    {"anchor": "2025"},
                    {"anchor": "2026"},
                ],
            )

            captured: dict[str, object] = {}

            def _capture_upload(reconciled, _processed_state, _options, **kwargs):
                captured["pending"] = [item.rel_dir for item in reconciled.pending]
                captured["summary"] = kwargs.get("reconciliation_summary")
                return 0

            with patch("dummer.cli.load_reconcile_artifacts", return_value=artifacts):
                with patch("dummer.cli.upload_from_reconcile_result", side_effect=_capture_upload):
                    exit_code = cli.main(
                        [
                            "--script-dir",
                            str(script_dir),
                            "--local-state",
                            str(local_state),
                            "--processed-state",
                            str(processed_state),
                            "--report-dir",
                            str(report_dir),
                            "--pipeline-report-dir",
                            str(pipeline_report_dir),
                            "--bundle",
                            "gbo.ast.catalina.survey",
                            "--prefix",
                            "/dsk8/catalina",
                            "--config",
                            "/home/dum/conf.default.ini",
                            "--name",
                            "sbn",
                            "--path-filter",
                            "2026",
                        ]
                    )

            self.assertEqual(exit_code, 0)
            self.assertEqual(captured["pending"], ["collection/703/2026/26Apr30"])
            self.assertEqual(captured["summary"], [{"anchor": "2026"}])

    def test_main_rejects_multiple_prepared_local_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            script_dir = Path(tmp) / "script"
            script_dir.mkdir()
            local_state = script_dir / "local_state.txt"
            processed_state = script_dir / "processed_state.txt"
            local_state.write_text("a\t1\n", encoding="utf-8")
            processed_state.write_text("a\t1\n", encoding="utf-8")

            buffer = io.StringIO()
            with redirect_stdout(buffer):
                exit_code = cli.main(
                    [
                        "--script-dir",
                        str(script_dir),
                        "--local-state",
                        str(local_state),
                        "--local-manifest",
                        "/tmp/manifest.txt",
                        "--processed-state",
                        str(processed_state),
                    ]
                )

            self.assertEqual(exit_code, 1)
            self.assertIn("Choose only one prepared local inventory source", buffer.getvalue())

    def test_manifest_source_rebuilds_local_without_refresh_flag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            script_dir = base / "script"
            script_dir.mkdir()
            manifest = base / "manifest.txt"
            processed_state = base / "processed.txt"
            manifest.write_text("bundle/a/file.dat\n", encoding="utf-8")
            processed_state.write_text("a\t1\n", encoding="utf-8")

            buffer = io.StringIO()
            with redirect_stdout(buffer):
                exit_code = cli.main(
                    [
                        "--script-dir",
                        str(script_dir),
                        "--local-manifest",
                        str(manifest),
                        "--processed-state",
                        str(processed_state),
                        "--pipeline-report-dir",
                        "",
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertIn("Local inventory refreshed", buffer.getvalue())
            self.assertTrue((script_dir / "local_dirs.txt").exists())


if __name__ == "__main__":
    unittest.main()
