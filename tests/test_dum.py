from __future__ import annotations

import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from dummer.dum import (
    build_command,
    build_direct_file_only_exclude_patterns,
    direct_file_paths,
    execute_command,
    parse_ingress_report,
)


class DumCommandTests(unittest.TestCase):
    def test_builds_direct_file_only_exclude_patterns(self) -> None:
        patterns = build_direct_file_only_exclude_patterns(
            full_path="/dsk8/catalina/gbo.ast.catalina.survey/miscellaneous/I52/2023/23Dec11",
            bundle_prefix="/dsk8/catalina",
        )

        self.assertEqual(
            patterns,
            [
                "/dsk8/catalina/gbo.ast.catalina.survey/miscellaneous/I52/2023/23Dec11/*/*",
            ],
        )

    def test_build_command_appends_exclude_patterns(self) -> None:
        command = build_command(
            dum_binary="/opt/pds-ingress-client",
            log_level="warn",
            bundle_prefix="/dsk8/catalina",
            config_file="/home/dum/conf.default.ini",
            name_param="sbn",
            full_path="/dsk8/catalina/gbo.ast.catalina.survey/collection/2025/parent",
            num_threads=12,
            report_path="/tmp/report.json",
            exclude_patterns=["/abs/*/*", "trimmed/*/*"],
        )

        self.assertEqual(
            command,
            [
                "/opt/pds-ingress-client",
                "--log-level",
                "warn",
                "--prefix",
                "/dsk8/catalina/",
                "-c",
                "/home/dum/conf.default.ini",
                "-n",
                "sbn",
                "/dsk8/catalina/gbo.ast.catalina.survey/collection/2025/parent",
                "--num-threads",
                "12",
                "--report-path",
                "/tmp/report.json",
                "--exclude",
                "/abs/*/*",
                "--exclude",
                "trimmed/*/*",
            ],
        )

    def test_build_command_accepts_multiple_ingress_files(self) -> None:
        command = build_command(
            dum_binary="/opt/pds-ingress-client",
            log_level="warn",
            bundle_prefix="/dsk8/catalina",
            config_file="/home/dum/conf.default.ini",
            name_param="sbn",
            full_path=[
                "/dsk8/catalina/gbo.ast.catalina.survey/collection/file1.dat",
                "/dsk8/catalina/gbo.ast.catalina.survey/collection/file2.dat",
            ],
            num_threads=12,
            report_path="/tmp/report.json",
        )

        self.assertEqual(
            command,
            [
                "/opt/pds-ingress-client",
                "--log-level",
                "warn",
                "--prefix",
                "/dsk8/catalina/",
                "-c",
                "/home/dum/conf.default.ini",
                "-n",
                "sbn",
                "/dsk8/catalina/gbo.ast.catalina.survey/collection/file1.dat",
                "/dsk8/catalina/gbo.ast.catalina.survey/collection/file2.dat",
                "--num-threads",
                "12",
                "--report-path",
                "/tmp/report.json",
            ],
        )

    def test_direct_file_paths_contains_only_direct_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "bundle" / "dir"
            child = base / "child"
            child.mkdir(parents=True)
            first = base / "a.txt"
            second = base / "b.txt"
            nested = child / "nested.txt"
            first.write_text("alpha", encoding="utf-8")
            second.write_text("beta", encoding="utf-8")
            nested.write_text("nested", encoding="utf-8")

            files = direct_file_paths(str(base))

            self.assertEqual(files, [str(first), str(second)])

    def test_execute_command_captures_output_by_default(self) -> None:
        command = [
            sys.executable,
            "-u",
            "-c",
            "print('ingress line 1'); print('ingress line 2')",
        ]

        buffer = io.StringIO()
        with redirect_stdout(buffer):
            exit_code, output = execute_command(command)

        self.assertEqual(exit_code, 0)
        self.assertEqual(output, "ingress line 1\ningress line 2\n")
        self.assertEqual(buffer.getvalue(), "")

    def test_execute_command_streams_output_in_interactive_mode(self) -> None:
        command = [
            sys.executable,
            "-u",
            "-c",
            "print('ingress line 1'); print('ingress line 2')",
        ]

        buffer = io.StringIO()
        with redirect_stdout(buffer):
            exit_code, output = execute_command(command, interactive=True)

        self.assertEqual(exit_code, 0)
        self.assertIn("ingress line 1", output)
        self.assertIn("ingress line 2", output)
        self.assertEqual(buffer.getvalue(), output)

    def test_execute_command_interactive_sets_terminal_env(self) -> None:
        command = [
            sys.executable,
            "-u",
            "-c",
            "import os; print(os.environ.get('TERM')); print(os.environ.get('COLORTERM'))",
        ]

        buffer = io.StringIO()
        with redirect_stdout(buffer):
            exit_code, output = execute_command(command, interactive=True)

        self.assertEqual(exit_code, 0)
        self.assertIn("xterm-256color", output)
        self.assertIn("truecolor", output)

    def test_parse_ingress_report_rejects_boolean_counts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            report.write_text(
                '{"Total Failed": true, "Total Files": 1, "Total Uploaded": 1, '
                '"Total Skipped": 0, "Total Unprocessed": 0}',
                encoding="utf-8",
            )

            buffer = io.StringIO()
            with redirect_stdout(buffer):
                ok = parse_ingress_report(report)

            self.assertFalse(ok)
            self.assertIn("not a boolean", buffer.getvalue())

    def test_parse_ingress_report_rejects_unexpected_total_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            report.write_text(
                '{"Total Failed": 0, "Total Files": 3, "Total Uploaded": 3, '
                '"Total Skipped": 0, "Total Unprocessed": 0}',
                encoding="utf-8",
            )

            buffer = io.StringIO()
            with redirect_stdout(buffer):
                ok = parse_ingress_report(report, expected_total_files=4)

            self.assertFalse(ok)
            self.assertEqual(ok.processed_count, 3)
            self.assertIn("report file count does not match local inventory", buffer.getvalue())


if __name__ == "__main__":
    unittest.main()
