from __future__ import annotations

import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dummer import cli
from dummer.wizard import (
    _ask_path,
    _ensure_auto_managed_state_files,
    answers_to_env,
    collect_answers,
    merge_env_file,
    run_wizard,
)
from dummer.wizard_ui import WizardIO


class RecordingIO(WizardIO):
    def __init__(self, answers: list[str]) -> None:
        super().__init__(io.StringIO("\n".join(answers) + "\n"), io.StringIO())
        self.questions: list[str] = []

    def _record(self, question: str) -> None:
        self.questions.append(question)

    def ask(
        self,
        question: str,
        default: str | None = None,
        *,
        current: str | None = None,
        help_text: str | tuple[str, ...] | None = None,
    ) -> str:
        self._record(question)
        return super().ask(question, default, current=current, help_text=help_text)

    def ask_int(
        self,
        question: str,
        default: int | None = None,
        *,
        current: str | None = None,
        help_text: str | tuple[str, ...] | None = None,
    ) -> int | None:
        self._record(question)
        return super().ask_int(question, default, current=current, help_text=help_text)

    def ask_yes_no(
        self,
        question: str,
        default: bool,
        *,
        current: bool | None = None,
        help_text: str | tuple[str, ...] | None = None,
    ) -> bool:
        self._record(question)
        return super().ask_yes_no(question, default, current=current, help_text=help_text)

    def choose(
        self,
        question: str,
        choices: tuple[tuple[str, str], ...],
        default: str,
        *,
        current: str | None = None,
        help_text: str | tuple[str, ...] | None = None,
    ) -> str:
        self._record(question)
        return super().choose(question, choices, default, current=current, help_text=help_text)

    @property
    def output(self) -> str:
        return self.outstream.getvalue()


class WizardMappingTests(unittest.TestCase):
    def test_maps_crawl_and_processed_state_to_env(self) -> None:
        env = answers_to_env(
            {
                "local_path": "/data/bundle",
                "script_dir": "/var/lib/dummer",
                "local_source": "crawl",
                "processed_source": "state",
                "processed_state": "./processed_dirs.txt",
                "config": "/home/dum/conf.ini",
                "name": "sbn",
                "dum_binary": "/usr/local/bin/pds-ingress-client",
                "threads": 8,
                "report_dir": "/var/log/dum/reports",
                "pipeline_report_dir": "/var/log/dum/pipeline",
                "loop": True,
                "interactive": False,
                "direct_file_list_upload": False,
            }
        )

        self.assertEqual(env["DUMMER_LOCAL_PATH"], "/data/bundle")
        self.assertEqual(env["DUMMER_PROCESSED_STATE"], "./processed_dirs.txt")
        self.assertEqual(env["DUMMER_LOOP"], "true")
        self.assertNotIn("DUMMER_LOCAL_MANIFEST", env)
        self.assertNotIn("DUMMER_PROCESSED_S3_BUCKET", env)

    def test_maps_manifest_and_s3_known_dirs_to_env(self) -> None:
        env = answers_to_env(
            {
                "local_path": "/data/bundle",
                "script_dir": ".",
                "local_source": "manifest",
                "local_manifest": "/data/files.txt.gz",
                "local_root": "bundle",
                "processed_source": "s3",
                "processed_s3_bucket": "example-bucket",
                "processed_s3_prefix": "archive/bundle/",
                "processed_root": "archive/bundle",
                "processed_s3_region": "us-west-2",
                "processed_s3_known_dirs_file": "/data/dirs.txt",
                "processed_s3_known_dirs_workers": 12,
                "processed_s3_resume_from_state": True,
                "processed_s3_resume_cluster_depth": 3,
                "path_filter": "2026",
                "path_filter_depth": 2,
                "summary_anchor_component": 2,
                "config": "/home/dum/conf.ini",
                "name": "sbn",
                "dum_binary": "/usr/local/bin/pds-ingress-client",
                "threads": 12,
                "report_dir": "/var/log/dum/reports",
                "pipeline_report_dir": "/var/log/dum/pipeline",
                "loop": True,
                "interactive": False,
                "direct_file_list_upload": False,
            }
        )

        self.assertEqual(env["DUMMER_LOCAL_MANIFEST"], "/data/files.txt.gz")
        self.assertEqual(env["DUMMER_LOCAL_ROOT"], "bundle")
        self.assertEqual(env["DUMMER_PROCESSED_S3_BUCKET"], "example-bucket")
        self.assertEqual(env["DUMMER_PROCESSED_S3_KNOWN_DIRS_WORKERS"], "12")
        self.assertEqual(env["DUMMER_PROCESSED_S3_RESUME_FROM_STATE"], "true")
        self.assertEqual(env["DUMMER_PATH_FILTER_DEPTH"], "2")
        self.assertNotIn("DUMMER_PROCESSED_STATE", env)

    def test_direct_file_upload_drops_manifest_store(self) -> None:
        env = answers_to_env(
            {
                "local_path": "/data/bundle",
                "script_dir": ".",
                "local_source": "crawl",
                "processed_source": "state",
                "processed_state": "./processed_dirs.txt",
                "config": "/home/dum/conf.ini",
                "name": "sbn",
                "dum_binary": "/usr/local/bin/pds-ingress-client",
                "threads": 12,
                "report_dir": "/var/log/dum/reports",
                "pipeline_report_dir": "/var/log/dum/pipeline",
                "dum_manifest_store": "/var/lib/dummer/manifests",
                "direct_file_list_upload": True,
                "direct_file_list_batch_size": 25,
                "loop": False,
                "max_dirs": 2,
                "interactive": True,
            }
        )

        self.assertEqual(env["DUMMER_DIRECT_FILE_LIST_UPLOAD"], "true")
        self.assertEqual(env["DUMMER_DIRECT_FILE_LIST_BATCH_SIZE"], "25")
        self.assertNotIn("DUMMER_DUM_MANIFEST_STORE", env)

    def test_maps_integrity_check_settings_only_when_enabled(self) -> None:
        env = answers_to_env(
            {
                "local_path": "/data/bundle",
                "script_dir": ".",
                "local_source": "crawl",
                "processed_source": "s3",
                "processed_s3_bucket": "example-bucket",
                "processed_s3_prefix": "archive/bundle/",
                "processed_root": "archive/bundle",
                "config": "/home/dum/conf.ini",
                "name": "sbn",
                "dum_binary": "/usr/local/bin/pds-ingress-client",
                "threads": 12,
                "report_dir": "/var/log/dum/reports",
                "pipeline_report_dir": "/var/log/dum/pipeline",
                "direct_file_list_upload": False,
                "loop": True,
                "interactive": False,
                "integrity_check": True,
                "integrity_run_probability": 0.1,
                "integrity_max_dirs": 2,
                "integrity_max_files": 50,
                "integrity_report_dir": "/var/log/dum/integrity",
            }
        )

        self.assertEqual(env["DUMMER_INTEGRITY_CHECK"], "true")
        self.assertEqual(env["DUMMER_INTEGRITY_RUN_PROBABILITY"], "0.1")
        self.assertEqual(env["DUMMER_INTEGRITY_MAX_DIRS"], "2")
        self.assertEqual(env["DUMMER_INTEGRITY_MAX_FILES"], "50")
        self.assertEqual(env["DUMMER_INTEGRITY_REPORT_DIR"], "/var/log/dum/integrity")

        disabled = answers_to_env(
            {
                "local_path": "/data/bundle",
                "script_dir": ".",
                "local_source": "crawl",
                "processed_source": "state",
                "processed_state": "./processed_dirs.txt",
                "config": "/home/dum/conf.ini",
                "name": "sbn",
                "dum_binary": "/usr/local/bin/pds-ingress-client",
                "threads": 12,
                "report_dir": "/var/log/dum/reports",
                "pipeline_report_dir": "/var/log/dum/pipeline",
                "direct_file_list_upload": False,
                "loop": True,
                "interactive": False,
                "integrity_check": False,
                "integrity_max_files": 50,
            }
        )
        self.assertEqual(disabled["DUMMER_INTEGRITY_CHECK"], "false")
        self.assertNotIn("DUMMER_INTEGRITY_MAX_FILES", disabled)


class WizardEnvFileTests(unittest.TestCase):
    def test_merge_env_file_preserves_unknowns_clears_old_sources_and_backs_up(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            dotenv = Path(tmp) / ".env"
            dotenv.write_text(
                "\n".join(
                    [
                        "# keep this comment",
                        "OTHER_SETTING=yes",
                        "DUMMER_PROCESSED_S3_BUCKET=old-bucket",
                        "DUMMER_PROCESSED_STATE=old-state.txt",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            result = merge_env_file(
                dotenv,
                {
                    "DUMMER_LOCAL_PATH": "/data/bundle",
                    "DUMMER_PROCESSED_STATE": "./processed_dirs.txt",
                    "DUMMER_CONFIG": "/home/dum/conf.ini",
                },
            )

            text = dotenv.read_text(encoding="utf-8")
            self.assertIsNotNone(result.backup_path)
            self.assertTrue(result.backup_path and result.backup_path.exists())
            self.assertIn("# keep this comment", text)
            self.assertIn("OTHER_SETTING=yes", text)
            self.assertIn("DUMMER_PROCESSED_STATE=./processed_dirs.txt", text)
            self.assertNotIn("old-bucket", text)
            self.assertNotIn("DUMMER_PROCESSED_S3_BUCKET", text)

    def test_ensure_auto_managed_state_files_creates_starter_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            script_dir = base / "setup"
            script_dir.mkdir()
            created_files: list[str] = []
            io = RecordingIO([])

            _ensure_auto_managed_state_files(
                io,
                {
                    "script_dir": str(script_dir),
                    "local_source": "crawl",
                    "processed_source": "state",
                    "processed_state": "./processed_dirs.txt",
                },
                created_files=created_files,
            )

            self.assertTrue((script_dir / "local_dirs.txt").is_file())
            self.assertFalse((script_dir / "processed_dirs.txt").exists())
            self.assertEqual(created_files, [str(script_dir / "local_dirs.txt")])

    def test_ensure_auto_managed_state_files_creates_generated_processed_inventory_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            created_files: list[str] = []
            io = RecordingIO([])

            _ensure_auto_managed_state_files(
                io,
                {
                    "script_dir": str(base),
                    "local_source": "manifest",
                    "processed_source": "s3",
                },
                created_files=created_files,
            )

            self.assertTrue((base / "local_dirs.txt").is_file())
            self.assertTrue((base / "processed_s3_dirs.txt").is_file())

    def test_ensure_auto_managed_state_files_skips_when_setup_dir_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            script_dir = base / "new-setup"
            created_files: list[str] = []
            io = RecordingIO([])

            _ensure_auto_managed_state_files(
                io,
                {
                    "script_dir": str(script_dir),
                    "local_source": "crawl",
                    "processed_source": "state",
                    "processed_state": str(script_dir / "processed_dirs.txt"),
                },
                created_files=created_files,
            )

            self.assertFalse(script_dir.exists())
            self.assertEqual(created_files, [])

    def test_ask_path_creates_directory_when_confirmed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "reports"
            io = RecordingIO([str(target), "yes"])
            created_directories: list[str] = []
            result = _ask_path(
                io,
                "Report directory",
                str(target),
                creatable="dir",
                created_directories=created_directories,
            )

            self.assertEqual(result, str(target))
            self.assertTrue(target.is_dir())
            self.assertIn(str(target), created_directories)

    def test_ask_path_does_not_create_when_user_declines(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "reports"
            io = RecordingIO([str(target), "no", "no"])
            created_directories: list[str] = []
            result = _ask_path(
                io,
                "Report directory",
                str(target),
                creatable="dir",
                created_directories=created_directories,
            )

            self.assertEqual(result, str(target))
            self.assertFalse(target.exists())
            self.assertEqual(created_directories, [])


class WizardCliTests(unittest.TestCase):
    def test_cli_routes_wizard_and_setup_aliases(self) -> None:
        with patch("dummer.cli.run_wizard", return_value=0) as run_wizard:
            self.assertEqual(cli.main(["wizard", "--yes"]), 0)
            run_wizard.assert_called_with(["--yes"])

        with patch("dummer.cli.run_wizard", return_value=0) as run_wizard:
            self.assertEqual(cli.main(["setup", "--yes"]), 0)
            run_wizard.assert_called_with(["--yes"])

    def test_script_entrypoint_uses_shared_wizard(self) -> None:
        import dummer_setup

        with patch("dummer.wizard.run_wizard", return_value=0) as run_wizard:
            self.assertEqual(dummer_setup.main(["--yes"]), 0)
            run_wizard.assert_called_with(["--yes"])

    def test_run_wizard_creates_paths_during_questions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            bundle = base / "bundle"
            config = base / "conf.ini"
            dum_binary = base / "pds-ingress-client"
            bundle.mkdir()
            config.write_text("[dum]\n", encoding="utf-8")
            dum_binary.write_text("#!/bin/sh\n", encoding="utf-8")

            scripted = RecordingIO(
                [
                    str(base),
                    str(bundle),
                    "2",  # files may change, so no manifest-store prompt
                    "1",
                    "1",
                    "no",
                    "no",
                    "1",
                    str(base / "processed_dirs.txt"),
                    "yes",
                    str(base),
                    "bundle",
                    str(config),
                    "sbn",
                    str(dum_binary),
                    "12",
                    str(base / "reports"),
                    "yes",
                    str(base / "pipeline"),
                    "yes",
                    "warn",
                    "no",
                    "1",
                    "no",
                ]
            )

            exit_code = run_wizard(["--yes", "--no-probe"], io=scripted)

            self.assertEqual(exit_code, 0)
            self.assertTrue((base / "local_dirs.txt").is_file())
            self.assertTrue((base / "processed_dirs.txt").is_file())
            self.assertTrue((base / "reports").is_dir())
            self.assertTrue((base / "pipeline").is_dir())
            self.assertIn("Created files:", scripted.output)
            self.assertIn("You're ready to run:", scripted.output)

    def test_prompts_use_human_language_not_env_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            bundle = base / "bundle"
            config = base / "conf.ini"
            dum_binary = base / "pds-ingress-client"
            bundle.mkdir()
            config.write_text("[dum]\n", encoding="utf-8")
            dum_binary.write_text("#!/bin/sh\n", encoding="utf-8")

            scripted = RecordingIO(
                [
                    str(base),  # setup/state directory
                    str(bundle),
                    "1",  # append-only
                    "1",  # local crawl
                    "1",  # all folders
                    "help",
                    "no",  # focus label
                    "no",  # summary anchor
                    "1",  # processed state
                    str(base / "processed_dirs.txt"),
                    "yes",
                    str(base),
                    "bundle",
                    str(config),
                    "sbn",
                    str(dum_binary),
                    "12",
                    str(base / "reports"),
                    "yes",
                    str(base / "pipeline"),
                    "yes",
                    "warn",
                    "no",  # direct files
                    "yes",  # manifest store
                    str(base / "dum-manifests"),
                    "yes",
                    "1",  # run all
                    "no",  # interactive
                ]
            )
            result = collect_answers(
                io=scripted,
                existing={},
                output_path=base / ".env",
                allow_probe=False,
            )

        self.assertTrue(result.answers["append_only"])
        joined_questions = "\n".join(scripted.questions)
        self.assertNotIn("DUMMER_", joined_questions)
        self.assertNotIn("--crawl-max-depth", joined_questions)
        self.assertIn("Limit this setup to paths containing one label?", joined_questions)
        self.assertNotIn("Limit this setup to one batch, year, or path label?", joined_questions)
        self.assertIn("Where is the data/bundle directory containing the files you want to upload?", joined_questions)
        self.assertIn("Dummer first makes each file path relative to the data/bundle", scripted.output)
        self.assertIn("only paths with one matching part are", scripted.output)
        self.assertIn("included; everything else is ignored by this setup", scripted.output)
        self.assertNotIn("batch, year", scripted.output)
        self.assertNotIn("Create or refresh your .env file", scripted.output)
        self.assertNotIn("Plain-language answers", scripted.output)

    def test_help_can_be_requested_at_questions(self) -> None:
        scripted = RecordingIO(["help", "/data/bundle"])

        answer = scripted.ask(
            "Where is the data/bundle directory containing the files you want to upload?",
            "",
            current="not set",
            help_text="This is the top-level data/bundle directory containing the files you want to upload.",
        )

        self.assertEqual(answer, "/data/bundle")
        self.assertIn("This is the top-level data/bundle directory", scripted.output)
        self.assertNotIn("\033[2m", scripted.output)

    def test_help_body_uses_default_foreground_not_faint(self) -> None:
        scripted = RecordingIO(["help", "yes"])
        scripted.ask_yes_no("Continue?", True, help_text="Readable help should not use faint styling.")
        self.assertIn("Readable help should not use faint styling.", scripted.output)
        self.assertNotIn("\033[2m", scripted.output)

    def test_choice_help_explains_options_without_accepting_help_as_answer(self) -> None:
        scripted = RecordingIO(["help", "2"])

        answer = scripted.choose(
            "How should local files be counted?",
            (
                ("crawl", "Scan the data/bundle directory each run."),
                ("manifest", "Use an existing file list."),
            ),
            "crawl",
            help_text="Scanning is simplest. A file list is faster if another system already produces one.",
        )

        self.assertEqual(answer, "manifest")
        self.assertIn("Scanning is simplest", scripted.output)

    def test_missing_data_bundle_can_be_reentered(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            missing = base / "missing"
            bundle = base / "bundle"
            config = base / "conf.ini"
            dum_binary = base / "pds-ingress-client"
            bundle.mkdir()
            config.write_text("[dum]\n", encoding="utf-8")
            dum_binary.write_text("#!/bin/sh\n", encoding="utf-8")

            scripted = RecordingIO(
                [
                    str(base),
                    str(missing),
                    "yes",  # re-enter missing bundle path
                    str(bundle),
                    "1",
                    "1",
                    "1",
                    "no",
                    "no",
                    "1",
                    str(base / "processed_dirs.txt"),
                    "yes",
                    str(base),
                    "bundle",
                    str(config),
                    "sbn",
                    str(dum_binary),
                    "12",
                    str(base / "reports"),
                    "yes",
                    str(base / "pipeline"),
                    "yes",
                    "warn",
                    "no",
                    "yes",
                    str(base / "dum-manifests"),
                    "yes",
                    "1",
                    "no",
                ]
            )

            result = collect_answers(
                io=scripted,
                existing={},
                output_path=base / ".env",
                allow_probe=False,
            )

        self.assertEqual(result.answers["local_path"], str(bundle))
        self.assertIn("Could not find that data/bundle directory", scripted.output)
        self.assertIn("Try a different path?", "\n".join(scripted.questions))

    def test_creatable_path_issue_detects_unwritable_parent(self) -> None:
        from dummer.wizard import _creatable_path_issue

        with tempfile.TemporaryDirectory() as tmp:
            readonly = Path(tmp) / "readonly"
            readonly.mkdir()
            readonly.chmod(0o555)
            try:
                issue = _creatable_path_issue(str(readonly / "reports"), "dir")
            finally:
                readonly.chmod(0o755)

            self.assertIsNotNone(issue)
            self.assertIn("permission", issue.lower())

    def test_creatable_progress_file_prompts_to_create_not_revalidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "new_progress.txt"
            io = RecordingIO([str(target), "yes"])
            created_files: list[str] = []
            result = _ask_path(
                io,
                "Processed progress file",
                str(target),
                creatable="file",
                created_files=created_files,
            )
            self.assertEqual(result, str(target))
            self.assertTrue(target.is_file())
            self.assertIn(str(target), created_files)
            self.assertIn("Create a new empty progress file there?", "\n".join(io.questions))
            self.assertNotIn("Could not find", io.output)

    def test_existing_env_values_are_shown_as_current(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            bundle = base / "bundle"
            config = base / "conf.ini"
            dum_binary = base / "pds-ingress-client"
            reports = base / "reports"
            pipeline = base / "pipeline"
            manifests = base / "manifests"
            bundle.mkdir()
            reports.mkdir()
            pipeline.mkdir()
            manifests.mkdir()
            (base / "processed_dirs.txt").touch()
            config.write_text("[dum]\n", encoding="utf-8")
            dum_binary.write_text("#!/bin/sh\n", encoding="utf-8")

            existing = {
                "DUMMER_SCRIPT_DIR": str(base),
                "DUMMER_LOCAL_PATH": str(bundle),
                "DUMMER_PROCESSED_STATE": str(base / "processed_dirs.txt"),
                "DUMMER_PREFIX": str(base),
                "DUMMER_BUNDLE": "bundle",
                "DUMMER_CONFIG": str(config),
                "DUMMER_NAME": "sbn",
                "DUMMER_DUM_BINARY": str(dum_binary),
                "DUMMER_THREADS": "6",
                "DUMMER_REPORT_DIR": str(reports),
                "DUMMER_PIPELINE_REPORT_DIR": str(pipeline),
                "DUMMER_LOG_LEVEL": "warn",
                "DUMMER_DUM_MANIFEST_STORE": str(manifests),
                "DUMMER_LOOP": "true",
                "DUMMER_INTERACTIVE": "false",
            }
            scripted = RecordingIO(
                [
                    "",  # keep script dir
                    "",  # keep local path
                    "1",
                    "1",
                    "1",
                    "no",
                    "no",
                    "1",
                    "",  # keep processed state
                    "",  # keep prefix
                    "",  # keep bundle
                    "",  # keep config
                    "",  # keep name
                    "",  # keep binary
                    "",  # keep threads
                    "",  # keep reports
                    "",  # keep pipeline
                    "",  # keep log level
                    "no",
                    "yes",
                    "",  # keep manifest store
                    "1",
                    "no",
                ]
            )

            collect_answers(
                io=scripted,
                existing=existing,
                output_path=base / ".env",
                allow_probe=False,
            )

        output = scripted.output
        self.assertIn(f"Current: {bundle}", output)
        self.assertIn("Current: Use the processed progress file this setup updates.", output)
        self.assertIn(f"Current: {base / 'processed_dirs.txt'}", output)
        self.assertIn("Current: sbn", output)
        self.assertIn("Current: 6", output)
        self.assertIn("Current: yes", output)

    def test_script_dir_answer_loads_existing_env_from_that_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            script_dir = base / "setup"
            bundle = base / "bundle"
            config = base / "conf.ini"
            dum_binary = base / "pds-ingress-client"
            reports = base / "reports"
            pipeline = base / "pipeline"
            manifests = base / "manifests"
            script_dir.mkdir()
            bundle.mkdir()
            reports.mkdir()
            pipeline.mkdir()
            manifests.mkdir()
            config.write_text("[dum]\n", encoding="utf-8")
            dum_binary.write_text("#!/bin/sh\n", encoding="utf-8")
            (script_dir / ".env").write_text(
                "\n".join(
                    [
                        f"DUMMER_LOCAL_PATH={bundle}",
                        f"DUMMER_PROCESSED_STATE={script_dir / 'processed_dirs.txt'}",
                        f"DUMMER_PREFIX={base}",
                        "DUMMER_BUNDLE=bundle",
                        f"DUMMER_CONFIG={config}",
                        "DUMMER_NAME=sbn",
                        f"DUMMER_DUM_BINARY={dum_binary}",
                        "DUMMER_THREADS=5",
                        f"DUMMER_REPORT_DIR={reports}",
                        f"DUMMER_PIPELINE_REPORT_DIR={pipeline}",
                        f"DUMMER_DUM_MANIFEST_STORE={manifests}",
                        "DUMMER_LOOP=true",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            scripted = RecordingIO(
                [
                    str(script_dir),
                    "",
                    "1",
                    "1",
                    "1",
                    "no",
                    "no",
                    "1",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "warn",
                    "no",
                    "yes",
                    "",
                    "1",
                    "no",
                ]
            )

            result = collect_answers(
                io=scripted,
                existing={},
                output_path=base / ".env",
                allow_probe=False,
            )

        self.assertEqual(result.output_path, script_dir / ".env")
        self.assertEqual(result.answers["local_path"], str(bundle))
        self.assertIn(f"Loaded existing setup from {script_dir / '.env'}", scripted.output)

    def test_ctrl_c_exits_without_traceback_or_write(self) -> None:
        class InterruptingIO(WizardIO):
            def __init__(self) -> None:
                super().__init__(io.StringIO(), io.StringIO())

            def _readline(self, prompt: str) -> str:
                raise KeyboardInterrupt

            @property
            def output(self) -> str:
                return self.outstream.getvalue()

        with tempfile.TemporaryDirectory() as tmp:
            dotenv = Path(tmp) / ".env"
            scripted = InterruptingIO()

            exit_code = cli.run_wizard(["--output", str(dotenv)], io=scripted)

        self.assertEqual(exit_code, 130)
        self.assertIn("Setup cancelled", scripted.output)
        self.assertIn("No changes were written.", scripted.output)
        self.assertNotIn("Traceback", scripted.output)
        self.assertFalse(dotenv.exists())


if __name__ == "__main__":
    unittest.main()
