from __future__ import annotations

import argparse
import os
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Literal
from urllib.parse import urlencode
from urllib.request import urlopen

from .runtime_config import DEFAULTS, ENV_VAR_NAMES, read_dotenv
from .utils import sanitize_path
from .wizard_ui import WizardIO


WIZARD_DEST_ORDER: tuple[str, ...] = (
    "local_path",
    "local_state",
    "local_manifest",
    "local_root",
    "processed_state",
    "processed_manifest",
    "processed_crawl",
    "processed_root",
    "processed_s3_bucket",
    "processed_s3_prefix",
    "processed_s3_region",
    "processed_s3_known_dirs_file",
    "processed_s3_known_dirs_workers",
    "processed_s3_resume_from_state",
    "processed_s3_resume_cluster_depth",
    "processed_s3_max_retries",
    "processed_s3_retry_delay_seconds",
    "path_filter",
    "path_filter_depth",
    "crawl_min_depth",
    "crawl_max_depth",
    "summary_anchor_component",
    "bundle",
    "prefix",
    "config",
    "name",
    "dum_binary",
    "threads",
    "report_dir",
    "pipeline_report_dir",
    "dum_manifest_store",
    "script_dir",
    "log_level",
    "interactive",
    "direct_file_list_upload",
    "direct_file_list_batch_size",
    "max_dirs",
    "loop",
)
WIZARD_ENV_NAMES = tuple(ENV_VAR_NAMES[dest] for dest in WIZARD_DEST_ORDER)


@dataclass(frozen=True)
class EnvWriteResult:
    path: Path
    backup_path: Path | None


@dataclass(frozen=True)
class WizardAnswers:
    answers: dict[str, object]
    output_path: Path


def _existing_config(dotenv_path: Path) -> dict[str, str]:
    values = read_dotenv(dotenv_path)
    for env_name in WIZARD_ENV_NAMES:
        if env_name in os.environ:
            values[env_name] = os.environ[env_name]
    return values


def _current(existing: dict[str, str], dest: str) -> str | None:
    env_name = ENV_VAR_NAMES[dest]
    raw = existing.get(env_name)
    if raw not in (None, ""):
        return raw
    default = DEFAULTS.get(dest)
    if default is None:
        return None
    if isinstance(default, bool):
        return "true" if default else "false"
    return str(default)


def _configured(existing: dict[str, str], dest: str) -> str | None:
    raw = existing.get(ENV_VAR_NAMES[dest])
    if raw not in (None, ""):
        return raw
    return None


def _configured_bool(existing: dict[str, str], dest: str) -> bool | None:
    raw = _configured(existing, dest)
    if raw is None:
        return None
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _current_local_source(existing: dict[str, str]) -> str | None:
    if _configured(existing, "local_manifest"):
        return "Use an existing file list."
    if _configured(existing, "local_state"):
        return "Track local progress in a file."
    if _configured(existing, "local_path"):
        return "Scan the data/bundle directory each run."
    return None


def _current_processed_source(existing: dict[str, str]) -> str | None:
    if _configured(existing, "processed_s3_bucket"):
        return "List a public S3 bucket."
    if _configured(existing, "processed_manifest"):
        return "Use a file list from the destination."
    if _configured(existing, "processed_crawl"):
        return "Scan a destination mirror on disk."
    if _configured(existing, "processed_state"):
        return "Use the processed progress file this setup updates."
    return None


def _current_crawl_depth(existing: dict[str, str]) -> str | None:
    min_depth = _configured(existing, "crawl_min_depth")
    max_depth = _configured(existing, "crawl_max_depth")
    if min_depth is None and max_depth is None:
        return None
    if min_depth == max_depth and min_depth is not None:
        return f"Only folder level {min_depth} below the data/bundle directory."
    if max_depth == "0":
        return "Only files directly inside the data/bundle directory."
    if max_depth == "1" and min_depth is None:
        return "Only one folder level below the data/bundle directory."
    if min_depth and max_depth:
        return f"Folder levels {min_depth} through {max_depth} below the data/bundle directory."
    if max_depth:
        return f"Folders up to level {max_depth} below the data/bundle directory."
    return f"Folders at level {min_depth} or deeper below the data/bundle directory."


def _one_based_current(existing: dict[str, str], dest: str) -> str | None:
    raw = _configured(existing, dest)
    if raw is None:
        return None
    try:
        return str(int(raw) + 1)
    except ValueError:
        return raw


def _current_run_style(existing: dict[str, str]) -> str | None:
    loop = _configured_bool(existing, "loop")
    if loop is True:
        return "Keep going until there is no more pending work or something fails."
    if loop is False:
        max_dirs = _configured(existing, "max_dirs")
        if max_dirs:
            return f"Only process {max_dirs} folder(s) each run."
        return "Only process a limited number of folders each run."
    return None


def _creatable_path_issue(path: str, kind: Literal["file", "dir"]) -> str | None:
    resolved = Path(sanitize_path(path))
    if kind == "file":
        if resolved.exists():
            if resolved.is_dir():
                return f"{resolved} is a directory, not a file path."
            if not os.access(resolved, os.W_OK):
                return f"No write permission for {resolved}."
            return None
        target = resolved.parent
    else:
        target = resolved

    if target.exists():
        if not target.is_dir():
            return f"{target} exists but is not a directory."
        if not os.access(target, os.W_OK):
            return f"No write permission for {target}."
        return None

    probe = target
    while not probe.exists() and probe.parent != probe:
        probe = probe.parent
    if not probe.exists():
        return f"No parent directory exists to create {target}."
    if not os.access(probe, os.W_OK):
        return f"No permission to create {target} under {probe}."
    return None


def _accept_creatable_path(
    io: WizardIO,
    answer: str,
    kind: Literal["file", "dir"],
) -> str | None:
    issue = _creatable_path_issue(answer, kind)
    if issue:
        io.warn(issue)
        if io.ask_yes_no("Enter a different path?", True):
            return None
        io.note("Keeping that value. Dummer will create it on the first run if needed.")
    return answer


def _create_directory_now(
    io: WizardIO,
    path: str,
    *,
    created_directories: list[str] | None = None,
) -> bool:
    resolved = Path(sanitize_path(path))
    if resolved.exists():
        if resolved.is_dir():
            return True
        io.warn(f"{resolved} exists but is not a directory.")
        return False
    try:
        resolved.mkdir(parents=True, exist_ok=True)
        io.success(f"Created directory {resolved}")
        if created_directories is not None:
            created_directories.append(str(resolved))
        return True
    except OSError as exc:
        io.warn(f"Could not create directory {path}: {exc}")
        return False


def _create_file_now(
    io: WizardIO,
    path: str,
    *,
    created_files: list[str] | None = None,
) -> bool:
    resolved = Path(sanitize_path(path))
    if resolved.exists():
        if resolved.is_file():
            return True
        io.warn(f"{resolved} exists but is not a file.")
        return False
    try:
        resolved.parent.mkdir(parents=True, exist_ok=True)
        resolved.touch(exist_ok=True)
        io.success(f"Created file {resolved}")
        if created_files is not None:
            created_files.append(str(resolved))
        return True
    except OSError as exc:
        io.warn(f"Could not create file {path}: {exc}")
        return False


def _ask_path(
    io: WizardIO,
    question: str,
    default: str | None,
    *,
    current: str | None = None,
    required: bool = False,
    must_exist: bool = False,
    creatable: Literal["file", "dir"] | None = None,
    missing_label: str = "path",
    help_text: str | tuple[str, ...] | None = None,
    created_files: list[str] | None = None,
    created_directories: list[str] | None = None,
) -> str:
    while True:
        display_current = current
        if display_current in (None, "") and required:
            display_current = "not set"
        answer = io.ask(question, default or "", current=display_current, help_text=help_text)
        if answer or not required:
            pass
        else:
            io.warn("Enter a path, or press Ctrl-C to exit.")
            continue

        if not answer:
            return answer

        resolved = Path(sanitize_path(answer))
        if resolved.exists():
            return answer

        if must_exist:
            io.warn(f"Could not find that {missing_label}: {answer}")
            if io.ask_yes_no("Try a different path?", True):
                default = answer
                continue
            io.note("Keeping that value — useful when preparing setup for another machine.")
            return answer

        if creatable == "file":
            io.note(f"No file yet at {answer}.")
            if io.ask_yes_no("Create a new empty progress file there?", True):
                accepted = _accept_creatable_path(io, answer, "file")
                if accepted is None:
                    default = answer
                    continue
                if _create_file_now(io, accepted, created_files=created_files):
                    return accepted
                if io.ask_yes_no("Enter a different path?", True):
                    default = answer
                    continue
                io.note("Keeping that value. Dummer will create it on the first run if needed.")
                return accepted
        elif creatable == "dir":
            io.note(f"No directory yet at {answer}.")
            if io.ask_yes_no("Create that directory?", True):
                accepted = _accept_creatable_path(io, answer, "dir")
                if accepted is None:
                    default = answer
                    continue
                if _create_directory_now(io, accepted, created_directories=created_directories):
                    return accepted
                if io.ask_yes_no("Enter a different path?", True):
                    default = answer
                    continue
                io.note("Keeping that value. Dummer will create it on the first run if needed.")
                return accepted

        if creatable is not None:
            if io.ask_yes_no("Enter a different path?", True):
                default = answer
                continue
            io.note("Keeping that value. Dummer will create it on the first run if needed.")
            return answer

        return answer


def _one_based_component_to_index(value: int | None) -> int | None:
    if value is None:
        return None
    return value - 1


def _path_parent(path: str | None) -> str | None:
    if not path:
        return None
    return sanitize_path(str(Path(path).parent))


def _sample_relative_file_dirs(local_path: str | None, limit: int = 6) -> list[str]:
    if not local_path:
        return []
    base = Path(sanitize_path(local_path))
    if not base.is_dir():
        return []
    samples: list[str] = []
    for root, _dirs, files in os.walk(base):
        direct_files = [name for name in files if not name.startswith(".")]
        if not direct_files:
            continue
        root_path = Path(root)
        rel_dir = "." if root_path == base else root_path.relative_to(base).as_posix()
        samples.append(rel_dir)
        if len(samples) >= limit:
            break
    return samples


def _looks_like_path(path: str | None) -> bool:
    return bool(path and Path(sanitize_path(path)).exists())


def _probe_public_s3(bucket: str, prefix: str | None, region: str | None) -> str:
    base = f"https://{bucket}.s3.amazonaws.com"
    if region:
        base = f"https://{bucket}.s3.{region}.amazonaws.com"
    params = {"list-type": "2", "max-keys": "1"}
    if prefix:
        params["prefix"] = prefix.lstrip("/")
    url = f"{base}/?{urlencode(params)}"
    with urlopen(url, timeout=10) as response:
        response.read(4096)
    return url


HELP_SETUP_DIR = (
    "This is where the saved setup file and Dummer progress files should live.",
    "Use a stable directory that will still exist when scheduled or repeated uploads run.",
    "The data itself does not need to be inside this directory.",
    "If it does not exist yet, Dummer can create it when you confirm during setup.",
)
HELP_BUNDLE_DIR = (
    "This is the top-level data/bundle directory containing the files you want to upload.",
    "Choose the directory that all upload paths should be understood relative to.",
)
HELP_APPEND_ONLY = (
    "This decides whether Dummer can safely reuse work from earlier runs.",
    "Choose no when existing files, labels, inventories, and data are stable once they appear. New files and folders can still be added.",
    "Choose yes or unsure if older files can be edited, renamed, removed, replaced, or relabeled later.",
    "Stable data lets Dummer offer checksum caching later in setup. You can still override that saved choice on future runs.",
)
HELP_LOCAL_SOURCE = (
    "Dummer needs a local inventory before it can decide what still needs upload.",
    "Scanning is simplest. A file list is faster if another system already produces one.",
    "A progress file lets Dummer reuse folder counts from earlier runs; Dummer can create an empty one if needed.",
)
HELP_UPLOAD_FOLDERS = (
    "Dummer sends upload work in folder-sized units by default. You can override that later if needed.",
    "Pick the folder level that represents one batch, collection, date, or other unit you want tracked.",
    "If you are unsure, use the option that treats every folder with direct files as an upload folder.",
)
HELP_PATH_FILTER = (
    "This limits which upload folders this setup considers when building and comparing inventory.",
    "Dummer first makes each file path relative to the data/bundle directory, then splits that relative path into slash-separated parts.",
    "If you enter a label, only paths with one matching part are included; everything else is ignored by this setup.",
    "Leave it off when one setup should cover the whole data/bundle directory.",
)
HELP_PROCESSED_SOURCE = (
    "This is how Dummer checks what has already reached the destination before doing new work.",
    "Use a progress file if Dummer should track processed folders itself — an empty file can be created during setup.",
    "Use a file list if one already exists, a filesystem mirror if processed files are mounted locally, or public S3 if uploaded files can be listed anonymously.",
)
HELP_REPORT_DIR = (
    "Upload-client reports are written here after each upload attempt.",
    "If it does not exist yet, Dummer can create it when you confirm during setup.",
)
HELP_PIPELINE_REPORT_DIR = (
    "Dummer run summaries and reconciliation reports are written here.",
    "If it does not exist yet, Dummer can create it when you confirm during setup.",
)
HELP_PROGRESS_FILE = (
    "Dummer reads and updates this tab-separated progress file each run.",
    "If the file does not exist yet, Dummer can create an empty one when you confirm during setup.",
)
HELP_PREFIX = (
    "This removes a common parent path so reports show useful relative upload paths.",
    "For example, stripping /data from /data/bundle/2026/file.txt leaves bundle/2026/file.txt.",
)
HELP_BUNDLE_LABEL = (
    "This short name appears in reports and generated upload paths.",
    "Use a stable human label for the data bundle, such as a collection name or top-level directory name.",
)
HELP_UPLOAD_CLIENT = (
    "Dummer runs the upload client after it decides which work is pending.",
    "These values tell Dummer where that client is, which client configuration to use, and where reports should be written.",
)
HELP_MANIFEST_STORE = (
    "Choose yes to let DUM cache per-folder checksum manifests for append-only data.",
    "On later runs, those cached manifests let DUM avoid repeating checksum work for files it has already seen.",
    "This assumes existing files, labels, inventories, and data do not change after they appear.",
    "If it does not exist yet, Dummer can create it when you confirm during setup.",
    "You can change this later by rerunning setup, or override it for one run with the command-line option.",
)


def answers_to_env(answers: dict[str, object]) -> dict[str, str]:
    env: dict[str, object] = {
        "local_path": answers.get("local_path"),
        "script_dir": answers.get("script_dir") or ".",
        "config": answers.get("config"),
        "name": answers.get("name"),
        "dum_binary": answers.get("dum_binary") or DEFAULTS["dum_binary"],
        "threads": answers.get("threads") or DEFAULTS["threads"],
        "report_dir": answers.get("report_dir"),
        "pipeline_report_dir": answers.get("pipeline_report_dir"),
        "bundle": answers.get("bundle"),
        "prefix": answers.get("prefix"),
        "log_level": answers.get("log_level") or DEFAULTS["log_level"],
    }

    if answers.get("local_source") == "state":
        env["local_state"] = answers.get("local_state")
    elif answers.get("local_source") == "manifest":
        env["local_manifest"] = answers.get("local_manifest")
        env["local_root"] = answers.get("local_root")

    processed_source = answers.get("processed_source")
    if processed_source == "state":
        env["processed_state"] = answers.get("processed_state")
    elif processed_source == "manifest":
        env["processed_manifest"] = answers.get("processed_manifest")
        env["processed_root"] = answers.get("processed_root")
    elif processed_source == "crawl":
        env["processed_crawl"] = answers.get("processed_crawl")
        env["processed_root"] = answers.get("processed_root")
    elif processed_source == "s3":
        env["processed_s3_bucket"] = answers.get("processed_s3_bucket")
        env["processed_s3_prefix"] = answers.get("processed_s3_prefix")
        env["processed_root"] = answers.get("processed_root")
        env["processed_s3_region"] = answers.get("processed_s3_region")
        env["processed_s3_known_dirs_file"] = answers.get("processed_s3_known_dirs_file")
        env["processed_s3_known_dirs_workers"] = answers.get("processed_s3_known_dirs_workers") or DEFAULTS[
            "processed_s3_known_dirs_workers"
        ]
        env["processed_s3_resume_from_state"] = bool(answers.get("processed_s3_resume_from_state"))
        env["processed_s3_resume_cluster_depth"] = answers.get("processed_s3_resume_cluster_depth") or DEFAULTS[
            "processed_s3_resume_cluster_depth"
        ]
        env["processed_s3_max_retries"] = answers.get("processed_s3_max_retries") or DEFAULTS["processed_s3_max_retries"]
        env["processed_s3_retry_delay_seconds"] = answers.get("processed_s3_retry_delay_seconds") or DEFAULTS[
            "processed_s3_retry_delay_seconds"
        ]

    env["path_filter"] = answers.get("path_filter")
    env["path_filter_depth"] = answers.get("path_filter_depth")
    env["crawl_min_depth"] = answers.get("crawl_min_depth")
    env["crawl_max_depth"] = answers.get("crawl_max_depth")
    env["summary_anchor_component"] = answers.get("summary_anchor_component")

    direct_file_list_upload = bool(answers.get("direct_file_list_upload"))
    env["direct_file_list_upload"] = direct_file_list_upload
    env["direct_file_list_batch_size"] = answers.get("direct_file_list_batch_size") or DEFAULTS["direct_file_list_batch_size"]
    if not direct_file_list_upload:
        env["dum_manifest_store"] = answers.get("dum_manifest_store")

    env["loop"] = bool(answers.get("loop"))
    env["max_dirs"] = answers.get("max_dirs") or DEFAULTS["max_dirs"]
    env["interactive"] = bool(answers.get("interactive"))

    rendered: dict[str, str] = {}
    for dest in WIZARD_DEST_ORDER:
        value = env.get(dest)
        if value in (None, ""):
            continue
        if isinstance(value, bool):
            rendered[ENV_VAR_NAMES[dest]] = "true" if value else "false"
        else:
            rendered[ENV_VAR_NAMES[dest]] = str(value)
    return rendered


def _quote_env(value: str) -> str:
    if value == "":
        return ""
    if any(ch.isspace() for ch in value) or "#" in value or value.startswith(("'", '"')):
        return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return value


def merge_env_file(dotenv_path: Path, generated: dict[str, str]) -> EnvWriteResult:
    dotenv_path.parent.mkdir(parents=True, exist_ok=True)
    backup_path: Path | None = None
    preserved_lines: list[str] = []
    if dotenv_path.exists():
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        backup_path = dotenv_path.with_name(f"{dotenv_path.name}.backup-{timestamp}")
        shutil.copy2(dotenv_path, backup_path)
        for line in dotenv_path.read_text(encoding="utf-8").splitlines():
            raw = line.strip()
            if "=" in raw and not raw.startswith("#"):
                key = raw.split("=", 1)[0].strip()
                if key in WIZARD_ENV_NAMES:
                    continue
            preserved_lines.append(line)

    lines: list[str] = []
    lines.extend(preserved_lines)
    if lines and lines[-1].strip():
        lines.append("")
    lines.append("# Generated by Dummer setup. Edit these values or override them on the command line.")
    for dest in WIZARD_DEST_ORDER:
        env_name = ENV_VAR_NAMES[dest]
        if env_name in generated:
            lines.append(f"{env_name}={_quote_env(generated[env_name])}")
    dotenv_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return EnvWriteResult(path=dotenv_path, backup_path=backup_path)


def _setup_relative_path(raw_path: object, script_dir: Path) -> Path | None:
    if raw_path in (None, ""):
        return None
    path = Path(str(raw_path))
    if path.is_absolute():
        return path
    return script_dir / path


def _generated_local_state_path(script_dir: Path) -> Path:
    return script_dir / "local_dirs.txt"


def _generated_processed_state_path(script_dir: Path, processed_source: object) -> Path:
    if processed_source == "manifest":
        return script_dir / "processed_parse_dirs.txt"
    return script_dir / "processed_s3_dirs.txt"


def _state_paths_to_create(answers: dict[str, object], script_dir: Path) -> Iterable[tuple[Path, bool]]:
    if answers.get("local_source") == "state":
        local_state = _setup_relative_path(answers.get("local_state"), script_dir)
        if local_state is not None:
            yield local_state, False
    else:
        yield _generated_local_state_path(script_dir), True

    processed_source = answers.get("processed_source")
    if processed_source == "state":
        processed_state = _setup_relative_path(answers.get("processed_state"), script_dir)
        if processed_state is not None:
            yield processed_state, False
    else:
        yield _generated_processed_state_path(script_dir, processed_source), True


def _ensure_auto_managed_state_files(
    io: WizardIO,
    answers: dict[str, object],
    *,
    created_files: list[str],
) -> None:
    script_dir = Path(sanitize_path(str(answers.get("script_dir") or ".")))
    if not script_dir.is_dir():
        return
    for path, auto_managed in _state_paths_to_create(answers, script_dir):
        if not auto_managed or path.exists():
            continue
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch(exist_ok=True)
            io.success(f"Created starter file {path}")
            created_files.append(str(path))
        except OSError as exc:
            io.warn(f"Could not create starter file {path}: {exc}")


def collect_answers(
    *,
    io: WizardIO,
    existing: dict[str, str],
    output_path: Path,
    explicit_output: bool = False,
    allow_probe: bool = True,
) -> WizardAnswers:
    answers: dict[str, object] = {}
    created_files: list[str] = []
    created_directories: list[str] = []
    io.welcome()

    io.section("Workspace", hint="Where to save this setup and progress records between runs.")
    answers["script_dir"] = _ask_path(
        io,
        "Where should setup and progress files be saved?",
        _current(existing, "script_dir") or ".",
        current=_configured(existing, "script_dir"),
        required=True,
        creatable="dir",
        help_text=HELP_SETUP_DIR,
        created_files=created_files,
        created_directories=created_directories,
    )
    script_dir = sanitize_path(str(answers["script_dir"]))
    actual_output_path = output_path if explicit_output else Path(script_dir) / ".env"
    script_dotenv = Path(script_dir) / ".env"
    if script_dotenv.exists() and script_dotenv != output_path:
        existing.update(_existing_config(script_dotenv))
        existing[ENV_VAR_NAMES["script_dir"]] = script_dir
        io.success(f"Loaded existing setup from {script_dotenv}")

    answers["local_path"] = _ask_path(
        io,
        "Where is the data/bundle directory containing the files you want to upload?",
        _current(existing, "local_path"),
        current=_configured(existing, "local_path"),
        required=True,
        must_exist=True,
        missing_label="data/bundle directory",
        help_text=HELP_BUNDLE_DIR,
    )
    local_path = str(answers["local_path"])

    samples = _sample_relative_file_dirs(local_path) if allow_probe else []
    io.list_items("Example folders with files:", tuple(samples))

    io.section("Local data", hint="How local files should be counted before upload decisions are made.")
    change_style = io.choose(
        "Are existing files (labels, inventories, data) ever expected to change?",
        (
            ("append", "No - existing files stay the same; only new files or folders are added."),
            ("change", "Yes - existing files, labels, inventories, or data may be corrected later."),
            ("unsure", "Not sure yet."),
        ),
        "append",
        help_text=HELP_APPEND_ONLY,
    )
    answers["append_only"] = change_style == "append"
    if answers["append_only"]:
        io.note("Append-only data lets Dummer skip repeat work for stable folders.")
    else:
        io.note("Dummer won't assume previously uploaded folders stay unchanged.")

    local_source = io.choose(
        "How should local files be counted?",
        (
            ("crawl", "Scan the data/bundle directory each run."),
            ("manifest", "Use an existing file list."),
            ("state", "Track local progress in a file (empty file created if needed)."),
        ),
        "manifest" if _current(existing, "local_manifest") else "state" if _current(existing, "local_state") else "crawl",
        current=_current_local_source(existing),
        help_text=HELP_LOCAL_SOURCE,
    )
    answers["local_source"] = local_source
    if local_source == "manifest":
        answers["local_manifest"] = _ask_path(
            io,
            "Local file list path",
            _current(existing, "local_manifest"),
            current=_configured(existing, "local_manifest"),
            required=True,
            must_exist=True,
            missing_label="local file list",
            help_text="Enter the path to the text file that lists every local file in this data/bundle.",
        )
        answers["local_root"] = io.ask(
            "Strip this prefix from each listed path before comparing",
            _current(existing, "local_root") or "",
            current=_configured(existing, "local_root"),
            help_text="Use this when the file list includes a leading path that should not count as part of the upload path.",
        )
    elif local_source == "state":
        answers["local_state"] = _ask_path(
            io,
            "Local progress file",
            _current(existing, "local_state") or f"{script_dir}/local_dirs.txt",
            current=_configured(existing, "local_state"),
            required=True,
            creatable="file",
            help_text=HELP_PROGRESS_FILE,
            created_files=created_files,
        created_directories=created_directories,
        )

    io.section("Upload folders", hint="Choose the folder level that represents one upload unit.")
    depth_choice = io.choose(
        "Which folders should Dummer treat as upload folders?",
        (
            ("all", "Every folder that directly contains files."),
            ("root", "Only files in the root of the data/bundle directory."),
            ("children", "One level below the root."),
            ("exact", "A specific depth I'll specify."),
        ),
        "all",
        current=_current_crawl_depth(existing),
        help_text=HELP_UPLOAD_FOLDERS,
    )
    if depth_choice == "root":
        answers["crawl_max_depth"] = 0
    elif depth_choice == "children":
        answers["crawl_max_depth"] = 1
    elif depth_choice == "exact":
        depth = io.ask_int(
            "Folder depth below root (root files = 0)",
            None,
            current=_configured(existing, "crawl_max_depth"),
            help_text="Depth counts path parts below the data/bundle directory. Root files are depth 0; immediate child folders are depth 1.",
        )
        answers["crawl_min_depth"] = depth
        answers["crawl_max_depth"] = depth

    if io.ask_yes_no(
        "Limit this setup to paths containing one label?",
        bool(_current(existing, "path_filter")),
        current=bool(_configured(existing, "path_filter")) if _configured(existing, "path_filter") else None,
        help_text=HELP_PATH_FILTER,
    ):
        answers["path_filter"] = io.ask(
            "Path label to include as a whole path part",
            _current(existing, "path_filter") or "",
            current=_configured(existing, "path_filter"),
            help_text=(
                "Enter the exact directory name or path part to include.",
                "For example, with a relative path collection_a/batch_003/file.lbl, collection_a and batch_003 are labels you could filter on.",
            ),
        )
        filter_position = io.ask_int(
            "Position of that label, counting from the left (blank if unsure)",
            None,
            current=_one_based_current(existing, "path_filter_depth"),
            help_text="Leave this blank unless the label must appear at a specific position in the relative path.",
        )
        answers["path_filter_depth"] = _one_based_component_to_index(filter_position)

    if io.ask_yes_no(
        "Summarize reports by one path component?",
        bool(_current(existing, "summary_anchor_component")),
        current=bool(_configured(existing, "summary_anchor_component"))
        if _configured(existing, "summary_anchor_component")
        else None,
        help_text="Use this when reports should group work by a path part such as year, batch, or collection.",
    ):
        anchor_position = io.ask_int(
            "Which component, counting from the left (1 = first)",
            None,
            current=_one_based_current(existing, "summary_anchor_component"),
            help_text="Count path parts from the left in the upload path. For bundle/2026/day1/file.txt, bundle is 1 and 2026 is 2.",
        )
        answers["summary_anchor_component"] = _one_based_component_to_index(anchor_position)

    io.section("Processed state", hint="Where to compare against data that already exists at the destination.")
    processed_source = io.choose(
        "Where can Dummer check what has already reached the destination?",
        (
            ("state", "Use the processed progress file this setup updates."),
            ("manifest", "Use a file list from the destination."),
            ("crawl", "Scan a destination mirror on disk."),
            ("s3", "List a public S3 bucket."),
        ),
        "s3" if _current(existing, "processed_s3_bucket") else "state",
        current=_current_processed_source(existing),
        help_text=HELP_PROCESSED_SOURCE,
    )
    answers["processed_source"] = processed_source
    if processed_source == "state":
        answers["processed_state"] = _ask_path(
            io,
            "Processed progress file",
            _current(existing, "processed_state") or f"{script_dir}/processed_dirs.txt",
            current=_configured(existing, "processed_state"),
            required=True,
            creatable="file",
            help_text=HELP_PROGRESS_FILE,
            created_files=created_files,
        created_directories=created_directories,
        )
    elif processed_source == "manifest":
        answers["processed_manifest"] = _ask_path(
            io,
            "Processed file list",
            _current(existing, "processed_manifest"),
            current=_configured(existing, "processed_manifest"),
            required=True,
            must_exist=True,
            missing_label="processed file list",
            help_text="Enter the path to a text file listing files already present at the destination.",
        )
        answers["processed_root"] = io.ask(
            "Strip this prefix from processed paths before comparing",
            _current(existing, "processed_root") or "",
            current=_configured(existing, "processed_root"),
            help_text="Use this when destination paths include a leading prefix that local paths do not include.",
        )
    elif processed_source == "crawl":
        answers["processed_crawl"] = _ask_path(
            io,
            "Processed filesystem directory",
            _current(existing, "processed_crawl"),
            current=_configured(existing, "processed_crawl"),
            required=True,
            must_exist=True,
            missing_label="processed filesystem directory",
            help_text="Enter the local or mounted directory that mirrors files already present at the destination.",
        )
        answers["processed_root"] = io.ask(
            "Strip this prefix from processed paths before comparing",
            _current(existing, "processed_root") or "",
            current=_configured(existing, "processed_root"),
            help_text="Use this when destination paths include a leading prefix that local paths do not include.",
        )
    else:
        answers["processed_s3_bucket"] = io.ask(
            "S3 bucket name",
            _current(existing, "processed_s3_bucket") or "",
            current=_configured(existing, "processed_s3_bucket"),
            help_text="Enter only the bucket name, not an s3:// URL. The bucket must allow anonymous list access for this check.",
        )
        answers["processed_s3_prefix"] = io.ask(
            "Key prefix inside the bucket",
            _current(existing, "processed_s3_prefix") or "",
            current=_configured(existing, "processed_s3_prefix"),
            help_text="Enter the folder-like prefix inside the bucket where already-uploaded files live, or leave blank for the whole bucket.",
        )
        answers["processed_root"] = io.ask(
            "Strip this prefix from S3 paths before comparing",
            _current(existing, "processed_root") or "",
            current=_configured(existing, "processed_root"),
            help_text="Use this when S3 keys include a leading prefix that should not count as part of the local upload path.",
        )
        answers["processed_s3_region"] = io.ask(
            "AWS region (if required)",
            _current(existing, "processed_s3_region") or "",
            current=_configured(existing, "processed_s3_region"),
            help_text="Most public buckets work without this. Add a region only if anonymous listing requires a regional endpoint.",
        )
        if allow_probe and io.ask_yes_no(
            "Test anonymous bucket listing now?",
            True,
            help_text="This makes one small read-only request for at most one key. It does not upload, crawl locally, or save progress.",
        ):
            try:
                url = _probe_public_s3(
                    str(answers["processed_s3_bucket"]),
                    str(answers.get("processed_s3_prefix") or ""),
                    str(answers.get("processed_s3_region") or ""),
                )
                io.success(f"Public listing works: {url}")
            except Exception as exc:  # pragma: no cover - exact urllib failures vary by platform
                io.warn(f"Public listing failed: {exc}")
        if io.ask_yes_no(
            "Use a prebuilt list of processed folders for S3 counting?",
            bool(_current(existing, "processed_s3_known_dirs_file")),
            current=bool(_configured(existing, "processed_s3_known_dirs_file"))
            if _configured(existing, "processed_s3_known_dirs_file")
            else None,
            help_text="Use this if you already have a list of destination folders. It can make S3 reconciliation faster and more exact.",
        ):
            answers["processed_s3_known_dirs_file"] = io.ask(
                "Processed folder list path",
                _current(existing, "processed_s3_known_dirs_file") or "",
                current=_configured(existing, "processed_s3_known_dirs_file"),
                help_text="Enter the path to the file listing destination folders that should be checked.",
            )
            answers["processed_s3_known_dirs_workers"] = int(
                io.ask(
                    "Parallel folder checks",
                    _current(existing, "processed_s3_known_dirs_workers") or "10",
                    current=_configured(existing, "processed_s3_known_dirs_workers"),
                    help_text="Higher numbers may finish faster but make more simultaneous S3 list requests.",
                )
            )
        answers["processed_s3_resume_from_state"] = io.ask_yes_no(
            "Resume interrupted S3 scans from saved progress?",
            True,
            current=_configured_bool(existing, "processed_s3_resume_from_state"),
            help_text="Choose yes so long S3 checks can continue from saved progress after interruption.",
        )
        if answers["processed_s3_resume_from_state"]:
            answers["processed_s3_resume_cluster_depth"] = int(
                io.ask(
                    "Path parts per restart group",
                    _current(existing, "processed_s3_resume_cluster_depth") or "2",
                    current=_configured(existing, "processed_s3_resume_cluster_depth"),
                    help_text="This controls how broadly S3 scan progress is grouped for restart. The default is usually fine.",
                )
            )

    io.section("Paths & labels", hint="How upload paths should be displayed and matched.")
    answers["prefix"] = io.ask(
        "Parent path to strip from upload paths",
        _current(existing, "prefix") or _path_parent(local_path) or "",
        current=_configured(existing, "prefix"),
        help_text=HELP_PREFIX,
    )
    answers["bundle"] = io.ask(
        "Short dataset label for reports",
        _current(existing, "bundle") or Path(local_path).name if local_path else "",
        current=_configured(existing, "bundle"),
        help_text=HELP_BUNDLE_LABEL,
    )

    io.section("Upload client", hint="Connection to the program that performs the actual upload.")
    answers["config"] = _ask_path(
        io,
        "Upload client configuration file",
        _current(existing, "config"),
        current=_configured(existing, "config"),
        required=True,
        must_exist=True,
        missing_label="upload client configuration file",
        help_text=HELP_UPLOAD_CLIENT,
    )
    answers["name"] = io.ask(
        "Upload identity/name",
        _current(existing, "name") or "",
        current=_configured(existing, "name"),
        help_text="Enter the upload identity used by the upload client configuration, often a node or operator name.",
    )
    answers["dum_binary"] = _ask_path(
        io,
        "Upload client program path",
        _current(existing, "dum_binary") or str(DEFAULTS["dum_binary"]),
        current=_configured(existing, "dum_binary"),
        required=True,
        must_exist=True,
        missing_label="upload client program",
        help_text="Enter the executable path for the upload client program.",
    )
    answers["threads"] = int(
        io.ask(
            "Upload worker threads",
            _current(existing, "threads") or str(DEFAULTS["threads"]),
            current=_configured(existing, "threads"),
            help_text="This controls how many uploads the upload client may perform in parallel.",
        )
    )
    answers["report_dir"] = _ask_path(
        io,
        "Upload client report directory",
        _current(existing, "report_dir"),
        current=_configured(existing, "report_dir"),
        required=True,
        creatable="dir",
        help_text=HELP_REPORT_DIR,
        created_files=created_files,
        created_directories=created_directories,
    )
    answers["pipeline_report_dir"] = _ask_path(
        io,
        "Run summary report directory",
        _current(existing, "pipeline_report_dir"),
        current=_configured(existing, "pipeline_report_dir"),
        required=True,
        creatable="dir",
        help_text=HELP_PIPELINE_REPORT_DIR,
        created_files=created_files,
        created_directories=created_directories,
    )
    answers["log_level"] = io.ask(
        "Upload client log level",
        _current(existing, "log_level") or "warn",
        current=_configured(existing, "log_level"),
        help_text="Use warn for quieter normal runs, info for more detail, or debug when diagnosing a problem.",
    )

    io.section("Run behavior", hint="How much pending work to attempt and how much output to show.")
    direct_file_list = io.ask_yes_no(
        "Send individual file paths instead of whole folders?",
        _current(existing, "direct_file_list_upload") == "true",
        current=_configured_bool(existing, "direct_file_list_upload"),
        help_text=(
            "Leave this off unless you need it. Dummer uploads folder-by-folder by default. "
            "Turn this on only when files are not grouped into useful subdirectories, "
            "or when a folder is too large and must be split into smaller upload batches."
        ),
    )
    answers["direct_file_list_upload"] = direct_file_list
    if direct_file_list:
        answers["direct_file_list_batch_size"] = int(
            io.ask(
                "Maximum file paths per upload batch",
                _current(existing, "direct_file_list_batch_size") or "500",
                current=_configured(existing, "direct_file_list_batch_size"),
                help_text="When file-path mode is on, each upload command includes at most this many paths.",
            )
        )
    elif answers.get("append_only") and io.ask_yes_no(
        "Reuse checksum manifests for append-only folders?",
        bool(_current(existing, "dum_manifest_store")),
        current=bool(_configured(existing, "dum_manifest_store")) if _configured(existing, "dum_manifest_store") else None,
        help_text=HELP_MANIFEST_STORE,
    ):
        answers["dum_manifest_store"] = _ask_path(
            io,
            "Checksum helper directory",
            _current(existing, "dum_manifest_store") or f"{script_dir}/dum-manifests",
            current=_configured(existing, "dum_manifest_store"),
            creatable="dir",
            help_text="Choose where reusable checksum helper files should be stored.",
            created_files=created_files,
        created_directories=created_directories,
        )

    run_style = io.choose(
        "When a run finds pending folders, how far should it go?",
        (
            ("all", "Process everything until done or failure."),
            ("limited", "Stop after a fixed number of folders."),
        ),
        "all",
        current=_current_run_style(existing),
        help_text="Use all for unattended catch-up runs. Use a fixed number when each run should do a small controlled amount of work.",
    )
    answers["loop"] = run_style == "all"
    if run_style == "limited":
        answers["max_dirs"] = int(
            io.ask(
                "Folders per run",
                _current(existing, "max_dirs") or "1",
                current=_configured(existing, "max_dirs"),
                help_text="Enter how many upload folders one run should process before stopping.",
            )
        )
    answers["interactive"] = io.ask_yes_no(
        "Show live upload-client output while running?",
        _current(existing, "interactive") == "true",
        current=_configured_bool(existing, "interactive"),
        help_text="Choose yes when running by hand and you want live output. Choose no for quieter scheduled runs; reports are still written.",
    )

    _ensure_auto_managed_state_files(io, answers, created_files=created_files)

    io.section("Review")
    io.summary_panel(_human_summary_lines(answers))
    io.blank()
    io.note(f"Writes to {actual_output_path}")
    if actual_output_path.exists():
        io.note("Existing setup file will be backed up first.")
    answers["_created_files"] = tuple(created_files)
    answers["_created_directories"] = tuple(created_directories)
    return WizardAnswers(answers=answers, output_path=actual_output_path)


def _human_summary_lines(answers: dict[str, object]) -> tuple[str, ...]:
    lines = [
        f"Upload from {answers.get('local_path') or '(not set)'}",
    ]
    local_source = answers.get("local_source")
    if local_source == "crawl":
        lines.append("Inventory local files by scanning the bundle directory")
    elif local_source == "manifest":
        lines.append("Inventory local files from a file list")
    else:
        lines.append("Reuse an existing local progress file")

    processed_source = answers.get("processed_source")
    if processed_source == "s3":
        lines.append("Check processed files via public S3 listing")
    elif processed_source == "manifest":
        lines.append("Check processed files from a file list")
    elif processed_source == "crawl":
        lines.append("Check processed files from a filesystem mirror")
    else:
        lines.append("Check processed files from a progress file")

    if answers.get("path_filter"):
        lines.append(f"Focus on paths containing {answers['path_filter']}")
    if answers.get("dum_manifest_store"):
        lines.append("Reuse per-folder checksum helpers")
    if answers.get("direct_file_list_upload"):
        lines.append("Override folder uploads with individual file paths for flat or oversized folders")
    if answers.get("loop"):
        lines.append("Run until pending work is finished or a failure stops the run")
    else:
        lines.append(f"Process at most {answers.get('max_dirs') or 1} folder(s) per run")
    return tuple(lines)


def run_wizard(
    argv: list[str] | None = None,
    *,
    io: WizardIO | None = None,
) -> int:
    parser = argparse.ArgumentParser(
        prog="dummer setup",
        description="Create or update saved Dummer setup values.",
    )
    parser.add_argument("--output", default=None, help="Write setup values to this file instead of .env")
    parser.add_argument("--script-dir", default=None, help="Default setup/state directory to use when choosing .env")
    parser.add_argument("--yes", action="store_true", help="Write without asking for final confirmation")
    parser.add_argument("--no-probe", action="store_true", help="Skip path samples and public S3 list checks")
    ns = parser.parse_args([] if argv is None else argv)

    wizard_io = io or WizardIO()
    dotenv_path = Path(ns.output) if ns.output else Path(ns.script_dir or ".") / ".env"
    existing = _existing_config(dotenv_path)
    if ns.script_dir and ENV_VAR_NAMES["script_dir"] not in existing:
        existing[ENV_VAR_NAMES["script_dir"]] = ns.script_dir

    try:
        wizard_answers = collect_answers(
            io=wizard_io,
            existing=existing,
            output_path=dotenv_path,
            explicit_output=ns.output is not None,
            allow_probe=not ns.no_probe,
        )
        generated = answers_to_env(wizard_answers.answers)
        wizard_io.env_preview(generated, WIZARD_ENV_NAMES)
        wizard_io.blank()
        if not ns.yes and not wizard_io.ask_yes_no(
            "Write this setup file now?",
            True,
            help_text="Choose yes to save the generated setup values. If a file already exists, it is backed up first.",
        ):
            wizard_io.note("No changes written.")
            return 1

        result = merge_env_file(wizard_answers.output_path, generated)
        answers = wizard_answers.answers
        wizard_io.finish(
            env_path=str(result.path),
            backup_path=str(result.backup_path) if result.backup_path else None,
            created_files=tuple(str(path) for path in answers.get("_created_files", ())),
            created_directories=tuple(str(path) for path in answers.get("_created_directories", ())),
        )
        return 0
    except KeyboardInterrupt:
        wizard_io.cancelled()
        return 130


def main(argv: list[str] | None = None) -> int:
    return run_wizard(sys.argv[1:] if argv is None else argv)
