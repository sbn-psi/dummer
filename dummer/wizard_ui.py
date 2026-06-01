from __future__ import annotations

import os
import sys
import textwrap
from typing import TextIO

"""
Wizard terminal styling.

Readable text always uses the terminal's default foreground (no dim/faint).
Color is reserved for short accents — borders, arrows, keywords — so light
themes, dark themes, and high-contrast profiles stay legible.

Color is off when stdout is not a TTY or NO_COLOR is set.
"""


SECTIONS: tuple[str, ...] = (
    "Workspace",
    "Local data",
    "Upload folders",
    "Processed state",
    "Paths & labels",
    "Upload client",
    "Run behavior",
    "E2E verification",
    "Review",
)


def _color_enabled(outstream: TextIO | None) -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("TERM", "").lower() == "dumb":
        return False
    return outstream.isatty() if outstream else sys.stdout.isatty()


class _Palette:
    """Semantic roles — not raw SGR names — so usage stays theme-safe."""

    def __init__(self, enabled: bool) -> None:
        if enabled:
            self.reset = "\033[0m"
            self.bold = "\033[1m"
            self.accent = "\033[36m"  # interactive hints, borders
            self.section = "\033[1;34m"  # section rules
            self.prompt = "\033[35m"  # input arrow
            self.ok = "\033[32m"
            self.warn = "\033[33m"
            self.tag = "\033[32m"  # default / current labels
        else:
            self.reset = self.bold = ""
            self.accent = self.section = self.prompt = ""
            self.ok = self.warn = self.tag = ""


def _strip_styles(text: str, palette: _Palette) -> str:
    visible = text
    for token in (palette.bold, palette.reset, palette.accent, palette.ok, palette.tag, palette.section, palette.prompt):
        visible = visible.replace(token, "")
    return visible


def _box(
    title: str,
    body_lines: tuple[str, ...],
    *,
    width: int = 72,
    palette: _Palette,
    footer: str | None = None,
) -> str:
    inner = max(width - 4, 20)
    top = f"╭{'─' * (inner + 2)}╮"
    bottom = f"╰{'─' * (inner + 2)}╯"
    lines = [top]
    if title:
        pad = inner - len(_strip_styles(title, palette))
        lines.append(f"│ {title}{' ' * max(pad, 0)} │")
        if body_lines:
            lines.append(f"│ {' ' * inner} │")
    for raw in body_lines:
        for chunk in textwrap.wrap(raw, width=inner) or [""]:
            lines.append(f"│ {chunk.ljust(inner)} │")
    if footer:
        lines.append(f"│ {' ' * inner} │")
        pad = inner - len(_strip_styles(footer, palette))
        lines.append(f"│ {footer}{' ' * max(pad, 0)} │")
    lines.append(bottom)
    return "\n".join(lines)


class WizardIO:
    def __init__(
        self,
        instream: TextIO | None = None,
        outstream: TextIO | None = None,
        *,
        width: int = 72,
    ) -> None:
        self.instream = instream or sys.stdin
        self.outstream = outstream or sys.stdout
        self.width = width
        self._c = _Palette(_color_enabled(self.outstream))
        self._section_index = 0

    def write(self, text: str = "") -> None:
        print(text, file=self.outstream, flush=True)

    def blank(self, lines: int = 1) -> None:
        for _ in range(lines):
            self.write()

    def welcome(self) -> None:
        body = (
            "Tell Dummer where your upload data lives and how it changes over time.",
            "The answers are saved so future runs can use the same setup.",
        )
        footer = (
            f"{self._c.accent}?{self._c.reset}  "
            f"type {self._c.bold}{self._c.accent}help{self._c.reset} or "
            f"{self._c.bold}{self._c.accent}?{self._c.reset} at any prompt"
        )
        self.write(_box(f"{self._c.accent}Dummer setup{self._c.reset}", body, width=self.width, palette=self._c, footer=footer))
        self.blank(2)

    def _question_help_suffix(self) -> str:
        return f" (type {self._c.accent}help{self._c.reset} for more)"

    def show_help(self, help_text: str | tuple[str, ...] | None) -> None:
        self.blank()
        lines: tuple[str, ...]
        if help_text is None:
            lines = ("Use the current value, enter a new value, or press Ctrl-C to exit setup.",)
        elif isinstance(help_text, str):
            lines = tuple(help_text.splitlines())
        else:
            lines = help_text

        inner = max(self.width - 8, 24)
        border = "─" * inner
        self.write(f"  {self._c.accent}┌ help {border}{self._c.reset}")
        for line in lines:
            for chunk in textwrap.wrap(line, width=inner) or [""]:
                self.write(f"  {self._c.accent}│{self._c.reset}  {chunk}")
        self.write(f"  {self._c.accent}└{border}{self._c.reset}")

    def section(self, title: str, *, hint: str | None = None) -> None:
        if self._section_index < len(SECTIONS):
            step = self._section_index + 1
            total = len(SECTIONS)
            label = f"{SECTIONS[self._section_index]}  ({step}/{total})"
            self._section_index += 1
        else:
            label = title
        self.blank()
        rule = "─" * self.width
        self.write(f"{self._c.section}{rule}{self._c.reset}")
        self.write(f"{self._c.bold}{label}{self._c.reset}")
        self.write(f"{self._c.section}{rule}{self._c.reset}")
        if hint:
            self.write(f"  {hint}")
            self.blank()

    def note(self, text: str) -> None:
        self.write(f"  {self._c.accent}·{self._c.reset}  {text}")

    def success(self, text: str) -> None:
        self.write(f"  {self._c.ok}✓{self._c.reset}  {text}")

    def warn(self, text: str) -> None:
        self.write(f"  {self._c.warn}!{self._c.reset}  {text}")

    def show_current(self, value: str | None) -> None:
        if value not in (None, ""):
            self.write(f"  {self._c.bold}{self._c.accent}Current:{self._c.reset} {value}")

    def _write_question(self, question: str, *, current: str | None = None) -> None:
        self.blank()
        self.write(f"{self._c.bold}{question}{self._c.reset}{self._question_help_suffix()}")
        self.show_current(current)

    def _readline(self, prompt: str) -> str:
        print(prompt, end="", file=self.outstream, flush=True)
        answer = self.instream.readline()
        if answer == "":
            return ""
        return answer.strip()

    def ask(
        self,
        question: str,
        default: str | None = None,
        *,
        current: str | None = None,
        help_text: str | tuple[str, ...] | None = None,
    ) -> str:
        while True:
            self._write_question(question, current=current)
            suffix = f" {self._c.tag}[{default}]{self._c.reset}" if default not in (None, "") else ""
            answer = self._readline(f"  {self._c.prompt}→{self._c.reset}{suffix} ")
            if answer.strip().lower() in {"help", "?"}:
                self.show_help(help_text)
                continue
            if answer == "" and default is not None:
                return default
            return answer

    def ask_int(
        self,
        question: str,
        default: int | None = None,
        *,
        current: str | None = None,
        help_text: str | tuple[str, ...] | None = None,
    ) -> int | None:
        while True:
            raw = self.ask(
                question,
                "" if default is None else str(default),
                current=current,
                help_text=help_text,
            ).strip()
            if raw == "":
                return default
            try:
                return int(raw)
            except ValueError:
                self.warn("Enter a whole number, or leave blank for the default.")

    def ask_yes_no(
        self,
        question: str,
        default: bool,
        *,
        current: bool | None = None,
        help_text: str | tuple[str, ...] | None = None,
    ) -> bool:
        raw_current = None if current is None else ("yes" if current else "no")
        hint = f"{self._c.tag}[{'Y/n' if default else 'y/N'}]{self._c.reset}"
        while True:
            self._write_question(question, current=raw_current)
            raw = self._readline(f"  {self._c.prompt}→{self._c.reset} {hint} ").strip().lower()
            if raw in {"help", "?"}:
                self.show_help(help_text)
                continue
            if raw == "":
                return default
            if raw in {"y", "yes", "true", "1", "on"}:
                return True
            if raw in {"n", "no", "false", "0", "off"}:
                return False
            self.warn("Answer yes or no.")

    def choose(
        self,
        question: str,
        choices: tuple[tuple[str, str], ...],
        default: str,
        *,
        current: str | None = None,
        help_text: str | tuple[str, ...] | None = None,
    ) -> str:
        default_idx = next(idx for idx, (key, _label) in enumerate(choices, start=1) if key == default)

        def _render_choices() -> None:
            self._write_question(question, current=current)
            self.blank()
            for idx, (key, label) in enumerate(choices, start=1):
                tag = ""
                if key == default:
                    tag = f"  {self._c.tag}{'current' if current not in (None, '') else 'default'}{self._c.reset}"
                self.write(f"    {self._c.accent}{idx}.{self._c.reset}  {label}{tag}")

        _render_choices()

        while True:
            raw = self._readline(
                f"  {self._c.prompt}→{self._c.reset} Choose {self._c.tag}[{default_idx}]{self._c.reset} "
            )
            if raw.strip().lower() in {"help", "?"}:
                self.show_help(help_text)
                _render_choices()
                continue
            if raw == "":
                raw = str(default_idx)
            for idx, (key, _label) in enumerate(choices, start=1):
                if raw == str(idx) or raw.lower() == key.lower():
                    return key
            self.warn("Pick one of the numbers shown above.")

    def list_items(self, title: str, items: tuple[str, ...]) -> None:
        if not items:
            return
        self.blank()
        self.write(f"  {title}")
        for item in items:
            self.write(f"    {self._c.accent}•{self._c.reset}  {item}")

    def summary_panel(self, lines: tuple[str, ...]) -> None:
        body = tuple(f"  •  {line}" for line in lines)
        self.write(_box(f"{self._c.bold}Summary{self._c.reset}", body, width=self.width, palette=self._c))

    def env_preview(self, generated: dict[str, str], env_order: tuple[str, ...]) -> None:
        preview_lines = tuple(f"{name}={generated[name]}" for name in env_order if name in generated)
        if not preview_lines:
            return
        self.blank()
        self.write(_box(f"{self._c.bold}Generated configuration{self._c.reset}", preview_lines, width=self.width, palette=self._c))

    def finish(
        self,
        *,
        env_path: str,
        backup_path: str | None,
        created_files: tuple[str, ...] = (),
        created_directories: tuple[str, ...] = (),
        setup_failures: tuple[str, ...] = (),
    ) -> None:
        body: list[str] = [f"Setup saved to  {env_path}"]
        if backup_path:
            body.append(f"Backup at       {backup_path}")
        if created_files:
            body.append("Created files:   " + ", ".join(created_files))
        if created_directories:
            body.append("Created dirs:    " + ", ".join(created_directories))
        if setup_failures:
            body.append("")
            body.append("Could not create yet:")
            body.extend(f"  - {line}" for line in setup_failures)
            body.extend(
                [
                    "",
                    "Fix those paths, then rerun setup or create them manually.",
                    "Once they exist, run:",
                    "  dummer",
                ]
            )
            title = f"{self._c.warn}Setup saved with warnings{self._c.reset}"
        else:
            body.extend(
                [
                    "",
                    "You're ready to run:",
                    "  dummer",
                    "",
                    "Run setup again to change saved answers. Command-line options can override them for one run.",
                ]
            )
            title = f"{self._c.ok}Ready{self._c.reset}"
        self.blank()
        self.write(_box(title, tuple(body), width=self.width, palette=self._c))

    def cancelled(self) -> None:
        self.blank()
        self.write(_box(f"{self._c.warn}Setup cancelled{self._c.reset}", ("No changes were written.",), width=self.width, palette=self._c))
