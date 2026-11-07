#!/usr/bin/env python3
# ruff: noqa: RUF001 — Russian hook messages and intentional Cyrillic decision fixtures.
"""Codex PreToolUse adapter. Denial uses exit 2, supported by CLI 0.167.0.

Keep Claude's rules as a minimum; inspect every apply_patch source and destination.
This is a tool guard, not a security boundary for arbitrary programs or open PTYs.
"""
from __future__ import annotations

import io
import json
import re
import shlex
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import guard_pretool as legacy
from review_lib import repository_root, text_sets_decision


def relative_names(path: str, root: Path, cwd: Path) -> list[str]:
    value = Path(path)
    absolute = value if value.is_absolute() else cwd / value
    # Inspect lexical and symlink-resolved names; neither a symlink nor ../ is an escape.
    import os
    candidates = (Path(os.path.abspath(absolute)), absolute.resolve())
    return [p.relative_to(root).as_posix() for p in candidates if p.is_relative_to(root)]


def check_path(path: str, text: str, root: Path, cwd: Path) -> None:
    for rel in relative_names(path, root, cwd):
        if legacy.PROTECTED_PATH_RE.match(rel) or rel in (
                ".codex", ".claude", ".claude/agents", ".claude/skills", "scripts", "scripts/hooks",
                "docs", "docs/author-decisions.md"):
            legacy.block(legacy.PROTECTED_MSG)
        target = root / rel
        if re.fullmatch(r"docs/reviews/stage-\d+-r\d+\.md", rel) and (
                target.exists() or target.is_symlink()):
            legacy.block("Запрещено изменять существующее ревью; новый раунд — новый файл.")
        if legacy.GATE_FILE_RE.fullmatch(rel) and text_sets_decision(text):
            legacy.block("Решение автора на контрольной точке заполняет только автор.")


def check_patch(patch: str, root: Path, cwd: Path) -> None:
    if not patch.startswith("*** Begin Patch\n") or "\n*** End Patch" not in patch:
        legacy.block("Не распознан формат apply_patch; проверка файлов невозможна.")
    paths: list[str] = []
    changes: list[str] = []
    recognized = False
    for line in patch.splitlines()[1:]:
        if line.startswith(("*** Add File: ", "*** Update File: ", "*** Delete File: ", "*** Move to: ")):
            recognized = True
            path = line.split(": ", 1)[1]
            if not path:
                legacy.block("Пустой путь в apply_patch.")
            paths.append(path)
        elif line.startswith("*** ") and line not in ("*** End Patch", "*** End of File"):
            legacy.block("Неизвестная директива apply_patch.")
        elif line.startswith(("+", "-")):
            changes.append(line[1:])
    if not recognized:
        legacy.block("apply_patch не содержит проверяемых файлов.")
    for path in paths:
        check_path(path, "\n".join(changes), root, cwd)


def check_shell(command: str, root: Path, cwd: Path) -> None:
    # Shell-invoked patches have the same checks as the native apply_patch tool.
    for match in re.finditer(r"\*\*\* Begin Patch\n.*?\n\*\*\* End Patch", command, re.S):
        check_patch(match.group(), root, cwd)
    # Inspect canonical paths for writes, including absolute paths and symlink aliases.
    writes = legacy.WRITE_CMD_RE.search(command) or re.search(
        r"\bapply_patch\b|\bgit\s+(?:restore|reset|apply)\b|"
        r"write_text|write_bytes|unlink|shutil\.(?:move|copy)|os\.(?:remove|rename)", command)
    try:
        tokens = shlex.split(command)
    except ValueError:
        legacy.block("Не удалось разобрать команду терминала.")
    if writes:
        if legacy.PROTECTED_RE.search(command):
            legacy.block(legacy.PROTECTED_MSG)
        for token in tokens:
            if token.startswith("-") or "\n" in token:
                continue
            check_path(token, command, root, cwd)
    # git global flags (-C/-c/--git-dir/...) must not hide tag or commit from the guard.
    lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|()\n")
    lexer.whitespace_split = True
    lexer.whitespace = " \t\r"
    words = list(lexer)
    separators = {";", "&&", "||", "|", "(", ")", "\n"}
    contexts: dict[int, list[Path]] = {}
    candidates = [cwd]
    start = 0
    mutating = writes or re.search(r"\bgit\b.*\b(?:tag|commit)\b", command)
    for end in range(len(words) + 1):
        if end < len(words) and words[end] not in separators:
            continue
        segment = words[start:end]
        for i in range(start, end):
            contexts[i] = list(candidates)
        if segment and segment[0] == "cd":
            args = [x for x in segment[1:] if x not in ("--", "-L", "-P")]
            if not args or any(x in args[0] for x in ("$", "`", "~", "*", "?")) or args[0] == "-":
                if mutating:
                    legacy.block("Неоднозначный cd перед записью; "
                                 "используйте явный workdir или абсолютный путь.")
            else:
                # Include both outcomes: with ||/; the following command may run even if cd fails.
                candidates = list(dict.fromkeys(
                    [*candidates, *[(c / args[0]).resolve() for c in candidates]]))
                if len(candidates) > 32:
                    legacy.block("Слишком много неоднозначных переходов cd перед изменением файлов.")
        elif writes:
            for candidate in candidates:
                for token in segment:
                    if not token.startswith("-") and "\n" not in token:
                        check_path(token, command, root, candidate)
        start = end + 1
    for index, word in enumerate(words):
        if Path(word).name != "git":
            continue
        git_cwds = contexts.get(index, [cwd])
        j = index + 1
        custom_worktree = False
        while j < len(words):
            token = words[j]
            if token in ("-C", "-c", "--git-dir", "--work-tree") and j + 1 < len(words):
                if token == "-C":
                    git_cwds = [(c / words[j + 1]).resolve() for c in git_cwds]
                elif token in ("--git-dir", "--work-tree"):
                    custom_worktree = True
                j += 2
            elif token.startswith("-C") and len(token) > 2:
                git_cwds = [(c / token[2:]).resolve() for c in git_cwds]
                j += 1
            elif token.startswith(("--git-dir=", "--work-tree=")):
                custom_worktree = True
                j += 1
            elif token.startswith("-"):
                j += 1
            else:
                break
        if j >= len(words) or words[j] not in ("tag", "commit"):
            continue
        roots = {repository_root(str(c)) for c in git_cwds}
        if custom_worktree or len(roots) != 1:
            legacy.block("Неоднозначная Git-рабочая область; "
                         "используйте явный workdir без --git-dir/--work-tree.")
        end = j + 1
        while end < len(words) and words[end] not in separators:
            end += 1
        tail = words[j + 1:end]
        legacy.ROOT = roots.pop()
        if words[j] == "tag" and not any(t in ("-d", "--delete") for t in tail):
            positional = []
            skip = False
            for token in tail:
                if skip:
                    skip = False
                elif token in ("-m", "--message", "-F", "--file", "-u", "--local-user"):
                    skip = True
                elif not token.startswith("-"):
                    positional.append(token)
            for item in positional:
                match = re.fullmatch(r"stage-(\d+)", item)
                if match:
                    if legacy.git(legacy.ROOT, "status", "--porcelain"):
                        legacy.block("Stage-тег требует чистого рабочего дерева и зафиксированного ревью.")
                    legacy.check_tag(int(match.group(1)))
                    position = positional.index(item)
                    targets = positional[position + 1:]
                    target = (legacy.git(legacy.ROOT, "rev-parse", targets[0] + "^{commit}")
                              if targets else None)
                    if targets and target != legacy.git(legacy.ROOT, "rev-parse", "HEAD"):
                        legacy.block("Stage-тег должен указывать на текущий проверенный HEAD.")
        elif words[j] == "commit":
            legacy.check_commit("git commit " + shlex.join(tail))
    # Preserve all original Bash checks, including staged-file protection on commit.
    legacy.ROOT = root
    previous = sys.stdin
    try:
        sys.stdin = io.StringIO(json.dumps({"tool_name": "Bash", "tool_input": {"command": command}}))
        legacy.main()
    finally:
        sys.stdin = previous


def main() -> None:
    try:
        data: dict[str, Any] = json.load(sys.stdin)
        root = repository_root(data.get("cwd"))
        cwd = Path(data.get("cwd") or root).resolve()
        tool = data.get("tool_name")
        args = data.get("tool_input")
        if not isinstance(args, dict):
            legacy.block("Отсутствует объект tool_input.")
        command = args.get("command", args.get("cmd", args.get("patch")))
        if tool in ("Bash", "exec_command", "shell_command", "shell", "apply_patch"):
            if not isinstance(command, str):
                legacy.block("Отсутствует строка tool_input.command.")
            if tool == "apply_patch":
                check_patch(command, root, cwd)
            else:
                workdir = args.get("workdir", args.get("cwd"))
                shell_cwd = Path(workdir).resolve() if workdir else cwd
                check_shell(command, root, shell_cwd)
        elif tool in ("Edit", "Write", "MultiEdit"):
            text = args.get("new_string") or args.get("content") or ""
            if tool == "MultiEdit":
                text = "\n".join(e.get("new_string", "") for e in args.get("edits", []))
            check_path(args["file_path"], text, root, cwd)
        else:
            legacy.block(f"Неизвестный формат инструмента: {tool}")
    except Exception as exc:
        legacy.block(f"Ошибка проверки Codex PreToolUse: {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    main()
