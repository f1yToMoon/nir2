#!/usr/bin/env python3
"""PreToolUse-хук Claude Code.

Блокирует (exit 2, причина в stderr — её видит агент):
1. `git tag stage-N` без принятого ревью для текущего HEAD (docs/reviews/stage-N-rK.md, Verdict PASS*)
   и, для этапов 1 и 4, без решения автора в отчёте контрольной точки (GO/CORRECT/PIVOT).
2. Попытку агента отметить «Решение автора» в docs/gate-G*-report.md (Edit/Write/Bash).
3. Изменение уже записанного файла ревью (ревью неизменяемы; новое мнение — новый раунд).
4. Изменение агентом механизма приёмки: хуков, настроек Claude, ревьюера, скиллов, docs/REVIEWER.md.
   Эти файлы меняет только автор, вручную, вне Claude.
5. Коммит, в который попали изменения этих файлов (даже сделанные массовой командой вроде ruff --fix).
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from review_lib import (
    ACCEPTING,
    GATE_BY_STAGE,
    author_decision,
    git,
    latest_review,
    review_covers_head,
    text_sets_decision,
)

ROOT = Path(os.environ.get("CLAUDE_PROJECT_DIR", ".")).resolve()
TAG_RE = re.compile(r"\bgit\s+tag\b(?![^;&|]*\s-d\b)[^;&|]*?\bstage-(\d+)\b")
GATE_FILE_RE = re.compile(r"docs/gate-G\d-report\.md")
PROTECTED_RE = re.compile(
    r"(^|[\s'\"=/])("
    r"\.claude/settings(\.local)?\.json|\.claude/agents/|\.claude/skills/|"
    r"scripts/hooks/|docs/REVIEWER\.md"
    r")"
)
WRITE_CMD_RE = re.compile(
    r"(sed\s+-i|perl\s+-p?i|\btee\b|>>?|\brm\b|\bmv\b|\bcp\b|git\s+rm|chmod|truncate|"
    r"python3?\s+-c|\bln\b|git\s+checkout\s+(?!HEAD\b|--)\S|--source[= ](?!HEAD\b))"
)
COMMIT_RE = re.compile(r"\bgit\s+commit\b")
PROTECTED_PATH_RE = re.compile(
    r"^(\.claude/settings(\.local)?\.json|\.claude/agents/|\.claude/skills/|scripts/hooks/|docs/REVIEWER\.md)"
)
COMMIT_MSG = (
    "Коммит запрещён: в нём изменены файлы механизма приёмки ({files}). Их меняет только автор вручную. "
    "Вероятно, их задела массовая команда (ruff --fix, форматтер, sed по маске). Верни их к версии HEAD: "
    "`git restore --staged --worktree -- <файлы>` и повтори коммит. "
    "Если изменение нужно — опиши его в отчёте и попроси автора."
)
PROTECTED_MSG = (
    "Запрещено: файлы механизма приёмки (.claude/settings*.json, .claude/agents/, .claude/skills/, "
    "scripts/hooks/, docs/REVIEWER.md) меняет только автор вручную. Если правило мешает, "
    "опиши проблему в отчёте этапа и остановись."
)


def block(msg: str) -> None:
    print(msg, file=sys.stderr)
    sys.exit(2)


def check_tag(stage: int) -> None:
    path, hdr = latest_review(ROOT, stage)
    if path is None:
        block(
            f"Тег stage-{stage} запрещён: нет ревью docs/reviews/stage-{stage}-r*.md. "
            f"Запусти /review {stage}."
        )
    verdict = hdr.get("Verdict", "")
    if verdict not in ACCEPTING:
        block(
            f"Тег stage-{stage} запрещён: последнее ревью {path.name} имеет Verdict={verdict or '?'}. "
            "Закрой замечания и запусти новый раунд ревью."
        )
    ok, why = review_covers_head(ROOT, hdr.get("Reviewed-Commit", ""))
    if not ok:
        block(f"Тег stage-{stage} запрещён: ревью {path.name} не относится к текущему HEAD ({why}). "
              "Нужен новый раунд /review.")
    gate = GATE_BY_STAGE.get(stage)
    if gate:
        decision = author_decision(ROOT, gate)
        if decision not in {"GO", "CORRECT", "PIVOT"}:
            block(
                f"Тег stage-{stage} запрещён: в docs/gate-{gate}-report.md нет решения автора "
                "(GO/CORRECT/PIVOT). Решение принимает только автор — остановись и попроси его."
            )


def check_commit(cmd: str) -> None:
    staged = git(ROOT, "diff", "--cached", "--name-only").splitlines()
    files = set(staged)
    if re.search(r"\bgit\s+commit\b[^;&|]*\s(-a|--all|-[a-zA-Z]*a[a-zA-Z]*)\b", cmd):
        files |= set(git(ROOT, "diff", "--name-only").splitlines())
    if re.search(r"\bgit\s+add\b", cmd):
        # «git add ... && git commit» в одной команде: учесть изменения рабочего дерева и новые файлы
        files |= set(git(ROOT, "diff", "--name-only").splitlines())
        files |= set(git(ROOT, "ls-files", "--others", "--exclude-standard").splitlines())
    bad = sorted(f for f in files if f and PROTECTED_PATH_RE.match(f))
    if bad:
        block(COMMIT_MSG.format(files=", ".join(bad)))


def main() -> None:
    try:
        data = json.load(sys.stdin)
    except Exception:
        sys.exit(0)
    tool = data.get("tool_name", "")
    tin = data.get("tool_input", {}) or {}

    if tool == "Bash":
        cmd = tin.get("command", "") or ""
        for m in TAG_RE.finditer(cmd):
            check_tag(int(m.group(1)))
        if COMMIT_RE.search(cmd):
            check_commit(cmd)
        if GATE_FILE_RE.search(cmd) and text_sets_decision(cmd):
            block("Запрещено: отмечать «Решение автора» в отчёте контрольной точки может только автор.")
        if PROTECTED_RE.search(cmd) and WRITE_CMD_RE.search(cmd):
            block(PROTECTED_MSG)
        if re.search(r"docs/reviews/stage-\d+-r\d+\.md", cmd) and re.search(
            r"(sed\s+-i|>\s*docs/reviews|tee\s+docs/reviews|rm\s|git\s+rm|mv\s)", cmd
        ):
            block("Запрещено изменять или удалять существующие ревью. Новое мнение — новый раунд /review.")
        sys.exit(0)

    if tool in {"Edit", "Write", "MultiEdit"}:
        fp = str(tin.get("file_path", ""))
        rel = os.path.relpath(fp, ROOT) if os.path.isabs(fp) else fp
        new_text = tin.get("new_string") or tin.get("content") or ""
        if tool == "MultiEdit":
            new_text = "\n".join(e.get("new_string", "") for e in tin.get("edits", []))
        if PROTECTED_RE.search("/" + rel.replace(os.sep, "/")):
            block(PROTECTED_MSG)
        if GATE_FILE_RE.search(rel) and text_sets_decision(new_text):
            block("Запрещено: отмечать «Решение автора» в отчёте контрольной точки может только автор. "
                  "Оставь поле пустым и попроси автора.")
        if re.match(r"docs/reviews/stage-\d+-r\d+\.md$", rel) and Path(ROOT, rel).exists():
            block("Запрещено изменять существующий файл ревью. Новое мнение — новый раунд /review.")
    sys.exit(0)


if __name__ == "__main__":
    main()
