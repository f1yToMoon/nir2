"""Общие функции хуков: поиск ревью этапа и решения автора на контрольной точке."""
from __future__ import annotations

import re
import subprocess
from pathlib import Path

GATE_BY_STAGE = {1: "G1", 4: "G2"}
ACCEPTING = {"PASS", "PASS_WITH_NOTES"}

_DECISION_SECTION = re.compile(r"решение\s+автора", re.IGNORECASE)
_CHECKED = re.compile(r"\[\s*[xXхХ]\s*\][`*\s]*(GO|CORRECT|PIVOT)", re.IGNORECASE)
_LINE = re.compile(r"решение\s+автора\s*[:：]\s*\**\s*(GO|CORRECT|PIVOT)", re.IGNORECASE)


def git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), *args], capture_output=True, text=True, check=False
    ).stdout.strip()


def latest_review(root: Path, stage: int) -> tuple[Path | None, dict[str, str]]:
    files = sorted(
        (root / "docs" / "reviews").glob(f"stage-{stage}-r*.md"),
        key=lambda p: int(re.search(r"-r(\d+)\.md$", p.name).group(1))  # type: ignore[union-attr]
        if re.search(r"-r(\d+)\.md$", p.name) else -1,
    )
    if not files:
        return None, {}
    path = files[-1]
    header: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines()[:30]:
        m = re.match(r"^\s*(Stage|Round|Reviewed-Commit|Base|Verdict)\s*:\s*(\S+)", line)
        if m:
            header[m.group(1)] = m.group(2)
    return path, header


def review_covers_head(root: Path, reviewed: str) -> tuple[bool, str]:
    """Ревью действительно для HEAD, если после reviewed менялись только docs/reviews/
    и отчёты контрольных точок (туда автор вписывает решение)."""
    head = git(root, "rev-parse", "HEAD")
    if not reviewed:
        return False, "в ревью нет Reviewed-Commit"
    full = git(root, "rev-parse", "--verify", "--quiet", reviewed + "^{commit}")
    if not full:
        return False, f"коммит {reviewed} из ревью не найден"
    if full == head:
        return True, ""
    changed = [f for f in git(root, "diff", "--name-only", f"{full}..{head}").splitlines() if f]
    other = [
        f for f in changed
        if not f.startswith("docs/reviews/") and not re.match(r"docs/gate-G\d-report\.md$", f)
    ]
    if other:
        return False, (
            f"после ревьюированного коммита {full[:8]} изменены файлы: "
            + ", ".join(other[:8]) + (" …" if len(other) > 8 else "")
        )
    return True, ""


def author_decision(root: Path, gate: str) -> str | None:
    path = root / "docs" / f"gate-{gate}-report.md"
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8")
    m = _DECISION_SECTION.search(text)
    if not m:
        return None
    section = text[m.start():]
    for rx in (_LINE, _CHECKED):
        d = rx.search(section)
        if d:
            return d.group(1).upper()
    return None


def text_sets_decision(text: str) -> bool:
    """True, если фрагмент текста отмечает решение автора (используется для запрета агенту)."""
    return bool(_CHECKED.search(text) or _LINE.search(text))
