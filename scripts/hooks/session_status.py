#!/usr/bin/env python3
"""SessionStart-хук: печатает состояние этапов, чтобы агент начинал с актуальной картины.

Вывод в stdout добавляется в контекст сессии Claude Code.
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from review_lib import GATE_BY_STAGE, author_decision, git, latest_review, review_covers_head  # noqa: E402

ROOT = Path(os.environ.get("CLAUDE_PROJECT_DIR", ".")).resolve()


def main() -> None:
    tags = [t for t in git(ROOT, "tag", "--list", "stage-*").splitlines() if t]
    done = sorted(int(re.sub(r"\D", "", t)) for t in tags if re.sub(r"\D", "", t))
    current = (done[-1] + 1) if done else 0
    lines = ["[НИР] Состояние этапов (из хука SessionStart):"]
    lines.append(f"- Завершённые этапы (теги): {', '.join(f'stage-{n}' for n in done) or 'нет'}")
    lines.append(f"- Текущий этап: {current}")
    path, hdr = latest_review(ROOT, current)
    if path is None:
        lines.append(f"- Ревью этапа {current}: ещё не было")
    else:
        ok, why = review_covers_head(ROOT, hdr.get("Reviewed-Commit", ""))
        lines.append(
            f"- Последнее ревью этапа {current}: {path.relative_to(ROOT)} — Verdict={hdr.get('Verdict', '?')}"
            + ("" if ok else f" (устарело: {why})")
        )
        if hdr.get("Verdict") == "CHANGES_REQUESTED":
            lines.append("  Открытые замечания этого ревью нужно закрыть до следующего раунда.")
    gate = GATE_BY_STAGE.get(current)
    if gate:
        d = author_decision(ROOT, gate)
        lines.append(f"- Контрольная точка {gate}: решение автора — {d or 'не принято'}")
    lines.append("- Правила приёмки: docs/REVIEWER.md. Цикл этапа: /stage <N>. Ревью: /review <N>.")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
