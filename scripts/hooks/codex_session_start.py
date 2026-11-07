#!/usr/bin/env python3
# ruff: noqa: RUF001 — Russian hook messages and intentional Cyrillic decision fixtures.
"""Adapt the shared stage-status formatter to Codex's SessionStart JSON contract."""
from __future__ import annotations

import contextlib
import io
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import session_status
from review_lib import GATE_BY_STAGE, author_decision, latest_review, repository_root


def main() -> None:
    data = json.load(sys.stdin)
    root = repository_root(data.get("cwd"))
    session_status.ROOT = root
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        session_status.main()
    context = output.getvalue()
    # Show concrete IDs instead of relying only on an advisory reminder.
    for stage in range(10):
        path, header = latest_review(root, stage)
        if path and header.get("Verdict") == "CHANGES_REQUESTED":
            rows = [line for line in path.read_text().splitlines()
                    if line.startswith(f"| S{stage}-R{header.get('Round')}-")
                    and any(level in line for level in ("BLOCKER", "MAJOR", "MINOR"))]
            context += f"\nОткрытые замечания последнего ревью этапа {stage}:\n" + "\n".join(rows)
    for stage, gate in GATE_BY_STAGE.items():
        decision = author_decision(root, gate) or "не принято"
        context += f"\n{gate} (после этапа {stage}): решение автора — {decision}."
    context += ("\nЦикл: реализация → проверки → отчёт → коммит → независимое ревью → исправления. "
                "Не более 3 раундов за запуск. После приёмки этапа: push и остановка; "
                "затем пользователь проверяет результат в ChatGPT Web. Stop-хук не используется.")
    print(json.dumps({"hookSpecificOutput": {"hookEventName": "SessionStart", "additionalContext": context}},
                     ensure_ascii=False))


if __name__ == "__main__":
    main()
