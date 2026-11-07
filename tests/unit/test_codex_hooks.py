# ruff: noqa: RUF001 — Russian hook messages and intentional Cyrillic decision fixtures.
"""Policies run against disposable Git repositories, never the real acceptance files/tags."""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
ADAPTER = ROOT / "scripts/hooks/codex_pretool.py"


def git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    git(tmp_path, "init", "-q")
    git(tmp_path, "config", "user.name", "Hook fixture")
    git(tmp_path, "config", "user.email", "fixture@example.invalid")
    for name, content in {
        "ordinary.txt": "before\n", "docs/REVIEWER.md": "fixture\n",
        "scripts/hooks/policy.py": "# fixture\n", ".codex/hooks.json": "{}\n",
        ".claude/settings.json": "{}\n", "docs/gate-G1-report.md": "## Решение автора\n[ ] GO\n",
        "docs/gate-G2-report.md": "## Решение автора\n[ ] GO\n", "subdir/.keep": "\n",
    }.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    git(tmp_path, "add", ".")
    git(tmp_path, "commit", "-qm", "fixture")
    git(tmp_path, "tag", "stage-2")
    return tmp_path


def hook(repo: Path, command: str, tool: str = "Bash", cwd: Path | None = None,
         adapter: Path = ADAPTER) -> subprocess.CompletedProcess[str]:
    payload = {"cwd": str(cwd or repo), "hook_event_name": "PreToolUse", "tool_name": tool,
               "tool_input": {"command": command}}
    return subprocess.run(["python3", str(adapter)], input=json.dumps(payload), capture_output=True,
                          text=True, cwd=repo, env={**os.environ, "CLAUDE_PROJECT_DIR": str(repo)})


def review(repo: Path, stage: int = 3, verdict: str = "PASS", round_: int = 1) -> Path:
    path = repo / f"docs/reviews/stage-{stage}-r{round_}.md"
    path.parent.mkdir(exist_ok=True)
    path.write_text(f"Stage: {stage}\nRound: {round_}\nReviewed-Commit: {git(repo, 'rev-parse', 'HEAD')}\n"
                    f"Verdict: {verdict}\n\n| S{stage}-R{round_}-01 | MAJOR | fixture |\n")
    git(repo, "add", str(path))
    git(repo, "commit", "-qm", "fixture review")
    return path


@pytest.mark.parametrize("command", ["cat scripts/hooks/policy.py", "git status --short",
                                     "printf x > ordinary.txt"])
def test_allowed_shell(repo: Path, command: str) -> None:
    assert hook(repo, command).returncode == 0


@pytest.mark.parametrize("path", [".codex/hooks.json", ".codex/config.toml", ".claude/settings.json",
                                  "scripts/hooks/policy.py", "docs/REVIEWER.md"])
@pytest.mark.parametrize("action", ["Add", "Update", "Delete"])
def test_every_patch_file_is_protected(repo: Path, path: str, action: str) -> None:
    patch = ("*** Begin Patch\n*** Update File: ordinary.txt\n@@\n-before\n+after\n"
             f"*** {action} File: {path}\n" + ("+x\n" if action != "Delete" else "") + "*** End Patch\n")
    assert hook(repo, patch, "apply_patch").returncode == 2
    assert (repo / "ordinary.txt").read_text() == "before\n"


@pytest.mark.parametrize("source,destination", [("ordinary.txt", "scripts/hooks/renamed.py"),
                                               ("scripts/hooks/policy.py", "ordinary.py")])
def test_patch_move_checks_both_paths(repo: Path, source: str, destination: str) -> None:
    patch = f"*** Begin Patch\n*** Update File: {source}\n*** Move to: {destination}\n@@\n+x\n*** End Patch\n"
    assert hook(repo, patch, "apply_patch").returncode == 2


def test_patch_regular_file_and_new_review_allowed(repo: Path) -> None:
    for path in ("ordinary.txt", "docs/reviews/stage-3-r1.md"):
        patch = f"*** Begin Patch\n*** Add File: {path}\n+text\n*** End Patch\n"
        assert hook(repo, patch, "apply_patch").returncode == 0


def test_existing_review_is_immutable_in_shell_and_patch(repo: Path) -> None:
    path = review(repo)
    assert hook(repo, f"printf x > {path}").returncode == 2
    patch = f"*** Begin Patch\n*** Delete File: {path}\n*** End Patch\n"
    assert hook(repo, patch, "apply_patch").returncode == 2


@pytest.mark.parametrize("value", ["Решение автора: GO", "[x] CORRECT", "[Х] PIVOT"])
def test_author_decision_cannot_be_filled(repo: Path, value: str) -> None:
    patch = f"*** Begin Patch\n*** Update File: docs/gate-G1-report.md\n@@\n+{value}\n*** End Patch\n"
    assert hook(repo, patch, "apply_patch").returncode == 2
    assert hook(repo, f"printf '{value}' >> docs/gate-G2-report.md").returncode == 2


def test_stage_tag_requires_current_acceptance(repo: Path) -> None:
    assert hook(repo, "git tag stage-3").returncode == 2
    review(repo)
    assert hook(repo, "git tag stage-3").returncode == 0
    (repo / "ordinary.txt").write_text("new\n")
    git(repo, "commit", "-qam", "changed implementation")
    assert hook(repo, "git tag stage-3").returncode == 2


def test_latest_changes_requested_blocks_tag(repo: Path) -> None:
    review(repo)
    review(repo, verdict="CHANGES_REQUESTED", round_=2)
    assert hook(repo, "git tag stage-3").returncode == 2


def test_annotated_tag_and_explicit_head_are_allowed(repo: Path) -> None:
    review(repo)
    assert hook(repo, "git tag -a stage-3 -m 'Accepted stage 3'").returncode == 0
    assert hook(repo, "git tag stage-3 HEAD").returncode == 0
    assert hook(repo, "git tag stage-3 HEAD~1").returncode == 2


def test_dirty_worktree_cannot_be_tagged(repo: Path) -> None:
    review(repo)
    (repo / "ordinary.txt").write_text("uncommitted\n")
    assert hook(repo, "git tag stage-3").returncode == 2


def test_newline_after_valid_tag_is_not_a_target_revision(repo: Path) -> None:
    review(repo)
    assert hook(repo, "git tag stage-3\ngit status --short").returncode == 0


@pytest.mark.parametrize("stage,gate", [(1, "G1"), (4, "G2")])
def test_gate_tag_requires_author_decision(repo: Path, stage: int, gate: str) -> None:
    review(repo, stage=stage)
    assert hook(repo, f"git tag stage-{stage}").returncode == 2
    path = repo / f"docs/gate-{gate}-report.md"
    path.write_text("## Решение автора\nРешение автора: GO\n")
    git(repo, "commit", "-qam", "fixture author decision")
    assert hook(repo, f"git tag stage-{stage}").returncode == 0


def test_git_global_options_do_not_hide_tag(repo: Path) -> None:
    assert hook(repo, f"git -C {repo} tag stage-3").returncode == 2
    review(repo)
    assert hook(repo, f"git -C {repo} tag stage-3").returncode == 0


def test_staged_protected_file_cannot_be_committed(repo: Path) -> None:
    (repo / ".codex/hooks.json").write_text('{"changed": true}\n')
    git(repo, "add", ".codex/hooks.json")
    for adapter in (ADAPTER, ROOT / "scripts/hooks/guard_pretool.py"):
        assert hook(repo, "git commit -m changed", adapter=adapter).returncode == 2


@pytest.mark.parametrize("command", ["git commit -m changed -- .codex/hooks.json",
                                     "git commit --only .codex/hooks.json -m changed",
                                     "git commit --include .codex/hooks.json -m changed"])
def test_unstaged_protected_pathspec_cannot_be_committed(repo: Path, command: str) -> None:
    (repo / ".codex/hooks.json").write_text('{"changed": true}\n')
    for adapter in (ADAPTER, ROOT / "scripts/hooks/guard_pretool.py"):
        assert hook(repo, command, adapter=adapter).returncode == 2


@pytest.mark.parametrize("separator", [" && ", "; ", "\n"])
def test_shell_cd_does_not_hide_existing_review(repo: Path, separator: str) -> None:
    review(repo)
    assert hook(repo, f"cd docs/reviews{separator}rm stage-3-r1.md").returncode == 2


def test_readonly_cd_and_clear_workdir_are_allowed(repo: Path) -> None:
    assert hook(repo, "cd scripts && cat hooks/policy.py").returncode == 0
    assert hook(repo, "cd subdir && printf x > ordinary.txt").returncode == 0


def test_shell_protected_paths_and_embedded_patch(repo: Path) -> None:
    for command in ("rm -rf .codex", "rm -rf scripts", "sed -i s/x/y/ scripts/hooks/policy.py",
                    "apply_patch <<'PATCH'\n*** Begin Patch\n*** Delete File: .codex/hooks.json\n"
                    "*** End Patch\nPATCH"):
        assert hook(repo, command).returncode == 2


def test_subdirectory_absolute_path_and_symlink_are_resolved(repo: Path) -> None:
    (repo / "alias").symlink_to(repo / "scripts/hooks", target_is_directory=True)
    for path in ("../scripts/hooks/policy.py", str(repo / ".codex/hooks.json"), "../alias/policy.py"):
        patch = f"*** Begin Patch\n*** Delete File: {path}\n*** End Patch\n"
        assert hook(repo, patch, "apply_patch", cwd=repo / "subdir").returncode == 2
    assert hook(repo, "rm alias/policy.py").returncode == 2


def test_session_start_reports_ids_and_gates_from_subdirectory(repo: Path) -> None:
    review(repo, verdict="CHANGES_REQUESTED")
    payload = json.dumps({"cwd": str(repo / "subdir"), "hook_event_name": "SessionStart"})
    result = subprocess.run(["python3", str(ROOT / "scripts/hooks/codex_session_start.py")],
                            input=payload, text=True, capture_output=True, check=True)
    text = json.loads(result.stdout)["hookSpecificOutput"]["additionalContext"]
    assert all(value in text for value in ("S3-R1-01", "CHANGES_REQUESTED", "G1", "G2", "3"))


def test_malformed_payload_denies(repo: Path) -> None:
    result = subprocess.run(["python3", str(ADAPTER)], input="broken", capture_output=True,
                            text=True, cwd=repo)
    assert result.returncode == 2
