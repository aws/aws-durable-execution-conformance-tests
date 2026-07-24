# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for posting AI reviews and minimizing their superseded comments."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
POST_SUMMARY_SCRIPT = REPOSITORY_ROOT / "scripts" / "post_ai_review_summary.sh"

MOCK_GH = """\
#!/usr/bin/env python3

import os
import sys
from pathlib import Path

args = sys.argv[1:]
repository = os.environ["GITHUB_REPOSITORY"]
pr_number = os.environ["PR_NUMBER"]

if f"repos/{repository}/pulls/{pr_number}" in args:
    print("expected-head-sha")
    raise SystemExit

if f"repos/{repository}/issues/{pr_number}/comments" in args:
    body = next(argument.removeprefix("body=") for argument in args if argument.startswith("body="))
    Path(os.environ["MOCK_POSTED_BODY"]).write_text(body, encoding="utf-8")
    print("NEW_COMMENT")
    raise SystemExit

if "--paginate" in args:
    is_inline_fetch = any("reviewThreads" in argument for argument in args)
    fail_variable = "MOCK_FAIL_INLINE_FETCH" if is_inline_fetch else "MOCK_FAIL_FETCH"
    comments_variable = "MOCK_INLINE_COMMENTS" if is_inline_fetch else "MOCK_COMMENTS"
    if os.environ.get(fail_variable) == "true":
        raise SystemExit(1)
    sys.stdout.write(Path(os.environ[comments_variable]).read_text(encoding="utf-8"))
    raise SystemExit

comment_id = next(argument.removeprefix("id=") for argument in args if argument.startswith("id="))
with Path(os.environ["MOCK_MINIMIZED_IDS"]).open("a", encoding="utf-8") as minimized_ids:
    minimized_ids.write(f"{comment_id}\\n")

if comment_id in os.environ["MOCK_FAIL_ID"].split(","):
    raise SystemExit(1)

print('{"data":{"minimizeComment":{"minimizedComment":{"isMinimized":true}}}}')
"""


def _comment(
    comment_id: str,
    body: str,
    *,
    author: str = "github-actions",
    minimized: bool = False,
) -> dict[str, object]:
    return {
        "id": comment_id,
        "body": body,
        "isMinimized": minimized,
        "author": {"login": author},
    }


def _page(
    comments: list[dict[str, object]],
    *,
    has_next_page: bool = False,
) -> dict[str, object]:
    return {
        "data": {
            "repository": {
                "pullRequest": {
                    "comments": {
                        "nodes": comments,
                        "pageInfo": {
                            "hasNextPage": has_next_page,
                            "endCursor": "next-page" if has_next_page else None,
                        },
                    }
                }
            }
        }
    }


def _inline_comment(
    comment_id: str,
    body: str,
    *,
    author: str = "github-actions",
    minimized: bool = False,
    reply_to: str | None = None,
) -> dict[str, object]:
    comment = _comment(
        comment_id,
        body,
        author=author,
        minimized=minimized,
    )
    comment["replyTo"] = {"id": reply_to} if reply_to is not None else None
    return comment


def _thread_page(
    comments: list[dict[str, object]],
    *,
    has_next_page: bool = False,
) -> dict[str, object]:
    return {
        "data": {
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "nodes": [{"comments": {"nodes": [comment]}} for comment in comments],
                        "pageInfo": {
                            "hasNextPage": has_next_page,
                            "endCursor": ("next-inline-page" if has_next_page else None),
                        },
                    }
                }
            }
        }
    }


@pytest.mark.parametrize(
    ("reviewer", "marker", "title", "other_marker", "other_title"),
    [
        (
            "claude",
            "<!-- ai-pr-review:claude -->",
            "Claude AI review",
            "<!-- ai-pr-review:codex -->",
            "Codex AI review",
        ),
        (
            "codex",
            "<!-- ai-pr-review:codex -->",
            "Codex AI review",
            "<!-- ai-pr-review:claude -->",
            "Claude AI review",
        ),
    ],
)
def test_post_summary_minimizes_only_exact_same_reviewer_comments(
    tmp_path: Path,
    reviewer: str,
    marker: str,
    title: str,
    other_marker: str,
    other_title: str,
) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    mock_gh = bin_dir / "gh"
    mock_gh.write_text(MOCK_GH, encoding="utf-8")
    mock_gh.chmod(0o755)

    comments_file = tmp_path / "comments.json"
    pages = [
        _page(
            [
                _comment("OLD_MARKER", f"{marker}\n## {title}\n\nOld marker summary"),
                _comment("MARKER_PREFIX", f"{marker} extra\n## {title}"),
                _comment("NEW_COMMENT", f"{marker}\n## {title}\n\nCurrent summary"),
                _comment("OTHER_AI", f"{other_marker}\n## {other_title}"),
            ],
            has_next_page=True,
        ),
        _page(
            [
                _comment("OLD_LEGACY", f"## {title}\n\nOld legacy summary"),
                _comment("HEADER_PREFIX", f"## {title}er metrics"),
                _comment("HUMAN", f"{marker}\n## {title}", author="alice"),
                _comment("MINIMIZED", f"{marker}\n## {title}", minimized=True),
                _comment("NOT_FIRST_LINE", f"Context\n## {title}"),
            ]
        ),
    ]
    comments_file.write_text(
        "".join(f"{json.dumps(page)}\n" for page in pages),
        encoding="utf-8",
    )

    other_reviewer = "codex" if reviewer == "claude" else "claude"
    current_inline_marker = f"<!-- ai-pr-review:inline:{reviewer}:123:1:primary -->"
    inline_comments_file = tmp_path / "inline-comments.json"
    inline_pages = [
        _thread_page(
            [
                _inline_comment(
                    "OLD_INLINE_PRIMARY",
                    f"<!-- ai-pr-review:inline:{reviewer}:100:1:primary -->\nFinding",
                ),
                _inline_comment(
                    "CURRENT_INLINE",
                    f"{current_inline_marker}\nCurrent finding",
                ),
                _inline_comment(
                    "OTHER_INLINE",
                    f"<!-- ai-pr-review:inline:{other_reviewer}:100:1:primary -->\nFinding",
                ),
                _inline_comment(
                    "INLINE_PREFIX",
                    f"<!-- ai-pr-review:inline:{reviewer}:100:1:primary --> extra\nFinding",
                ),
            ],
            has_next_page=True,
        ),
        _thread_page(
            [
                _inline_comment(
                    "OLD_INLINE_RETRY",
                    f"<!-- ai-pr-review:inline:{reviewer}:122:2:retry -->\nFinding",
                ),
                _inline_comment(
                    "INLINE_HUMAN",
                    f"<!-- ai-pr-review:inline:{reviewer}:100:1:primary -->\nFinding",
                    author="alice",
                ),
                _inline_comment(
                    "INLINE_MINIMIZED",
                    f"<!-- ai-pr-review:inline:{reviewer}:100:1:primary -->\nFinding",
                    minimized=True,
                ),
                _inline_comment(
                    "INLINE_NOT_FIRST_LINE",
                    f"Context\n<!-- ai-pr-review:inline:{reviewer}:100:1:primary -->",
                ),
                _inline_comment("INLINE_LEGACY", "Finding without a marker"),
                _inline_comment(
                    "INLINE_REPLY",
                    f"<!-- ai-pr-review:inline:{reviewer}:100:1:primary -->\nReply",
                    reply_to="ROOT",
                ),
            ]
        ),
    ]
    inline_comments_file.write_text(
        "".join(f"{json.dumps(page)}\n" for page in inline_pages),
        encoding="utf-8",
    )

    summary_file = tmp_path / "summary.md"
    summary_file.write_text("No actionable findings.", encoding="utf-8")
    posted_body = tmp_path / "posted-body.md"
    minimized_ids = tmp_path / "minimized-ids.txt"

    environment = os.environ.copy()
    environment.update(
        {
            "PATH": f"{bin_dir}{os.pathsep}{environment['PATH']}",
            "GH_TOKEN": "test-token",
            "GITHUB_REPOSITORY": "example/repository",
            "GITHUB_RUN_ID": "123",
            "GITHUB_SERVER_URL": "https://github.example",
            "PR_NUMBER": "42",
            "RUNNER_TEMP": str(tmp_path),
            "MOCK_COMMENTS": str(comments_file),
            "MOCK_INLINE_COMMENTS": str(inline_comments_file),
            "MOCK_POSTED_BODY": str(posted_body),
            "MOCK_MINIMIZED_IDS": str(minimized_ids),
            "MOCK_FAIL_ID": "OLD_MARKER,OLD_INLINE_PRIMARY",
            "MOCK_FAIL_FETCH": "false",
            "MOCK_FAIL_INLINE_FETCH": "false",
            "CURRENT_INLINE_COMMENT_MARKER": current_inline_marker,
        }
    )

    result = subprocess.run(
        [
            "bash",
            POST_SUMMARY_SCRIPT,
            reviewer,
            "expected-head-sha",
            str(summary_file),
        ],
        check=False,
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert minimized_ids.read_text(encoding="utf-8").splitlines() == [
        "OLD_MARKER",
        "OLD_LEGACY",
        "OLD_INLINE_PRIMARY",
        "OLD_INLINE_RETRY",
    ]
    assert f"::warning::Failed to minimize previous {title} comment (OLD_MARKER)." in result.stdout
    assert f"Minimized 1 previous {title} comment(s)." in result.stdout
    assert f"::warning::Failed to minimize previous {title} inline comment (OLD_INLINE_PRIMARY)." in result.stdout
    assert f"Minimized 1 previous {title} inline comment(s)." in result.stdout
    assert posted_body.read_text(encoding="utf-8") == (
        f"{marker}\n"
        f"## {title}\n\n"
        "No actionable findings.\n\n"
        "Reviewed commit `expected-head-sha`. "
        "[Workflow run](https://github.example/example/repository/actions/runs/123)"
    )


def test_post_summary_treats_summary_fetch_failure_as_cleanup_warning(
    tmp_path: Path,
) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    mock_gh = bin_dir / "gh"
    mock_gh.write_text(MOCK_GH, encoding="utf-8")
    mock_gh.chmod(0o755)

    summary_file = tmp_path / "summary.md"
    summary_file.write_text("No actionable findings.", encoding="utf-8")
    posted_body = tmp_path / "posted-body.md"
    minimized_ids = tmp_path / "minimized-ids.txt"
    inline_comments_file = tmp_path / "inline-comments.json"
    inline_comments_file.write_text(
        json.dumps(
            _thread_page(
                [
                    _inline_comment(
                        "OLD_INLINE",
                        "<!-- ai-pr-review:inline:claude:100:1:primary -->\nFinding",
                    )
                ]
            )
        ),
        encoding="utf-8",
    )

    environment = os.environ.copy()
    environment.update(
        {
            "PATH": f"{bin_dir}{os.pathsep}{environment['PATH']}",
            "GH_TOKEN": "test-token",
            "GITHUB_REPOSITORY": "example/repository",
            "GITHUB_RUN_ID": "123",
            "GITHUB_SERVER_URL": "https://github.example",
            "PR_NUMBER": "42",
            "RUNNER_TEMP": str(tmp_path),
            "MOCK_COMMENTS": str(tmp_path / "unused-comments.json"),
            "MOCK_INLINE_COMMENTS": str(inline_comments_file),
            "MOCK_POSTED_BODY": str(posted_body),
            "MOCK_MINIMIZED_IDS": str(minimized_ids),
            "MOCK_FAIL_ID": "",
            "MOCK_FAIL_FETCH": "true",
            "MOCK_FAIL_INLINE_FETCH": "false",
            "CURRENT_INLINE_COMMENT_MARKER": ("<!-- ai-pr-review:inline:claude:123:1:primary -->"),
        }
    )

    result = subprocess.run(
        [
            "bash",
            POST_SUMMARY_SCRIPT,
            "claude",
            "expected-head-sha",
            str(summary_file),
        ],
        check=False,
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert posted_body.is_file()
    assert minimized_ids.read_text(encoding="utf-8").splitlines() == ["OLD_INLINE"]
    assert "::warning::Failed to list previous Claude AI review comments for cleanup." in result.stdout
    assert "Minimized 1 previous Claude AI review inline comment(s)." in result.stdout


def test_post_summary_treats_inline_fetch_failure_as_cleanup_warning(
    tmp_path: Path,
) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    mock_gh = bin_dir / "gh"
    mock_gh.write_text(MOCK_GH, encoding="utf-8")
    mock_gh.chmod(0o755)

    comments_file = tmp_path / "comments.json"
    comments_file.write_text(
        json.dumps(
            _page(
                [
                    _comment(
                        "OLD_SUMMARY",
                        "<!-- ai-pr-review:claude -->\n## Claude AI review",
                    )
                ]
            )
        ),
        encoding="utf-8",
    )
    summary_file = tmp_path / "summary.md"
    summary_file.write_text("No actionable findings.", encoding="utf-8")
    posted_body = tmp_path / "posted-body.md"
    minimized_ids = tmp_path / "minimized-ids.txt"

    environment = os.environ.copy()
    environment.update(
        {
            "PATH": f"{bin_dir}{os.pathsep}{environment['PATH']}",
            "GH_TOKEN": "test-token",
            "GITHUB_REPOSITORY": "example/repository",
            "GITHUB_RUN_ID": "123",
            "GITHUB_SERVER_URL": "https://github.example",
            "PR_NUMBER": "42",
            "RUNNER_TEMP": str(tmp_path),
            "MOCK_COMMENTS": str(comments_file),
            "MOCK_INLINE_COMMENTS": str(tmp_path / "unused-inline-comments.json"),
            "MOCK_POSTED_BODY": str(posted_body),
            "MOCK_MINIMIZED_IDS": str(minimized_ids),
            "MOCK_FAIL_ID": "",
            "MOCK_FAIL_FETCH": "false",
            "MOCK_FAIL_INLINE_FETCH": "true",
            "CURRENT_INLINE_COMMENT_MARKER": ("<!-- ai-pr-review:inline:claude:123:1:primary -->"),
        }
    )

    result = subprocess.run(
        [
            "bash",
            POST_SUMMARY_SCRIPT,
            "claude",
            "expected-head-sha",
            str(summary_file),
        ],
        check=False,
        cwd=REPOSITORY_ROOT,
        env=environment,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert posted_body.is_file()
    assert minimized_ids.read_text(encoding="utf-8").splitlines() == ["OLD_SUMMARY"]
    assert "Minimized 1 previous Claude AI review comment(s)." in result.stdout
    assert "::warning::Failed to list previous Claude AI review inline comments for cleanup." in result.stdout
