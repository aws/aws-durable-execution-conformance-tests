# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the validate module.

Covers ``discover_suites`` and ``parse_not_implemented``; additional
pure-function tests for ``validate`` belong here too.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from aws_durable_execution_sdk_testing.validate import (
    discover_suites,
    parse_not_implemented,
)

if TYPE_CHECKING:
    from pathlib import Path

# --- discover_suites --------------------------------------------------------


def _make_requirement(dir_path: Path, name: str) -> None:
    """Create a minimal requirement YAML file inside dir_path."""
    dir_path.mkdir(parents=True, exist_ok=True)
    (dir_path / name).write_text("---\ndescription: stub\n")


def test_discovers_folders_with_yaml(tmp_path: Path) -> None:
    """Only folders containing at least one YAML are returned."""
    _make_requirement(tmp_path / "step", "1-1.yaml")
    _make_requirement(tmp_path / "callback", "4-1.yaml")

    assert discover_suites(tmp_path) == ["callback", "step"]


def test_results_are_sorted(tmp_path: Path) -> None:
    """Suites are returned in sorted (deterministic) order."""
    for suite in ("wait", "invoke", "child", "step"):
        _make_requirement(tmp_path / suite, "x.yaml")

    assert discover_suites(tmp_path) == ["child", "invoke", "step", "wait"]


def test_discovers_non_operation_suites(tmp_path: Path) -> None:
    """Capability and integration suites are discovered like operation suites."""
    _make_requirement(tmp_path / "serdes", "10-1.yaml")
    _make_requirement(tmp_path / "otel", "11-1.yaml")

    assert discover_suites(tmp_path) == ["otel", "serdes"]


def test_ignores_empty_dirs(tmp_path: Path) -> None:
    """Directories with no YAML files are excluded."""
    _make_requirement(tmp_path / "step", "1-1.yaml")
    (tmp_path / "empty").mkdir()
    (tmp_path / "only_txt").mkdir()
    (tmp_path / "only_txt" / "notes.txt").write_text("not a requirement")

    assert discover_suites(tmp_path) == ["step"]


def test_finds_yaml_in_nested_subdirs(tmp_path: Path) -> None:
    """A YAML nested below the suite folder still counts."""
    nested = tmp_path / "map" / "sub"
    _make_requirement(nested, "9-1.yaml")

    assert discover_suites(tmp_path) == ["map"]


def test_ignores_top_level_files(tmp_path: Path) -> None:
    """Files directly under tests_dir are not mistaken for suites."""
    _make_requirement(tmp_path / "step", "1-1.yaml")
    (tmp_path / ".yamllint.yaml").write_text("rules: {}\n")

    assert discover_suites(tmp_path) == ["step"]


def test_missing_directory_returns_empty(tmp_path: Path) -> None:
    """A non-existent tests_dir yields an empty list, not an error."""
    assert discover_suites(tmp_path / "does_not_exist") == []


def test_empty_directory_returns_empty(tmp_path: Path) -> None:
    """An existing but empty tests_dir yields an empty list."""
    assert discover_suites(tmp_path) == []


def test_accepts_string_path(tmp_path: Path) -> None:
    """The helper accepts a str path as well as a Path."""
    _make_requirement(tmp_path / "step", "1-1.yaml")

    assert discover_suites(str(tmp_path)) == ["step"]


# --- parse_not_implemented --------------------------------------------------


def _write_template(tmp_path: Path, body: str) -> str:
    path = tmp_path / "template.yaml"
    path.write_text(body)
    return str(path)


def test_not_implemented_top_level_block(tmp_path: Path) -> None:
    template = _write_template(
        tmp_path,
        """
TestingMetadata:
  NotImplemented:
    - id: "8-13"
      reason: "toleratedFailurePercentage rejected at build()"
    - id: "8-22"
      reason: "same as 8-13"
Resources:
  ParallelBasic:
    Type: AWS::Serverless::Function
    TestingMetadata:
      TestDescription: ["8-1"]
""",
    )
    assert parse_not_implemented(template) == {
        "8-13": "toleratedFailurePercentage rejected at build()",
        "8-22": "same as 8-13",
    }


def test_not_implemented_on_resource(tmp_path: Path) -> None:
    template = _write_template(
        tmp_path,
        """
Resources:
  MapItemNamer:
    Type: AWS::Serverless::Function
    TestingMetadata:
      NotImplemented:
        - id: "9-14"
          reason: "MapConfig has no itemNamer field"
""",
    )
    assert parse_not_implemented(template) == {"9-14": "MapConfig has no itemNamer field"}


def test_not_implemented_first_reason_wins_on_duplicate(tmp_path: Path) -> None:
    template = _write_template(
        tmp_path,
        """
TestingMetadata:
  NotImplemented:
    - id: "8-13"
      reason: "first"
    - id: "8-13"
      reason: "second"
""",
    )
    assert parse_not_implemented(template) == {"8-13": "first"}


def test_not_implemented_missing_reason_defaults_empty(tmp_path: Path) -> None:
    template = _write_template(
        tmp_path,
        """
TestingMetadata:
  NotImplemented:
    - id: "8-13"
""",
    )
    assert parse_not_implemented(template) == {"8-13": ""}


def test_not_implemented_null_reason_defaults_empty(tmp_path: Path) -> None:
    # Explicit null value (key present, no value) must not become the string "None".
    template = _write_template(
        tmp_path,
        """
TestingMetadata:
  NotImplemented:
    - id: "8-13"
      reason:
""",
    )
    assert parse_not_implemented(template) == {"8-13": ""}


def test_not_implemented_absent_returns_empty(tmp_path: Path) -> None:
    template = _write_template(
        tmp_path,
        """
Resources:
  ParallelBasic:
    Type: AWS::Serverless::Function
    TestingMetadata:
      TestDescription: ["8-1"]
""",
    )
    assert parse_not_implemented(template) == {}


def test_not_implemented_ignores_entries_without_id(tmp_path: Path) -> None:
    template = _write_template(
        tmp_path,
        """
TestingMetadata:
  NotImplemented:
    - reason: "no id, ignored"
    - id: "8-13"
      reason: "kept"
""",
    )
    assert parse_not_implemented(template) == {"8-13": "kept"}


def test_not_implemented_tolerates_cfn_intrinsic_tags(tmp_path: Path) -> None:
    # The template loader must handle !GetAtt etc. without choking.
    template = _write_template(
        tmp_path,
        """
TestingMetadata:
  NotImplemented:
    - id: "8-13"
      reason: "gap"
Resources:
  ParallelBasic:
    Type: AWS::Serverless::Function
    Properties:
      Role: !GetAtt DurableFunctionRole.Arn
    TestingMetadata:
      TestDescription: ["8-1"]
""",
    )
    assert parse_not_implemented(template) == {"8-13": "gap"}
