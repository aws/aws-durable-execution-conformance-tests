# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for pure functions in the validate module."""

from __future__ import annotations

from typing import TYPE_CHECKING

from aws_durable_execution_conformance_tests.validate import (
    discover_suites,
    parse_function_descriptions,
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


def _write_template(tmp_path: Path, body: str) -> str:
    path = tmp_path / "template.yaml"
    path.write_text(body)
    return str(path)


# --- parse_function_descriptions -------------------------------------------


def test_function_description_from_literal_numeric_prefix(tmp_path: Path) -> None:
    template = _write_template(
        tmp_path,
        """
Resources:
  StepBasic:
    Type: AWS::Serverless::Function
    Properties:
      FunctionName: 1-1-step-basic
""",
    )

    assert parse_function_descriptions(template) == [("StepBasic", "1-1")]


def test_function_description_from_tagged_sub_prefix(tmp_path: Path) -> None:
    template = _write_template(
        tmp_path,
        """
Resources:
  OtelWaitResume:
    Type: AWS::Serverless::Function
    Properties:
      FunctionName: !Sub "otel-2-${AWS::StackName}"
""",
    )

    assert parse_function_descriptions(template) == [("OtelWaitResume", "otel-2")]


def test_function_description_from_long_form_sub_sequence(tmp_path: Path) -> None:
    template = _write_template(
        tmp_path,
        """
Resources:
  OtelRetry:
    Type: AWS::Serverless::Function
    Properties:
      FunctionName:
        Fn::Sub:
          - "OTEL-3-${Suffix}"
          - Suffix: retry
""",
    )

    assert parse_function_descriptions(template) == [("OtelRetry", "otel-3")]


def test_function_description_from_long_form_sub_string(tmp_path: Path) -> None:
    template = _write_template(
        tmp_path,
        """
Resources:
  MapBasic:
    Type: AWS::Serverless::Function
    Properties:
      FunctionName:
        Fn::Sub: "9-1-${AWS::StackName}"
""",
    )

    assert parse_function_descriptions(template) == [("MapBasic", "9-1")]


def test_function_descriptions_ignore_unprefixed_and_missing_names(
    tmp_path: Path,
) -> None:
    template = _write_template(
        tmp_path,
        """
Resources:
  UnprefixedFunction:
    Type: AWS::Serverless::Function
    Properties:
      FunctionName: step-basic
  MissingFunctionName:
    Type: AWS::Serverless::Function
    Properties:
      Handler: index.handler
  NotAFunction:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: 1-2-not-a-function
""",
    )

    assert parse_function_descriptions(template) == []
