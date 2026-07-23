# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the validate module.

Covers ``discover_suites`` and ``parse_not_implemented``; additional
pure-function tests for ``validate`` belong here too.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from aws_durable_execution_conformance_tests.validate import (
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


# region End-to-end ExpectedLogs validation


def _insights_row(timestamp: str, message: str) -> list[dict[str, str]]:
    return [
        {"field": "@timestamp", "value": timestamp},
        {"field": "@message", "value": message},
        {"field": "@ptr", "value": "ptr"},
    ]


class _StubCfnClient:
    def describe_stack_resource(self, **kwargs):
        return {"StackResourceDetail": {"PhysicalResourceId": "my-stack-PluginFn-abc123"}}


class _StubLogsClient:
    """Stub Logs Insights client returning one completed query."""

    def __init__(self, rows: list[list[dict[str, str]]]) -> None:
        self._rows = rows
        self.queries: list[dict] = []

    def start_query(self, **kwargs):
        self.queries.append(kwargs)
        return {"queryId": "q-1"}

    def get_query_results(self, **kwargs):
        return {"status": "Complete", "results": self._rows}


def _run_expected_logs(monkeypatch, expected_logs, messages_in_order):
    """Drive _validate_expected_logs end-to-end with stubbed AWS clients."""
    import aws_durable_execution_conformance_tests.cloudwatch as cloudwatch_module
    from aws_durable_execution_conformance_tests.clients import AwsClients
    from aws_durable_execution_conformance_tests.validate import _validate_expected_logs
    from aws_durable_execution_conformance_tests.variables import PlaceholderContext

    monkeypatch.setattr(cloudwatch_module.time, "sleep", lambda _s: None)

    rows = [_insights_row(f"2026-07-23 22:32:3{i}.000", msg) for i, msg in enumerate(messages_in_order)]
    logs_client = _StubLogsClient(rows)
    description_data = {
        "Variables": {"INPUT_1": "abc123XY"},
        "ExpectedLogs": expected_logs,
    }
    context = PlaceholderContext()
    context.resolve_variables(description_data["Variables"])

    errors = _validate_expected_logs(
        description_data=description_data,
        stack_name="my-stack",
        function_name="PluginFn",
        execution_arn="arn:aws:lambda:us-west-2:123456789012:function:f:$LATEST/durable-execution/execution/e1",
        start_time_ms=1_000_000,
        aws_clients=AwsClients({"cloudformation": _StubCfnClient(), "logs": logs_client}),
        context=context,
    )
    return errors, logs_client


def test_e2e_expected_logs_ordered_pass(monkeypatch) -> None:
    """Full path: CFN log-group resolution -> Insights query -> placeholder
    substitution -> ordered validation. Mirrors a plugin-suite spec."""
    errors, logs_client = _run_expected_logs(
        monkeypatch,
        expected_logs=[
            {"pattern": "CONFPLUGIN invocation-start first=true", "count": 1},
            {"pattern": "Greeting step running for: ${INPUT_1}", "count": 1},
            {"pattern": "CONFPLUGIN invocation-end status=SUCCEEDED", "count": 1},
            {"pattern": "CONFPLUGIN invocation-start first=false", "count": 0},
            {"pattern": "concurrent-hook", "count": 1, "unordered": True},
        ],
        messages_in_order=[
            "concurrent-hook",
            "CONFPLUGIN invocation-start first=true",
            "Greeting step running for: abc123XY",
            "CONFPLUGIN invocation-end status=SUCCEEDED",
        ],
    )
    assert errors == []
    # The Insights query is scoped to the execution ARN
    assert "durable-execution/execution/e1" in logs_client.queries[0]["queryString"]


def test_e2e_expected_logs_ordered_violation_fails(monkeypatch) -> None:
    """Same path, but the terminal line precedes the start line: the
    ordered-by-default semantics must reject it."""
    errors, _ = _run_expected_logs(
        monkeypatch,
        expected_logs=[
            {"pattern": "CONFPLUGIN invocation-start first=true", "count": 1},
            {"pattern": "CONFPLUGIN invocation-end status=SUCCEEDED", "count": 1},
        ],
        messages_in_order=[
            "CONFPLUGIN invocation-end status=SUCCEEDED",
            "CONFPLUGIN invocation-start first=true",
        ],
    )
    assert len(errors) == 1
    assert "invocation-end status=SUCCEEDED" in errors[0]


def test_e2e_expected_logs_absent_field_skips_validation(monkeypatch) -> None:
    from aws_durable_execution_conformance_tests.clients import AwsClients
    from aws_durable_execution_conformance_tests.validate import _validate_expected_logs

    errors = _validate_expected_logs(
        description_data={},
        stack_name="my-stack",
        function_name="PluginFn",
        execution_arn="arn:whatever",
        start_time_ms=0,
        aws_clients=AwsClients({}),  # must not be touched when ExpectedLogs is absent
        context=None,
    )
    assert errors == []


# endregion
