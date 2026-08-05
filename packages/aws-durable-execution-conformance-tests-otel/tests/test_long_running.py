# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contracts for deferred long-running OpenTelemetry conformance runs."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from aws_durable_execution_conformance_tests.validate import (
    _CfnSafeLoader,
    parse_function_descriptions,
)
from aws_durable_execution_conformance_tests_otel import long_running
from aws_durable_execution_conformance_tests_otel.long_running import (
    CALLBACK_CASE,
    MAX_DELAY_SECONDS,
    STATE_VERSION,
    SUPPORTED_VIEWS,
    ExecutionState,
    RunState,
    _premature_executions,
    _requirement_cases,
    _requirement_for_view,
    _resolved_input,
    _send_due_callback,
    _validate_delay,
    _validate_view,
    run_to_completion,
)

ROOT = Path(__file__).resolve().parents[3]
EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples"
WORKFLOWS_DIR = ROOT / ".github" / "workflows"
EXPECTED_MAPPINGS = [
    ("OtelLongRunning1Wait", "otel-long-running-1"),
    ("OtelLongRunning2Retry", "otel-long-running-2"),
    ("OtelLongRunning3Callback", "otel-long-running-3"),
    ("OtelLongRunning4ChainedInvoke", "otel-long-running-4"),
]


@pytest.mark.parametrize("value", [1, 60, MAX_DELAY_SECONDS])
def test_delay_accepts_the_supported_range(value: int) -> None:
    assert _validate_delay(value) == value


@pytest.mark.parametrize("value", [0, MAX_DELAY_SECONDS + 1])
def test_delay_rejects_values_outside_one_day(value: int) -> None:
    with pytest.raises(ValueError, match="1 through 86400"):
        _validate_delay(value)


def test_run_state_round_trips_callback_progress(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    state = RunState(
        language="python",
        view="invocation",
        region="us-west-2",
        name="python-xray-long",
        stack_name="conformance-tests-python-xray-long",
        template="template.yaml",
        delay_seconds=86400,
        launched_at_ms=1000,
        source_revision="a" * 40,
        otel_service_name="custom-long-running-service",
        executions=[
            ExecutionState(
                description_id=CALLBACK_CASE,
                function_name="OtelLongRunning3Callback",
                execution_arn="arn:execution",
                invocation_started_at_ms=1000,
                bindings={"LONG_DELAY_SECONDS": "86400"},
                callback_sent_at_ms=86401000,
            )
        ],
    )

    state.save(state_path)

    assert RunState.load(state_path) == state
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["version"] == STATE_VERSION
    assert payload["source_revision"] == "a" * 40
    assert payload["otel_service_name"] == "custom-long-running-service"


def test_run_state_rejects_an_old_schema_before_reading_new_fields(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({"version": STATE_VERSION - 1}), encoding="utf-8")

    with pytest.raises(ValueError, match=f"version {STATE_VERSION - 1}; expected {STATE_VERSION}"):
        RunState.load(state_path)


@pytest.mark.parametrize(
    ("language", "view"),
    [
        ("java", "invocation"),
        ("java", "execution"),
        ("javascript", "invocation"),
        ("javascript", "execution"),
        ("python", "invocation"),
        ("python", "execution"),
    ],
)
def test_runtime_accepts_supported_telemetry_views(language: str, view: str) -> None:
    assert _validate_view(language, view) == view


def test_long_running_uses_the_resource_service_name_by_default() -> None:
    assert SUPPORTED_VIEWS["java"] == {"execution", "invocation"}
    options = long_running._otel_options("java", "execution", "us-west-2")
    assert options["otel_service_name"] == "durable-execution-conformance"
    python_options = long_running._otel_options("python", "execution", "us-west-2")
    assert python_options["otel_service_name"] == "durable-execution-conformance"
    configured_options = long_running._otel_options(
        "python",
        "execution",
        "us-west-2",
        service_name="custom-long-running-service",
    )
    assert configured_options["otel_service_name"] == "custom-long-running-service"


@pytest.mark.parametrize("phase", ["launch", "run"])
def test_long_running_launch_commands_accept_the_resource_service_name(
    phase: str,
) -> None:
    command = [
        phase,
        "--template",
        "template.yaml",
        "--language",
        "python",
        "--name",
        "test",
        "--state-file",
        "state.json",
        "--lambda-execution-role-arn",
        "arn:role",
        "--otel-layer-arn",
        "arn:layer",
        "--otel-service-name",
        "custom-long-running-service",
    ]
    if phase == "run":
        command.extend(
            [
                "--result-file",
                "result.json",
                "--history-dir",
                "history",
                "--report-file",
                "report",
            ]
        )

    args = long_running._parser().parse_args(command)

    assert args.otel_service_name == "custom-long-running-service"


def test_long_running_checks_always_write_trace_artifacts() -> None:
    assert long_running._otel_options("python", "invocation", "us-west-2")["otel_write_trace_artifact"] is True


def test_requirement_input_uses_the_workflow_delay_override() -> None:
    requirement = yaml.safe_load(_requirement_cases()["otel-long-running-1"].read_text(encoding="utf-8"))

    resolved, bindings = _resolved_input(requirement, 86400)

    assert resolved == {"scenario": "long-wait", "delay_seconds": "86400"}
    assert bindings == {"LONG_DELAY_SECONDS": "86400"}


@pytest.mark.parametrize(
    ("view", "assertion_key"),
    [
        ("invocation", "TelemetryAssertions"),
        ("execution", "ExecutionTelemetryAssertions"),
    ],
)
def test_requirement_selects_assertions_for_the_deployed_view(
    view: str,
    assertion_key: str,
) -> None:
    requirement = yaml.safe_load(_requirement_cases()["otel-long-running-1"].read_text(encoding="utf-8"))

    selected = _requirement_for_view(requirement, view)

    assert selected["TelemetryAssertions"] == requirement[assertion_key]


class _CallbackClient:
    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []

    def send_durable_execution_callback_success(
        self,
        **kwargs: Any,
    ) -> dict[str, Any]:
        self.requests.append(kwargs)
        return {}


def test_due_callback_is_sent_once_and_persisted() -> None:
    execution = ExecutionState(
        description_id=CALLBACK_CASE,
        function_name="OtelLongRunning3Callback",
        execution_arn="arn:execution",
        invocation_started_at_ms=1000,
        bindings={},
    )
    state = RunState(
        language="python",
        view="invocation",
        region="us-west-2",
        name="test",
        stack_name="stack",
        template="template.yaml",
        delay_seconds=10,
        launched_at_ms=0,
        source_revision="a" * 40,
        executions=[execution],
    )
    history = {
        "Events": [
            {
                "EventType": "CallbackStarted",
                "CallbackStartedDetails": {"CallbackId": "callback-1"},
            }
        ]
    }
    client = _CallbackClient()

    assert _send_due_callback(state, execution, history, client, 10999) is False
    assert _send_due_callback(state, execution, history, client, 11000) is True
    assert _send_due_callback(state, execution, history, client, 12000) is False
    assert execution.callback_sent_at_ms == 11000
    assert client.requests == [
        {
            "CallbackId": "callback-1",
            "Result": b'"callback-complete"',
        }
    ]


def test_completed_callback_is_recovered_without_resending() -> None:
    execution = ExecutionState(
        description_id=CALLBACK_CASE,
        function_name="OtelLongRunning3Callback",
        execution_arn="arn:execution",
        invocation_started_at_ms=1000,
        bindings={},
    )
    state = RunState(
        language="python",
        view="invocation",
        region="us-west-2",
        name="test",
        stack_name="stack",
        template="template.yaml",
        delay_seconds=10,
        launched_at_ms=0,
        source_revision="a" * 40,
        executions=[execution],
    )
    history = {
        "Events": [
            {
                "EventType": "CallbackSucceeded",
                "EventTimestamp": 11.0,
            },
            {
                "EventType": "ExecutionSucceeded",
                "EventTimestamp": 12.0,
            },
        ]
    }
    client = _CallbackClient()

    assert _send_due_callback(state, execution, history, client, 12000) is True
    assert execution.callback_sent_at_ms == 11000
    assert client.requests == []


def test_terminal_callback_without_an_outcome_is_not_resent() -> None:
    execution = ExecutionState(
        description_id=CALLBACK_CASE,
        function_name="OtelLongRunning3Callback",
        execution_arn="arn:execution",
        invocation_started_at_ms=1000,
        bindings={},
    )
    state = RunState(
        language="python",
        view="invocation",
        region="us-west-2",
        name="test",
        stack_name="stack",
        template="template.yaml",
        delay_seconds=10,
        launched_at_ms=0,
        source_revision="a" * 40,
        executions=[execution],
    )
    history = {
        "Events": [
            {
                "EventType": "ExecutionFailed",
                "EventTimestamp": 12.0,
            },
        ]
    }
    client = _CallbackClient()

    assert _send_due_callback(state, execution, history, client, 12000) is False
    assert execution.callback_sent_at_ms is None
    assert client.requests == []


def test_terminal_event_timestamps_enforce_the_configured_delay() -> None:
    executions = [
        ExecutionState(
            description_id=f"otel-long-running-{case_number}",
            function_name=f"Function{case_number}",
            execution_arn=f"arn:execution:{case_number}",
            invocation_started_at_ms=1000,
            bindings={},
        )
        for case_number in range(1, 5)
    ]
    state = RunState(
        language="python",
        view="invocation",
        region="us-west-2",
        name="test",
        stack_name="stack",
        template="template.yaml",
        delay_seconds=10,
        launched_at_ms=1000,
        source_revision="a" * 40,
        executions=executions,
    )
    statuses = {execution.description_id: "SUCCEEDED" for execution in executions}
    histories = {
        execution.description_id: {
            "Events": [
                {
                    "EventType": "ExecutionSucceeded",
                    "EventTimestamp": 10.999,
                },
            ]
        }
        for execution in executions
    }

    assert _premature_executions(state, statuses, histories) == executions

    for history in histories.values():
        history["Events"][0]["EventTimestamp"] = 11.0

    assert _premature_executions(state, statuses, histories) == []


def test_launch_retains_stack_after_a_failed_deployment_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Exporter:
        def __init__(self) -> None:
            self.parameter_overrides: dict[str, str] = {}

    configured_options: list[Any] = []

    class _Profile:
        def configure(self, options: Any) -> _Exporter:
            configured_options.append(options)
            return _Exporter()

    class _Deployer:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def build(self) -> None:
            pass

        def deploy(self, **_kwargs: Any) -> None:
            raise RuntimeError("deployment failed")

    deleted: list[tuple[str, str]] = []
    args = argparse.Namespace(
        language="python",
        view="invocation",
        delay_seconds=10,
        state_file=str(tmp_path / "state.json"),
        template="template.yaml",
        build_dir=str(tmp_path / "build"),
        region="us-west-2",
        name="failed-deploy",
        otel_layer_arn="arn:layer",
        otel_service_name="custom-long-running-service",
        lambda_execution_role_arn="arn:role",
        source_revision="a" * 40,
    )
    requirements = {
        description_id: tmp_path / f"{description_id}.yaml" for _function_name, description_id in EXPECTED_MAPPINGS
    }
    monkeypatch.setattr(long_running, "_requirement_cases", lambda: requirements)
    monkeypatch.setattr(long_running, "parse_function_descriptions", lambda _path: EXPECTED_MAPPINGS)
    monkeypatch.setattr(long_running, "AdotExporterProfile", _Profile)
    monkeypatch.setattr(long_running, "Deployer", _Deployer)
    monkeypatch.setattr(long_running, "delete_stack", lambda name, region: deleted.append((name, region)))

    with pytest.raises(RuntimeError, match="deployment failed"):
        long_running.launch(args)

    assert deleted == []
    assert configured_options[0].service_name == "custom-long-running-service"


def test_launch_rejects_duplicate_requirement_mappings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requirements = {
        description_id: tmp_path / f"{description_id}.yaml" for _function_name, description_id in EXPECTED_MAPPINGS
    }
    duplicate_mappings = [
        *EXPECTED_MAPPINGS,
        ("DuplicateLongWait", "otel-long-running-1"),
    ]
    args = argparse.Namespace(
        language="python",
        view="invocation",
        delay_seconds=10,
        state_file=str(tmp_path / "state.json"),
        template="template.yaml",
    )
    monkeypatch.setattr(long_running, "_requirement_cases", lambda: requirements)
    monkeypatch.setattr(long_running, "parse_function_descriptions", lambda _path: duplicate_mappings)

    with pytest.raises(ValueError, match="must map every otel-long-running requirement exactly once"):
        long_running.launch(args)


def _short_run_args(tmp_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        state_file=str(tmp_path / "state.json"),
        result_file=str(tmp_path / "result.json"),
        check_timeout=900.0,
        check_interval=15.0,
    )


def _save_short_run_state(args: argparse.Namespace) -> int:
    RunState(
        language="python",
        view="invocation",
        region="us-west-2",
        name="short",
        stack_name="conformance-tests-short",
        template="template.yaml",
        delay_seconds=600,
        launched_at_ms=0,
        source_revision="a" * 40,
        executions=[],
    ).save(Path(args.state_file))
    return 0


def test_deferred_check_can_retain_a_terminal_stack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cleanup_options: list[bool] = []

    def fake_check(
        _args: argparse.Namespace,
        *,
        delete_terminal_stack: bool = True,
    ) -> int:
        cleanup_options.append(delete_terminal_stack)
        return 0

    monkeypatch.setattr(long_running, "check", fake_check)

    assert (
        long_running.main(
            [
                "check",
                "--state-file",
                "state.json",
                "--result-file",
                "result.json",
                "--history-dir",
                "history",
                "--report-file",
                "report",
                "--no-cleanup",
            ]
        )
        == 0
    )
    assert cleanup_options == [False]


def test_deferred_check_finishes_after_sending_the_due_callback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "state.json"
    result_path = tmp_path / "result.json"
    execution = ExecutionState(
        description_id=CALLBACK_CASE,
        function_name="OtelLongRunning3Callback",
        execution_arn="arn:execution",
        invocation_started_at_ms=1000,
        bindings={},
    )
    RunState(
        language="python",
        view="invocation",
        region="us-west-2",
        name="test",
        stack_name="stack",
        template="template.yaml",
        delay_seconds=10,
        launched_at_ms=0,
        source_revision="a" * 40,
        executions=[execution],
    ).save(state_path)
    pending_history = {
        "ExecutionStatus": "RUNNING",
        "Events": [
            {
                "EventType": "CallbackStarted",
                "CallbackStartedDetails": {"CallbackId": "callback-1"},
            }
        ],
    }
    terminal_history = {
        "ExecutionStatus": "SUCCEEDED",
        "Events": [
            *pending_history["Events"],
            {
                "EventType": "CallbackSucceeded",
                "EventTimestamp": 11.5,
            },
            {
                "EventType": "ExecutionSucceeded",
                "EventTimestamp": 12.0,
            },
        ],
    }
    histories = iter([pending_history, terminal_history])
    client = _CallbackClient()
    sleeps: list[float] = []
    monotonic_values = iter([0.0, 0.0])
    validated_histories: list[dict[str, Any]] = []
    validated_finished_at_ms: list[int] = []

    def fake_validate_terminal_execution(**kwargs: Any) -> Any:
        validated_histories.append(kwargs["history"])
        validated_finished_at_ms.append(kwargs["finished_at_ms"])
        return long_running.ReportEntry(
            id=CALLBACK_CASE,
            suite=long_running.SUITE,
            status=long_running.ReportStatus.PASSED,
        )

    monkeypatch.setattr(long_running.AwsClients, "create", lambda *_args, **_kwargs: {"lambda": client})
    monkeypatch.setattr(long_running, "get_execution_history", lambda *_args, **_kwargs: next(histories))
    monkeypatch.setattr(long_running, "_requirement_cases", lambda: {CALLBACK_CASE: tmp_path / "requirement.yaml"})
    monkeypatch.setattr(long_running, "_validate_terminal_execution", fake_validate_terminal_execution)
    monkeypatch.setattr(long_running, "_emit_report", lambda *_args, **_kwargs: None)
    time_values = iter([11.0, 31.0])
    monkeypatch.setattr(long_running.time, "time", lambda: next(time_values))
    monkeypatch.setattr(long_running.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(long_running.time, "sleep", sleeps.append)

    args = argparse.Namespace(
        state_file=str(state_path),
        result_file=str(result_path),
        history_dir=str(tmp_path / "history"),
        report_file=str(tmp_path / "report"),
        otel_backend="xray",
        check_timeout=900.0,
        check_interval=15.0,
    )

    assert long_running.check(args, delete_terminal_stack=False) == 0
    assert json.loads(result_path.read_text(encoding="utf-8")) == {
        "status": "passed",
        "state_changed": True,
        "rollover_ready": True,
    }
    assert RunState.load(state_path).executions[0].callback_sent_at_ms == 11000
    assert client.requests == [
        {
            "CallbackId": "callback-1",
            "Result": b'"callback-complete"',
        }
    ]
    assert sleeps == [15.0]
    assert validated_histories == [terminal_history]
    assert validated_finished_at_ms == [31000]


def test_callback_completion_poll_preserves_pending_state_at_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution = ExecutionState(
        description_id=CALLBACK_CASE,
        function_name="OtelLongRunning3Callback",
        execution_arn="arn:execution",
        invocation_started_at_ms=1000,
        bindings={},
        callback_sent_at_ms=11000,
    )
    state = RunState(
        language="python",
        view="invocation",
        region="us-west-2",
        name="test",
        stack_name="stack",
        template="template.yaml",
        delay_seconds=10,
        launched_at_ms=0,
        source_revision="a" * 40,
        executions=[execution],
    )
    history = {"ExecutionStatus": "RUNNING", "Events": []}
    histories = {CALLBACK_CASE: history}
    statuses: dict[str, str | None] = {CALLBACK_CASE: "RUNNING"}
    sleeps: list[float] = []
    monotonic_values = iter([0.0, 0.0, 901.0])

    monkeypatch.setattr(long_running, "get_execution_history", lambda *_args, **_kwargs: history)
    monkeypatch.setattr(long_running.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(long_running.time, "sleep", sleeps.append)

    long_running._poll_pending_executions(
        state,
        _CallbackClient(),
        histories,
        statuses,
        tmp_path / "history",
        timeout=900.0,
        interval=15.0,
    )

    assert statuses == {CALLBACK_CASE: "RUNNING"}
    assert sleeps == [15.0]


def test_premature_failure_is_reported_after_advancing_a_pending_callback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "state.json"
    result_path = tmp_path / "result.json"
    premature_execution = ExecutionState(
        description_id="otel-long-running-1",
        function_name="OtelLongRunning1Wait",
        execution_arn="arn:premature",
        invocation_started_at_ms=1000,
        bindings={},
    )
    callback_execution = ExecutionState(
        description_id=CALLBACK_CASE,
        function_name="OtelLongRunning3Callback",
        execution_arn="arn:callback",
        invocation_started_at_ms=1000,
        bindings={},
    )
    RunState(
        language="python",
        view="invocation",
        region="us-west-2",
        name="test",
        stack_name="stack",
        template="template.yaml",
        delay_seconds=10,
        launched_at_ms=0,
        source_revision="a" * 40,
        executions=[premature_execution, callback_execution],
    ).save(state_path)
    premature_history = {
        "ExecutionStatus": "FAILED",
        "Events": [
            {
                "EventType": "ExecutionFailed",
                "EventTimestamp": 10.0,
            }
        ],
    }
    pending_history = {
        "ExecutionStatus": "RUNNING",
        "Events": [
            {
                "EventType": "CallbackStarted",
                "CallbackStartedDetails": {"CallbackId": "callback-1"},
            }
        ],
    }
    histories = iter([premature_history, pending_history, pending_history])
    client = _CallbackClient()
    reports: list[Any] = []
    monotonic_values = iter([0.0, 0.0, 901.0])
    time_values = iter([11.0, 31.0])
    premature_requirement = tmp_path / "premature.yaml"
    premature_requirement.write_text("description: premature execution\n", encoding="utf-8")

    monkeypatch.setattr(long_running.AwsClients, "create", lambda *_args, **_kwargs: {"lambda": client})
    monkeypatch.setattr(long_running, "get_execution_history", lambda *_args, **_kwargs: next(histories))
    monkeypatch.setattr(
        long_running,
        "_requirement_cases",
        lambda: {
            premature_execution.description_id: premature_requirement,
            CALLBACK_CASE: tmp_path / "callback.yaml",
        },
    )
    monkeypatch.setattr(long_running, "_emit_report", lambda report, *_args, **_kwargs: reports.append(report))
    monkeypatch.setattr(long_running.time, "time", lambda: next(time_values))
    monkeypatch.setattr(long_running.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(long_running.time, "sleep", lambda _seconds: None)

    args = argparse.Namespace(
        state_file=str(state_path),
        result_file=str(result_path),
        history_dir=str(tmp_path / "history"),
        report_file=str(tmp_path / "report"),
        otel_backend="xray",
        check_timeout=900.0,
        check_interval=15.0,
    )

    assert long_running.check(args, delete_terminal_stack=False) == 1
    assert json.loads(result_path.read_text(encoding="utf-8")) == {
        "status": "failed",
        "state_changed": True,
        "rollover_ready": False,
    }
    assert RunState.load(state_path).executions[1].callback_sent_at_ms == 11000
    assert client.requests == [
        {
            "CallbackId": "callback-1",
            "Result": b'"callback-complete"',
        }
    ]
    assert len(reports) == 1
    assert [entry.id for entry in reports[0].entries] == [premature_execution.description_id]


def test_terminal_premature_failure_is_rollover_ready(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "state.json"
    result_path = tmp_path / "result.json"
    requirement_path = tmp_path / "requirement.yaml"
    requirement_path.write_text("description: premature execution\n", encoding="utf-8")
    execution = ExecutionState(
        description_id="otel-long-running-1",
        function_name="OtelLongRunning1Wait",
        execution_arn="arn:premature",
        invocation_started_at_ms=1000,
        bindings={},
    )
    RunState(
        language="python",
        view="invocation",
        region="us-west-2",
        name="test",
        stack_name="stack",
        template="template.yaml",
        delay_seconds=10,
        launched_at_ms=0,
        source_revision="a" * 40,
        executions=[execution],
    ).save(state_path)
    history = {
        "ExecutionStatus": "FAILED",
        "Events": [
            {
                "EventType": "ExecutionFailed",
                "EventTimestamp": 10.0,
            }
        ],
    }
    reports: list[Any] = []
    time_values = iter([11.0, 12.0])

    monkeypatch.setattr(long_running.AwsClients, "create", lambda *_args, **_kwargs: {"lambda": object()})
    monkeypatch.setattr(long_running, "get_execution_history", lambda *_args, **_kwargs: history)
    monkeypatch.setattr(long_running, "_requirement_cases", lambda: {execution.description_id: requirement_path})
    monkeypatch.setattr(long_running, "_emit_report", lambda report, *_args, **_kwargs: reports.append(report))
    monkeypatch.setattr(
        long_running,
        "_validate_terminal_execution",
        lambda **_kwargs: pytest.fail("premature executions must not run terminal validation"),
    )
    monkeypatch.setattr(long_running.time, "time", lambda: next(time_values))

    args = argparse.Namespace(
        state_file=str(state_path),
        result_file=str(result_path),
        history_dir=str(tmp_path / "history"),
        report_file=str(tmp_path / "report"),
        otel_backend="xray",
        check_timeout=900.0,
        check_interval=15.0,
    )

    assert long_running.check(args, delete_terminal_stack=False) == 1
    assert json.loads(result_path.read_text(encoding="utf-8")) == {
        "status": "failed",
        "state_changed": False,
        "rollover_ready": True,
    }
    assert len(reports) == 1
    assert reports[0].exit_code() == 1


def test_callback_state_survives_a_transient_polling_read_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "state.json"
    result_path = tmp_path / "result.json"
    execution = ExecutionState(
        description_id=CALLBACK_CASE,
        function_name="OtelLongRunning3Callback",
        execution_arn="arn:execution",
        invocation_started_at_ms=1000,
        bindings={},
    )
    RunState(
        language="python",
        view="invocation",
        region="us-west-2",
        name="test",
        stack_name="stack",
        template="template.yaml",
        delay_seconds=10,
        launched_at_ms=0,
        source_revision="a" * 40,
        executions=[execution],
    ).save(state_path)
    pending_history = {
        "ExecutionStatus": "RUNNING",
        "Events": [
            {
                "EventType": "CallbackStarted",
                "CallbackStartedDetails": {"CallbackId": "callback-1"},
            }
        ],
    }
    histories = iter([pending_history, None])
    client = _CallbackClient()
    monotonic_values = iter([0.0, 0.0, 901.0])
    time_values = iter([11.0, 31.0])

    monkeypatch.setattr(long_running.AwsClients, "create", lambda *_args, **_kwargs: {"lambda": client})
    monkeypatch.setattr(long_running, "get_execution_history", lambda *_args, **_kwargs: next(histories))
    monkeypatch.setattr(long_running, "_requirement_cases", lambda: {CALLBACK_CASE: tmp_path / "requirement.yaml"})
    monkeypatch.setattr(long_running.time, "time", lambda: next(time_values))
    monkeypatch.setattr(long_running.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(long_running.time, "sleep", lambda _seconds: None)

    args = argparse.Namespace(
        state_file=str(state_path),
        result_file=str(result_path),
        history_dir=str(tmp_path / "history"),
        report_file=str(tmp_path / "report"),
        otel_backend="xray",
        check_timeout=900.0,
        check_interval=15.0,
    )

    assert long_running.check(args, delete_terminal_stack=False) == 0
    assert json.loads(result_path.read_text(encoding="utf-8")) == {
        "status": "pending",
        "state_changed": True,
        "rollover_ready": False,
    }
    assert RunState.load(state_path).executions[0].callback_sent_at_ms == 11000
    assert len(client.requests) == 1


def test_short_run_polls_again_after_sending_the_due_callback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    args = _short_run_args(tmp_path)
    statuses = iter(
        [
            {"status": "pending", "state_changed": True},
            {"status": "passed", "state_changed": False},
        ]
    )
    check_calls = 0
    cleanup_options: list[bool] = []
    sleeps: list[float] = []

    def fake_check(
        check_args: argparse.Namespace,
        *,
        delete_terminal_stack: bool = True,
    ) -> int:
        nonlocal check_calls
        check_calls += 1
        cleanup_options.append(delete_terminal_stack)
        Path(check_args.result_file).write_text(json.dumps(next(statuses)), encoding="utf-8")
        return 0

    monkeypatch.setattr(long_running, "launch", _save_short_run_state)
    monkeypatch.setattr(long_running, "check", fake_check)
    monkeypatch.setattr(long_running.time, "time", lambda: 600.0)
    monkeypatch.setattr(long_running.time, "monotonic", lambda: 0.0)
    monkeypatch.setattr(long_running.time, "sleep", sleeps.append)

    assert run_to_completion(args) == 0
    assert check_calls == 2
    assert cleanup_options == [False, False]
    assert sleeps == [15.0]


def test_short_run_retains_stack_after_an_error_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    args = _short_run_args(tmp_path)
    deleted: list[tuple[str, str]] = []
    cleanup_options: list[bool] = []

    def fake_check(
        check_args: argparse.Namespace,
        *,
        delete_terminal_stack: bool = True,
    ) -> int:
        cleanup_options.append(delete_terminal_stack)
        Path(check_args.result_file).write_text(
            json.dumps({"status": "error", "state_changed": False}),
            encoding="utf-8",
        )
        return 1

    monkeypatch.setattr(long_running, "launch", _save_short_run_state)
    monkeypatch.setattr(long_running, "check", fake_check)
    monkeypatch.setattr(long_running, "delete_stack", lambda name, region: deleted.append((name, region)))
    monkeypatch.setattr(long_running.time, "time", lambda: 600.0)
    monkeypatch.setattr(long_running.time, "monotonic", lambda: 0.0)

    assert run_to_completion(args) == 1
    assert cleanup_options == [False]
    assert deleted == []


def test_short_run_retains_stack_after_poll_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    args = _short_run_args(tmp_path)
    monotonic_values = iter([0.0, 901.0])
    deleted: list[tuple[str, str]] = []
    cleanup_options: list[bool] = []

    def fake_check(
        check_args: argparse.Namespace,
        *,
        delete_terminal_stack: bool = True,
    ) -> int:
        cleanup_options.append(delete_terminal_stack)
        Path(check_args.result_file).write_text(
            json.dumps({"status": "pending", "state_changed": False}),
            encoding="utf-8",
        )
        return 0

    monkeypatch.setattr(long_running, "launch", _save_short_run_state)
    monkeypatch.setattr(long_running, "check", fake_check)
    monkeypatch.setattr(long_running, "delete_stack", lambda name, region: deleted.append((name, region)))
    monkeypatch.setattr(long_running.time, "time", lambda: 600.0)
    monkeypatch.setattr(long_running.time, "monotonic", lambda: next(monotonic_values))

    with pytest.raises(RuntimeError, match=r"remained pending after 900.0 seconds"):
        run_to_completion(args)
    assert cleanup_options == [False]
    assert deleted == []


@pytest.mark.parametrize("language", ["java", "javascript", "python"])
def test_long_running_templates_map_the_complete_suite(language: str) -> None:
    template_path = EXAMPLES_DIR / language / "template-long-running.yaml"

    assert parse_function_descriptions(str(template_path)) == EXPECTED_MAPPINGS
    with template_path.open(encoding="utf-8") as stream:
        template = yaml.load(stream, Loader=_CfnSafeLoader)

    globals_config = template["Globals"]["Function"]
    assert globals_config["DurableConfig"] == {
        "ExecutionTimeout": 180000,
        "RetentionPeriodInDays": 3,
    }
    assert globals_config["Tracing"] == "Active"
    assert template["Parameters"]["OtelView"] == {
        "Type": "String",
        "Default": "invocation",
        "AllowedValues": ["invocation", "execution"],
        "Description": "OpenTelemetry plugin view used by the long-running functions",
    }
    assert globals_config["Environment"]["Variables"]["OTEL_PLUGIN_MODE"] == {
        "Ref": "OtelView",
    }
    assert set(template["Resources"]) == {
        *(logical_id for logical_id, _description_id in EXPECTED_MAPPINGS),
        "OtelLongRunning4InvokeTarget",
    }
    assert template["Resources"]["OtelLongRunning4ChainedInvoke"]["Properties"]["Environment"]["Variables"][
        "OTEL_INVOKE_TARGET_FUNCTION_NAME"
    ] == {"Sub": "${OtelLongRunning4InvokeTarget.Arn}:$LATEST"}


def test_python_long_running_handler_names_match_requirement_numbers() -> None:
    template_path = EXAMPLES_DIR / "python" / "template-long-running.yaml"
    with template_path.open(encoding="utf-8") as stream:
        resources = yaml.load(stream, Loader=_CfnSafeLoader)["Resources"]

    assert {logical_id: resource["Properties"]["Handler"] for logical_id, resource in resources.items()} == {
        "OtelLongRunning1Wait": "otel_long_running_1_wait.handler",
        "OtelLongRunning2Retry": "otel_long_running_2_retry.handler",
        "OtelLongRunning3Callback": "otel_long_running_3_callback.handler",
        "OtelLongRunning4ChainedInvoke": "otel_long_running_4_chained_invoke.handler",
        "OtelLongRunning4InvokeTarget": "otel_long_running_4_chained_invoke.target_handler",
    }


def test_long_running_handlers_use_runtime_delay_inputs() -> None:
    python_source = EXAMPLES_DIR / "python" / "src"
    javascript_source = EXAMPLES_DIR / "javascript" / "handlers"
    java_source = (
        EXAMPLES_DIR
        / "java"
        / "src"
        / "main"
        / "java"
        / "software"
        / "amazon"
        / "lambda"
        / "durable"
        / "conformance"
        / "otel"
    )

    assert "long_delay_seconds(event)" in (python_source / "otel_long_running_1_wait.py").read_text(encoding="utf-8")
    python_retry_source = (python_source / "otel_long_running_2_retry.py").read_text(encoding="utf-8")
    assert "long_delay_seconds(event)" in python_retry_source
    assert "jitter_strategy=JitterStrategy.NONE" in python_retry_source
    assert "long_delay_seconds(event)" in (python_source / "otel_long_running_4_chained_invoke.py").read_text(
        encoding="utf-8"
    )
    assert "longDelaySeconds(event)" in (javascript_source / "otel_21_long_retry.ts").read_text(encoding="utf-8")
    assert "longDelaySeconds(event)" in (javascript_source / "otel_23_long_chained_invoke.ts").read_text(
        encoding="utf-8"
    )
    assert "longDelaySeconds(event)" in (java_source / "OtelLongRunning2Retry.java").read_text(encoding="utf-8")
    assert "longDelaySeconds(event)" in (java_source / "OtelLongRunning4InvokeTarget.java").read_text(encoding="utf-8")


@pytest.mark.parametrize("language", ["java", "javascript", "python"])
def test_language_workflows_run_short_and_deferred_xray_runs(language: str) -> None:
    entry_workflow = (WORKFLOWS_DIR / f"{language}-opentelemetry.yml").read_text(encoding="utf-8")
    entry_workflow_config = yaml.safe_load(entry_workflow)
    orchestrator = (WORKFLOWS_DIR / "opentelemetry.yml").read_text(encoding="utf-8")
    orchestrator_config = yaml.safe_load(orchestrator)
    workflow = (WORKFLOWS_DIR / "opentelemetry-long-running.yml").read_text(encoding="utf-8")
    workflow_config = yaml.safe_load(workflow)

    assert "  pull_request:" in entry_workflow
    assert "  push:" in entry_workflow
    assert entry_workflow.count("    branches: [main]") == 2
    assert "  schedule:" in entry_workflow
    assert "  workflow_dispatch:" in entry_workflow
    assert 'cron: "0 7 * * *"' in entry_workflow
    assert 'default: "82800"' in entry_workflow
    preset = entry_workflow_config["jobs"]["conformance"]
    assert preset["uses"] == "./.github/workflows/opentelemetry.yml"
    assert preset["with"]["language"] == language

    assert "github.event_name == 'pull_request' || github.event_name == 'push'" in orchestrator
    assert "github.event_name == 'schedule' && 'auto'" in orchestrator
    assert "&& 'short'" in orchestrator
    assert "&& '60'" in orchestrator
    for view in ("invocation", "execution"):
        delay_expression = orchestrator_config["jobs"][f"long-running-{view}"]["with"]["delay_seconds"]
        assert "inputs.phase == 'short'" in delay_expression
        assert "&& '60'" in delay_expression

    assert "  workflow_call:" in workflow
    assert 'default: "60"' in workflow
    assert "  pull_request:" not in workflow
    assert "  push:" not in workflow
    assert "  schedule:" not in workflow
    assert "  workflow_dispatch:" not in workflow
    assert "github.event.pull_request.head.repo.full_name == github.repository" in workflow
    assert "env.PHASE != 'short'" in workflow
    assert "phase=launch" in workflow
    assert "phase=check" in workflow
    assert "check_status: ${{ steps.check.outputs.status }}" in workflow
    assert "value: ${{ jobs.run.outputs.check_status }}" in workflow
    assert "rollover_ready: ${{ steps.check.outputs.rollover_ready }}" in workflow
    assert "value: ${{ jobs.run.outputs.rollover_ready }}" in workflow
    run_steps = {step["name"]: step for step in workflow_config["jobs"]["run"]["steps"]}
    check_script = run_steps["Check long-running conformance executions"]["run"]
    assert 'echo "rollover_ready=false" >> "$GITHUB_OUTPUT"' in check_script
    assert "if jq -e 'has(\"rollover_ready\")'" in check_script
    assert "done < <(jq -r '.executions[].description_id' \"$STATE_FILE\")" in check_script
    assert 'if [ "$status" = "passed" ] || [ "$status" = "failed" ]; then' in check_script
    assert "all(.[]; is_terminal)" in check_script
    assert 'cd "$CURRENT_CHECKER"' in check_script
    workflow_support_checkout = run_steps["Check out workflow support"]
    assert workflow_support_checkout["with"] == {
        "repository": "${{ job.workflow_repository }}",
        "ref": "${{ job.workflow_sha }}",
        "path": ".build/workflow-support",
    }
    assert "Short run" in workflow_config["jobs"]["run"]["name"]
    assert "Daily run" in workflow_config["jobs"]["run"]["name"]
    assert "inputs.view" not in workflow_config["jobs"]["run"]["name"]
    assert run_steps["Check out next conformance tests"]["with"] == {
        "repository": "${{ inputs.conformance_repository }}",
        "ref": "${{ inputs.conformance_test_sha }}",
        "clean": False,
    }
    assert run_steps["Prepare next conformance example"]["if"] == ("steps.check.outputs.rollover_ready == 'true'")
    launch_step = run_steps["Launch long-running conformance executions"]
    assert "steps.state.outputs.phase == 'launch'" in launch_step["if"]
    assert "steps.check.outputs.rollover_ready == 'true'" in launch_step["if"]
    assert 'rm -f "$STATE_FILE"' in launch_step["run"]
    persist_step = run_steps["Persist active run state"]
    assert "steps.state.outputs.phase == 'launch'" in persist_step["if"]
    assert "steps.check.outputs.rollover_ready == 'true'" in persist_step["if"]
    assert run_steps["Persist updated callback state"]["if"] == (
        "steps.check.outputs.state_changed == 'true' && steps.check.outputs.rollover_ready != 'true'"
    )
    retire_script = run_steps["Retire previous state artifact"]["run"]
    assert '[ "$STATE_CHANGED" = "true" ] || [ "$ROLLOVER_READY" = "true" ]' in retire_script

    assert not any(name.startswith("next-long-running-") for name in orchestrator_config["jobs"])

    assert 'echo "No active $LANGUAGE $OTEL_VIEW long-running OTel run."' in workflow
    assert 'echo "active=false"' not in workflow
    assert "inputs.delay_seconds || '82800'" in workflow
    assert workflow.count('--source-revision "$CONFORMANCE_TEST_SHA"') == 2
    assert "source_revision=$(jq -r '.source_revision // empty' \"$STATE_FILE\")" in workflow
    assert "ref: ${{ steps.state.outputs.source_revision || inputs.conformance_test_sha }}" in workflow
    assert "actions: write" in workflow
    assert "otel-long-running-state" in workflow
    assert "aws_durable_execution_conformance_tests_otel.long_running launch" in workflow
    assert "aws_durable_execution_conformance_tests_otel.long_running check" in workflow
    assert "aws_durable_execution_conformance_tests_otel.long_running run" in workflow
    assert "--check-timeout 900" in workflow
    assert "--check-interval 15" in workflow
    assert 'gh api "repos/$RELEASE_REPOSITORY/releases/latest"' in workflow
    assert '--view "$OTEL_VIEW"' in workflow
    assert "--otel-layer-arn" in workflow
    assert workflow.count('--otel-service-name "$OTEL_SERVICE_NAME"') == 2
    assert "--otel-backend xray" in workflow
    assert "retention-days: 5" in workflow
    assert "actions/artifacts/$ARTIFACT_ID" in workflow
    assert "CHECK_EXIT_CODE" in workflow
    assert "id: launch" in workflow
    assert "id: persist" in workflow
    assert "steps.persist.outcome == 'failure'" not in workflow
    assert 'aws cloudformation delete-stack --stack-name "conformance-tests-$TEST_NAME"' not in workflow
    assert (
        workflow.index("- name: Persist updated callback state")
        < workflow.index("- name: Upload reports and histories")
        < workflow.index("- name: Retire previous state artifact")
    )
    assert "matrix:" not in workflow
    assert workflow_config["jobs"]["run"]["env"]["STATE_ARTIFACT"] == (
        "${{ inputs.language }}-otel-long-running-${{ inputs.view }}-${{ inputs.aws_region || 'us-west-2' }}-state"
    )
    assert workflow_config["jobs"]["run"]["env"]["TEST_NAME"] == (
        "${{ inputs.resource_prefix }}-olr-${{ inputs.view == 'invocation' && 'i' || 'e' }}"
        "${{ inputs.phase == 'short' && '-short' || '' }}"
    )
    assert workflow_config["env"]["OTEL_SERVICE_NAME"] == "durable-execution-conformance"
    assert "github.run_number" not in workflow
    assert "github.run_attempt" not in workflow


@pytest.mark.parametrize(
    ("legacy_status", "history_payloads", "expected_rollover_ready"),
    [
        (
            "passed",
            [
                {"ExecutionStatus": "SUCCEEDED"},
                {"Events": [{"EventType": "ExecutionSucceeded"}]},
            ],
            "true",
        ),
        (
            "failed",
            [
                {"ExecutionStatus": "FAILED"},
                {"Events": [{"EventType": "ExecutionTimedOut"}]},
            ],
            "true",
        ),
    ],
)
def test_workflow_derives_legacy_rollover_readiness_from_all_histories(
    tmp_path: Path,
    legacy_status: str,
    history_payloads: list[dict[str, Any]],
    expected_rollover_ready: str,
) -> None:
    if shutil.which("bash") is None or shutil.which("jq") is None:
        pytest.skip("workflow compatibility test requires bash and jq")

    workflow = yaml.safe_load((WORKFLOWS_DIR / "opentelemetry-long-running.yml").read_text(encoding="utf-8"))
    check_script = next(
        step["run"]
        for step in workflow["jobs"]["run"]["steps"]
        if step["name"] == "Check long-running conformance executions"
    )
    state_path = tmp_path / "state.json"
    result_path = tmp_path / "result.json"
    history_dir = tmp_path / "history"
    output_path = tmp_path / "github-output"
    bin_dir = tmp_path / "bin"
    history_dir.mkdir()
    bin_dir.mkdir()
    descriptions = [f"otel-long-running-{index}" for index in range(1, len(history_payloads) + 1)]
    state_path.write_text(
        json.dumps({"executions": [{"description_id": description_id} for description_id in descriptions]}),
        encoding="utf-8",
    )
    result_path.write_text(
        json.dumps(
            {
                "status": legacy_status,
                "state_changed": False,
            }
        ),
        encoding="utf-8",
    )
    for description_id, history in zip(descriptions, history_payloads, strict=True):
        (history_dir / f"{description_id}.json").write_text(
            json.dumps(history),
            encoding="utf-8",
        )
    fake_hatch = bin_dir / "hatch"
    fake_hatch.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_hatch.chmod(0o755)

    environment = {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "STATE_FILE": str(state_path),
        "RESULT_FILE": str(result_path),
        "HISTORY_DIR": str(history_dir),
        "REPORT_FILE": str(tmp_path / "report"),
        "GITHUB_OUTPUT": str(output_path),
    }
    subprocess.run(
        ["bash", "-c", check_script],
        check=True,
        cwd=ROOT,
        env=environment,
    )

    outputs = dict(line.split("=", 1) for line in output_path.read_text(encoding="utf-8").splitlines())
    assert outputs["status"] == legacy_status
    assert outputs["rollover_ready"] == expected_rollover_ready


def test_workflow_migrates_legacy_failure_before_retaining_pending_callback(
    tmp_path: Path,
) -> None:
    if shutil.which("bash") is None or shutil.which("jq") is None:
        pytest.skip("workflow compatibility test requires bash and jq")

    workflow = yaml.safe_load((WORKFLOWS_DIR / "opentelemetry-long-running.yml").read_text(encoding="utf-8"))
    check_script = next(
        step["run"]
        for step in workflow["jobs"]["run"]["steps"]
        if step["name"] == "Check long-running conformance executions"
    )
    state_path = tmp_path / "state.json"
    result_path = tmp_path / "result.json"
    history_dir = tmp_path / "history"
    output_path = tmp_path / "github-output"
    hatch_calls_path = tmp_path / "hatch-calls"
    bin_dir = tmp_path / "bin"
    current_checker = tmp_path / "current-checker"
    history_dir.mkdir()
    bin_dir.mkdir()
    current_checker.mkdir()
    state_path.write_text(
        json.dumps(
            {
                "executions": [
                    {
                        "description_id": "otel-long-running-1",
                    },
                    {
                        "description_id": CALLBACK_CASE,
                        "callback_sent_at_ms": None,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    result_path.write_text(
        json.dumps(
            {
                "status": "failed",
                "state_changed": False,
            }
        ),
        encoding="utf-8",
    )
    (history_dir / "otel-long-running-1.json").write_text(
        json.dumps(
            {
                "ExecutionStatus": "FAILED",
                "Events": [{"EventType": "ExecutionFailed"}],
            }
        ),
        encoding="utf-8",
    )
    (history_dir / f"{CALLBACK_CASE}.json").write_text(
        json.dumps(
            {
                "ExecutionStatus": "RUNNING",
                "Events": [
                    {
                        "EventType": "CallbackStarted",
                        "CallbackStartedDetails": {"CallbackId": "callback-1"},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    fake_hatch = bin_dir / "hatch"
    fake_hatch.write_text(
        """#!/bin/sh
printf '%s\n' "$PWD" >> "$HATCH_CALLS"
if [ "$PWD" != "$CURRENT_CHECKER" ]; then
  exit 1
fi
jq '.executions[1].callback_sent_at_ms = 11000' "$STATE_FILE" > "$STATE_FILE.tmp"
mv "$STATE_FILE.tmp" "$STATE_FILE"
printf '%s\n' '{"status":"failed","state_changed":true,"rollover_ready":false}' > "$RESULT_FILE"
exit 1
""",
        encoding="utf-8",
    )
    fake_hatch.chmod(0o755)

    environment = {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "CURRENT_CHECKER": str(current_checker),
        "STATE_FILE": str(state_path),
        "RESULT_FILE": str(result_path),
        "HISTORY_DIR": str(history_dir),
        "REPORT_FILE": str(tmp_path / "report"),
        "GITHUB_OUTPUT": str(output_path),
        "HATCH_CALLS": str(hatch_calls_path),
    }
    subprocess.run(
        ["bash", "-c", check_script],
        check=True,
        cwd=ROOT,
        env=environment,
    )

    outputs = dict(line.split("=", 1) for line in output_path.read_text(encoding="utf-8").splitlines())
    migrated_state = json.loads(state_path.read_text(encoding="utf-8"))
    assert outputs == {
        "status": "failed",
        "state_changed": "true",
        "rollover_ready": "false",
        "check_exit_code": "1",
    }
    assert migrated_state["executions"][1]["callback_sent_at_ms"] == 11000
    assert hatch_calls_path.read_text(encoding="utf-8").splitlines() == [
        str(ROOT),
        str(current_checker),
    ]
