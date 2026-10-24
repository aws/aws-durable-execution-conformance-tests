# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contracts for deferred long-running OpenTelemetry conformance runs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

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

EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples"
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
