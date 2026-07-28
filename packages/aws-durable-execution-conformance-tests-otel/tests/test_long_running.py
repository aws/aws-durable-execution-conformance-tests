# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contracts for deferred long-running OpenTelemetry conformance runs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from aws_durable_execution_conformance_tests.validate import (
    _CfnSafeLoader,
    parse_function_descriptions,
)
from aws_durable_execution_conformance_tests_otel.long_running import (
    CALLBACK_CASE,
    MAX_DELAY_SECONDS,
    ExecutionState,
    RunState,
    _premature_executions,
    _requirement_cases,
    _resolved_input,
    _send_due_callback,
    _validate_delay,
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
        region="us-west-2",
        name="python-xray-long",
        stack_name="conformance-tests-python-xray-long",
        template="template.yaml",
        delay_seconds=86400,
        launched_at_ms=1000,
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
    assert json.loads(state_path.read_text(encoding="utf-8"))["version"] == 1


def test_requirement_input_uses_the_workflow_delay_override() -> None:
    requirement = yaml.safe_load(_requirement_cases()["otel-long-running-1"].read_text(encoding="utf-8"))

    resolved, bindings = _resolved_input(requirement, 86400)

    assert resolved == {"scenario": "long-wait", "delay_seconds": "86400"}
    assert bindings == {"LONG_DELAY_SECONDS": "86400"}


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
        region="us-west-2",
        name="test",
        stack_name="stack",
        template="template.yaml",
        delay_seconds=10,
        launched_at_ms=1000,
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


def test_non_callback_cases_cannot_finish_before_the_configured_delay() -> None:
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
        region="us-west-2",
        name="test",
        stack_name="stack",
        template="template.yaml",
        delay_seconds=10,
        launched_at_ms=1000,
        executions=executions,
    )
    statuses = {execution.description_id: "SUCCEEDED" for execution in executions}

    assert _premature_executions(state, statuses, 10999) == [
        executions[0],
        executions[1],
        executions[3],
    ]
    assert _premature_executions(state, statuses, 11000) == []


@pytest.mark.parametrize("language", ["java", "python", "typescript"])
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
    assert set(template["Resources"]) == {
        *(logical_id for logical_id, _description_id in EXPECTED_MAPPINGS),
        "OtelLongRunning4InvokeTarget",
    }
    assert template["Resources"]["OtelLongRunning4ChainedInvoke"]["Properties"]["Environment"]["Variables"][
        "OTEL_INVOKE_TARGET_FUNCTION_NAME"
    ] == {"Sub": "${OtelLongRunning4InvokeTarget.Arn}:$LATEST"}


def test_long_running_handlers_use_runtime_delay_inputs() -> None:
    python_source = EXAMPLES_DIR / "python" / "src"
    typescript_source = EXAMPLES_DIR / "typescript" / "handlers"
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

    assert "long_delay_seconds(event)" in (python_source / "otel_20_long_wait.py").read_text(encoding="utf-8")
    assert "long_delay_seconds(event)" in (python_source / "otel_23_long_chained_invoke.py").read_text(encoding="utf-8")
    assert "longDelaySeconds(event)" in (typescript_source / "otel_21_long_retry.ts").read_text(encoding="utf-8")
    assert "longDelaySeconds(event)" in (typescript_source / "otel_23_long_chained_invoke.ts").read_text(
        encoding="utf-8"
    )
    assert "longDelaySeconds(event)" in (java_source / "OtelLongRunning2Retry.java").read_text(encoding="utf-8")
    assert "longDelaySeconds(event)" in (java_source / "OtelLongRunning4InvokeTarget.java").read_text(encoding="utf-8")


@pytest.mark.parametrize("language", ["java", "python", "typescript"])
def test_language_workflows_launch_and_resume_xray_runs(language: str) -> None:
    workflow = (WORKFLOWS_DIR / f"{language}-opentelemetry-long-running.yml").read_text(encoding="utf-8")

    assert "  schedule:" in workflow
    assert "  workflow_dispatch:" in workflow
    assert 'cron: "0 7 * * *"' in workflow
    assert "github.event_name == 'schedule' && 'auto'" in workflow
    assert "phase=launch" in workflow
    assert "phase=check" in workflow
    assert 'default: "86400"' in workflow
    assert "inputs.delay_seconds || '86400'" in workflow
    assert "actions: write" in workflow
    assert "otel-long-running-state" in workflow
    assert "aws_durable_execution_conformance_tests_otel.long_running launch" in workflow
    assert "aws_durable_execution_conformance_tests_otel.long_running check" in workflow
    assert "aws-observability/aws-otel-" in workflow
    assert "--otel-layer-arn" in workflow
    assert "--otel-backend xray" in workflow
    assert "retention-days: 5" in workflow
    assert "actions/artifacts/$ARTIFACT_ID" in workflow
    assert "CHECK_EXIT_CODE" in workflow
    assert workflow.index("- name: Persist updated callback state") < workflow.index(
        "- name: Retire previous state artifact"
    )
