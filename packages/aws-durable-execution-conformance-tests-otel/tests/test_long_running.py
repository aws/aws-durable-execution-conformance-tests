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

    class _Profile:
        def configure(self, _options: Any) -> _Exporter:
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

    assert "long_delay_seconds(event)" in (python_source / "otel_long_running_1_wait.py").read_text(encoding="utf-8")
    python_retry_source = (python_source / "otel_long_running_2_retry.py").read_text(encoding="utf-8")
    assert "long_delay_seconds(event)" in python_retry_source
    assert "jitter_strategy=JitterStrategy.NONE" in python_retry_source
    assert "long_delay_seconds(event)" in (python_source / "otel_long_running_4_chained_invoke.py").read_text(
        encoding="utf-8"
    )
    assert "longDelaySeconds(event)" in (typescript_source / "otel_21_long_retry.ts").read_text(encoding="utf-8")
    assert "longDelaySeconds(event)" in (typescript_source / "otel_23_long_chained_invoke.ts").read_text(
        encoding="utf-8"
    )
    assert "longDelaySeconds(event)" in (java_source / "OtelLongRunning2Retry.java").read_text(encoding="utf-8")
    assert "longDelaySeconds(event)" in (java_source / "OtelLongRunning4InvokeTarget.java").read_text(encoding="utf-8")


@pytest.mark.parametrize("language", ["java", "python", "typescript"])
def test_language_workflows_run_short_and_deferred_xray_runs(language: str) -> None:
    entry_workflow = (WORKFLOWS_DIR / f"{language}-opentelemetry.yml").read_text(encoding="utf-8")
    entry_workflow_config = yaml.safe_load(entry_workflow)
    workflow = (WORKFLOWS_DIR / f"{language}-opentelemetry-long-running.yml").read_text(encoding="utf-8")
    workflow_config = yaml.safe_load(workflow)
    display_name = {
        "java": "Java",
        "python": "Python",
        "typescript": "TypeScript",
    }[language]

    assert "  pull_request:" in entry_workflow
    assert "  push:" in entry_workflow
    assert entry_workflow.count("    branches: [main]") == 2
    assert "  schedule:" in entry_workflow
    assert "  workflow_dispatch:" in entry_workflow
    assert 'cron: "0 7 * * *"' in entry_workflow
    assert "github.event_name == 'pull_request' || github.event_name == 'push'" in entry_workflow
    assert "github.event_name == 'schedule' && 'auto'" in entry_workflow
    assert "&& 'short'" in entry_workflow
    assert "&& '60'" in entry_workflow
    assert 'default: "82800"' in entry_workflow
    assert f"uses: ./.github/workflows/{language}-opentelemetry-long-running.yml" in entry_workflow
    for view in ("invocation", "execution"):
        delay_expression = entry_workflow_config["jobs"][f"long-running-{view}"]["with"]["delay_seconds"]
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
    assert (
        f"""if [ -z "$artifact_id" ]; then
            echo "No active {display_name} $OTEL_VIEW long-running OTel run."
            exit 1
          fi"""
        in workflow
    )
    assert 'echo "active=false"' not in workflow
    assert "inputs.delay_seconds || '82800'" in workflow
    source_revision = "$CONFORMANCE_REF" if language == "python" else "$GITHUB_SHA"
    assert workflow.count(f'--source-revision "{source_revision}"') == 2
    assert "source_revision=$(jq -r '.source_revision // empty' \"$STATE_FILE\")" in workflow
    checkout_fallback = "inputs.conformance_test_sha" if language == "python" else "github.sha"
    assert f"ref: ${{{{ steps.state.outputs.source_revision || {checkout_fallback} }}}}" in workflow
    assert "actions: write" in workflow
    assert "otel-long-running-state" in workflow
    assert "aws_durable_execution_conformance_tests_otel.long_running launch" in workflow
    assert "aws_durable_execution_conformance_tests_otel.long_running check" in workflow
    assert "aws_durable_execution_conformance_tests_otel.long_running run" in workflow
    assert "--check-timeout 900" in workflow
    assert "--check-interval 15" in workflow
    assert "aws-observability/aws-otel-" in workflow
    assert '--view "$OTEL_VIEW"' in workflow
    assert "--otel-layer-arn" in workflow
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
        f"{language}-otel-long-running-${{{{ inputs.view }}}}-${{{{ inputs.aws_region || 'us-west-2' }}}}-state"
    )
    assert workflow_config["jobs"]["run"]["env"]["TEST_NAME"] == (
        f"{language[0]}-olr-${{{{ inputs.view == 'invocation' && 'i' || 'e' }}}}"
        "${{ inputs.phase == 'short' && '-short' || '' }}"
    )
    assert "github.run_number" not in workflow
    assert "github.run_attempt" not in workflow
