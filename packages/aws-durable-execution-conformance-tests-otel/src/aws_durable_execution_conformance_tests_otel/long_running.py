# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Launch and later validate day-scale OpenTelemetry conformance executions."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from aws_durable_execution_conformance_tests.callback import (
    CallbackAction,
    CallbackSender,
)
from aws_durable_execution_conformance_tests.clients import AwsClients
from aws_durable_execution_conformance_tests.config import STACK_NAME_PREFIX
from aws_durable_execution_conformance_tests.extensions import ValidationContext
from aws_durable_execution_conformance_tests.history import (
    EventHistoryMatcher,
    load_yaml_file,
)
from aws_durable_execution_conformance_tests.report import (
    Report,
    ReportEntry,
    ReportStatus,
    RunMetadata,
)
from aws_durable_execution_conformance_tests.reporters import (
    append_github_summary,
    render_console,
    write_report,
)
from aws_durable_execution_conformance_tests.sam import (
    Deployer,
    Invoker,
    delete_stack,
)
from aws_durable_execution_conformance_tests.validate import (
    _validate_execution_result,
    get_execution_history,
    get_execution_status,
    parse_function_descriptions,
    save_execution_history,
)
from aws_durable_execution_conformance_tests.variables import PlaceholderContext
from aws_durable_execution_conformance_tests_otel.exporters import (
    AdotExporterProfile,
    ExporterOptions,
    normalize_runtime,
)
from aws_durable_execution_conformance_tests_otel.extension import OtelExtension
from aws_durable_execution_conformance_tests_otel.model import parse_timestamp

SUITE = "otel-long-running"
STATE_VERSION = 3
MAX_DELAY_SECONDS = 86400
DEFAULT_CHECK_TIMEOUT = 900.0
DEFAULT_CHECK_INTERVAL = 15.0
CALLBACK_CASE = "otel-long-running-3"
TERMINAL_STATUSES = frozenset({"SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED"})
TERMINAL_EVENT_TYPES = frozenset(
    {
        "ExecutionSucceeded",
        "ExecutionFailed",
        "ExecutionTimedOut",
        "ExecutionCancelled",
    }
)
CALLBACK_OUTCOME_EVENT_TYPES = frozenset(
    {
        "CallbackSucceeded",
        "CallbackFailed",
        "CallbackTimedOut",
    }
)
SUPPORTED_VIEWS = {
    "java": frozenset({"execution", "invocation"}),
    "javascript": frozenset({"execution", "invocation"}),
    "python": frozenset({"execution", "invocation"}),
}


@dataclass
class ExecutionState:
    """Persisted metadata for one asynchronously invoked requirement."""

    description_id: str
    function_name: str
    execution_arn: str
    invocation_started_at_ms: int
    bindings: dict[str, Any]
    callback_sent_at_ms: int | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ExecutionState:
        return cls(
            description_id=str(value["description_id"]),
            function_name=str(value["function_name"]),
            execution_arn=str(value["execution_arn"]),
            invocation_started_at_ms=int(value["invocation_started_at_ms"]),
            bindings=dict(value.get("bindings", {})),
            callback_sent_at_ms=(
                int(value["callback_sent_at_ms"]) if value.get("callback_sent_at_ms") is not None else None
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "description_id": self.description_id,
            "function_name": self.function_name,
            "execution_arn": self.execution_arn,
            "invocation_started_at_ms": self.invocation_started_at_ms,
            "bindings": self.bindings,
            "callback_sent_at_ms": self.callback_sent_at_ms,
        }


@dataclass
class RunState:
    """Persisted state shared between the launch and scheduled check phases."""

    language: str
    view: str
    region: str
    name: str
    stack_name: str
    template: str
    delay_seconds: int
    launched_at_ms: int
    executions: list[ExecutionState]
    source_revision: str
    version: int = STATE_VERSION
    suite: str = SUITE

    @classmethod
    def load(cls, path: Path) -> RunState:
        value = json.loads(path.read_text(encoding="utf-8"))
        version = int(value["version"])
        if version != STATE_VERSION:
            raise ValueError(f"Unsupported long-running state version {version}; expected {STATE_VERSION}")
        state = cls(
            version=version,
            suite=str(value["suite"]),
            language=str(value["language"]),
            view=str(value["view"]),
            region=str(value["region"]),
            name=str(value["name"]),
            stack_name=str(value["stack_name"]),
            template=str(value["template"]),
            delay_seconds=int(value["delay_seconds"]),
            launched_at_ms=int(value["launched_at_ms"]),
            executions=[ExecutionState.from_dict(item) for item in value["executions"]],
            source_revision=str(value["source_revision"]),
        )
        if state.suite != SUITE:
            raise ValueError(f"State suite is {state.suite!r}; expected {SUITE!r}")
        if not state.source_revision:
            raise ValueError("Long-running state source revision cannot be empty")
        _validate_view(state.language, state.view)
        _validate_delay(state.delay_seconds)
        return state

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": self.version,
            "suite": self.suite,
            "language": self.language,
            "view": self.view,
            "region": self.region,
            "name": self.name,
            "stack_name": self.stack_name,
            "template": self.template,
            "delay_seconds": self.delay_seconds,
            "launched_at_ms": self.launched_at_ms,
            "executions": [execution.to_dict() for execution in self.executions],
            "source_revision": self.source_revision,
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _validate_delay(value: int) -> int:
    if value < 1 or value > MAX_DELAY_SECONDS:
        raise ValueError(f"delay seconds must be from 1 through {MAX_DELAY_SECONDS}")
    return value


def _validate_view(language: str, view: str) -> str:
    runtime = normalize_runtime(language)
    supported = SUPPORTED_VIEWS.get(runtime)
    if supported is None:
        raise ValueError(f"Long-running OTel does not support runtime {language!r}")
    if view not in supported:
        choices = ", ".join(sorted(supported))
        raise ValueError(f"Long-running OTel runtime {runtime!r} supports view(s): {choices}; received {view!r}")
    return view


def _requirement_cases() -> dict[str, Path]:
    suites = {suite.name: suite for suite in OtelExtension().requirement_suites()}
    root = suites[SUITE].root
    return {path.stem: path for path in sorted(root.glob("*.yaml"))}


def _requirement_for_view(requirement: dict[str, Any], view: str) -> dict[str, Any]:
    assertion_key = "TelemetryAssertions" if view == "invocation" else "ExecutionTelemetryAssertions"
    assertions = requirement.get(assertion_key)
    if not isinstance(assertions, Mapping):
        raise ValueError(f"Long-running requirement is missing {assertion_key}")
    selected = dict(requirement)
    selected["TelemetryAssertions"] = assertions
    return selected


def _otel_options(
    language: str,
    view: str,
    region: str,
    backend: str = "xray",
) -> dict[str, Any]:
    return {
        "language": language,
        "region": region,
        "suite": [SUITE],
        "otel_backend": backend,
        "otel_exporter": "adot",
        "otel_service_name": _query_service_name(language, view),
        "otel_poll_timeout": 120.0,
        "otel_poll_interval": 2.0,
        "otel_poll_attempts": 60,
    }


def _query_service_name(language: str, view: str) -> str:
    if normalize_runtime(language) == "java" and view == "execution":
        return "workflow"
    return "invocation"


def _resolved_input(
    requirement: dict[str, Any],
    delay_seconds: int,
) -> tuple[Any, dict[str, Any]]:
    variables = dict(requirement.get("Variables", {}))
    variables["LONG_DELAY_SECONDS"] = delay_seconds
    context = PlaceholderContext()
    context.resolve_variables(variables)
    return context.substitute(requirement.get("Input")), context.bindings


def launch(args: argparse.Namespace) -> int:
    """Build, deploy, and asynchronously start every long-running case."""

    runtime = normalize_runtime(args.language)
    view = _validate_view(runtime, args.view)
    delay_seconds = _validate_delay(args.delay_seconds)
    state_path = Path(args.state_file)
    if state_path.exists():
        raise ValueError(f"State file already exists: {state_path}")

    template_path = Path(args.template)
    requirements = _requirement_cases()
    mappings = [
        (function_name, description_id)
        for function_name, description_id in parse_function_descriptions(str(template_path))
        if description_id in requirements
    ]
    mapping_counts = Counter(description_id for _, description_id in mappings)
    if mapping_counts != Counter(requirements.keys()):
        raise ValueError(f"{template_path} must map every {SUITE} requirement exactly once")

    stack_name = f"{STACK_NAME_PREFIX}-{args.name}"
    build_dir = Path(args.build_dir) / stack_name
    profile = AdotExporterProfile()
    exporter = profile.configure(
        ExporterOptions(
            runtime=runtime,
            region=args.region,
            endpoint=None,
            service_name="invocation",
            layer_arn=args.otel_layer_arn,
        )
    )
    parameters = {
        **exporter.parameter_overrides,
        "LambdaExecutionRoleArn": args.lambda_execution_role_arn,
    }
    if len(SUPPORTED_VIEWS[runtime]) > 1:
        parameters["OtelView"] = view

    deployer = Deployer(
        template_path=str(template_path),
        build_dir=str(build_dir),
        region=args.region,
    )
    print(f"=== Deploying long-running stack {stack_name!r} ===")
    deployer.build()
    deployer.deploy(
        stack_name=stack_name,
        parameter_overrides=parameters,
    )

    clients = AwsClients.create(args.region)
    invoker = Invoker(
        stack_name=stack_name,
        region=args.region,
        lambda_client=clients["lambda"],
        cfn_client=clients["cloudformation"],
    )
    executions: list[ExecutionState] = []
    launched_at_ms = int(time.time() * 1000)
    with tempfile.TemporaryDirectory(prefix="otel-long-running-events-") as temp_dir:
        for function_name, description_id in mappings:
            requirement = load_yaml_file(str(requirements[description_id]))
            resolved_input, bindings = _resolved_input(
                requirement,
                delay_seconds,
            )
            event_path = Path(temp_dir) / f"{description_id}.json"
            event_path.write_text(
                json.dumps({"Input": resolved_input}),
                encoding="utf-8",
            )
            invocation_started_at_ms = int(time.time() * 1000)
            result = invoker.invoke_async(
                function_name=function_name,
                event_file_path=str(event_path),
            )
            response = json.loads(result.output)
            execution_arn = response.get("DurableExecutionArn")
            if not execution_arn:
                raise RuntimeError(f"{description_id} invocation returned no DurableExecutionArn")
            executions.append(
                ExecutionState(
                    description_id=description_id,
                    function_name=function_name,
                    execution_arn=str(execution_arn),
                    invocation_started_at_ms=invocation_started_at_ms,
                    bindings=bindings,
                )
            )
            print(f"  Launched {description_id}: {execution_arn}")

    RunState(
        language=runtime,
        view=view,
        region=args.region,
        name=args.name,
        stack_name=stack_name,
        template=str(template_path),
        delay_seconds=delay_seconds,
        launched_at_ms=launched_at_ms,
        executions=executions,
        source_revision=str(args.source_revision),
    ).save(state_path)
    print(f"Saved {len(executions)} deferred execution(s) to {state_path}")
    return 0


def _callback_id(history: dict[str, Any]) -> str | None:
    for event in history.get("Events", history.get("events", [])):
        if event.get("EventType") != "CallbackStarted":
            continue
        details = event.get("CallbackStartedDetails", {})
        callback_id = details.get("CallbackId")
        if callback_id:
            return str(callback_id)
    return None


def _latest_event(
    history: dict[str, Any],
    event_types: frozenset[str],
) -> dict[str, Any] | None:
    events = history.get("Events", history.get("events", []))
    return next(
        (event for event in reversed(events) if event.get("EventType") in event_types),
        None,
    )


def _event_timestamp_ms(event: Mapping[str, Any] | None) -> int | None:
    if event is None or event.get("EventTimestamp") is None:
        return None
    return int(parse_timestamp(event["EventTimestamp"]).timestamp() * 1000)


def _send_due_callback(
    state: RunState,
    execution: ExecutionState,
    history: dict[str, Any],
    lambda_client: Any,
    now_ms: int,
) -> bool:
    if execution.description_id != CALLBACK_CASE:
        return False
    if execution.callback_sent_at_ms is not None:
        return False

    completion_event = _latest_event(history, CALLBACK_OUTCOME_EVENT_TYPES)
    if completion_event is not None:
        execution.callback_sent_at_ms = _event_timestamp_ms(completion_event) or now_ms
        print(f"Recovered completed delayed callback for {execution.description_id}")
        return True
    if get_execution_status(history) in TERMINAL_STATUSES:
        return False
    if now_ms - execution.invocation_started_at_ms < state.delay_seconds * 1000:
        return False

    callback_id = _callback_id(history)
    if callback_id is None:
        return False
    CallbackSender(lambda_client).send(
        callback_id,
        CallbackAction(
            callback_name="otel-long-callback",
            operation="success",
            payload="callback-complete",
        ),
    )
    execution.callback_sent_at_ms = now_ms
    print(f"Sent delayed success callback for {execution.description_id}")
    return True


def _premature_executions(
    state: RunState,
    statuses: Mapping[str, str | None],
    histories: Mapping[str, dict[str, Any]],
) -> list[ExecutionState]:
    premature: list[ExecutionState] = []
    for execution in state.executions:
        if statuses[execution.description_id] not in TERMINAL_STATUSES:
            continue
        terminal_at_ms = _event_timestamp_ms(
            _latest_event(
                histories[execution.description_id],
                TERMINAL_EVENT_TYPES,
            )
        )
        due_at_ms = execution.invocation_started_at_ms + state.delay_seconds * 1000
        if terminal_at_ms is None or terminal_at_ms < due_at_ms:
            premature.append(execution)
    return premature


def _write_check_result(
    path: Path,
    *,
    status: str,
    state_changed: bool,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "status": status,
                "state_changed": state_changed,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def _refresh_execution_history(
    execution: ExecutionState,
    lambda_client: Any,
    histories: dict[str, dict[str, Any]],
    statuses: dict[str, str | None],
    output_dir: Path,
) -> None:
    history = get_execution_history(
        execution.execution_arn,
        lambda_client,
    )
    if history is None:
        raise RuntimeError(f"Could not retrieve history for {execution.description_id}")
    histories[execution.description_id] = history
    statuses[execution.description_id] = get_execution_status(history)
    save_execution_history(
        execution.description_id,
        history,
        output_dir=output_dir,
    )


def _poll_pending_executions(
    state: RunState,
    lambda_client: Any,
    histories: dict[str, dict[str, Any]],
    statuses: dict[str, str | None],
    output_dir: Path,
    *,
    timeout: float,
    interval: float,
) -> None:
    pending = [
        execution for execution in state.executions if statuses[execution.description_id] not in TERMINAL_STATUSES
    ]
    deadline = time.monotonic() + timeout
    while pending:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        print(
            "Waiting for callback-triggered executions to finish: "
            + ", ".join(execution.description_id for execution in pending)
        )
        time.sleep(min(interval, remaining))
        for execution in pending:
            _refresh_execution_history(
                execution,
                lambda_client,
                histories,
                statuses,
                output_dir,
            )
        pending = [execution for execution in pending if statuses[execution.description_id] not in TERMINAL_STATUSES]


def _fail_premature_executions(
    state: RunState,
    executions: list[ExecutionState],
    requirements: Mapping[str, Path],
    result_path: Path,
    report_file: str,
    now_ms: int,
    *,
    delete_terminal_stack: bool,
) -> int:
    report = _new_report(state, now_ms)
    for execution in executions:
        report.add(
            ReportEntry(
                id=execution.description_id,
                suite=SUITE,
                status=ReportStatus.FAILED,
                function=execution.function_name,
                description=load_yaml_file(str(requirements[execution.description_id])).get("description"),
                errors=[
                    "Execution reached a terminal state before the "
                    f"configured {state.delay_seconds}-second delay elapsed"
                ],
            )
        )
    _emit_report(report, report_file)
    _write_check_result(
        result_path,
        status="failed",
        state_changed=False,
    )
    if delete_terminal_stack:
        delete_stack(state.stack_name, state.region)
    return 1


def _validate_terminal_execution(
    *,
    state: RunState,
    execution: ExecutionState,
    history: dict[str, Any],
    requirement_path: Path,
    clients: AwsClients,
    output_dir: Path,
    finished_at_ms: int,
    backend: str,
) -> ReportEntry:
    requirement = _requirement_for_view(
        load_yaml_file(str(requirement_path)),
        state.view,
    )
    context = PlaceholderContext()
    for name, value in execution.bindings.items():
        context.bind(name, value)

    expected_events = requirement.get("ExpectedExecutionHistory", [])
    actual_events = history.get("Events", history.get("events", []))
    match_result = EventHistoryMatcher(context=context).match(
        expected_events,
        actual_events,
    )
    errors = list(match_result.errors)
    if not errors:
        errors.extend(
            _validate_execution_result(
                execution_arn=execution.execution_arn,
                expected_result=requirement.get("ExpectedResult", {}),
                lambda_client=clients["lambda"],
                context=context,
            )
        )

    if not errors:
        validation_context = ValidationContext(
            description_id=execution.description_id,
            function_name=execution.function_name,
            execution_arn=execution.execution_arn,
            invocation_started_at_ms=execution.invocation_started_at_ms,
            invocation_finished_at_ms=finished_at_ms,
            region=state.region,
            language=state.language,
            requirement=requirement,
            execution_history=history,
            output_dir=output_dir,
            placeholders={
                **match_result.resolved_placeholders,
                "EXECUTION_ARN": execution.execution_arn,
            },
            options=_otel_options(state.language, state.view, state.region, backend),
            aws_clients=clients,
        )
        errors.extend(OtelExtension().validate_telemetry(validation_context))

    return ReportEntry(
        id=execution.description_id,
        suite=SUITE,
        status=ReportStatus.FAILED if errors else ReportStatus.PASSED,
        function=execution.function_name,
        description=requirement.get("description"),
        errors=errors,
    )


def check(
    args: argparse.Namespace,
    *,
    delete_terminal_stack: bool = True,
) -> int:
    """Check deferred executions and validate terminal histories and traces."""

    state_path = Path(args.state_file)
    result_path = Path(args.result_file)
    state = RunState.load(state_path)
    requirements = _requirement_cases()
    clients = AwsClients.create(state.region, ("xray",))
    output_dir = Path(args.history_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    now_ms = int(time.time() * 1000)
    state_changed = False
    histories: dict[str, dict[str, Any]] = {}
    statuses: dict[str, str | None] = {}
    for execution in state.executions:
        _refresh_execution_history(
            execution,
            clients["lambda"],
            histories,
            statuses,
            output_dir,
        )

    premature = _premature_executions(state, statuses, histories)
    if premature:
        return _fail_premature_executions(
            state,
            premature,
            requirements,
            result_path,
            args.report_file,
            now_ms,
            delete_terminal_stack=delete_terminal_stack,
        )

    callback_progressed = False
    for execution in state.executions:
        callback_was_pending = execution.callback_sent_at_ms is None
        execution_changed = _send_due_callback(
            state,
            execution,
            histories[execution.description_id],
            clients["lambda"],
            now_ms,
        )
        state_changed |= execution_changed
        callback_progressed |= (
            execution_changed
            and callback_was_pending
            and execution.callback_sent_at_ms is not None
            and statuses[execution.description_id] not in TERMINAL_STATUSES
        )
    if state_changed:
        state.save(state_path)

    if callback_progressed:
        _poll_pending_executions(
            state,
            clients["lambda"],
            histories,
            statuses,
            output_dir,
            timeout=args.check_timeout,
            interval=args.check_interval,
        )
        premature = _premature_executions(state, statuses, histories)
        if premature:
            return _fail_premature_executions(
                state,
                premature,
                requirements,
                result_path,
                args.report_file,
                now_ms,
                delete_terminal_stack=delete_terminal_stack,
            )

    pending = {description_id: status for description_id, status in statuses.items() if status not in TERMINAL_STATUSES}
    if pending:
        print(
            "Long-running executions are still pending: "
            + ", ".join(f"{description_id}={status or 'UNKNOWN'}" for description_id, status in sorted(pending.items()))
        )
        _write_check_result(
            result_path,
            status="pending",
            state_changed=state_changed,
        )
        return 0

    report = _new_report(state, now_ms)
    for execution in state.executions:
        report.add(
            _validate_terminal_execution(
                state=state,
                execution=execution,
                history=histories[execution.description_id],
                requirement_path=requirements[execution.description_id],
                clients=clients,
                output_dir=output_dir,
                finished_at_ms=now_ms,
                backend=args.otel_backend,
            )
        )

    _emit_report(report, args.report_file)

    status = "passed" if report.exit_code() == 0 else "failed"
    _write_check_result(
        result_path,
        status=status,
        state_changed=state_changed,
    )
    if delete_terminal_stack:
        delete_stack(state.stack_name, state.region)
    return report.exit_code()


def run_to_completion(args: argparse.Namespace) -> int:
    """Launch a short run and poll it through callback completion and validation."""

    state_path = Path(args.state_file)
    result_path = Path(args.result_file)
    launch(args)
    state = RunState.load(state_path)
    due_at = (state.launched_at_ms / 1000) + state.delay_seconds
    remaining = max(0.0, due_at - time.time())
    if remaining:
        print(f"Waiting {remaining:.1f} seconds before checking long-running executions")
        time.sleep(remaining)

    deadline = time.monotonic() + args.check_timeout
    while True:
        result_path.unlink(missing_ok=True)
        exit_code = check(args, delete_terminal_stack=False)
        if not result_path.is_file():
            raise RuntimeError("Long-running check did not write a result")
        result = json.loads(result_path.read_text(encoding="utf-8"))
        status = str(result.get("status", "error"))
        if status == "passed":
            return exit_code
        if status == "failed":
            return exit_code or 1
        if status == "error":
            return exit_code or 1
        if status != "pending":
            raise RuntimeError(f"Long-running check returned unknown status {status!r}")
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Long-running executions remained pending after {args.check_timeout} seconds")
        time.sleep(args.check_interval)


def _new_report(state: RunState, now_ms: int) -> Report:
    return Report(
        run=RunMetadata(
            name=state.name,
            template=state.template,
            region=state.region,
            language=state.language,
            suites=[SUITE],
            started_at=datetime.fromtimestamp(
                state.launched_at_ms / 1000,
                tz=UTC,
            ).isoformat(),
            finished_at=datetime.now(UTC).isoformat(),
            duration_seconds=(now_ms - state.launched_at_ms) / 1000,
        )
    )


def _emit_report(report: Report, report_file: str) -> None:
    print(render_console(report))
    write_report(report, "json", report_file)
    write_report(report, "junit", report_file)
    if summary_path := os.environ.get("GITHUB_STEP_SUMMARY"):
        append_github_summary(report, summary_path)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=("Launch or check the deferred one-day OpenTelemetry conformance suite.")
    )
    subparsers = parser.add_subparsers(dest="phase", required=True)

    launch_parser = subparsers.add_parser("launch")
    launch_parser.add_argument("--template", required=True)
    launch_parser.add_argument("--language", required=True)
    launch_parser.add_argument(
        "--view",
        choices=["execution", "invocation"],
        default="invocation",
    )
    launch_parser.add_argument("--region", default="us-west-2")
    launch_parser.add_argument("--name", required=True)
    launch_parser.add_argument("--state-file", required=True)
    launch_parser.add_argument(
        "--source-revision",
        default=os.environ.get("GITHUB_SHA", "local"),
    )
    launch_parser.add_argument("--build-dir", default=".build/long-running")
    launch_parser.add_argument(
        "--delay-seconds",
        type=int,
        default=MAX_DELAY_SECONDS,
    )
    launch_parser.add_argument("--lambda-execution-role-arn", required=True)
    launch_parser.add_argument("--otel-layer-arn", required=True)

    check_parser = subparsers.add_parser("check")
    check_parser.add_argument("--state-file", required=True)
    check_parser.add_argument("--result-file", required=True)
    check_parser.add_argument("--history-dir", required=True)
    check_parser.add_argument("--report-file", required=True)
    check_parser.add_argument(
        "--otel-backend",
        choices=["xray"],
        default="xray",
    )
    check_parser.add_argument(
        "--cleanup",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Delete the stack after terminal validation (pass --no-cleanup to retain it).",
    )
    check_parser.add_argument(
        "--check-timeout",
        type=float,
        default=DEFAULT_CHECK_TIMEOUT,
    )
    check_parser.add_argument(
        "--check-interval",
        type=float,
        default=DEFAULT_CHECK_INTERVAL,
    )

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--template", required=True)
    run_parser.add_argument("--language", required=True)
    run_parser.add_argument(
        "--view",
        choices=["execution", "invocation"],
        default="invocation",
    )
    run_parser.add_argument("--region", default="us-west-2")
    run_parser.add_argument("--name", required=True)
    run_parser.add_argument("--state-file", required=True)
    run_parser.add_argument(
        "--source-revision",
        default=os.environ.get("GITHUB_SHA", "local"),
    )
    run_parser.add_argument("--build-dir", default=".build/long-running")
    run_parser.add_argument(
        "--delay-seconds",
        type=int,
        default=600,
    )
    run_parser.add_argument("--lambda-execution-role-arn", required=True)
    run_parser.add_argument("--otel-layer-arn", required=True)
    run_parser.add_argument("--result-file", required=True)
    run_parser.add_argument("--history-dir", required=True)
    run_parser.add_argument("--report-file", required=True)
    run_parser.add_argument(
        "--otel-backend",
        choices=["xray"],
        default="xray",
    )
    run_parser.add_argument(
        "--check-timeout",
        type=float,
        default=DEFAULT_CHECK_TIMEOUT,
    )
    run_parser.add_argument(
        "--check-interval",
        type=float,
        default=DEFAULT_CHECK_INTERVAL,
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.phase == "launch":
            return launch(args)
        if args.check_timeout <= 0 or args.check_interval <= 0:
            raise ValueError("check timeout and interval must be positive")
        if args.phase == "check":
            return check(args, delete_terminal_stack=args.cleanup)
        return run_to_completion(args)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"Long-running OTel {args.phase} failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
