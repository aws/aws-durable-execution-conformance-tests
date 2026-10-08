"""Bounded diagnostic canary against existing case17 functions; never deploys resources."""

from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from aws_durable_execution_conformance_tests.history import EventHistoryMatcher, load_yaml_file
from aws_durable_execution_conformance_tests.variables import PlaceholderContext
from aws_durable_execution_conformance_tests_otel.backends.collector import CollectorBackend
from aws_durable_execution_conformance_tests_otel.model import TelemetryQuery, trace_to_dict
from aws_durable_execution_conformance_tests_otel.polling import PollingPolicy
from aws_durable_execution_conformance_tests_otel.validators import validate_trace

REGION = "us-west-2"
TEST_ACCOUNT = "164176880947"
SERVICE = "durable-execution-conformance"
TERMINAL = {"SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED"}
SENSITIVE_VALUES: set[str] = set()
CURRENT_STAGE = "bootstrap"


def sanitized(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: "<redacted>"
            if key.lower() in {"callbackid", "checkpointtoken", "nextmarker", "nexttoken"}
            else sanitized(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [sanitized(item) for item in value]
    if isinstance(value, str):
        for secret in SENSITIVE_VALUES:
            value = value.replace(secret, "<redacted>")
    return value


def preflight(sts: Any, client: Any, expected_account: str, view: str) -> dict[str, Any]:
    if view not in {"invocation", "execution"} or expected_account != TEST_ACCOUNT:
        raise ValueError("Invalid fixed canary scope")
    if client.meta.endpoint_url != "https://lambda.us-west-2.amazonaws.com":
        raise ValueError("Probe requires the real regional Lambda endpoint")
    if sts.meta.endpoint_url not in {"https://sts.us-west-2.amazonaws.com", "https://sts.amazonaws.com"}:
        raise ValueError("Probe requires the real STS endpoint")
    identity = sts.get_caller_identity()
    if identity["Account"] != expected_account:
        raise ValueError("STS account does not match the configured conformance testing account")
    suffix = "inv" if view == "invocation" else "exec"
    name = f"conformance-tests-py-s3-{suffix}-otel-{view}-17"
    config = client.get_function_configuration(FunctionName=name)
    expected_arn = f"arn:aws:lambda:{REGION}:{expected_account}:function:{name}"
    if (
        config.get("FunctionArn") != expected_arn
        or config.get("FunctionName") != name
        or config.get("State") != "Active"
    ):
        raise ValueError("Existing test function ARN/state mismatch")
    if config.get("Runtime") != "python3.13" or config.get("Handler") != "otel_17_wait_for_callback_failure.handler":
        raise ValueError("Existing function is not the known Python case17 fixture")
    if config.get("DurableConfig", {}).get("ExecutionTimeout") != 300:
        raise ValueError("Existing test durable timeout differs from the known bounded fixture")
    if any(not isinstance(config.get(key), str) or not config[key] for key in ("CodeSha256", "RevisionId")):
        raise ValueError("Existing function lacks code/configuration revision evidence")
    expected_environment = {
        "OTEL_SERVICE_NAME": SERVICE,
        "OTEL_PLUGIN_MODE": view,
        "OTEL_S3_BUCKET": f"dex-otel-py-{suffix}-{expected_account}-{REGION}",
        "OTEL_S3_PREFIX": "traces",
    }
    environment = config.get("Environment", {}).get("Variables", {})
    if any(environment.get(key) != value for key, value in expected_environment.items()):
        raise ValueError("Existing function telemetry view/service/S3 target differs from successful CI")
    return {
        key: config.get(key)
        for key in (
            "FunctionName",
            "FunctionArn",
            "Runtime",
            "Handler",
            "State",
            "CodeSha256",
            "RevisionId",
            "LastModified",
            "DurableConfig",
        )
    }


def read_created_execution(client: Any, operation: str, request: dict[str, Any], deadline: float | None) -> Any:
    """Retry only visibility reads for the ARN returned and validated by Invoke."""
    if operation not in {"get_durable_execution_history", "get_durable_execution"}:
        raise ValueError("Only durable execution reads may use the visibility retry")
    while True:
        try:
            return getattr(client, operation)(**request)
        except ClientError as error:
            if (
                deadline is None
                or error.response.get("Error", {}).get("Code") != "ResourceNotFoundException"
                or time.monotonic() >= deadline
            ):
                raise
            time.sleep(min(2, max(0, deadline - time.monotonic())))


def history(client: Any, arn: str, deadline: float | None = None) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    marker = None
    for _ in range(10):
        request = {"DurableExecutionArn": arn, "IncludeExecutionData": True, "MaxItems": 100}
        if marker:
            request["Marker"] = marker
        page = read_created_execution(client, "get_durable_execution_history", request, deadline)
        events.extend(page.get("Events", []))
        marker = page.get("NextMarker")
        if not marker:
            return events
    raise RuntimeError("Probe history exceeded bounded pagination")


def execute_probe(view: str, expected_account: str, run_name: str, output_dir: Path) -> dict[str, Any]:
    global CURRENT_STAGE
    CURRENT_STAGE = "preflight"
    # Invoke and callback writes are never retried automatically. Only the
    # validated newly-created ARN's visibility reads have explicit bounded retries.
    config = Config(connect_timeout=10, read_timeout=25, retries={"mode": "standard", "total_max_attempts": 1})
    client = boto3.client("lambda", region_name=REGION, config=config)
    sts = boto3.client("sts", region_name=REGION, config=config)
    target = preflight(sts, client, expected_account, view)
    output_dir.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "purpose": "diagnostic; not a published conformance claim",
        "portability_limit": "This probe gates failure on InvocationCompleted. Candidate YAML Delay:1 does not guarantee that phase; portable coverage remains unproven.",
        "view": view,
        "region": REGION,
        "target": target,
        "execution_name": run_name,
        "deployment_provenance": "Existing repository CI case17 fixture; code hash/revision recorded, SDK source commit not inferred from deployment name",
        "qualifier_provenance": "Existing conformance sam.Invoker._invoke_boto3 explicitly uses Qualifier=$LATEST",
    }
    (output_dir / "preflight.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
    CURRENT_STAGE = "invoke"
    response = client.invoke(
        FunctionName=target["FunctionName"],
        Qualifier="$LATEST",
        InvocationType="Event",
        DurableExecutionName=run_name,
        Payload=json.dumps({"scenario": "wait-for-callback-failure"}).encode(),
    )
    arn = response.get("DurableExecutionArn")
    if not isinstance(arn, str) or not arn.startswith(target["FunctionArn"] + ":$LATEST/durable-execution/"):
        raise RuntimeError("Invoke did not return an execution in the exact test-function scope")
    report["execution_arn"] = arn
    (output_dir / "execution-created.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
    deadline = time.monotonic() + 180
    started_callback = None
    CURRENT_STAGE = "await_callback_after_invocation_completed"
    while time.monotonic() < deadline:
        events = history(client, arn, deadline)
        callbacks = [event for event in events if event.get("EventType") == "CallbackStarted"]
        if callbacks:
            if len(callbacks) != 1 or callbacks[0].get("Name") != "otel-failed-callback create callback id":
                raise RuntimeError("Unexpected callback target in the existing case17 handler")
            completed_invocations = [
                event
                for event in events
                if event.get("EventType") == "InvocationCompleted" and event["EventId"] > callbacks[0]["EventId"]
            ]
            if completed_invocations:
                # Exercise the suspended/resumed path, not a live callback-promise race.
                started_callback = callbacks[0]
                report["invocation_completed_before_failure"] = completed_invocations[0]["EventId"]
                break
        time.sleep(2)
    if started_callback is None:
        raise RuntimeError("Timed out waiting for the probe callback")
    # The request deliberately has exactly one key. Never log the capability ID.
    capability = started_callback["CallbackStartedDetails"]["CallbackId"]
    SENSITIVE_VALUES.add(capability)
    CURRENT_STAGE = "send_callback_failure"
    client.send_durable_execution_callback_failure(CallbackId=capability)
    report["failure_request_keys"] = ["CallbackId"]
    execution = {}
    CURRENT_STAGE = "await_terminal_execution"
    while time.monotonic() < deadline:
        execution = read_created_execution(
            client, "get_durable_execution", {"DurableExecutionArn": arn, "IncludeExecutionData": True}, deadline
        )
        if execution.get("Status") in TERMINAL:
            break
        time.sleep(2)
    if execution.get("Status") not in TERMINAL:
        raise RuntimeError("Probe execution did not reach a terminal outcome within the bound")
    CURRENT_STAGE = "read_terminal_history"
    events = history(client, arn, deadline)
    (output_dir / "history.json").write_text(json.dumps(sanitized({"Events": events}), indent=2, default=str) + "\n")
    failures = [
        event
        for event in events
        if event.get("EventType") == "CallbackFailed" and event.get("Id") == started_callback["Id"]
    ]
    details = failures[0].get("CallbackFailedDetails", {}) if len(failures) == 1 else None
    error_absent = isinstance(details, dict) and "Error" not in details
    error_wrapper = details.get("Error") if isinstance(details, dict) else None
    error_payload = error_wrapper.get("Payload") if isinstance(error_wrapper, dict) else None
    error_payload_empty = (
        isinstance(error_wrapper, dict)
        and isinstance(error_payload, dict)
        and not error_payload
        and error_wrapper.get("Truncated") is False
    )
    report.update(
        execution_status=execution["Status"],
        callback_failed_events=len(failures),
        service_error_absent=error_absent,
        service_error_payload_empty=error_payload_empty,
        callback_failure_details=sanitized(failures[0].get("CallbackFailedDetails")) if failures else None,
    )
    # Preserve service evidence even if telemetry retrieval/SDK behavior fails later.
    (output_dir / "service-result.json").write_text(json.dumps(sanitized(report), indent=2, default=str) + "\n")
    suffix = "inv" if view == "invocation" else "exec"
    CURRENT_STAGE = "read_telemetry"
    backend = CollectorBackend(
        boto3.client("s3", region_name=REGION, config=config),
        f"dex-otel-py-{suffix}-{expected_account}-{REGION}",
        "traces",
    )
    query = TelemetryQuery(arn, SERVICE, execution["StartTimestamp"], execution.get("EndTimestamp", datetime.now(UTC)))
    callback_operation_id = started_callback["Id"]
    trace = backend.find_trace(
        query,
        PollingPolicy(timeout_seconds=120, interval_seconds=3, max_attempts=40),
        accept=lambda value: any(
            span.attributes.get("durable.operation.id") == callback_operation_id
            and span.attributes.get("durable.operation.status") == "FAILED"
            for span in value.spans
        ),
    )
    (output_dir / "trace.json").write_text(json.dumps(sanitized(trace_to_dict(trace)), indent=2, default=str) + "\n")
    leaves = [
        span
        for span in trace.spans
        if span.attributes.get("durable.operation.id") == started_callback["Id"]
        and span.attributes.get("durable.operation.status") == "FAILED"
    ]
    report["terminal_callback_span_statuses"] = [span.status for span in leaves]
    candidate = load_yaml_file(str(Path(__file__).parent / "callback-probe-fixtures" / f"otel-{view}-25.yaml"))
    context = PlaceholderContext()
    context.bind("EXECUTION_ARN", arn)
    context.bind("SERVICE_NAME", SERVICE)
    result = EventHistoryMatcher(context).match(candidate["ExpectedExecutionHistory"], events)
    report["candidate_history_errors"] = result.errors
    report["candidate_telemetry_errors"] = validate_trace(
        trace, context.substitute(candidate["TelemetryAssertions"]), query
    )
    CURRENT_STAGE = "verify_configuration_stability"
    after = client.get_function_configuration(FunctionName=target["FunctionName"])
    report["code_unchanged"] = after["CodeSha256"] == target["CodeSha256"]
    report["configuration_unchanged"] = after["RevisionId"] == target["RevisionId"]
    report["feasible"] = bool(
        error_payload_empty
        and leaves
        and all(span.status == "UNSET" for span in leaves)
        and execution["Status"] == "FAILED"
        and report["code_unchanged"]
        and report["configuration_unchanged"]
        and not result.errors
        and not report["candidate_telemetry_errors"]
    )
    (output_dir / "result.json").write_text(json.dumps(sanitized(report), indent=2, default=str) + "\n")
    return report


def main() -> int:
    output = Path("probe-results")
    output.mkdir(exist_ok=True)
    try:
        if (
            os.environ.get("AWS_REGION") != REGION
            or os.environ.get("PROBE_REQUESTED_REGION") != REGION
            or os.environ.get("PROBE_PHASE") != "short"
            or os.environ.get("PROBE_SDK_REF")
        ):
            raise ValueError("Canary requires fixed region, short phase and existing deployed fixture")
        view = os.environ["PROBE_VIEW"]
        run_name = f"ci-noerr-{os.environ['GITHUB_RUN_ID']}-{os.environ['GITHUB_RUN_ATTEMPT']}-{view}"
        report = execute_probe(view, os.environ["EXPECTED_TEST_ACCOUNT"], run_name, output)
        print(
            json.dumps(
                {
                    key: report[key]
                    for key in (
                        "view",
                        "execution_status",
                        "service_error_absent",
                        "service_error_payload_empty",
                        "terminal_callback_span_statuses",
                        "feasible",
                    )
                }
            )
        )
        return 0 if report["feasible"] else 1
    except Exception as error:
        # Do not print service exception messages/tracebacks that might contain a callback capability.
        detail = {
            "error_type": type(error).__name__,
            "service_error_code": getattr(error, "response", {}).get("Error", {}).get("Code"),
            "operation_name": getattr(error, "operation_name", None),
            "stage": CURRENT_STAGE,
        }
        (output / "probe-error.json").write_text(json.dumps(detail, indent=2) + "\n")
        print(json.dumps(detail))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
