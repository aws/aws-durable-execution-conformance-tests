# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Validation logic for durable execution test descriptions.

Handles both synchronous and asynchronous invocation validation, including
execution history retrieval, event matching, and callback handling.
"""
# ruff: noqa: T201

from __future__ import annotations

import contextlib
import json
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import boto3
import yaml
from botocore.exceptions import BotoCoreError, ClientError

from aws_durable_execution_sdk_testing.callback import (
    CallbackAction,
    CallbackError,
    CallbackSender,
)
from aws_durable_execution_sdk_testing.cloudwatch import (
    CloudWatchLogRetriever,
    CloudWatchLogValidator,
)
from aws_durable_execution_sdk_testing.config import (
    OUTPUT_DIR,
    POLL_INTERVAL_SECONDS,
    POLL_NO_PROGRESS_TIMEOUT_SECONDS,
)
from aws_durable_execution_sdk_testing.history import (
    EventHistoryMatcher,
    load_yaml_file,
)
from aws_durable_execution_sdk_testing.sam import (
    EventFileError,
    Invoker,
    SamCliError,
)
from aws_durable_execution_sdk_testing.variables import PlaceholderContext

# region Constants

_TERMINAL_STATUSES: frozenset[str] = frozenset(
    {
        "SUCCEEDED",
        "FAILED",
        "TIMED_OUT",
        "CANCELLED",
    }
)

_CALLBACK_CREATED_EVENT_TYPE: str = "CallbackStarted"


# endregion


# region Models


@dataclass(frozen=True)
class DescriptionResult:
    """Result of validating a single test description."""

    description_id: str
    function_name: str
    passed: bool
    optional: bool = False
    errors: list[str] = field(default_factory=list)
    placeholders: dict = field(default_factory=dict)


@dataclass(frozen=True)
class AsyncValidationConfig:
    """Configuration for async validation polling.

    Attributes:
        poll_interval_seconds: Seconds between each poll.
        no_progress_timeout_seconds: Seconds to wait with no new events
            before timing out.
    """

    poll_interval_seconds: float = POLL_INTERVAL_SECONDS
    no_progress_timeout_seconds: float = POLL_NO_PROGRESS_TIMEOUT_SECONDS

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AsyncValidationConfig:
        """Create config from a dictionary (e.g. from YAML).

        Args:
            data: Dictionary with optional polling configuration keys.

        Returns:
            A new AsyncValidationConfig instance.
        """
        return cls(
            poll_interval_seconds=float(data.get("PollIntervalSeconds", POLL_INTERVAL_SECONDS)),
            no_progress_timeout_seconds=float(
                data.get(
                    "NoProgressTimeoutSeconds",
                    POLL_NO_PROGRESS_TIMEOUT_SECONDS,
                )
            ),
        )


@dataclass(frozen=True)
class AsyncValidationResult:
    """Result of an async validation run.

    Attributes:
        passed: Whether all assertions passed.
        errors: List of error messages if validation failed.
        placeholders: Resolved placeholder values from event matching.
        callbacks_sent: Number of callbacks successfully sent.
        final_status: The execution status when polling stopped.
        event_count: Total number of events observed.
    """

    passed: bool
    errors: list[str] = field(default_factory=list)
    placeholders: dict[str, Any] = field(default_factory=dict)
    callbacks_sent: int = 0
    final_status: str | None = None
    event_count: int = 0


# endregion


# region Template parsing


def _cfn_tag_constructor(loader: yaml.SafeLoader, tag_suffix: str, node: yaml.Node) -> dict:
    """Handle CloudFormation intrinsic function tags like !GetAtt, !Ref, !Sub, etc."""
    match node:
        case yaml.ScalarNode():
            return {tag_suffix: loader.construct_scalar(node)}
        case yaml.SequenceNode():
            return {tag_suffix: loader.construct_sequence(node)}
        case yaml.MappingNode():
            return {tag_suffix: loader.construct_mapping(node)}
        case _:
            return {tag_suffix: None}


class _CfnSafeLoader(yaml.SafeLoader):
    """Custom YAML loader that handles CloudFormation intrinsic function tags."""


_CfnSafeLoader.add_multi_constructor("!", _cfn_tag_constructor)


def parse_function_descriptions(template_path: str) -> list[tuple[str, str]]:
    """Parse template.yaml and return (function_name, description_id) pairs.

    Each AWS::Serverless::Function with a TestingMetadata.TestDescription field
    yields one tuple per description ID in the list.

    We use TestingMetadata (a custom top-level key on the resource) instead of
    Metadata to avoid conflicts with SAM CLI, which reserves Metadata for its
    own purposes (BuildMethod, BuildProperties, DockerTag, etc.).
    """
    with open(template_path) as f:
        loader: _CfnSafeLoader = _CfnSafeLoader(f)
        try:
            template = loader.get_single_data()
        finally:
            loader.dispose()

    results: list[tuple[str, str]] = []
    for name, resource in template.get("Resources", {}).items():
        if resource.get("Type") != "AWS::Serverless::Function":
            continue
        testing_metadata: dict = resource.get("TestingMetadata", {})
        description_ids: list = testing_metadata.get("TestDescription", [])
        results.extend((name, did) for did in description_ids)
    return results


def parse_not_implemented(template_path: str) -> dict[str, str]:
    """Parse declared intentional SDK gaps from a SAM template.

    A ``NotImplemented`` block declares requirement IDs this SDK's template
    intentionally does not satisfy, each with a human-readable reason. It may
    appear as a top-level ``TestingMetadata`` block (sibling of ``Resources``)
    and/or under any function resource's ``TestingMetadata`` -- a not-implemented
    ID has no function to attach to, so the top-level form is expected, but both
    are supported. Each entry is a mapping ``{ id: <requirement id>, reason: <str> }``::

        TestingMetadata:
          NotImplemented:
            - id: "8-13"
              reason: "toleratedFailurePercentage rejected at build() in this SDK"

    Args:
        template_path: Path to the SAM template file.

    Returns:
        Mapping of requirement ID -> reason. When an ID is declared more than
        once, the first reason wins. Empty when nothing is declared.
    """
    with open(template_path, encoding="utf-8") as f:
        loader: _CfnSafeLoader = _CfnSafeLoader(f)
        try:
            template = loader.get_single_data()
        finally:
            loader.dispose()

    result: dict[str, str] = {}
    if not isinstance(template, dict):
        return result

    def _ingest(metadata: Any) -> None:
        if not isinstance(metadata, dict):
            return
        for entry in metadata.get("NotImplemented", []) or []:
            if not isinstance(entry, dict):
                continue
            description_id = entry.get("id")
            if not description_id or description_id in result:
                continue
            result[description_id] = str(entry.get("reason") or "")

    # Top-level TestingMetadata block.
    _ingest(template.get("TestingMetadata", {}))

    # Per-resource TestingMetadata blocks.
    for resource in template.get("Resources", {}).values():
        if isinstance(resource, dict):
            _ingest(resource.get("TestingMetadata", {}))

    return result


def discover_test_files(
    tests_dir: str | Path,
    suite: str | list[str] | None = None,
) -> dict[str, str]:
    """Scan the tests directory recursively and return a map of description_id -> file path.

    Args:
        tests_dir: Root directory containing test requirement YAML files.
        suite: If a string or list of strings, only scan the subdirectories
            matching those suite names (e.g. "step", "serdes"). If None,
            "all", or a list containing "all", scan all subdirectories.

    Returns:
        Mapping of description_id (file stem) to absolute file path.
    """
    tests_path: Path = Path(tests_dir)
    if not tests_path.is_dir():
        return {}

    # Normalize to a list
    if isinstance(suite, str):
        suites: list[str] = [suite]
    elif suite is None:
        suites = ["all"]
    else:
        suites = suite

    # If "all" is anywhere in the list, scan everything
    if "all" in suites:
        result: dict[str, str] = {}
        for yaml_file in tests_path.rglob("*.yaml"):
            result[yaml_file.stem] = str(yaml_file)
        return result

    result = {}
    for suite_name in suites:
        scan_path: Path = tests_path / suite_name
        if not scan_path.is_dir():
            continue
        for yaml_file in scan_path.rglob("*.yaml"):
            result[yaml_file.stem] = str(yaml_file)
    return result


def discover_suites(tests_dir: str | Path) -> list[str]:
    """Discover the available suites from the requirements tree.

    Scans the immediate subdirectories of ``tests_dir`` and returns the names
    of those that contain at least one requirement YAML file (searched
    recursively). This lets the CLI validate ``--suite`` values against the
    suites that actually exist on disk, so newly added operation, capability,
    or integration suites are picked up without editing a hardcoded list.

    Args:
        tests_dir: Root directory containing requirement suite folders.

    Returns:
        Sorted list of suite names (folder names). Empty when ``tests_dir`` is
        not a directory or contains no requirement suite folders.
    """
    tests_path: Path = Path(tests_dir)
    if not tests_path.is_dir():
        return []

    suites: list[str] = []
    for child in sorted(tests_path.iterdir()):
        if not child.is_dir():
            continue
        if next(child.rglob("*.yaml"), None) is not None:
            suites.append(child.name)
    return suites


# endregion


# region Execution history retrieval


def get_execution_history(execution_arn: str, region: str) -> dict[str, Any] | None:
    """Retrieve durable execution event history via the boto3 SDK.

    Args:
        execution_arn: The durable execution ARN.
        region: AWS region.

    Returns:
        Parsed response dict, or None on failure.
    """
    client = boto3.client("lambda", region_name=region)
    try:
        response: dict[str, Any] = client.get_durable_execution_history(
            DurableExecutionArn=execution_arn,
            IncludeExecutionData=True,
        )
    except (ClientError, BotoCoreError) as e:
        print(f"  Failed to get execution history: {e}", file=sys.stderr)
        return None
    else:
        return response


def get_durable_execution(execution_arn: str, region: str) -> dict[str, Any] | None:
    """Retrieve durable execution details including status and result.

    Uses the get_durable_execution API to retrieve the execution's current
    status, result (if succeeded), and error information (if failed).

    Args:
        execution_arn: The durable execution ARN.
        region: AWS region.

    Returns:
        Parsed response dict, or None on failure.
    """
    client = boto3.client("lambda", region_name=region)
    try:
        response: dict[str, Any] = client.get_durable_execution(
            DurableExecutionArn=execution_arn,
        )
    except (ClientError, BotoCoreError) as e:
        print(f"  Failed to get durable execution: {e}", file=sys.stderr)
        return None
    else:
        return response


def get_execution_status(history: dict) -> str | None:
    """Extract the execution status from a history response.

    Checks the top-level ExecutionStatus field first. If absent, scans
    the event list for terminal event types (ExecutionSucceeded, etc.).

    Args:
        history: The parsed history response dict.

    Returns:
        The execution status string, or None if not determined.
    """
    explicit: str | None = history.get("ExecutionStatus")
    if explicit:
        return explicit

    # Fall back to scanning the last terminal event
    events: list[dict[str, Any]] = history.get("Events", history.get("events", []))
    for event in reversed(events):
        match event.get("EventType"):
            case "ExecutionSucceeded":
                return "SUCCEEDED"
            case "ExecutionFailed":
                return "FAILED"
            case "ExecutionTimedOut":
                return "TIMED_OUT"
            case "ExecutionCancelled":
                return "CANCELLED"
    return None


def _extract_execution_output(events: list[dict[str, Any]]) -> Any | None:
    """Extract the execution output from the ExecutionSucceeded event.

    The output is stored in ExecutionSucceededDetails.Result.Payload.

    Args:
        events: List of event dicts from the execution history.

    Returns:
        The parsed output value, or None if not found.
    """
    for event in reversed(events):
        if event.get("EventType") != "ExecutionSucceeded":
            continue
        details: dict[str, Any] = event.get("ExecutionSucceededDetails", {})
        result: dict[str, Any] | None = details.get("Result")
        if result is None:
            return None
        payload: str | None = result.get("Payload")
        if payload is None:
            return None
        with contextlib.suppress(json.JSONDecodeError, TypeError):
            return json.loads(payload)
        return payload
    return None


def save_execution_history(description_id: str, history: dict, output_dir: str | Path | None = None) -> str:
    """Save execution history JSON to output/<description_id>.json.

    Args:
        description_id: The test description ID (e.g. "1-8").
        history: The raw execution history dict from the AWS CLI.
        output_dir: Optional directory to save the history file. Defaults to OUTPUT_DIR.

    Returns:
        The path to the saved file.
    """
    resolved_dir: Path = Path(output_dir) if output_dir is not None else Path(OUTPUT_DIR)
    resolved_dir.mkdir(parents=True, exist_ok=True)
    output_path: Path = resolved_dir / f"{description_id}.json"
    with open(output_path, "w") as f:
        json.dump(history, f, indent=4, default=str)
    print(f"  Saved execution history to {output_path}")
    return str(output_path)


def _validate_execution_result(
    execution_arn: str,
    expected_result: dict[str, Any],
    region: str,
    context: PlaceholderContext | None = None,
) -> list[str]:
    """Validate execution status and result using get_durable_execution.

    Retrieves the execution details and validates:
    - Status first (always checked).
    - If SUCCEEDED, validates the Result field.
    - If non-success, validates status only.

    Args:
        execution_arn: The durable execution ARN.
        expected_result: Dict with ExpectedResult fields from the test
            requirement (ExecutionStatus, Result).
            Note: expected result is compared against the actual after a single json.loads;
                  YAML authors should write the post-decode form they expect
        context: Optional PlaceholderContext for substituting placeholders
            in expected values.
        region: AWS region.

    Returns:
        A list of error strings. Empty list means validation passed.
    """
    errors: list[str] = []

    execution: dict[str, Any] | None = get_durable_execution(execution_arn, region)
    if execution is None:
        errors.append("Failed to retrieve durable execution details")
        return errors

    actual_status: str | None = execution.get("Status")

    # Validate status
    expected_status: str | None = expected_result.get("ExecutionStatus")
    if expected_status and actual_status != expected_status:
        errors.append(f"Expected ExecutionStatus={expected_status!r}, got {actual_status!r}")
        return errors

    # If non-success, validate status only
    if actual_status != "SUCCEEDED":
        return errors

    # If succeeded, validate the result
    if "Result" not in expected_result:
        return errors
    expected_output: Any = expected_result.get("Result")

    actual_result_raw: str | None = execution.get("Result")
    actual_output: Any = None
    if actual_result_raw is not None:
        try:
            actual_output = json.loads(actual_result_raw)
        except (json.JSONDecodeError, TypeError):
            actual_output = actual_result_raw

    # Substitute placeholders in expected output
    if context is not None:
        expected_output = context.substitute(expected_output)

    if actual_output != expected_output:
        errors.append(f"Expected Result={expected_output!r}, got {actual_output!r}")

    return errors


# endregion


# region Callback helpers


def extract_callback_events(
    events: list[dict[str, Any]],
    already_handled: set[str],
) -> list[dict[str, Any]]:
    """Find CallbackStarted events that haven't been handled yet.

    Args:
        events: List of event dicts from the execution history.
        already_handled: Set of callback IDs already processed.

    Returns:
        List of new CallbackStarted event dicts.
    """
    new_callbacks: list[dict[str, Any]] = []
    for event in events:
        if event.get("EventType") != _CALLBACK_CREATED_EVENT_TYPE:
            continue
        details: dict[str, Any] = event.get("CallbackStartedDetails", {})
        callback_id: str | None = details.get("CallbackId")
        if callback_id and callback_id not in already_handled:
            new_callbacks.append(event)
    return new_callbacks


def find_matching_action(
    callback_event: dict[str, Any],
    actions: list[CallbackAction],
    used_indices: set[int],
    context: PlaceholderContext | None = None,
) -> tuple[CallbackAction | None, int | None]:
    """Find the first matching CallbackAction for a callback event.

    Matching is done by callback name. Wildcard "*" matches any name.
    Each action is used at most once (tracked by used_indices).
    Placeholder references in action callback_name are resolved via context.

    Args:
        callback_event: The CallbackStarted event dict.
        actions: List of configured CallbackActions.
        used_indices: Set of action indices already consumed.
        context: Optional PlaceholderContext for resolving placeholders
            in action callback names.

    Returns:
        Tuple of (matching action, index) or (None, None) if no match.
    """
    event_callback_name: str = callback_event.get("Name", "")

    for i, action in enumerate(actions):
        if i in used_indices:
            continue
        resolved_name: str = action.callback_name
        if context is not None:
            resolved_name = str(context.substitute(resolved_name))
        if resolved_name in ("*", event_callback_name):
            return action, i
    return None, None


# endregion


# region Async validator


class PollingValidator:
    """Polls execution history, handles callbacks, and asserts results.

    Args:
        region: AWS region for API calls.
        config: Polling configuration. Uses defaults if not provided.
        context: PlaceholderContext for placeholder resolution during matching.
    """

    def __init__(
        self,
        region: str,
        config: AsyncValidationConfig | None = None,
        context: PlaceholderContext | None = None,
    ):
        self._region = region
        self._config: AsyncValidationConfig = config or AsyncValidationConfig()
        self._context: PlaceholderContext = context or PlaceholderContext()
        self._callback_sender = CallbackSender(region=region)

    def validate(
        self,
        execution_arn: str,
        expected_events: list[dict[str, Any]],
        expected_result: dict[str, Any] | None,
        callback_actions: list[CallbackAction] | None = None,
    ) -> AsyncValidationResult:
        """Poll execution history, handle callbacks, and assert results.

        When expected_result is None, polling stops as soon as the expected
        event history is fully matched — without waiting for the execution
        to reach a terminal state. When expected_result is provided, polling
        continues until a terminal status is reached.

        Args:
            execution_arn: The durable execution ARN to monitor.
            expected_events: Expected event history for matching.
            expected_result: Expected final result (ExecutionStatus, Output).
                If None, polling ends when all expected events are matched.
            callback_actions: List of callback actions to perform when
                CallbackCreated events appear.

        Returns:
            AsyncValidationResult with pass/fail status and details.
        """
        actions: list[CallbackAction] = callback_actions or []
        handled_callback_ids: set[str] = set()
        used_action_indices: set[int] = set()
        callbacks_sent: int = 0
        last_event_count: int = 0
        last_progress_time: float = time.time()
        errors: list[str] = []
        final_status: str | None = None
        actual_events: list[dict[str, Any]] = []
        history_only_mode: bool = expected_result is None

        while True:
            time.sleep(self._config.poll_interval_seconds)

            history: dict | None = get_execution_history(execution_arn, self._region)
            if history is None:
                errors.append("Failed to retrieve execution history during polling")
                return AsyncValidationResult(
                    passed=False,
                    errors=errors,
                    callbacks_sent=callbacks_sent,
                    final_status=final_status,
                )

            actual_events = history.get("Events", history.get("events", []))
            current_event_count: int = len(actual_events)
            final_status = get_execution_status(history)

            # Check if execution reached a terminal state (before
            # no-progress check so quick completions exit cleanly)
            if final_status in _TERMINAL_STATUSES:
                break

            # Check for progress
            if current_event_count > last_event_count:
                last_event_count = current_event_count
                last_progress_time = time.time()
            else:
                elapsed: float = time.time() - last_progress_time
                if elapsed >= self._config.no_progress_timeout_seconds:
                    errors.append(
                        f"No new events for {elapsed:.1f}s (timeout: {self._config.no_progress_timeout_seconds}s)"
                    )
                    break

            # Handle new CallbackStarted events
            new_callbacks: list[dict[str, Any]] = extract_callback_events(actual_events, handled_callback_ids)
            for cb_event in new_callbacks:
                details: dict[str, Any] = cb_event.get("CallbackStartedDetails", {})
                callback_id: str = details.get("CallbackId", "")

                action, idx = find_matching_action(cb_event, actions, used_action_indices, self._context)
                if action is None:
                    if not actions:
                        # No actions configured — timeout test, expected behaviour
                        print(
                            f"  No CallbackAction configured for"
                            f" '{cb_event.get('Name', '<anonymous>')}'; leaving callback open"
                        )
                    else:
                        event_name: str = cb_event.get("Name", "<anonymous>")
                        errors.append(f"No matching CallbackAction for callback '{event_name}' (id={callback_id})")
                    handled_callback_ids.add(callback_id)
                    continue

                if idx is not None:
                    used_action_indices.add(idx)

                # Heartbeat is a keep-alive, not terminal — don't
                # consume the callback so it remains matchable for a
                # follow-up success/failure action.
                if action.operation != "heartbeat":
                    handled_callback_ids.add(callback_id)

                try:
                    if action.delay_seconds > 0:
                        time.sleep(action.delay_seconds)
                    resolved_action: CallbackAction = CallbackAction(
                        callback_name=action.callback_name,
                        operation=action.operation,
                        payload=self._context.substitute(action.payload),
                    )
                    self._callback_sender.send(callback_id, resolved_action)
                    callbacks_sent += 1
                    print(f"  Sent {action.operation} callback for '{action.callback_name}' (id={callback_id})")
                except CallbackError as e:
                    errors.append(str(e))

            # In history-only mode, stop polling once all expected
            # events are matched rather than waiting for terminal status.
            if history_only_mode and expected_events:
                matcher = EventHistoryMatcher(context=self._context)
                match_result = matcher.match(expected_events, actual_events)
                if match_result.success:
                    return AsyncValidationResult(
                        passed=True,
                        errors=[],
                        placeholders=match_result.resolved_placeholders,
                        callbacks_sent=callbacks_sent,
                        final_status=final_status,
                        event_count=len(actual_events),
                    )

        # --- Final assertion: match event history ---
        if expected_events:
            matcher = EventHistoryMatcher(context=self._context)
            match_result = matcher.match(expected_events, actual_events)
            if not match_result.success:
                errors.extend(match_result.errors)
                return AsyncValidationResult(
                    passed=False,
                    errors=errors,
                    placeholders=match_result.resolved_placeholders,
                    callbacks_sent=callbacks_sent,
                    final_status=final_status,
                    event_count=len(actual_events),
                )
            placeholders: dict[str, Any] = match_result.resolved_placeholders
        else:
            placeholders = {}

        # --- Assert expected result (status + result) ---
        if expected_result:
            result_errors: list[str] = _validate_execution_result(
                execution_arn=execution_arn,
                expected_result=expected_result,
                context=self._context,
                region=self._region,
            )
            errors.extend(result_errors)

        return AsyncValidationResult(
            passed=len(errors) == 0,
            errors=errors,
            placeholders=placeholders,
            callbacks_sent=callbacks_sent,
            final_status=final_status,
            event_count=len(actual_events),
        )


# endregion


# region Single test description validation


def _validate_expected_logs(
    description_data: dict[str, Any],
    stack_name: str,
    function_name: str,
    start_time_ms: int,
    region: str,
    context: PlaceholderContext | None = None,
) -> list[str]:
    """Validate ExpectedLogs from a test description against CloudWatch Logs.

    Args:
        description_data: Parsed YAML test description.
        stack_name: CloudFormation stack name for resolving the log group.
        function_name: Logical resource ID of the Lambda function.
        start_time_ms: Epoch milliseconds marking the start of the invocation.
        context: Optional PlaceholderContext for substituting placeholders.
        region: AWS region.

    Returns:
        A list of error strings. Empty list means all log expectations passed.
    """
    expected_logs: list[dict[str, Any]] | None = description_data.get("ExpectedLogs")
    if not expected_logs:
        return []

    log_validator = CloudWatchLogValidator()
    log_retriever = CloudWatchLogRetriever(region=region)

    log_group: str = log_retriever.get_log_group_name(stack_name, function_name)

    log_events: list[dict] = log_retriever.get_log_events(
        log_group_name=log_group,
        start_time_ms=start_time_ms,
        wait_seconds=10,
    )

    log_result = log_validator.validate(expected_logs, log_events, context=context)
    return log_result.errors


def validate_description(
    function_name: str,
    description_id: str,
    test_file: str,
    invoker: Invoker,
    tmp_dir: str,
    region: str,
    output_dir: str | None = None,
) -> DescriptionResult:
    """Invoke a function for a given test description and assert the execution history."""
    if not Path(test_file).is_file():
        return DescriptionResult(
            description_id=description_id,
            function_name=function_name,
            passed=False,
            errors=[f"Test file not found: {test_file}"],
        )

    description_data = load_yaml_file(test_file)

    # --- Read optional flag ---
    is_optional: bool = bool(description_data.get("optional", False))

    # --- Resolve variables and create placeholder context ---
    context: PlaceholderContext = PlaceholderContext()
    context.resolve_variables(description_data.get("Variables"))

    # --- Check if this is an async test ---
    is_async: bool = bool(description_data.get("AsyncInvoke", False))
    if is_async:
        return _validate_description_async(
            function_name=function_name,
            description_id=description_id,
            description_data=description_data,
            invoker=invoker,
            tmp_dir=tmp_dir,
            is_optional=is_optional,
            context=context,
            output_dir=output_dir,
            region=region,
        )

    # --- Substitute placeholders in Input ---
    raw_input: Any = description_data.get("Input")
    resolved_input: Any = context.substitute(raw_input)

    # --- Write a temporary JSON event file for the invoker ---
    event_payload = {"Input": resolved_input}
    event_file: str = str(Path(tmp_dir) / f"{description_id}_event.json")
    with open(event_file, "w") as ef:
        json.dump(event_payload, ef)

    # --- Invoke ---
    print(f"  Invoking {function_name} with event from {description_id}.yaml ...")
    invocation_start_ms = int(time.time() * 1000)
    try:
        inv_result = invoker.invoke(
            function_name=function_name,
            event_file_path=event_file,
        )
    except (FileNotFoundError, EventFileError, SamCliError) as e:
        return DescriptionResult(
            description_id=description_id,
            function_name=function_name,
            passed=False,
            optional=is_optional,
            errors=[f"Invocation failed: {e}"],
        )

    # --- Extract execution ARN ---
    try:
        response = json.loads(inv_result.output)
    except json.JSONDecodeError:
        return DescriptionResult(
            description_id=description_id,
            function_name=function_name,
            passed=False,
            optional=is_optional,
            errors=["Could not parse invocation output as JSON"],
        )
    execution_arn = response.get("DurableExecutionArn")
    if not execution_arn:
        return DescriptionResult(
            description_id=description_id,
            function_name=function_name,
            passed=False,
            optional=is_optional,
            errors=["No DurableExecutionArn in invocation response"],
        )

    # --- Get history ---
    history = get_execution_history(execution_arn, region)
    if history is None:
        return DescriptionResult(
            description_id=description_id,
            function_name=function_name,
            passed=False,
            optional=is_optional,
            errors=["Failed to retrieve execution history"],
        )

    # --- Save history to output/<description_id>.json before asserting ---
    save_execution_history(description_id, history, output_dir=output_dir)

    # --- Assert execution history ---
    expected_events = (
        description_data.get("ExpectedExecutionHistory") if isinstance(description_data, dict) else description_data
    )
    if not expected_events:
        return DescriptionResult(
            description_id=description_id,
            function_name=function_name,
            passed=False,
            optional=is_optional,
            errors=["No ExpectedExecutionHistory found in description file"],
        )

    actual_events = history.get("Events", history.get("events", []))
    matcher = EventHistoryMatcher(context=context)
    match_result = matcher.match(expected_events, actual_events)

    if not match_result.success:
        return DescriptionResult(
            description_id=description_id,
            function_name=function_name,
            passed=False,
            optional=is_optional,
            errors=match_result.errors,
            placeholders=match_result.resolved_placeholders,
        )

    # --- Assert expected result (status + result) ---
    expected_result: dict[str, Any] | None = description_data.get("ExpectedResult")
    if expected_result:
        result_errors: list[str] = _validate_execution_result(
            execution_arn=execution_arn,
            expected_result=expected_result,
            context=context,
            region=region,
        )
        if result_errors:
            return DescriptionResult(
                description_id=description_id,
                function_name=function_name,
                passed=False,
                optional=is_optional,
                errors=result_errors,
                placeholders=match_result.resolved_placeholders,
            )

    # --- Assert expected logs (if specified) ---
    log_errors: list[str] = _validate_expected_logs(
        description_data=description_data,
        stack_name=invoker.stack_name,
        function_name=function_name,
        start_time_ms=invocation_start_ms,
        context=context,
        region=region,
    )
    if log_errors:
        return DescriptionResult(
            description_id=description_id,
            function_name=function_name,
            passed=False,
            optional=is_optional,
            errors=log_errors,
            placeholders=match_result.resolved_placeholders,
        )

    return DescriptionResult(
        description_id=description_id,
        function_name=function_name,
        passed=True,
        optional=is_optional,
        errors=[],
        placeholders=match_result.resolved_placeholders,
    )


def _validate_description_async(
    function_name: str,
    description_id: str,
    description_data: dict,
    invoker: Invoker,
    tmp_dir: str,
    region: str,
    is_optional: bool = False,  # noqa: FBT001, FBT002
    context: PlaceholderContext | None = None,
    output_dir: str | None = None,
) -> DescriptionResult:
    """Validate a test description using async invocation with polling.

    Flow:
    1. Invoke the function asynchronously (InvocationType=Event)
    2. Poll execution history, handling callbacks as they appear
    3. Assert final event history and execution result

    Args:
        function_name: Logical resource ID of the Lambda function.
        description_id: The test requirement ID.
        description_data: Parsed YAML test description.
        invoker: The Invoker instance for SAM remote invoke.
        tmp_dir: Temporary directory for event files.
        is_optional: Whether this test requirement is optional.
        context: PlaceholderContext with pre-resolved variables.
        output_dir: Optional directory for execution history files.
        region: AWS region.

    Returns:
        DescriptionResult with pass/fail status.
    """
    if context is None:
        context = PlaceholderContext()

    # --- Substitute placeholders in Input ---
    raw_input: Any = description_data.get("Input")
    resolved_input: Any = context.substitute(raw_input)

    # --- Write event file ---
    event_payload: dict = {"Input": resolved_input}
    event_file: str = str(Path(tmp_dir) / f"{description_id}_event.json")
    with open(event_file, "w") as ef:
        json.dump(event_payload, ef)

    # --- Parse callback actions ---
    callback_actions: list[CallbackAction] = []
    raw_actions: list[dict] = description_data.get("CallbackActions", [])
    for raw in raw_actions:
        try:
            callback_actions.append(CallbackAction.from_dict(raw))
        except ValueError as e:
            return DescriptionResult(
                description_id=description_id,
                function_name=function_name,
                passed=False,
                optional=is_optional,
                errors=[f"Invalid CallbackAction: {e}"],
            )

    # --- Parse async config overrides ---
    async_config_data: dict = description_data.get("AsyncConfig", {})
    async_config: AsyncValidationConfig = AsyncValidationConfig.from_dict(async_config_data)

    # --- Async invoke ---
    print(f"  Async invoking {function_name} with event from {description_id}.yaml ...")
    invocation_start_ms: int = int(time.time() * 1000)
    try:
        inv_result = invoker.invoke_async(
            function_name=function_name,
            event_file_path=event_file,
        )
    except (FileNotFoundError, EventFileError, SamCliError) as e:
        return DescriptionResult(
            description_id=description_id,
            function_name=function_name,
            passed=False,
            optional=is_optional,
            errors=[f"Async invocation failed: {e}"],
        )

    # --- Extract execution ARN from async response ---
    execution_arn: str | None = None
    try:
        response: dict = json.loads(inv_result.output)
        execution_arn = response.get("DurableExecutionArn")
    except (json.JSONDecodeError, TypeError):
        pass

    if not execution_arn:
        return DescriptionResult(
            description_id=description_id,
            function_name=function_name,
            passed=False,
            optional=is_optional,
            errors=[f"No DurableExecutionArn in async invocation response. Output: {inv_result.output[:200]}"],
        )

    # --- Poll and validate ---
    expected_events: list[dict] = description_data.get("ExpectedExecutionHistory", [])
    expected_result: dict | None = description_data.get("ExpectedResult")

    validator = PollingValidator(region=region, config=async_config, context=context)
    print(f"  Polling execution history for {execution_arn} ...")
    async_result: AsyncValidationResult = validator.validate(
        execution_arn=execution_arn,
        expected_events=expected_events,
        expected_result=expected_result,
        callback_actions=callback_actions,
    )

    # --- Save final history ---
    final_history: dict | None = get_execution_history(execution_arn, region)
    if final_history:
        save_execution_history(description_id, final_history, output_dir=output_dir)

    if not async_result.passed:
        return DescriptionResult(
            description_id=description_id,
            function_name=function_name,
            passed=False,
            optional=is_optional,
            errors=async_result.errors,
            placeholders=async_result.placeholders,
        )

    # --- Assert expected logs (if specified) ---
    log_errors: list[str] = _validate_expected_logs(
        description_data=description_data,
        stack_name=invoker.stack_name,
        function_name=function_name,
        start_time_ms=invocation_start_ms,
        context=context,
        region=region,
    )
    if log_errors:
        return DescriptionResult(
            description_id=description_id,
            function_name=function_name,
            passed=False,
            optional=is_optional,
            errors=log_errors,
            placeholders=async_result.placeholders,
        )

    return DescriptionResult(
        description_id=description_id,
        function_name=function_name,
        passed=True,
        optional=is_optional,
        errors=[],
        placeholders=async_result.placeholders,
    )


# endregion
