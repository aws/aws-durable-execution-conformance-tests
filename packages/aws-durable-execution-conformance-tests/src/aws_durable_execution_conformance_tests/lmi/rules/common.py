# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Shared evidence envelope, ownership, history and admission rules."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from itertools import pairwise
from typing import Any, TypeGuard

from aws_durable_execution_conformance_tests.lmi.rules.values import (
    _CALL_METHODS,
    HISTORY_STATUSES,
    RECOVERY_POLICIES,
    RUNTIMES,
    SCHEMA_VERSION,
    is_json_integer,
    same_json,
)
from aws_durable_execution_conformance_tests.lmi.rules.view import CaseView


def optional_shapes(view: CaseView) -> None:
    """Check supplied wire fields without enabling another scenario's behavior."""
    if "scenario" in view.value:
        view.string(view.value["scenario"], "scenario")
    if "coverageProfile" in view.value:
        view.string(view.value["coverageProfile"], "coverageProfile")
        if view.standard_behavior and view.value["coverageProfile"] != "standard":
            view.error("coverageProfile must be standard for shared lifecycle evidence")
    if "recoveryPolicy" in view.profile and view.profile["recoveryPolicy"] not in RECOVERY_POLICIES:
        view.error("profile.recoveryPolicy must be reuse-worker or replace-worker")
    if "budgets" in view.value:
        budgets = view.mapping(view.value["budgets"], "budgets")
        for field in ("admissionMs", "cleanupMs", "recoveryMs", "observationGraceMs"):
            if field in budgets:
                view.number(budgets[field], f"budgets.{field}")
    if "replay" not in view.value:
        return
    replay = view.mapping(view.value["replay"], "replay")
    for field in ("before", "after"):
        if field in replay:
            for operation in view.records(replay[field], f"replay.{field}", nonempty=True):
                view.operation_shape(operation, f"replay.{field}", completed=field == "before")
    if "operationExecutions" in replay:
        for execution in view.records(replay["operationExecutions"], "replay.operationExecutions", nonempty=True):
            for field in ("requestId", "operationId"):
                view.string(execution.get(field), f"replay.operationExecutions.{field}")
    if "retry" in replay:
        retry = view.mapping(replay["retry"], "replay.retry")
        view.string(retry.get("operationId"), "replay.retry.operationId")
        attempts = view.records(retry.get("attempts"), "replay.retry.attempts")
        if len(attempts) < 2:
            view.error("retry requires at least two observed attempts")
        for attempt in attempts:
            for field in ("attemptId", "requestId", "outcome"):
                view.string(attempt.get(field), f"retry.{field}")
    if "sideEffects" in replay:
        for count in view.records(replay["sideEffects"], "replay.sideEffects", nonempty=True):
            view.string(count.get("operationId"), "replay.sideEffects.operationId")
            for field in ("count", "expectedCount"):
                if not is_json_integer(count.get(field)) or count[field] < 0:
                    view.error(f"replay.sideEffects.{field} must be a non-negative integer")


def common(view: CaseView, *, validate_behavior: bool = True, allow_no_launches: bool = False) -> None:
    value = view.value
    if not is_json_integer(value.get("schemaVersion")) or value.get("schemaVersion") != SCHEMA_VERSION:
        view.error(f"unsupported schemaVersion; expected {SCHEMA_VERSION}")
    for key in ("runId", "caseId"):
        view.string(value.get(key), key)
    if value.get("runtime") not in RUNTIMES:
        view.error(f"runtime must be one of {RUNTIMES}")
    if value.get("requirementId") not in view.requirement_ids:
        view.error("unknown requirementId")
    artifact = view.mapping(value.get("artifact"), "artifact")
    for key in ("sdk", "conformance"):
        view.string(artifact.get(key), f"artifact.{key}")
    view.profile = view.mapping(value.get("profile"), "profile")
    for key in ("functionArn", "configurationDigest"):
        view.string(view.profile.get(key), f"profile.{key}")
    concurrency = view.profile.get("nativeConcurrency")
    if not is_json_integer(concurrency) or concurrency < 1:
        view.error("profile.nativeConcurrency must be a positive integer")
    if view.number(view.profile.get("timeoutMs"), "profile.timeoutMs") <= 0:
        view.error("profile.timeoutMs must be positive")
    if validate_behavior:
        # Physical retirement can retain partial behavior diagnostics;
        # only the observation validators require their supplied shapes.
        optional_shapes(view)
    observation_end = view.number(value.get("observationEndedAtMs"), "observationEndedAtMs")
    attempt_ordinals: set[tuple[str, int]] = set()
    for attempt in view.records(value.get("attempts"), "attempts", nonempty=not allow_no_launches):
        request_id = view.string(attempt.get("requestId"), "attempt.requestId")
        if request_id in view.attempts:
            view.error(f"duplicate requestId {request_id}")
        view.attempts[request_id] = attempt
        for key in ("executionArn", "environmentId", "workerId", "processId"):
            view.string(attempt.get(key), f"attempt {request_id}.{key}")
        role = view.string(attempt.get("role"), f"attempt {request_id}.role")
        ordinal = attempt.get("attempt")
        if not is_json_integer(ordinal) or ordinal < 1:
            view.error(f"attempt {request_id}.attempt must be a positive integer")
        else:
            identity = (role, int(ordinal))
            if identity in attempt_ordinals:
                view.error(f"duplicate attempt ordinal {ordinal} for role {role}")
            attempt_ordinals.add(identity)
        start = view.number(attempt.get("startedAtMs"), f"attempt {request_id}.startedAtMs")
        end = view.number(attempt.get("endedAtMs"), f"attempt {request_id}.endedAtMs")
        if end <= start or end > observation_end:
            view.error(f"attempt {request_id} has an invalid observation interval")
        if attempt.get("outcome") not in ("SUCCEEDED", "FAILED", "PENDING", "TIMED_OUT"):
            view.error(f"attempt {request_id} has no recognized invocation outcome")
        for field in ("startedAtMsSource", "endedAtMsSource"):
            if field in attempt and attempt[field] not in ("platform", "fixture"):
                view.error(f"attempt {request_id}.{field} must be platform or fixture")
    if validate_behavior:
        by_role: dict[str, list[tuple[float, int]]] = {}
        for attempt in view.attempts.values():
            admission_role = attempt.get("role")
            admitted_at = attempt.get("startedAtMs")
            ordinal = attempt.get("attempt")
            if isinstance(admission_role, str) and isinstance(admitted_at, (int, float)) and is_json_integer(ordinal):
                by_role.setdefault(admission_role, []).append((admitted_at, int(ordinal)))
        for admission_role, admissions in by_role.items():
            if any(earlier[0] < later[0] and earlier[1] > later[1] for earlier, later in pairwise(sorted(admissions))):
                view.error(f"attempt ordinals contradict the admission order for {admission_role}", "PLACEMENT")
    selected = view.mapping(value.get("selectedRequests"), "selectedRequests") if validate_behavior else {}
    if validate_behavior and not selected:
        view.error("selectedRequests must not be empty")
    seen: set[str] = set()
    for role, request_id in selected.items():
        if not isinstance(request_id, str) or request_id not in view.attempts:
            view.error(f"selectedRequests.{role} references a missing attempt")
            continue
        if request_id in seen:
            view.error("distinct selected roles must bind distinct original requests", "PLACEMENT")
        seen.add(request_id)
        attempt = view.attempts[request_id]
        if attempt.get("role") != role or attempt.get("attempt") != 1:
            view.error(f"selectedRequests.{role} is a retry or a substituted role", "PLACEMENT")
        if (
            view.standard_behavior
            and role not in ("resume", "recovery")
            and view.value.get("requirementId") != "lmi-lifecycle-5"
            and any(
                other.get("executionArn") == attempt.get("executionArn")
                and view.number(other.get("startedAtMs"), "prior attempt.startedAtMs")
                < view.number(attempt.get("startedAtMs"), "selected attempt.startedAtMs")
                for other in view.attempts.values()
            )
        ):
            view.error(f"selectedRequests.{role} is not the first invocation of its execution", "PLACEMENT")
        if (
            view.standard_behavior
            and view.value.get("requirementId") == "lmi-lifecycle-4"
            and (role == "victim" or role.startswith("peer"))
            and any(
                other is not attempt
                and other.get("executionArn") == attempt.get("executionArn")
                and other.get("startedAtMs") == attempt.get("startedAtMs")
                for other in view.attempts.values()
            )
        ):
            view.error(f"tied native admissions cannot establish the original {role} invocation")
        view.selected[role] = attempt
    view.events = view.records(value.get("events"), "events", nonempty=not allow_no_launches)
    for event in view.events:
        kind = view.string(event.get("kind"), "event.kind")
        owned_record(view, event, "event", observation_end)
        if validate_behavior and kind == "admission_probe":
            probe = event.get("probe")
            if probe not in ("operation", "checkpoint"):
                view.error("admission_probe.probe must be operation or checkpoint")
            if event.get("outcome") not in ("ACCEPTED", "REJECTED"):
                subject = f"{probe} probe" if probe in ("operation", "checkpoint") else "admission probe"
                view.error(f"{subject} outcome must be ACCEPTED or REJECTED")
        if kind in {"observation_error", "control_error"}:
            view.error(f"{kind} reports failed observation or control publication")
    sdk_requests = view.records(value.get("sdkRequests"), "sdkRequests")
    for request in sdk_requests:
        owned_record(view, request, "sdkRequest", observation_end)
        view.string(request.get("method"), "sdkRequest.method")
        if request.get("outcome") not in ("DISPATCHED", "REJECTED"):
            view.error("sdkRequest.outcome must be DISPATCHED or REJECTED")
        if validate_behavior and "updates" in request:
            for update in view.records(request["updates"], "sdkRequest.updates"):
                for field in ("id", "type", "action"):
                    view.string(update.get(field), f"sdkRequest.updates.{field}")
    if validate_behavior:
        for event in view.events:
            call_kind = event.get("kind")
            method = _CALL_METHODS.get(call_kind) if isinstance(call_kind, str) else None
            if method is None:
                continue
            outcome = event.get("outcome", "DISPATCHED")
            if outcome not in ("DISPATCHED", "REJECTED"):
                view.error("observed SDK call outcome must be DISPATCHED or REJECTED")
                continue
            if not _has_matching_sdk_request(event, method, outcome, sdk_requests):
                view.error("observed SDK call lacks a unique matching SDK request")
    for fault in view.records(value.get("faultsCreated"), "faultsCreated"):
        if "releasedAtMs" in fault:
            view.number(fault["releasedAtMs"], "faultsCreated.releasedAtMs")
    histories(view, check_expectations=validate_behavior, allow_no_launches=allow_no_launches)
    collection_errors = value.get("collectionErrors", [])
    if not isinstance(collection_errors, list):
        view.error("collectionErrors must be an array of non-empty strings")
    else:
        for index, error in enumerate(collection_errors):
            message = view.string(error, f"collectionErrors[{index}]")
            if message:
                view.error(f"collectionErrors[{index}]: {message}")
    collection = view.mapping(value.get("collection"), "collection")
    watermarks = view.mapping(collection.get("watermarks"), "collection.watermarks")
    for source in ("lifecycle", "sdkRequests", "history"):
        if view.number(watermarks.get(source), f"collection.watermarks.{source}") < observation_end:
            view.error(f"{source} collection does not cover the observation window")
    references = collection.get("evidenceUris")
    if not isinstance(references, list) or not references:
        view.error("collection.evidenceUris must retain the raw evidence references")
    else:
        for index, uri in enumerate(references):
            view.string(uri, f"collection.evidenceUris[{index}]")
    if validate_behavior:
        no_late_dispatch(view)


def owned_record(view: CaseView, record: Mapping[str, Any], kind: str, end: float) -> None:
    request_id = record.get("requestId")
    if not isinstance(request_id, str) or request_id not in view.attempts:
        view.error(f"{kind} references an unrecorded request")
        return
    if record.get("ownerRequestId") != request_id:
        view.error(f"{kind} is attributed to a different invocation", "ASSERTION")
    if record.get("executionArn") != view.attempts[request_id].get("executionArn"):
        view.error(f"{kind} is attributed to a different execution", "ASSERTION")
    time = view.number(record.get("atMs"), f"{kind}.atMs")
    if time > end or time < view.attempts[request_id].get("startedAtMs", 0):
        view.error(f"{kind} lies outside its observation window")
    observed_during = record.get("observedDuringRequestId")
    if observed_during is not None:
        if not isinstance(observed_during, str) or observed_during not in view.attempts:
            view.error(f"{kind}.observedDuringRequestId references an unrecorded request")
        else:
            observed_attempt = view.attempts[observed_during]
            start = view.number(observed_attempt.get("startedAtMs"), "observed attempt.startedAtMs")
            finish = view.number(observed_attempt.get("endedAtMs"), "observed attempt.endedAtMs")
            if not start <= time <= finish:
                view.error(f"{kind} lies outside its observedDuringRequestId interval", "ASSERTION")


def histories(view: CaseView, *, check_expectations: bool = True, allow_no_launches: bool = False) -> None:
    expected = view.mapping(view.value.get("expectedHistories"), "expectedHistories") if check_expectations else {}
    actual: dict[str, Mapping[str, Any]] = {}
    for history in view.records(view.value.get("histories"), "histories", nonempty=not allow_no_launches):
        arn = view.string(history.get("executionArn"), "history.executionArn")
        if arn in actual:
            view.error(f"duplicate service history {arn}")
        actual[arn] = history
        if history.get("status") not in HISTORY_STATUSES:
            view.error(f"history {arn}.status must be a recognized execution status")
        if "startedAtMs" in history:
            started = view.number(history["startedAtMs"], f"history {arn}.startedAtMs")
            admissions: list[float] = []
            for attempt in view.attempts.values():
                admitted = attempt.get("startedAtMs")
                if (
                    attempt.get("executionArn") == arn
                    and isinstance(admitted, (int, float))
                    and not isinstance(admitted, bool)
                ):
                    admissions.append(float(admitted))
            if admissions and started > min(admissions):
                view.error(f"history {arn}.startedAtMs follows its first native admission")
        if history.get("ownerCaseId") != view.value.get("caseId"):
            view.error(f"history {arn} belongs to a different case", "ASSERTION")
        operations = view.records(history.get("operations"), "history.operations")
        ids: set[str] = set()
        for operation in operations:
            view.operation_shape(operation, "history operation", check_interval=True)
            operation_id = operation.get("id")
            if operation_id in ids:
                view.error("duplicate operation identity in service history")
            if isinstance(operation_id, str):
                ids.add(operation_id)
        if check_expectations:
            expectation = view.mapping(expected.get(arn), f"expectedHistories.{arn}")
            allow_running = False
            if ("status" in expectation) == ("allowedStatuses" in expectation):
                view.error(f"expectedHistories.{arn} must declare exactly one of status or allowedStatuses")
            if "allowedStatuses" in expectation:
                allowed = expectation["allowedStatuses"]
                if not _valid_history_status_allowance(allowed):
                    view.error(
                        f"expectedHistories.{arn}.allowedStatuses must contain distinct recognized execution statuses"
                    )
                else:
                    allow_running = "RUNNING" in allowed
            elif expectation.get("status") not in HISTORY_STATUSES:
                view.error(f"expectedHistories.{arn}.status must be a recognized execution status")
            if not allow_running and "result" not in expectation and "error" not in expectation:
                view.error(f"history {arn} needs an independently declared expected result/error")
            view.errors.extend(_history_expectation_mismatches(arn, history, expectation))
    arns = {attempt.get("executionArn") for attempt in view.attempts.values()}
    if set(actual) != arns or (check_expectations and set(expected) != arns):
        view.error("histories and expectedHistories must cover every launched execution exactly")


def no_late_dispatch(view: CaseView) -> None:
    disposal = view.value.get("requirementId") in ("lmi-lifecycle-2", "lmi-lifecycle-7")
    for request_id in view.attempts:
        closures = [
            event
            for event in view.events
            if event.get("requestId") == request_id
            and event.get("kind") in ("wrapper_return", "wrapper_raise", "admission_closed")
            and isinstance(event.get("atMs"), (float, int))
        ]
        if not closures:
            continue
        closed = min(event["atMs"] for event in closures)
        wrapper = None
        if disposal and request_id == view.selected.get("victim", {}).get("requestId"):
            exits = [event for event in closures if event["kind"] != "admission_closed"]
            if len(exits) == 1 and exits[0]["kind"] == "wrapper_return":
                wrapper = exits[0]
            elif len(exits) > 1:
                view.error("disposal needs exactly one wrapper exit for the selected victim")
        for request in view.value.get("sdkRequests", []):
            if (
                isinstance(request, Mapping)
                and request.get("requestId") == request_id
                and request.get("outcome") == "DISPATCHED"
                and isinstance(request.get("atMs"), (float, int))
                and request["atMs"] >= closed
            ):
                late_boundaries = [boundary for boundary in closures if view.at_or_after(request, boundary)]
                if late_boundaries:
                    view.error(f"request {request_id} dispatched SDK work after admission closed", "ASSERTION")
        for event in view.events:
            if (
                event.get("requestId") == request_id
                and event.get("kind") == "admission_probe"
                and isinstance(event.get("atMs"), (float, int))
                and event["atMs"] >= closed
            ):
                probe, outcome = event.get("probe"), event.get("outcome")
                if wrapper is not None:
                    # Check every tied closure even if an earlier one already closed admission.
                    # Short-circuiting here would hide malformed evidence based on its array order.
                    following = [boundary for boundary in closures if view.at_or_after(event, boundary)]
                    if permits_disposal_operation_probe(
                        event,
                        wrapper,
                        following,
                        lambda first, second: (
                            None
                            if (order := view.at_or_after(first, second, require_sequence=True)) is None
                            else not order
                        ),
                    ):
                        continue
                    if not following:
                        continue  # The probe was ordered before every closure in the same millisecond.
                if outcome != "REJECTED":
                    view.error(f"closed invocation admitted a {probe} probe", "ASSERTION")


def _has_matching_sdk_request(event: Mapping[str, Any], method: str, outcome: str, requests: Any) -> bool:
    call_id = event.get("callId")
    return (
        isinstance(call_id, str)
        and bool(call_id.strip())
        and sum(
            isinstance(request, Mapping)
            and request.get("method") == method
            and request.get("outcome") == outcome
            and all(request.get(key) == event.get(key) for key in ("requestId", "callId", "atMs"))
            for request in (requests if isinstance(requests, (list, tuple)) else ())
        )
        == 1
    )


def normalize_retirement_events(events: Sequence[Mapping[str, Any]], errors: list[str]) -> list[Mapping[str, Any]]:
    aliases = {"observed_task_start": "task_start", "observed_task_exit": "task_exit"}
    normalized = []
    for event in events:
        kind = event.get("kind")
        if isinstance(kind, str) and (kind == "observed_task" or kind.startswith("observed_task_")):
            if kind not in aliases:
                errors.append("INCOMPLETE: retirement task alias has an unsupported kind")
            elif event.get("sourceKind") != aliases[kind]:
                errors.append("INCOMPLETE: retirement task alias has missing or mismatched sourceKind")
            else:
                event = {**event, "kind": aliases[kind]}
        normalized.append(event)
    return normalized


def permits_disposal_operation_probe(
    probe: Mapping[str, Any],
    wrapper: Mapping[str, Any],
    following: Sequence[Mapping[str, Any]],
    precedes: Callable[[Mapping[str, Any], Mapping[str, Any]], bool | None],
) -> bool:
    return (
        probe.get("probe") == "operation"
        and probe.get("outcome") == "ACCEPTED"
        and wrapper in following
        and not any(boundary is not wrapper and precedes(boundary, wrapper) is True for boundary in following)
    )


def original_admission_conflicts(
    selected: Mapping[str, Any], attempts: Sequence[Mapping[str, Any]]
) -> tuple[bool, bool]:
    """Report an earlier or tied native admission to the selected execution."""
    request, execution, start = selected.get("requestId"), selected.get("executionArn"), selected.get("startedAtMs")
    if not isinstance(request, str) or not isinstance(execution, str) or type(start) not in (int, float):
        return False, False
    other_starts = [
        attempt["startedAtMs"]
        for attempt in attempts
        if attempt.get("requestId") != request
        and attempt.get("executionArn") == execution
        and type(attempt.get("startedAtMs")) in (int, float)
    ]
    return any(other < start for other in other_starts), start in other_starts


def _valid_history_status_allowance(allowed: Any) -> TypeGuard[list[str]]:
    return (
        isinstance(allowed, list)
        and bool(allowed)
        and all(isinstance(status, str) and status in HISTORY_STATUSES for status in allowed)
        and len(set(allowed)) == len(allowed)
    )


def _history_expectation_mismatches(arn: str, history: Mapping[str, Any], expectation: Mapping[str, Any]) -> list[str]:
    errors = []
    allowed = expectation.get("allowedStatuses")
    if _valid_history_status_allowance(allowed) and history.get("status") not in allowed:
        errors.append(f"ASSERTION: service history {arn}.status is outside the declared allowedStatuses")
    for key, item in expectation.items():
        if key != "allowedStatuses" and (key not in history or not same_json(history[key], item)):
            errors.append(f"ASSERTION: service history {arn}.{key} differs from the fixture expectation")
    return errors
