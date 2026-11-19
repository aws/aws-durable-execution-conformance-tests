# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Version 1 lifecycle evidence contract and fail-closed observation validators.

See ``evidence-schema.md`` next to this module. Times are milliseconds on one
coordinator-normalized observation clock, not uncorrelated process wall clocks.
The schema is an adapter contract, not proof that an untrusted producer is honest.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from decimal import Decimal
from itertools import pairwise
from typing import Any, Literal, Protocol, TypeGuard

SCHEMA_VERSION = 1
REQUIREMENT_IDS = tuple(f"lmi-lifecycle-{index}" for index in range(1, 8))
RUNTIMES = ("javascript", "java", "python")
HISTORY_STATUSES = ("SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED", "RUNNING", "PENDING")
TERMINAL_STATUSES = ("SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED")
COMPLETED_OPERATION_STATUSES = ("SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED")

_WORK_INTERVALS = (
    ("task_start", "task_exit", "taskId"),
    ("io_start", "io_exit", "taskId"),
    ("checkpoint_call", "checkpoint_exit", "callId"),
    ("poll_call", "poll_exit", "callId"),
)
_CALL_METHODS = {"checkpoint_call": "CheckpointDurableExecution", "poll_call": "GetDurableExecutionState"}


class EvidenceValidator(Protocol):
    def __call__(self, evidence: Mapping[str, Any], *, phase: str = "final") -> Sequence[str]: ...


def is_json_integer(value: Any) -> TypeGuard[int | float]:
    """Accept finite integral JSON numbers, independently of their spelling."""
    return type(value) is int or (type(value) is float and math.isfinite(value) and value.is_integer())


def same_json(left: Any, right: Any) -> bool:
    """Compare decoded checkpoint data without Python's bool/number equality.

    ``True == 1`` holds in Python, so plain container equality would accept a
    stored number that an SDK turned into a boolean, or the reverse. JSON keeps
    those types distinct, and so must a checkpoint comparison.
    """
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left is right
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        return set(left) == set(right) and all(same_json(left[key], right[key]) for key in left)
    if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        return len(left) == len(right) and all(same_json(a, b) for a, b in zip(left, right, strict=True))
    if isinstance(left, Decimal) or isinstance(right, Decimal):
        if not isinstance(left, (int, float, Decimal)) or not isinstance(right, (int, float, Decimal)):
            return False
        left_number, right_number = Decimal(str(left)), Decimal(str(right))
        return left_number.is_finite() and right_number.is_finite() and left_number == right_number
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return left == right
    return type(left) is type(right) and bool(left == right)


class _Evidence:
    def __init__(
        self,
        value: Mapping[str, Any],
        requirement_ids: tuple[str, ...] = REQUIREMENT_IDS,
        *,
        standard_behavior: bool = True,
    ) -> None:
        self.value = value
        self.requirement_ids = requirement_ids
        self.standard_behavior = standard_behavior
        self.errors: list[str] = []
        self.attempts: dict[str, Mapping[str, Any]] = {}
        self.selected: dict[str, Mapping[str, Any]] = {}
        self.events: list[Mapping[str, Any]] = []
        self.profile: Mapping[str, Any] = {}

    def error(self, message: str, kind: str = "INCOMPLETE") -> None:
        self.errors.append(f"{kind}: {message}")

    def mapping(self, value: Any, path: str) -> Mapping[str, Any]:
        if not isinstance(value, Mapping):
            self.error(f"{path} must be an object")
            return {}
        return value

    def records(self, value: Any, path: str, *, nonempty: bool = False) -> list[Mapping[str, Any]]:
        if not isinstance(value, list):
            self.error(f"{path} must be an array")
            return []
        if nonempty and not value:
            self.error(f"{path} must not be empty")
        return [self.mapping(item, f"{path}[{index}]") for index, item in enumerate(value)]

    def string(self, value: Any, path: str) -> str:
        if not isinstance(value, str) or not value.strip():
            self.error(f"{path} must be a non-empty string")
            return ""
        return value

    def number(self, value: Any, path: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            self.error(f"{path} must be a finite non-negative number")
            return 0
        return float(value)

    def event(self, request: Mapping[str, Any], kind: str) -> Mapping[str, Any]:
        matches = [
            event
            for event in self.events
            if event.get("requestId") == request.get("requestId") and event.get("kind") == kind
        ]
        if len(matches) != 1:
            self.error(f"request {request.get('requestId')} needs exactly one {kind} event")
            return {}
        return matches[0]

    def require_roles(self, *roles: str) -> bool:
        missing = [role for role in roles if role not in self.selected]
        if missing:
            self.error(f"selectedRequests is missing roles: {', '.join(missing)}")
        return not missing

    def operation_shape(
        self,
        operation: Mapping[str, Any],
        path: str,
        *,
        completed: bool = False,
        check_interval: bool = False,
        partial: bool = False,
    ) -> None:
        """Validate checkpoint fields, allowing omissions in provisional cleanup fragments."""
        for key in ("id", "type", "status"):
            if not partial or key in operation:
                self.string(operation.get(key), f"{path}.{key}")
        if "name" not in operation:
            if not partial:
                self.error(f"{path}.name is required (null for an unnamed operation)")
        elif operation["name"] is not None:
            self.string(operation["name"], f"{path}.name")
        if "parentId" not in operation:
            if not partial:
                self.error(f"{path}.parentId is required (null for the root)")
        elif operation["parentId"] is not None:
            self.string(operation["parentId"], f"{path}.parentId")
        start = self.number(operation["startedAtMs"], f"{path}.startedAtMs") if "startedAtMs" in operation else None
        end = self.number(operation["endedAtMs"], f"{path}.endedAtMs") if "endedAtMs" in operation else None
        if check_interval and start is not None and end is not None and end < start:
            self.error(f"{path} endedAtMs precedes startedAtMs")
        if completed and not partial:
            if operation.get("status") not in COMPLETED_OPERATION_STATUSES:
                self.error(f"{path} must have a terminal operation status")
            outcome_key = "result" if operation.get("status") == "SUCCEEDED" else "error"
            if outcome_key not in operation:
                self.error(f"{path} must preserve the stored {outcome_key}")

    def optional_shapes(self) -> None:
        """Check supplied wire fields without enabling another scenario's behavior."""
        if "scenario" in self.value:
            self.string(self.value["scenario"], "scenario")
        if "coverageProfile" in self.value:
            self.string(self.value["coverageProfile"], "coverageProfile")
            if self.standard_behavior and self.value["coverageProfile"] != "standard":
                self.error("coverageProfile must be standard for shared lifecycle evidence")
        if "recoveryPolicy" in self.profile and self.profile["recoveryPolicy"] not in (
            "reuse-worker",
            "replace-worker",
        ):
            self.error("profile.recoveryPolicy must be reuse-worker or replace-worker")
        if "budgets" in self.value:
            budgets = self.mapping(self.value["budgets"], "budgets")
            for field in ("admissionMs", "cleanupMs", "recoveryMs", "observationGraceMs"):
                if field in budgets:
                    self.number(budgets[field], f"budgets.{field}")
        if "replay" not in self.value:
            return
        replay = self.mapping(self.value["replay"], "replay")
        for field in ("before", "after"):
            if field in replay:
                for operation in self.records(replay[field], f"replay.{field}", nonempty=True):
                    self.operation_shape(operation, f"replay.{field}", completed=field == "before")
        if "operationExecutions" in replay:
            for execution in self.records(replay["operationExecutions"], "replay.operationExecutions", nonempty=True):
                for field in ("requestId", "operationId"):
                    self.string(execution.get(field), f"replay.operationExecutions.{field}")
        if "retry" in replay:
            retry = self.mapping(replay["retry"], "replay.retry")
            self.string(retry.get("operationId"), "replay.retry.operationId")
            attempts = self.records(retry.get("attempts"), "replay.retry.attempts")
            if len(attempts) < 2:
                self.error("retry requires at least two observed attempts")
            for attempt in attempts:
                for field in ("attemptId", "requestId", "outcome"):
                    self.string(attempt.get(field), f"retry.{field}")
        if "sideEffects" in replay:
            for count in self.records(replay["sideEffects"], "replay.sideEffects", nonempty=True):
                self.string(count.get("operationId"), "replay.sideEffects.operationId")
                for field in ("count", "expectedCount"):
                    if not is_json_integer(count.get(field)) or count[field] < 0:
                        self.error(f"replay.sideEffects.{field} must be a non-negative integer")

    def common(self, *, validate_behavior: bool = True) -> None:
        value = self.value
        if not is_json_integer(value.get("schemaVersion")) or value.get("schemaVersion") != SCHEMA_VERSION:
            self.error(f"unsupported schemaVersion; expected {SCHEMA_VERSION}")
        for key in ("runId", "caseId"):
            self.string(value.get(key), key)
        if value.get("runtime") not in RUNTIMES:
            self.error(f"runtime must be one of {RUNTIMES}")
        if value.get("requirementId") not in self.requirement_ids:
            self.error("unknown requirementId")
        artifact = self.mapping(value.get("artifact"), "artifact")
        for key in ("sdk", "conformance"):
            self.string(artifact.get(key), f"artifact.{key}")
        self.profile = self.mapping(value.get("profile"), "profile")
        for key in ("functionArn", "configurationDigest"):
            self.string(self.profile.get(key), f"profile.{key}")
        concurrency = self.profile.get("nativeConcurrency")
        if not is_json_integer(concurrency) or concurrency < 1:
            self.error("profile.nativeConcurrency must be a positive integer")
        if self.number(self.profile.get("timeoutMs"), "profile.timeoutMs") <= 0:
            self.error("profile.timeoutMs must be positive")
        if validate_behavior:
            # Physical retirement can retain partial behavior diagnostics;
            # only the observation validators require their supplied shapes.
            self.optional_shapes()
        observation_end = self.number(value.get("observationEndedAtMs"), "observationEndedAtMs")
        attempt_ordinals: set[tuple[str, int]] = set()
        for attempt in self.records(value.get("attempts"), "attempts", nonempty=True):
            request_id = self.string(attempt.get("requestId"), "attempt.requestId")
            if request_id in self.attempts:
                self.error(f"duplicate requestId {request_id}")
            self.attempts[request_id] = attempt
            for key in ("executionArn", "environmentId", "workerId", "processId"):
                self.string(attempt.get(key), f"attempt {request_id}.{key}")
            role = self.string(attempt.get("role"), f"attempt {request_id}.role")
            ordinal = attempt.get("attempt")
            if not is_json_integer(ordinal) or ordinal < 1:
                self.error(f"attempt {request_id}.attempt must be a positive integer")
            else:
                identity = (role, int(ordinal))
                if identity in attempt_ordinals:
                    self.error(f"duplicate attempt ordinal {ordinal} for role {role}")
                attempt_ordinals.add(identity)
            start = self.number(attempt.get("startedAtMs"), f"attempt {request_id}.startedAtMs")
            end = self.number(attempt.get("endedAtMs"), f"attempt {request_id}.endedAtMs")
            if end <= start or end > observation_end:
                self.error(f"attempt {request_id} has an invalid observation interval")
            if attempt.get("outcome") not in ("SUCCEEDED", "FAILED", "PENDING", "TIMED_OUT"):
                self.error(f"attempt {request_id} has no recognized invocation outcome")
            for field in ("startedAtMsSource", "endedAtMsSource"):
                if field in attempt and attempt[field] not in ("platform", "fixture"):
                    self.error(f"attempt {request_id}.{field} must be platform or fixture")
        if validate_behavior:
            by_role: dict[str, list[tuple[float, int]]] = {}
            for attempt in self.attempts.values():
                admission_role = attempt.get("role")
                admitted_at = attempt.get("startedAtMs")
                ordinal = attempt.get("attempt")
                if (
                    isinstance(admission_role, str)
                    and isinstance(admitted_at, (int, float))
                    and is_json_integer(ordinal)
                ):
                    by_role.setdefault(admission_role, []).append((admitted_at, int(ordinal)))
            for admission_role, admissions in by_role.items():
                if any(
                    earlier[0] < later[0] and earlier[1] > later[1] for earlier, later in pairwise(sorted(admissions))
                ):
                    self.error(f"attempt ordinals contradict the admission order for {admission_role}", "PLACEMENT")
        selected = self.mapping(value.get("selectedRequests"), "selectedRequests") if validate_behavior else {}
        if validate_behavior and not selected:
            self.error("selectedRequests must not be empty")
        seen: set[str] = set()
        for role, request_id in selected.items():
            if not isinstance(request_id, str) or request_id not in self.attempts:
                self.error(f"selectedRequests.{role} references a missing attempt")
                continue
            if request_id in seen:
                self.error("distinct selected roles must bind distinct original requests", "PLACEMENT")
            seen.add(request_id)
            attempt = self.attempts[request_id]
            if attempt.get("role") != role or attempt.get("attempt") != 1:
                self.error(f"selectedRequests.{role} is a retry or a substituted role", "PLACEMENT")
            if (
                self.standard_behavior
                and role not in ("resume", "recovery")
                and self.value.get("requirementId") != "lmi-lifecycle-5"
                and any(
                    other.get("executionArn") == attempt.get("executionArn")
                    and self.number(other.get("startedAtMs"), "prior attempt.startedAtMs")
                    < self.number(attempt.get("startedAtMs"), "selected attempt.startedAtMs")
                    for other in self.attempts.values()
                )
            ):
                self.error(f"selectedRequests.{role} is not the first invocation of its execution", "PLACEMENT")
            if (
                self.standard_behavior
                and self.value.get("requirementId") == "lmi-lifecycle-4"
                and (role == "victim" or role.startswith("peer"))
                and any(
                    other is not attempt
                    and other.get("executionArn") == attempt.get("executionArn")
                    and other.get("startedAtMs") == attempt.get("startedAtMs")
                    for other in self.attempts.values()
                )
            ):
                self.error(f"tied native admissions cannot establish the original {role} invocation")
            self.selected[role] = attempt
        self.events = self.records(value.get("events"), "events", nonempty=True)
        for event in self.events:
            kind = self.string(event.get("kind"), "event.kind")
            self.owned_record(event, "event", observation_end)
            if validate_behavior and kind == "admission_probe":
                probe = event.get("probe")
                if probe not in ("operation", "checkpoint"):
                    self.error("admission_probe.probe must be operation or checkpoint")
                if event.get("outcome") not in ("ACCEPTED", "REJECTED"):
                    subject = f"{probe} probe" if probe in ("operation", "checkpoint") else "admission probe"
                    self.error(f"{subject} outcome must be ACCEPTED or REJECTED")
            if kind in {"observation_error", "control_error"}:
                self.error(f"{kind} reports failed observation or control publication")
        sdk_requests = self.records(value.get("sdkRequests"), "sdkRequests")
        for request in sdk_requests:
            self.owned_record(request, "sdkRequest", observation_end)
            self.string(request.get("method"), "sdkRequest.method")
            if request.get("outcome") not in ("DISPATCHED", "REJECTED"):
                self.error("sdkRequest.outcome must be DISPATCHED or REJECTED")
            if validate_behavior and "updates" in request:
                for update in self.records(request["updates"], "sdkRequest.updates"):
                    for field in ("id", "type", "action"):
                        self.string(update.get(field), f"sdkRequest.updates.{field}")
        if validate_behavior:
            for event in self.events:
                call_kind = event.get("kind")
                method = _CALL_METHODS.get(call_kind) if isinstance(call_kind, str) else None
                if method is None:
                    continue
                outcome = event.get("outcome", "DISPATCHED")
                if outcome not in ("DISPATCHED", "REJECTED"):
                    self.error("observed SDK call outcome must be DISPATCHED or REJECTED")
                    continue
                if not _has_matching_sdk_request(event, method, outcome, sdk_requests):
                    self.error("observed SDK call lacks a unique matching SDK request")
        for fault in self.records(value.get("faultsCreated"), "faultsCreated"):
            if "releasedAtMs" in fault:
                self.number(fault["releasedAtMs"], "faultsCreated.releasedAtMs")
        self.histories(check_expectations=validate_behavior)
        collection_errors = value.get("collectionErrors", [])
        if not isinstance(collection_errors, list):
            self.error("collectionErrors must be an array of non-empty strings")
        else:
            for index, error in enumerate(collection_errors):
                message = self.string(error, f"collectionErrors[{index}]")
                if message:
                    self.error(f"collectionErrors[{index}]: {message}")
        collection = self.mapping(value.get("collection"), "collection")
        watermarks = self.mapping(collection.get("watermarks"), "collection.watermarks")
        for source in ("lifecycle", "sdkRequests", "history"):
            if self.number(watermarks.get(source), f"collection.watermarks.{source}") < observation_end:
                self.error(f"{source} collection does not cover the observation window")
        references = collection.get("evidenceUris")
        if not isinstance(references, list) or not references:
            self.error("collection.evidenceUris must retain the raw evidence references")
        else:
            for index, uri in enumerate(references):
                self.string(uri, f"collection.evidenceUris[{index}]")
        if validate_behavior:
            self.no_late_dispatch()

    def owned_record(self, record: Mapping[str, Any], kind: str, end: float) -> None:
        request_id = record.get("requestId")
        if not isinstance(request_id, str) or request_id not in self.attempts:
            self.error(f"{kind} references an unrecorded request")
            return
        if record.get("ownerRequestId") != request_id:
            self.error(f"{kind} is attributed to a different invocation", "ASSERTION")
        if record.get("executionArn") != self.attempts[request_id].get("executionArn"):
            self.error(f"{kind} is attributed to a different execution", "ASSERTION")
        time = self.number(record.get("atMs"), f"{kind}.atMs")
        if time > end or time < self.attempts[request_id].get("startedAtMs", 0):
            self.error(f"{kind} lies outside its observation window")
        observed_during = record.get("observedDuringRequestId")
        if observed_during is not None:
            if not isinstance(observed_during, str) or observed_during not in self.attempts:
                self.error(f"{kind}.observedDuringRequestId references an unrecorded request")
            else:
                observed_attempt = self.attempts[observed_during]
                start = self.number(observed_attempt.get("startedAtMs"), "observed attempt.startedAtMs")
                finish = self.number(observed_attempt.get("endedAtMs"), "observed attempt.endedAtMs")
                if not start <= time <= finish:
                    self.error(f"{kind} lies outside its observedDuringRequestId interval", "ASSERTION")

    def histories(self, *, check_expectations: bool = True) -> None:
        expected = self.mapping(self.value.get("expectedHistories"), "expectedHistories") if check_expectations else {}
        actual: dict[str, Mapping[str, Any]] = {}
        for history in self.records(self.value.get("histories"), "histories", nonempty=True):
            arn = self.string(history.get("executionArn"), "history.executionArn")
            if arn in actual:
                self.error(f"duplicate service history {arn}")
            actual[arn] = history
            if history.get("status") not in HISTORY_STATUSES:
                self.error(f"history {arn}.status must be a recognized execution status")
            if "startedAtMs" in history:
                started = self.number(history["startedAtMs"], f"history {arn}.startedAtMs")
                admissions: list[float] = []
                for attempt in self.attempts.values():
                    admitted = attempt.get("startedAtMs")
                    if (
                        attempt.get("executionArn") == arn
                        and isinstance(admitted, (int, float))
                        and not isinstance(admitted, bool)
                    ):
                        admissions.append(float(admitted))
                if admissions and started > min(admissions):
                    self.error(f"history {arn}.startedAtMs follows its first native admission")
            if history.get("ownerCaseId") != self.value.get("caseId"):
                self.error(f"history {arn} belongs to a different case", "ASSERTION")
            operations = self.records(history.get("operations"), "history.operations")
            ids: set[str] = set()
            for operation in operations:
                self.operation_shape(operation, "history operation", check_interval=True)
                operation_id = operation.get("id")
                if operation_id in ids:
                    self.error("duplicate operation identity in service history")
                if isinstance(operation_id, str):
                    ids.add(operation_id)
            if check_expectations:
                expectation = self.mapping(expected.get(arn), f"expectedHistories.{arn}")
                allow_running = False
                if ("status" in expectation) == ("allowedStatuses" in expectation):
                    self.error(f"expectedHistories.{arn} must declare exactly one of status or allowedStatuses")
                if "allowedStatuses" in expectation:
                    allowed = expectation["allowedStatuses"]
                    if (
                        not isinstance(allowed, list)
                        or not allowed
                        or any(not isinstance(status, str) or status not in HISTORY_STATUSES for status in allowed)
                        or len(set(allowed)) != len(allowed)
                    ):
                        self.error(
                            f"expectedHistories.{arn}.allowedStatuses must contain distinct recognized execution statuses"
                        )
                    else:
                        allow_running = "RUNNING" in allowed
                        if history.get("status") not in allowed:
                            self.error(
                                f"service history {arn}.status is outside the declared allowedStatuses", "ASSERTION"
                            )
                elif expectation.get("status") not in HISTORY_STATUSES:
                    self.error(f"expectedHistories.{arn}.status must be a recognized execution status")
                if not allow_running and "result" not in expectation and "error" not in expectation:
                    self.error(f"history {arn} needs an independently declared expected result/error")
                for key, item in expectation.items():
                    if key != "allowedStatuses" and (key not in history or not same_json(history[key], item)):
                        self.error(f"service history {arn}.{key} differs from the fixture expectation", "ASSERTION")
        arns = {attempt.get("executionArn") for attempt in self.attempts.values()}
        if set(actual) != arns or (check_expectations and set(expected) != arns):
            self.error("histories and expectedHistories must cover every launched execution exactly")

    def at_or_after(
        self, observation: Mapping[str, Any], boundary: Mapping[str, Any], *, require_sequence: bool = False
    ) -> bool | None:
        at, closed = observation["atMs"], boundary["atMs"]
        if at != closed:
            return at > closed
        sequences = [row["sequence"] for row in (observation, boundary) if "sequence" in row]
        if any(not is_json_integer(seq) or seq < 0 for seq in sequences):
            self.error("same-time admission observation and closure need valid sequence numbers when supplied")
            return None
        if len(sequences) < 2:
            if require_sequence:
                self.error("same-time observations need sequence numbers to establish their order")
                return None
            return True  # Admission evidence otherwise uses an inclusive timestamp boundary.
        if sequences[0] == sequences[1]:
            self.error("same-time admission observation and closure need distinct sequence numbers")
            return None
        return sequences[0] > sequences[1]

    def no_late_dispatch(self) -> None:
        disposal = self.value.get("requirementId") in ("lmi-lifecycle-2", "lmi-lifecycle-7")
        for request_id in self.attempts:
            closures = [
                event
                for event in self.events
                if event.get("requestId") == request_id
                and event.get("kind") in ("wrapper_return", "wrapper_raise", "admission_closed")
                and isinstance(event.get("atMs"), (float, int))
            ]
            if not closures:
                continue
            closed = min(event["atMs"] for event in closures)
            wrapper = None
            if disposal and request_id == self.selected.get("victim", {}).get("requestId"):
                exits = [event for event in closures if event["kind"] != "admission_closed"]
                if len(exits) == 1 and exits[0]["kind"] == "wrapper_return":
                    wrapper = exits[0]
                elif len(exits) > 1:
                    self.error("disposal needs exactly one wrapper exit for the selected victim")
            for request in self.value.get("sdkRequests", []):
                if (
                    isinstance(request, Mapping)
                    and request.get("requestId") == request_id
                    and request.get("outcome") == "DISPATCHED"
                    and isinstance(request.get("atMs"), (float, int))
                    and request["atMs"] >= closed
                ):
                    late_boundaries = [boundary for boundary in closures if self.at_or_after(request, boundary)]
                    if late_boundaries:
                        self.error(f"request {request_id} dispatched SDK work after admission closed", "ASSERTION")
            for event in self.events:
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
                        following = [boundary for boundary in closures if self.at_or_after(event, boundary)]
                        if (
                            probe == "operation"
                            and outcome == "ACCEPTED"
                            and wrapper in following
                            and not any(
                                boundary is not wrapper
                                and (
                                    boundary["atMs"] < wrapper["atMs"]
                                    or (
                                        boundary["atMs"] == wrapper["atMs"]
                                        and self.at_or_after(boundary, wrapper, require_sequence=True) is False
                                    )
                                )
                                for boundary in following
                            )
                        ):
                            continue
                        if not following:
                            continue  # The probe was ordered before every closure in the same millisecond.
                    if outcome != "REJECTED":
                        self.error(f"closed invocation admitted a {probe} probe", "ASSERTION")

    def overlap(self) -> None:
        if not self.require_roles("victim", "peer"):
            return
        victim, peer = self.selected["victim"], self.selected["peer"]
        if victim.get("executionArn") == peer.get("executionArn"):
            self.error("overlap needs distinct durable executions", "PLACEMENT")
        if victim.get("environmentId") != peer.get("environmentId"):
            self.error("selected requests did not overlap in the same environment", "PLACEMENT")
        runtime = self.value.get("runtime")
        if runtime == "javascript" and any(victim.get(key) != peer.get(key) for key in ("workerId", "processId")):
            self.error("JavaScript overlap requires the same Node.js worker and process", "PLACEMENT")
        if runtime == "java" and victim.get("processId") != peer.get("processId"):
            self.error("Java overlap requires the same JVM process", "PLACEMENT")
        if runtime == "python" and victim.get("processId") == peer.get("processId"):
            self.error("Python native overlap requires different processes", "PLACEMENT")
        if self.profile.get("nativeConcurrency", 0) < 2:
            self.error("native overlap requires a deployed concurrency of at least two", "PLACEMENT")
        start = max(victim.get("startedAtMs", 0), peer.get("startedAtMs", 0))
        end = min(victim.get("endedAtMs", 0), peer.get("endedAtMs", 0))
        if start >= end:
            self.error("selected invocation intervals do not overlap", "PLACEMENT")
        for attempt in (victim, peer):
            barrier = self.event(attempt, "barrier_arrived")
            if not start <= barrier.get("atMs", -1) < end:
                self.error("cohort barrier was not established inside actual native overlap", "PLACEMENT")

        if self.value.get("requirementId") == "lmi-lifecycle-1":
            self.overlap_activity()

    def overlap_activity(self) -> None:
        if not self.require_roles("victim", "peer"):
            return
        expectations = self.value["expectedHistories"]
        victim = expectations[self.selected["victim"]["executionArn"]]
        peer = expectations[self.selected["peer"]["executionArn"]]
        if "result" in victim and "result" in peer and same_json(victim["result"], peer["result"]):
            self.error("overlap fixture must declare distinct expected results for the selected executions")
        requests = self.records(self.value.get("sdkRequests"), "sdkRequests")
        histories = self.records(self.value.get("histories"), "histories")
        overlap_start = max(self.selected[role]["startedAtMs"] for role in ("victim", "peer"))
        overlap_end = min(self.selected[role]["endedAtMs"] for role in ("victim", "peer"))
        for role in ("victim", "peer"):
            attempt = self.selected[role]
            if attempt.get("outcome") != "SUCCEEDED" or any(
                other.get("executionArn") == attempt.get("executionArn") and other is not attempt
                for other in self.attempts.values()
            ):
                self.error(f"overlap {role} needs an original successful execution without retry", "ASSERTION")
            arn = attempt.get("executionArn")
            history = next((history for history in histories if history.get("executionArn") == arn), {})
            expectation = expectations[arn]
            markers = [
                operation
                for operation in self.records(history.get("operations"), "overlap history.operations")
                if operation.get("type") == "STEP" and operation.get("name") == "record-marker"
            ]
            if len(markers) != 1 or "result" not in expectation:
                self.error(f"overlap {role} needs exactly one record-marker step and an expected result")
            elif (
                markers[0].get("status") != "SUCCEEDED"
                or "result" not in markers[0]
                or not same_json(markers[0]["result"], expectation["result"])
            ):
                self.error(f"overlap {role} record-marker step does not match the fixture expectation", "ASSERTION")
            elif not any(
                request.get("requestId") == attempt["requestId"]
                and request.get("method") == "CheckpointDurableExecution"
                and request.get("outcome") == "DISPATCHED"
                and overlap_start <= request["atMs"] < overlap_end
                and isinstance(request.get("updates"), list)
                and any(
                    isinstance(update, Mapping)
                    and update.get("id") == markers[0]["id"]
                    and update.get("type") == "STEP"
                    and update.get("action") == "SUCCEED"
                    for update in request["updates"]
                )
                for request in requests
            ):
                self.error(f"overlap {role} lacks an attributed checkpoint dispatch for record-marker during overlap")

    def disposal(self, expected_outcome: str) -> None:
        if not self.require_roles("victim"):
            return
        victim = self.selected["victim"]
        root = self.event(victim, "root_completed")
        wrapper = self.event(victim, "wrapper_return")
        if root.get("outcome") not in ("SUCCEEDED", "FAILED") or root.get("outcome") != victim.get("outcome"):
            self.error("root completion and actual invocation outcome disagree", "ASSERTION")
        if victim.get("outcome") != expected_outcome:
            self.error(f"disposal requires the selected victim outcome {expected_outcome}", "ASSERTION")
        arn = victim.get("executionArn")
        if sum(attempt.get("executionArn") == arn for attempt in self.attempts.values()) == 1:
            history = next(item for item in self.value["histories"] if item["executionArn"] == arn)
            if history.get("status") in TERMINAL_STATUSES and history["status"] != expected_outcome:
                self.error("terminal service history disagrees with the single disposal invocation", "ASSERTION")
        if root.get("atMs", 0) > wrapper.get("atMs", 0):
            self.error("wrapper return precedes root completion", "ASSERTION")
        if wrapper:
            self.require_probes(victim, wrapper, "checkpoint")
        exited = self.event(victim, "task_exit")
        if exited.get("atMs", -1) < root.get("atMs", 0):
            self.error("task exit preceded root completion in the disposal fixture", "ASSERTION")

    def require_probes(self, victim: Mapping[str, Any], boundary: Mapping[str, Any], *probes: str) -> None:
        # Outcomes are checked separately; only probes at or after this scenario's boundary count as coverage.
        for probe in probes:
            if not any(
                event.get("requestId") == victim.get("requestId")
                and event.get("kind") == "admission_probe"
                and event.get("probe") == probe
                and "atMs" in boundary
                and self.at_or_after(event, boundary)
                for event in self.events
            ):
                self.error(f"missing post-closure {probe} admission probe")

    def observation_grace(self, boundary: float) -> None:
        budgets = self.mapping(self.value.get("budgets"), "budgets")
        grace = self.number(budgets.get("observationGraceMs"), "budgets.observationGraceMs")
        if self.value.get("observationEndedAtMs", 0) < boundary + grace:
            self.error(
                "observationEndedAtMs does not cover the declared observationGraceMs after the scenario boundary"
            )

    def deadline(self, *, check_grace: bool = True) -> None:
        if not self.require_roles("victim"):
            return
        victim = self.selected["victim"]
        timeout = self.event(victim, "platform_timeout")
        inflight = self.event(victim, "work_in_flight")
        closed = self.event(victim, "admission_closed")
        exited = self.event(victim, "task_exit")
        if inflight and exited:
            controlled = self.string(inflight.get("taskId"), "work_in_flight.taskId")
            finished = self.string(exited.get("taskId"), "task_exit.taskId")
            if controlled and finished and controlled != finished:
                self.error("deadline task exit is not the controlled in-flight task", "ASSERTION")
            for event in normalize_retirement_events(self.events, self.errors):
                if event.get("requestId") != victim.get("requestId") or event.get("taskId") != controlled:
                    continue
                if (
                    event.get("kind") == "task_start"
                    and self.at_or_after(inflight, event, require_sequence=True) is False
                ):
                    self.error("controlled task started after it was observed in flight", "ASSERTION")
                if event.get("kind") == "task_exit" and event is not exited:
                    self.error("deadline controlled task must have exactly one recorded exit")
        time = timeout.get("atMs", 0)
        if time < victim.get("startedAtMs", 0) + self.profile.get("timeoutMs", 0):
            self.error("platform timeout did not cross the configured invocation deadline", "ASSERTION")
        if time > victim.get("endedAtMs", 0):
            self.error("platform timeout follows native invocation completion", "ASSERTION")
        if victim.get("outcome") != "TIMED_OUT" or timeout.get("source") != "platform":
            self.error("deadline requires an observed platform timeout, not a simulated cancellation", "ASSERTION")
        ordering = {
            kind: self.at_or_after(event, timeout, require_sequence=True)
            if "atMs" in event and "atMs" in timeout
            else None
            for kind, event in (("inflight", inflight), ("admissionMs", closed), ("cleanupMs", exited))
        }
        if ordering["inflight"] is True or ordering["cleanupMs"] is False:
            self.error("controlled work was not in flight across the real deadline", "ASSERTION")
        budgets = self.mapping(self.value.get("budgets"), "budgets")
        for field, event in (("admissionMs", closed), ("cleanupMs", exited)):
            budget = self.number(budgets.get(field), f"budgets.{field}")
            if ordering[field] is False or event.get("atMs", time) > time + budget:
                self.error(f"deadline {field} exceeds the declared fixture budget or precedes the timeout", "ASSERTION")
        if check_grace:
            self.observation_grace(time)
        self.require_probes(victim, closed, "operation", "checkpoint")
        for fault in self.records(self.value.get("faultsCreated"), "faultsCreated"):
            release = fault.get("releasedAtMs")
            if isinstance(release, (int, float)) and release <= exited.get("atMs", 0):
                self.error("fault release supplied the cleanup being asserted", "ASSERTION")

    def recovery(self) -> None:
        self.overlap()
        self.deadline(check_grace=False)
        if not self.require_roles("victim", "peer", "recovery"):
            return
        victim, recovery = self.selected["victim"], self.selected["recovery"]
        peers = [attempt for role, attempt in self.selected.items() if role.startswith("peer")]
        timeout = self.event(victim, "platform_timeout").get("atMs", 0)
        recovered = self.event(recovery, "worker_recovered").get("atMs", 0)
        self.observation_grace(recovered)
        if recovery.get("environmentId") != victim.get("environmentId"):
            self.error("replacement environment cannot establish worker recovery", "PLACEMENT")
        policy = self.profile.get("recoveryPolicy")
        if policy == "reuse-worker":
            if any(recovery.get(key) != victim.get(key) for key in ("workerId", "processId")):
                self.error("recovery substituted a different worker/process", "PLACEMENT")
        elif policy == "replace-worker":
            if recovery.get("replacesWorkerId") != victim.get("workerId") or recovery.get("workerId") == victim.get(
                "workerId"
            ):
                self.error("replacement must identify the original failed worker capacity", "PLACEMENT")
        else:
            self.error("profile.recoveryPolicy must explicitly select reuse-worker or replace-worker")
        if any(
            attempt is not recovery
            and attempt.get("executionArn") == recovery.get("executionArn")
            and attempt["startedAtMs"] == recovery["startedAtMs"]
            for attempt in self.attempts.values()
        ):
            self.error("tied native admissions cannot establish the original recovery invocation")
        if recovery.get("outcome") != "SUCCEEDED":
            self.error("recovered capacity did not complete the selected recovery request", "ASSERTION")
        if any(
            recovery.get("executionArn") == attempt.get("executionArn")
            and (attempt in (victim, *peers) or attempt["startedAtMs"] < recovery["startedAtMs"])
            for attempt in self.attempts.values()
        ):
            self.error("recovery must be an independent request, not a service retry", "PLACEMENT")
        if recovery.get("startedAtMs", 0) < timeout or recovered < recovery.get("startedAtMs", 0):
            self.error("recovery preceded the selected victim's timeout", "ASSERTION")
        # Ordering alone does not bound the measurement: requirement 4 declares
        # recoveryMs, so the observed recovery must fit inside it.
        recovery_budget = self.number(
            self.mapping(self.value.get("budgets"), "budgets").get("recoveryMs"), "budgets.recoveryMs"
        )
        if not timeout <= recovered <= timeout + recovery_budget:
            self.error("recovery exceeded the declared recoveryMs budget", "ASSERTION")
        if len(peers) != self.profile.get("nativeConcurrency", 0) - 1:
            self.error("recovery must occupy every other native capacity slot", "PLACEMENT")
        if self.value.get("runtime") == "python":
            processes = [victim.get("processId"), *(peer.get("processId") for peer in peers)]
            if len(set(processes)) != len(processes):
                self.error("recovery peers must occupy distinct Python processes", "PLACEMENT")
            if any(recovery.get("processId") == peer.get("processId") for peer in peers):
                self.error("recovery cannot use an occupied Python peer process", "PLACEMENT")
            if recovery.get("processId") == victim.get("processId"):
                if victim.get("endedAtMsSource") != "platform" or recovery.get("startedAtMsSource") != "platform":
                    self.error("same-process Python recovery requires platform native completion and admission")
                elif recovery["startedAtMs"] < victim["endedAtMs"]:
                    self.error("Python recovery preceded the victim's native invocation completion", "PLACEMENT")
        for peer in peers:
            if peer.get("environmentId") != victim.get("environmentId") or not peer.get(
                "startedAtMs", 0
            ) < timeout < recovered <= peer.get("endedAtMs", 0):
                self.error("established peer capacity was not held through recovery", "PLACEMENT")
            progress = [
                event.get("atMs", 0)
                for event in self.events
                if event.get("kind") == "peer_progress" and event.get("requestId") == peer.get("requestId")
            ]
            if not any(time < timeout for time in progress) or not any(
                timeout < time <= recovered for time in progress
            ):
                self.error("healthy peer lacks independent progress across the timeout", "ASSERTION")
        for fault in self.records(self.value.get("faultsCreated"), "faultsCreated"):
            if isinstance(fault.get("releasedAtMs"), (int, float)) and fault["releasedAtMs"] <= recovered:
                self.error("fault release cannot satisfy worker recovery", "ASSERTION")

    def replay(self) -> None:
        if not self.require_roles("victim", "resume"):
            return
        victim, resume = self.selected["victim"], self.selected["resume"]
        pending = self.event(victim, "pending")
        wrapper = self.event(victim, "wrapper_return")
        if victim.get("outcome") != "PENDING" or victim.get("executionArn") != resume.get("executionArn"):
            self.error("resume must continue the original suspended execution", "ASSERTION")
        if not pending.get("atMs", 0) <= wrapper.get("atMs", 0) < resume.get("startedAtMs", 0):
            self.error("resume did not follow a real PENDING wrapper return", "ASSERTION")
        replay = self.mapping(self.value.get("replay"), "replay")
        before = self.records(replay.get("before"), "replay.before", nonempty=True)
        after = self.records(replay.get("after"), "replay.after", nonempty=True)
        after_by_id = {operation.get("id"): operation for operation in after}
        same_execution = [
            attempt for attempt in self.attempts.values() if attempt.get("executionArn") == victim.get("executionArn")
        ]
        if any(attempt["startedAtMs"] < victim["startedAtMs"] for attempt in same_execution):
            self.error("replay victim is not the original invocation of the selected execution", "PLACEMENT")
        if any(attempt is not victim and attempt["startedAtMs"] == victim["startedAtMs"] for attempt in same_execution):
            self.error("tied native admissions cannot establish the original replay invocation")
        pending_requests = {attempt["requestId"] for attempt in same_execution if attempt.get("outcome") == "PENDING"}
        suspending_requests = {
            event["requestId"]: event["atMs"]
            for event in self.events
            if event.get("kind") == "wrapper_return"
            and event.get("requestId") in pending_requests
            and any(later["startedAtMs"] > event["atMs"] for later in same_execution)
        }
        if not any(
            request.get("requestId") in suspending_requests
            and request.get("method") == "CheckpointDurableExecution"
            and request.get("outcome") == "DISPATCHED"
            and self.attempts[request["requestId"]]["startedAtMs"]
            <= request["atMs"]
            <= suspending_requests[request["requestId"]]
            and isinstance(request.get("updates"), list)
            and any(
                isinstance(update, Mapping)
                and update.get("type") in ("WAIT", "CALLBACK")
                and update.get("action") == "START"
                and isinstance(update.get("id"), str)
                and after_by_id.get(update["id"], {}).get("type") == update["type"]
                and after_by_id[update["id"]].get("status") == "SUCCEEDED"
                and "endedAtMs" in after_by_id[update["id"]]
                and suspending_requests[request["requestId"]] < after_by_id[update["id"]]["endedAtMs"]
                and any(later["startedAtMs"] >= after_by_id[update["id"]]["endedAtMs"] for later in same_execution)
                for update in request["updates"]
            )
            for request in self.value["sdkRequests"]
        ):
            self.error("replay needs an observed wait or callback checkpoint followed by suspension and resume")
        if any(not same_json(after_by_id.get(operation.get("id")), operation) for operation in before):
            self.error("replay changed completed operation identity, result, or failure", "ASSERTION")
        ids: set[str] = set()
        for operation in before:
            operation_id = operation.get("id")
            if operation_id in ids:
                self.error("replay.before repeats an operation identity")
            if isinstance(operation_id, str):
                ids.add(operation_id)
        final_history = next(
            history for history in self.value["histories"] if history["executionArn"] == victim["executionArn"]
        )
        if not same_json(final_history.get("operations"), after):
            self.error("replay.after differs from the collected service history", "ASSERTION")
        statuses = {operation.get("status") for operation in before}
        if not {"SUCCEEDED", "FAILED"} <= statuses:
            self.error("replay must preserve both a completed success and a completed failure")
        completed = {operation.get("id") for operation in before}
        if any(
            operation.get("status") in COMPLETED_OPERATION_STATUSES
            and "endedAtMs" in operation
            and operation["endedAtMs"] <= pending.get("atMs", 0)
            and operation.get("id") not in completed
            for operation in after
        ):
            self.error("replay.before omits an operation known to have completed before suspension")
        # Operations completed before the first resume stay completed for every
        # later invocation of the same execution, and a body without external
        # effects is invisible to the side-effect counts. So no request after
        # the victim's on this execution may run them, not only the selected
        # first resume.
        later = {resume.get("requestId")} | {
            request_id
            for request_id, attempt in self.attempts.items()
            if attempt.get("executionArn") == victim.get("executionArn") and request_id != victim.get("requestId")
        }
        executions = self.records(replay.get("operationExecutions"), "replay.operationExecutions", nonempty=True)
        for execution in executions:
            request_id = self.string(execution.get("requestId"), "replay.operationExecutions.requestId")
            operation_id = self.string(execution.get("operationId"), "replay.operationExecutions.operationId")
            if request_id not in self.attempts:
                self.error("operationExecutions references an unrecorded request")
            if request_id in later and operation_id in completed:
                self.error("resume repeated completed user code", "ASSERTION")
        if any(
            event.get("kind") == "body"
            and event.get("operationId") in completed
            and (
                event.get("requestId") in later
                or (
                    event.get("requestId") == victim.get("requestId")
                    and "atMs" in pending
                    and self.at_or_after(event, pending, require_sequence=True) is True
                )
            )
            for event in self.events
        ):
            self.error("resume repeated completed user code", "ASSERTION")
        if any(
            operation.get("type") == "STEP" and operation.get("endedAtMs", 0) > pending.get("atMs", 0)
            for operation in before
        ):
            self.error("replay.before contains an operation that completed after suspension", "ASSERTION")
        completed_steps = [
            operation
            for operation in before
            if operation.get("type") == "STEP" and operation.get("status") in COMPLETED_OPERATION_STATUSES
        ]
        if not completed_steps:
            self.error("replay requires a completed user-code step before suspension")
        for operation in completed_steps:
            operation_id = operation.get("id")
            has_entry = any(
                item.get("requestId") == victim.get("requestId") and item.get("operationId") == operation_id
                for item in executions
            )
            timed_bodies = [
                event
                for event in self.events
                if event.get("kind") == "body"
                and event.get("requestId") == victim.get("requestId")
                and event.get("operationId") == operation_id
            ]
            if not has_entry or not any(
                "atMs" in pending
                and self.at_or_after(event, pending, require_sequence=True) is False
                and event["atMs"] <= operation.get("endedAtMs", event["atMs"])
                for event in timed_bodies
            ):
                self.error(
                    "completed replay step lacks observed user-code execution on the original request before suspension"
                )
            if "endedAtMs" in operation and any(event["atMs"] > operation["endedAtMs"] for event in timed_bodies):
                self.error("completed replay step body occurred after recorded completion", "ASSERTION")
        retry = self.mapping(replay.get("retry"), "replay.retry")
        retry_id = self.string(retry.get("operationId"), "replay.retry.operationId")
        retry_attempts = self.records(retry.get("attempts"), "replay.retry.attempts", nonempty=True)
        retry_attempt_ids = [self.string(item.get("attemptId"), "retry.attemptId") for item in retry_attempts]
        if len(retry_attempt_ids) < 2 or len(set(retry_attempt_ids)) != len(retry_attempt_ids):
            self.error("retry requires distinct observed attempts")
        if retry_attempts and (
            retry_attempts[0].get("outcome") != "FAILED" or retry_attempts[-1].get("outcome") != "SUCCEEDED"
        ):
            self.error("retry must demonstrate a failed attempt followed by success", "ASSERTION")
        for retry_attempt in retry_attempts:
            self.string(retry_attempt.get("outcome"), "retry.outcome")
            if retry_attempt.get("requestId") not in self.attempts:
                self.error("retry references an unrecorded request")
            elif self.attempts[retry_attempt["requestId"]].get("executionArn") != victim.get("executionArn"):
                self.error("retry attempt did not run on the suspended execution", "ASSERTION")
            if not any(
                item.get("operationId") == retry_id
                and item.get("attemptId") == retry_attempt.get("attemptId")
                and item.get("requestId") == retry_attempt.get("requestId")
                for item in executions
            ):
                self.error("retry attempt lacks observed user-code execution")
        observed_retry_bodies = [
            (item.get("requestId"), item.get("attemptId"))
            for item in executions
            if item.get("operationId") == retry_id
            and item.get("requestId") in self.attempts
            and self.attempts[item["requestId"]].get("executionArn") == victim.get("executionArn")
        ]
        declared_retry_bodies = [(item.get("requestId"), item.get("attemptId")) for item in retry_attempts]
        if len(observed_retry_bodies) != len(declared_retry_bodies) or any(
            observed_retry_bodies.count(body) != 1 for body in declared_retry_bodies
        ):
            self.error("retry body executions must match the declared retry attempts exactly")
        timed_retries: dict[tuple[str, str], Mapping[str, Any]] = {}
        for event in self.events:
            retry_request_id = event.get("requestId")
            if (
                event.get("kind") != "body"
                or event.get("operationId") != retry_id
                or not isinstance(retry_request_id, str)
                or self.attempts[retry_request_id].get("executionArn") != victim.get("executionArn")
            ):
                continue
            attempt_id = event.get("attemptId")
            if attempt_id is None:
                attempt_number = event.get("attempt")
                if is_json_integer(attempt_number) and attempt_number >= 1:
                    attempt_id = f"{retry_request_id}:{int(attempt_number)}"
            if not isinstance(attempt_id, str) or not attempt_id.strip():
                self.error("observed retry body does not match a declared attempt")
                continue
            identity = (retry_request_id, attempt_id)
            if identity not in declared_retry_bodies:
                self.error("observed retry body does not match a declared attempt")
            elif identity in timed_retries:
                self.error("retry attempt has multiple timed user-code entries")
            else:
                timed_retries[identity] = event
        timed_retry_bodies = [
            timed_retries[identity] for identity in declared_retry_bodies if identity in timed_retries
        ]
        last_by_request: dict[str, Mapping[str, Any]] = {}
        invalid_order = any(
            earlier["atMs"] > later_body["atMs"] for earlier, later_body in pairwise(timed_retry_bodies)
        )
        for body in timed_retry_bodies:
            request_id = body["requestId"]
            earlier = last_by_request.get(request_id)
            if earlier is not None and self.at_or_after(body, earlier, require_sequence=True) is False:
                invalid_order = True
            last_by_request[request_id] = body
        if invalid_order:
            self.error("retry attempt order contradicts timed user-code entries", "ASSERTION")
        latest_admission = 0
        for declared_request, declared_attempt in declared_retry_bodies:
            if not isinstance(declared_request, str) or not isinstance(declared_attempt, str):
                continue
            owner = self.attempts.get(declared_request)
            if owner is not None:
                latest_admission = max(latest_admission, owner["startedAtMs"])
            timed_body = timed_retries.get((declared_request, declared_attempt))
            if timed_body is not None and timed_body["atMs"] < latest_admission:
                self.error("retry body precedes a prior attempt's admission", "ASSERTION")
        # replay.after equals the collected final history (checked above), so a
        # retry that never checkpointed cannot satisfy the retry semantics.
        retried = next((operation for operation in after if operation.get("id") == retry_id), None)
        if retried is None or retried.get("status") != "SUCCEEDED":
            self.error("the retried operation must succeed in the collected final history", "ASSERTION")
        elif "result" not in retried:
            self.error("the retried operation must preserve its stored result", "INCOMPLETE")
        if (
            retried is not None
            and "endedAtMs" in retried
            and any(body["atMs"] > retried["endedAtMs"] for body in timed_retry_bodies)
        ):
            self.error("timed retry body occurred after the recorded operation completion", "ASSERTION")
        if (
            retried is not None
            and "endedAtMs" in retried
            and any(
                item.get("requestId") in self.attempts
                and self.attempts[item["requestId"]]["startedAtMs"] > retried["endedAtMs"]
                for item in retry_attempts
            )
        ):
            self.error("retry owner was admitted after the recorded operation completion", "ASSERTION")
        if retry_id in completed:
            self.error("retry must not repeat an already completed operation", "ASSERTION")
        counts = self.records(replay.get("sideEffects"), "replay.sideEffects", nonempty=True)
        effect_ids = {self.string(item.get("operationId"), "replay.sideEffects.operationId") for item in counts}
        if len(effect_ids) != len(counts):
            self.error("side-effect counts must be unique per operation")
        if not completed | {retry_id} <= effect_ids:
            self.error("side-effect counts must cover stored successes, failures, and retries")
        for count in counts:
            actual = count.get("count")
            expected = count.get("expectedCount")
            if not is_json_integer(actual) or not is_json_integer(expected) or actual < 0 or expected < 0:
                self.error("side-effect counts must be non-negative integers")
            elif actual != expected:
                self.error("side-effect count differs from the fixture's retry policy", "ASSERTION")

    def warm_reuse(self) -> None:
        if not self.require_roles("victim", "warm"):
            return
        victim, warm = self.selected["victim"], self.selected["warm"]
        if victim.get("endedAtMsSource") != "platform":
            self.error("warm reuse requires the victim's platform native completion time")
        if warm.get("startedAtMsSource") != "platform":
            self.error("warm reuse requires the warm request's platform native start time")
        closed = self.event(victim, "wrapper_return")
        if any(victim.get(key) != warm.get(key) for key in ("environmentId", "workerId", "processId")):
            self.error("warm reuse must identify the original process and worker", "PLACEMENT")
        if warm.get("startedAtMs", 0) <= closed.get("atMs", 0) or warm.get("startedAtMs", 0) < victim.get(
            "endedAtMs", 0
        ):
            self.error("warm invocation was not sequential reuse", "PLACEMENT")
        late = self.event(victim, "late_activity")
        if late.get("observedDuringRequestId") != warm.get("requestId") or not warm.get("startedAtMs", 0) <= late.get(
            "atMs", -1
        ) <= warm.get("endedAtMs", 0):
            self.error("late activity was not observed during the selected warm invocation", "ASSERTION")
        exited = self.event(victim, "task_exit")
        if exited.get("atMs", 0) < late.get("atMs", 0):
            self.error("late activity continued after the recorded task exit", "ASSERTION")

    def last_activity(self, request_id: Any) -> float:
        """The latest recorded time of an invocation, its events and SDK requests."""
        sdk_requests = self.value.get("sdkRequests", [])
        records = [*self.events, *(sdk_requests if isinstance(sdk_requests, list) else [])]
        times = [self.attempts.get(request_id, {}).get("endedAtMs")]
        times += [
            record.get("atMs")
            for record in records
            if isinstance(record, Mapping) and record.get("requestId") == request_id
        ]
        valid = [
            float(value)
            for value in times
            if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
        ]
        return max(valid, default=0.0)

    def observed_retirement_work(self, retirement: Mapping[str, Any], end: float) -> None:
        events = normalize_retirement_events(self.events, self.errors)
        settled_by_request = {
            item.get("id"): item.get("settledAtMs")
            for item in self.records(retirement.get("resources"), "retirement.resources")
            if item.get("kind") == "invocation"
        }
        completions = normalize_retirement_events(
            self.records(retirement.get("completions", []), "retirement.completions"), self.errors
        )
        for completion in completions:
            self.owned_record(completion, "retirement.completion", end)
            if self.number(completion.get("atMs"), "retirement.completion.atMs") <= self.value.get(
                "observationEndedAtMs", 0
            ):
                self.error("retirement-only completion must follow the behavior observation window")
        unmatched = set(range(len(completions)))
        for start_kind, end_kind, identity in _WORK_INTERVALS:
            starts = [event for event in events if event.get("kind") == start_kind]
            ends = [event for event in events if event.get("kind") == end_kind]
            ends.extend(completion for completion in completions if completion.get("kind") == end_kind)
            seen: set[tuple[str, str]] = set()
            for start in starts:
                request_id = self.string(start.get("requestId"), f"{start_kind}.requestId")
                work_id = self.string(start.get(identity), f"{start_kind}.{identity}")
                key = (request_id, work_id)
                candidates = [
                    item for item in ends if item.get("requestId") == request_id and item.get(identity) == work_id
                ]
                rejected = start_kind in _CALL_METHODS and start.get("outcome") == "REJECTED"
                if key in seen or (len(candidates) != 1 and not (rejected and not candidates)):
                    self.error(f"retirement lacks a unique completed {start_kind}/{end_kind} interval")
                elif candidates:
                    completion = candidates[0]
                    if self.at_or_after(completion, start, require_sequence=True) is False or completion[
                        "atMs"
                    ] > self.number(settled_by_request.get(request_id), "invocation.settledAtMs"):
                        self.error(f"retirement {start_kind}/{end_kind} interval lies outside settlement")
                    unmatched.difference_update([index for index in unmatched if completions[index] is completion])
                seen.add(key)
        if unmatched:
            self.error("retirement-only completion has no unique observed work start")

    def retirement(self, *, audit_observed_work: bool = True) -> None:
        retirement = self.mapping(self.value.get("retirement"), "retirement")
        end = self.number(retirement.get("retiredAtMs"), "retirement.retiredAtMs")
        if end < self.value.get("observationEndedAtMs", 0):
            self.error("retirement precedes completion of assertions")
        if audit_observed_work or "completions" in retirement:
            self.observed_retirement_work(retirement, end)
        created_resources = self.records(self.value.get("resourcesCreated"), "resourcesCreated")
        identities = {
            (item.get("kind"), item.get("id"))
            for item in created_resources
            if item.get("kind") in ("invocation", "execution")
        }
        required = {("invocation", item["requestId"]) for item in self.attempts.values()}
        required.update(("execution", item["executionArn"]) for item in self.attempts.values())
        if required != identities:
            self.error("resourcesCreated must match observations for every launched invocation and execution")
        for created_key, settled_key, time_key in (
            ("resourcesCreated", "resources", "settledAtMs"),
            ("faultsCreated", "faults", "releasedAtMs"),
        ):
            created = self.records(self.value.get(created_key), created_key)
            settled = self.records(retirement.get(settled_key), f"retirement.{settled_key}")
            created_ids = [self.string(item.get("id"), f"{created_key}.id") for item in created]
            settled_ids = [self.string(item.get("id"), f"retirement.{settled_key}.id") for item in settled]
            if (
                len(set(created_ids)) != len(created_ids)
                or len(set(settled_ids)) != len(settled_ids)
                or set(created_ids) != set(settled_ids)
            ):
                self.error(f"retirement must settle every owned {settled_key} exactly once")
            for item in (*created, *settled):
                if item.get("ownerCaseId") != self.value.get("caseId"):
                    self.error("cleanup attempted to change another case's resources", "ASSERTION")
            if settled_key == "faults":
                for fault in created:
                    if (
                        "releasedAtMs" in fault
                        and self.number(fault["releasedAtMs"], "faultsCreated.releasedAtMs") > end
                    ):
                        self.error("observed fault release occurred after retirement ended")
            for item in settled:
                time = self.number(item.get(time_key), f"retirement.{settled_key}.{time_key}")
                if time > end:
                    self.error("retirement ended before owned work settled")
                if settled_key == "resources":
                    self.string(item.get("evidenceUri"), "retirement.resources.evidenceUri")
                    self.string(item.get("outcome"), "retirement.resources.outcome")
                    kind = self.string(item.get("kind"), "retirement.resources.kind")
                    original = next((resource for resource in created if resource.get("id") == item.get("id")), {})
                    if original.get("kind") != kind:
                        self.error("retirement changed an owned resource's kind")
                    if kind not in {"invocation", "execution", "callback", "control"}:
                        self.error(f"retirement has no completion contract for resource kind {kind}")
                    if kind == "execution" and item.get("outcome") not in TERMINAL_STATUSES:
                        self.error("an execution may still resume after retirement")
                    if kind == "execution":
                        histories = self.value.get("histories")
                        observed = next(
                            (
                                history
                                for history in (histories if isinstance(histories, list) else [])
                                if isinstance(history, Mapping) and history.get("executionArn") == item.get("id")
                            ),
                            {},
                        )
                        # An earlier RUNNING or PENDING snapshot may become terminal.
                        if observed.get("status") in TERMINAL_STATUSES and item.get("outcome") != observed["status"]:
                            self.error(
                                "retirement execution outcome differs from the terminal service history", "ASSERTION"
                            )
                        if "startedAtMs" in observed and time < self.number(
                            observed["startedAtMs"], "history execution.startedAtMs"
                        ):
                            self.error("execution settled before its service start")
                        if (
                            self.standard_behavior
                            and self.value.get("requirementId") in ("lmi-lifecycle-2", "lmi-lifecycle-7")
                            and self.selected.get("victim", {}).get("executionArn") == item.get("id")
                            and sum(attempt.get("executionArn") == item.get("id") for attempt in self.attempts.values())
                            == 1
                            and item.get("outcome")
                            != ("SUCCEEDED" if self.value["requirementId"] == "lmi-lifecycle-2" else "FAILED")
                        ):
                            self.error(
                                "terminal retirement outcome disagrees with the single disposal invocation", "ASSERTION"
                            )
                        operations = self.records(observed.get("operations", []), "history.operations")
                        if any(
                            self.number(operation[field], f"history operation.{field}") > time
                            for operation in operations
                            for field in ("startedAtMs", "endedAtMs")
                            if field in operation
                        ):
                            self.error("execution settled before its recorded service operation")
                    if kind == "invocation" and item.get("outcome") != "QUIESCENT":
                        self.error("invocation quiescence is not established")
                    if kind == "invocation" and time < self.last_activity(item.get("id")):
                        self.error("invocation settled before its recorded activity")
                    if kind == "callback" and item.get("outcome") not in ("SUCCEEDED", "FAILED", "TIMED_OUT"):
                        self.error("callback completion is not established")
                    if kind == "control" and item.get("outcome") != "DELETED":
                        self.error("control object deletion is not established")
                if settled_key == "faults":
                    if item.get("mode") not in ("normal", "emergency"):
                        self.error("fault release must record normal or emergency mode")
                    if self.standard_behavior and self.value.get("requirementId") in (
                        "lmi-lifecycle-3",
                        "lmi-lifecycle-4",
                    ):
                        # Only the victim's cleanup and the selected recovery are
                        # asserted; a healthy peer may stay occupied after both.
                        victim = self.selected.get("victim", {})
                        recovery = self.selected.get("recovery", {})
                        asserted = [
                            event.get("atMs", 0)
                            for event in self.events
                            if (event.get("kind") == "task_exit" and event.get("requestId") == victim.get("requestId"))
                            or (
                                recovery
                                and event.get("kind") == "worker_recovered"
                                and event.get("requestId") == recovery.get("requestId")
                            )
                        ]
                        if asserted and time <= max(asserted):
                            self.error("fault release cannot retroactively establish cleanup or recovery", "ASSERTION")


def validate_evidence(evidence: Mapping[str, Any], *, phase: Literal["observations", "final"] = "final") -> list[str]:
    """Validate all observations, then optionally case-owned retirement.

    The coordinator calls ``observations`` before releasing faults and retains
    those errors even if emergency release later succeeds. ``final`` additionally
    requires complete retirement evidence.
    """
    if not isinstance(evidence, Mapping):
        return ["INCOMPLETE: evidence must be an object"]
    if phase not in ("observations", "final"):
        return ["INCOMPLETE: unknown evidence validation phase"]
    validator = _Evidence(evidence)
    try:
        validator.common()
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        validator.error(f"malformed evidence: {error}")
    # Do not interpret malformed structures as behavior observations. Retirement
    # is still checked independently: pre-existing observation errors cannot
    # suppress quiescence checks before the scheduler grants another lease.
    if not validator.errors:
        try:
            _validate_observations(validator, evidence)
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            validator.error(f"malformed evidence: {error}")
    if phase == "final":
        try:
            validator.retirement()
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            validator.error(f"malformed retirement evidence: {error}")
    return validator.errors


def validate_common_evidence(
    evidence: Mapping[str, Any],
    *,
    phase: Literal["observations", "final"] = "final",
    requirement_ids: tuple[str, ...] = REQUIREMENT_IDS,
) -> list[str]:
    """Validate shared ownership and retirement for an explicit scenario catalog.

    Callers must also invoke their catalog's behavior validator. This helper does
    not validate a scenario merely because its ownership envelope is complete.
    """
    if not isinstance(evidence, Mapping):
        return ["INCOMPLETE: evidence must be an object"]
    if phase not in ("observations", "final"):
        return ["INCOMPLETE: unknown evidence validation phase"]
    validator = _Evidence(evidence, requirement_ids, standard_behavior=False)
    try:
        validator.common()
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        validator.error(f"malformed evidence: {error}")
    if phase == "final":
        try:
            validator.retirement()
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            validator.error(f"malformed retirement evidence: {error}")
    return validator.errors


def _validate_observations(validator: _Evidence, evidence: Mapping[str, Any]) -> None:
    requirement = evidence.get("requirementId")
    checks = {
        "lmi-lifecycle-1": validator.overlap,
        "lmi-lifecycle-2": lambda: validator.disposal("SUCCEEDED"),
        "lmi-lifecycle-3": validator.deadline,
        "lmi-lifecycle-4": validator.recovery,
        "lmi-lifecycle-5": validator.replay,
        "lmi-lifecycle-6": validator.warm_reuse,
        "lmi-lifecycle-7": lambda: validator.disposal("FAILED"),
    }
    if requirement in checks:
        checks[requirement]()


def _has_matching_sdk_request(
    event: Mapping[str, Any], method: str, outcome: str, requests: Sequence[Mapping[str, Any]]
) -> bool:
    call_id = event.get("callId")
    return (
        isinstance(call_id, str)
        and bool(call_id.strip())
        and sum(
            request.get("method") == method
            and request.get("outcome") == outcome
            and all(request.get(key) == event.get(key) for key in ("requestId", "callId", "atMs"))
            for request in requests
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


def validate_retirement_evidence(evidence: Mapping[str, Any]) -> list[str]:
    """Audit a separate post-cleanup snapshot without accepting its behavior.

    Failed cases may lack mandatory scenario roles, but every actual launch must
    still be accounted for by the coordinator. This audit reuses common source
    ownership, history shape and resource-inventory checks, then independently
    requires actual wrapper/task/request completion. Neither expected
    business outcomes nor cleanup-time recovery can turn the failed case green.
    """
    if not isinstance(evidence, Mapping):
        return ["INCOMPLETE: retirement evidence must be an object"]
    requirement = evidence.get("requirementId")
    validator = _Evidence(
        evidence, requirement_ids=(requirement,) if isinstance(requirement, str) else (), standard_behavior=False
    )
    try:
        validator.string(requirement, "requirementId")
        validator.common(validate_behavior=False)
        validator.events = normalize_retirement_events(validator.events, validator.errors)
        validator.retirement(audit_observed_work=False)
        if validator.errors:
            return validator.errors
        retirement = evidence["retirement"]
        validator.events.extend(normalize_retirement_events(retirement.get("completions", []), validator.errors))
        records = {item["id"]: item for item in retirement["resources"]}
        histories = {item["executionArn"]: item for item in evidence["histories"]}
        for arn, history in histories.items():
            if history["status"] not in TERMINAL_STATUSES:
                validator.error("retirement service history still permits execution resume")
            if records[arn].get("outcome") != history["status"]:
                validator.error("retirement execution outcome differs from actual service history")
        call_kinds = {method: kind for kind, method in _CALL_METHODS.items()}
        dispatches: set[tuple[str, str, str]] = set()
        for request in evidence["sdkRequests"]:
            if request.get("outcome") != "DISPATCHED":
                continue
            kind = call_kinds.get(request.get("method"))
            if kind is None:
                validator.error("retirement has no completion contract for dispatched SDK method")
                continue
            call_id = validator.string(request.get("callId"), "dispatched SDK request.callId")
            key = (request["requestId"], kind, call_id)
            if key in dispatches or not any(
                event.get("requestId") == key[0]
                and event.get("kind") == kind
                and event.get("callId") == call_id
                and event.get("outcome", "DISPATCHED") == "DISPATCHED"
                and event.get("atMs") == request.get("atMs")
                for event in validator.events
            ):
                validator.error(f"retirement cannot correlate dispatched SDK request with its unique {kind}")
            dispatches.add(key)
        for request_id in validator.attempts:
            local = [event for event in validator.events if event["requestId"] == request_id]
            sequences = [event.get("sequence") for event in local]
            if any(not is_json_integer(sequence) or sequence < 0 for sequence in sequences) or len(sequences) != len(
                set(sequences)
            ):
                validator.error("retirement requires unique per-request semantic sequence numbers")
                continue
            ordered = sorted(local, key=lambda event: event["sequence"])
            if any(first["atMs"] > second["atMs"] for first, second in pairwise(ordered)):
                validator.error("retirement sequence contradicts its normalized timestamps")

            def boundary(event: Mapping[str, Any]) -> tuple[float, int]:
                return event["atMs"], int(event["sequence"])

            wrappers = [event for event in local if event["kind"] in {"wrapper_return", "wrapper_raise"}]
            entries = [event for event in local if event["kind"] == "wrapper_enter"]
            roots = [event for event in local if event["kind"] == "task_exit" and event.get("taskId") == "root"]
            if len(entries) != 1 or len(wrappers) != 1 or len(roots) != 1:
                validator.error(
                    "retirement requires actual entry, wrapper exit and root-task exit for every native request"
                )
            elif any(boundary(entries[0]) > boundary(end) for end in (wrappers[0], roots[0])):
                validator.error("retirement completion precedes actual invocation entry")
            if validator.number(records[request_id].get("settledAtMs"), "invocation.settledAtMs") < max(
                (event["atMs"] for event in local), default=0
            ):
                validator.error("invocation retirement precedes its last actual activity")
            for start_kind, end_kind, identity in _WORK_INTERVALS:
                started = [event for event in local if event["kind"] == start_kind]
                ended = [event for event in local if event["kind"] == end_kind]
                identifiers = {
                    validator.string(event.get(identity), f"{start_kind}.{identity}") for event in started + ended
                }
                for identifier in identifiers:
                    starts = [event for event in started if event.get(identity) == identifier]
                    ends = [event for event in ended if event.get(identity) == identifier]
                    rejected = (
                        start_kind in _CALL_METHODS
                        and len(starts) == 1
                        and starts[0].get("outcome") == "REJECTED"
                        and _has_matching_sdk_request(
                            starts[0], _CALL_METHODS[start_kind], "REJECTED", evidence["sdkRequests"]
                        )
                    )
                    if len(starts) != 1 or (len(ends) != 1 and not (rejected and not ends)):
                        validator.error(f"retirement lacks the unique {start_kind}/{end_kind} completion pair")
                        continue
                    if not ends:
                        continue
                    start, end = starts[0], ends[0]
                    if boundary(start) > boundary(end):
                        validator.error("retirement reverses an observed work interval")

    except (AttributeError, KeyError, TypeError, ValueError, OverflowError) as error:
        validator.error(f"malformed independent retirement evidence: {error}")
    return validator.errors
