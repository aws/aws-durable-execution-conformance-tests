# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Version 1 lifecycle evidence contract and fail-closed observation validators.

See ``evidence-schema.md`` next to this module. Times are milliseconds on one
coordinator-normalized observation clock, not uncorrelated process wall clocks.
The schema is an adapter contract, not proof that an untrusted producer is honest.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
from collections import Counter, deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from decimal import Decimal, InvalidOperation
from itertools import pairwise
from typing import Any, Literal, Protocol, TypeGuard
from urllib.parse import urlsplit

from aws_durable_execution_conformance_tests.lmi.artifacts import case_key, invoke_rejection_proven

SCHEMA_VERSION = 1
REQUIREMENT_IDS = tuple(f"lmi-lifecycle-{index}" for index in range(1, 8))
RUNTIMES = ("javascript", "java", "python")
TERMINAL_STATUSES = ("SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED")
CUTOFF_ACTIVE_STATUSES = ("RUNNING", "PAUSING", "PAUSED")
HISTORY_STATUSES = (*TERMINAL_STATUSES, *CUTOFF_ACTIVE_STATUSES, "PENDING")
RECOVERY_POLICIES = ("reuse-worker", "replace-worker")
COMPLETED_OPERATION_STATUSES = ("SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED")

_WORK_INTERVALS = (
    ("task_start", "task_exit", "taskId"),
    ("io_start", "io_exit", "taskId"),
    ("checkpoint_call", "checkpoint_exit", "callId"),
    ("poll_call", "poll_exit", "callId"),
)
_CALL_METHODS = {"checkpoint_call": "CheckpointDurableExecution", "poll_call": "GetDurableExecutionState"}


class EvidenceValidator(Protocol):
    def __call__(
        self, evidence: Mapping[str, Any], *, phase: Literal["observations", "final"] = "final"
    ) -> Sequence[str]: ...


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


def _decode_json_wire(value: str) -> Any:
    def reject_constant(constant: str) -> Any:
        raise ValueError(f"Not a JSON constant: {constant}")

    try:
        return json.loads(value, parse_constant=reject_constant, parse_float=Decimal, parse_int=Decimal)
    except InvalidOperation as error:
        raise ValueError("JSON number is outside the supported decimal range") from error


def _decimal_identity(value: Decimal) -> tuple[int, str, int]:
    sign, digits, exponent = value.as_tuple()
    coefficient = "".join(map(str, digits)).rstrip("0")
    if not coefficient:
        return 0, "0", 0
    return sign, coefficient, int(exponent) + len(digits) - len(coefficient)


def _json_result_fingerprint(wire: str) -> str:
    """Hash the exact JSON semantics without converting decimals to binary64."""

    def normalize(value: Any) -> Any:
        if isinstance(value, Decimal):
            return ["number", *_decimal_identity(value)]
        if isinstance(value, list):
            return ["array", [normalize(item) for item in value]]
        if isinstance(value, dict):
            return ["object", [[key, normalize(value[key])] for key in sorted(value)]]
        return ["literal", value]

    parsed = _decode_json_wire(wire)
    return hashlib.sha256(json.dumps(normalize(parsed), separators=(",", ":")).encode()).hexdigest()


def _valid_fingerprint(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def _same_execution_result(history: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    if "result" not in history:
        return False
    if "resultFingerprint" not in history:
        return same_json(history["result"], expected["result"])
    try:
        expected_digest = _json_result_fingerprint(json.dumps(expected["result"], allow_nan=False))
    except TypeError, ValueError:
        return False
    return history["resultFingerprint"] == expected_digest and _same_checkpoint_display(
        history["result"], expected["result"]
    )


def _same_checkpoint_display(left: Any, right: Any) -> bool:
    """A lossless digest can reconcile numeric display rounding, not a type change."""
    if (type(left) is int and type(right) is float) or (type(left) is float and type(right) is int):
        try:
            rounded = float(left)
            return math.isfinite(rounded) and rounded == float(right)
        except OverflowError:
            return False
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(_same_checkpoint_display(a, b) for a, b in zip(left, right, strict=True))
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        return left.keys() == right.keys() and all(_same_checkpoint_display(left[key], right[key]) for key in left)
    return same_json(left, right)


def same_checkpoint_result(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    if "result" not in left or "result" not in right:
        return False
    fingerprint = left.get("resultFingerprint")
    if (
        left.get("resultEncoding") == right.get("resultEncoding") == "json"
        and _valid_fingerprint(fingerprint)
        and fingerprint == right.get("resultFingerprint")
    ):
        return _same_checkpoint_display(left["result"], right["result"])
    return same_json(left["result"], right["result"])


def same_checkpoint(left: Any, right: Any) -> bool:
    if not isinstance(left, Mapping) or not isinstance(right, Mapping):
        return False
    if same_checkpoint_result(left, right):
        return same_json(
            {key: value for key, value in left.items() if key != "result"},
            {key: value for key, value in right.items() if key != "result"},
        )
    return same_json(left, right)


def same_checkpoint_set(left: Mapping[Any, Any], right: Mapping[Any, Any]) -> bool:
    return left.keys() == right.keys() and all(
        same_checkpoint(operation, right[identity]) for identity, operation in left.items()
    )


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


def completed_suspension_checkpoint(
    attempts: Sequence[Mapping[str, Any]],
    events: Sequence[Mapping[str, Any]],
    requests: Sequence[Mapping[str, Any]],
    checkpoints: Sequence[Mapping[str, Any]],
) -> bool:
    by_request = {attempt["requestId"]: attempt for attempt in attempts}
    by_id = {operation.get("id"): operation for operation in checkpoints}
    suspensions = {
        event["requestId"]: event["atMs"]
        for event in events
        if event.get("kind") == "wrapper_return"
        and event.get("requestId") in by_request
        and by_request[event["requestId"]].get("outcome") == "PENDING"
        and type(event.get("atMs")) in (int, float)
    }
    for request in requests:
        request_id, at = request.get("requestId"), request.get("atMs")
        if (
            not isinstance(request_id, str)
            or request_id not in suspensions
            or request.get("method") != "CheckpointDurableExecution"
            or request.get("outcome") != "DISPATCHED"
            or type(at) not in (int, float)
            or not by_request[request_id]["startedAtMs"] <= at <= suspensions[request_id]
            or not isinstance(request.get("updates"), list)
        ):
            continue
        for update in request["updates"]:
            if (
                not isinstance(update, Mapping)
                or update.get("type") not in ("WAIT", "CALLBACK")
                or update.get("action") != "START"
                or not isinstance(update.get("id"), str)
            ):
                continue
            operation = by_id.get(update["id"], {})
            ended = operation.get("endedAtMs")
            if (
                operation.get("type") == update["type"]
                and operation.get("status") == "SUCCEEDED"
                and type(ended) in (int, float)
                and suspensions[request_id] < ended
                and any(later["startedAtMs"] >= ended for later in attempts)
            ):
                return True
    return False


def replay_checkpoint_timing(
    before: Sequence[Mapping[str, Any]],
    after: Sequence[Mapping[str, Any]],
    pending_at: int | float,
    observed_ends: Mapping[str, set[int | float]] | None = None,
) -> tuple[bool, bool]:
    """Return whether a completed checkpoint is omitted or ends after suspension."""
    before_ids = {operation.get("id") for operation in before}

    def times(operation: Mapping[str, Any]) -> set[int | float]:
        operation_id = operation.get("id")
        ends = (
            observed_ends.get(operation_id, set())
            if observed_ends is not None and isinstance(operation_id, str)
            else set()
        )
        end = operation.get("endedAtMs")
        return {*ends, end} if isinstance(end, (int, float)) and not isinstance(end, bool) else ends

    omitted = any(
        operation.get("status") in COMPLETED_OPERATION_STATUSES
        and operation.get("id") not in before_ids
        and any(end <= pending_at for end in times(operation))
        for operation in after
    )
    late = any(
        operation.get("type") == "STEP" and any(end > pending_at for end in times(operation)) for operation in before
    )
    return omitted, late


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


def has_overlap_marker_dispatch(
    requests: Sequence[Mapping[str, Any]], request_id: Any, marker_id: Any, start: int | float, end: int | float
) -> bool:
    return any(
        request.get("requestId") == request_id
        and request.get("method") == "CheckpointDurableExecution"
        and request.get("outcome") == "DISPATCHED"
        and isinstance(at := request.get("atMs"), (int, float))
        and not isinstance(at, bool)
        and start <= at < end
        and isinstance(request.get("updates"), list)
        and any(
            isinstance(update, Mapping)
            and update.get("id") == marker_id
            and update.get("type") == "STEP"
            and update.get("action") == "SUCCEED"
            for update in request["updates"]
        )
        for request in requests
    )


def peer_progress_spans_timeout(progress: Sequence[int | float], timeout: int | float, recovered: int | float) -> bool:
    return any(at < timeout for at in progress) and any(timeout < at <= recovered for at in progress)


def completed_replay_step_witness(
    operation_id: Any,
    request_id: Any,
    entries: Sequence[Mapping[str, Any]],
    bodies: Sequence[Mapping[str, Any]],
    before_suspension: Callable[[Mapping[str, Any]], bool],
    ended_at: int | float | None,
) -> bool:
    return any(
        row.get("requestId") == request_id and row.get("operationId") == operation_id for row in entries
    ) and any(
        row.get("kind") == "body"
        and row.get("requestId") == request_id
        and row.get("operationId") == operation_id
        and before_suspension(row)
        and (ended_at is None or row["atMs"] <= ended_at)
        for row in bodies
    )


def retry_body_identity(row: Mapping[str, Any]) -> tuple[str, str] | None:
    request_id, attempt_id = row.get("requestId"), row.get("attemptId")
    if not isinstance(request_id, str) or not request_id.strip():
        return None
    if attempt_id is None and row.get("kind") == "body":
        attempt_number = row.get("attempt")
        if is_json_integer(attempt_number) and attempt_number >= 1:
            attempt_id = f"{request_id}:{int(attempt_number)}"
    if isinstance(attempt_id, str) and attempt_id.strip():
        return request_id, attempt_id
    return None


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
        if key == "allowedStatuses":
            continue
        matches = (
            _same_execution_result(history, expectation)
            if key == "result"
            else key in history and same_json(history[key], item)
        )
        if not matches:
            errors.append(f"ASSERTION: service history {arn}.{key} differs from the fixture expectation")
    return errors


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
        if "resultEncoding" in operation and (
            operation["resultEncoding"] not in ("json", "raw") or "result" not in operation
        ):
            self.error(f"{path}.resultEncoding requires a stored result and json or raw encoding")
        if "resultFingerprint" in operation and (
            operation.get("resultEncoding") != "json" or not _valid_fingerprint(operation["resultFingerprint"])
        ):
            self.error(f"{path}.resultFingerprint requires JSON encoding and a SHA-256 digest")
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
            if outcome_key not in operation and not (outcome_key == "result" and operation.get("type") == "CONTEXT"):
                self.error(f"{path} must preserve the stored {outcome_key}")

    def optional_shapes(self) -> None:
        """Check supplied wire fields without enabling another scenario's behavior."""
        if "scenario" in self.value:
            self.string(self.value["scenario"], "scenario")
        if "coverageProfile" in self.value:
            self.string(self.value["coverageProfile"], "coverageProfile")
            if self.standard_behavior and self.value["coverageProfile"] != "standard":
                self.error("coverageProfile must be standard for shared lifecycle evidence")
        if "recoveryPolicy" in self.profile and self.profile["recoveryPolicy"] not in RECOVERY_POLICIES:
            self.error("profile.recoveryPolicy must be reuse-worker or replace-worker")
        if "budgets" in self.value:
            budgets = self.mapping(self.value["budgets"], "budgets")
            for field in ("admissionMs", "cleanupMs", "recoveryMs", "observationGraceMs"):
                if field in budgets:
                    self.number(budgets[field], f"budgets.{field}")
        if "replay" not in self.value:
            return
        replay = self.mapping(self.value["replay"], "replay")
        for field in ("before", "after", "expectedBefore"):
            if field in replay:
                for operation in self.records(replay[field], f"replay.{field}", nonempty=field != "expectedBefore"):
                    self.operation_shape(operation, f"replay.{field}", completed=field == "before")
        if "projectionErrors" in replay:
            projection_errors = replay["projectionErrors"]
            if not isinstance(projection_errors, list) or any(
                not isinstance(error, str) or not error.strip() for error in projection_errors
            ):
                self.error("replay.projectionErrors must be an array of nonempty strings")
        for field in ("durableWaitObserved", "suspensionCheckpointMatches"):
            if field in replay and type(replay[field]) is not bool:
                self.error(f"replay.{field} must be a boolean")
        if "operationExecutions" in replay:
            for execution in self.records(replay["operationExecutions"], "replay.operationExecutions", nonempty=True):
                for field in ("requestId", "operationId"):
                    self.string(execution.get(field), f"replay.operationExecutions.{field}")
        if "retry" in replay:
            retry = self.mapping(replay["retry"], "replay.retry")
            self.string(retry.get("operationId"), "replay.retry.operationId")
            attempts = self.records(retry.get("attempts"), "replay.retry.attempts")
            missing = replay.get("missingExpectedEffects")
            if len(attempts) < 2 and not (len(attempts) == 1 and isinstance(missing, list) and "retry" in missing):
                self.error("retry requires at least two observed attempts")
            for attempt in attempts:
                for field in ("attemptId", "requestId", "outcome"):
                    self.string(attempt.get(field), f"retry.{field}")
        if "missingExpectedEffects" in replay:
            missing = replay["missingExpectedEffects"]
            if not isinstance(missing, list) or any(not isinstance(name, str) or not name.strip() for name in missing):
                self.error("replay.missingExpectedEffects must be an array of nonempty operation names")
        if "sideEffects" in replay:
            for count in self.records(replay["sideEffects"], "replay.sideEffects", nonempty=True):
                self.string(count.get("operationId"), "replay.sideEffects.operationId")
                for field in ("count", "expectedCount"):
                    if not is_json_integer(count.get(field)) or count[field] < 0:
                        self.error(f"replay.sideEffects.{field} must be a non-negative integer")

    def envelope(self) -> None:
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

    def common(self, *, validate_behavior: bool = True, allow_no_launches: bool = False) -> None:
        self.envelope()
        value = self.value
        if validate_behavior:
            artifact = value.get("artifact")
            revision = artifact.get("conformance") if isinstance(artifact, Mapping) else None
            if isinstance(revision, str) and revision.strip().lower() == "unverified":
                self.error("conformance revision is unverified")
            # Physical retirement can retain partial behavior diagnostics;
            # only the observation validators require their supplied shapes.
            self.optional_shapes()
        observation_end = self.number(value.get("observationEndedAtMs"), "observationEndedAtMs")
        attempt_ordinals: set[tuple[str, int]] = set()
        for attempt in self.records(value.get("attempts"), "attempts", nonempty=not allow_no_launches):
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
            if "activeAtObservationEnd" in attempt:
                if (
                    not validate_behavior
                    or not self.standard_behavior
                    or value.get("requirementId") not in ("lmi-lifecycle-3", "lmi-lifecycle-4")
                    or attempt["activeAtObservationEnd"] is not True
                    or role != "victim"
                    or not is_json_integer(ordinal)
                    or ordinal < 2
                    or start > observation_end
                    or "endedAtMs" in attempt
                    or "outcome" in attempt
                ):
                    self.error(f"attempt {request_id} cannot represent an active retry at observation end")
                continue
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
        victim = self.selected.get("victim", {})
        for request_id, attempt in self.attempts.items():
            if attempt.get("activeAtObservationEnd") is True and (
                request_id in seen or attempt.get("executionArn") != victim.get("executionArn")
            ):
                self.error(f"attempt {request_id} is not a nonselected retry on the victim execution")
        self.events = self.records(value.get("events"), "events", nonempty=not allow_no_launches)
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
            self.event_sequence(event, "event")
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
        self.histories(check_expectations=validate_behavior, allow_no_launches=allow_no_launches)
        for arn in deferred_service_requests(value).values():
            if arn not in {attempt.get("executionArn") for attempt in self.attempts.values()}:
                self.error("deferred service request is outside the observed execution inventory")
        self.collection(observation_end)
        if validate_behavior:
            self.no_late_dispatch()

    def collection(self, observation_end: float | None = None) -> Mapping[str, Any]:
        value = self.value
        collection_errors = value.get("collectionErrors", [])
        if not isinstance(collection_errors, list):
            self.error("collectionErrors must be an array of non-empty strings")
        else:
            for index, error in enumerate(collection_errors):
                message = self.string(error, f"collectionErrors[{index}]")
                if message:
                    self.error(f"collectionErrors[{index}]: {message}")
        collection = self.mapping(value.get("collection"), "collection")
        if observation_end is not None:
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
        return collection

    def event_sequence(self, event: Mapping[str, Any], path: str) -> None:
        if "sequence" in event and (not is_json_integer(event["sequence"]) or event["sequence"] < 0):
            self.error(f"{path}.sequence must be a non-negative integer")

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
                finish = (
                    end
                    if observed_attempt.get("activeAtObservationEnd") is True
                    else self.number(observed_attempt.get("endedAtMs"), "observed attempt.endedAtMs")
                )
                if not start <= time <= finish:
                    self.error(f"{kind} lies outside its observedDuringRequestId interval", "ASSERTION")

    def result_fingerprint(self, record: Mapping[str, Any], path: str) -> None:
        if "resultFingerprint" in record and (
            "result" not in record or not _valid_fingerprint(record["resultFingerprint"])
        ):
            self.error(f"{path}.resultFingerprint requires a stored result and a SHA-256 digest")

    def histories(self, *, check_expectations: bool = True, allow_no_launches: bool = False) -> None:
        expected = self.mapping(self.value.get("expectedHistories"), "expectedHistories") if check_expectations else {}
        actual: dict[str, Mapping[str, Any]] = {}
        for history in self.records(self.value.get("histories"), "histories", nonempty=not allow_no_launches):
            arn = self.string(history.get("executionArn"), "history.executionArn")
            if arn in actual:
                self.error(f"duplicate service history {arn}")
            actual[arn] = history
            self.result_fingerprint(history, f"history {arn}")
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
                self.result_fingerprint(expectation, f"expectedHistories.{arn}")
                allows_unfinished = False
                if ("status" in expectation) == ("allowedStatuses" in expectation):
                    self.error(f"expectedHistories.{arn} must declare exactly one of status or allowedStatuses")
                if "allowedStatuses" in expectation:
                    allowed = expectation["allowedStatuses"]
                    if not _valid_history_status_allowance(allowed):
                        self.error(
                            f"expectedHistories.{arn}.allowedStatuses must contain distinct recognized execution statuses"
                        )
                    else:
                        allows_unfinished = any(status in allowed for status in CUTOFF_ACTIVE_STATUSES)
                elif expectation.get("status") not in HISTORY_STATUSES:
                    self.error(f"expectedHistories.{arn}.status must be a recognized execution status")
                if not allows_unfinished and "result" not in expectation and "error" not in expectation:
                    self.error(f"history {arn} needs an independently declared expected result/error")
                self.errors.extend(_history_expectation_mismatches(arn, history, expectation))
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
                        if permits_disposal_operation_probe(
                            event,
                            wrapper,
                            following,
                            lambda first, second: (
                                None
                                if (order := self.at_or_after(first, second, require_sequence=True)) is None
                                else not order
                            ),
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
            elif not has_overlap_marker_dispatch(
                requests, attempt["requestId"], markers[0]["id"], overlap_start, overlap_end
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
        earlier, tied = original_admission_conflicts(recovery, list(self.attempts.values()))
        if tied:
            self.error("tied native admissions cannot establish the original recovery invocation")
        if recovery.get("outcome") != "SUCCEEDED":
            self.error("recovered capacity did not complete the selected recovery request", "ASSERTION")
        if earlier or any(recovery.get("executionArn") == attempt.get("executionArn") for attempt in (victim, *peers)):
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
            if not peer_progress_spans_timeout(progress, timeout, recovered):
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
        if replay.get("durableWaitObserved") is False:
            self.error("replay did not verify a durable wait across suspension and resume", "ASSERTION")
        if replay.get("suspensionCheckpointMatches") is False:
            self.error("replay did not verify the checkpoint snapshot of a public suspension resume", "ASSERTION")
        before = self.records(replay.get("before"), "replay.before", nonempty=True)
        after = self.records(replay.get("after"), "replay.after", nonempty=True)
        after_by_id = {operation.get("id"): operation for operation in after}
        for error in replay.get("projectionErrors", []):
            self.error(f"replay service projection: {error}")
        if "expectedBefore" in replay:
            expected_before = self.records(replay["expectedBefore"], "replay.expectedBefore")
            expected_by_id = {operation.get("id"): operation for operation in expected_before}
            observed_by_id = {operation.get("id"): operation for operation in before}
            if len(expected_by_id) != len(expected_before):
                self.error("replay.expectedBefore contains duplicate operation IDs")
            if not same_checkpoint_set(expected_by_id, observed_by_id):
                self.error(
                    "replay checkpoint snapshot differs from the complete pre-resume service history", "ASSERTION"
                )
        same_execution = [
            attempt for attempt in self.attempts.values() if attempt.get("executionArn") == victim.get("executionArn")
        ]
        has_earlier, tied = original_admission_conflicts(victim, same_execution)
        if has_earlier:
            self.error("replay victim is not the original invocation of the selected execution", "PLACEMENT")
        if tied:
            self.error("tied native admissions cannot establish the original replay invocation")
        if not completed_suspension_checkpoint(same_execution, self.events, self.value["sdkRequests"], after):
            self.error("replay needs an observed wait or callback checkpoint followed by suspension and resume")
        if any(not same_checkpoint(after_by_id.get(operation.get("id")), operation) for operation in before):
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
        recorded = final_history.get("operations")
        if (
            not isinstance(recorded, list)
            or len(recorded) != len(after)
            or not all(same_checkpoint(left, right) for left, right in zip(recorded, after, strict=True))
        ):
            self.error("replay.after differs from the collected service history", "ASSERTION")
        statuses = {operation.get("status") for operation in before}
        if not {"SUCCEEDED", "FAILED"} <= statuses:
            self.error("replay must preserve both a completed success and a completed failure")
        completed = {operation.get("id") for operation in before}
        omitted, late = replay_checkpoint_timing(before, after, pending.get("atMs", 0))
        if omitted:
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
        if late:
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
            timed_bodies = [
                event
                for event in self.events
                if event.get("kind") == "body"
                and event.get("requestId") == victim.get("requestId")
                and event.get("operationId") == operation_id
            ]
            if not completed_replay_step_witness(
                operation_id,
                victim.get("requestId"),
                executions,
                timed_bodies,
                lambda event: "atMs" in pending and self.at_or_after(event, pending, require_sequence=True) is False,
                operation.get("endedAtMs"),
            ):
                self.error(
                    "completed replay step lacks observed user-code execution on the original request before suspension"
                )
            if "endedAtMs" in operation and any(event["atMs"] > operation["endedAtMs"] for event in timed_bodies):
                self.error("completed replay step body occurred after recorded completion", "ASSERTION")
        missing = replay.get("missingExpectedEffects", [])
        for name in missing:
            self.error(f"replay omitted the expected side-effect operation {name}", "ASSERTION")
        retry_id = None
        if "retry" in replay:
            retry = self.mapping(replay.get("retry"), "replay.retry")
            retry_id = self.string(retry.get("operationId"), "replay.retry.operationId")
            retry_attempts = self.records(retry.get("attempts"), "replay.retry.attempts", nonempty=True)
            retry_attempt_ids = [self.string(item.get("attemptId"), "retry.attemptId") for item in retry_attempts]
            if (len(retry_attempt_ids) < 2 and "retry" not in missing) or len(set(retry_attempt_ids)) != len(
                retry_attempt_ids
            ):
                self.error("retry requires distinct observed attempts")
            if len(retry_attempts) >= 2 and (
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
                identity = retry_body_identity(event)
                if identity is None:
                    self.error("observed retry body does not match a declared attempt")
                    continue
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
        elif "retry" not in missing:
            self.error("replay.retry must be an object")
        counts = self.records(replay.get("sideEffects"), "replay.sideEffects", nonempty=True)
        effect_ids = {self.string(item.get("operationId"), "replay.sideEffects.operationId") for item in counts}
        if len(effect_ids) != len(counts):
            self.error("side-effect counts must be unique per operation")
        if not completed | ({retry_id} if retry_id is not None else set()) <= effect_ids:
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
        if late and (
            late.get("observedDuringRequestId") != warm.get("requestId")
            or not warm.get("startedAtMs", 0) <= late.get("atMs", -1) <= warm.get("endedAtMs", 0)
        ):
            self.error("late activity was not observed during the selected warm invocation", "ASSERTION")
        exited = self.event(victim, "task_exit")
        if not late or not exited:
            return
        late_at, exit_at = late["atMs"], exited["atMs"]
        late_after_exit = late_at > exit_at
        if late_at == exit_at:
            late_sequence, exit_sequence = late.get("sequence"), exited.get("sequence")
            if late_sequence is None or exit_sequence is None or late_sequence == exit_sequence:
                self.error("same-timestamp late_activity and task_exit need distinct sequences")
                return
            late_after_exit = late_sequence > exit_sequence
        if late_after_exit:
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
            self.event_sequence(completion, "retirement.completion")
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
                rejected = (
                    start_kind in _CALL_METHODS
                    and start.get("outcome") == "REJECTED"
                    and _has_matching_sdk_request(
                        start, _CALL_METHODS[start_kind], "REJECTED", self.value.get("sdkRequests", [])
                    )
                )
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

    def retirement(
        self, *, audit_observed_work: bool = True, retired_requests: Mapping[str, set[str]] | None = None
    ) -> None:
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
                            and len(
                                {
                                    request
                                    for request, attempt in self.attempts.items()
                                    if attempt.get("executionArn") == item.get("id")
                                }
                                | (retired_requests or {}).get(item["id"], set())
                            )
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


def _owned_record_enriches(previous: Any, current: Any, *, event: bool = False) -> bool:
    if not isinstance(previous, Mapping) or not isinstance(current, Mapping):
        return False
    metadata = {"observedDuringRequestId"}
    if event:
        metadata.add("sequence")
        for record in (previous, current):
            if "sequence" in record and (not is_json_integer(record["sequence"]) or record["sequence"] < 0):
                return False
        if "sequence" in previous and "sequence" not in current:
            return False
    # The shared owned-record validator treats missing and null correlation as
    # unassigned. Once an invocation ID is present it cannot be replaced.
    if previous.get("observedDuringRequestId") is not None and (
        "observedDuringRequestId" not in current
        or not same_json(previous["observedDuringRequestId"], current["observedDuringRequestId"])
    ):
        return False
    return same_json(
        {key: value for key, value in previous.items() if key not in metadata},
        {key: value for key, value in current.items() if key not in metadata},
    )


def deferred_service_requests(evidence: Mapping[str, Any]) -> dict[str, str]:
    collection = evidence.get("collection")
    rows = collection.get("deferredServiceRequests", []) if isinstance(collection, Mapping) else []
    if not isinstance(rows, list):
        raise ValueError("Deferred service requests must be a list")
    requests: dict[str, str] = {}
    for row in rows:
        request = row.get("requestId") if isinstance(row, Mapping) else None
        arn = row.get("executionArn") if isinstance(row, Mapping) else None
        if (
            not isinstance(request, str)
            or not request.strip()
            or request in requests
            or not isinstance(arn, str)
            or not arn.strip()
        ):
            raise ValueError("Deferred service requests must have unique request IDs and owned execution ARNs")
        requests[request] = arn
    return requests


def deferred_retirement_errors(original: Mapping[str, Any], final: Mapping[str, Any]) -> list[str]:
    obligations = deferred_service_requests(original)
    if not obligations:
        return []
    attempts = final.get("attempts", [])
    if not isinstance(attempts, list):
        return ["Retirement cannot reconcile deferred service requests without an invocation inventory"]
    indexed = {
        attempt["requestId"]: attempt
        for attempt in attempts
        if isinstance(attempt, Mapping) and isinstance(attempt.get("requestId"), str)
    }
    cutoff = original.get("observationEndedAtMs")
    errors = []
    for request, arn in obligations.items():
        terminal = indexed.get(request, {})
        started = terminal.get("startedAtMs")
        if terminal.get("executionArn") != arn:
            errors.append(f"Retirement omitted deferred service request {request}")
        elif (
            not isinstance(cutoff, (int, float))
            or isinstance(cutoff, bool)
            or not isinstance(started, (int, float))
            or isinstance(started, bool)
            or not 0 <= cutoff < started < math.inf
        ):
            errors.append(f"Retirement did not prove deferred service request {request} started after the cutoff")
    return errors


def active_pre_cutoff_errors(original: Mapping[str, Any], final: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    original_attempts = original.get("attempts", [])
    if not isinstance(original_attempts, list):
        return ["Retirement cannot reconcile malformed original attempts"]
    active_retries = {
        item["requestId"]
        for item in original_attempts
        if isinstance(item, Mapping)
        and item.get("activeAtObservationEnd") is True
        and isinstance(item.get("requestId"), str)
    }
    cutoff = original.get("observationEndedAtMs")
    if active_retries and isinstance(cutoff, (int, float)) and not isinstance(cutoff, bool):
        for field, name in (("events", "events"), ("sdkRequests", "SDK requests")):
            before, after = original.get(field, []), final.get(field, [])
            if not isinstance(before, list) or not isinstance(after, list):
                errors.append(f"Retirement cannot reconcile malformed {name} for an active retry")
                continue
            retained = [
                row
                for row in after
                if isinstance(row, Mapping)
                and isinstance(row.get("requestId"), str)
                and row["requestId"] in active_retries
                and isinstance(at := row.get("atMs"), (int, float))
                and not isinstance(at, bool)
                and at <= cutoff
            ]
            fully_retained = True
            for row in before:
                if (
                    not isinstance(row, Mapping)
                    or not isinstance(request := row.get("requestId"), str)
                    or request not in active_retries
                ):
                    continue
                match = next(
                    (
                        index
                        for index, final_row in enumerate(retained)
                        if _owned_record_enriches(row, final_row, event=name == "events")
                    ),
                    None,
                )
                if match is None:
                    fully_retained = False
                    break
                retained.pop(match)
            if not fully_retained or retained:
                errors.append(f"Retirement disagrees on pre-cutoff {name} for an active retry")

    return errors


def cleanup_admission_errors(
    admission_sources: Sequence[Mapping[str, Any]], *, disposal_victim: str | None = None
) -> list[str]:
    def records(value: Any) -> list[Mapping[str, Any]]:
        return [record for record in value if isinstance(record, Mapping)] if isinstance(value, list) else []

    def timestamp(value: Any) -> float | None:
        try:
            if isinstance(value, (int, float)) and not isinstance(value, bool) and 0 <= value < math.inf:
                return float(value)
        except OverflowError:
            pass
        return None

    findings: list[str] = []
    known_requests = {
        attempt["requestId"]
        for source in admission_sources
        for attempt in records(source.get("attempts"))
        if isinstance(attempt.get("requestId"), str)
    }
    closures: dict[str, float] = {}
    source_events = [records(source.get("events")) for source in admission_sources]

    def disposal_boundaries(events: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
        return [
            event
            for event in events
            if event.get("requestId") == disposal_victim
            and event.get("kind") in ("wrapper_return", "wrapper_raise", "admission_closed")
            and timestamp(event.get("atMs")) is not None
        ]

    def boundary_key(event: Mapping[str, Any]) -> tuple[str, float]:
        return event["kind"], event["atMs"]

    known_boundaries = {boundary_key(event) for events in source_events for event in disposal_boundaries(events)}
    known_wrappers = {boundary for boundary in known_boundaries if boundary[0] != "admission_closed"}

    def diagnostic_probe(source: Sequence[Mapping[str, Any]], record: Mapping[str, Any]) -> bool:
        if disposal_victim is None or record.get("requestId") != disposal_victim or len(known_wrappers) != 1:
            return False
        boundaries = disposal_boundaries(source)
        wrappers = [event for event in boundaries if event["kind"] == "wrapper_return"]
        if len(wrappers) > 1 or next(iter(known_wrappers))[0] != "wrapper_return":
            return False
        check = _Evidence({})
        following = [event for event in boundaries if check.at_or_after(record, event)]
        for error in check.errors:
            if error not in findings:
                findings.append(error)
        if check.errors:
            return False
        if (
            record.get("probe") == "operation"
            and record.get("outcome") == "ACCEPTED"
            and (next(iter(known_wrappers))[1] < record["atMs"] or (wrappers and wrappers[0] in following))
        ):
            return True
        # Sequence comparisons are valid within a snapshot only.
        earlier = {boundary for boundary in known_boundaries if boundary[1] <= record["atMs"]}
        return (
            record.get("outcome") == "ACCEPTED"
            and not following
            and earlier <= {boundary_key(event) for event in boundaries}
        )

    # A later observation can reveal an earlier closure. Preserve every known
    # boundary and recheck old activity as well as newly discovered requests.
    for event in (event for events in source_events for event in events):
        request_id, time = event.get("requestId"), timestamp(event.get("atMs"))
        if (
            isinstance(request_id, str)
            and request_id in known_requests
            and time is not None
            and event.get("kind") in ("wrapper_return", "wrapper_raise", "admission_closed")
        ):
            closures[request_id] = min(closures.get(request_id, time), time)
    for collection in ("sdkRequests", "events"):
        for source, events in zip(admission_sources, source_events, strict=True):
            for record in records(source.get(collection)):
                if collection == "events" and record.get("kind") != "admission_probe":
                    continue
                request_id, time = record.get("requestId"), timestamp(record.get("atMs"))
                closed = closures.get(request_id) if isinstance(request_id, str) else None
                if closed is None or time is None or time < closed:
                    continue
                if collection == "sdkRequests" and record.get("outcome") == "DISPATCHED":
                    finding = f"ASSERTION: request {request_id} dispatched SDK work after admission closed"
                elif (
                    collection == "events"
                    and record.get("probe") in ("operation", "checkpoint")
                    and record.get("outcome") != "REJECTED"
                    and not diagnostic_probe(events, record)
                ):
                    finding = (
                        f"ASSERTION: request {request_id} admitted a {record['probe']} probe after admission closed"
                    )
                else:
                    continue
                if finding not in findings:
                    findings.append(finding)
    return findings


def frozen_attempt_retirement_errors(original: Mapping[str, Any], final: Mapping[str, Any]) -> list[str]:
    """Reconcile all frozen invocation facts against independent retirement."""
    errors: list[str] = []
    original_attempts = original.get("attempts", [])
    observed = final.get("attempts", [])
    if not isinstance(original_attempts, list) or not isinstance(observed, list):
        raise ValueError("Retirement invocation inventories must be lists")
    indexed = {item.get("requestId"): item for item in observed if isinstance(item, Mapping)}
    errors.extend(deferred_retirement_errors(original, final))

    for attempt in original_attempts:
        if not isinstance(attempt, Mapping):
            raise ValueError("Original invocation inventory is malformed")
        settled = indexed.get(attempt.get("requestId"), {})
        if attempt.get("activeAtObservationEnd") is True:
            cutoff = original.get("observationEndedAtMs")
            end = settled.get("endedAtMs")
            if (
                attempt.get("startedAtMs") != settled.get("startedAtMs")
                or "activeAtObservationEnd" in settled
                or settled.get("outcome") not in ("SUCCEEDED", "FAILED", "TIMED_OUT", "PENDING")
                or not isinstance(cutoff, (int, float))
                or isinstance(cutoff, bool)
                or not isinstance(end, (int, float))
                or isinstance(end, bool)
                or not 0 <= cutoff < end < math.inf
            ):
                errors.append("Retirement did not reconcile an active retry with its actual service completion")
        # PENDING belongs to the suspended invocation; its resume has a new
        # request ID and cannot replace that already observed native outcome.
        for key in (
            "executionArn",
            "environmentId",
            "workerId",
            "processId",
            "role",
            "attempt",
            "outcome",
        ):
            if key in attempt and attempt[key] != settled.get(key):
                errors.append(f"Retirement changed an original invocation's {key}")
        # Native completion can widen a provisional fixture-observed interval,
        # but it cannot erase any part of the original observation window.
        for key in ("startedAtMs", "endedAtMs"):
            if key not in attempt:
                continue
            before, after = attempt[key], settled.get(key)
            if any(
                isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0
                for value in (before, after)
            ):
                errors.append(f"Retirement cannot reconcile a malformed invocation {key}")
            elif (key == "startedAtMs" and after > before) or (key == "endedAtMs" and after < before):
                errors.append(f"Retirement narrowed an original invocation's {key}")
    return errors


OPERATION_TERMINAL_STATUSES = ("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT", "STOPPED")


_TASK_EVENT_ALIASES = {"observed_task_start": "task_start", "observed_task_exit": "task_exit"}


def _physical_event_kind(event: Mapping[str, Any]) -> Any:
    kind = event.get("kind")
    if isinstance(kind, str) and (source := _TASK_EVENT_ALIASES.get(kind)) == event.get("sourceKind") and source:
        return source
    return kind


def tied_late_activity_follows_exit(
    source_events: Sequence[Mapping[str, Any]], late: Mapping[str, Any], task_exit: Mapping[str, Any]
) -> bool | None:
    """Order late activity against a task exit with the same timestamp in one snapshot.

    Snapshots may renumber sequences, so ``late`` is compared only with this
    snapshot's copy of the exit. ``None`` means the snapshot cannot order them.
    """
    sequences = [
        event.get("sequence")
        for event in source_events
        if _physical_event_kind(event) == "task_exit"
        and event.get("requestId") == task_exit.get("requestId")
        and same_json(event.get("taskId"), task_exit.get("taskId"))
        and type(event.get("atMs")) in (int, float)
        and event.get("atMs") == task_exit.get("atMs")
    ]
    late_sequence, exit_sequence = late.get("sequence"), sequences[0] if len(sequences) == 1 else None
    if not is_json_integer(late_sequence) or not is_json_integer(exit_sequence):
        return None
    if late_sequence < 0 or exit_sequence < 0 or late_sequence == exit_sequence:
        return None
    return late_sequence > exit_sequence


_CLEANUP_LATE_ACTIVITY = "ASSERTION: cleanup late activity continued after the frozen victim task exit"
_UNORDERED_CLEANUP_LATE_ACTIVITY = (
    "INCOMPLETE: cleanup late activity tied with the frozen victim task exit needs distinct sequences"
)
_LATE_ACTIVITY_IDENTITY = ("taskId", "callId", "gateId", "operationId", "attemptId", "attempt")


def cleanup_late_activity_errors(
    frozen: Mapping[str, Any],
    sources: Sequence[Mapping[str, Any]],
    victim_exit: Mapping[str, Any],
    bound_execution: Callable[[Any], str | None],
    *,
    final: bool,
) -> list[str]:
    """Check late activity seen during cleanup against the victim's frozen task exit.

    Each tie is ordered within one snapshot. By the final verdict, every tied
    activity a cleanup snapshot reported must be ordered in some snapshot; an
    identity seen several times needs as many ordered copies.
    """

    def timestamp(value: Any) -> float | None:
        try:
            if type(value) in (int, float) and math.isfinite(value) and value >= 0:
                return value
        except OverflowError:
            pass
        return None

    findings: list[str] = []
    victim_id, exited = victim_exit.get("requestId"), timestamp(victim_exit.get("atMs"))
    seen: Counter[str] = Counter()
    ordered: Counter[str] = Counter()
    for source in sources:
        raw = source.get("events")
        events = [item for item in raw if isinstance(item, Mapping)] if isinstance(raw, list) else []
        local_seen: Counter[str] = Counter()
        local_ordered: Counter[str] = Counter()
        for event in events:
            if event.get("kind") != "late_activity":
                continue
            request_id = event.get("requestId")
            execution_arn = bound_execution(request_id)
            if execution_arn is None:
                continue
            if event.get("ownerRequestId") != request_id or event.get("executionArn") != execution_arn:
                if final:
                    findings.append("INCOMPLETE: cleanup late activity has inconsistent native ownership")
                continue
            during = event.get("observedDuringRequestId")
            if during is not None and bound_execution(during) is None:
                continue
            observed_at = timestamp(event.get("atMs"))
            if request_id != victim_id or observed_at is None or exited is None:
                continue
            if observed_at != exited:
                if observed_at > exited:
                    findings.append(_CLEANUP_LATE_ACTIVITY)
                continue
            identity = json.dumps([event.get(key) for key in _LATE_ACTIVITY_IDENTITY], default=str)
            local_seen[identity] += 1
            follows = tied_late_activity_follows_exit(events, event, victim_exit)
            if follows is not None:
                local_ordered[identity] += 1
            if follows:
                findings.append(_CLEANUP_LATE_ACTIVITY)
        # The live check already reports an unordered tie in frozen evidence.
        if source is not frozen:
            seen |= local_seen
        ordered |= local_ordered
    if final and any(ordered[identity] < count for identity, count in seen.items()):
        findings.append(_UNORDERED_CLEANUP_LATE_ACTIVITY)
    return findings


def frozen_history_work_errors(original: Mapping[str, Any], final: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []

    # Cleanup may complete active work, but cannot rewrite already terminal
    # service facts. Fixture expectations are separate behavioral evidence.
    def preserve_history_fields(
        before: Mapping[str, Any], after: Mapping[str, Any] | None, owner: str, fields: tuple[str, ...]
    ) -> None:
        if after is None:
            errors.append(f"Retirement omitted {owner}")
            return
        for key in fields:
            if key == "result" and same_checkpoint_result(before, after):
                continue
            if key in before and (key not in after or not same_json(before[key], after[key])):
                errors.append(f"Retirement changed {owner}.{key}")

    final_histories = {
        item.get("executionArn"): item for item in final.get("histories", []) if isinstance(item, Mapping)
    }
    for history in original.get("histories", []):
        if not isinstance(history, Mapping):
            continue  # Already reported as an unidentified original inventory row.
        arn = history.get("executionArn")
        if not isinstance(arn, str) or not arn.strip():
            continue
        completed_history = final_histories.get(arn)
        if history.get("status") in TERMINAL_STATUSES:
            preserve_history_fields(
                history,
                completed_history,
                f"terminal execution {arn}",
                ("status", "result", "error", "resultFingerprint"),
            )
        before_operations = history.get("operations", [])
        after_operations = completed_history.get("operations", []) if completed_history is not None else []
        if not isinstance(before_operations, list) or not isinstance(after_operations, list):
            raise ValueError("Retirement operation inventories must be lists")
        indexed_operations = {item.get("id"): item for item in after_operations if isinstance(item, Mapping)}
        for operation in before_operations:
            if not isinstance(operation, Mapping):
                raise ValueError("Original operation inventory is malformed")
            operation_id = operation.get("id")
            operation_fields: tuple[str, ...] = ("name", "type", "parentId")
            owner = f"operation {operation_id} in {arn}"
            if operation.get("status") in OPERATION_TERMINAL_STATUSES:
                operation_fields += ("status", "result", "error", "resultEncoding", "resultFingerprint")
                owner = f"terminal {owner}"
            preserve_history_fields(operation, indexed_operations.get(operation_id), owner, operation_fields)
    before_events, after_events = original.get("events", []), final.get("events", [])
    if not isinstance(before_events, list) or not isinstance(after_events, list):
        raise ValueError("Retirement observation inventories must be lists")
    start_kinds = {
        "task_start",
        "io_start",
        "checkpoint_call",
        "poll_call",
    }
    fields = (
        "kind",
        "requestId",
        "ownerRequestId",
        "executionArn",
        "atMs",
        "taskId",
        "callId",
        "gateId",
    )

    # Completion facts and late activity are preserved as well as starts: a
    # snapshot that drops them would audit less than was already observed.
    preserved_kinds = start_kinds | {
        "wrapper_enter",
        "task_exit",
        "io_exit",
        "checkpoint_exit",
        "poll_exit",
        "wrapper_return",
        "wrapper_raise",
        "late_activity",
    }

    def event_identity(event: Mapping[str, Any]) -> dict[str, Any]:
        identity = {key: event.get(key) for key in fields}
        identity["kind"] = _physical_event_kind(event)
        if "observedDuringRequestId" in event:
            identity["observedDuringRequestId"] = event["observedDuringRequestId"]
        return identity

    def preserved(events: list[Any]) -> list[dict[str, Any]]:
        if any(not isinstance(event, Mapping) for event in events):
            raise ValueError("Retirement observations must be objects")
        return [event_identity(event) for event in events if _physical_event_kind(event) in preserved_kinds]

    remaining_events = preserved(after_events)
    for event in sorted(preserved(before_events), key=lambda item: item.get("observedDuringRequestId") is None):
        match = next(
            (index for index, item in enumerate(remaining_events) if _owned_record_enriches(event, item)), None
        )
        if match is None:
            errors.append("Retirement omitted or changed previously observed work")
        else:
            remaining_events.pop(match)

    def sequenced(events: list[Any]) -> dict[Any, list[tuple[int | float, dict[str, Any]]]]:
        by_request: dict[Any, list[tuple[int | float, dict[str, Any]]]] = {}
        for event in events:
            if _physical_event_kind(event) not in preserved_kinds or "sequence" not in event:
                continue
            sequence = event["sequence"]
            if not is_json_integer(sequence) or sequence < 0:
                errors.append("Retirement cannot reconcile a malformed semantic sequence")
                continue
            by_request.setdefault(event.get("requestId"), []).append((sequence, event_identity(event)))
        return by_request

    # Sequence values can change when additional observations are collected.
    # Preserve the known relative order, particularly at equal timestamps;
    # partial events without sequence numbers do not yet establish that order.
    original_order, final_order = sequenced(before_events), sequenced(after_events)
    for request_id, records in original_order.items():
        if len({sequence for sequence, _ in records}) != len(records):
            errors.append("Retirement cannot reconcile duplicate original semantic sequences")
            continue
        remaining_order = iter(sorted(final_order.get(request_id, []), key=lambda item: item[0]))
        for _, identity in sorted(records, key=lambda item: item[0]):
            if not any(_owned_record_enriches(identity, candidate) for _, candidate in remaining_order):
                errors.append("Retirement changed the semantic order of previously observed work")
                break
    before_requests, after_requests = original.get("sdkRequests", []), final.get("sdkRequests", [])
    if not isinstance(before_requests, list) or not isinstance(after_requests, list):
        raise ValueError("Retirement SDK request inventories must be lists")
    remaining = list(after_requests)
    for request in sorted(
        before_requests, key=lambda item: not isinstance(item, Mapping) or item.get("observedDuringRequestId") is None
    ):
        if not isinstance(request, Mapping):
            raise ValueError("Original SDK request inventory is malformed")
        match = next((index for index, item in enumerate(remaining) if _owned_record_enriches(request, item)), None)
        if match is None:
            errors.append("Retirement omitted or changed a previously observed SDK request")
        else:
            remaining.pop(match)

    return errors


def retirement_artifact_errors(original: Mapping[str, Any], final: Mapping[str, Any]) -> list[str]:
    if "artifact" not in original:
        return []
    before, after = original["artifact"], final.get("artifact")
    if not isinstance(before, Mapping) or not isinstance(after, Mapping):
        return ["Retirement artifact metadata must remain an object"]
    return [
        f"Retirement artifact.{key} differs from the original observations"
        for key, value in before.items()
        if key not in after or not same_json(value, after[key])
    ]


def cleanup_replay_history_errors(frozen: Mapping[str, Any], observed: Mapping[str, Any]) -> list[str]:
    findings = []
    expected, before = observed.get("expectedBefore"), frozen.get("before")
    if isinstance(expected, list) and isinstance(before, list):
        expected_ids = {
            row.get("id"): row for row in expected if isinstance(row, Mapping) and isinstance(row.get("id"), str)
        }
        frozen_ids = {
            row.get("id"): row for row in before if isinstance(row, Mapping) and isinstance(row.get("id"), str)
        }
        if len(expected_ids) != len(expected):
            findings.append("INCOMPLETE: cleanup pre-resume service checkpoints have duplicate or malformed IDs")
        elif not same_checkpoint_set(frozen_ids, expected_ids):
            findings.append("ASSERTION: cleanup service history contradicts the frozen replay checkpoint snapshot")
    if observed.get("durableWaitObserved") is False:
        findings.append("ASSERTION: cleanup service history refutes the durable wait suspension")
    if observed.get("suspensionCheckpointMatches") is False:
        findings.append("ASSERTION: cleanup service history refutes the public suspension replay checkpoint snapshot")
    return findings


def cleanup_replay_count_errors(count: Mapping[str, Any], expected: int | float | None) -> list[str]:
    identity = count["operationId"]
    if expected is None:
        reported, observed = count.get("expectedCount"), count.get("count")
        if is_json_integer(reported) and reported >= 0 and is_json_integer(observed) and observed > reported:
            return [f"ASSERTION: cleanup side-effect count for {identity} exceeds the fixture oracle"]
        return []
    errors = []
    if "expectedCount" in count and not same_json(count["expectedCount"], expected):
        errors.append(f"ASSERTION: cleanup side-effect expectation for {identity} changed the frozen fixture oracle")
    if is_json_integer(count.get("count")) and count["count"] != expected:
        errors.append(f"ASSERTION: cleanup side-effect count for {identity} differs from the frozen fixture oracle")
    return errors


def repeated_completed_body_error(operation_id: str) -> str:
    return f"ASSERTION: cleanup evidence reports repeated completed user code for {operation_id}"


# Lambda OperationStatus differs from the execution-level status vocabulary.
_OPERATION_NONTERMINAL_STATUSES = ("STARTED", "PENDING", "READY")
# replay.before contains completed checkpoints; only current operation views progress.
_OPERATION_RECORD_PATHS: frozenset[tuple[str, ...]] = frozenset({("histories", "operations"), ("replay", "after")})
_OUTCOME_RECORD_PATHS = _OPERATION_RECORD_PATHS | {("histories",), ("replay", "before")}
_OBSERVATION_IDENTITIES: dict[tuple[str, ...], str] = {
    ("attempts",): "requestId",
    ("histories",): "executionArn",
    ("histories", "operations"): "id",
    ("faultsCreated",): "id",
    ("resourcesCreated",): "id",
    ("replay", "before"): "id",
    ("replay", "after"): "id",
    ("replay", "retry", "attempts"): "attemptId",
    ("replay", "sideEffects"): "operationId",
}


def _retry_attempt_order(evidence: Mapping[str, Any]) -> tuple[str, ...] | None:
    replay = evidence.get("replay")
    retry = replay.get("retry") if isinstance(replay, Mapping) else None
    attempts = retry.get("attempts") if isinstance(retry, Mapping) else None
    if not isinstance(attempts, list):
        return None
    identifiers = []
    for attempt in attempts:
        identifier = attempt.get("attemptId") if isinstance(attempt, Mapping) else None
        if not isinstance(identifier, str) or not identifier.strip():
            return None
        identifiers.append(identifier)
    # Singleton fragments establish no ordering constraint. The caller diagnoses
    # duplicate identities before using an observed sequence as an ordering.
    if len(identifiers) < 2:
        return None
    return tuple(identifiers)


def _side_effect_observation_errors(evidence: Mapping[str, Any], *, saved: bool = False) -> list[str]:
    replay = evidence.get("replay")
    counts = replay.get("sideEffects") if isinstance(replay, Mapping) else None
    if not isinstance(counts, list):
        return []
    errors = []
    seen: set[str] = set()
    for count in counts:
        if not isinstance(count, Mapping):
            continue
        operation_id = count.get("operationId")
        if isinstance(operation_id, str):
            if operation_id in seen:
                errors.append(f"INCOMPLETE: duplicate side-effect operationId {operation_id}")
            seen.add(operation_id)
        for counter_field in ("count", "expectedCount"):
            if counter_field not in count:
                continue
            value = count[counter_field]
            valid = is_json_integer(value) if saved else type(value) is int
            if not valid or value < 0:
                errors.append(f"INCOMPLETE: side-effect {counter_field} must be a non-negative integer")
    return errors


def _observation_advanced(previous: Any, current: Any, path: tuple[str, ...]) -> bool:
    if path == ("histories", "status"):
        return previous in (*CUTOFF_ACTIVE_STATUSES, "PENDING") and current in HISTORY_STATUSES
    if path[:-1] in _OPERATION_RECORD_PATHS and path[-1:] == ("status",):
        return previous in _OPERATION_NONTERMINAL_STATUSES and current in (
            *_OPERATION_NONTERMINAL_STATUSES,
            *OPERATION_TERMINAL_STATUSES,
        )
    if path == ("replay", "sideEffects", "count"):
        return type(previous) is int and type(current) is int and 0 <= previous <= current
    increasing = path == ("observationEndedAtMs",) or path[:2] == ("collection", "watermarks")
    increasing |= path == ("attempts", "endedAtMs")
    decreasing = path == ("attempts", "startedAtMs")
    if not (increasing or decreasing):
        return False
    if any(type(value) not in (int, float) or not math.isfinite(value) or value < 0 for value in (previous, current)):
        return False
    return current >= previous if increasing else current <= previous


@dataclass
class _RetentionChanges:
    omitted: bool = False
    conflicts: set[tuple[str, ...]] = dataclass_field(default_factory=set)

    def __bool__(self) -> bool:
        return self.omitted or bool(self.conflicts)

    def merge(self, other: _RetentionChanges) -> None:
        self.omitted |= other.omitted
        self.conflicts.update(other.conflicts)


def _operation_entries_compatible(previous: Any, current: Any) -> bool:
    if same_json(previous, current):
        return True
    if not isinstance(previous, Mapping) or not isinstance(current, Mapping):
        return False
    for key in ("requestId", "operationId"):
        if not isinstance(previous.get(key), str) or not previous[key].strip() or previous[key] != current.get(key):
            return False
    if ("attemptId" in previous) != ("attemptId" in current):
        return False
    if "attemptId" in previous:
        attempt_id = previous["attemptId"]
        if not isinstance(attempt_id, str) or not attempt_id.strip() or attempt_id != current["attemptId"]:
            return False
    elif not (previous.keys() <= current.keys() or current.keys() <= previous.keys()):
        # Without a retry identity, complementary partial descriptions do not
        # establish that two observations came from the same body occurrence.
        return False
    return all(same_json(previous[key], current[key]) for key in previous.keys() & current.keys())


def _retain_operation_entries(previous: list[Any], current: list[Any]) -> tuple[list[Any], _RetentionChanges]:
    """Match compatible body entries one-to-one; their order is not an identity."""
    candidates = [
        [index for index, item in enumerate(current) if _operation_entries_compatible(record, item)]
        for record in previous
    ]
    by_previous: dict[int, int] = {}
    by_current: dict[int, int] = {}
    for start in range(len(previous)):
        pending = deque([start])
        parents: dict[int, int] = {}
        endpoint: int | None = None
        while pending and endpoint is None:
            original = pending.popleft()
            for observed in candidates[original]:
                if observed in parents:
                    continue
                parents[observed] = original
                if observed not in by_current:
                    endpoint = observed
                    break
                pending.append(by_current[observed])
        # Reassign an earlier match when necessary, so a bare entry cannot
        # consume the only occurrence compatible with another entry's metadata.
        while endpoint is not None:
            original = parents[endpoint]
            prior_match = by_previous.get(original)
            by_previous[original] = endpoint
            by_current[endpoint] = original
            endpoint = prior_match

    retained = copy.deepcopy(current)
    changes = _RetentionChanges()
    for index, record in enumerate(previous):
        match = by_previous.get(index)
        if match is None:
            retained.append(copy.deepcopy(record))
            changes.omitted = True
        elif isinstance(record, Mapping):
            for key, value in record.items():
                if key not in retained[match]:
                    retained[match][key] = copy.deepcopy(value)
                    changes.omitted = True
    return retained, changes


def _observation_updates(
    previous: Mapping[str, Any], current: Mapping[str, Any], path: tuple[str, ...] = ()
) -> dict[str, Any]:
    """Extract new facts without replaying stale fields from a supplied object."""
    updates: dict[str, Any] = {}
    for key, value in current.items():
        if key in previous and same_json(previous[key], value):
            continue
        atomic = path == ("expectedHistories",) or (path in _OUTCOME_RECORD_PATHS and key in ("result", "error"))
        if key not in previous or atomic:
            updates[key] = copy.deepcopy(value)
        elif isinstance(previous[key], Mapping) and isinstance(value, Mapping):
            nested = _observation_updates(previous[key], value, (*path, key))
            if nested:
                updates[key] = nested
        elif isinstance(previous[key], list) and isinstance(value, list) and (*path, key) in _OBSERVATION_IDENTITIES:
            identity = _OBSERVATION_IDENTITIES[(*path, key)]
            previous_counts = Counter(
                record[identity]
                for record in previous[key]
                if isinstance(record, Mapping) and isinstance(record.get(identity), str)
            )
            current_counts = Counter(
                record[identity]
                for record in value
                if isinstance(record, Mapping) and isinstance(record.get(identity), str)
            )
            if any(current_counts[identifier] > count for identifier, count in previous_counts.items()):
                # A lone extra occurrence would merge back into the known row.
                # Preserve this inventory's full multiplicity and facts instead.
                updates[key] = copy.deepcopy(value)
                continue
            remaining = list(previous[key])
            records = []
            for record in value:
                match = next(
                    (
                        index
                        for index, item in enumerate(remaining)
                        if isinstance(record, Mapping)
                        and isinstance(record.get(identity), str)
                        and isinstance(item, Mapping)
                        and record[identity] == item.get(identity)
                    ),
                    None,
                )
                if match is None:
                    records.append(copy.deepcopy(record))
                    continue
                prior = remaining.pop(match)
                nested = _observation_updates(prior, record, (*path, key))
                if nested:
                    nested[identity] = record[identity]
                    if (
                        (*path, key) in _OUTCOME_RECORD_PATHS
                        and {"status", "result", "error"} & nested.keys()
                        and "status" in record
                    ):
                        nested["status"] = record["status"]
                        # Status can make an unchanged outcome immutable. Carry
                        # present values (including null), but not removed fields.
                        for outcome in ("result", "error"):
                            if outcome in record:
                                nested[outcome] = copy.deepcopy(record[outcome])
                    records.append(nested)
            if records:
                updates[key] = records
        else:
            # Unkeyed lists stay whole: operationExecutions can contain multiple
            # body occurrences with the same request/operation identities.
            updates[key] = copy.deepcopy(value)
    return updates


def _retain_observations(
    original: Mapping[str, Any], captured: Mapping[str, Any], path: tuple[str, ...] = ()
) -> tuple[dict[str, Any], _RetentionChanges]:
    """Keep every prior fact while adding a failed collector's new observations.

    Native intervals, observation watermarks and unfinished service histories
    may advance. Other conflicting facts retain their original values. Lists
    preserve known records, including multiplicity, so collection cannot erase
    owned work. The raw snapshots are audited separately during retirement.
    """
    retained = copy.deepcopy(dict(captured))
    changed = _RetentionChanges()
    completing_execution = (
        path == ("histories",)
        and original.get("status") in (*CUTOFF_ACTIVE_STATUSES, "PENDING")
        and captured.get("status") in ("SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED")
    )
    progressing_operation = path in _OPERATION_RECORD_PATHS and _observation_advanced(
        original.get("status"), captured.get("status"), (*path, "status")
    )
    for key, previous in original.items():
        if key in ("result", "error") and (progressing_operation or (completing_execution and previous is None)):
            # Active operations can replace intermediate poll results or retry
            # errors. Terminal checkpoints and non-null execution outcomes stay
            # immutable; an unfinished execution's null is only a placeholder.
            continue
        if (
            key == "result"
            and path in _OPERATION_RECORD_PATHS | {("replay", "before")}
            and same_checkpoint_result(original, retained)
        ):
            continue
        if path == ("expectedHistories",) or (path in _OUTCOME_RECORD_PATHS and key in ("result", "error")):
            # Checkpoint JSON and each predeclared execution expectation are
            # atomic contracts: arrays, object contents and status allowances
            # cannot be rewritten to accommodate the observed result.
            if key not in retained or not same_json(previous, retained[key]):
                if key in retained:
                    changed.conflicts.add((*path, key))
                else:
                    changed.omitted = True
                retained[key] = copy.deepcopy(previous)
            continue
        if key not in retained:
            retained[key] = copy.deepcopy(previous)
            changed.omitted = True
        elif isinstance(previous, Mapping) and isinstance(retained[key], Mapping):
            retained[key], nested_changed = _retain_observations(previous, retained[key], (*path, key))
            changed.merge(nested_changed)
        elif (
            (*path, key) == ("replay", "operationExecutions")
            and isinstance(previous, list)
            and isinstance(retained[key], list)
        ):
            retained[key], nested_changed = _retain_operation_entries(previous, retained[key])
            changed.merge(nested_changed)
        elif isinstance(previous, list) and isinstance(retained[key], list):
            remaining = list(range(len(retained[key])))
            identity = _OBSERVATION_IDENTITIES.get((*path, key))
            events = (*path, key) == ("events",)
            owned_records = (*path, key) in (("events",), ("sdkRequests",))
            event_order: dict[str, list[tuple[int | float, int | float]]] = {}
            prior_records = previous
            if events and all(
                isinstance(record, Mapping) and is_json_integer(record.get("sequence"))
                for record in [*previous, *retained[key]]
            ):
                prior_records = sorted(previous, key=lambda record: record["sequence"])
                remaining.sort(key=lambda index: retained[key][index]["sequence"])
            elif owned_records:
                # Reserve already correlated occurrences before matching an
                # unattributed record that could fit more than one occurrence.
                prior_records = sorted(
                    previous,
                    key=lambda record: not isinstance(record, Mapping) or record.get("observedDuringRequestId") is None,
                )
            for record in prior_records:
                match = next(
                    (
                        index
                        for index in remaining
                        if same_json(record, retained[key][index])
                        or (
                            owned_records
                            and (
                                _owned_record_enriches(record, retained[key][index], event=events)
                                or _owned_record_enriches(retained[key][index], record, event=events)
                            )
                        )
                        or (
                            identity is not None
                            and isinstance(record, Mapping)
                            and isinstance(record.get(identity), str)
                            and isinstance(retained[key][index], Mapping)
                            and record[identity] == retained[key][index].get(identity)
                        )
                    ),
                    None,
                )
                if match is None:
                    retained[key].append(copy.deepcopy(record))
                    changed.omitted = True
                else:
                    remaining.remove(match)
                    if owned_records and isinstance(record, Mapping):
                        for metadata in (
                            ("observedDuringRequestId", "sequence") if events else ("observedDuringRequestId",)
                        ):
                            if metadata == "observedDuringRequestId" and record.get(metadata) is None:
                                continue
                            if metadata in record and (
                                metadata not in retained[key][match]
                                or (metadata == "observedDuringRequestId" and retained[key][match][metadata] is None)
                            ):
                                retained[key][match][metadata] = copy.deepcopy(record[metadata])
                                changed.omitted = True
                    if events and isinstance(record, Mapping):
                        old_sequence, new_sequence = record.get("sequence"), retained[key][match].get("sequence")
                        request_id = record.get("requestId")
                        if (
                            isinstance(request_id, str)
                            and is_json_integer(old_sequence)
                            and is_json_integer(new_sequence)
                        ):
                            event_order.setdefault(request_id, []).append((old_sequence, new_sequence))
                    if identity is not None and isinstance(record, Mapping):
                        retained[key][match], nested_changed = _retain_observations(
                            record, retained[key][match], (*path, key)
                        )
                        changed.merge(nested_changed)
            for order in event_order.values():
                # Numerical renumbering is harmless; reversing known local
                # event order would erase a lifecycle failure before its audit.
                if any(first[0] < second[0] and first[1] >= second[1] for first, second in pairwise(sorted(order))):
                    changed.conflicts.add((*path, key, "sequence"))
        elif not same_json(previous, retained[key]) and not _observation_advanced(
            previous, retained[key], (*path, key)
        ):
            retained[key] = copy.deepcopy(previous)
            changed.conflicts.add((*path, key))
    return retained, changed


def _cleanup_retry_errors(
    frozen_replay: Mapping[str, Any],
    sources: Sequence[Mapping[str, Any]],
    execution_arn: Any,
    bound_execution: Callable[[Any], str | None],
    *,
    final: bool,
) -> list[str]:
    """Check retry claims without promoting partial fragments to whole runs."""
    findings: list[str] = []
    frozen_retry = frozen_replay.get("retry")
    if not isinstance(frozen_retry, Mapping) or not isinstance(frozen_retry.get("operationId"), str):
        return findings
    operation_id = frozen_retry["operationId"]
    frozen_attempts = frozen_retry.get("attempts", [])
    frozen_ids = (
        {
            item["attemptId"]
            for item in frozen_attempts
            if isinstance(item, Mapping) and isinstance(item.get("attemptId"), str)
        }
        if isinstance(frozen_attempts, list)
        else set()
    )
    facts: dict[str, dict[str, Any]] = {}
    orders: list[list[str]] = []
    entries: dict[str, list[Mapping[str, Any]]] = {}
    for source in sources:
        replay = source.get("replay")
        if not isinstance(replay, Mapping):
            continue
        observed_entries = replay.get("operationExecutions", [])
        if isinstance(observed_entries, list):
            for entry in observed_entries:
                if (
                    isinstance(entry, Mapping)
                    and entry.get("operationId") == operation_id
                    and isinstance(entry.get("attemptId"), str)
                ):
                    entries.setdefault(entry["attemptId"], []).append(entry)
        if "retry" not in replay:
            continue
        retry = replay["retry"]
        if not isinstance(retry, Mapping):
            if final:
                findings.append("ASSERTION: cleanup retry observations must be an object")
            continue
        same_operation = "operationId" not in retry or same_json(retry["operationId"], operation_id)
        if not same_operation:
            findings.append("ASSERTION: cleanup retry operationId changed the frozen retry operation")
        attempts = retry.get("attempts", [])
        if not isinstance(attempts, list):
            if final:
                findings.append("ASSERTION: cleanup retry attempts must be an array")
            continue
        order = []
        for attempt in attempts:
            if not isinstance(attempt, Mapping):
                if final:
                    findings.append("ASSERTION: cleanup retry attempt must be an object")
                continue
            if "requestId" in attempt:
                bound_execution(attempt["requestId"])
            if not same_operation:
                continue
            attempt_id = attempt.get("attemptId")
            if not isinstance(attempt_id, str) or not attempt_id.strip():
                if final:
                    findings.append("ASSERTION: cleanup retry attempt lacks a non-empty attemptId")
                continue
            order.append(attempt_id)
            known = facts.setdefault(attempt_id, {})
            for key in ("requestId", "outcome"):
                if key not in attempt:
                    continue
                if key in known and not same_json(known[key], attempt[key]):
                    findings.append(f"ASSERTION: cleanup retry attempt {attempt_id} changed its known {key}")
                else:
                    known[key] = attempt[key]
        if len(set(order)) != len(order):
            findings.append("ASSERTION: cleanup retry observations repeat an attemptId")
        elif order:
            orders.append(order)

    # Each source contributes only its observed ordering edges. In particular,
    # separate singleton fragments do not imply a temporal relationship.
    successors: dict[str, set[str]] = {attempt_id: set() for attempt_id in facts}
    indegree = dict.fromkeys(facts, 0)
    for order in orders:
        for before, after in pairwise(order):
            if after not in successors[before]:
                successors[before].add(after)
                indegree[after] += 1
    ready = deque(attempt_id for attempt_id, degree in indegree.items() if degree == 0)
    visited = 0
    while ready:
        attempt_id = ready.popleft()
        visited += 1
        for following in successors[attempt_id]:
            indegree[following] -= 1
            if indegree[following] == 0:
                ready.append(following)
    if visited != len(facts):
        findings.append("ASSERTION: cleanup retry attempt order contradicts retained observations")

    if not final:
        return findings
    for attempt_id, known in facts.items():
        candidates = entries.get(attempt_id, [])
        request_id = known.get("requestId")
        if "requestId" not in known:
            candidate_requests = {
                item["requestId"]
                for item in candidates
                if isinstance(item.get("requestId"), str) and item["requestId"].strip()
            }
            if len(candidate_requests) == 1:
                request_id = next(iter(candidate_requests))
            else:
                if not candidate_requests:
                    bound_execution(None)
                findings.append(f"ASSERTION: cleanup retry attempt {attempt_id} has unresolved native correlation")
                continue
        owner = bound_execution(request_id)
        if owner is None:
            continue
        if owner != execution_arn:
            findings.append(f"ASSERTION: cleanup retry attempt {attempt_id} did not run on the suspended execution")
        matching = [item for item in candidates if item.get("requestId") == request_id]
        if not matching:
            findings.append(f"ASSERTION: cleanup retry attempt {attempt_id} lacks a correlated user-code entry")
            continue
        outcomes = [item["outcome"] for item in matching if "outcome" in item]
        if "outcome" not in known and outcomes and all(same_json(item, outcomes[0]) for item in outcomes):
            known["outcome"] = outcomes[0]
        if "outcome" in known and any(not same_json(known["outcome"], item) for item in outcomes):
            findings.append(f"ASSERTION: cleanup retry attempt {attempt_id} contradicts its user-code outcome")
    for order in orders:
        # A subset is not a new declaration of the full retry's endpoints.
        if (
            frozen_ids
            and frozen_ids <= set(order)
            and (facts[order[0]].get("outcome") != "FAILED" or facts[order[-1]].get("outcome") != "SUCCEEDED")
        ):
            findings.append("ASSERTION: cleanup retry observations do not start with failure and end with success")
    return findings


def _cleanup_checkpoint_errors(
    frozen_replay: Mapping[str, Any], sources: Sequence[Mapping[str, Any]], execution_arn: Any, *, final: bool
) -> tuple[list[str], set[str]]:
    """Reconcile supplied checkpoint facts without replacing partial views."""
    findings: list[str] = []

    def records(value: Any) -> list[Mapping[str, Any]]:
        return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []

    frozen_before = {
        item["id"]: item for item in records(frozen_replay.get("before")) if isinstance(item.get("id"), str)
    }
    frozen_after = {item["id"]: item for item in records(frozen_replay.get("after")) if isinstance(item.get("id"), str)}
    before_ids: set[str] = set(frozen_before)
    after_ids: set[str] = set(frozen_after)
    # The accepted original after snapshot was checked against service history.
    history_ids: set[str] = set(frozen_after)
    observations: dict[str, list[tuple[str, Mapping[str, Any]]]] = {}
    for source in sources:
        replay = source.get("replay")
        if isinstance(replay, Mapping):
            for view in ("before", "after"):
                if view not in replay:
                    continue
                operations = replay[view]
                if not isinstance(operations, list):
                    if final:
                        findings.append(f"ASSERTION: cleanup replay.{view} checkpoints must be an array")
                    continue
                seen: set[str] = set()
                for operation in operations:
                    operation_id = operation.get("id") if isinstance(operation, Mapping) else None
                    if not isinstance(operation_id, str) or not operation_id.strip():
                        if final:
                            findings.append(f"ASSERTION: cleanup replay.{view} checkpoint lacks an operation ID")
                        continue
                    if operation_id in seen:
                        findings.append(f"ASSERTION: cleanup replay.{view} repeats checkpoint {operation_id}")
                    seen.add(operation_id)
                    (before_ids if view == "before" else after_ids).add(operation_id)
                    observations.setdefault(operation_id, []).append((view, operation))
                    if (
                        view == "before"
                        and "status" in operation
                        and operation["status"] not in OPERATION_TERMINAL_STATUSES
                    ):
                        findings.append(f"ASSERTION: cleanup replay.before checkpoint {operation_id} is not completed")
                    prior = frozen_before.get(operation_id, frozen_after.get(operation_id))
                    if prior is not None:
                        path = ("replay", "before" if view == "before" or operation_id in frozen_before else "after")
                        _, changes = _retain_observations(prior, operation, path)
                        if changes.conflicts:
                            findings.append(
                                f"ASSERTION: cleanup replay.{view} changed frozen checkpoint {operation_id}"
                            )
        for history in records(source.get("histories")):
            if history.get("executionArn") != execution_arn:
                continue
            for operation in records(history.get("operations")):
                operation_id = operation.get("id")
                if isinstance(operation_id, str) and operation_id.strip():
                    history_ids.add(operation_id)
                    observations.setdefault(operation_id, []).append(("history", operation))

    for operation_id, observed in observations.items():
        identity: dict[str, Any] = {"id": operation_id}
        terminal: list[Mapping[str, Any]] = []
        for view, operation in observed:
            for key in ("name", "type", "parentId"):
                if key in operation:
                    if key in identity and not same_json(identity[key], operation[key]):
                        findings.append(f"ASSERTION: cleanup checkpoint {operation_id} changed its {key}")
                    else:
                        identity[key] = operation[key]
            if view == "before" or operation.get("status") in OPERATION_TERMINAL_STATUSES:
                terminal.append(operation)
            if (
                view == "after"
                and operation_id in before_ids
                and "status" in operation
                and operation["status"] not in OPERATION_TERMINAL_STATUSES
            ):
                findings.append(
                    f"ASSERTION: cleanup after checkpoint {operation_id} is incomplete despite before completion"
                )
        for index, operation in enumerate(terminal):
            for previous in terminal[:index]:
                _, changes = _retain_observations(previous, operation, ("replay", "before"))
                if changes.conflicts:
                    findings.append(
                        f"ASSERTION: cleanup completed checkpoint {operation_id} has contradictory observations"
                    )
            for view, partial in observed:
                if view != "history" and "status" not in partial:
                    _, changes = _retain_observations(operation, partial, ("replay", "before"))
                    if changes.conflicts:
                        findings.append(
                            f"ASSERTION: cleanup partial checkpoint {operation_id} contradicts its completed outcome"
                        )
    retry = frozen_replay.get("retry")
    if isinstance(retry, Mapping) and isinstance(retry.get("operationId"), str) and retry["operationId"] in before_ids:
        findings.append("ASSERTION: cleanup replay.before checkpoint contains the frozen retry operation")
    if final:
        for operation_id in before_ids - after_ids:
            findings.append(
                f"ASSERTION: cleanup before checkpoint {operation_id} lacks an after-checkpoint correlation"
            )
        for operation_id in after_ids - history_ids:
            findings.append(f"ASSERTION: cleanup after checkpoint {operation_id} lacks a service-history correlation")
    return findings, before_ids


def _cleanup_record_errors(sources: Sequence[Mapping[str, Any]], final_snapshot: Mapping[str, Any] | None) -> list[str]:
    """Reconcile every raw record's native owner, even for opaque event kinds."""
    findings: list[str] = []

    def records(value: Any) -> list[Mapping[str, Any]]:
        return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []

    def identifier(value: Any) -> TypeGuard[str]:
        return isinstance(value, str) and bool(value.strip())

    def timestamp(value: Any) -> int | float | None:
        try:
            if type(value) in (int, float) and value >= 0 and math.isfinite(value):
                return value
        except OverflowError:
            pass
        return None

    bindings: dict[str, set[str]] = {}
    starts: dict[str, int | float] = {}
    ends: dict[str, int | float] = {}
    windows: list[int | float] = []
    for source in sources:
        end = timestamp(source.get("observationEndedAtMs"))
        if end is not None:
            windows.append(end)
        for attempt in records(source.get("attempts")):
            request_id = attempt.get("requestId")
            if not identifier(request_id):
                continue
            known = bindings.setdefault(request_id, set())
            execution_arn = attempt.get("executionArn")
            if identifier(execution_arn):
                known.add(execution_arn)
            start, end = timestamp(attempt.get("startedAtMs")), timestamp(attempt.get("endedAtMs"))
            if start is not None:
                starts[request_id] = min(starts.get(request_id, start), start)
            if end is not None:
                ends[request_id] = max(ends.get(request_id, end), end)
    known_executions = {execution for observed in bindings.values() for execution in observed}
    retirement = final_snapshot.get("retirement") if final_snapshot is not None else None
    settlements = {
        (item.get("kind"), item.get("id")): item
        for item in records(retirement.get("resources") if isinstance(retirement, Mapping) else None)
        if identifier(item.get("kind")) and identifier(item.get("id"))
    }

    for source in sources:
        for collection in ("events", "sdkRequests"):
            for record in records(source.get(collection)):
                field = "kind" if collection == "events" else "method"
                if field in record and not identifier(record[field]):
                    findings.append(f"INCOMPLETE: cleanup {collection}.{field} must be a non-empty string")
                if (
                    collection == "sdkRequests"
                    and "outcome" in record
                    and record["outcome"] not in ("DISPATCHED", "REJECTED")
                ):
                    findings.append("INCOMPLETE: cleanup sdkRequests.outcome must be DISPATCHED or REJECTED")
                observed_at = timestamp(record.get("atMs"))
                if "atMs" in record and observed_at is None:
                    findings.append(f"INCOMPLETE: cleanup {collection}.atMs must be a finite non-negative number")
                for key in ("requestId", "ownerRequestId", "executionArn", "observedDuringRequestId"):
                    if (
                        key in record
                        and not (key == "observedDuringRequestId" and record[key] is None)
                        and not identifier(record[key])
                    ):
                        findings.append(f"INCOMPLETE: cleanup {collection}.{key} must identify its native owner")
                if final_snapshot is None:
                    continue

                referenced_requests: set[str] = set()
                for key in ("requestId", "ownerRequestId", "observedDuringRequestId"):
                    request_id = record.get(key)
                    if not identifier(request_id):
                        continue
                    referenced_requests.add(request_id)
                    if len(bindings.get(request_id, set())) != 1:
                        findings.append(f"INCOMPLETE: cleanup {collection}.{key} has an unresolved native binding")
                request_id = record.get("requestId", record.get("ownerRequestId"))
                execution_arn = record.get("executionArn")
                if not referenced_requests and identifier(execution_arn):
                    candidates = {request for request, known in bindings.items() if known == {execution_arn}}
                    if len(candidates) == 1:
                        referenced_requests.update(candidates)
                        request_id = next(iter(candidates))
                if not referenced_requests:
                    findings.append(f"INCOMPLETE: cleanup {collection} has no resolved native owner")
                expected = bindings.get(request_id, set()) if identifier(request_id) else set()
                if identifier(execution_arn) and execution_arn not in known_executions:
                    findings.append(f"INCOMPLETE: cleanup {collection}.executionArn has an unresolved execution owner")
                elif identifier(execution_arn) and len(expected) == 1 and execution_arn not in expected:
                    findings.append(f"ASSERTION: cleanup {collection} is attributed to a different execution")
                owner = record.get("ownerRequestId")
                if (
                    identifier(record.get("requestId"))
                    and identifier(owner)
                    and len(bindings.get(record["requestId"], set())) == 1
                    and len(bindings.get(owner, set())) == 1
                    and owner != record["requestId"]
                ):
                    findings.append(f"ASSERTION: cleanup {collection} is attributed to a different invocation")

                # Event-only discoveries need settlement coverage too: the
                # snapshot containing an event may predate its native binding.
                for referenced in referenced_requests:
                    settlement = settlements.get(("invocation", referenced), {})
                    settled_at = timestamp(settlement.get("settledAtMs"))
                    if (
                        settlement.get("outcome") != "QUIESCENT"
                        or settled_at is None
                        or (observed_at is not None and settled_at < observed_at)
                    ):
                        findings.append(f"INCOMPLETE: cleanup {collection} native owner {referenced} is not settled")
                referenced_executions = {
                    execution for referenced in referenced_requests for execution in bindings.get(referenced, set())
                }
                if identifier(execution_arn):
                    referenced_executions.add(execution_arn)
                for execution in referenced_executions:
                    settlement = settlements.get(("execution", execution), {})
                    if (
                        settlement.get("outcome") not in TERMINAL_STATUSES
                        or timestamp(settlement.get("settledAtMs")) is None
                    ):
                        findings.append(f"INCOMPLETE: cleanup {collection} execution owner {execution} is not settled")

                if observed_at is None:
                    continue  # Omitted timestamp metadata can still be completed.
                started = starts.get(request_id) if identifier(request_id) else None
                if (windows and observed_at > max(windows)) or (started is not None and observed_at < started):
                    findings.append(
                        f"INCOMPLETE: cleanup {collection}.atMs lies outside the retained observation window"
                    )
                during = record.get("observedDuringRequestId")
                if identifier(during):
                    if during not in starts or during not in ends:
                        findings.append(
                            f"INCOMPLETE: cleanup {collection}.observedDuringRequestId lacks a native interval"
                        )
                    elif not starts[during] <= observed_at <= ends[during]:
                        findings.append(
                            f"ASSERTION: cleanup {collection}.atMs lies outside its observedDuringRequestId interval"
                        )
    return findings


def _cleanup_behavior_errors(
    original: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    *,
    standard: bool,
    known_snapshots: Sequence[Mapping[str, Any]] = (),
    final: bool = False,
    saved: bool = False,
) -> list[str]:
    """Detect specific refutations without replacing frozen business evidence."""
    findings: list[str] = []

    def records(value: Any) -> list[Mapping[str, Any]]:
        return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []

    def timestamp(value: Any) -> float | None:
        if type(value) not in (int, float) or value < 0:
            return None
        try:
            return float(value) if math.isfinite(value) else None
        except OverflowError:
            return None

    attempts = {
        item["requestId"]: item for item in records(original.get("attempts")) if isinstance(item.get("requestId"), str)
    }
    events = records(original.get("events"))
    admission_sources = (original, *known_snapshots, snapshot)
    findings.extend(_cleanup_record_errors(admission_sources, snapshot if final else None))
    releases: list[float] = []
    # Absence can be completed later; an explicitly malformed release time is
    # invalid evidence even if a later snapshot replaces or omits that field.
    for source in admission_sources:
        fault_collections = [("faultsCreated", source.get("faultsCreated"))]
        retirement = source.get("retirement")
        if isinstance(retirement, Mapping):
            fault_collections.append(("retirement.faults", retirement.get("faults")))
        for location, collection in fault_collections:
            for fault in records(collection):
                if "releasedAtMs" not in fault:
                    continue
                released = timestamp(fault["releasedAtMs"])
                if released is None:
                    findings.append(f"INCOMPLETE: cleanup {location}.releasedAtMs must be a finite non-negative number")
                else:
                    releases.append(released)
    selected = original.get("selectedRequests")
    disposal_victim = (
        selected.get("victim")
        if standard
        and isinstance(selected, Mapping)
        and original.get("requirementId") in ("lmi-lifecycle-2", "lmi-lifecycle-7")
        else None
    )
    findings.extend(cleanup_admission_errors(admission_sources, disposal_victim=disposal_victim))
    for source in (snapshot, *known_snapshots):
        if isinstance(replay := source.get("replay"), Mapping):
            findings.extend(cleanup_replay_projection_errors(replay))
    if not standard:
        return findings

    if not isinstance(selected, Mapping):
        return findings
    current_attempts = {
        item["requestId"]: item for item in records(snapshot.get("attempts")) if isinstance(item.get("requestId"), str)
    }

    def role(role_name: str) -> Mapping[str, Any]:
        request_id = selected.get(role_name)
        return attempts.get(request_id, {}) if isinstance(request_id, str) else {}

    def boundary(role_name: str, kind: str) -> float | None:
        request_id = role(role_name).get("requestId")
        matches = [
            event
            for event in events
            if request_id is not None and event.get("requestId") == request_id and event.get("kind") == kind
        ]
        return timestamp(matches[0].get("atMs")) if len(matches) == 1 else None

    def native_time(role_name: str, key: str) -> float | None:
        previous = role(role_name)
        current = current_attempts.get(previous.get("requestId"), {})
        return timestamp(current[key] if key in current else previous.get(key))

    def changed_time(role_name: str, key: str) -> bool:
        previous = role(role_name)
        current = current_attempts.get(previous.get("requestId"), {})
        return key in current and timestamp(current[key]) is not None and not same_json(current[key], previous.get(key))

    def observed_events(role_name: str, kind: str) -> list[Mapping[str, Any]]:
        owner = role(role_name)
        request_id = owner.get("requestId")
        observed = []
        for source in admission_sources:
            for event in records(source.get("events")):
                if request_id is None or event.get("requestId") != request_id or event.get("kind") != kind:
                    continue
                if any(
                    key in event and not same_json(event[key], expected)
                    for key, expected in (
                        ("ownerRequestId", request_id),
                        ("executionArn", owner.get("executionArn")),
                    )
                ):
                    if final:
                        findings.append(f"INCOMPLETE: cleanup {kind} event has inconsistent native ownership")
                    continue
                if "atMs" in event and timestamp(event["atMs"]) is None and final:
                    findings.append(f"INCOMPLETE: cleanup {kind} event has an invalid timestamp")
                observed.append(event)
        return observed

    def known_interval(role_name: str) -> tuple[float | None, float | None]:
        request_id = role(role_name).get("requestId")
        native = [
            attempt
            for source in admission_sources
            for attempt in records(source.get("attempts"))
            if request_id is not None and attempt.get("requestId") == request_id
        ]
        starts = [time for attempt in native if (time := timestamp(attempt.get("startedAtMs"))) is not None]
        ends = [time for attempt in native if (time := timestamp(attempt.get("endedAtMs"))) is not None]
        return min(starts) if starts else None, max(ends) if ends else None

    requirement = original.get("requirementId")
    if requirement in ("lmi-lifecycle-2", "lmi-lifecycle-7"):
        returned, exited = boundary("victim", "wrapper_return"), boundary("victim", "task_exit")
        for event in observed_events("victim", "root_completed"):
            if "outcome" in event and not same_json(event["outcome"], role("victim").get("outcome")):
                findings.append("ASSERTION: cleanup root completion contradicts the frozen invocation outcome")
            completed_at = timestamp(event.get("atMs"))
            if completed_at is not None and any(end is not None and completed_at > end for end in (returned, exited)):
                findings.append("ASSERTION: cleanup root completion follows the frozen wrapper or task exit")
    if requirement in ("lmi-lifecycle-1", "lmi-lifecycle-4") and final:
        # Native metadata can widen a provisional interval. A barrier claim is
        # disproved only after every supplied native interval is available.
        intervals = [known_interval(name) for name in ("victim", "peer")]
        starts = [start for start, _ in intervals if start is not None]
        ends = [end for _, end in intervals if end is not None]
        if len(starts) == len(ends) == 2:
            for name in ("victim", "peer"):
                for event in observed_events(name, "barrier_arrived"):
                    arrived = timestamp(event.get("atMs"))
                    if arrived is not None and not max(starts) <= arrived < min(ends):
                        findings.append("PLACEMENT: cleanup barrier lies outside the established native overlap")
    if requirement in ("lmi-lifecycle-3", "lmi-lifecycle-4"):
        profile = original.get("profile")
        profile = profile if isinstance(profile, Mapping) else {}
        fixture_budgets = original.get("budgets")
        fixture_budgets = fixture_budgets if isinstance(fixture_budgets, Mapping) else {}
        start, end = known_interval("victim")
        configured_timeout = timestamp(profile.get("timeoutMs"))
        inflight, exited = boundary("victim", "work_in_flight"), boundary("victim", "task_exit")
        timeout_events = observed_events("victim", "platform_timeout")
        for event in timeout_events:
            if "source" in event and event["source"] != "platform":
                findings.append("ASSERTION: cleanup timeout source is not the observed platform deadline")
            timed_out = timestamp(event.get("atMs"))
            if timed_out is None:
                continue
            if final and (
                (start is not None and configured_timeout is not None and timed_out < start + configured_timeout)
                or (end is not None and timed_out > end)
            ):
                findings.append("ASSERTION: cleanup timeout falls outside the configured native deadline")
            if (inflight is not None and timed_out <= inflight) or (exited is not None and timed_out > exited):
                findings.append("ASSERTION: cleanup timeout contradicts the frozen in-flight work or task exit")
            for kind, budget_key in (("admission_closed", "admissionMs"), ("task_exit", "cleanupMs")):
                measured, budget = boundary("victim", kind), timestamp(fixture_budgets.get(budget_key))
                if measured is not None and budget is not None and not timed_out <= measured <= timed_out + budget:
                    findings.append(f"ASSERTION: cleanup timeout contradicts the frozen {budget_key} measurement")
        if requirement == "lmi-lifecycle-4":
            if profile.get("recoveryPolicy") == "replace-worker":
                recovery_id = role("recovery").get("requestId")
                for source in admission_sources:
                    for attempt in records(source.get("attempts")):
                        if (
                            recovery_id is not None
                            and attempt.get("requestId") == recovery_id
                            and "replacesWorkerId" in attempt
                            and not same_json(attempt["replacesWorkerId"], role("victim").get("workerId"))
                        ):
                            findings.append("PLACEMENT: cleanup recovery replacement changed the frozen failed worker")
            recovery_start, _ = known_interval("recovery")
            recovery_budget = timestamp(fixture_budgets.get("recoveryMs"))
            for event in observed_events("recovery", "worker_recovered"):
                recovered = timestamp(event.get("atMs"))
                if recovered is None:
                    continue
                if final and recovery_start is not None and recovered < recovery_start:
                    findings.append("ASSERTION: cleanup worker recovery precedes its native invocation")
                for timeout_event in timeout_events:
                    timed_out = timestamp(timeout_event.get("atMs"))
                    if (
                        timed_out is not None
                        and recovery_budget is not None
                        and not (timed_out <= recovered <= timed_out + recovery_budget)
                    ):
                        findings.append("ASSERTION: cleanup worker recovery exceeds the frozen recovery measurement")
                if final:
                    for peer in selected:
                        if isinstance(peer, str) and peer.startswith("peer"):
                            _, peer_end = known_interval(peer)
                            if peer_end is not None and recovered > peer_end:
                                findings.append("PLACEMENT: cleanup worker recovery outlived established peer capacity")
    if requirement in ("lmi-lifecycle-3", "lmi-lifecycle-4"):
        limits = [boundary("victim", "task_exit")]
        if requirement == "lmi-lifecycle-4":
            limits.extend(timestamp(event.get("atMs")) for event in observed_events("recovery", "worker_recovered"))
        known_limits = [time for time in limits if time is not None]
        # A later release acknowledgment cannot erase an earlier release from
        # either observation records or a provisional/final retirement record.
        if releases and known_limits and min(releases) <= max(known_limits):
            findings.append("ASSERTION: cleanup evidence reports fault release before the asserted cleanup or recovery")
    if requirement == "lmi-lifecycle-6" and (
        changed_time("victim", "endedAtMs") or changed_time("warm", "startedAtMs")
    ):
        start, end, returned = (
            native_time("warm", "startedAtMs"),
            native_time("victim", "endedAtMs"),
            boundary("victim", "wrapper_return"),
        )
        if start is not None and ((end is not None and start < end) or (returned is not None and start <= returned)):
            findings.append("PLACEMENT: cleanup evidence disproves sequential warm reuse")
    if requirement == "lmi-lifecycle-4" and (
        changed_time("victim", "endedAtMs") or changed_time("recovery", "startedAtMs")
    ):
        start, end, timeout = (
            native_time("recovery", "startedAtMs"),
            native_time("victim", "endedAtMs"),
            boundary("victim", "platform_timeout"),
        )
        if start is not None and timeout is not None and start < timeout:
            findings.append("ASSERTION: cleanup evidence places recovery before the victim timeout")
        victim, recovery = role("victim"), role("recovery")
        if (
            original.get("runtime") == "python"
            and victim.get("processId")
            and victim.get("processId") == recovery.get("processId")
            and start is not None
            and end is not None
            and start < end
        ):
            findings.append("PLACEMENT: cleanup evidence places Python recovery before victim native completion")
    if requirement in ("lmi-lifecycle-5", "lmi-lifecycle-6"):
        sources = (original, *known_snapshots, snapshot)
        executions_by_request: dict[str, set[str]] = {}
        for source in sources:
            for attempt in records(source.get("attempts")):
                request_id, execution_arn = attempt.get("requestId"), attempt.get("executionArn")
                if (
                    isinstance(request_id, str)
                    and request_id.strip()
                    and isinstance(execution_arn, str)
                    and execution_arn.strip()
                ):
                    executions_by_request.setdefault(request_id, set()).add(execution_arn)

        def bound_execution(request_id: Any) -> str | None:
            # A new native request may only gain its execution binding during
            # retirement. Do not turn that provisional absence into a failure.
            if not isinstance(request_id, str) or not request_id.strip():
                if final:
                    findings.append("INCOMPLETE: cleanup replay/activity lacks an identified native request")
                return None
            if request_id not in attempts and not final:
                return None
            bindings = executions_by_request.get(request_id, set())
            if len(bindings) != 1:
                if final:
                    findings.append(
                        f"INCOMPLETE: cleanup native request {request_id} has an unresolved or conflicting execution binding"
                    )
                return None
            return next(iter(bindings))

        if requirement == "lmi-lifecycle-5":
            replay = original.get("replay")
            replay = replay if isinstance(replay, Mapping) else {}
            completed = {
                operation["id"]
                for operation in records(replay.get("before"))
                if isinstance(operation.get("id"), str) and operation.get("status") in OPERATION_TERMINAL_STATUSES
            }
            expected_counts = {
                item["operationId"]: item["expectedCount"]
                for item in records(replay.get("sideEffects"))
                if isinstance(item.get("operationId"), str) and is_json_integer(item.get("expectedCount"))
            }
            victim = role("victim")
            checkpoint_errors, observed_before = _cleanup_checkpoint_errors(
                replay, sources, victim.get("executionArn"), final=final
            )
            findings.extend(checkpoint_errors)
            completed.update(observed_before)
            returned = boundary("victim", "wrapper_return")
            resume_id = role("resume").get("requestId")
            for source in sources:
                for event in records(source.get("events")):
                    if event.get("kind") != "pending" or event.get("requestId") != victim.get("requestId"):
                        continue
                    request_id = event.get("requestId")
                    execution_arn = bound_execution(request_id)
                    if execution_arn is None:
                        continue
                    if any(
                        key in event and event[key] != expected
                        for key, expected in (("ownerRequestId", request_id), ("executionArn", execution_arn))
                    ):
                        if final:
                            findings.append("INCOMPLETE: cleanup PENDING event has inconsistent native ownership")
                        continue
                    if "atMs" not in event:
                        continue  # A partial event may gain its timestamp during collection.
                    pending_at = timestamp(event["atMs"])
                    if pending_at is None:
                        if final:
                            findings.append("INCOMPLETE: cleanup PENDING event has an invalid timestamp")
                    elif returned is not None and pending_at > returned:
                        findings.append("ASSERTION: cleanup PENDING event follows the frozen wrapper return")
                for attempt in records(source.get("attempts")):
                    started = timestamp(attempt.get("startedAtMs"))
                    if (
                        isinstance(resume_id, str)
                        and attempt.get("requestId") == resume_id
                        and started is not None
                        and returned is not None
                        and started <= returned
                    ):
                        findings.append(
                            "ASSERTION: cleanup evidence places resume before the frozen PENDING wrapper return"
                        )
                if "replay" not in source:
                    continue
                observed_replay = source["replay"]
                if not isinstance(observed_replay, Mapping):
                    if final:
                        findings.append("INCOMPLETE: cleanup replay observations must be an object")
                    continue
                findings.extend(cleanup_replay_history_errors(replay, observed_replay))
                entries = observed_replay.get("operationExecutions", [])
                if not isinstance(entries, list):
                    if final:
                        findings.append("INCOMPLETE: cleanup operation executions must be an array")
                    entries = []
                for entry in entries:
                    if not isinstance(entry, Mapping):
                        if final:
                            findings.append("INCOMPLETE: cleanup operation execution must be an object")
                        continue
                    request_id, operation_id = entry.get("requestId"), entry.get("operationId")
                    execution_arn = bound_execution(request_id)
                    if not isinstance(operation_id, str) or not operation_id.strip():
                        if final:
                            findings.append("INCOMPLETE: cleanup operation execution lacks an operationId")
                        continue
                    if (
                        execution_arn is not None
                        and execution_arn == victim.get("executionArn")
                        and request_id != victim.get("requestId")
                        and operation_id in completed
                    ):
                        findings.append(repeated_completed_body_error(operation_id))
                if final:
                    findings.extend(_side_effect_observation_errors(source, saved=saved))
                counts = observed_replay.get("sideEffects", [])
                if not isinstance(counts, list):
                    if final:
                        findings.append("INCOMPLETE: cleanup side-effect observations must be an array")
                    counts = []
                for count in counts:
                    if not isinstance(count, Mapping):
                        if final:
                            findings.append("INCOMPLETE: cleanup side-effect observation must be an object")
                        continue
                    operation_id = count.get("operationId")
                    if not isinstance(operation_id, str) or not operation_id.strip():
                        if final:
                            findings.append("INCOMPLETE: cleanup side-effect observation lacks an operationId")
                        continue
                    expected = expected_counts.get(operation_id)
                    findings.extend(cleanup_replay_count_errors(count, expected))
            findings.extend(
                _cleanup_retry_errors(replay, sources, victim.get("executionArn"), bound_execution, final=final)
            )
        else:
            victim_id = role("victim").get("requestId")
            exits = [
                event
                for event in events
                if victim_id is not None and event.get("requestId") == victim_id and event.get("kind") == "task_exit"
            ]
            findings.extend(
                cleanup_late_activity_errors(
                    original, sources, exits[0] if len(exits) == 1 else {}, bound_execution, final=final
                )
            )
    return findings


def cleanup_replay_projection_errors(replay: Mapping[str, Any]) -> list[str]:
    if "projectionErrors" not in replay:
        return []
    raw = replay["projectionErrors"]
    if not isinstance(raw, list) or any(not isinstance(error, str) or not error.strip() for error in raw):
        return ["INCOMPLETE: cleanup replay projection errors are malformed"]
    return [f"INCOMPLETE: cleanup replay projection: {error}" for error in raw]


def _check_active_retirement(validator: _Evidence) -> None:
    current = validator.value
    active = any(item.get("activeAtObservationEnd") is True for item in validator.attempts.values())
    deferred = deferred_service_requests(current)
    if not active and not deferred and "retirementSnapshot" not in current:
        return
    snapshot = current.get("retirementSnapshot")
    if not isinstance(snapshot, Mapping):
        validator.error("active-at-cutoff or deferred request evidence requires an independent retirement audit")
        return
    if (
        any(
            not same_json(snapshot.get(key), current.get(key))
            for key in ("runId", "caseId", "requirementId", "profile")
        )
        or snapshot.get("coverageProfile", "standard") != current.get("coverageProfile", "standard")
        or not same_json(current.get("runtime"), snapshot.get("runtime"))
        or retirement_artifact_errors(current, snapshot)
    ):
        validator.error("independent retirement snapshot belongs to a different scope")
        return
    independent = validate_retirement_evidence(snapshot)
    if independent:
        validator.errors.extend(independent)
        return

    request_ids = {attempt["requestId"] for attempt in snapshot["attempts"]}
    if request_ids != set(validator.attempts) | deferred.keys():
        validator.error("independent retirement requests must match the frozen and deferred inventories")
    for finding in (
        active_pre_cutoff_errors(current, snapshot)
        + frozen_attempt_retirement_errors(current, snapshot)
        + frozen_history_work_errors(current, snapshot)
    ):
        validator.error(finding)

    outer_retirement = current.get("retirement")
    if isinstance(outer_retirement, Mapping):
        for field in ("resources", "faults"):
            audited = {record["id"]: record for record in snapshot["retirement"][field]}
            projected = validator.records(outer_retirement.get(field), f"retirement.{field}")
            for record in projected:
                key = record.get("id")
                if not isinstance(key, str) or not same_json(record, audited.get(key)):
                    validator.error(f"projected retirement {field} contradicts the audited settlement for {key}")

    validator.errors.extend(
        _cleanup_behavior_errors(current, snapshot, standard=validator.standard_behavior, final=True, saved=True)
    )


def validate_evidence(
    evidence: Mapping[str, Any],
    *,
    phase: Literal["observations", "final"] = "final",
    retired_requests: Mapping[str, set[str]] | None = None,
) -> list[str]:
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
            validator.retirement(retired_requests=retired_requests)
            _check_active_retirement(validator)
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
            _check_active_retirement(validator)
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


def controls_only_retirement(evidence: Mapping[str, Any]) -> bool:
    retirement = evidence.get("retirement")
    return isinstance(retirement, Mapping) and retirement.get("mode") == "controls-only"


def _validate_controls_only(validator: _Evidence) -> list[str]:
    value = validator.value
    validator.envelope()
    collection = validator.collection()
    validator.number(value.get("observationEndedAtMs"), "observationEndedAtMs")
    for key in ("attempts", "events", "histories", "sdkRequests", "faultsCreated"):
        if value.get(key) != []:
            validator.error(f"controls-only retirement cannot contain {key}")
    if value.get("selectedRequests") != {} or "replay" in value or collection.get("watermarks") != {}:
        validator.error("controls-only retirement cannot assert runtime observations")
    resources = validator.records(value.get("resourcesCreated"), "resourcesCreated", nonempty=True)
    if any(resource.get("kind") != "control" for resource in resources):
        validator.error("controls-only retirement must own only control objects")
    retirement = validator.mapping(value.get("retirement"), "retirement")
    launches = validator.records(retirement.get("undispatchedLaunches"), "retirement.undispatchedLaunches")
    markers: set[str] = set()
    for launch in launches:
        marker = validator.string(launch.get("marker"), "undispatched launch marker")
        validator.string(launch.get("durableExecutionName"), "undispatched launch execution name")
        prepared = launch.get("dispatchState") == "PREPARED" and not any(
            field in launch for field in ("arn", "executionArn", "requestId")
        )
        if (
            marker in markers
            or launch.get("functionArn") != validator.profile.get("functionArn")
            or not (prepared or invoke_rejection_proven(launch))
        ):
            validator.error("controls-only retirement has an unresolved or mismatched launch intent")
        markers.add(marker)
    validator.retirement()
    if validator.errors:
        return validator.errors

    receipts = validator.records(value.get("controlRecords"), "controlRecords", nonempty=True)
    states = {
        "hold": ("ACKNOWLEDGED", "REJECTED"),
        "release": ("ACKNOWLEDGED",),
        "delete": ("ACKNOWLEDGED",),
    }
    for record in receipts:
        validator.string(record.get("gate"), "controlRecords.gate")
        action = record.get("action")
        if not isinstance(action, str) or action not in states:
            validator.error("controls-only retirement contains an unknown control action")
        elif record.get("state") not in states[action]:
            validator.error("controls-only retirement contains an unresolved control receipt")
    if validator.errors:
        return validator.errors
    holds = {
        record.get("gate")
        for record in receipts
        if record.get("action") == "hold" and record.get("state") != "REJECTED"
    }
    if len(holds) != len(resources) or holds != {resource.get("gate") for resource in resources}:
        validator.error("controls-only inventory must cover every possibly held control")
    if any(record.get("gate") not in holds and record.get("state") != "REJECTED" for record in receipts):
        validator.error("controls-only receipts refer to a control outside the inventory")
    settlements = {item["id"]: item for item in retirement["resources"]}
    case_id, run_id = value["caseId"], value["runId"]
    prefix = f"/runs/{run_id}/cases/{case_key(case_id)}/control/"
    buckets = {urlsplit(resource["id"]).netloc for resource in resources}
    if len(buckets) != 1:
        validator.error("controls-only retirement refers to multiple control buckets")
    for resource in resources:
        identifier = urlsplit(resource["id"])
        gate = validator.string(resource.get("gate"), "control gate")
        if (
            "/" in gate
            or gate in (".", "..")
            or (
                identifier.scheme != "s3"
                or not identifier.netloc
                or identifier.path != prefix + gate
                or identifier.query
                or identifier.fragment
            )
        ):
            validator.error("controls-only retirement references a control outside the admitted case")
        records = [record for record in receipts if record.get("gate") == gate]
        holds_for_gate = [
            record for record in records if record.get("action") == "hold" and record.get("state") == "ACKNOWLEDGED"
        ]
        releases = [
            record for record in records if record.get("action") == "release" and record.get("state") == "ACKNOWLEDGED"
        ]
        deletes = [
            record for record in records if record.get("action") == "delete" and record.get("state") == "ACKNOWLEDGED"
        ]
        if len(holds_for_gate) != 1 or len(releases) != 1 or len(deletes) != 1:
            validator.error(f"control {gate} requires unique hold, release and deletion receipts")
            continue
        receipt_times = [
            (
                validator.number(record.get("started"), f"control {gate} {action} started"),
                validator.number(record.get("time"), f"control {gate} {action} time"),
            )
            for action, record in zip(
                ("hold", "release", "delete"), (holds_for_gate[0], releases[0], deletes[0]), strict=True
            )
        ]
        (hold_started, held), (release_started, released), (delete_started, deleted) = receipt_times
        if (
            not hold_started <= release_started <= released <= delete_started <= deleted
            or not hold_started <= held <= released
            or (holds_for_gate[0]["state"] == "ACKNOWLEDGED" and held > release_started)
        ):
            validator.error(f"control {gate} has out-of-order retirement receipts")
        settlement = settlements[resource["id"]]
        evidence_uri = settlement["evidenceUri"]
        receipt_uri = urlsplit(evidence_uri)
        if (
            settlement.get("gate") != gate
            or settlement["settledAtMs"] != deleted * 1000
            or evidence_uri not in collection["evidenceUris"]
            or receipt_uri.scheme != "file"
            or receipt_uri.netloc
            or receipt_uri.query
            or receipt_uri.fragment
            or not receipt_uri.path.endswith(f"/{case_key(case_id)}/control-records.json")
        ):
            validator.error(f"control {gate} settlement does not match its retained deletion receipt")
    return validator.errors


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


def retained_fault_releases(
    sources: Sequence[Mapping[str, Any]], retired_at: Any
) -> tuple[list[int | float], list[str]]:
    """Check observed release receipts even if later snapshots replace them."""

    def timestamp(value: Any) -> int | float | None:
        if type(value) is int and value >= 0:
            return value
        if type(value) is float and math.isfinite(value) and value >= 0:
            return value
        return None

    end = timestamp(retired_at)
    releases: list[int | float] = []
    errors: list[str] = []
    for source in sources:
        retirement = source.get("retirement")
        for location, records in (
            ("faultsCreated", source.get("faultsCreated")),
            ("retirement.faults", retirement.get("faults") if isinstance(retirement, Mapping) else None),
        ):
            for fault in records if isinstance(records, list) else ():
                if not isinstance(fault, Mapping) or "releasedAtMs" not in fault:
                    continue
                released = timestamp(fault["releasedAtMs"])
                if released is None:
                    errors.append(f"INCOMPLETE: cleanup {location}.releasedAtMs must be a finite non-negative number")
                else:
                    releases.append(released)
                    if end is not None and released > end:
                        errors.append("INCOMPLETE: observed fault release occurred after retirement ended")
    return releases, list(dict.fromkeys(errors))


def _validate_sdk_dispatches(validator: _Evidence, events: Sequence[Mapping[str, Any]]) -> None:
    call_kinds = {method: kind for kind, method in _CALL_METHODS.items()}
    dispatches: set[tuple[str, str, str]] = set()
    for request in validator.value["sdkRequests"]:
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
            for event in events
        ):
            validator.error(f"retirement cannot correlate dispatched SDK request with its unique {kind}")
        dispatches.add(key)


def validate_observed_retirement(evidence: Mapping[str, Any]) -> list[str]:
    """Verify settlement against the captured case and its retirement record."""
    requirement = evidence.get("requirementId")
    validator = _Evidence(
        evidence, requirement_ids=(requirement,) if isinstance(requirement, str) else (), standard_behavior=False
    )
    try:
        validator.common(validate_behavior=False)
        validator.retirement()
        if not validator.errors:
            _validate_sdk_dispatches(validator, normalize_retirement_events(validator.events, validator.errors))
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError) as error:
        validator.error(f"malformed retirement evidence: {error}")
    return validator.errors


def validate_retirement_evidence(evidence: Mapping[str, Any], *, allow_no_launches: bool = False) -> list[str]:
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
        if deferred_service_requests(evidence):
            validator.error("independent retirement evidence must settle every deferred service request")
            return validator.errors
        if controls_only_retirement(evidence):
            return _validate_controls_only(validator)
        validator.common(validate_behavior=False, allow_no_launches=allow_no_launches)
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
        _validate_sdk_dispatches(validator, validator.events)
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
