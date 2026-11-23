# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contract constants, the validator protocol and exact JSON comparison helpers."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, Protocol, TypeGuard

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


def mappings(value: Any) -> list[Mapping[str, Any]]:
    """The objects in a JSON array; nothing for any other value."""
    return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def timestamp(value: Any) -> int | float | None:
    """A finite non-negative JSON number that is not a boolean, else ``None``."""
    if type(value) is int and value >= 0:
        return value
    if type(value) is float and math.isfinite(value) and value >= 0:
        return value
    return None


_SUSPENSION_OPERATION_TYPES = frozenset({"WAIT", "CALLBACK"})

# The error type the service reports in InvocationCompletedDetails when it
# rejects an invocation for capacity before any handler runs. Confirmed from
# retained InvocationCompleted records in aws/aws-durable-execution-sdk-python
# LMI runs 43973096696, 43978687067 and 44191933207.
UNDISPATCHED_ERROR_TYPES = frozenset({"TooManyRequestsException"})


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


def same_checkpoint(left: Any, right: Any, *, include_timing: bool = False) -> bool:
    """Compare checkpoint identity, result and failure.

    A wait or callback's service completion time is not part of its
    checkpoint: SDK snapshots cannot carry it, so only same-source comparisons
    include it. Every other field, including step timing, must match.
    """
    if not isinstance(left, Mapping) or not isinstance(right, Mapping):
        return False
    ignored = {"result"} if same_checkpoint_result(left, right) else set()
    if not include_timing and left.get("type") in _SUSPENSION_OPERATION_TYPES:
        ignored.add("endedAtMs")
    return same_json(
        {key: value for key, value in left.items() if key not in ignored},
        {key: value for key, value in right.items() if key not in ignored},
    )


def same_checkpoint_set(left: Mapping[Any, Any], right: Mapping[Any, Any]) -> bool:
    return left.keys() == right.keys() and all(
        same_checkpoint(operation, right[identity]) for identity, operation in left.items()
    )
