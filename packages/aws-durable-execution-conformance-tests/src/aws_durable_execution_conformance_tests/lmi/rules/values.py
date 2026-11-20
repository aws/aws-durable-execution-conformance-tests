# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contract constants, the validator protocol and exact JSON comparison helpers."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any, Literal, Protocol, TypeGuard

SCHEMA_VERSION = 1
REQUIREMENT_IDS = tuple(f"lmi-lifecycle-{index}" for index in range(1, 8))
RUNTIMES = ("javascript", "java", "python")
RECOVERY_POLICIES = ("reuse-worker", "replace-worker")
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
