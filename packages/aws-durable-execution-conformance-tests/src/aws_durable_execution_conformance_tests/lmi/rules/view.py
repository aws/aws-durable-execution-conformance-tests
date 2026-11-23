# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Per-case evaluation state: the evidence frame, derived indexes and findings."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

from aws_durable_execution_conformance_tests.lmi.rules.frames import CaseFrames
from aws_durable_execution_conformance_tests.lmi.rules.values import (
    COMPLETED_OPERATION_STATUSES,
    REQUIREMENT_IDS,
    _valid_fingerprint,
    is_json_integer,
)


class CaseView:
    """Mutable state for one evaluation of one case.

    Rules read ``value`` without mutating it and record findings with ``error``.
    The ``common`` rule fills the ``attempts``, ``selected``, ``events`` and
    ``profile`` indexes, so scenario rules must run after it. The independent
    retirement audit then normalizes ``events`` and adds retirement completions,
    and task-alias checks append to ``errors`` directly. One frame per case
    today; later passes add cleanup and retirement frames through ``CaseFrames``.
    """

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

    @classmethod
    def from_frames(
        cls,
        frames: CaseFrames,
        requirement_ids: tuple[str, ...] = REQUIREMENT_IDS,
        *,
        standard_behavior: bool = True,
    ) -> CaseView:
        return cls(frames.frozen.value, requirement_ids, standard_behavior=standard_behavior)
