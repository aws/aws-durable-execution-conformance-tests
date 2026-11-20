# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Cleanup re-check state: frozen evidence plus later snapshots of the same run."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from functools import cached_property
from typing import Any

from aws_durable_execution_conformance_tests.lmi.rules.common import normalize_retirement_events
from aws_durable_execution_conformance_tests.lmi.rules.replay import retry_body_identity
from aws_durable_execution_conformance_tests.lmi.rules.values import is_json_integer, mappings, same_json, timestamp


class ReconcileView:
    """Mutable state for one cleanup re-check of one frozen case.

    ``original`` is the frozen evidence. ``sources`` holds it, then the known
    cleanup snapshots, then ``snapshot``, the newest one. Rules record findings
    in ``findings``; some helpers below record findings too, so call them in the
    order the checks need.
    """

    def __init__(
        self,
        original: Mapping[str, Any],
        snapshot: Mapping[str, Any],
        *,
        standard: bool,
        known_snapshots: Sequence[Mapping[str, Any]] = (),
        observation_snapshots: Sequence[Mapping[str, Any]] = (),
        final: bool = False,
    ) -> None:
        self.original = original
        self.snapshot = snapshot
        self.standard = standard
        self.known_snapshots = known_snapshots
        self.observation_snapshots = observation_snapshots
        self.final = final
        self.findings: list[str] = []
        self.attempts = {
            item["requestId"]: item
            for item in mappings(original.get("attempts"))
            if isinstance(item.get("requestId"), str)
        }
        self.events = mappings(original.get("events"))
        self.sources = (original, *known_snapshots, snapshot)

    # Fields below are read on first use, at the point the re-checks need them.
    @cached_property
    def requirement(self) -> Any:
        return self.original.get("requirementId")

    @cached_property
    def has_selection(self) -> bool:
        """Scenario re-checks run only for evidence with a selectedRequests object."""
        return isinstance(self.original.get("selectedRequests"), Mapping)

    @cached_property
    def selected(self) -> Mapping[str, Any]:
        selected = self.original.get("selectedRequests")
        return selected if isinstance(selected, Mapping) else {}

    @cached_property
    def current_attempts(self) -> dict[Any, Mapping[str, Any]]:
        return {
            item["requestId"]: item
            for item in mappings(self.snapshot.get("attempts"))
            if isinstance(item.get("requestId"), str)
        }

    @cached_property
    def profile(self) -> Mapping[str, Any]:
        profile = self.original.get("profile")
        return profile if isinstance(profile, Mapping) else {}

    @cached_property
    def budgets(self) -> Mapping[str, Any]:
        budgets = self.original.get("budgets")
        return budgets if isinstance(budgets, Mapping) else {}

    @cached_property
    def window_end(self) -> int | float | None:
        return timestamp(self.original.get("observationEndedAtMs"))

    @cached_property
    def executions_by_request(self) -> dict[str, set[str]]:
        executions_by_request: dict[str, set[str]] = {}
        for source in self.sources:
            for attempt in mappings(source.get("attempts")):
                request_id, execution_arn = attempt.get("requestId"), attempt.get("executionArn")
                if (
                    isinstance(request_id, str)
                    and request_id.strip()
                    and isinstance(execution_arn, str)
                    and execution_arn.strip()
                ):
                    executions_by_request.setdefault(request_id, set()).add(execution_arn)
        return executions_by_request

    def at_or_after(
        self, observation: Mapping[str, Any], boundary_event: Mapping[str, Any], *, require_sequence: bool = False
    ) -> bool | None:
        at = timestamp(observation.get("atMs"))
        closed = timestamp(boundary_event.get("atMs"))
        if at is None or closed is None:
            return None
        if at != closed:
            return at > closed
        identity_fields = (
            "kind",
            "requestId",
            "atMs",
            "method",
            "probe",
            "taskId",
            "callId",
            "gateId",
            "operationId",
            "attemptId",
            "attempt",
        )

        def local_match(rows: Sequence[Mapping[str, Any]], target: Mapping[str, Any]) -> Mapping[str, Any] | None:
            if any(row is target for row in rows):
                return target
            body_identity = retry_body_identity(target) if target.get("kind") == "body" else None
            fields = (
                tuple(key for key in identity_fields if key not in ("attemptId", "attempt"))
                if body_identity is not None
                else identity_fields
            )
            matches = [
                row
                for row in rows
                if all(same_json(row.get(key), target.get(key)) for key in fields)
                and (body_identity is None or retry_body_identity(row) == body_identity)
            ]
            return matches[0] if len(matches) == 1 else None

        orders: set[bool] = set()
        timestamp_fallback = False
        for source in self.sources:
            raw = [
                *normalize_retirement_events(mappings(source.get("events")), []),
                *mappings(source.get("sdkRequests")),
            ]
            left, right = local_match(raw, observation), local_match(raw, boundary_event)
            if left is None or right is None or left is right:
                continue
            sequences = [row["sequence"] for row in (left, right) if "sequence" in row]
            if any(not is_json_integer(sequence) or sequence < 0 for sequence in sequences):
                self.findings.append("INCOMPLETE: same-time observations need valid semantic sequences")
                continue
            if len(sequences) < 2:
                timestamp_fallback |= not require_sequence
                continue
            if sequences[0] == sequences[1]:
                self.findings.append("INCOMPLETE: same-time observations need distinct semantic sequences")
                continue
            orders.add(sequences[0] > sequences[1])
        if len(orders) == 1:
            return next(iter(orders))
        if not orders and timestamp_fallback:
            return True if self.final else None
        if self.final:
            self.findings.append(
                "INCOMPLETE: same-time observations need sequences in one snapshot to establish their order"
            )
        return None

    def role(self, role_name: str) -> Mapping[str, Any]:
        request_id = self.selected.get(role_name)
        return self.attempts.get(request_id, {}) if isinstance(request_id, str) else {}

    def boundary_record(self, role_name: str, kind: str) -> Mapping[str, Any]:
        request_id = self.role(role_name).get("requestId")
        matches = [
            event
            for event in self.events
            if request_id is not None and event.get("requestId") == request_id and event.get("kind") == kind
        ]
        return matches[0] if len(matches) == 1 else {}

    def boundary(self, role_name: str, kind: str) -> int | float | None:
        return timestamp(self.boundary_record(role_name, kind).get("atMs"))

    def role_attempts(self, role_name: str) -> list[Mapping[str, Any]]:
        request_id = self.role(role_name).get("requestId")
        if request_id is None:
            return []
        return [
            attempt
            for source in self.sources
            for attempt in mappings(source.get("attempts"))
            if attempt.get("requestId") == request_id
        ]

    def platform_times(self, role_name: str, key: str) -> list[int | float]:
        return [
            time
            for attempt in self.role_attempts(role_name)
            if attempt.get(f"{key}Source") == "platform" and (time := timestamp(attempt.get(key))) is not None
        ]

    def native_time(self, role_name: str, key: str) -> int | float | None:
        authoritative = self.platform_times(role_name, key)
        if authoritative:
            return max(authoritative) if key == "startedAtMs" else min(authoritative)
        previous = self.role(role_name)
        current = self.current_attempts.get(previous.get("requestId"), {})
        return timestamp(current[key] if key in current else previous.get(key))

    def changed_time(self, role_name: str, key: str) -> bool:
        previous = timestamp(self.role(role_name).get(key))
        current = self.native_time(role_name, key)
        return current is not None and current != previous

    def observed_events(self, role_name: str, kind: str) -> list[Mapping[str, Any]]:
        owner = self.role(role_name)
        request_id = owner.get("requestId")
        observed = []
        for source in self.sources:
            rows = mappings(source.get("events"))
            if kind in ("task_start", "task_exit"):
                rows = normalize_retirement_events(rows, [])
            for event in rows:
                if request_id is None or event.get("requestId") != request_id or event.get("kind") != kind:
                    continue
                if any(
                    key in event and not same_json(event[key], expected)
                    for key, expected in (
                        ("ownerRequestId", request_id),
                        ("executionArn", owner.get("executionArn")),
                    )
                ):
                    if self.final:
                        self.findings.append(f"INCOMPLETE: cleanup {kind} event has inconsistent native ownership")
                    continue
                if "atMs" in event and timestamp(event["atMs"]) is None and self.final:
                    self.findings.append(f"INCOMPLETE: cleanup {kind} event has an invalid timestamp")
                observed.append(event)
        return observed

    def known_interval(self, role_name: str) -> tuple[int | float | None, int | float | None]:
        native = self.role_attempts(role_name)
        starts = [time for attempt in native if (time := timestamp(attempt.get("startedAtMs"))) is not None]
        ends = [time for attempt in native if (time := timestamp(attempt.get("endedAtMs"))) is not None]
        platform_starts = self.platform_times(role_name, "startedAtMs")
        platform_ends = self.platform_times(role_name, "endedAtMs")
        return (
            max(platform_starts) if platform_starts else min(starts, default=None),
            min(platform_ends) if platform_ends else max(ends, default=None),
        )

    def bound_execution(self, request_id: Any) -> str | None:
        # A new native request may only gain its execution binding during
        # retirement. Do not turn that provisional absence into a failure.
        if not isinstance(request_id, str) or not request_id.strip():
            if self.final:
                self.findings.append("INCOMPLETE: cleanup replay/activity lacks an identified native request")
            return None
        if request_id not in self.attempts and not self.final:
            return None
        bindings = self.executions_by_request.get(request_id, set())
        if len(bindings) != 1:
            if self.final:
                self.findings.append(
                    f"INCOMPLETE: cleanup native request {request_id} has an unresolved or conflicting execution binding"
                )
            return None
        return next(iter(bindings))
