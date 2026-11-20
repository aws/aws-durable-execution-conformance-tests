# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Lifecycle 1 (and the lifecycle 4 cohort): native overlap and execution isolation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from aws_durable_execution_conformance_tests.lmi.rules.values import (
    mappings,
    same_json,
    timestamp,
)
from aws_durable_execution_conformance_tests.lmi.rules.view import CaseView

if TYPE_CHECKING:
    from aws_durable_execution_conformance_tests.lmi.rules.reconcile import ReconcileView


def overlap(view: CaseView) -> None:
    if not view.require_roles("victim", "peer"):
        return
    victim, peer = view.selected["victim"], view.selected["peer"]
    if victim.get("executionArn") == peer.get("executionArn"):
        view.error("overlap needs distinct durable executions", "PLACEMENT")
    if victim.get("environmentId") != peer.get("environmentId"):
        view.error("selected requests did not overlap in the same environment", "PLACEMENT")
    runtime = view.value.get("runtime")
    if runtime == "javascript" and any(victim.get(key) != peer.get(key) for key in ("workerId", "processId")):
        view.error("JavaScript overlap requires the same Node.js worker and process", "PLACEMENT")
    if runtime == "java" and victim.get("processId") != peer.get("processId"):
        view.error("Java overlap requires the same JVM process", "PLACEMENT")
    if runtime == "python" and victim.get("processId") == peer.get("processId"):
        view.error("Python native overlap requires different processes", "PLACEMENT")
    if view.profile.get("nativeConcurrency", 0) < 2:
        view.error("native overlap requires a deployed concurrency of at least two", "PLACEMENT")
    start = max(victim.get("startedAtMs", 0), peer.get("startedAtMs", 0))
    end = min(victim.get("endedAtMs", 0), peer.get("endedAtMs", 0))
    if start >= end:
        view.error("selected invocation intervals do not overlap", "PLACEMENT")
    for attempt in (victim, peer):
        barrier = view.event(attempt, "barrier_arrived")
        if not start <= barrier.get("atMs", -1) < end:
            view.error("cohort barrier was not established inside actual native overlap", "PLACEMENT")

    if view.value.get("requirementId") == "lmi-lifecycle-1":
        overlap_activity(view)


def overlap_activity(view: CaseView) -> None:
    if not view.require_roles("victim", "peer"):
        return
    expectations = view.value["expectedHistories"]
    victim = expectations[view.selected["victim"]["executionArn"]]
    peer = expectations[view.selected["peer"]["executionArn"]]
    if "result" in victim and "result" in peer and same_json(victim["result"], peer["result"]):
        view.error("overlap fixture must declare distinct expected results for the selected executions")
    requests = view.records(view.value.get("sdkRequests"), "sdkRequests")
    histories = view.records(view.value.get("histories"), "histories")
    overlap_start = max(view.selected[role]["startedAtMs"] for role in ("victim", "peer"))
    overlap_end = min(view.selected[role]["endedAtMs"] for role in ("victim", "peer"))
    for role in ("victim", "peer"):
        attempt = view.selected[role]
        if attempt.get("outcome") != "SUCCEEDED" or any(
            other.get("executionArn") == attempt.get("executionArn") and other is not attempt
            for other in view.attempts.values()
        ):
            view.error(f"overlap {role} needs an original successful execution without retry", "ASSERTION")
        arn = attempt.get("executionArn")
        history = next((history for history in histories if history.get("executionArn") == arn), {})
        expectation = expectations[arn]
        markers = [
            operation
            for operation in view.records(history.get("operations"), "overlap history.operations")
            if operation.get("type") == "STEP" and operation.get("name") == "record-marker"
        ]
        if len(markers) != 1 or "result" not in expectation:
            view.error(f"overlap {role} needs exactly one record-marker step and an expected result")
        elif (
            markers[0].get("status") != "SUCCEEDED"
            or "result" not in markers[0]
            or not same_json(markers[0]["result"], expectation["result"])
        ):
            view.error(f"overlap {role} record-marker step does not match the fixture expectation", "ASSERTION")
        elif not has_overlap_marker_dispatch(
            requests, attempt["requestId"], markers[0]["id"], overlap_start, overlap_end
        ):
            view.error(f"overlap {role} lacks an attributed checkpoint dispatch for record-marker during overlap")


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


def reconcile_overlap_identity(view: ReconcileView, corrected: Mapping[str, Mapping[str, Any]]) -> dict[str, set[str]]:
    """Check that each overlap role stays an original execution with one marker step."""
    marker_ids: dict[str, set[str]] = {}
    for role_name in ("victim", "peer"):
        execution = view.role(role_name).get("executionArn")
        selected_request = view.role(role_name).get("requestId")
        window = view.window_end
        if (
            isinstance(execution, str)
            and window is not None
            and any(
                request != selected_request
                and attempt.get("executionArn") == execution
                and (started := timestamp(attempt.get("startedAtMs"))) is not None
                and started <= window
                for request, attempt in corrected.items()
            )
        ):
            view.findings.append(f"ASSERTION: overlap {role_name} needs an original successful execution without retry")
        markers = {
            operation["id"]
            for source in view.sources
            for history in mappings(source.get("histories"))
            if history.get("executionArn") == execution
            for operation in mappings(history.get("operations"))
            if operation.get("type") == "STEP"
            and operation.get("name") == "record-marker"
            and isinstance(operation.get("id"), str)
        }
        marker_ids[role_name] = markers
        if len(markers) > 1:
            view.findings.append(f"ASSERTION: cleanup overlap {role_name} observed more than one record-marker step")
    return marker_ids


def reconcile_barrier(view: ReconcileView, marker_ids: Mapping[str, set[str]]) -> None:
    """Check barrier arrivals and marker dispatches against the established native overlap."""
    # Native metadata can widen a provisional interval. A barrier claim is
    # disproved only after every supplied native interval is available.
    intervals = [view.known_interval(name) for name in ("victim", "peer")]
    starts = [start for start, _ in intervals if start is not None]
    ends = [end for _, end in intervals if end is not None]
    if len(starts) == len(ends) == 2:
        for name in ("victim", "peer"):
            for event in view.observed_events(name, "barrier_arrived"):
                arrived = timestamp(event.get("atMs"))
                if arrived is not None and not max(starts) <= arrived < min(ends):
                    view.findings.append("PLACEMENT: cleanup barrier lies outside the established native overlap")
        if view.requirement == "lmi-lifecycle-1":
            requests = [row for source in view.sources for row in mappings(source.get("sdkRequests"))]
            for name, markers in marker_ids.items():
                if len(markers) == 1 and not has_overlap_marker_dispatch(
                    requests, view.role(name).get("requestId"), next(iter(markers)), max(starts), min(ends)
                ):
                    view.findings.append(
                        f"INCOMPLETE: cleanup overlap {name} lacks a marker checkpoint dispatch inside native overlap"
                    )
