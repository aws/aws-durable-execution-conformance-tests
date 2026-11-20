# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Lifecycle 6: warm reuse and late activity."""

from __future__ import annotations

from typing import TYPE_CHECKING

from aws_durable_execution_conformance_tests.lmi.rules.values import mappings, timestamp
from aws_durable_execution_conformance_tests.lmi.rules.view import CaseView

if TYPE_CHECKING:
    from aws_durable_execution_conformance_tests.lmi.rules.reconcile import ReconcileView


def warm_reuse(view: CaseView) -> None:
    if not view.require_roles("victim", "warm"):
        return
    victim, warm = view.selected["victim"], view.selected["warm"]
    if victim.get("endedAtMsSource") != "platform":
        view.error("warm reuse requires the victim's platform native completion time")
    if warm.get("startedAtMsSource") != "platform":
        view.error("warm reuse requires the warm request's platform native start time")
    closed = view.event(victim, "wrapper_return")
    if any(victim.get(key) != warm.get(key) for key in ("environmentId", "workerId", "processId")):
        view.error("warm reuse must identify the original process and worker", "PLACEMENT")
    if warm.get("startedAtMs", 0) <= closed.get("atMs", 0) or warm.get("startedAtMs", 0) < victim.get("endedAtMs", 0):
        view.error("warm invocation was not sequential reuse", "PLACEMENT")
    late = view.event(victim, "late_activity")
    if late.get("observedDuringRequestId") != warm.get("requestId") or not warm.get("startedAtMs", 0) <= late.get(
        "atMs", -1
    ) <= warm.get("endedAtMs", 0):
        view.error("late activity was not observed during the selected warm invocation", "ASSERTION")
    exited = view.event(victim, "task_exit")
    if exited.get("atMs", 0) < late.get("atMs", 0):
        view.error("late activity continued after the recorded task exit", "ASSERTION")


def reconcile_warm_reuse(view: ReconcileView) -> None:
    """Reject later native times that disprove sequential warm reuse."""
    if not (view.changed_time("victim", "endedAtMs") or view.changed_time("warm", "startedAtMs")):
        return
    start, end, returned = (
        view.native_time("warm", "startedAtMs"),
        view.native_time("victim", "endedAtMs"),
        view.boundary("victim", "wrapper_return"),
    )
    if start is not None and ((end is not None and start < end) or (returned is not None and start <= returned)):
        view.findings.append("PLACEMENT: cleanup evidence disproves sequential warm reuse")


def reconcile_late_activity(view: ReconcileView) -> None:
    """Reject late victim activity after the frozen victim task exit."""
    victim_id = view.role("victim").get("requestId")
    exited = view.boundary("victim", "task_exit")
    for source in view.sources:
        for event in mappings(source.get("events")):
            if event.get("kind") != "late_activity":
                continue
            request_id = event.get("requestId")
            execution_arn = view.bound_execution(request_id)
            if execution_arn is None:
                continue
            if event.get("ownerRequestId") != request_id or event.get("executionArn") != execution_arn:
                if view.final:
                    view.findings.append("INCOMPLETE: cleanup late activity has inconsistent native ownership")
                continue
            during = event.get("observedDuringRequestId")
            if during is not None and view.bound_execution(during) is None:
                continue
            observed_at = timestamp(event.get("atMs"))
            if request_id == victim_id and observed_at is not None and exited is not None and observed_at > exited:
                view.findings.append("ASSERTION: cleanup late activity continued after the frozen victim task exit")
