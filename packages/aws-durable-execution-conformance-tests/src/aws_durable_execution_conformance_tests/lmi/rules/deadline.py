# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Lifecycle 3 (and the lifecycle 4 victim): crossing a real invocation deadline."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from aws_durable_execution_conformance_tests.lmi.rules import disposal as disposal_rules
from aws_durable_execution_conformance_tests.lmi.rules.common import normalize_retirement_events
from aws_durable_execution_conformance_tests.lmi.rules.values import timestamp
from aws_durable_execution_conformance_tests.lmi.rules.view import CaseView

if TYPE_CHECKING:
    from aws_durable_execution_conformance_tests.lmi.rules.reconcile import ReconcileView


def observation_grace(view: CaseView, boundary: float) -> None:
    budgets = view.mapping(view.value.get("budgets"), "budgets")
    grace = view.number(budgets.get("observationGraceMs"), "budgets.observationGraceMs")
    if view.value.get("observationEndedAtMs", 0) < boundary + grace:
        view.error("observationEndedAtMs does not cover the declared observationGraceMs after the scenario boundary")


def deadline(view: CaseView, *, check_grace: bool = True) -> None:
    if not view.require_roles("victim"):
        return
    victim = view.selected["victim"]
    timeout = view.event(victim, "platform_timeout")
    inflight = view.event(victim, "work_in_flight")
    closed = view.event(victim, "admission_closed")
    exited = view.event(victim, "task_exit")
    if inflight and exited:
        controlled = view.string(inflight.get("taskId"), "work_in_flight.taskId")
        finished = view.string(exited.get("taskId"), "task_exit.taskId")
        if controlled and finished and controlled != finished:
            view.error("deadline task exit is not the controlled in-flight task", "ASSERTION")
        for event in normalize_retirement_events(view.events, view.errors):
            if event.get("requestId") != victim.get("requestId") or event.get("taskId") != controlled:
                continue
            if event.get("kind") == "task_start" and view.at_or_after(inflight, event, require_sequence=True) is False:
                view.error("controlled task started after it was observed in flight", "ASSERTION")
            if event.get("kind") == "task_exit" and event is not exited:
                view.error("deadline controlled task must have exactly one recorded exit")
    time = timeout.get("atMs", 0)
    if time < victim.get("startedAtMs", 0) + view.profile.get("timeoutMs", 0):
        view.error("platform timeout did not cross the configured invocation deadline", "ASSERTION")
    if time > victim.get("endedAtMs", 0):
        view.error("platform timeout follows native invocation completion", "ASSERTION")
    if victim.get("outcome") != "TIMED_OUT" or timeout.get("source") != "platform":
        view.error("deadline requires an observed platform timeout, not a simulated cancellation", "ASSERTION")
    ordering = {
        kind: view.at_or_after(event, timeout, require_sequence=True) if "atMs" in event and "atMs" in timeout else None
        for kind, event in (("inflight", inflight), ("admissionMs", closed), ("cleanupMs", exited))
    }
    if ordering["inflight"] is True or ordering["cleanupMs"] is False:
        view.error("controlled work was not in flight across the real deadline", "ASSERTION")
    budgets = view.mapping(view.value.get("budgets"), "budgets")
    for field, event in (("admissionMs", closed), ("cleanupMs", exited)):
        budget = view.number(budgets.get(field), f"budgets.{field}")
        if ordering[field] is False or event.get("atMs", time) > time + budget:
            view.error(f"deadline {field} exceeds the declared fixture budget or precedes the timeout", "ASSERTION")
    if check_grace:
        observation_grace(view, time)
    disposal_rules.require_probes(view, victim, closed, "operation", "checkpoint")
    for fault in view.records(view.value.get("faultsCreated"), "faultsCreated"):
        release = fault.get("releasedAtMs")
        if isinstance(release, (int, float)) and release <= exited.get("atMs", 0):
            view.error("fault release supplied the cleanup being asserted", "ASSERTION")


def reconcile_deadline(view: ReconcileView) -> list[Mapping[str, Any]]:
    """Check later timeout, in-flight work and task exit observations; return the timeouts."""
    profile, budgets = view.profile, view.budgets
    grace, window_end = timestamp(budgets.get("observationGraceMs")), view.window_end
    start, end = view.known_interval("victim")
    configured_timeout = timestamp(profile.get("timeoutMs"))
    controlled_task = view.boundary_record("victim", "work_in_flight").get("taskId")
    work_observations = view.observed_events("victim", "work_in_flight")
    task_exits = [
        event for event in view.observed_events("victim", "task_exit") if event.get("taskId") == controlled_task
    ]
    task_starts = [
        event for event in view.observed_events("victim", "task_start") if event.get("taskId") == controlled_task
    ]
    for work in work_observations:
        if isinstance(controlled_task, str) and work.get("taskId", controlled_task) != controlled_task:
            view.findings.append("ASSERTION: cleanup in-flight work does not identify the controlled deadline task")
        if any(view.at_or_after(work, start_event, require_sequence=True) is False for start_event in task_starts):
            view.findings.append("ASSERTION: cleanup controlled task started after it was observed in flight")
        if any(view.at_or_after(work, exit_event, require_sequence=True) is True for exit_event in task_exits):
            view.findings.append("ASSERTION: cleanup in-flight work follows the controlled task exit")
    timeout_events = view.observed_events("victim", "platform_timeout")
    for event in timeout_events:
        if "source" in event and event["source"] != "platform":
            view.findings.append("ASSERTION: cleanup timeout source is not the observed platform deadline")
        timed_out = timestamp(event.get("atMs"))
        if timed_out is None:
            continue
        if (
            view.requirement == "lmi-lifecycle-3"
            and grace is not None
            and window_end is not None
            and window_end - timed_out < grace
        ):
            view.findings.append("INCOMPLETE: cleanup observation window lacks the declared grace after timeout")
        if (start is not None and configured_timeout is not None and timed_out < start + configured_timeout) or (
            end is not None and timed_out > end
        ):
            view.findings.append("ASSERTION: cleanup timeout falls outside the configured native deadline")
        if any(view.at_or_after(work, event, require_sequence=True) is True for work in work_observations) or any(
            view.at_or_after(exit_event, event, require_sequence=True) is False for exit_event in task_exits
        ):
            view.findings.append("ASSERTION: cleanup timeout contradicts the frozen in-flight work or task exit")
        for observed_measurements, budget_key in (
            (view.observed_events("victim", "admission_closed"), "admissionMs"),
            (task_exits, "cleanupMs"),
        ):
            budget = timestamp(budgets.get(budget_key))
            for measured in observed_measurements:
                measured_at = timestamp(measured.get("atMs"))
                if view.at_or_after(measured, event, require_sequence=True) is False:
                    if budget_key == "admissionMs":
                        view.findings.append("ASSERTION: cleanup admission closed before the observed platform timeout")
                    else:
                        view.findings.append("ASSERTION: cleanup task exited before the observed platform timeout")
                elif measured_at is not None and budget is not None and measured_at > timed_out + budget:
                    view.findings.append(f"ASSERTION: cleanup timeout contradicts the frozen {budget_key} measurement")
    return timeout_events


def reconcile_fault_release(view: ReconcileView, releases: Sequence[int | float]) -> None:
    """Reject a fault release before the asserted cleanup or recovery."""
    limits = [view.boundary("victim", "task_exit")]
    if view.requirement == "lmi-lifecycle-4":
        limits.extend(timestamp(event.get("atMs")) for event in view.observed_events("recovery", "worker_recovered"))
    known_limits = [time for time in limits if time is not None]
    # A later release acknowledgment cannot erase an earlier release from
    # either observation records or a provisional/final retirement record.
    if releases and known_limits and min(releases) <= max(known_limits):
        view.findings.append(
            "ASSERTION: cleanup evidence reports fault release before the asserted cleanup or recovery"
        )
