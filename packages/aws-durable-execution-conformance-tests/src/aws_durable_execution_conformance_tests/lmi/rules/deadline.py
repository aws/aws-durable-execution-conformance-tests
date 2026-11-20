# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Lifecycle 3 (and the lifecycle 4 victim): crossing a real invocation deadline."""

from __future__ import annotations

from aws_durable_execution_conformance_tests.lmi.rules import disposal as disposal_rules
from aws_durable_execution_conformance_tests.lmi.rules.common import normalize_retirement_events
from aws_durable_execution_conformance_tests.lmi.rules.view import CaseView


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
