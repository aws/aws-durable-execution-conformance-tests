# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Lifecycle 2 and 7: normal return or failure, and post-return admission."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from aws_durable_execution_conformance_tests.lmi.rules.values import (
    TERMINAL_STATUSES,
    same_json,
    timestamp,
)
from aws_durable_execution_conformance_tests.lmi.rules.view import CaseView

if TYPE_CHECKING:
    from aws_durable_execution_conformance_tests.lmi.rules.reconcile import ReconcileView


def disposal(view: CaseView, expected_outcome: str) -> None:
    if not view.require_roles("victim"):
        return
    victim = view.selected["victim"]
    root = view.event(victim, "root_completed")
    wrapper = view.event(victim, "wrapper_return")
    if root.get("outcome") not in ("SUCCEEDED", "FAILED") or root.get("outcome") != victim.get("outcome"):
        view.error("root completion and actual invocation outcome disagree", "ASSERTION")
    if victim.get("outcome") != expected_outcome:
        view.error(f"disposal requires the selected victim outcome {expected_outcome}", "ASSERTION")
    arn = victim.get("executionArn")
    if sum(attempt.get("executionArn") == arn for attempt in view.attempts.values()) == 1:
        history = next(item for item in view.value["histories"] if item["executionArn"] == arn)
        if history.get("status") in TERMINAL_STATUSES and history["status"] != expected_outcome:
            view.error("terminal service history disagrees with the single disposal invocation", "ASSERTION")
    if root.get("atMs", 0) > wrapper.get("atMs", 0):
        view.error("wrapper return precedes root completion", "ASSERTION")
    if wrapper:
        require_probes(view, victim, wrapper, "checkpoint")
    exited = view.event(victim, "task_exit")
    if exited.get("atMs", -1) < root.get("atMs", 0):
        view.error("task exit preceded root completion in the disposal fixture", "ASSERTION")


def require_probes(view: CaseView, victim: Mapping[str, Any], boundary: Mapping[str, Any], *probes: str) -> None:
    # Outcomes are checked separately; only probes at or after this scenario's boundary count as coverage.
    for probe in probes:
        if not any(
            event.get("requestId") == victim.get("requestId")
            and event.get("kind") == "admission_probe"
            and event.get("probe") == probe
            and "atMs" in boundary
            and view.at_or_after(event, boundary)
            for event in view.events
        ):
            view.error(f"missing post-closure {probe} admission probe")


def reconcile_disposal(view: ReconcileView) -> None:
    """Check later root completions against the frozen disposal boundaries."""
    returned, exited = view.boundary("victim", "wrapper_return"), view.boundary("victim", "task_exit")
    for event in view.observed_events("victim", "root_completed"):
        if "outcome" in event and not same_json(event["outcome"], view.role("victim").get("outcome")):
            view.findings.append("ASSERTION: cleanup root completion contradicts the frozen invocation outcome")
        completed_at = timestamp(event.get("atMs"))
        if completed_at is not None and any(end is not None and completed_at > end for end in (returned, exited)):
            view.findings.append("ASSERTION: cleanup root completion follows the frozen wrapper or task exit")
