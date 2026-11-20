# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Lifecycle 6: warm reuse and late activity."""

from __future__ import annotations

from aws_durable_execution_conformance_tests.lmi.rules.view import CaseView


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
