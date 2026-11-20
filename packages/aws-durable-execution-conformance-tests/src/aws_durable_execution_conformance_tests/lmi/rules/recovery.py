# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Lifecycle 4: healthy peer progress and worker recovery."""

from __future__ import annotations

from collections.abc import Sequence

from aws_durable_execution_conformance_tests.lmi.rules import deadline as deadline_rules
from aws_durable_execution_conformance_tests.lmi.rules import overlap as overlap_rules
from aws_durable_execution_conformance_tests.lmi.rules.common import original_admission_conflicts
from aws_durable_execution_conformance_tests.lmi.rules.view import CaseView


def recovery(view: CaseView) -> None:
    overlap_rules.overlap(view)
    deadline_rules.deadline(view, check_grace=False)
    if not view.require_roles("victim", "peer", "recovery"):
        return
    victim, recovery = view.selected["victim"], view.selected["recovery"]
    peers = [attempt for role, attempt in view.selected.items() if role.startswith("peer")]
    timeout = view.event(victim, "platform_timeout").get("atMs", 0)
    recovered = view.event(recovery, "worker_recovered").get("atMs", 0)
    deadline_rules.observation_grace(view, recovered)
    if recovery.get("environmentId") != victim.get("environmentId"):
        view.error("replacement environment cannot establish worker recovery", "PLACEMENT")
    policy = view.profile.get("recoveryPolicy")
    if policy == "reuse-worker":
        if any(recovery.get(key) != victim.get(key) for key in ("workerId", "processId")):
            view.error("recovery substituted a different worker/process", "PLACEMENT")
    elif policy == "replace-worker":
        if recovery.get("replacesWorkerId") != victim.get("workerId") or recovery.get("workerId") == victim.get(
            "workerId"
        ):
            view.error("replacement must identify the original failed worker capacity", "PLACEMENT")
    else:
        view.error("profile.recoveryPolicy must explicitly select reuse-worker or replace-worker")
    earlier, tied = original_admission_conflicts(recovery, list(view.attempts.values()))
    if tied:
        view.error("tied native admissions cannot establish the original recovery invocation")
    if recovery.get("outcome") != "SUCCEEDED":
        view.error("recovered capacity did not complete the selected recovery request", "ASSERTION")
    if earlier or any(recovery.get("executionArn") == attempt.get("executionArn") for attempt in (victim, *peers)):
        view.error("recovery must be an independent request, not a service retry", "PLACEMENT")
    if recovery.get("startedAtMs", 0) < timeout or recovered < recovery.get("startedAtMs", 0):
        view.error("recovery preceded the selected victim's timeout", "ASSERTION")
    # Ordering alone does not bound the measurement: requirement 4 declares
    # recoveryMs, so the observed recovery must fit inside it.
    recovery_budget = view.number(
        view.mapping(view.value.get("budgets"), "budgets").get("recoveryMs"), "budgets.recoveryMs"
    )
    if not timeout <= recovered <= timeout + recovery_budget:
        view.error("recovery exceeded the declared recoveryMs budget", "ASSERTION")
    if len(peers) != view.profile.get("nativeConcurrency", 0) - 1:
        view.error("recovery must occupy every other native capacity slot", "PLACEMENT")
    if view.value.get("runtime") == "python":
        processes = [victim.get("processId"), *(peer.get("processId") for peer in peers)]
        if len(set(processes)) != len(processes):
            view.error("recovery peers must occupy distinct Python processes", "PLACEMENT")
        if any(recovery.get("processId") == peer.get("processId") for peer in peers):
            view.error("recovery cannot use an occupied Python peer process", "PLACEMENT")
        if recovery.get("processId") == victim.get("processId"):
            if victim.get("endedAtMsSource") != "platform" or recovery.get("startedAtMsSource") != "platform":
                view.error("same-process Python recovery requires platform native completion and admission")
            elif recovery["startedAtMs"] < victim["endedAtMs"]:
                view.error("Python recovery preceded the victim's native invocation completion", "PLACEMENT")
    for peer in peers:
        if peer.get("environmentId") != victim.get("environmentId") or not peer.get(
            "startedAtMs", 0
        ) < timeout < recovered <= peer.get("endedAtMs", 0):
            view.error("established peer capacity was not held through recovery", "PLACEMENT")
        progress = [
            event.get("atMs", 0)
            for event in view.events
            if event.get("kind") == "peer_progress" and event.get("requestId") == peer.get("requestId")
        ]
        if not peer_progress_spans_timeout(progress, timeout, recovered):
            view.error("healthy peer lacks independent progress across the timeout", "ASSERTION")
    for fault in view.records(view.value.get("faultsCreated"), "faultsCreated"):
        if isinstance(fault.get("releasedAtMs"), (int, float)) and fault["releasedAtMs"] <= recovered:
            view.error("fault release cannot satisfy worker recovery", "ASSERTION")


def peer_progress_spans_timeout(progress: Sequence[int | float], timeout: int | float, recovered: int | float) -> bool:
    return any(at < timeout for at in progress) and any(timeout < at <= recovered for at in progress)
