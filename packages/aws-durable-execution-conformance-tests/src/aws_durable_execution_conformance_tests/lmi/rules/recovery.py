# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Lifecycle 4: healthy peer progress and worker recovery."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from aws_durable_execution_conformance_tests.lmi.rules import deadline as deadline_rules
from aws_durable_execution_conformance_tests.lmi.rules import overlap as overlap_rules
from aws_durable_execution_conformance_tests.lmi.rules.common import original_admission_conflicts
from aws_durable_execution_conformance_tests.lmi.rules.values import mappings, same_json, timestamp
from aws_durable_execution_conformance_tests.lmi.rules.view import CaseView

if TYPE_CHECKING:
    from aws_durable_execution_conformance_tests.lmi.rules.reconcile import ReconcileView


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


def reconcile_recovery_event(view: ReconcileView) -> None:
    """Require the recovery request's worker_recovered event to stay unique."""
    recovery = view.role("recovery").get("requestId")
    if isinstance(recovery, str) and any(
        sum(
            row.get("requestId") == recovery and row.get("kind") == "worker_recovered"
            for row in mappings(source.get("events"))
        )
        > 1
        for source in view.sources
    ):
        view.findings.append(f"INCOMPLETE: request {recovery} needs exactly one worker_recovered event")


def reconcile_recovery(view: ReconcileView, timeout_events: Sequence[Mapping[str, Any]]) -> None:
    """Check later recovery observations against the victim timeouts and peer capacity."""
    grace, window_end = timestamp(view.budgets.get("observationGraceMs")), view.window_end
    if view.profile.get("recoveryPolicy") == "replace-worker":
        recovery_id = view.role("recovery").get("requestId")
        for source in view.sources:
            for attempt in mappings(source.get("attempts")):
                if (
                    recovery_id is not None
                    and attempt.get("requestId") == recovery_id
                    and "replacesWorkerId" in attempt
                    and not same_json(attempt["replacesWorkerId"], view.role("victim").get("workerId"))
                ):
                    view.findings.append("PLACEMENT: cleanup recovery replacement changed the frozen failed worker")
    recovery_start, _ = view.known_interval("recovery")
    recovery_budget = timestamp(view.budgets.get("recoveryMs"))
    if (
        view.final
        and recovery_start is not None
        and any((at := timestamp(event.get("atMs"))) is not None and recovery_start < at for event in timeout_events)
    ):
        view.findings.append("ASSERTION: cleanup evidence places recovery before the victim timeout")
    for event in view.observed_events("recovery", "worker_recovered"):
        recovered = timestamp(event.get("atMs"))
        if recovered is None:
            continue
        if view.final and grace is not None and window_end is not None and window_end - recovered < grace:
            view.findings.append("INCOMPLETE: cleanup observation window lacks the declared grace after recovery")
        if view.final and recovery_start is not None and recovered < recovery_start:
            view.findings.append("ASSERTION: cleanup worker recovery precedes its native invocation")
        for timeout_event in timeout_events:
            timed_out = timestamp(timeout_event.get("atMs"))
            if (
                timed_out is not None
                and recovery_budget is not None
                and not (timed_out <= recovered <= timed_out + recovery_budget)
            ):
                view.findings.append("ASSERTION: cleanup worker recovery exceeds the frozen recovery measurement")
        if view.final:
            for peer in view.selected:
                if isinstance(peer, str) and peer.startswith("peer"):
                    _, peer_end = view.known_interval(peer)
                    if peer_end is not None and recovered > peer_end:
                        view.findings.append("PLACEMENT: cleanup worker recovery outlived established peer capacity")
                    progress = [
                        at
                        for row in view.observed_events(peer, "peer_progress")
                        if (at := timestamp(row.get("atMs"))) is not None
                    ]
                    for timeout_event in timeout_events:
                        timed_out = timestamp(timeout_event.get("atMs"))
                        if timed_out is not None and not peer_progress_spans_timeout(progress, timed_out, recovered):
                            view.findings.append(
                                "ASSERTION: cleanup healthy peer lacks progress across the timeout and recovery"
                            )


def reconcile_python_recovery(view: ReconcileView) -> None:
    """Reject a Python recovery in the victim process before the victim completed natively."""
    if not (view.changed_time("victim", "endedAtMs") or view.changed_time("recovery", "startedAtMs")):
        return
    start, end = view.native_time("recovery", "startedAtMs"), view.native_time("victim", "endedAtMs")
    victim, recovery = view.role("victim"), view.role("recovery")
    if (
        view.original.get("runtime") == "python"
        and victim.get("processId")
        and victim.get("processId") == recovery.get("processId")
        and start is not None
        and end is not None
        and start < end
    ):
        view.findings.append("PLACEMENT: cleanup evidence places Python recovery before victim native completion")
