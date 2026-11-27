# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Lifecycle 6: warm reuse and late activity."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from aws_durable_execution_conformance_tests.lmi.rules.common import normalize_retirement_events
from aws_durable_execution_conformance_tests.lmi.rules.values import is_json_integer, mappings, same_json, timestamp
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
    if late and (
        late.get("observedDuringRequestId") != warm.get("requestId")
        or not warm.get("startedAtMs", 0) <= late.get("atMs", -1) <= warm.get("endedAtMs", 0)
    ):
        view.error("late activity was not observed during the selected warm invocation", "ASSERTION")
    exited = view.event(victim, "task_exit")
    if not late or not exited:
        return
    late_at, exit_at = late["atMs"], exited["atMs"]
    late_after_exit = late_at > exit_at
    if late_at == exit_at:
        late_sequence, exit_sequence = late.get("sequence"), exited.get("sequence")
        if late_sequence is None or exit_sequence is None or late_sequence == exit_sequence:
            view.error("same-timestamp late_activity and task_exit need distinct sequences")
            return
        late_after_exit = late_sequence > exit_sequence
    if late_after_exit:
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


_CLEANUP_LATE_ACTIVITY = "ASSERTION: cleanup late activity continued after the frozen victim task exit"
_UNORDERED_CLEANUP_LATE_ACTIVITY = (
    "INCOMPLETE: cleanup late activity tied with the frozen victim task exit needs distinct sequences"
)
_LATE_ACTIVITY_IDENTITY = ("taskId", "callId", "gateId", "operationId", "attemptId", "attempt")


def tied_late_activity_follows_exit(
    source_events: Sequence[Mapping[str, Any]], late: Mapping[str, Any], task_exit: Mapping[str, Any]
) -> bool | None:
    """Order late activity against a task exit with the same timestamp in one snapshot.

    Snapshots may renumber sequences, so ``late`` is compared only with this
    snapshot's copy of the exit, including its ``observed_task_exit`` alias.
    ``None`` means the snapshot cannot order them.
    """
    sequences = [
        event.get("sequence")
        for event in normalize_retirement_events(source_events, [])
        if event.get("kind") == "task_exit"
        and event.get("requestId") == task_exit.get("requestId")
        and same_json(event.get("taskId"), task_exit.get("taskId"))
        and type(event.get("atMs")) in (int, float)
        and event.get("atMs") == task_exit.get("atMs")
    ]
    late_sequence, exit_sequence = late.get("sequence"), sequences[0] if len(sequences) == 1 else None
    if not is_json_integer(late_sequence) or not is_json_integer(exit_sequence):
        return None
    if late_sequence < 0 or exit_sequence < 0 or late_sequence == exit_sequence:
        return None
    return late_sequence > exit_sequence


def reconcile_late_activity(view: ReconcileView) -> None:
    """Reject late victim activity after the frozen victim task exit.

    Each tie is ordered within one snapshot. Every tied activity a cleanup
    snapshot reported must be ordered in some snapshot; an identity seen several
    times needs as many ordered copies.
    """
    victim_id = view.role("victim").get("requestId")
    victim_exit = view.boundary_record("victim", "task_exit")
    exited = timestamp(victim_exit.get("atMs"))
    seen: Counter[str] = Counter()
    ordered: Counter[str] = Counter()
    for source in view.sources:
        events = mappings(source.get("events"))
        local_seen: Counter[str] = Counter()
        local_ordered: Counter[str] = Counter()
        for event in events:
            if event.get("kind") != "late_activity":
                continue
            request_id = event.get("requestId")
            execution_arn = view.bound_execution(request_id)
            if execution_arn is None:
                continue
            if event.get("ownerRequestId") != request_id or event.get("executionArn") != execution_arn:
                view.findings.append("INCOMPLETE: cleanup late activity has inconsistent native ownership")
                continue
            during = event.get("observedDuringRequestId")
            if during is not None and view.bound_execution(during) is None:
                continue
            observed_at = timestamp(event.get("atMs"))
            if request_id != victim_id or observed_at is None or exited is None:
                continue
            if observed_at != exited:
                if observed_at > exited:
                    view.findings.append(_CLEANUP_LATE_ACTIVITY)
                continue
            identity = json.dumps([event.get(key) for key in _LATE_ACTIVITY_IDENTITY], default=str)
            local_seen[identity] += 1
            follows = tied_late_activity_follows_exit(events, event, victim_exit)
            if follows is not None:
                local_ordered[identity] += 1
            if follows:
                view.findings.append(_CLEANUP_LATE_ACTIVITY)
        # The live check already reports an unordered tie in frozen evidence.
        if source is not view.original:
            seen |= local_seen
        ordered |= local_ordered
    if any(ordered[identity] < count for identity, count in seen.items()):
        view.findings.append(_UNORDERED_CLEANUP_LATE_ACTIVITY)
