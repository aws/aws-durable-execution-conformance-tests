# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Offline adjudication of frozen LMI observations and later evidence."""

from __future__ import annotations

import copy
import math
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from itertools import pairwise
from typing import Any

from aws_durable_execution_conformance_tests.lmi.coordinator import (
    _OPERATION_TERMINAL_STATUSES,
    _cleanup_record_errors,
    _native_intervals,
    _retain_observations,
)
from aws_durable_execution_conformance_tests.lmi.evidence import (
    _CALL_METHODS,
    _WORK_INTERVALS,
    TERMINAL_STATUSES,
    EvidenceValidator,
    _has_matching_sdk_request,
    _history_expectation_mismatches,
    completed_replay_step_witness,
    completed_suspension_checkpoint,
    has_overlap_marker_dispatch,
    is_json_integer,
    normalize_retirement_events,
    original_admission_conflicts,
    peer_progress_spans_timeout,
    permits_disposal_operation_probe,
    replay_checkpoint_timing,
    retained_fault_releases,
    retry_body_identity,
    same_json,
    validate_evidence,
    validate_retirement_evidence,
)


def _retry_attempt_order(evidence: Mapping[str, Any]) -> tuple[str, ...] | None:
    replay = evidence.get("replay")
    retry = replay.get("retry") if isinstance(replay, Mapping) else None
    attempts = retry.get("attempts") if isinstance(retry, Mapping) else None
    if not isinstance(attempts, list):
        return None
    identifiers = []
    for attempt in attempts:
        identifier = attempt.get("attemptId") if isinstance(attempt, Mapping) else None
        if not isinstance(identifier, str) or not identifier.strip():
            return None
        identifiers.append(identifier)
    # Singleton fragments establish no ordering constraint. The caller diagnoses
    # duplicate identities before using an observed sequence as an ordering.
    if len(identifiers) < 2:
        return None
    return tuple(identifiers)


def _side_effect_observation_errors(evidence: Mapping[str, Any]) -> list[str]:
    replay = evidence.get("replay")
    counts = replay.get("sideEffects") if isinstance(replay, Mapping) else None
    if not isinstance(counts, list):
        return []
    errors = []
    seen: set[str] = set()
    for count in counts:
        if not isinstance(count, Mapping):
            continue
        operation_id = count.get("operationId")
        if isinstance(operation_id, str):
            if operation_id in seen:
                errors.append(f"INCOMPLETE: duplicate side-effect operationId {operation_id}")
            seen.add(operation_id)
        for counter_field in ("count", "expectedCount"):
            if counter_field in count and (not is_json_integer(count[counter_field]) or count[counter_field] < 0):
                errors.append(f"INCOMPLETE: side-effect {counter_field} must be a non-negative integer")
    return errors


def _side_effect_progress_errors(snapshots: Sequence[Mapping[str, Any]]) -> list[str]:
    counts: dict[str, int | float] = {}
    expectations: dict[str, int | float] = {}
    errors: list[str] = []
    for snapshot in snapshots:
        replay = snapshot.get("replay")
        records = replay.get("sideEffects") if isinstance(replay, Mapping) else None
        for record in records if isinstance(records, list) else ():
            if not isinstance(record, Mapping) or not isinstance(operation := record.get("operationId"), str):
                continue
            count, expected = record.get("count"), record.get("expectedCount")
            if is_json_integer(count) and count >= 0:
                if operation in counts and count < counts[operation]:
                    errors.append(
                        f"ASSERTION: observed side-effect count differs from earlier observations for {operation}"
                    )
                counts[operation] = max(count, counts.get(operation, count))
            if is_json_integer(expected) and expected >= 0:
                first = expectations.setdefault(operation, expected)
                if expected != first:
                    errors.append(
                        f"ASSERTION: observed side-effect count differs from the declared expectation for {operation}"
                    )
    return errors


def _project_retirement_completions(
    events: Sequence[Mapping[str, Any]],
    completions: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    work_intervals = {start: (end, identity) for start, end, identity in _WORK_INTERVALS}
    starts: set[tuple[str, str, str]] = set()
    for event in normalize_retirement_events([event for event in events if isinstance(event, Mapping)], []):
        kind = event.get("kind")
        interval = work_intervals.get(kind) if isinstance(kind, str) else None
        if interval:
            end_kind, identity = interval
            request, work_id = event.get("requestId"), event.get(identity)
            if isinstance(request, str) and isinstance(work_id, str):
                starts.add((end_kind, request, work_id))

    identity_by_end = {end: identity for _, end, identity in _WORK_INTERVALS}
    projected = []
    valid = [completion for completion in completions if isinstance(completion, Mapping)]
    for original, normalized in zip(valid, normalize_retirement_events(valid, []), strict=True):
        kind = normalized.get("kind")
        if not isinstance(kind, str) or kind not in identity_by_end:
            continue
        request, work_id = normalized.get("requestId"), normalized.get(identity_by_end[kind])
        if isinstance(request, str) and isinstance(work_id, str) and (kind, request, work_id) in starts:
            projected.append(original)
    return projected


def _observed_checkpoint_ends(sources: Sequence[Mapping[str, Any]], execution_arn: Any) -> dict[str, set[int | float]]:
    ends: dict[str, set[int | float]] = {}
    for source in sources:
        replay = source.get("replay")
        inventories = [replay.get("before"), replay.get("after")] if isinstance(replay, Mapping) else []
        histories = source.get("histories")
        for history in histories if isinstance(histories, list) else ():
            if isinstance(history, Mapping) and history.get("executionArn") == execution_arn:
                inventories.append(history.get("operations"))
        for operations in inventories:
            for operation in operations if isinstance(operations, list) else ():
                if not isinstance(operation, Mapping) or operation.get("executionArn", execution_arn) != execution_arn:
                    continue
                operation_id, end = operation.get("id"), operation.get("endedAtMs")
                if (
                    isinstance(operation_id, str)
                    and isinstance(end, (int, float))
                    and not isinstance(end, bool)
                    and (isinstance(end, int) or math.isfinite(end))
                    and end >= 0
                ):
                    ends.setdefault(operation_id, set()).add(end)
    return ends


def _cleanup_retry_errors(
    frozen_replay: Mapping[str, Any],
    sources: Sequence[Mapping[str, Any]],
    execution_arn: Any,
    bound_execution: Callable[[Any], str | None],
    after_or_at: Callable[[Mapping[str, Any], Mapping[str, Any]], bool | None],
    *,
    final: bool,
) -> list[str]:
    """Check retry claims without promoting partial fragments to whole runs."""
    findings: list[str] = []
    frozen_retry = frozen_replay.get("retry")
    if not isinstance(frozen_retry, Mapping) or not isinstance(frozen_retry.get("operationId"), str):
        return findings
    operation_id = frozen_retry["operationId"]
    frozen_attempts = frozen_retry.get("attempts", [])
    frozen_ids = (
        {
            item["attemptId"]
            for item in frozen_attempts
            if isinstance(item, Mapping) and isinstance(item.get("attemptId"), str)
        }
        if isinstance(frozen_attempts, list)
        else set()
    )
    facts: dict[str, dict[str, Any]] = {}
    orders: list[list[str]] = []
    entries: dict[str, list[Mapping[str, Any]]] = {}
    for source in sources:
        replay = source.get("replay")
        if not isinstance(replay, Mapping):
            continue
        observed_entries = replay.get("operationExecutions", [])
        if isinstance(observed_entries, list):
            for entry in observed_entries:
                if (
                    isinstance(entry, Mapping)
                    and entry.get("operationId") == operation_id
                    and isinstance(entry.get("attemptId"), str)
                ):
                    entries.setdefault(entry["attemptId"], []).append(entry)
        if "retry" not in replay:
            continue
        retry = replay["retry"]
        if not isinstance(retry, Mapping):
            if final:
                findings.append("ASSERTION: cleanup retry observations must be an object")
            continue
        same_operation = "operationId" not in retry or same_json(retry["operationId"], operation_id)
        if not same_operation:
            findings.append("ASSERTION: cleanup retry operationId changed the frozen retry operation")
        attempts = retry.get("attempts", [])
        if not isinstance(attempts, list):
            if final:
                findings.append("ASSERTION: cleanup retry attempts must be an array")
            continue
        order = []
        for attempt in attempts:
            if not isinstance(attempt, Mapping):
                if final:
                    findings.append("ASSERTION: cleanup retry attempt must be an object")
                continue
            if "requestId" in attempt:
                bound_execution(attempt["requestId"])
            if not same_operation:
                continue
            attempt_id = attempt.get("attemptId")
            if not isinstance(attempt_id, str) or not attempt_id.strip():
                if final:
                    findings.append("ASSERTION: cleanup retry attempt lacks a non-empty attemptId")
                continue
            order.append(attempt_id)
            known = facts.setdefault(attempt_id, {})
            for key in ("requestId", "outcome"):
                if key not in attempt:
                    continue
                if key in known and not same_json(known[key], attempt[key]):
                    findings.append(f"ASSERTION: cleanup retry attempt {attempt_id} changed its known {key}")
                else:
                    known[key] = attempt[key]
        if len(set(order)) != len(order):
            findings.append("ASSERTION: cleanup retry observations repeat an attemptId")
        elif order:
            orders.append(order)

    # Each source contributes only its observed ordering edges. In particular,
    # separate singleton fragments do not imply a temporal relationship.
    successors: dict[str, set[str]] = {attempt_id: set() for attempt_id in facts}
    indegree = dict.fromkeys(facts, 0)
    for order in orders:
        for before, after in pairwise(order):
            if after not in successors[before]:
                successors[before].add(after)
                indegree[after] += 1
    roots = [attempt_id for attempt_id, degree in indegree.items() if degree == 0]
    ready = deque(roots)
    visited = 0
    predecessors: dict[str, set[str]] = {attempt_id: set() for attempt_id in facts}
    while ready:
        attempt_id = ready.popleft()
        visited += 1
        for following in successors[attempt_id]:
            predecessors[following].update(predecessors[attempt_id])
            predecessors[following].add(attempt_id)
            indegree[following] -= 1
            if indegree[following] == 0:
                ready.append(following)
    if visited != len(facts):
        findings.append("ASSERTION: cleanup retry attempt order contradicts retained observations")

    if not final:
        return findings
    declared: dict[str, str] = {}
    for attempt_id, known in facts.items():
        candidates = entries.get(attempt_id, [])
        request_id = known.get("requestId")
        if "requestId" not in known:
            candidate_requests = {
                item["requestId"]
                for item in candidates
                if isinstance(item.get("requestId"), str) and item["requestId"].strip()
            }
            if len(candidate_requests) == 1:
                request_id = next(iter(candidate_requests))
            else:
                if not candidate_requests:
                    bound_execution(None)
                findings.append(f"ASSERTION: cleanup retry attempt {attempt_id} has unresolved native correlation")
                continue
        owner = bound_execution(request_id)
        if owner is None:
            continue
        if owner != execution_arn:
            findings.append(f"ASSERTION: cleanup retry attempt {attempt_id} did not run on the suspended execution")
        if isinstance(request_id, str) and owner == execution_arn:
            declared[attempt_id] = request_id
        matching = [item for item in candidates if item.get("requestId") == request_id]
        if not matching:
            findings.append(f"ASSERTION: cleanup retry attempt {attempt_id} lacks a correlated user-code entry")
            continue
        outcomes = [item["outcome"] for item in matching if "outcome" in item]
        if "outcome" not in known and outcomes and all(same_json(item, outcomes[0]) for item in outcomes):
            known["outcome"] = outcomes[0]
        known_outcome = known.get("outcome")
        if not isinstance(known_outcome, str) or not known_outcome.strip():
            findings.append(f"INCOMPLETE: cleanup retry attempt {attempt_id} outcome must be a non-empty string")
        if "outcome" in known and any(not same_json(known["outcome"], item) for item in outcomes):
            findings.append(f"ASSERTION: cleanup retry attempt {attempt_id} contradicts its user-code outcome")
    timed: dict[str, list[Mapping[str, Any]]] = {}
    native_starts = {
        request: endpoints["startedAtMs"][0]
        for request, endpoints in _native_intervals(sources).items()
        if "startedAtMs" in endpoints
    }
    retry_ends = _observed_checkpoint_ends(sources, execution_arn).get(operation_id, set())
    body_times: dict[tuple[str, str], int | float] = {}
    for source in sources:
        replay = source.get("replay")
        for key, rows in (
            ("entries", replay.get("operationExecutions", []) if isinstance(replay, Mapping) else []),
            ("events", source.get("events", [])),
        ):
            if not isinstance(rows, list):
                continue
            seen: set[tuple[str, str]] = set()
            for row in rows:
                if (
                    not isinstance(row, Mapping)
                    or row.get("operationId") != operation_id
                    or (key == "events" and row.get("kind") != "body")
                    or bound_execution(row.get("requestId")) != execution_arn
                ):
                    continue
                identity = retry_body_identity(row)
                if identity is None or declared.get(identity[1]) != identity[0]:
                    findings.append("ASSERTION: cleanup observed retry body does not match a declared attempt")
                    continue
                if identity in seen:
                    findings.append("ASSERTION: cleanup retry attempt has duplicate user-code entries")
                seen.add(identity)
                at = row.get("atMs")
                if key == "events" and isinstance(at, (int, float)) and not isinstance(at, bool):
                    if identity in body_times and not same_json(body_times[identity], at):
                        findings.append("ASSERTION: cleanup retry body changed its observed timestamp")
                    body_times.setdefault(identity, at)
                    timed.setdefault(identity[1], []).append(row)
                    if retry_ends and at > min(retry_ends):
                        findings.append(
                            "ASSERTION: cleanup timed retry body occurred after recorded operation completion"
                        )
    for attempt_id, bodies in timed.items():
        ancestry = predecessors[attempt_id]
        latest_admission = max(
            (native_starts.get(declared.get(prior, ""), 0) for prior in ancestry | {attempt_id}),
            default=0,
        )
        for body in bodies:
            if body["atMs"] < latest_admission:
                findings.append("ASSERTION: cleanup retry body precedes a prior attempt's admission")
            if any(
                previous["atMs"] > body["atMs"]
                or (previous["requestId"] == body["requestId"] and after_or_at(body, previous) is False)
                for prior in ancestry
                for previous in timed.get(prior, [])
            ):
                findings.append("ASSERTION: cleanup retry attempt order contradicts timed user-code entries")
    if retry_ends and any(native_starts.get(request, 0) > min(retry_ends) for request in declared.values()):
        findings.append("ASSERTION: cleanup retry owner was admitted after recorded operation completion")
    leaves = [attempt_id for attempt_id, following in successors.items() if not following]
    if frozen_ids:
        if visited == len(facts) and (len(roots) != 1 or len(leaves) != 1):
            findings.append("INCOMPLETE: cleanup retry order does not establish unique first and last attempts")
        elif len(roots) == len(leaves) == 1 and (
            roots[0] == leaves[0]
            or facts[roots[0]].get("outcome") != "FAILED"
            or facts[leaves[0]].get("outcome") != "SUCCEEDED"
        ):
            findings.append("ASSERTION: cleanup retry observations do not start with failure and end with success")
    return findings


def _cleanup_checkpoint_errors(
    frozen_replay: Mapping[str, Any],
    sources: Sequence[Mapping[str, Any]],
    execution_arn: Any,
    *,
    final: bool,
    observation_end: int | float | None = None,
) -> tuple[list[str], set[str]]:
    """Reconcile supplied checkpoint facts without replacing partial views."""
    findings: list[str] = []

    def records(value: Any) -> list[Mapping[str, Any]]:
        return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []

    frozen_before = {
        item["id"]: item for item in records(frozen_replay.get("before")) if isinstance(item.get("id"), str)
    }
    frozen_after = {item["id"]: item for item in records(frozen_replay.get("after")) if isinstance(item.get("id"), str)}
    before_ids: set[str] = set(frozen_before)
    after_ids: set[str] = set(frozen_after)
    # The accepted original after snapshot was checked against service history.
    history_ids: set[str] = set(frozen_after)
    observations: dict[str, list[tuple[str, Mapping[str, Any]]]] = {}
    for source in sources:
        replay = source.get("replay")
        if isinstance(replay, Mapping):
            for view in ("before", "after"):
                if view not in replay:
                    continue
                operations = replay[view]
                if not isinstance(operations, list):
                    if final:
                        findings.append(f"ASSERTION: cleanup replay.{view} checkpoints must be an array")
                    continue
                seen: set[str] = set()
                for operation in operations:
                    operation_id = operation.get("id") if isinstance(operation, Mapping) else None
                    if not isinstance(operation_id, str) or not operation_id.strip():
                        if final:
                            findings.append(f"ASSERTION: cleanup replay.{view} checkpoint lacks an operation ID")
                        continue
                    if operation_id in seen:
                        findings.append(f"ASSERTION: cleanup replay.{view} repeats checkpoint {operation_id}")
                    seen.add(operation_id)
                    (before_ids if view == "before" else after_ids).add(operation_id)
                    observations.setdefault(operation_id, []).append((view, operation))
                    if (
                        view == "before"
                        and "status" in operation
                        and operation["status"] not in _OPERATION_TERMINAL_STATUSES
                    ):
                        findings.append(f"ASSERTION: cleanup replay.before checkpoint {operation_id} is not completed")
                    prior = frozen_before.get(operation_id, frozen_after.get(operation_id))
                    if prior is not None:
                        path = ("replay", "before" if view == "before" or operation_id in frozen_before else "after")
                        _, changes = _retain_observations(prior, operation, path)
                        if changes.conflicts:
                            findings.append(
                                f"ASSERTION: cleanup replay.{view} changed frozen checkpoint {operation_id}"
                            )
        for history in records(source.get("histories")):
            if history.get("executionArn") != execution_arn:
                continue
            for operation in records(history.get("operations")):
                operation_id = operation.get("id")
                if isinstance(operation_id, str) and operation_id.strip():
                    history_ids.add(operation_id)
                    observations.setdefault(operation_id, []).append(("history", operation))

    for operation_id, observed in observations.items():
        identity: dict[str, Any] = {"id": operation_id}
        terminal: list[Mapping[str, Any]] = []
        for view, operation in observed:
            for key in ("name", "type", "parentId"):
                if key in operation:
                    if key in identity and not same_json(identity[key], operation[key]):
                        findings.append(f"ASSERTION: cleanup checkpoint {operation_id} changed its {key}")
                    else:
                        identity[key] = operation[key]
            if view == "before" or operation.get("status") in _OPERATION_TERMINAL_STATUSES:
                terminal.append(operation)
            if (
                view == "after"
                and operation_id in before_ids
                and "status" in operation
                and operation["status"] not in _OPERATION_TERMINAL_STATUSES
            ):
                findings.append(
                    f"ASSERTION: cleanup after checkpoint {operation_id} is incomplete despite before completion"
                )
        for index, operation in enumerate(terminal):
            for previous in terminal[:index]:
                _, changes = _retain_observations(previous, operation, ("replay", "before"))
                if changes.conflicts:
                    findings.append(
                        f"ASSERTION: cleanup completed checkpoint {operation_id} has contradictory observations"
                    )
            for view, partial in observed:
                if view != "history" and "status" not in partial:
                    _, changes = _retain_observations(operation, partial, ("replay", "before"))
                    if changes.conflicts:
                        findings.append(
                            f"ASSERTION: cleanup partial checkpoint {operation_id} contradicts its completed outcome"
                        )
    retry = frozen_replay.get("retry")
    if isinstance(retry, Mapping) and isinstance(retry.get("operationId"), str) and retry["operationId"] in before_ids:
        findings.append("ASSERTION: cleanup replay.before checkpoint contains the frozen retry operation")
    if final:
        if observation_end is not None:
            for operation_id, observed in observations.items():
                completed_inside_window = [
                    row
                    for view, row in observed
                    if view == "history"
                    and row.get("status") in _OPERATION_TERMINAL_STATUSES
                    and isinstance(ended := row.get("endedAtMs"), (int, float))
                    and not isinstance(ended, bool)
                    and 0 <= ended <= observation_end
                ]
                if not completed_inside_window:
                    continue
                checkpoint = frozen_after.get(operation_id)
                if checkpoint is None:
                    findings.append(
                        f"INCOMPLETE: cleanup replay.after omits a completed service operation inside the observation window: {operation_id}"
                    )
                elif any(
                    key in row and (key not in checkpoint or not same_json(checkpoint[key], row[key]))
                    for row in completed_inside_window
                    for key in ("status", "result", "error")
                ):
                    findings.append(
                        f"ASSERTION: cleanup replay.after differs from a completed service operation inside the observation window: {operation_id}"
                    )
        for operation_id in before_ids - after_ids:
            findings.append(
                f"ASSERTION: cleanup before checkpoint {operation_id} lacks an after-checkpoint correlation"
            )
        for operation_id in after_ids - history_ids:
            findings.append(f"ASSERTION: cleanup after checkpoint {operation_id} lacks a service-history correlation")
    return findings, before_ids


def _cleanup_behavior_errors(
    original: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    *,
    standard: bool,
    known_snapshots: Sequence[Mapping[str, Any]] = (),
    observation_snapshots: Sequence[Mapping[str, Any]] = (),
    final: bool = False,
) -> list[str]:
    """Detect specific refutations without replacing frozen business evidence."""
    findings: list[str] = []

    def records(value: Any) -> list[Mapping[str, Any]]:
        return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []

    def timestamp(value: Any) -> int | float | None:
        if type(value) is int and value >= 0:
            return value
        if type(value) is float and math.isfinite(value) and value >= 0:
            return value
        return None

    def at_or_after(
        observation: Mapping[str, Any], boundary_event: Mapping[str, Any], *, require_sequence: bool = False
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
        for source in admission_sources:
            raw = [*normalize_retirement_events(records(source.get("events")), []), *records(source.get("sdkRequests"))]
            left, right = local_match(raw, observation), local_match(raw, boundary_event)
            if left is None or right is None or left is right:
                continue
            sequences = [row["sequence"] for row in (left, right) if "sequence" in row]
            if any(not is_json_integer(sequence) or sequence < 0 for sequence in sequences):
                findings.append("INCOMPLETE: same-time observations need valid semantic sequences")
                continue
            if len(sequences) < 2:
                timestamp_fallback |= not require_sequence
                continue
            if sequences[0] == sequences[1]:
                findings.append("INCOMPLETE: same-time observations need distinct semantic sequences")
                continue
            orders.add(sequences[0] > sequences[1])
        if len(orders) == 1:
            return next(iter(orders))
        if not orders and timestamp_fallback:
            return True if final else None
        if final:
            findings.append(
                "INCOMPLETE: same-time observations need sequences in one snapshot to establish their order"
            )
        return None

    attempts = {
        item["requestId"]: item for item in records(original.get("attempts")) if isinstance(item.get("requestId"), str)
    }
    events = records(original.get("events"))
    admission_sources = (original, *known_snapshots, snapshot)
    findings.extend(_cleanup_record_errors(admission_sources, snapshot if final else None))
    if final:
        expectations = original.get("expectedHistories")
        window = timestamp(original.get("observationEndedAtMs"))
        if isinstance(expectations, Mapping) and window is not None:
            for source in admission_sources:
                retirement = source.get("retirement")
                if not isinstance(retirement, Mapping):
                    continue
                histories = {
                    history["executionArn"]: history
                    for history in records(source.get("histories"))
                    if isinstance(history.get("executionArn"), str)
                }
                for row in records(retirement.get("resources")):
                    arn = row.get("id")
                    outcome = row.get("outcome")
                    settled = timestamp(row.get("settledAtMs"))
                    if (
                        row.get("kind") != "execution"
                        or not isinstance(arn, str)
                        or not isinstance(outcome, str)
                        or outcome not in TERMINAL_STATUSES
                        or settled is None
                        or settled > window
                    ):
                        continue
                    expectation = expectations.get(arn)
                    if not isinstance(expectation, Mapping):
                        continue
                    history = histories.get(arn, {})
                    if history.get("status") == outcome:
                        findings.extend(_history_expectation_mismatches(arn, history, expectation))
                    else:
                        status_expectation = {
                            key: expectation[key] for key in ("status", "allowedStatuses") if key in expectation
                        }
                        findings.extend(_history_expectation_mismatches(arn, {"status": outcome}, status_expectation))
        request_frames = [records(source.get("sdkRequests")) for source in admission_sources]
        for source in admission_sources:
            for event in records(source.get("events")):
                kind = event.get("kind")
                method = _CALL_METHODS.get(kind) if isinstance(kind, str) else None
                if method is None:
                    continue
                outcome = event.get("outcome", "DISPATCHED")
                if outcome not in ("DISPATCHED", "REJECTED"):
                    findings.append("INCOMPLETE: cleanup SDK call outcome must be DISPATCHED or REJECTED")
                elif any(
                    sum(
                        request.get("method") == method
                        and request.get("outcome") == outcome
                        and all(request.get(key) == event.get(key) for key in ("requestId", "callId", "atMs"))
                        for request in requests
                    )
                    > 1
                    for requests in request_frames
                ) or not any(
                    _has_matching_sdk_request(event, method, outcome, requests) for requests in request_frames
                ):
                    finding = "ASSERTION: cleanup observed SDK call lacks a unique matching SDK request"
                    if finding not in findings:
                        findings.append(finding)
    final_retirement = snapshot.get("retirement")
    final_end = final_retirement.get("retiredAtMs") if final and isinstance(final_retirement, Mapping) else None
    releases, release_errors = retained_fault_releases((*observation_snapshots, *admission_sources), final_end)
    findings.extend(release_errors)
    known_requests = {
        attempt["requestId"]
        for source in admission_sources
        for attempt in records(source.get("attempts"))
        if isinstance(attempt.get("requestId"), str)
    }
    closures: dict[str, list[Mapping[str, Any]]] = {}
    # A later observation can reveal an earlier closure. Preserve every known
    # boundary and recheck old activity as well as newly discovered requests.
    for event in (event for source in admission_sources for event in records(source.get("events"))):
        request_id, time = event.get("requestId"), timestamp(event.get("atMs"))
        if (
            isinstance(request_id, str)
            and request_id in known_requests
            and time is not None
            and event.get("kind") in ("wrapper_return", "wrapper_raise", "admission_closed")
        ):
            closures.setdefault(request_id, []).append(event)
    selected_victim = original.get("selectedRequests", {})
    disposal_victim = (
        selected_victim.get("victim")
        if standard
        and isinstance(selected_victim, Mapping)
        and original.get("requirementId") in ("lmi-lifecycle-2", "lmi-lifecycle-7")
        else None
    )
    for collection in ("sdkRequests", "events"):
        for record in (record for source in admission_sources for record in records(source.get(collection))):
            if collection == "events" and record.get("kind") != "admission_probe":
                continue
            if collection == "sdkRequests" and record.get("outcome") != "DISPATCHED":
                continue
            request_id, time = record.get("requestId"), timestamp(record.get("atMs"))
            boundaries = closures.get(request_id, ()) if isinstance(request_id, str) else ()
            if time is None:
                continue
            following = [boundary for boundary in boundaries if at_or_after(record, boundary)]
            if not following:
                continue
            if collection == "events" and request_id == disposal_victim:
                exits = {
                    (boundary.get("kind"), boundary.get("atMs")): boundary
                    for boundary in boundaries
                    if boundary.get("kind") != "admission_closed"
                }
                wrapper = next(iter(exits.values())) if len(exits) == 1 else None
                if wrapper is not None and wrapper.get("kind") == "wrapper_return":
                    # Repeated snapshots can hold the very same wrapper exit.
                    canonical = [
                        wrapper
                        if boundary.get("kind") == "wrapper_return" and boundary["atMs"] == wrapper["atMs"]
                        else boundary
                        for boundary in following
                    ]
                    if permits_disposal_operation_probe(
                        record,
                        wrapper,
                        canonical,
                        lambda first, second: (
                            None if (order := at_or_after(first, second, require_sequence=True)) is None else not order
                        ),
                    ):
                        continue
            if collection == "sdkRequests" and record.get("outcome") == "DISPATCHED":
                finding = f"ASSERTION: request {request_id} dispatched SDK work after admission closed"
            elif (
                collection == "events"
                and record.get("probe") in ("operation", "checkpoint")
                and record.get("outcome") != "REJECTED"
            ):
                finding = f"ASSERTION: request {request_id} admitted a {record['probe']} probe after admission closed"
            else:
                continue
            if finding not in findings:
                findings.append(finding)
    if not standard:
        return findings

    selected = original.get("selectedRequests")
    if not isinstance(selected, Mapping):
        return findings
    current_attempts = {
        item["requestId"]: item for item in records(snapshot.get("attempts")) if isinstance(item.get("requestId"), str)
    }

    def role(role_name: str) -> Mapping[str, Any]:
        request_id = selected.get(role_name)
        return attempts.get(request_id, {}) if isinstance(request_id, str) else {}

    def boundary_record(role_name: str, kind: str) -> Mapping[str, Any]:
        request_id = role(role_name).get("requestId")
        matches = [
            event
            for event in events
            if request_id is not None and event.get("requestId") == request_id and event.get("kind") == kind
        ]
        return matches[0] if len(matches) == 1 else {}

    def boundary(role_name: str, kind: str) -> int | float | None:
        return timestamp(boundary_record(role_name, kind).get("atMs"))

    def role_attempts(role_name: str) -> list[Mapping[str, Any]]:
        request_id = role(role_name).get("requestId")
        if request_id is None:
            return []
        return [
            attempt
            for source in admission_sources
            for attempt in records(source.get("attempts"))
            if attempt.get("requestId") == request_id
        ]

    def platform_times(role_name: str, key: str) -> list[int | float]:
        return [
            time
            for attempt in role_attempts(role_name)
            if attempt.get(f"{key}Source") == "platform" and (time := timestamp(attempt.get(key))) is not None
        ]

    def native_time(role_name: str, key: str) -> int | float | None:
        authoritative = platform_times(role_name, key)
        if authoritative:
            return max(authoritative) if key == "startedAtMs" else min(authoritative)
        previous = role(role_name)
        current = current_attempts.get(previous.get("requestId"), {})
        return timestamp(current[key] if key in current else previous.get(key))

    def changed_time(role_name: str, key: str) -> bool:
        previous = timestamp(role(role_name).get(key))
        current = native_time(role_name, key)
        return current is not None and current != previous

    def observed_events(role_name: str, kind: str) -> list[Mapping[str, Any]]:
        owner = role(role_name)
        request_id = owner.get("requestId")
        observed = []
        for source in admission_sources:
            rows = records(source.get("events"))
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
                    if final:
                        findings.append(f"INCOMPLETE: cleanup {kind} event has inconsistent native ownership")
                    continue
                if "atMs" in event and timestamp(event["atMs"]) is None and final:
                    findings.append(f"INCOMPLETE: cleanup {kind} event has an invalid timestamp")
                observed.append(event)
        return observed

    def known_interval(role_name: str) -> tuple[int | float | None, int | float | None]:
        native = role_attempts(role_name)
        starts = [time for attempt in native if (time := timestamp(attempt.get("startedAtMs"))) is not None]
        ends = [time for attempt in native if (time := timestamp(attempt.get("endedAtMs"))) is not None]
        platform_starts = platform_times(role_name, "startedAtMs")
        platform_ends = platform_times(role_name, "endedAtMs")
        return (
            max(platform_starts) if platform_starts else min(starts, default=None),
            min(platform_ends) if platform_ends else max(ends, default=None),
        )

    requirement = original.get("requirementId")
    if requirement in ("lmi-lifecycle-2", "lmi-lifecycle-7"):
        returned, exited = boundary("victim", "wrapper_return"), boundary("victim", "task_exit")
        for event in observed_events("victim", "root_completed"):
            if "outcome" in event and not same_json(event["outcome"], role("victim").get("outcome")):
                findings.append("ASSERTION: cleanup root completion contradicts the frozen invocation outcome")
            completed_at = timestamp(event.get("atMs"))
            if completed_at is not None and any(end is not None and completed_at > end for end in (returned, exited)):
                findings.append("ASSERTION: cleanup root completion follows the frozen wrapper or task exit")
    marker_ids: dict[str, set[str]] = {}
    if final:
        corrected: dict[str, Mapping[str, Any]] = dict(attempts)
        for source in admission_sources:
            for attempt in records(source.get("attempts")):
                request, start = attempt.get("requestId"), timestamp(attempt.get("startedAtMs"))
                if not isinstance(request, str) or start is None:
                    continue
                previous = corrected.get(request, {})
                prior = timestamp(previous.get("startedAtMs"))
                corrected[request] = {
                    **previous,
                    **attempt,
                    "startedAtMs": min(prior, start) if prior is not None else start,
                }
        for request, bounds in _native_intervals(admission_sources).items():
            if request in corrected and "startedAtMs" in bounds:
                corrected[request] = {**corrected[request], "startedAtMs": bounds["startedAtMs"][0]}
        admissions: dict[str, list[tuple[int | float, int]]] = {}
        for attempt in corrected.values():
            role_name, start, ordinal = (
                attempt.get("role"),
                timestamp(attempt.get("startedAtMs")),
                attempt.get("attempt"),
            )
            if isinstance(role_name, str) and start is not None and is_json_integer(ordinal):
                admissions.setdefault(role_name, []).append((start, int(ordinal)))
        for role_name, order in admissions.items():
            if any(left[0] < right[0] and left[1] > right[1] for left, right in pairwise(sorted(order))):
                findings.append(f"PLACEMENT: cleanup attempt ordinals contradict admission order for {role_name}")
        for role_name, request in selected.items():
            check_original = (
                role_name == "victim"
                if requirement == "lmi-lifecycle-5"
                else role_name != "resume" and (role_name != "recovery" or requirement == "lmi-lifecycle-4")
            )
            reject_tie = (requirement == "lmi-lifecycle-5" and role_name == "victim") or (
                requirement == "lmi-lifecycle-4"
                and (
                    role_name in ("victim", "recovery") or (isinstance(role_name, str) and role_name.startswith("peer"))
                )
            )
            if not check_original or not isinstance(request, str):
                continue
            original_attempt = corrected.get(request, {})
            earlier, tied = original_admission_conflicts(original_attempt, list(corrected.values()))
            if earlier:
                findings.append(f"PLACEMENT: cleanup selected {role_name} is not the original invocation")
            if reject_tie and tied:
                findings.append(
                    f"PLACEMENT: cleanup tied native admissions cannot establish the original {role_name} invocation"
                )
        if requirement == "lmi-lifecycle-1":
            for role_name in ("victim", "peer"):
                execution = role(role_name).get("executionArn")
                selected_request = role(role_name).get("requestId")
                window = timestamp(original.get("observationEndedAtMs"))
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
                    findings.append(
                        f"ASSERTION: overlap {role_name} needs an original successful execution without retry"
                    )
                markers = {
                    operation["id"]
                    for source in admission_sources
                    for history in records(source.get("histories"))
                    if history.get("executionArn") == execution
                    for operation in records(history.get("operations"))
                    if operation.get("type") == "STEP"
                    and operation.get("name") == "record-marker"
                    and isinstance(operation.get("id"), str)
                }
                marker_ids[role_name] = markers
                if len(markers) > 1:
                    findings.append(f"ASSERTION: cleanup overlap {role_name} observed more than one record-marker step")
    if requirement in ("lmi-lifecycle-1", "lmi-lifecycle-4") and final:
        # Native metadata can widen a provisional interval. A barrier claim is
        # disproved only after every supplied native interval is available.
        intervals = [known_interval(name) for name in ("victim", "peer")]
        starts = [start for start, _ in intervals if start is not None]
        ends = [end for _, end in intervals if end is not None]
        if len(starts) == len(ends) == 2:
            for name in ("victim", "peer"):
                for event in observed_events(name, "barrier_arrived"):
                    arrived = timestamp(event.get("atMs"))
                    if arrived is not None and not max(starts) <= arrived < min(ends):
                        findings.append("PLACEMENT: cleanup barrier lies outside the established native overlap")
            if requirement == "lmi-lifecycle-1":
                requests = [row for source in admission_sources for row in records(source.get("sdkRequests"))]
                for name, markers in marker_ids.items():
                    if len(markers) == 1 and not has_overlap_marker_dispatch(
                        requests, role(name).get("requestId"), next(iter(markers)), max(starts), min(ends)
                    ):
                        findings.append(
                            f"INCOMPLETE: cleanup overlap {name} lacks a marker checkpoint dispatch inside native overlap"
                        )
    unique_kinds = {
        "lmi-lifecycle-2": "root_completed",
        "lmi-lifecycle-7": "root_completed",
        "lmi-lifecycle-3": "platform_timeout",
        "lmi-lifecycle-4": "platform_timeout",
        "lmi-lifecycle-5": "pending",
    }
    unique_kind = unique_kinds.get(requirement) if isinstance(requirement, str) else None
    if unique_kind is not None:
        request = role("victim").get("requestId")
        if isinstance(request, str) and any(
            sum(
                row.get("requestId") == request and row.get("kind") == unique_kind
                for row in records(source.get("events"))
            )
            > 1
            for source in admission_sources
        ):
            findings.append(f"INCOMPLETE: request {request} needs exactly one {unique_kind} event")
    if requirement == "lmi-lifecycle-4":
        recovery = role("recovery").get("requestId")
        if isinstance(recovery, str) and any(
            sum(
                row.get("requestId") == recovery and row.get("kind") == "worker_recovered"
                for row in records(source.get("events"))
            )
            > 1
            for source in admission_sources
        ):
            findings.append(f"INCOMPLETE: request {recovery} needs exactly one worker_recovered event")
    if requirement in ("lmi-lifecycle-3", "lmi-lifecycle-4"):
        profile = original.get("profile")
        profile = profile if isinstance(profile, Mapping) else {}
        fixture_budgets = original.get("budgets")
        fixture_budgets = fixture_budgets if isinstance(fixture_budgets, Mapping) else {}
        grace = timestamp(fixture_budgets.get("observationGraceMs"))
        window_end = timestamp(original.get("observationEndedAtMs"))
        start, end = known_interval("victim")
        configured_timeout = timestamp(profile.get("timeoutMs"))
        controlled_task = boundary_record("victim", "work_in_flight").get("taskId")
        work_observations = observed_events("victim", "work_in_flight")
        task_exits = [
            event for event in observed_events("victim", "task_exit") if event.get("taskId") == controlled_task
        ]
        task_starts = [
            event for event in observed_events("victim", "task_start") if event.get("taskId") == controlled_task
        ]
        for work in work_observations:
            if isinstance(controlled_task, str) and work.get("taskId", controlled_task) != controlled_task:
                findings.append("ASSERTION: cleanup in-flight work does not identify the controlled deadline task")
            if any(at_or_after(work, start_event, require_sequence=True) is False for start_event in task_starts):
                findings.append("ASSERTION: cleanup controlled task started after it was observed in flight")
            if any(at_or_after(work, exit_event, require_sequence=True) is True for exit_event in task_exits):
                findings.append("ASSERTION: cleanup in-flight work follows the controlled task exit")
        timeout_events = observed_events("victim", "platform_timeout")
        for event in timeout_events:
            if "source" in event and event["source"] != "platform":
                findings.append("ASSERTION: cleanup timeout source is not the observed platform deadline")
            timed_out = timestamp(event.get("atMs"))
            if timed_out is None:
                continue
            if (
                final
                and requirement == "lmi-lifecycle-3"
                and grace is not None
                and window_end is not None
                and window_end - timed_out < grace
            ):
                findings.append("INCOMPLETE: cleanup observation window lacks the declared grace after timeout")
            if final and (
                (start is not None and configured_timeout is not None and timed_out < start + configured_timeout)
                or (end is not None and timed_out > end)
            ):
                findings.append("ASSERTION: cleanup timeout falls outside the configured native deadline")
            if any(at_or_after(work, event, require_sequence=True) is True for work in work_observations) or any(
                at_or_after(exit_event, event, require_sequence=True) is False for exit_event in task_exits
            ):
                findings.append("ASSERTION: cleanup timeout contradicts the frozen in-flight work or task exit")
            for observed_measurements, budget_key in (
                (observed_events("victim", "admission_closed"), "admissionMs"),
                (task_exits, "cleanupMs"),
            ):
                budget = timestamp(fixture_budgets.get(budget_key))
                for measured in observed_measurements:
                    measured_at = timestamp(measured.get("atMs"))
                    if at_or_after(measured, event, require_sequence=True) is False:
                        if budget_key == "admissionMs":
                            findings.append("ASSERTION: cleanup admission closed before the observed platform timeout")
                        else:
                            findings.append("ASSERTION: cleanup task exited before the observed platform timeout")
                    elif measured_at is not None and budget is not None and measured_at > timed_out + budget:
                        findings.append(f"ASSERTION: cleanup timeout contradicts the frozen {budget_key} measurement")
        if requirement == "lmi-lifecycle-4":
            if profile.get("recoveryPolicy") == "replace-worker":
                recovery_id = role("recovery").get("requestId")
                for source in admission_sources:
                    for attempt in records(source.get("attempts")):
                        if (
                            recovery_id is not None
                            and attempt.get("requestId") == recovery_id
                            and "replacesWorkerId" in attempt
                            and not same_json(attempt["replacesWorkerId"], role("victim").get("workerId"))
                        ):
                            findings.append("PLACEMENT: cleanup recovery replacement changed the frozen failed worker")
            recovery_start, _ = known_interval("recovery")
            recovery_budget = timestamp(fixture_budgets.get("recoveryMs"))
            if (
                final
                and recovery_start is not None
                and any(
                    (at := timestamp(event.get("atMs"))) is not None and recovery_start < at for event in timeout_events
                )
            ):
                findings.append("ASSERTION: cleanup evidence places recovery before the victim timeout")
            for event in observed_events("recovery", "worker_recovered"):
                recovered = timestamp(event.get("atMs"))
                if recovered is None:
                    continue
                if final and grace is not None and window_end is not None and window_end - recovered < grace:
                    findings.append("INCOMPLETE: cleanup observation window lacks the declared grace after recovery")
                if final and recovery_start is not None and recovered < recovery_start:
                    findings.append("ASSERTION: cleanup worker recovery precedes its native invocation")
                for timeout_event in timeout_events:
                    timed_out = timestamp(timeout_event.get("atMs"))
                    if (
                        timed_out is not None
                        and recovery_budget is not None
                        and not (timed_out <= recovered <= timed_out + recovery_budget)
                    ):
                        findings.append("ASSERTION: cleanup worker recovery exceeds the frozen recovery measurement")
                if final:
                    for peer in selected:
                        if isinstance(peer, str) and peer.startswith("peer"):
                            _, peer_end = known_interval(peer)
                            if peer_end is not None and recovered > peer_end:
                                findings.append("PLACEMENT: cleanup worker recovery outlived established peer capacity")
                            progress = [
                                at
                                for row in observed_events(peer, "peer_progress")
                                if (at := timestamp(row.get("atMs"))) is not None
                            ]
                            for timeout_event in timeout_events:
                                timed_out = timestamp(timeout_event.get("atMs"))
                                if timed_out is not None and not peer_progress_spans_timeout(
                                    progress, timed_out, recovered
                                ):
                                    findings.append(
                                        "ASSERTION: cleanup healthy peer lacks progress across the timeout and recovery"
                                    )
    if requirement in ("lmi-lifecycle-3", "lmi-lifecycle-4"):
        limits = [boundary("victim", "task_exit")]
        if requirement == "lmi-lifecycle-4":
            limits.extend(timestamp(event.get("atMs")) for event in observed_events("recovery", "worker_recovered"))
        known_limits = [time for time in limits if time is not None]
        # A later release acknowledgment cannot erase an earlier release from
        # either observation records or a provisional/final retirement record.
        if releases and known_limits and min(releases) <= max(known_limits):
            findings.append("ASSERTION: cleanup evidence reports fault release before the asserted cleanup or recovery")
    if requirement == "lmi-lifecycle-6" and (
        changed_time("victim", "endedAtMs") or changed_time("warm", "startedAtMs")
    ):
        start, end, returned = (
            native_time("warm", "startedAtMs"),
            native_time("victim", "endedAtMs"),
            boundary("victim", "wrapper_return"),
        )
        if start is not None and ((end is not None and start < end) or (returned is not None and start <= returned)):
            findings.append("PLACEMENT: cleanup evidence disproves sequential warm reuse")
    if requirement == "lmi-lifecycle-4" and (
        changed_time("victim", "endedAtMs") or changed_time("recovery", "startedAtMs")
    ):
        start, end = native_time("recovery", "startedAtMs"), native_time("victim", "endedAtMs")
        victim, recovery = role("victim"), role("recovery")
        if (
            original.get("runtime") == "python"
            and victim.get("processId")
            and victim.get("processId") == recovery.get("processId")
            and start is not None
            and end is not None
            and start < end
        ):
            findings.append("PLACEMENT: cleanup evidence places Python recovery before victim native completion")
    if requirement in ("lmi-lifecycle-5", "lmi-lifecycle-6"):
        sources = (original, *known_snapshots, snapshot)
        executions_by_request: dict[str, set[str]] = {}
        for source in sources:
            for attempt in records(source.get("attempts")):
                request_id, execution_arn = attempt.get("requestId"), attempt.get("executionArn")
                if (
                    isinstance(request_id, str)
                    and request_id.strip()
                    and isinstance(execution_arn, str)
                    and execution_arn.strip()
                ):
                    executions_by_request.setdefault(request_id, set()).add(execution_arn)

        def bound_execution(request_id: Any) -> str | None:
            # A new native request may only gain its execution binding during
            # retirement. Do not turn that provisional absence into a failure.
            if not isinstance(request_id, str) or not request_id.strip():
                if final:
                    findings.append("INCOMPLETE: cleanup replay/activity lacks an identified native request")
                return None
            if request_id not in attempts and not final:
                return None
            bindings = executions_by_request.get(request_id, set())
            if len(bindings) != 1:
                if final:
                    findings.append(
                        f"INCOMPLETE: cleanup native request {request_id} has an unresolved or conflicting execution binding"
                    )
                return None
            return next(iter(bindings))

        if requirement == "lmi-lifecycle-5":
            replay = original.get("replay")
            replay = replay if isinstance(replay, Mapping) else {}
            completed = {
                operation["id"]
                for operation in records(replay.get("before"))
                if isinstance(operation.get("id"), str) and operation.get("status") in _OPERATION_TERMINAL_STATUSES
            }
            expected_counts = {
                item["operationId"]: item["expectedCount"]
                for item in records(replay.get("sideEffects"))
                if isinstance(item.get("operationId"), str) and is_json_integer(item.get("expectedCount"))
            }
            victim = role("victim")
            checkpoint_errors, observed_before = _cleanup_checkpoint_errors(
                replay,
                sources,
                victim.get("executionArn"),
                final=final,
                observation_end=timestamp(original.get("observationEndedAtMs")),
            )
            findings.extend(checkpoint_errors)
            completed.update(observed_before)
            required_counts = set(completed)
            retry = replay.get("retry")
            if isinstance(retry, Mapping) and isinstance(retry.get("operationId"), str):
                required_counts.add(retry["operationId"])
            covered_counts: set[str] = set()
            for source in sources:
                fragment = source.get("replay")
                if not isinstance(fragment, Mapping):
                    continue
                for count in records(fragment.get("sideEffects")):
                    operation_id, expected = count.get("operationId"), count.get("expectedCount")
                    if (
                        isinstance(operation_id, str)
                        and operation_id in required_counts
                        and is_json_integer(expected)
                        and expected >= 0
                    ):
                        expected_counts.setdefault(operation_id, expected)
                    if isinstance(operation_id, str) and is_json_integer(actual := count.get("count")) and actual >= 0:
                        covered_counts.add(operation_id)
            if final:
                for missing in sorted(required_counts - (expected_counts.keys() & covered_counts)):
                    findings.append(
                        f"INCOMPLETE: cleanup replay completed operation lacks side-effect count coverage for {missing}"
                    )
            returned = boundary("victim", "wrapper_return")
            resume_id = role("resume").get("requestId")
            checkpoint_ends = _observed_checkpoint_ends(sources, victim.get("executionArn"))
            native_intervals = _native_intervals(sources)
            native_starts = {
                request: bounds["startedAtMs"][0]
                for request, bounds in native_intervals.items()
                if "startedAtMs" in bounds
            }
            resume_start = (
                native_intervals.get(resume_id, {}).get("startedAtMs") if isinstance(resume_id, str) else None
            )
            if (
                resume_start is not None
                and (final or resume_start[1])
                and returned is not None
                and resume_start[0] <= returned
            ):
                findings.append("ASSERTION: cleanup evidence places resume before the frozen PENDING wrapper return")
            if final:
                reconciled: dict[str, dict[str, Any]] = {}
                for source in sources:
                    for attempt in records(source.get("attempts")):
                        request = attempt.get("requestId")
                        if (
                            not isinstance(request, str)
                            or request not in native_starts
                            or executions_by_request.get(request, set()) != {victim.get("executionArn")}
                        ):
                            continue
                        reconciled[request] = {
                            **reconciled.get(request, {}),
                            **attempt,
                            "startedAtMs": native_starts[request],
                        }
                previously_witnessed = completed_suspension_checkpoint(
                    [
                        attempt
                        for attempt in attempts.values()
                        if attempt.get("executionArn") == victim.get("executionArn")
                        and timestamp(attempt.get("startedAtMs")) is not None
                    ],
                    events,
                    records(original.get("sdkRequests")),
                    records(replay.get("after")),
                )
                reconciled_after = [
                    {**operation, "endedAtMs": max(times)}
                    if isinstance(operation_id := operation.get("id"), str)
                    and (times := checkpoint_ends.get(operation_id, set()))
                    else operation
                    for operation in records(replay.get("after"))
                ]
                if previously_witnessed and not completed_suspension_checkpoint(
                    list(reconciled.values()),
                    [event for source in sources for event in records(source.get("events"))],
                    [request for source in sources for request in records(source.get("sdkRequests"))],
                    reconciled_after,
                ):
                    findings.append(
                        "ASSERTION: cleanup resume does not follow the observed wait or callback completion"
                    )
            observed_pending = [
                event for event in observed_events("victim", "pending") if timestamp(event.get("atMs")) is not None
            ]
            pending_times = {at for event in observed_pending if (at := timestamp(event.get("atMs"))) is not None}
            if final and pending_times:
                before_operations = []
                after_operations = []
                for source in sources:
                    replay_fragment = source.get("replay")
                    if isinstance(replay_fragment, Mapping):
                        before_operations.extend(records(replay_fragment.get("before")))
                        after_operations.extend(records(replay_fragment.get("after")))
                    for history in records(source.get("histories")):
                        if history.get("executionArn") == victim.get("executionArn"):
                            after_operations.extend(records(history.get("operations")))
                omitted, _ = replay_checkpoint_timing(
                    before_operations, after_operations, max(pending_times), checkpoint_ends
                )
                _, late = replay_checkpoint_timing(
                    before_operations, after_operations, min(pending_times), checkpoint_ends
                )
                if omitted:
                    findings.append(
                        "INCOMPLETE: cleanup replay.before omits an operation known to have completed before suspension"
                    )
                if late:
                    findings.append(
                        "ASSERTION: cleanup replay.before contains an operation that completed after suspension"
                    )
                frozen_ids = {
                    operation["id"]
                    for operation in records(replay.get("before"))
                    if isinstance(operation.get("id"), str)
                }
                newly_completed_steps = {
                    operation["id"]
                    for operation in (*before_operations, *after_operations)
                    if isinstance(operation.get("id"), str)
                    and operation["id"] in completed - frozen_ids
                    and operation.get("type") == "STEP"
                    and operation.get("status") in _OPERATION_TERMINAL_STATUSES
                }
                body_observations = [
                    event
                    for source in sources
                    for event in records(source.get("events"))
                    if timestamp(event.get("atMs")) is not None
                ]
                execution_entries = [
                    entry
                    for source in sources
                    if isinstance(fragment := source.get("replay"), Mapping)
                    for entry in records(fragment.get("operationExecutions"))
                ]
                for operation_id in newly_completed_steps:
                    step_ends = checkpoint_ends.get(operation_id, set())
                    if not completed_replay_step_witness(
                        operation_id,
                        victim.get("requestId"),
                        execution_entries,
                        body_observations,
                        lambda event: all(
                            at_or_after(event, pending, require_sequence=True) is False for pending in observed_pending
                        ),
                        min(step_ends) if step_ends else None,
                    ):
                        findings.append(
                            "INCOMPLETE: cleanup completed replay step lacks observed user-code execution "
                            "on the original request before suspension"
                        )
            for source in sources:
                for event in records(source.get("events")):
                    if (
                        event.get("kind") == "body"
                        and isinstance(operation_id := event.get("operationId"), str)
                        and operation_id in completed
                    ):
                        request_id = event.get("requestId")
                        execution_arn = bound_execution(request_id)
                        if (
                            execution_arn is not None
                            and execution_arn == victim.get("executionArn")
                            and (
                                request_id != victim.get("requestId")
                                or any(
                                    at_or_after(event, pending, require_sequence=True) is True
                                    for pending in observed_pending
                                )
                            )
                        ):
                            findings.append(
                                f"ASSERTION: cleanup evidence reports repeated completed user code for {operation_id}"
                            )
                        end_times = checkpoint_ends.get(operation_id, set())
                        body_at = timestamp(event.get("atMs"))
                        if (
                            execution_arn is not None
                            and execution_arn == victim.get("executionArn")
                            and body_at is not None
                            and end_times
                            and body_at > min(end_times)
                        ):
                            findings.append(
                                "ASSERTION: cleanup completed replay body occurred after recorded completion"
                            )
                    if event.get("kind") != "pending" or event.get("requestId") != victim.get("requestId"):
                        continue
                    request_id = event.get("requestId")
                    execution_arn = bound_execution(request_id)
                    if execution_arn is None:
                        continue
                    if any(
                        key in event and event[key] != expected
                        for key, expected in (("ownerRequestId", request_id), ("executionArn", execution_arn))
                    ):
                        if final:
                            findings.append("INCOMPLETE: cleanup PENDING event has inconsistent native ownership")
                        continue
                    if "atMs" not in event:
                        continue  # A partial event may gain its timestamp during collection.
                    pending_at = timestamp(event["atMs"])
                    if pending_at is None:
                        if final:
                            findings.append("INCOMPLETE: cleanup PENDING event has an invalid timestamp")
                    elif returned is not None and pending_at > returned:
                        findings.append("ASSERTION: cleanup PENDING event follows the frozen wrapper return")
                if "replay" not in source:
                    continue
                observed_replay = source["replay"]
                if not isinstance(observed_replay, Mapping):
                    if final:
                        findings.append("INCOMPLETE: cleanup replay observations must be an object")
                    continue
                entries = observed_replay.get("operationExecutions", [])
                if not isinstance(entries, list):
                    if final:
                        findings.append("INCOMPLETE: cleanup operation executions must be an array")
                    entries = []
                for entry in entries:
                    if not isinstance(entry, Mapping):
                        if final:
                            findings.append("INCOMPLETE: cleanup operation execution must be an object")
                        continue
                    request_id, operation_id = entry.get("requestId"), entry.get("operationId")
                    execution_arn = bound_execution(request_id)
                    if not isinstance(operation_id, str) or not operation_id.strip():
                        if final:
                            findings.append("INCOMPLETE: cleanup operation execution lacks an operationId")
                        continue
                    if (
                        execution_arn is not None
                        and execution_arn == victim.get("executionArn")
                        and request_id != victim.get("requestId")
                        and operation_id in completed
                    ):
                        findings.append(
                            f"ASSERTION: cleanup evidence reports repeated completed user code for {operation_id}"
                        )
                if final:
                    findings.extend(_side_effect_observation_errors(source))
                counts = observed_replay.get("sideEffects", [])
                if not isinstance(counts, list):
                    if final:
                        findings.append("INCOMPLETE: cleanup side-effect observations must be an array")
                    counts = []
                for count in counts:
                    if not isinstance(count, Mapping):
                        if final:
                            findings.append("INCOMPLETE: cleanup side-effect observation must be an object")
                        continue
                    operation_id = count.get("operationId")
                    if not isinstance(operation_id, str) or not operation_id.strip():
                        if final:
                            findings.append("INCOMPLETE: cleanup side-effect observation lacks an operationId")
                        continue
                    expected = expected_counts.get(operation_id)
                    if expected is None:
                        continue  # Cleanup cannot invent an oracle for an unasserted operation.
                    if "expectedCount" in count and not same_json(count["expectedCount"], expected):
                        findings.append(
                            f"ASSERTION: cleanup side-effect expectation for {operation_id} changed the frozen fixture oracle"
                        )
                    if is_json_integer(count.get("count")) and count["count"] != expected:
                        findings.append(
                            f"ASSERTION: cleanup side-effect count for {operation_id} differs from the frozen fixture oracle"
                        )
            findings.extend(
                _cleanup_retry_errors(
                    replay,
                    sources,
                    victim.get("executionArn"),
                    bound_execution,
                    lambda observation, boundary_event: at_or_after(observation, boundary_event, require_sequence=True),
                    final=final,
                )
            )
        else:
            victim_id = role("victim").get("requestId")
            exited = boundary("victim", "task_exit")
            for source in sources:
                for event in records(source.get("events")):
                    if event.get("kind") != "late_activity":
                        continue
                    request_id = event.get("requestId")
                    execution_arn = bound_execution(request_id)
                    if execution_arn is None:
                        continue
                    if event.get("ownerRequestId") != request_id or event.get("executionArn") != execution_arn:
                        if final:
                            findings.append("INCOMPLETE: cleanup late activity has inconsistent native ownership")
                        continue
                    during = event.get("observedDuringRequestId")
                    if during is not None and bound_execution(during) is None:
                        continue
                    observed_at = timestamp(event.get("atMs"))
                    if (
                        request_id == victim_id
                        and observed_at is not None
                        and exited is not None
                        and observed_at > exited
                    ):
                        findings.append("ASSERTION: cleanup late activity continued after the frozen victim task exit")
    return findings


def _evaluate_capture(
    evidence: Mapping[str, Any],
    *,
    retirement_evidence: Mapping[str, Any],
    cleanup_evidence: Mapping[str, Sequence[Mapping[str, Any]]],
    validator: EvidenceValidator,
    standard: bool,
    observation_evidence: Sequence[Mapping[str, Any]] = (),
    attempts: Sequence[Mapping[str, Any]] = (),
) -> tuple[list[str], list[str], dict[str, Any]]:
    """Evaluate captured evidence without querying a live target or mutating the capture."""
    if not evidence:
        return [], ["INCOMPLETE: no captured observations"], {}

    try:
        findings = list(validator(copy.deepcopy(dict(evidence)), phase="observations"))
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError, RecursionError):
        return [], ["INCOMPLETE: malformed captured observations"], dict(evidence)
    retry_orderings: set[tuple[str, ...]] = set()
    for snapshot in observation_evidence:
        findings.extend(_side_effect_observation_errors(snapshot))
        order = _retry_attempt_order(snapshot)
        if order is None:
            continue
        if len(set(order)) != len(order):
            findings.append("INCOMPLETE: duplicate retry attemptId in observations")
            continue
        for previous in retry_orderings:
            shared = set(previous) & set(order)
            if tuple(item for item in previous if item in shared) != tuple(item for item in order if item in shared):
                findings.append("Observed retry attempt order conflicts with earlier observations")
                break
        retry_orderings.add(order)

    findings.extend(_side_effect_progress_errors(observation_evidence))
    observation_findings = list(findings)
    final_evidence = dict(evidence)
    try:
        final_evidence = copy.deepcopy(final_evidence)
        frames = [frame for snapshots in cleanup_evidence.values() for frame in snapshots]
        retirement_snapshot = retirement_evidence or evidence
        findings.extend(
            _cleanup_behavior_errors(
                evidence,
                retirement_snapshot,
                standard=standard,
                known_snapshots=frames,
                observation_snapshots=observation_evidence,
                final=True,
            )
        )

        raw_retirement = retirement_evidence.get("retirement")
        if isinstance(raw_retirement, Mapping):
            retirement = copy.deepcopy(dict(raw_retirement))
            for field, inventory in (("resources", "resourcesCreated"), ("faults", "faultsCreated")):
                original = evidence.get(inventory)
                original_ids = {
                    item["id"]
                    for item in (original if isinstance(original, list) else [])
                    if isinstance(item, Mapping) and isinstance(item.get("id"), str)
                }
                settled = retirement.get(field)
                retirement[field] = [
                    item
                    for item in (settled if isinstance(settled, list) else [])
                    if isinstance(item, Mapping) and isinstance(item.get("id"), str) and item["id"] in original_ids
                ]
            window = evidence.get("observationEndedAtMs")
            existing_completions = retirement.get("completions")
            candidates = []
            if "completions" in retirement:
                if not isinstance(existing_completions, list):
                    findings.append("INCOMPLETE: malformed captured retirement completions")
                else:
                    identities = {kind: identity for _, kind, identity in _WORK_INTERVALS}
                    records = [item for item in existing_completions if isinstance(item, Mapping)]
                    normalized = normalize_retirement_events(records, findings)
                    for item, shape in zip(records, normalized, strict=True):
                        kind = shape.get("kind")
                        identity = identities.get(kind) if isinstance(kind, str) else None
                        if (
                            identity is not None
                            and isinstance(shape.get("requestId"), str)
                            and isinstance(shape.get(identity), str)
                        ):
                            candidates.append(item)
                    if len(candidates) != len(existing_completions):
                        findings.append("INCOMPLETE: malformed captured retirement completions")
            final_events = retirement_evidence.get("events")
            if isinstance(window, (int, float)) and not isinstance(window, bool) and isinstance(final_events, list):
                candidates.extend(
                    event
                    for event in final_events
                    if isinstance(event, Mapping)
                    and isinstance(at := event.get("atMs"), (int, float))
                    and not isinstance(at, bool)
                    and at > window
                )
            if candidates or "completions" in retirement:
                original_events = evidence.get("events")
                completions = _project_retirement_completions(
                    original_events if isinstance(original_events, list) else [], candidates
                )
                if completions or "completions" in retirement:
                    retirement["completions"] = completions
            final_evidence["retirement"] = retirement
        observed_attempts = evidence.get("attempts")
        observed_requests = {
            attempt["requestId"]
            for attempt in (observed_attempts if isinstance(observed_attempts, list) else [])
            if isinstance(attempt, Mapping) and isinstance(attempt.get("requestId"), str)
        }
        if any(
            "launchedAtMs" in attempt
            and isinstance(attempt.get("requestId"), str)
            and attempt["requestId"] not in observed_requests
            for attempt in attempts
        ):
            findings.append("INCOMPLETE: frozen observations do not cover all client-journaled launches")
        if (
            validator is validate_evidence
            and standard
            and evidence.get("requirementId") in ("lmi-lifecycle-2", "lmi-lifecycle-7")
        ):
            retired_requests: dict[str, set[str]] = {}
            if retirement_evidence and not validate_retirement_evidence(retirement_evidence):
                for attempt in retirement_evidence["attempts"]:
                    retired_requests.setdefault(attempt["executionArn"], set()).add(attempt["requestId"])
            findings.extend(validate_evidence(final_evidence, retired_requests=retired_requests))
        else:
            findings.extend(validator(copy.deepcopy(final_evidence), phase="final"))
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError, RecursionError):
        findings.append("INCOMPLETE: malformed captured evidence")
    return observation_findings, list(dict.fromkeys(findings)), final_evidence


def evaluate_evidence(
    evidence: Mapping[str, Any],
    *,
    retirement_evidence: Mapping[str, Any],
    cleanup_evidence: Mapping[str, Sequence[Mapping[str, Any]]],
    validator: EvidenceValidator,
    standard: bool,
    observation_evidence: Sequence[Mapping[str, Any]] = (),
    attempts: Sequence[Mapping[str, Any]] = (),
) -> list[str]:
    """Evaluate captured evidence without querying or mutating the capture."""
    _, findings, _ = _evaluate_capture(
        evidence,
        retirement_evidence=retirement_evidence,
        cleanup_evidence=cleanup_evidence,
        observation_evidence=observation_evidence,
        attempts=attempts,
        validator=validator,
        standard=standard,
    )
    return findings


def frozen_journal_errors(evidence: Mapping[str, Any], journal: Sequence[Mapping[str, Any]]) -> list[str]:
    """Bind frozen requests to the journal, allowing later service discoveries."""
    attempts = evidence.get("attempts")
    if not isinstance(attempts, list) or not all(isinstance(row, Mapping) for row in attempts):
        return ["INCOMPLETE: observed attempts are missing or malformed"]
    recorded: dict[str, Mapping[str, Any]] = {}
    observed: set[str] = set()
    for row in journal:
        request = row.get("requestId")
        if not isinstance(request, str) or not request.strip() or request in recorded:
            return ["INCOMPLETE: journaled requests must have unique native identities"]
        recorded[request] = row
    for row in attempts:
        request = row.get("requestId")
        if not isinstance(request, str) or request in observed or request not in recorded:
            return ["INCOMPLETE: observed requests do not match journaled attempts"]
        observed.add(request)
        launch = recorded[request]
        if (
            not is_json_integer(launch.get("attempt"))
            or not is_json_integer(row.get("attempt"))
            or launch["attempt"] != row["attempt"]
            or launch.get("role") != row.get("role")
            or ("executionArn" in launch and launch["executionArn"] != row.get("executionArn"))
        ):
            return [f"INCOMPLETE: observed identity does not match journaled request {request}"]
    if not observed or any(
        request not in observed and "discoveredAtMs" not in row for request, row in recorded.items()
    ):
        return ["INCOMPLETE: frozen observations do not cover all client-journaled launches"]
    return []


def adjudicate_evidence(
    status: str,
    errors: list[str],
    evidence: Mapping[str, Any],
    *,
    retirement_evidence: Mapping[str, Any],
    cleanup_evidence: Mapping[str, Sequence[Mapping[str, Any]]],
    validator: EvidenceValidator,
    standard: bool,
    observation_evidence: Sequence[Mapping[str, Any]] = (),
    attempts: Sequence[Mapping[str, Any]] = (),
    trusted_observations: bool = False,
    observation_findings: Sequence[str] = (),
) -> tuple[str, list[str], dict[str, Any]]:
    """Resolve a capture status with the shared offline verdict rules."""
    observed, findings, projected = _evaluate_capture(
        evidence,
        retirement_evidence=retirement_evidence,
        cleanup_evidence=cleanup_evidence,
        observation_evidence=observation_evidence,
        attempts=attempts,
        validator=validator,
        standard=standard,
    )
    observed.extend(observation_findings)
    findings.extend(observation_findings)
    combined = list(dict.fromkeys([*errors, *findings]))
    if (
        status == "INFRASTRUCTURE_ERROR"
        and trusted_observations
        and any(not item.startswith("INCOMPLETE:") for item in observed)
    ):
        status = "FAILED"
    elif status in ("COLLECTED", "PASSED"):
        if errors:
            status = "INCOMPLETE_EVIDENCE"
        elif any(not item.startswith("INCOMPLETE:") for item in findings):
            status = "FAILED"
        elif findings:
            status = "INCOMPLETE_EVIDENCE"
        else:
            status = "PASSED"
    return status, combined, projected
