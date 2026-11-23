# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Lifecycle 5: suspension, replay and retry."""

from __future__ import annotations

import math
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from itertools import pairwise
from typing import TYPE_CHECKING, Any

from aws_durable_execution_conformance_tests.lmi.rules.common import (
    original_admission_conflicts,
)
from aws_durable_execution_conformance_tests.lmi.rules.retention import (
    _OPERATION_TERMINAL_STATUSES,
    _native_intervals,
    _retain_observations,
)
from aws_durable_execution_conformance_tests.lmi.rules.values import (
    COMPLETED_OPERATION_STATUSES,
    is_json_integer,
    mappings,
    same_checkpoint,
    same_checkpoint_set,
    same_json,
    timestamp,
)
from aws_durable_execution_conformance_tests.lmi.rules.view import CaseView

if TYPE_CHECKING:
    from aws_durable_execution_conformance_tests.lmi.rules.reconcile import ReconcileView


def replay(view: CaseView) -> None:
    if not view.require_roles("victim", "resume"):
        return
    victim, resume = view.selected["victim"], view.selected["resume"]
    pending = view.event(victim, "pending")
    wrapper = view.event(victim, "wrapper_return")
    if victim.get("outcome") != "PENDING" or victim.get("executionArn") != resume.get("executionArn"):
        view.error("resume must continue the original suspended execution", "ASSERTION")
    if not pending.get("atMs", 0) <= wrapper.get("atMs", 0) < resume.get("startedAtMs", 0):
        view.error("resume did not follow a real PENDING wrapper return", "ASSERTION")
    replay = view.mapping(view.value.get("replay"), "replay")
    if replay.get("durableWaitObserved") is False:
        view.error("replay did not verify a durable wait across suspension and resume", "ASSERTION")
    if replay.get("suspensionCheckpointMatches") is False:
        view.error("replay did not verify the checkpoint snapshot of a public suspension resume", "ASSERTION")
    before = view.records(replay.get("before"), "replay.before", nonempty=True)
    after = view.records(replay.get("after"), "replay.after", nonempty=True)
    after_by_id = {operation.get("id"): operation for operation in after}
    for error in replay.get("projectionErrors", []):
        view.error(f"replay service projection: {error}")
    if "expectedBefore" in replay:
        expected_before = view.records(replay["expectedBefore"], "replay.expectedBefore")
        expected_by_id = {operation.get("id"): operation for operation in expected_before}
        observed_by_id = {operation.get("id"): operation for operation in before}
        if len(expected_by_id) != len(expected_before):
            view.error("replay.expectedBefore contains duplicate operation IDs")
        if not same_checkpoint_set(expected_by_id, observed_by_id):
            view.error("replay checkpoint snapshot differs from the complete pre-resume service history", "ASSERTION")
    same_execution = [
        attempt for attempt in view.attempts.values() if attempt.get("executionArn") == victim.get("executionArn")
    ]
    has_earlier, tied = original_admission_conflicts(victim, same_execution)
    if has_earlier:
        view.error("replay victim is not the original invocation of the selected execution", "PLACEMENT")
    if tied:
        view.error("tied native admissions cannot establish the original replay invocation")
    if not completed_suspension_checkpoint(same_execution, view.events, view.value["sdkRequests"], after):
        view.error("replay needs an observed wait or callback checkpoint followed by suspension and resume")
    if any(not same_checkpoint(after_by_id.get(operation.get("id")), operation) for operation in before):
        view.error("replay changed completed operation identity, result, or failure", "ASSERTION")
    ids: set[str] = set()
    for operation in before:
        operation_id = operation.get("id")
        if operation_id in ids:
            view.error("replay.before repeats an operation identity")
        if isinstance(operation_id, str):
            ids.add(operation_id)
    final_history = next(
        history for history in view.value["histories"] if history["executionArn"] == victim["executionArn"]
    )
    recorded = final_history.get("operations")
    if (
        not isinstance(recorded, list)
        or len(recorded) != len(after)
        or not all(
            same_checkpoint(left, right, include_timing=True) for left, right in zip(recorded, after, strict=True)
        )
    ):
        view.error("replay.after differs from the collected service history", "ASSERTION")
    statuses = {operation.get("status") for operation in before}
    if not {"SUCCEEDED", "FAILED"} <= statuses:
        view.error("replay must preserve both a completed success and a completed failure")
    completed = {operation.get("id") for operation in before}
    omitted, late = replay_checkpoint_timing(before, after, pending.get("atMs", 0))
    if omitted:
        view.error("replay.before omits an operation known to have completed before suspension")
    # Operations completed before the first resume stay completed for every
    # later invocation of the same execution, and a body without external
    # effects is invisible to the side-effect counts. So no request after
    # the victim's on this execution may run them, not only the selected
    # first resume.
    later = {resume.get("requestId")} | {
        request_id
        for request_id, attempt in view.attempts.items()
        if attempt.get("executionArn") == victim.get("executionArn") and request_id != victim.get("requestId")
    }
    executions = view.records(replay.get("operationExecutions"), "replay.operationExecutions", nonempty=True)
    for execution in executions:
        request_id = view.string(execution.get("requestId"), "replay.operationExecutions.requestId")
        operation_id = view.string(execution.get("operationId"), "replay.operationExecutions.operationId")
        if request_id not in view.attempts:
            view.error("operationExecutions references an unrecorded request")
        if request_id in later and operation_id in completed:
            view.error("resume repeated completed user code", "ASSERTION")
    if any(
        event.get("kind") == "body"
        and event.get("operationId") in completed
        and (
            event.get("requestId") in later
            or (
                event.get("requestId") == victim.get("requestId")
                and "atMs" in pending
                and view.at_or_after(event, pending, require_sequence=True) is True
            )
        )
        for event in view.events
    ):
        view.error("resume repeated completed user code", "ASSERTION")
    if late:
        view.error("replay.before contains an operation that completed after suspension", "ASSERTION")
    completed_steps = [
        operation
        for operation in before
        if operation.get("type") == "STEP" and operation.get("status") in COMPLETED_OPERATION_STATUSES
    ]
    if not completed_steps:
        view.error("replay requires a completed user-code step before suspension")
    for operation in completed_steps:
        operation_id = operation.get("id")
        timed_bodies = [
            event
            for event in view.events
            if event.get("kind") == "body"
            and event.get("requestId") == victim.get("requestId")
            and event.get("operationId") == operation_id
        ]
        if not completed_replay_step_witness(
            operation_id,
            victim.get("requestId"),
            executions,
            timed_bodies,
            lambda event: "atMs" in pending and view.at_or_after(event, pending, require_sequence=True) is False,
            operation.get("endedAtMs"),
        ):
            view.error(
                "completed replay step lacks observed user-code execution on the original request before suspension"
            )
        if "endedAtMs" in operation and any(event["atMs"] > operation["endedAtMs"] for event in timed_bodies):
            view.error("completed replay step body occurred after recorded completion", "ASSERTION")
    missing = replay.get("missingExpectedEffects", [])
    for name in missing:
        view.error(f"replay omitted the expected side-effect operation {name}", "ASSERTION")
    retry_id = None
    if "retry" in replay:
        retry = view.mapping(replay.get("retry"), "replay.retry")
        retry_id = view.string(retry.get("operationId"), "replay.retry.operationId")
        retry_attempts = view.records(retry.get("attempts"), "replay.retry.attempts", nonempty=True)
        retry_attempt_ids = [view.string(item.get("attemptId"), "retry.attemptId") for item in retry_attempts]
        if (len(retry_attempt_ids) < 2 and "retry" not in missing) or len(set(retry_attempt_ids)) != len(
            retry_attempt_ids
        ):
            view.error("retry requires distinct observed attempts")
        if len(retry_attempts) >= 2 and (
            retry_attempts[0].get("outcome") != "FAILED" or retry_attempts[-1].get("outcome") != "SUCCEEDED"
        ):
            view.error("retry must demonstrate a failed attempt followed by success", "ASSERTION")
        for retry_attempt in retry_attempts:
            view.string(retry_attempt.get("outcome"), "retry.outcome")
            if retry_attempt.get("requestId") not in view.attempts:
                view.error("retry references an unrecorded request")
            elif view.attempts[retry_attempt["requestId"]].get("executionArn") != victim.get("executionArn"):
                view.error("retry attempt did not run on the suspended execution", "ASSERTION")
            if not any(
                item.get("operationId") == retry_id
                and item.get("attemptId") == retry_attempt.get("attemptId")
                and item.get("requestId") == retry_attempt.get("requestId")
                for item in executions
            ):
                view.error("retry attempt lacks observed user-code execution")
        observed_retry_bodies = [
            (item.get("requestId"), item.get("attemptId"))
            for item in executions
            if item.get("operationId") == retry_id
            and item.get("requestId") in view.attempts
            and view.attempts[item["requestId"]].get("executionArn") == victim.get("executionArn")
        ]
        declared_retry_bodies = [(item.get("requestId"), item.get("attemptId")) for item in retry_attempts]
        if len(observed_retry_bodies) != len(declared_retry_bodies) or any(
            observed_retry_bodies.count(body) != 1 for body in declared_retry_bodies
        ):
            view.error("retry body executions must match the declared retry attempts exactly")
        timed_retries: dict[tuple[str, str], Mapping[str, Any]] = {}
        for event in view.events:
            retry_request_id = event.get("requestId")
            if (
                event.get("kind") != "body"
                or event.get("operationId") != retry_id
                or not isinstance(retry_request_id, str)
                or view.attempts[retry_request_id].get("executionArn") != victim.get("executionArn")
            ):
                continue
            identity = retry_body_identity(event)
            if identity is None:
                view.error("observed retry body does not match a declared attempt")
                continue
            if identity not in declared_retry_bodies:
                view.error("observed retry body does not match a declared attempt")
            elif identity in timed_retries:
                view.error("retry attempt has multiple timed user-code entries")
            else:
                timed_retries[identity] = event
        timed_retry_bodies = [
            timed_retries[identity] for identity in declared_retry_bodies if identity in timed_retries
        ]
        last_by_request: dict[str, Mapping[str, Any]] = {}
        invalid_order = any(
            earlier["atMs"] > later_body["atMs"] for earlier, later_body in pairwise(timed_retry_bodies)
        )
        for body in timed_retry_bodies:
            request_id = body["requestId"]
            earlier = last_by_request.get(request_id)
            if earlier is not None and view.at_or_after(body, earlier, require_sequence=True) is False:
                invalid_order = True
            last_by_request[request_id] = body
        if invalid_order:
            view.error("retry attempt order contradicts timed user-code entries", "ASSERTION")
        latest_admission = 0
        for declared_request, declared_attempt in declared_retry_bodies:
            if not isinstance(declared_request, str) or not isinstance(declared_attempt, str):
                continue
            owner = view.attempts.get(declared_request)
            if owner is not None:
                latest_admission = max(latest_admission, owner["startedAtMs"])
            timed_body = timed_retries.get((declared_request, declared_attempt))
            if timed_body is not None and timed_body["atMs"] < latest_admission:
                view.error("retry body precedes a prior attempt's admission", "ASSERTION")
        # replay.after equals the collected final history (checked above), so a
        # retry that never checkpointed cannot satisfy the retry semantics.
        retried = next((operation for operation in after if operation.get("id") == retry_id), None)
        if retried is None or retried.get("status") != "SUCCEEDED":
            view.error("the retried operation must succeed in the collected final history", "ASSERTION")
        elif "result" not in retried:
            view.error("the retried operation must preserve its stored result", "INCOMPLETE")
        if (
            retried is not None
            and "endedAtMs" in retried
            and any(body["atMs"] > retried["endedAtMs"] for body in timed_retry_bodies)
        ):
            view.error("timed retry body occurred after the recorded operation completion", "ASSERTION")
        if (
            retried is not None
            and "endedAtMs" in retried
            and any(
                item.get("requestId") in view.attempts
                and view.attempts[item["requestId"]]["startedAtMs"] > retried["endedAtMs"]
                for item in retry_attempts
            )
        ):
            view.error("retry owner was admitted after the recorded operation completion", "ASSERTION")
        if retry_id in completed:
            view.error("retry must not repeat an already completed operation", "ASSERTION")
    elif "retry" not in missing:
        view.error("replay.retry must be an object")
    counts = view.records(replay.get("sideEffects"), "replay.sideEffects", nonempty=True)
    effect_ids = {view.string(item.get("operationId"), "replay.sideEffects.operationId") for item in counts}
    if len(effect_ids) != len(counts):
        view.error("side-effect counts must be unique per operation")
    if not completed | ({retry_id} if retry_id is not None else set()) <= effect_ids:
        view.error("side-effect counts must cover stored successes, failures, and retries")
    for count in counts:
        actual = count.get("count")
        expected = count.get("expectedCount")
        if not is_json_integer(actual) or not is_json_integer(expected) or actual < 0 or expected < 0:
            view.error("side-effect counts must be non-negative integers")
        elif actual != expected:
            view.error("side-effect count differs from the fixture's retry policy", "ASSERTION")


def completed_suspension_checkpoint(
    attempts: Sequence[Mapping[str, Any]],
    events: Sequence[Mapping[str, Any]],
    requests: Sequence[Mapping[str, Any]],
    checkpoints: Sequence[Mapping[str, Any]],
) -> bool:
    by_request = {attempt["requestId"]: attempt for attempt in attempts}
    by_id = {operation.get("id"): operation for operation in checkpoints}
    suspensions = {
        event["requestId"]: event["atMs"]
        for event in events
        if event.get("kind") == "wrapper_return"
        and event.get("requestId") in by_request
        and by_request[event["requestId"]].get("outcome") == "PENDING"
        and type(event.get("atMs")) in (int, float)
    }
    for request in requests:
        request_id, at = request.get("requestId"), request.get("atMs")
        if (
            not isinstance(request_id, str)
            or request_id not in suspensions
            or request.get("method") != "CheckpointDurableExecution"
            or request.get("outcome") != "DISPATCHED"
            or type(at) not in (int, float)
            or not by_request[request_id]["startedAtMs"] <= at <= suspensions[request_id]
            or not isinstance(request.get("updates"), list)
        ):
            continue
        for update in request["updates"]:
            if (
                not isinstance(update, Mapping)
                or update.get("type") not in ("WAIT", "CALLBACK")
                or update.get("action") != "START"
                or not isinstance(update.get("id"), str)
            ):
                continue
            operation = by_id.get(update["id"], {})
            ended = operation.get("endedAtMs")
            if (
                operation.get("type") == update["type"]
                and operation.get("status") == "SUCCEEDED"
                and type(ended) in (int, float)
                and suspensions[request_id] < ended
                and any(later["startedAtMs"] >= ended for later in attempts)
            ):
                return True
    return False


def replay_checkpoint_timing(
    before: Sequence[Mapping[str, Any]],
    after: Sequence[Mapping[str, Any]],
    pending_at: int | float,
    observed_ends: Mapping[str, set[int | float]] | None = None,
) -> tuple[bool, bool]:
    """Return whether a completed checkpoint is omitted or ends after suspension."""
    before_ids = {operation.get("id") for operation in before}

    def times(operation: Mapping[str, Any]) -> set[int | float]:
        operation_id = operation.get("id")
        ends = (
            observed_ends.get(operation_id, set())
            if observed_ends is not None and isinstance(operation_id, str)
            else set()
        )
        end = operation.get("endedAtMs")
        return {*ends, end} if isinstance(end, (int, float)) and not isinstance(end, bool) else ends

    omitted = any(
        operation.get("status") in COMPLETED_OPERATION_STATUSES
        and operation.get("id") not in before_ids
        and any(end <= pending_at for end in times(operation))
        for operation in after
    )
    late = any(
        operation.get("type") == "STEP" and any(end > pending_at for end in times(operation)) for operation in before
    )
    return omitted, late


def completed_replay_step_witness(
    operation_id: Any,
    request_id: Any,
    entries: Sequence[Mapping[str, Any]],
    bodies: Sequence[Mapping[str, Any]],
    before_suspension: Callable[[Mapping[str, Any]], bool],
    ended_at: int | float | None,
) -> bool:
    return any(
        row.get("requestId") == request_id and row.get("operationId") == operation_id for row in entries
    ) and any(
        row.get("kind") == "body"
        and row.get("requestId") == request_id
        and row.get("operationId") == operation_id
        and before_suspension(row)
        and (ended_at is None or row["atMs"] <= ended_at)
        for row in bodies
    )


def retry_body_identity(row: Mapping[str, Any]) -> tuple[str, str] | None:
    request_id, attempt_id = row.get("requestId"), row.get("attemptId")
    if not isinstance(request_id, str) or not request_id.strip():
        return None
    if attempt_id is None and row.get("kind") == "body":
        attempt_number = row.get("attempt")
        if is_json_integer(attempt_number) and attempt_number >= 1:
            attempt_id = f"{request_id}:{int(attempt_number)}"
    if isinstance(attempt_id, str) and attempt_id.strip():
        return request_id, attempt_id
    return None


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
            findings.append("ASSERTION: cleanup retry observations must be an object")
            continue
        same_operation = "operationId" not in retry or same_json(retry["operationId"], operation_id)
        if not same_operation:
            findings.append("ASSERTION: cleanup retry operationId changed the frozen retry operation")
        attempts = retry.get("attempts", [])
        if not isinstance(attempts, list):
            findings.append("ASSERTION: cleanup retry attempts must be an array")
            continue
        order = []
        for attempt in attempts:
            if not isinstance(attempt, Mapping):
                findings.append("ASSERTION: cleanup retry attempt must be an object")
                continue
            if "requestId" in attempt:
                bound_execution(attempt["requestId"])
            if not same_operation:
                continue
            attempt_id = attempt.get("attemptId")
            if not isinstance(attempt_id, str) or not attempt_id.strip():
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
                    findings.append(f"ASSERTION: cleanup replay.{view} checkpoints must be an array")
                    continue
                seen: set[str] = set()
                for operation in operations:
                    operation_id = operation.get("id") if isinstance(operation, Mapping) else None
                    if not isinstance(operation_id, str) or not operation_id.strip():
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
        findings.append(f"ASSERTION: cleanup before checkpoint {operation_id} lacks an after-checkpoint correlation")
    for operation_id in after_ids - history_ids:
        findings.append(f"ASSERTION: cleanup after checkpoint {operation_id} lacks a service-history correlation")
    return findings, before_ids


def reconcile_replay(view: ReconcileView) -> None:
    """Check later replay, checkpoint, side-effect and retry observations."""
    replay = view.original.get("replay")
    replay = replay if isinstance(replay, Mapping) else {}
    completed = {
        operation["id"]
        for operation in mappings(replay.get("before"))
        if isinstance(operation.get("id"), str) and operation.get("status") in _OPERATION_TERMINAL_STATUSES
    }
    expected_counts = {
        item["operationId"]: item["expectedCount"]
        for item in mappings(replay.get("sideEffects"))
        if isinstance(item.get("operationId"), str) and is_json_integer(item.get("expectedCount"))
    }
    victim = view.role("victim")
    checkpoint_errors, observed_before = _cleanup_checkpoint_errors(
        replay,
        view.sources,
        victim.get("executionArn"),
        observation_end=view.window_end,
    )
    view.findings.extend(checkpoint_errors)
    completed.update(observed_before)
    required_counts = set(completed)
    retry = replay.get("retry")
    if isinstance(retry, Mapping) and isinstance(retry.get("operationId"), str):
        required_counts.add(retry["operationId"])
    covered_counts: set[str] = set()
    for source in view.sources:
        fragment = source.get("replay")
        if not isinstance(fragment, Mapping):
            continue
        for count in mappings(fragment.get("sideEffects")):
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
    for missing in sorted(required_counts - (expected_counts.keys() & covered_counts)):
        view.findings.append(
            f"INCOMPLETE: cleanup replay completed operation lacks side-effect count coverage for {missing}"
        )
    returned = view.boundary("victim", "wrapper_return")
    resume_id = view.role("resume").get("requestId")
    checkpoint_ends = _observed_checkpoint_ends(view.sources, victim.get("executionArn"))
    native_intervals = _native_intervals(view.sources)
    native_starts = {
        request: bounds["startedAtMs"][0] for request, bounds in native_intervals.items() if "startedAtMs" in bounds
    }
    resume_start = native_intervals.get(resume_id, {}).get("startedAtMs") if isinstance(resume_id, str) else None
    if resume_start is not None and returned is not None and resume_start[0] <= returned:
        view.findings.append("ASSERTION: cleanup evidence places resume before the frozen PENDING wrapper return")
    reconciled: dict[str, dict[str, Any]] = {}
    for source in view.sources:
        for attempt in mappings(source.get("attempts")):
            request = attempt.get("requestId")
            if (
                not isinstance(request, str)
                or request not in native_starts
                or view.executions_by_request.get(request, set()) != {victim.get("executionArn")}
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
            for attempt in view.attempts.values()
            if attempt.get("executionArn") == victim.get("executionArn")
            and timestamp(attempt.get("startedAtMs")) is not None
        ],
        view.events,
        mappings(view.original.get("sdkRequests")),
        mappings(replay.get("after")),
    )
    reconciled_after = [
        {**operation, "endedAtMs": max(times)}
        if isinstance(operation_id := operation.get("id"), str) and (times := checkpoint_ends.get(operation_id, set()))
        else operation
        for operation in mappings(replay.get("after"))
    ]
    if previously_witnessed and not completed_suspension_checkpoint(
        list(reconciled.values()),
        [event for source in view.sources for event in mappings(source.get("events"))],
        [request for source in view.sources for request in mappings(source.get("sdkRequests"))],
        reconciled_after,
    ):
        view.findings.append("ASSERTION: cleanup resume does not follow the observed wait or callback completion")
    observed_pending = [
        event for event in view.observed_events("victim", "pending") if timestamp(event.get("atMs")) is not None
    ]
    pending_times = {at for event in observed_pending if (at := timestamp(event.get("atMs"))) is not None}
    if pending_times:
        before_operations = []
        after_operations = []
        for source in view.sources:
            replay_fragment = source.get("replay")
            if isinstance(replay_fragment, Mapping):
                before_operations.extend(mappings(replay_fragment.get("before")))
                after_operations.extend(mappings(replay_fragment.get("after")))
            for history in mappings(source.get("histories")):
                if history.get("executionArn") == victim.get("executionArn"):
                    after_operations.extend(mappings(history.get("operations")))
        omitted, _ = replay_checkpoint_timing(before_operations, after_operations, max(pending_times), checkpoint_ends)
        _, late = replay_checkpoint_timing(before_operations, after_operations, min(pending_times), checkpoint_ends)
        if omitted:
            view.findings.append(
                "INCOMPLETE: cleanup replay.before omits an operation known to have completed before suspension"
            )
        if late:
            view.findings.append(
                "ASSERTION: cleanup replay.before contains an operation that completed after suspension"
            )
        frozen_ids = {
            operation["id"] for operation in mappings(replay.get("before")) if isinstance(operation.get("id"), str)
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
            for source in view.sources
            for event in mappings(source.get("events"))
            if timestamp(event.get("atMs")) is not None
        ]
        execution_entries = [
            entry
            for source in view.sources
            if isinstance(fragment := source.get("replay"), Mapping)
            for entry in mappings(fragment.get("operationExecutions"))
        ]
        for operation_id in newly_completed_steps:
            step_ends = checkpoint_ends.get(operation_id, set())
            if not completed_replay_step_witness(
                operation_id,
                victim.get("requestId"),
                execution_entries,
                body_observations,
                lambda event: all(
                    view.at_or_after(event, pending, require_sequence=True) is False for pending in observed_pending
                ),
                min(step_ends) if step_ends else None,
            ):
                view.findings.append(
                    "INCOMPLETE: cleanup completed replay step lacks observed user-code execution "
                    "on the original request before suspension"
                )
    for source in view.sources:
        for event in mappings(source.get("events")):
            if (
                event.get("kind") == "body"
                and isinstance(operation_id := event.get("operationId"), str)
                and operation_id in completed
            ):
                request_id = event.get("requestId")
                execution_arn = view.bound_execution(request_id)
                if (
                    execution_arn is not None
                    and execution_arn == victim.get("executionArn")
                    and (
                        request_id != victim.get("requestId")
                        or any(
                            view.at_or_after(event, pending, require_sequence=True) is True
                            for pending in observed_pending
                        )
                    )
                ):
                    view.findings.append(
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
                    view.findings.append("ASSERTION: cleanup completed replay body occurred after recorded completion")
            if event.get("kind") != "pending" or event.get("requestId") != victim.get("requestId"):
                continue
            request_id = event.get("requestId")
            execution_arn = view.bound_execution(request_id)
            if execution_arn is None:
                continue
            if any(
                key in event and event[key] != expected
                for key, expected in (("ownerRequestId", request_id), ("executionArn", execution_arn))
            ):
                view.findings.append("INCOMPLETE: cleanup PENDING event has inconsistent native ownership")
                continue
            if "atMs" not in event:
                continue  # A partial event may gain its timestamp during collection.
            pending_at = timestamp(event["atMs"])
            if pending_at is None:
                view.findings.append("INCOMPLETE: cleanup PENDING event has an invalid timestamp")
            elif returned is not None and pending_at > returned:
                view.findings.append("ASSERTION: cleanup PENDING event follows the frozen wrapper return")
        if "replay" not in source:
            continue
        observed_replay = source["replay"]
        if not isinstance(observed_replay, Mapping):
            view.findings.append("INCOMPLETE: cleanup replay observations must be an object")
            continue
        view.findings.extend(cleanup_replay_history_errors(replay, observed_replay))
        entries = observed_replay.get("operationExecutions", [])
        if not isinstance(entries, list):
            view.findings.append("INCOMPLETE: cleanup operation executions must be an array")
            entries = []
        for entry in entries:
            if not isinstance(entry, Mapping):
                view.findings.append("INCOMPLETE: cleanup operation execution must be an object")
                continue
            request_id, operation_id = entry.get("requestId"), entry.get("operationId")
            execution_arn = view.bound_execution(request_id)
            if not isinstance(operation_id, str) or not operation_id.strip():
                view.findings.append("INCOMPLETE: cleanup operation execution lacks an operationId")
                continue
            if (
                execution_arn is not None
                and execution_arn == victim.get("executionArn")
                and request_id != victim.get("requestId")
                and operation_id in completed
            ):
                view.findings.append(
                    f"ASSERTION: cleanup evidence reports repeated completed user code for {operation_id}"
                )
        view.findings.extend(_side_effect_observation_errors(source))
        counts = observed_replay.get("sideEffects", [])
        if not isinstance(counts, list):
            view.findings.append("INCOMPLETE: cleanup side-effect observations must be an array")
            counts = []
        for count in counts:
            if not isinstance(count, Mapping):
                view.findings.append("INCOMPLETE: cleanup side-effect observation must be an object")
                continue
            operation_id = count.get("operationId")
            if not isinstance(operation_id, str) or not operation_id.strip():
                view.findings.append("INCOMPLETE: cleanup side-effect observation lacks an operationId")
                continue
            view.findings.extend(cleanup_replay_count_errors(count, expected_counts.get(operation_id)))
    view.findings.extend(
        _cleanup_retry_errors(
            replay,
            view.sources,
            victim.get("executionArn"),
            view.bound_execution,
            lambda observation, boundary_event: view.at_or_after(observation, boundary_event, require_sequence=True),
        )
    )


def cleanup_replay_history_errors(frozen: Mapping[str, Any], observed: Mapping[str, Any]) -> list[str]:
    findings = []
    expected, before = observed.get("expectedBefore"), frozen.get("before")
    if isinstance(expected, list) and isinstance(before, list):
        expected_ids = {
            row.get("id"): row for row in expected if isinstance(row, Mapping) and isinstance(row.get("id"), str)
        }
        frozen_ids = {
            row.get("id"): row for row in before if isinstance(row, Mapping) and isinstance(row.get("id"), str)
        }
        if len(expected_ids) != len(expected):
            findings.append("INCOMPLETE: cleanup pre-resume service checkpoints have duplicate or malformed IDs")
        elif not same_checkpoint_set(frozen_ids, expected_ids):
            findings.append("ASSERTION: cleanup service history contradicts the frozen replay checkpoint snapshot")
    if observed.get("durableWaitObserved") is False:
        findings.append("ASSERTION: cleanup service history refutes the durable wait suspension")
    if observed.get("suspensionCheckpointMatches") is False:
        findings.append("ASSERTION: cleanup service history refutes the public suspension replay checkpoint snapshot")
    return findings


def cleanup_replay_count_errors(count: Mapping[str, Any], expected: int | float | None) -> list[str]:
    identity = count["operationId"]
    if expected is None:
        reported, observed = count.get("expectedCount"), count.get("count")
        if is_json_integer(reported) and reported >= 0 and is_json_integer(observed) and observed > reported:
            return [f"ASSERTION: cleanup side-effect count for {identity} exceeds the fixture oracle"]
        return []
    errors = []
    if "expectedCount" in count and not same_json(count["expectedCount"], expected):
        errors.append(f"ASSERTION: cleanup side-effect expectation for {identity} changed the frozen fixture oracle")
    if is_json_integer(count.get("count")) and count["count"] != expected:
        errors.append(f"ASSERTION: cleanup side-effect count for {identity} differs from the frozen fixture oracle")
    return errors


def cleanup_replay_projection_errors(replay: Mapping[str, Any]) -> list[str]:
    if "projectionErrors" not in replay:
        return []
    raw = replay["projectionErrors"]
    if not isinstance(raw, list) or any(not isinstance(error, str) or not error.strip() for error in raw):
        return ["INCOMPLETE: cleanup replay projection errors are malformed"]
    return [f"INCOMPLETE: cleanup replay projection: {error}" for error in raw]
