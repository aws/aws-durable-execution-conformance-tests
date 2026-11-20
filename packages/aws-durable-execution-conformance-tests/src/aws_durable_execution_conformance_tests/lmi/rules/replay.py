# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Lifecycle 5: suspension, replay and retry."""

from __future__ import annotations

from collections.abc import Mapping
from itertools import pairwise
from typing import Any

from aws_durable_execution_conformance_tests.lmi.rules.values import (
    COMPLETED_OPERATION_STATUSES,
    is_json_integer,
    same_json,
)
from aws_durable_execution_conformance_tests.lmi.rules.view import CaseView


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
    before = view.records(replay.get("before"), "replay.before", nonempty=True)
    after = view.records(replay.get("after"), "replay.after", nonempty=True)
    after_by_id = {operation.get("id"): operation for operation in after}
    same_execution = [
        attempt for attempt in view.attempts.values() if attempt.get("executionArn") == victim.get("executionArn")
    ]
    if any(attempt["startedAtMs"] < victim["startedAtMs"] for attempt in same_execution):
        view.error("replay victim is not the original invocation of the selected execution", "PLACEMENT")
    if any(attempt is not victim and attempt["startedAtMs"] == victim["startedAtMs"] for attempt in same_execution):
        view.error("tied native admissions cannot establish the original replay invocation")
    pending_requests = {attempt["requestId"] for attempt in same_execution if attempt.get("outcome") == "PENDING"}
    suspending_requests = {
        event["requestId"]: event["atMs"]
        for event in view.events
        if event.get("kind") == "wrapper_return"
        and event.get("requestId") in pending_requests
        and any(later["startedAtMs"] > event["atMs"] for later in same_execution)
    }
    if not any(
        request.get("requestId") in suspending_requests
        and request.get("method") == "CheckpointDurableExecution"
        and request.get("outcome") == "DISPATCHED"
        and view.attempts[request["requestId"]]["startedAtMs"]
        <= request["atMs"]
        <= suspending_requests[request["requestId"]]
        and isinstance(request.get("updates"), list)
        and any(
            isinstance(update, Mapping)
            and update.get("type") in ("WAIT", "CALLBACK")
            and update.get("action") == "START"
            and isinstance(update.get("id"), str)
            and after_by_id.get(update["id"], {}).get("type") == update["type"]
            and after_by_id[update["id"]].get("status") == "SUCCEEDED"
            and "endedAtMs" in after_by_id[update["id"]]
            and suspending_requests[request["requestId"]] < after_by_id[update["id"]]["endedAtMs"]
            and any(later["startedAtMs"] >= after_by_id[update["id"]]["endedAtMs"] for later in same_execution)
            for update in request["updates"]
        )
        for request in view.value["sdkRequests"]
    ):
        view.error("replay needs an observed wait or callback checkpoint followed by suspension and resume")
    if any(not same_json(after_by_id.get(operation.get("id")), operation) for operation in before):
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
    if not same_json(final_history.get("operations"), after):
        view.error("replay.after differs from the collected service history", "ASSERTION")
    statuses = {operation.get("status") for operation in before}
    if not {"SUCCEEDED", "FAILED"} <= statuses:
        view.error("replay must preserve both a completed success and a completed failure")
    completed = {operation.get("id") for operation in before}
    if any(
        operation.get("status") in COMPLETED_OPERATION_STATUSES
        and "endedAtMs" in operation
        and operation["endedAtMs"] <= pending.get("atMs", 0)
        and operation.get("id") not in completed
        for operation in after
    ):
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
    if any(
        operation.get("type") == "STEP" and operation.get("endedAtMs", 0) > pending.get("atMs", 0)
        for operation in before
    ):
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
        has_entry = any(
            item.get("requestId") == victim.get("requestId") and item.get("operationId") == operation_id
            for item in executions
        )
        timed_bodies = [
            event
            for event in view.events
            if event.get("kind") == "body"
            and event.get("requestId") == victim.get("requestId")
            and event.get("operationId") == operation_id
        ]
        if not has_entry or not any(
            "atMs" in pending
            and view.at_or_after(event, pending, require_sequence=True) is False
            and event["atMs"] <= operation.get("endedAtMs", event["atMs"])
            for event in timed_bodies
        ):
            view.error(
                "completed replay step lacks observed user-code execution on the original request before suspension"
            )
        if "endedAtMs" in operation and any(event["atMs"] > operation["endedAtMs"] for event in timed_bodies):
            view.error("completed replay step body occurred after recorded completion", "ASSERTION")
    retry = view.mapping(replay.get("retry"), "replay.retry")
    retry_id = view.string(retry.get("operationId"), "replay.retry.operationId")
    retry_attempts = view.records(retry.get("attempts"), "replay.retry.attempts", nonempty=True)
    retry_attempt_ids = [view.string(item.get("attemptId"), "retry.attemptId") for item in retry_attempts]
    if len(retry_attempt_ids) < 2 or len(set(retry_attempt_ids)) != len(retry_attempt_ids):
        view.error("retry requires distinct observed attempts")
    if retry_attempts and (
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
        attempt_id = event.get("attemptId")
        if attempt_id is None:
            attempt_number = event.get("attempt")
            if is_json_integer(attempt_number) and attempt_number >= 1:
                attempt_id = f"{retry_request_id}:{int(attempt_number)}"
        if not isinstance(attempt_id, str) or not attempt_id.strip():
            view.error("observed retry body does not match a declared attempt")
            continue
        identity = (retry_request_id, attempt_id)
        if identity not in declared_retry_bodies:
            view.error("observed retry body does not match a declared attempt")
        elif identity in timed_retries:
            view.error("retry attempt has multiple timed user-code entries")
        else:
            timed_retries[identity] = event
    timed_retry_bodies = [timed_retries[identity] for identity in declared_retry_bodies if identity in timed_retries]
    last_by_request: dict[str, Mapping[str, Any]] = {}
    invalid_order = any(earlier["atMs"] > later_body["atMs"] for earlier, later_body in pairwise(timed_retry_bodies))
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
    counts = view.records(replay.get("sideEffects"), "replay.sideEffects", nonempty=True)
    effect_ids = {view.string(item.get("operationId"), "replay.sideEffects.operationId") for item in counts}
    if len(effect_ids) != len(counts):
        view.error("side-effect counts must be unique per operation")
    if not completed | {retry_id} <= effect_ids:
        view.error("side-effect counts must cover stored successes, failures, and retries")
    for count in counts:
        actual = count.get("count")
        expected = count.get("expectedCount")
        if not is_json_integer(actual) or not is_json_integer(expected) or actual < 0 or expected < 0:
            view.error("side-effect counts must be non-negative integers")
        elif actual != expected:
            view.error("side-effect count differs from the fixture's retry policy", "ASSERTION")
