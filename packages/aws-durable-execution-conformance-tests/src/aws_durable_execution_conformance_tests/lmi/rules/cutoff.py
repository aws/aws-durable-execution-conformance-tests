# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Observation-cutoff obligations: retries active at the cutoff and deferred service requests.

Collection freezes observations at ``observationEndedAtMs``. A victim retry can
still be running then (``activeAtObservationEnd``), and a service retry can start
only after it (a deferred service request). Collection records both; adjudication
proves them against the capture's independent retirement evidence.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any

from aws_durable_execution_conformance_tests.lmi.rules.retention import _owned_record_enriches


def deferred_service_requests(evidence: Mapping[str, Any]) -> dict[str, str]:
    collection = evidence.get("collection")
    rows = collection.get("deferredServiceRequests", []) if isinstance(collection, Mapping) else []
    if not isinstance(rows, list):
        raise ValueError("Deferred service requests must be a list")
    requests: dict[str, str] = {}
    for row in rows:
        request = row.get("requestId") if isinstance(row, Mapping) else None
        arn = row.get("executionArn") if isinstance(row, Mapping) else None
        if (
            not isinstance(request, str)
            or not request.strip()
            or request in requests
            or not isinstance(arn, str)
            or not arn.strip()
        ):
            raise ValueError("Deferred service requests must have unique request IDs and owned execution ARNs")
        requests[request] = arn
    return requests


def cutoff_obligations(evidence: Mapping[str, Any]) -> tuple[set[str], dict[str, str]]:
    """Return the retries active at the cutoff and the deferred service requests.

    Raises ``ValueError`` for a malformed deferral inventory.
    """
    attempts = evidence.get("attempts", [])
    active = {
        item["requestId"]
        for item in (attempts if isinstance(attempts, list) else [])
        if isinstance(item, Mapping)
        and item.get("activeAtObservationEnd") is True
        and isinstance(item.get("requestId"), str)
    }
    return active, deferred_service_requests(evidence)


def requests_after_cutoff(attempts: Sequence[Mapping[str, Any]], cutoff_ms: Any) -> set[str]:
    """Exclude proven late service retries while keeping every role's first attempt."""
    if type(cutoff_ms) not in (int, float) or not 0 <= cutoff_ms < math.inf:
        return set()
    return {
        request
        for item in attempts
        if item.get("source") == "service-observed"
        and type(ordinal := item.get("attempt")) is int
        and ordinal > 1
        and isinstance(request := item.get("requestId"), str)
        and bool(request)
        and isinstance(arn := item.get("executionArn"), str)
        and bool(arn)
        and isinstance(start := item.get("nativeStartedAtMs"), (int, float))
        and not isinstance(start, bool)
        and cutoff_ms < start < math.inf
    }


def active_retry_interval(attempt: Mapping[str, Any], cutoff: Any, victim_arn: Any) -> bool:
    """Check the service-bound interval for a nonselected retry of the admitted victim."""
    ordinal, started, ended = (attempt.get(key) for key in ("attempt", "nativeStartedAtMs", "nativeEndedAtMs"))
    return (
        attempt.get("source") == "service-observed"
        and attempt.get("role") == "victim"
        and isinstance(victim_arn, str)
        and bool(victim_arn)
        and attempt.get("executionArn") == victim_arn
        and type(ordinal) is int
        and ordinal > 1
        and isinstance(started, (int, float))
        and not isinstance(started, bool)
        and isinstance(ended, (int, float))
        and not isinstance(ended, bool)
        and isinstance(cutoff, (int, float))
        and not isinstance(cutoff, bool)
        and 0 <= started <= cutoff < ended < math.inf
    )


def active_pre_cutoff_errors(original: Mapping[str, Any], final: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    original_attempts = original.get("attempts", [])
    if not isinstance(original_attempts, list):
        return ["Retirement cannot reconcile malformed original attempts"]
    active_retries = {
        item["requestId"]
        for item in original_attempts
        if isinstance(item, Mapping)
        and item.get("activeAtObservationEnd") is True
        and isinstance(item.get("requestId"), str)
    }
    cutoff = original.get("observationEndedAtMs")
    if active_retries and isinstance(cutoff, (int, float)) and not isinstance(cutoff, bool):
        for field, name in (("events", "events"), ("sdkRequests", "SDK requests")):
            before, after = original.get(field, []), final.get(field, [])
            if not isinstance(before, list) or not isinstance(after, list):
                errors.append(f"Retirement cannot reconcile malformed {name} for an active retry")
                continue
            retained = [
                row
                for row in after
                if isinstance(row, Mapping)
                and isinstance(row.get("requestId"), str)
                and row["requestId"] in active_retries
                and isinstance(at := row.get("atMs"), (int, float))
                and not isinstance(at, bool)
                and at <= cutoff
            ]
            fully_retained = True
            for row in before:
                if (
                    not isinstance(row, Mapping)
                    or not isinstance(request := row.get("requestId"), str)
                    or request not in active_retries
                ):
                    continue
                match = next(
                    (
                        index
                        for index, final_row in enumerate(retained)
                        if _owned_record_enriches(row, final_row, event=name == "events")
                    ),
                    None,
                )
                if match is None:
                    fully_retained = False
                    break
                retained.pop(match)
            if not fully_retained or retained:
                errors.append(f"Retirement disagrees on pre-cutoff {name} for an active retry")

    return errors


def deferred_retirement_errors(
    original: Mapping[str, Any], final: Mapping[str, Any], *, undispatched: bool = False
) -> list[str]:
    obligations = deferred_service_requests(original)
    if not obligations:
        return []
    attempts = final.get("attempts", [])
    if not isinstance(attempts, list):
        return ["Retirement cannot reconcile deferred service requests without an invocation inventory"]
    indexed = {
        attempt["requestId"]: attempt
        for attempt in attempts
        if isinstance(attempt, Mapping) and isinstance(attempt.get("requestId"), str)
    }
    if undispatched:
        retirement = final.get("retirement")
        resources = retirement.get("resources") if isinstance(retirement, Mapping) else None
        if isinstance(resources, list):
            for item in resources:
                if (
                    isinstance(item, Mapping)
                    and item.get("kind") == "invocation"
                    and item.get("outcome") == "UNDISPATCHED"
                    and isinstance(item.get("id"), str)
                    and item["id"] not in indexed
                ):
                    # Physical retirement only; proven undispatched still never
                    # satisfies a deferred obligation for behavior purposes.
                    indexed[item["id"]] = item
    cutoff = original.get("observationEndedAtMs")
    errors = []
    for request, arn in obligations.items():
        terminal = indexed.get(request, {})
        started = terminal.get("startedAtMs")
        if terminal.get("executionArn") != arn:
            errors.append(f"Retirement omitted deferred service request {request}")
        elif (
            not isinstance(cutoff, (int, float))
            or isinstance(cutoff, bool)
            or not isinstance(started, (int, float))
            or isinstance(started, bool)
            or not 0 <= cutoff < started < math.inf
        ):
            errors.append(f"Retirement did not prove deferred service request {request} started after the cutoff")
    return errors


def active_retirement_errors(original: Mapping[str, Any], final: Mapping[str, Any]) -> list[str]:
    """Prove that each retry active at the cutoff reached its actual service completion."""
    errors: list[str] = []
    original_attempts = original.get("attempts", [])
    observed = final.get("attempts", [])
    if not isinstance(original_attempts, list) or not isinstance(observed, list):
        raise ValueError("Retirement invocation inventories must be lists")
    indexed = {item.get("requestId"): item for item in observed if isinstance(item, Mapping)}
    errors.extend(deferred_retirement_errors(original, final))
    for attempt in original_attempts:
        if not isinstance(attempt, Mapping):
            raise ValueError("Original invocation inventory is malformed")
        if attempt.get("activeAtObservationEnd") is not True:
            continue
        settled = indexed.get(attempt.get("requestId"), {})
        cutoff = original.get("observationEndedAtMs")
        end = settled.get("endedAtMs")
        if (
            attempt.get("startedAtMs") != settled.get("startedAtMs")
            or "activeAtObservationEnd" in settled
            or settled.get("outcome") not in ("SUCCEEDED", "FAILED", "TIMED_OUT", "PENDING")
            or not isinstance(cutoff, (int, float))
            or isinstance(cutoff, bool)
            or not isinstance(end, (int, float))
            or isinstance(end, bool)
            or not 0 <= cutoff < end < math.inf
        ):
            errors.append("Retirement did not reconcile an active retry with its actual service completion")
    return errors
