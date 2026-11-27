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
from aws_durable_execution_conformance_tests.lmi.rules.values import is_json_integer, mappings


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


def unsettled_deferred_requests(deferred: Mapping[str, str], retirement_evidence: Mapping[str, Any]) -> set[str]:
    """Deferred requests the independent retirement snapshot still does not cover.

    A deferred request is covered when the retirement snapshot settled it as its
    own attempt, or proved that request ``UNDISPATCHED`` through an UNDISPATCHED
    invocation settlement; either must match the request ID and ``executionArn``.
    The UNDISPATCHED proof is ``undispatched_settlements``'s, established by the
    retirement audit that runs before this check, and it covers only that one
    invocation. This never calls ``deferred_retirement_errors``: that function
    also rejects a physically settled UNDISPATCHED deferral as an unsatisfied
    obligation, which adjudication's cutoff audit relies on, so this coordinator
    check only decides whether the target is safe to reuse.
    """
    retirement = retirement_evidence.get("retirement")
    covered = {
        (item.get("requestId"), item.get("executionArn")) for item in mappings(retirement_evidence.get("attempts"))
    }
    covered |= {
        (item.get("id"), item.get("executionArn"))
        for item in mappings(retirement.get("resources") if isinstance(retirement, Mapping) else None)
        if item.get("kind") == "invocation" and item.get("outcome") == "UNDISPATCHED"
    }
    return {request for request, arn in deferred.items() if (request, arn) not in covered}


def _active_retries(evidence: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    """Attempts active at the cutoff, keyed by their ``requestId``."""
    attempts = evidence.get("attempts", [])
    return {
        item["requestId"]: item
        for item in (attempts if isinstance(attempts, list) else [])
        if isinstance(item, Mapping)
        and item.get("activeAtObservationEnd") is True
        and isinstance(item.get("requestId"), str)
    }


def cutoff_obligations(evidence: Mapping[str, Any]) -> tuple[set[str], dict[str, str]]:
    """Return the retries active at the cutoff and the deferred service requests.

    Raises ``ValueError`` for a malformed deferral inventory.
    """
    return set(_active_retries(evidence)), deferred_service_requests(evidence)


def _settles_active_retry(attempt: Mapping[str, Any], settled: Mapping[str, Any], cutoff: Any) -> bool:
    """Whether ``settled`` proves ``attempt``, active at the cutoff, reached its actual service completion.

    Requires the same native start, no remaining ``activeAtObservationEnd``
    key, a recognized terminal invocation outcome, and a finite ``endedAtMs``
    strictly after ``cutoff``. Shared by ``active_retirement_errors`` (which
    indexes ``settled`` by ``requestId`` alone) and ``unsettled_active_retries``
    (which also requires an ``executionArn`` match before calling this).
    """
    end = settled.get("endedAtMs")
    return (
        attempt.get("startedAtMs") == settled.get("startedAtMs")
        and "activeAtObservationEnd" not in settled
        and settled.get("outcome") in ("SUCCEEDED", "FAILED", "TIMED_OUT", "PENDING")
        and isinstance(cutoff, (int, float))
        and not isinstance(cutoff, bool)
        and isinstance(end, (int, float))
        and not isinstance(end, bool)
        and 0 <= cutoff < end < math.inf
    )


def unsettled_active_retries(evidence: Mapping[str, Any], retirement_evidence: Mapping[str, Any]) -> set[str]:
    """Retries active at the cutoff that the independent retirement snapshot still does not settle.

    A retry active at the cutoff can have no in-window events or SDK requests
    of its own, for example when native admission came before the cutoff but
    wrapper initialization came after it; ``_cleanup_record_errors`` then has
    nothing to key on. This checks the frozen ``attempts`` inventory directly
    instead, with ``_settles_active_retry`` plus an ``executionArn`` identity
    match: exactly the settlement condition ``active_retirement_errors`` checks
    for each active retry, never calling that function itself, because it also
    runs ``deferred_retirement_errors``, which deliberately rejects a physically
    settled UNDISPATCHED deferral that ``unsettled_deferred_requests`` accepts.
    The active set comes directly from ``evidence["attempts"]`` rather than
    ``cutoff_obligations``, so a malformed deferred inventory elsewhere in
    ``evidence`` cannot make this raise.
    """
    active = _active_retries(evidence)
    if not active:
        return set()
    cutoff = evidence.get("observationEndedAtMs")
    indexed: dict[tuple[str, Any], Mapping[str, Any]] = {}
    for item in mappings(retirement_evidence.get("attempts")):
        request = item.get("requestId")
        if isinstance(request, str):
            indexed[(request, item.get("executionArn"))] = item
    return {
        request
        for request, attempt in active.items()
        if not _settles_active_retry(attempt, indexed.get((request, attempt.get("executionArn")), {}), cutoff)
    }


def requests_after_cutoff(attempts: Sequence[Mapping[str, Any]], cutoff_ms: Any) -> set[str]:
    """Exclude proven late service retries while keeping every role's first attempt."""
    if type(cutoff_ms) not in (int, float) or not 0 <= cutoff_ms < math.inf:
        return set()
    return {
        request
        for item in attempts
        if item.get("source") == "service-observed"
        and is_json_integer(ordinal := item.get("attempt"))
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
        and is_json_integer(ordinal)
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
                        # `sequence` is renumberable metadata for SDK requests as
                        # well as events (cumulative-snapshot rule); relative
                        # order is checked by `reconcile_admission_closures`.
                        index
                        for index, final_row in enumerate(retained)
                        if _owned_record_enriches(row, final_row, event=True)
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


def deferred_retirement_errors(original: Mapping[str, Any], final: Mapping[str, Any]) -> list[str]:
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
    cutoff = original.get("observationEndedAtMs")
    for attempt in original_attempts:
        if not isinstance(attempt, Mapping):
            raise ValueError("Original invocation inventory is malformed")
        if attempt.get("activeAtObservationEnd") is not True:
            continue
        settled = indexed.get(attempt.get("requestId"), {})
        if not _settles_active_retry(attempt, settled, cutoff):
            errors.append("Retirement did not reconcile an active retry with its actual service completion")
    return errors
