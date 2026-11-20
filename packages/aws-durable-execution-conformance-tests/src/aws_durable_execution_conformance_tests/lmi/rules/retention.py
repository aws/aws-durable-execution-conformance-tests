# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Evidence retention: merging snapshots of one run and reconciling their records."""

from __future__ import annotations

import copy
import math
from collections import Counter, deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from itertools import pairwise
from typing import Any, TypeGuard

from aws_durable_execution_conformance_tests.lmi.rules.values import (
    HISTORY_STATUSES,
    TERMINAL_STATUSES,
    is_json_integer,
    same_json,
)

# Lambda OperationStatus differs from the execution-level status vocabulary.
_OPERATION_NONTERMINAL_STATUSES = ("STARTED", "PENDING", "READY")
_OPERATION_TERMINAL_STATUSES = ("SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT", "STOPPED")
# replay.before contains completed checkpoints; only current operation views progress.
_OPERATION_RECORD_PATHS: frozenset[tuple[str, ...]] = frozenset({("histories", "operations"), ("replay", "after")})
_OUTCOME_RECORD_PATHS = _OPERATION_RECORD_PATHS | {("histories",), ("replay", "before")}
_OBSERVATION_IDENTITIES: dict[tuple[str, ...], str] = {
    ("attempts",): "requestId",
    ("histories",): "executionArn",
    ("histories", "operations"): "id",
    ("faultsCreated",): "id",
    ("resourcesCreated",): "id",
    ("replay", "before"): "id",
    ("replay", "after"): "id",
    ("replay", "retry", "attempts"): "attemptId",
    ("replay", "sideEffects"): "operationId",
}


def _observation_advanced(previous: Any, current: Any, path: tuple[str, ...]) -> bool:
    if path in (("attempts", "startedAtMsSource"), ("attempts", "endedAtMsSource")):
        return previous == "fixture" and current == "platform"
    if path == ("histories", "status"):
        return previous in ("RUNNING", "PENDING") and current in HISTORY_STATUSES
    if path[:-1] in _OPERATION_RECORD_PATHS and path[-1:] == ("status",):
        return previous in _OPERATION_NONTERMINAL_STATUSES and current in (
            *_OPERATION_NONTERMINAL_STATUSES,
            *_OPERATION_TERMINAL_STATUSES,
        )
    if path == ("replay", "sideEffects", "count"):
        return is_json_integer(previous) and is_json_integer(current) and 0 <= previous <= current
    increasing = path == ("observationEndedAtMs",) or path[:2] == ("collection", "watermarks")
    increasing |= path == ("attempts", "endedAtMs")
    decreasing = path == ("attempts", "startedAtMs")
    if not (increasing or decreasing):
        return False
    if any(type(value) not in (int, float) or not math.isfinite(value) or value < 0 for value in (previous, current)):
        return False
    return current >= previous if increasing else current <= previous


def _owned_record_enriches(previous: Any, current: Any, *, event: bool = False) -> bool:
    if not isinstance(previous, Mapping) or not isinstance(current, Mapping):
        return False
    metadata = {"observedDuringRequestId"}
    if event:
        metadata.add("sequence")
        for record in (previous, current):
            if "sequence" in record and (not is_json_integer(record["sequence"]) or record["sequence"] < 0):
                return False
        if "sequence" in previous and "sequence" not in current:
            return False
    # The shared owned-record validator treats missing and null correlation as
    # unassigned. Once an invocation ID is present it cannot be replaced.
    if previous.get("observedDuringRequestId") is not None and (
        "observedDuringRequestId" not in current
        or not same_json(previous["observedDuringRequestId"], current["observedDuringRequestId"])
    ):
        return False
    return same_json(
        {key: value for key, value in previous.items() if key not in metadata},
        {key: value for key, value in current.items() if key not in metadata},
    )


@dataclass
class _RetentionChanges:
    omitted: set[tuple[str, ...]] = field(default_factory=set)
    conflicts: set[tuple[str, ...]] = field(default_factory=set)

    def __bool__(self) -> bool:
        return bool(self.omitted or self.conflicts)

    def merge(self, other: _RetentionChanges) -> None:
        self.omitted.update(other.omitted)
        self.conflicts.update(other.conflicts)


def _operation_entries_compatible(previous: Any, current: Any) -> bool:
    if same_json(previous, current):
        return True
    if not isinstance(previous, Mapping) or not isinstance(current, Mapping):
        return False
    for key in ("requestId", "operationId"):
        if not isinstance(previous.get(key), str) or not previous[key].strip() or previous[key] != current.get(key):
            return False
    if ("attemptId" in previous) != ("attemptId" in current):
        return False
    if "attemptId" in previous:
        attempt_id = previous["attemptId"]
        if not isinstance(attempt_id, str) or not attempt_id.strip() or attempt_id != current["attemptId"]:
            return False
    elif not (previous.keys() <= current.keys() or current.keys() <= previous.keys()):
        # Without a retry identity, complementary partial descriptions do not
        # establish that two observations came from the same body occurrence.
        return False
    return all(same_json(previous[key], current[key]) for key in previous.keys() & current.keys())


def _retain_operation_entries(previous: list[Any], current: list[Any]) -> tuple[list[Any], _RetentionChanges]:
    """Match compatible body entries one-to-one; their order is not an identity."""
    candidates = [
        [index for index, item in enumerate(current) if _operation_entries_compatible(record, item)]
        for record in previous
    ]
    by_previous: dict[int, int] = {}
    by_current: dict[int, int] = {}
    for start in range(len(previous)):
        pending = deque([start])
        parents: dict[int, int] = {}
        endpoint: int | None = None
        while pending and endpoint is None:
            original = pending.popleft()
            for observed in candidates[original]:
                if observed in parents:
                    continue
                parents[observed] = original
                if observed not in by_current:
                    endpoint = observed
                    break
                pending.append(by_current[observed])
        # Reassign an earlier match when necessary, so a bare entry cannot
        # consume the only occurrence compatible with another entry's metadata.
        while endpoint is not None:
            original = parents[endpoint]
            prior_match = by_previous.get(original)
            by_previous[original] = endpoint
            by_current[endpoint] = original
            endpoint = prior_match

    retained = copy.deepcopy(current)
    changes = _RetentionChanges()
    for index, record in enumerate(previous):
        match = by_previous.get(index)
        if match is None:
            retained.append(copy.deepcopy(record))
            changes.omitted.add(("replay", "operationExecutions"))
        elif isinstance(record, Mapping):
            for key, value in record.items():
                if key not in retained[match]:
                    retained[match][key] = copy.deepcopy(value)
                    changes.omitted.add(("replay", "operationExecutions"))
    return retained, changes


def _observation_updates(
    previous: Mapping[str, Any], current: Mapping[str, Any], path: tuple[str, ...] = ()
) -> dict[str, Any]:
    """Extract new facts without replaying stale fields from a supplied object."""
    updates: dict[str, Any] = {}
    for key, value in current.items():
        if key in previous and same_json(previous[key], value):
            continue
        atomic = path == ("expectedHistories",) or (path in _OUTCOME_RECORD_PATHS and key in ("result", "error"))
        if key not in previous or atomic:
            updates[key] = copy.deepcopy(value)
        elif isinstance(previous[key], Mapping) and isinstance(value, Mapping):
            nested = _observation_updates(previous[key], value, (*path, key))
            if nested:
                updates[key] = nested
        elif isinstance(previous[key], list) and isinstance(value, list) and (*path, key) in _OBSERVATION_IDENTITIES:
            identity = _OBSERVATION_IDENTITIES[(*path, key)]
            previous_counts = Counter(
                record[identity]
                for record in previous[key]
                if isinstance(record, Mapping) and isinstance(record.get(identity), str)
            )
            current_counts = Counter(
                record[identity]
                for record in value
                if isinstance(record, Mapping) and isinstance(record.get(identity), str)
            )
            if any(current_counts[identifier] > count for identifier, count in previous_counts.items()):
                # A lone extra occurrence would merge back into the known row.
                # Preserve this inventory's full multiplicity and facts instead.
                updates[key] = copy.deepcopy(value)
                continue
            remaining = list(previous[key])
            records = []
            for record in value:
                match = next(
                    (
                        index
                        for index, item in enumerate(remaining)
                        if isinstance(record, Mapping)
                        and isinstance(record.get(identity), str)
                        and isinstance(item, Mapping)
                        and record[identity] == item.get(identity)
                    ),
                    None,
                )
                if match is None:
                    records.append(copy.deepcopy(record))
                    continue
                prior = remaining.pop(match)
                nested = _observation_updates(prior, record, (*path, key))
                if nested:
                    nested[identity] = record[identity]
                    if (*path, key) == ("attempts",):
                        for time_field in ("startedAtMs", "endedAtMs"):
                            source_field = f"{time_field}Source"
                            if time_field in nested or source_field in nested:
                                for field_name in (time_field, source_field):
                                    if field_name in record:
                                        nested[field_name] = record[field_name]
                    if (
                        (*path, key) in _OUTCOME_RECORD_PATHS
                        and {"status", "result", "error"} & nested.keys()
                        and "status" in record
                    ):
                        nested["status"] = record["status"]
                        # Status can make an unchanged outcome immutable. Carry
                        # present values (including null), but not removed fields.
                        for outcome in ("result", "error"):
                            if outcome in record:
                                nested[outcome] = copy.deepcopy(record[outcome])
                    records.append(nested)
            if records:
                updates[key] = records
        else:
            # Unkeyed lists stay whole: operationExecutions can contain multiple
            # body occurrences with the same request/operation identities.
            updates[key] = copy.deepcopy(value)
    return updates


def _retain_observations(
    original: Mapping[str, Any],
    captured: Mapping[str, Any],
    path: tuple[str, ...] = (),
    *,
    compare_sdk_order: bool = True,
) -> tuple[dict[str, Any], _RetentionChanges]:
    """Keep every prior fact while adding a failed collector's new observations.

    Native intervals, observation watermarks and unfinished service histories
    may advance. Other conflicting facts retain their original values. Lists
    preserve known records, including multiplicity, so collection cannot erase
    owned work. The raw snapshots are audited separately during retirement.
    """
    retained = copy.deepcopy(dict(captured))
    changed = _RetentionChanges()
    completing_execution = (
        path == ("histories",)
        and original.get("status") in ("RUNNING", "PENDING")
        and captured.get("status") in ("SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED")
    )
    progressing_operation = path in _OPERATION_RECORD_PATHS and _observation_advanced(
        original.get("status"), captured.get("status"), (*path, "status")
    )
    for key, previous in original.items():
        if path == ("attempts",) and key in ("startedAtMs", "endedAtMs"):
            source_key = f"{key}Source"
            if original.get(source_key) == captured.get(source_key) == "platform" and key in captured:
                if type(captured[key]) not in (int, float) or previous != captured[key]:
                    retained[key] = previous
                    changed.conflicts.add((*path, key))
                continue
        if path == ("attempts",) and key in ("startedAtMsSource", "endedAtMsSource"):
            timestamp_key = key.removesuffix("Source")
            if (
                previous == "platform"
                and key not in captured
                and type(captured.get(timestamp_key)) in (int, float)
                and captured[timestamp_key] == original.get(timestamp_key)
            ):
                retained[key] = previous
                continue
            if retained.get(key) == "platform" and timestamp_key not in captured and previous != "platform":
                retained[key] = previous
                changed.conflicts.add((*path, key))
                continue

        if (
            path == ("attempts",)
            and key in ("startedAtMs", "endedAtMs")
            and original.get(f"{key}Source") in (None, "fixture")
            and captured.get(f"{key}Source") == "platform"
            and key in captured
        ):
            continue  # The platform measurement supersedes the fixture estimate.

        if (
            path == ("attempts",)
            and key in ("startedAtMs", "endedAtMs")
            and original.get(f"{key}Source") == "platform"
            and captured.get(f"{key}Source") in (None, "fixture")
        ):
            retained[key] = previous
            continue
        if key in ("result", "error") and (progressing_operation or (completing_execution and previous is None)):
            # Active operations can replace intermediate poll results or retry
            # errors. Terminal checkpoints and non-null execution outcomes stay
            # immutable; an unfinished execution's null is only a placeholder.
            continue
        if path == ("expectedHistories",) or (path in _OUTCOME_RECORD_PATHS and key in ("result", "error")):
            # Checkpoint JSON and each predeclared execution expectation are
            # atomic contracts: arrays, object contents and status allowances
            # cannot be rewritten to accommodate the observed result.
            if key not in retained or not same_json(previous, retained[key]):
                if key in retained:
                    changed.conflicts.add((*path, key))
                else:
                    changed.omitted.add((*path, key))
                retained[key] = copy.deepcopy(previous)
            continue
        if key not in retained:
            retained[key] = copy.deepcopy(previous)
            changed.omitted.add((*path, key))
        elif isinstance(previous, Mapping) and isinstance(retained[key], Mapping):
            retained[key], nested_changed = _retain_observations(previous, retained[key], (*path, key))
            changed.merge(nested_changed)
        elif (
            (*path, key) == ("replay", "operationExecutions")
            and isinstance(previous, list)
            and isinstance(retained[key], list)
        ):
            retained[key], nested_changed = _retain_operation_entries(previous, retained[key])
            changed.merge(nested_changed)
        elif isinstance(previous, list) and isinstance(retained[key], list):
            remaining = list(range(len(retained[key])))
            identity = _OBSERVATION_IDENTITIES.get((*path, key))
            events = (*path, key) == ("events",)
            owned_records = (*path, key) in (("events",), ("sdkRequests",))
            event_order: dict[str, list[tuple[int, int]]] = {}
            prior_records = previous
            if events and all(
                isinstance(record, Mapping) and is_json_integer(record.get("sequence"))
                for record in [*previous, *retained[key]]
            ):
                prior_records = sorted(previous, key=lambda record: record["sequence"])
                remaining.sort(key=lambda index: retained[key][index]["sequence"])
            elif owned_records:
                # Reserve already correlated occurrences before matching an
                # unattributed record that could fit more than one occurrence.
                prior_records = sorted(
                    previous,
                    key=lambda record: not isinstance(record, Mapping) or record.get("observedDuringRequestId") is None,
                )
            for record in prior_records:
                match = next(
                    (
                        index
                        for index in remaining
                        if same_json(record, retained[key][index])
                        or (
                            owned_records
                            and (
                                _owned_record_enriches(record, retained[key][index], event=owned_records)
                                or _owned_record_enriches(retained[key][index], record, event=owned_records)
                            )
                        )
                        or (
                            identity is not None
                            and isinstance(record, Mapping)
                            and isinstance(record.get(identity), str)
                            and isinstance(retained[key][index], Mapping)
                            and record[identity] == retained[key][index].get(identity)
                        )
                    ),
                    None,
                )
                if match is None:
                    retained[key].append(copy.deepcopy(record))
                    changed.omitted.add((*path, key))
                else:
                    remaining.remove(match)
                    raw_sequence = retained[key][match].get("sequence") if events else None
                    if owned_records and isinstance(record, Mapping):
                        for metadata in ("observedDuringRequestId", "sequence"):
                            if metadata == "observedDuringRequestId" and record.get(metadata) is None:
                                continue
                            if metadata in record and (
                                metadata not in retained[key][match]
                                or (metadata == "observedDuringRequestId" and retained[key][match][metadata] is None)
                            ):
                                retained[key][match][metadata] = copy.deepcopy(record[metadata])
                                changed.omitted.add((*path, key))
                    if events and isinstance(record, Mapping):
                        old_sequence, new_sequence = record.get("sequence"), raw_sequence
                        request_id = record.get("requestId")
                        if (
                            isinstance(request_id, str)
                            and is_json_integer(old_sequence)
                            and is_json_integer(new_sequence)
                        ):
                            event_order.setdefault(request_id, []).append((int(old_sequence), int(new_sequence)))
                    if identity is not None and isinstance(record, Mapping):
                        retained[key][match], nested_changed = _retain_observations(
                            record, retained[key][match], (*path, key)
                        )
                        changed.merge(nested_changed)
            for order in event_order.values():
                # Numerical renumbering is harmless; reversing known local
                # event order would erase a lifecycle failure before its audit.
                if compare_sdk_order and any(
                    first[0] < second[0] and first[1] >= second[1] for first, second in pairwise(sorted(order))
                ):
                    changed.conflicts.add((*path, key, "sequence"))
        elif (
            path == ("attempts",)
            and key in ("startedAtMsSource", "endedAtMsSource")
            and previous == "platform"
            and retained[key] == "fixture"
        ):
            retained[key] = previous
        elif not same_json(previous, retained[key]) and not _observation_advanced(
            previous, retained[key], (*path, key)
        ):
            retained[key] = copy.deepcopy(previous)
            changed.conflicts.add((*path, key))
    if path == ("attempts",):
        for key in ("startedAtMs", "endedAtMs"):
            source_key = f"{key}Source"
            if (
                captured.get(source_key) == "platform"
                and key not in captured
                and original.get(source_key) != "platform"
            ):
                retained.pop(source_key, None)
                changed.conflicts.add((*path, source_key))
    if not path:
        for attempt in retained.get("attempts", ()) if isinstance(retained.get("attempts"), list) else ():
            if isinstance(attempt, dict):
                for endpoint in ("startedAtMs", "endedAtMs"):
                    source = f"{endpoint}Source"
                    if endpoint not in attempt and attempt.get(source) == "platform":
                        attempt.pop(source)
        if compare_sdk_order and _sdk_frames_order_changed(original, captured):
            changed.conflicts.add(("sdkRequests", "sequence"))
    return retained, changed


def _sdk_closure_order_changed(
    request: Mapping[str, Any],
    settled_request: Mapping[str, Any],
    before_events: Sequence[Any],
    after_events: Sequence[Any],
) -> bool:
    if request.get("outcome") == "DISPATCHED":
        local_closures = []
        for events in (before_events, after_events):
            local_closures.append(
                [
                    event
                    for event in events
                    if event.get("kind") in ("wrapper_return", "wrapper_raise", "admission_closed")
                    and event.get("requestId") == request.get("requestId")
                    and event.get("atMs") == request.get("atMs")
                ]
            )
        for before_boundary in local_closures[0]:
            for after_boundary in local_closures[1]:
                if before_boundary.get("kind") != after_boundary.get("kind"):
                    continue
                sequences = [
                    request.get("sequence"),
                    before_boundary.get("sequence"),
                    settled_request.get("sequence"),
                    after_boundary.get("sequence"),
                ]
                if all(is_json_integer(sequence) for sequence in sequences) and (
                    (sequences[0] < sequences[1]) != (sequences[2] < sequences[3])
                ):
                    return True

    return False


def _sdk_frames_order_changed(before: Mapping[str, Any], after: Mapping[str, Any]) -> bool:
    prior_requests = before.get("sdkRequests", [])
    current_requests = after.get("sdkRequests", [])
    if not isinstance(prior_requests, list) or not isinstance(current_requests, list):
        return False
    return any(
        _sdk_closure_order_changed(request, candidate, before.get("events", []), after.get("events", []))
        for request in prior_requests
        if isinstance(request, Mapping)
        for candidate in current_requests
        if isinstance(candidate, Mapping)
        if _owned_record_enriches(request, candidate, event=True)
    )


def _event_frames_order_changed(before: Mapping[str, Any], after: Mapping[str, Any]) -> bool:
    _, changes = _retain_observations({"events": before.get("events", [])}, {"events": after.get("events", [])})
    return ("events", "sequence") in changes.conflicts


def _native_intervals(
    sources: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, tuple[int | float, bool]]]:
    """Resolve each endpoint with its source before using native intervals."""
    native: dict[str, dict[str, tuple[int | float, bool]]] = {}
    for source in sources:
        attempts = source.get("attempts", [])
        for attempt in attempts if isinstance(attempts, list) else ():
            if not isinstance(attempt, Mapping) or not isinstance(request := attempt.get("requestId"), str):
                continue
            endpoints = native.setdefault(request, {})
            for endpoint in ("startedAtMs", "endedAtMs"):
                at = attempt.get(endpoint)
                if (
                    not isinstance(at, (int, float))
                    or isinstance(at, bool)
                    or at < 0
                    or (isinstance(at, float) and not math.isfinite(at))
                ):
                    continue
                platform = attempt.get(f"{endpoint}Source") == "platform"
                previous = endpoints.get(endpoint)
                if previous is None or (platform and not previous[1]):
                    endpoints[endpoint] = (at, platform)
                elif platform == previous[1]:
                    # Conflicting platform measurements are audit errors. Use
                    # the stricter bound for checks that also consume them.
                    select = max if (endpoint == "startedAtMs") == platform else min
                    endpoints[endpoint] = (select(previous[0], at), platform)
    return native


def _cleanup_record_errors(sources: Sequence[Mapping[str, Any]], final_snapshot: Mapping[str, Any] | None) -> list[str]:
    """Reconcile every raw record's native owner, even for opaque event kinds."""
    findings: list[str] = []

    def records(value: Any) -> list[Mapping[str, Any]]:
        return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []

    def identifier(value: Any) -> TypeGuard[str]:
        return isinstance(value, str) and bool(value.strip())

    def timestamp(value: Any) -> int | float | None:
        try:
            if type(value) in (int, float) and value >= 0 and math.isfinite(value):
                return value
        except OverflowError:
            pass
        return None

    bindings: dict[str, set[str]] = {}
    native = _native_intervals(sources)
    starts = {request: bounds["startedAtMs"][0] for request, bounds in native.items() if "startedAtMs" in bounds}
    ends = {request: bounds["endedAtMs"][0] for request, bounds in native.items() if "endedAtMs" in bounds}
    windows: list[int | float] = []
    for source in sources:
        end = timestamp(source.get("observationEndedAtMs"))
        if end is not None:
            windows.append(end)
        for attempt in records(source.get("attempts")):
            request_id = attempt.get("requestId")
            if not identifier(request_id):
                continue
            known = bindings.setdefault(request_id, set())
            execution_arn = attempt.get("executionArn")
            if identifier(execution_arn):
                known.add(execution_arn)
    known_executions = {execution for observed in bindings.values() for execution in observed}
    retirement = final_snapshot.get("retirement") if final_snapshot is not None else None
    settlements = {
        (item.get("kind"), item.get("id")): item
        for item in records(retirement.get("resources") if isinstance(retirement, Mapping) else None)
        if identifier(item.get("kind")) and identifier(item.get("id"))
    }

    for source in sources:
        collections = {name: records(source.get(name)) for name in ("events", "sdkRequests")}
        replay = source.get("replay")
        if isinstance(replay, Mapping):
            collections["replay.operationExecutions"] = records(replay.get("operationExecutions"))
            retry = replay.get("retry")
            if isinstance(retry, Mapping):
                collections["replay.retry.attempts"] = records(retry.get("attempts"))
        for collection, inventory in collections.items():
            for record in inventory:
                if collection.startswith("replay.") and not {
                    "requestId",
                    "ownerRequestId",
                    "executionArn",
                    "observedDuringRequestId",
                }.intersection(record):
                    continue  # Behavior-only partial records need no physical owner yet.
                field = {"events": "kind", "sdkRequests": "method"}.get(collection)
                if field is not None and field in record and not identifier(record[field]):
                    findings.append(f"INCOMPLETE: cleanup {collection}.{field} must be a non-empty string")
                if (
                    collection == "sdkRequests"
                    and "outcome" in record
                    and record["outcome"] not in ("DISPATCHED", "REJECTED")
                ):
                    findings.append("INCOMPLETE: cleanup sdkRequests.outcome must be DISPATCHED or REJECTED")
                observed_at = timestamp(record.get("atMs"))
                if "atMs" in record and observed_at is None:
                    findings.append(f"INCOMPLETE: cleanup {collection}.atMs must be a finite non-negative number")
                for key in ("requestId", "ownerRequestId", "executionArn", "observedDuringRequestId"):
                    if (
                        key in record
                        and not (key == "observedDuringRequestId" and record[key] is None)
                        and not identifier(record[key])
                    ):
                        findings.append(f"INCOMPLETE: cleanup {collection}.{key} must identify its native owner")
                if final_snapshot is None:
                    continue

                referenced_requests: set[str] = set()
                for key in ("requestId", "ownerRequestId", "observedDuringRequestId"):
                    request_id = record.get(key)
                    if not identifier(request_id):
                        continue
                    referenced_requests.add(request_id)
                    if len(bindings.get(request_id, set())) != 1:
                        findings.append(f"INCOMPLETE: cleanup {collection}.{key} has an unresolved native binding")
                request_id = record.get("requestId", record.get("ownerRequestId"))
                execution_arn = record.get("executionArn")
                if not referenced_requests and identifier(execution_arn):
                    candidates = {request for request, known in bindings.items() if known == {execution_arn}}
                    if len(candidates) == 1:
                        referenced_requests.update(candidates)
                        request_id = next(iter(candidates))
                if not referenced_requests:
                    findings.append(f"INCOMPLETE: cleanup {collection} has no resolved native owner")
                expected = bindings.get(request_id, set()) if identifier(request_id) else set()
                if identifier(execution_arn) and execution_arn not in known_executions:
                    findings.append(f"INCOMPLETE: cleanup {collection}.executionArn has an unresolved execution owner")
                elif identifier(execution_arn) and len(expected) == 1 and execution_arn not in expected:
                    findings.append(f"ASSERTION: cleanup {collection} is attributed to a different execution")
                owner = record.get("ownerRequestId")
                if (
                    identifier(record.get("requestId"))
                    and identifier(owner)
                    and len(bindings.get(record["requestId"], set())) == 1
                    and len(bindings.get(owner, set())) == 1
                    and owner != record["requestId"]
                ):
                    findings.append(f"ASSERTION: cleanup {collection} is attributed to a different invocation")

                # Event-only discoveries need settlement coverage too: the
                # snapshot containing an event may predate its native binding.
                for referenced in referenced_requests:
                    settlement = settlements.get(("invocation", referenced), {})
                    settled_at = timestamp(settlement.get("settledAtMs"))
                    if (
                        settlement.get("outcome") != "QUIESCENT"
                        or settled_at is None
                        or (observed_at is not None and settled_at < observed_at)
                    ):
                        findings.append(f"INCOMPLETE: cleanup {collection} native owner {referenced} is not settled")
                referenced_executions = {
                    execution for referenced in referenced_requests for execution in bindings.get(referenced, set())
                }
                if identifier(execution_arn):
                    referenced_executions.add(execution_arn)
                for execution in referenced_executions:
                    settlement = settlements.get(("execution", execution), {})
                    if (
                        settlement.get("outcome") not in TERMINAL_STATUSES
                        or timestamp(settlement.get("settledAtMs")) is None
                    ):
                        findings.append(f"INCOMPLETE: cleanup {collection} execution owner {execution} is not settled")

                if observed_at is None:
                    continue  # Omitted timestamp metadata can still be completed.
                started = starts.get(request_id) if identifier(request_id) else None
                if (windows and observed_at > max(windows)) or (started is not None and observed_at < started):
                    findings.append(
                        f"INCOMPLETE: cleanup {collection}.atMs lies outside the retained observation window"
                    )
                during = record.get("observedDuringRequestId")
                if identifier(during):
                    if during not in starts or during not in ends:
                        findings.append(
                            f"INCOMPLETE: cleanup {collection}.observedDuringRequestId lacks a native interval"
                        )
                    elif not starts[during] <= observed_at <= ends[during]:
                        findings.append(
                            f"ASSERTION: cleanup {collection}.atMs lies outside its observedDuringRequestId interval"
                        )
    return findings
