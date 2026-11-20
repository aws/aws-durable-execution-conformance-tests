# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Case-owned retirement, settlement and the independent retirement audit."""

from __future__ import annotations

import math
from collections.abc import Mapping
from itertools import pairwise
from typing import Any

from aws_durable_execution_conformance_tests.lmi.rules import common as common_rules
from aws_durable_execution_conformance_tests.lmi.rules.common import (
    _has_matching_sdk_request,
    normalize_retirement_events,
)
from aws_durable_execution_conformance_tests.lmi.rules.values import (
    _CALL_METHODS,
    _WORK_INTERVALS,
    TERMINAL_STATUSES,
    is_json_integer,
)
from aws_durable_execution_conformance_tests.lmi.rules.view import CaseView


def last_activity(view: CaseView, request_id: Any) -> float:
    """The latest recorded time of an invocation, its events and SDK requests."""
    sdk_requests = view.value.get("sdkRequests", [])
    records = [*view.events, *(sdk_requests if isinstance(sdk_requests, list) else [])]
    times = [view.attempts.get(request_id, {}).get("endedAtMs")]
    times += [
        record.get("atMs")
        for record in records
        if isinstance(record, Mapping) and record.get("requestId") == request_id
    ]
    valid = [
        float(value)
        for value in times
        if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
    ]
    return max(valid, default=0.0)


def observed_retirement_work(view: CaseView, retirement: Mapping[str, Any], end: float) -> None:
    events = normalize_retirement_events(view.events, view.errors)
    settled_by_request = {
        item.get("id"): item.get("settledAtMs")
        for item in view.records(retirement.get("resources"), "retirement.resources")
        if item.get("kind") == "invocation"
    }
    completions = normalize_retirement_events(
        view.records(retirement.get("completions", []), "retirement.completions"), view.errors
    )
    for completion in completions:
        common_rules.owned_record(view, completion, "retirement.completion", end)
        if view.number(completion.get("atMs"), "retirement.completion.atMs") <= view.value.get(
            "observationEndedAtMs", 0
        ):
            view.error("retirement-only completion must follow the behavior observation window")
    unmatched = set(range(len(completions)))
    for start_kind, end_kind, identity in _WORK_INTERVALS:
        starts = [event for event in events if event.get("kind") == start_kind]
        ends = [event for event in events if event.get("kind") == end_kind]
        ends.extend(completion for completion in completions if completion.get("kind") == end_kind)
        seen: set[tuple[str, str]] = set()
        for start in starts:
            request_id = view.string(start.get("requestId"), f"{start_kind}.requestId")
            work_id = view.string(start.get(identity), f"{start_kind}.{identity}")
            key = (request_id, work_id)
            candidates = [
                item for item in ends if item.get("requestId") == request_id and item.get(identity) == work_id
            ]
            rejected = start_kind in _CALL_METHODS and start.get("outcome") == "REJECTED"
            if key in seen or (len(candidates) != 1 and not (rejected and not candidates)):
                view.error(f"retirement lacks a unique completed {start_kind}/{end_kind} interval")
            elif candidates:
                completion = candidates[0]
                if view.at_or_after(completion, start, require_sequence=True) is False or completion[
                    "atMs"
                ] > view.number(settled_by_request.get(request_id), "invocation.settledAtMs"):
                    view.error(f"retirement {start_kind}/{end_kind} interval lies outside settlement")
                unmatched.difference_update([index for index in unmatched if completions[index] is completion])
            seen.add(key)
    if unmatched:
        view.error("retirement-only completion has no unique observed work start")


def retirement(view: CaseView, *, audit_observed_work: bool = True) -> None:
    retirement = view.mapping(view.value.get("retirement"), "retirement")
    end = view.number(retirement.get("retiredAtMs"), "retirement.retiredAtMs")
    if end < view.value.get("observationEndedAtMs", 0):
        view.error("retirement precedes completion of assertions")
    if audit_observed_work or "completions" in retirement:
        observed_retirement_work(view, retirement, end)
    created_resources = view.records(view.value.get("resourcesCreated"), "resourcesCreated")
    identities = {
        (item.get("kind"), item.get("id"))
        for item in created_resources
        if item.get("kind") in ("invocation", "execution")
    }
    required = {("invocation", item["requestId"]) for item in view.attempts.values()}
    required.update(("execution", item["executionArn"]) for item in view.attempts.values())
    if required != identities:
        view.error("resourcesCreated must match observations for every launched invocation and execution")
    for created_key, settled_key, time_key in (
        ("resourcesCreated", "resources", "settledAtMs"),
        ("faultsCreated", "faults", "releasedAtMs"),
    ):
        created = view.records(view.value.get(created_key), created_key)
        settled = view.records(retirement.get(settled_key), f"retirement.{settled_key}")
        created_ids = [view.string(item.get("id"), f"{created_key}.id") for item in created]
        settled_ids = [view.string(item.get("id"), f"retirement.{settled_key}.id") for item in settled]
        if (
            len(set(created_ids)) != len(created_ids)
            or len(set(settled_ids)) != len(settled_ids)
            or set(created_ids) != set(settled_ids)
        ):
            view.error(f"retirement must settle every owned {settled_key} exactly once")
        for item in (*created, *settled):
            if item.get("ownerCaseId") != view.value.get("caseId"):
                view.error("cleanup attempted to change another case's resources", "ASSERTION")
        if settled_key == "faults":
            for fault in created:
                if "releasedAtMs" in fault and view.number(fault["releasedAtMs"], "faultsCreated.releasedAtMs") > end:
                    view.error("observed fault release occurred after retirement ended")
        for item in settled:
            time = view.number(item.get(time_key), f"retirement.{settled_key}.{time_key}")
            if time > end:
                view.error("retirement ended before owned work settled")
            if settled_key == "resources":
                view.string(item.get("evidenceUri"), "retirement.resources.evidenceUri")
                view.string(item.get("outcome"), "retirement.resources.outcome")
                kind = view.string(item.get("kind"), "retirement.resources.kind")
                original = next((resource for resource in created if resource.get("id") == item.get("id")), {})
                if original.get("kind") != kind:
                    view.error("retirement changed an owned resource's kind")
                if kind not in {"invocation", "execution", "callback", "control"}:
                    view.error(f"retirement has no completion contract for resource kind {kind}")
                if kind == "execution" and item.get("outcome") not in TERMINAL_STATUSES:
                    view.error("an execution may still resume after retirement")
                if kind == "execution":
                    histories = view.value.get("histories")
                    observed = next(
                        (
                            history
                            for history in (histories if isinstance(histories, list) else [])
                            if isinstance(history, Mapping) and history.get("executionArn") == item.get("id")
                        ),
                        {},
                    )
                    # An earlier RUNNING or PENDING snapshot may become terminal.
                    if observed.get("status") in TERMINAL_STATUSES and item.get("outcome") != observed["status"]:
                        view.error(
                            "retirement execution outcome differs from the terminal service history", "ASSERTION"
                        )
                    if "startedAtMs" in observed and time < view.number(
                        observed["startedAtMs"], "history execution.startedAtMs"
                    ):
                        view.error("execution settled before its service start")
                    if (
                        view.standard_behavior
                        and view.value.get("requirementId") in ("lmi-lifecycle-2", "lmi-lifecycle-7")
                        and view.selected.get("victim", {}).get("executionArn") == item.get("id")
                        and sum(attempt.get("executionArn") == item.get("id") for attempt in view.attempts.values())
                        == 1
                        and item.get("outcome")
                        != ("SUCCEEDED" if view.value["requirementId"] == "lmi-lifecycle-2" else "FAILED")
                    ):
                        view.error(
                            "terminal retirement outcome disagrees with the single disposal invocation", "ASSERTION"
                        )
                    operations = view.records(observed.get("operations", []), "history.operations")
                    if any(
                        view.number(operation[field], f"history operation.{field}") > time
                        for operation in operations
                        for field in ("startedAtMs", "endedAtMs")
                        if field in operation
                    ):
                        view.error("execution settled before its recorded service operation")
                if kind == "invocation" and item.get("outcome") != "QUIESCENT":
                    view.error("invocation quiescence is not established")
                if kind == "invocation" and time < last_activity(view, item.get("id")):
                    view.error("invocation settled before its recorded activity")
                if kind == "callback" and item.get("outcome") not in ("SUCCEEDED", "FAILED", "TIMED_OUT"):
                    view.error("callback completion is not established")
                if kind == "control" and item.get("outcome") != "DELETED":
                    view.error("control object deletion is not established")
            if settled_key == "faults":
                if item.get("mode") not in ("normal", "emergency"):
                    view.error("fault release must record normal or emergency mode")
                if view.standard_behavior and view.value.get("requirementId") in (
                    "lmi-lifecycle-3",
                    "lmi-lifecycle-4",
                ):
                    # Only the victim's cleanup and the selected recovery are
                    # asserted; a healthy peer may stay occupied after both.
                    victim = view.selected.get("victim", {})
                    recovery = view.selected.get("recovery", {})
                    asserted = [
                        event.get("atMs", 0)
                        for event in view.events
                        if (event.get("kind") == "task_exit" and event.get("requestId") == victim.get("requestId"))
                        or (
                            recovery
                            and event.get("kind") == "worker_recovered"
                            and event.get("requestId") == recovery.get("requestId")
                        )
                    ]
                    if asserted and time <= max(asserted):
                        view.error("fault release cannot retroactively establish cleanup or recovery", "ASSERTION")


def validate_retirement_evidence(evidence: Mapping[str, Any]) -> list[str]:
    """Audit a separate post-cleanup snapshot without accepting its behavior.

    Failed cases may lack mandatory scenario roles, but every actual launch must
    still be accounted for by the coordinator. This audit reuses common source
    ownership, history shape and resource-inventory checks, then independently
    requires actual wrapper/task/request completion. Neither expected
    business outcomes nor cleanup-time recovery can turn the failed case green.
    """
    if not isinstance(evidence, Mapping):
        return ["INCOMPLETE: retirement evidence must be an object"]
    requirement = evidence.get("requirementId")
    validator = CaseView(
        evidence, requirement_ids=(requirement,) if isinstance(requirement, str) else (), standard_behavior=False
    )
    try:
        validator.string(requirement, "requirementId")
        common_rules.common(validator, validate_behavior=False)
        validator.events = normalize_retirement_events(validator.events, validator.errors)
        retirement(validator, audit_observed_work=False)
        if validator.errors:
            return validator.errors
        record = evidence["retirement"]
        validator.events.extend(normalize_retirement_events(record.get("completions", []), validator.errors))
        records = {item["id"]: item for item in record["resources"]}
        histories = {item["executionArn"]: item for item in evidence["histories"]}
        for arn, history in histories.items():
            if history["status"] not in TERMINAL_STATUSES:
                validator.error("retirement service history still permits execution resume")
            if records[arn].get("outcome") != history["status"]:
                validator.error("retirement execution outcome differs from actual service history")
        call_kinds = {method: kind for kind, method in _CALL_METHODS.items()}
        dispatches: set[tuple[str, str, str]] = set()
        for request in evidence["sdkRequests"]:
            if request.get("outcome") != "DISPATCHED":
                continue
            kind = call_kinds.get(request.get("method"))
            if kind is None:
                validator.error("retirement has no completion contract for dispatched SDK method")
                continue
            call_id = validator.string(request.get("callId"), "dispatched SDK request.callId")
            key = (request["requestId"], kind, call_id)
            if key in dispatches or not any(
                event.get("requestId") == key[0]
                and event.get("kind") == kind
                and event.get("callId") == call_id
                and event.get("outcome", "DISPATCHED") == "DISPATCHED"
                and event.get("atMs") == request.get("atMs")
                for event in validator.events
            ):
                validator.error(f"retirement cannot correlate dispatched SDK request with its unique {kind}")
            dispatches.add(key)
        for request_id in validator.attempts:
            local = [event for event in validator.events if event["requestId"] == request_id]
            sequences = [event.get("sequence") for event in local]
            if any(not is_json_integer(sequence) or sequence < 0 for sequence in sequences) or len(sequences) != len(
                set(sequences)
            ):
                validator.error("retirement requires unique per-request semantic sequence numbers")
                continue
            ordered = sorted(local, key=lambda event: event["sequence"])
            if any(first["atMs"] > second["atMs"] for first, second in pairwise(ordered)):
                validator.error("retirement sequence contradicts its normalized timestamps")

            def boundary(event: Mapping[str, Any]) -> tuple[float, int]:
                return event["atMs"], int(event["sequence"])

            wrappers = [event for event in local if event["kind"] in {"wrapper_return", "wrapper_raise"}]
            entries = [event for event in local if event["kind"] == "wrapper_enter"]
            roots = [event for event in local if event["kind"] == "task_exit" and event.get("taskId") == "root"]
            if len(entries) != 1 or len(wrappers) != 1 or len(roots) != 1:
                validator.error(
                    "retirement requires actual entry, wrapper exit and root-task exit for every native request"
                )
            elif any(boundary(entries[0]) > boundary(end) for end in (wrappers[0], roots[0])):
                validator.error("retirement completion precedes actual invocation entry")
            if validator.number(records[request_id].get("settledAtMs"), "invocation.settledAtMs") < max(
                (event["atMs"] for event in local), default=0
            ):
                validator.error("invocation retirement precedes its last actual activity")
            for start_kind, end_kind, identity in _WORK_INTERVALS:
                started = [event for event in local if event["kind"] == start_kind]
                ended = [event for event in local if event["kind"] == end_kind]
                identifiers = {
                    validator.string(event.get(identity), f"{start_kind}.{identity}") for event in started + ended
                }
                for identifier in identifiers:
                    starts = [event for event in started if event.get(identity) == identifier]
                    ends = [event for event in ended if event.get(identity) == identifier]
                    rejected = (
                        start_kind in _CALL_METHODS
                        and len(starts) == 1
                        and starts[0].get("outcome") == "REJECTED"
                        and _has_matching_sdk_request(
                            starts[0], _CALL_METHODS[start_kind], "REJECTED", evidence["sdkRequests"]
                        )
                    )
                    if len(starts) != 1 or (len(ends) != 1 and not (rejected and not ends)):
                        validator.error(f"retirement lacks the unique {start_kind}/{end_kind} completion pair")
                        continue
                    if not ends:
                        continue
                    start, end = starts[0], ends[0]
                    if boundary(start) > boundary(end):
                        validator.error("retirement reverses an observed work interval")

    except (AttributeError, KeyError, TypeError, ValueError, OverflowError) as error:
        validator.error(f"malformed independent retirement evidence: {error}")
    return validator.errors
