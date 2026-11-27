# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Normalize observed SDK/service evidence for the pinned shared validator.

This module runs only in the driver. It never fills in a missing SDK boundary or
uses a fixture's expected answer as an observed result. Raw source objects remain
in the case artifact directory and S3 ledger.
"""

from __future__ import annotations

import copy
import json
import math
from collections import Counter, defaultdict
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, TypeGuard

from aws_durable_execution_conformance_tests.lmi.cloud import Cloud
from aws_durable_execution_conformance_tests.lmi.coordinator import ScenarioContext
from aws_durable_execution_conformance_tests.lmi.observations import (
    is_platform_timeout,
    is_undispatched_rejection,
    service_error_type,
    service_native_intervals,
    suspension_resumptions,
)
from aws_durable_execution_conformance_tests.lmi.observations import (
    timestamp_ms as _milliseconds,
)
from aws_durable_execution_conformance_tests.lmi.rules.common import rejected_call_settled
from aws_durable_execution_conformance_tests.lmi.rules.controls import invoke_rejection_proven
from aws_durable_execution_conformance_tests.lmi.rules.cutoff import active_retry_interval, requests_after_cutoff
from aws_durable_execution_conformance_tests.lmi.rules.values import (
    CUTOFF_ACTIVE_STATUSES,
    _decimal_identity,
    _decode_json_wire,
    _json_result_fingerprint,
    is_json_integer,
    same_checkpoint,
)
from aws_durable_execution_conformance_tests.lmi.runtime import FixtureBinding
from aws_durable_execution_conformance_tests.validate import inherit_wait_for_callback_event_names

SOURCE_PROTOCOL_VERSION = 2
_TERMINAL_EVENTS = {
    "SUCCEEDED": "ExecutionSucceeded",
    "FAILED": "ExecutionFailed",
    "TIMED_OUT": "ExecutionTimedOut",
    "STOPPED": "ExecutionStopped",
}
DEADLINE_REQUIREMENTS = {"deadline-during-step": "lmi-lifecycle-3", "peer-recovery": "lmi-lifecycle-4"}
_ACTIVE_FAULTS = set(DEADLINE_REQUIREMENTS)
_STANDARD_REPLAY_EFFECTS = {"success": 1, "failure": 1, "retry": 2}
_WORK_IDENTITIES = {
    "task_start": "taskId",
    "task_exit": "taskId",
    "observed_task_start": "taskId",
    "observed_task_exit": "taskId",
    "io_start": "taskId",
    "io_exit": "taskId",
    "checkpoint_call": "callId",
    "checkpoint_exit": "callId",
    "poll_call": "callId",
    "poll_exit": "callId",
}
_OPERATION_TYPES = {
    "Step": "STEP",
    "Wait": "WAIT",
    "Callback": "CALLBACK",
    "Context": "CONTEXT",
    "ChainedInvoke": "CHAINED_INVOKE",
}


def _valid_timestamp(value: Any) -> TypeGuard[int | float]:
    try:
        return type(value) in (int, float) and math.isfinite(value) and value >= 0
    except OverflowError:
        return False


def _display_json(value: Any) -> Any:
    if isinstance(value, Decimal):
        sign, coefficient, exponent = _decimal_identity(value)
        if exponent >= 0 and len(coefficient) + exponent <= 4096:
            return int(value)
        approximate = float(value)
        if math.isfinite(approximate):
            return approximate
        return f"{'-' if sign else ''}{coefficient}e{exponent}"
    if isinstance(value, list):
        return [_display_json(item) for item in value]
    if isinstance(value, dict):
        return {key: _display_json(item) for key, item in value.items()}
    return value


def _decoded(value: Any) -> Any:
    if isinstance(value, Mapping) and "Payload" in value:
        if value.get("Truncated") is True:
            raise ValueError("Truncated service data cannot establish checkpoint equality")
        value = value["Payload"]
    if isinstance(value, str):
        try:
            return _display_json(_decode_json_wire(value))
        except ValueError:
            # Callback payloads may be plain UTF-8, rather than JSON results.
            return value
    return value


def _error(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    if value.get("Truncated") is True:
        raise ValueError("Truncated service data cannot establish checkpoint equality")
    if "Payload" in value:
        value = value["Payload"]
    if not isinstance(value, Mapping):
        return None
    return {
        "type": value.get("ErrorType", value.get("type")),
        "message": value.get("ErrorMessage", value.get("message")),
    }


def _serialized_encoding(payload: str) -> str:
    try:
        _decode_json_wire(payload)
    except ValueError:
        return "raw"
    return "json"


def _service_event_time(event: Mapping[str, Any]) -> float | None:
    try:
        occurred = _milliseconds(event.get("EventTimestamp"))
    except ValueError, OverflowError:
        return None
    return occurred if _valid_timestamp(occurred) else None


def _retain_service_payload(
    target: dict[str, Any],
    key: str,
    payload: Any,
    owner: str,
    errors: list[str],
    *,
    retain_encoding: bool = False,
) -> None:
    try:
        if isinstance(payload, Mapping) and payload.get("Truncated") is True:
            raise ValueError("Truncated service data cannot establish checkpoint equality")
        value = _error(payload) if key == "error" else _decoded(payload)
    except ValueError as error:
        errors.append(f"{owner}: {error}")
        return
    if key == "error" and value is None:
        errors.append(f"{owner} has no structured service error")
    else:
        target[key] = value
        wire = payload.get("Payload") if isinstance(payload, Mapping) else payload
        if retain_encoding and key == "result" and isinstance(wire, str):
            target["resultEncoding"] = _serialized_encoding(wire)
            if target["resultEncoding"] == "json":
                target["resultFingerprint"] = _json_result_fingerprint(wire)


def _resources(value: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if "threads" in value:
        result["threads"] = [{**thread, "id": str(thread["id"])} for thread in value["threads"]]
    if "fds" in value:
        result["fds"] = value["fds"]
    if "rssBytes" in value or "rss" in value:
        result["rssBytes"] = value.get("rssBytes", value.get("rss"))
    return result


def _invocation_completions(history: Sequence[Mapping[str, Any]]) -> Iterator[Mapping[str, Any]]:
    for event in history:
        if event.get("EventType") == "InvocationCompleted" or "InvocationCompletedDetails" in event:
            details = event.get("InvocationCompletedDetails")
            yield details if isinstance(details, Mapping) else {}


def observation_exclusions(
    context: ScenarioContext, cloud: Cloud, ledger: Sequence[Mapping[str, Any]], cutoff_ms: float | None
) -> set[str]:
    excluded = requests_after_cutoff(context.attempts, cutoff_ms)
    if cutoff_ms is None:
        return excluded
    published: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in ledger:
        if isinstance(request := row.get("request"), str):
            published[request].append(row)
    for launch in cloud.invocations:
        arn = launch.get("arn")
        established = {
            entry["role"]
            for entry in context.attempts
            if entry.get("executionArn") == arn
            and entry.get("requestId") in launch.get("native_requests", ())
            and entry.get("attempt") == 1
        }
        if not arn or not {launch.get("role"), *launch.get("resume_roles", ())} <= established:
            continue
        for request, (start, _) in service_native_intervals(cloud.raw_histories.get(arn, [])).items():
            if start <= cutoff_ms or request in launch.get("native_requests", ()):
                continue
            rows = published[request]
            if all(
                row.get("phase") != "WRAPPER_ENTER"
                and row.get("marker") == launch.get("marker")
                and row.get("execution") == arn
                and row.get("run") == context.run_id
                and row.get("caseId") == context.scenario.case_id
                and row.get("commit") == cloud.manifest.get("commit")
                and _valid_timestamp(row.get("time"))
                and row["time"] * 1000 > cutoff_ms
                and (
                    row.get("phase") != "CONFORMANCE"
                    or (
                        isinstance(observation := row.get("observation"), Mapping)
                        and _valid_timestamp(observation.get("atMs"))
                        and observation["atMs"] > cutoff_ms
                    )
                )
                for row in rows
            ):
                excluded.add(request)
    return excluded


def _awaiting_bind(launch: Mapping[str, Any] | None, execution: Any) -> bool:
    """True when ``launch`` could still legitimately bind ``execution``.

    A launch ``_bind_requests`` already skipped as conflicting can never
    become bound, so it is never merely "not yet bound". Otherwise a launch
    awaits binding when it has no bound execution yet, or when it is already
    bound to this same execution (and is just missing this one request from
    ``native_requests``). A launch already bound to a *different* execution
    is a real ownership mismatch, not a binding gap.
    """
    return (
        launch is not None
        and not launch.get("launch_conflict")
        and isinstance(execution, str)
        and bool(execution)
        and (not launch.get("arn") or launch.get("arn") == execution)
    )


def ledger_seal_status(
    records: Sequence[Mapping[str, Any]],
    histories: Mapping[str, Sequence[Mapping[str, Any]]],
    launches: Sequence[Mapping[str, Any]],
    excluded: set[str],
    *,
    defer_unbound: bool = False,
) -> tuple[set[str], set[str], list[str]]:
    """Return known requests, missing seals/boundaries and invalid identities or seals.

    ``defer_unbound`` only applies to a sealed stream that is otherwise fully
    consistent with an owned launch (matching identity fields, no foreign
    service completion owner) but whose launch has not yet recorded this
    request (no ``arn`` yet, or ``arn`` matches but the request is absent
    from ``native_requests``). A budget-expiry refresh can cache such a
    stream before binding runs (see ``Cloud.refresh``); treating that as
    "missing" rather than an error avoids fencing a target for a gap that a
    later complete refresh resolves on its own. Every other mismatch (no
    matching launch, a launch already skipped as conflicting, a launch bound
    to a different execution, a foreign completion owner or a genuine
    identity mismatch) stays an error.
    """
    streams = defaultdict(list)
    errors = []
    for record in records:
        request = record.get("request")
        if not isinstance(request, str) or not request.strip():
            errors.append("A lifecycle ledger record has no native request identity")
        elif request not in excluded:
            streams[request].append(record)

    completion_owners = {}
    for service_arn, history in histories.items():
        for details in _invocation_completions(history):
            request = details.get("RequestId")
            if not isinstance(request, str) or not request.strip():
                errors.append(f"Service invocation completion for {service_arn} has no request ID")
            elif request not in excluded:
                if request in completion_owners:
                    errors.append(f"Native request {request} has multiple service invocation completions")
                completion_owners[request] = service_arn

    by_marker = {item.get("marker"): item for item in launches}
    known = set(streams) | set(completion_owners)
    missing = set()
    for request in sorted(known):
        stream = streams[request]
        wrappers = [record for record in stream if record.get("phase") == "WRAPPER_ENTER"]
        seals = [record for record in stream if record.get("phase") == "SEALED"]
        if len(wrappers) > 1 or len(seals) > 1:
            errors.append(f"Native request {request} has duplicate wrapper entries or ledger seals")
            continue
        if not seals:
            missing.add(request)
            continue
        if not wrappers:
            errors.append(f"Native request {request} has a ledger seal but no wrapper entry")
            continue
        wrapper = wrappers[0]
        marker = wrapper.get("marker")
        arn = wrapper.get("execution")
        launch = by_marker.get(marker) if isinstance(marker, str) and marker else None
        valid_arn = isinstance(arn, str) and bool(arn)
        identity_matches = not any(
            record.get(field) != wrapper.get(field)
            for record in stream
            for field in ("run", "commit", "caseId", "marker", "execution")
        )
        completion_consistent = completion_owners.get(request, arn) == arn
        not_yet_bound = (
            defer_unbound
            and valid_arn
            and identity_matches
            and completion_consistent
            and _awaiting_bind(launch, arn)
            and launch is not None
            and request not in launch.get("native_requests", ())
        )
        if not_yet_bound:
            missing.add(request)
            continue
        if (
            launch is None
            or not valid_arn
            or launch.get("arn") != arn
            or request not in launch.get("native_requests", ())
            or not completion_consistent
            or not identity_matches
        ):
            errors.append(f"Native request {request} has inconsistent ledger, launch or service ownership")
            continue
        seal = seals[0]
        ordinals = [
            int(sequence) for record in stream if is_json_integer(sequence := record.get("sequence")) and sequence >= 1
        ]
        if (
            not is_json_integer(seal.get("sourceProtocol"))
            or seal["sourceProtocol"] != SOURCE_PROTOCOL_VERSION
            or "observation" in seal
            or len(ordinals) != len(stream)
            or len(set(ordinals)) != len(ordinals)
            or max(ordinals) != len(ordinals)
            or seal["sequence"] != len(ordinals)
        ):
            errors.append(f"Native request {request} has an invalid or nonterminal ledger seal")
            continue
        high = seal.get("throughCapture")
        captures = [
            observation.get("captureSequence")
            if isinstance(observation := record.get("observation"), Mapping)
            else None
            for record in stream
            if record.get("phase") == "CONFORMANCE"
        ]
        if (
            not is_json_integer(high)
            or high < 0
            or len(captures) != high
            or any(not is_json_integer(capture) or not 1 <= capture <= high for capture in captures)
            or len(set(captures)) != high
        ):
            errors.append(f"Native request {request} has an incomplete capture sequence at its ledger seal")
    return known, missing, errors


def undispatched_requests(
    records: Sequence[Mapping[str, Any]],
    histories: Mapping[str, Sequence[Mapping[str, Any]]],
    launches: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    """The service proves these requests never reached a handler.

    A request qualifies only if it has no runtime trace of any phase (no
    ledger row, published or not, and no binding to a launch) and exactly one
    service-recorded completion, on an owned execution, that is an allowlisted
    platform rejection with a valid native interval. Such a request is exempt
    only from the physical-retirement seal requirement; it never satisfies
    behavior evidence.
    """
    published = {record.get("request") for record in records}
    bound = {request for launch in launches for request in launch.get("native_requests", ())}
    owned = {launch.get("arn") for launch in launches if launch.get("arn")}
    found: defaultdict[str, list[tuple[str, Mapping[str, Any]]]] = defaultdict(list)
    for arn, history in histories.items():
        for event in history:
            # Count leniently, the same way _invocation_completions matches
            # events, so a malformed extra completion for the same request
            # cannot be hidden behind a technicality and still disqualify it.
            if event.get("EventType") != "InvocationCompleted" and "InvocationCompletedDetails" not in event:
                continue
            details = event.get("InvocationCompletedDetails")
            if isinstance(details, Mapping) and isinstance(details.get("RequestId"), str):
                found[details["RequestId"]].append((arn, event))
    proven: dict[str, dict[str, Any]] = {}
    for request, completions in found.items():
        if len(completions) != 1 or request in published or request in bound:
            continue
        ((arn, event),) = completions
        details = event["InvocationCompletedDetails"]
        interval = service_native_intervals([event]).get(request)  # also requires EventType == InvocationCompleted
        event_id = event.get("EventId")
        if (
            arn in owned
            and interval is not None
            and is_undispatched_rejection(details)
            and type(event_id) is int
            and event_id >= 1
        ):
            proven[request] = {
                "executionArn": arn,
                "serviceErrorType": service_error_type(details),
                "sourceEventId": event_id,
                "startedAtMs": interval[0],
                "settledAtMs": interval[1],
            }
    return proven


def _terminal_end(history: Sequence[Mapping[str, Any]], result: Mapping[str, Any]) -> float | None:
    end = result.get("EndTimestamp")
    if end is None:
        status = result.get("Status")
        kind = _TERMINAL_EVENTS.get(status) if isinstance(status, str) else None
        terminals = [event for event in history if kind is not None and event.get("EventType") == kind]
        end = terminals[0].get("EventTimestamp") if len(terminals) == 1 else None
    try:
        value = _milliseconds(end)
    except ValueError, OverflowError:
        return None
    return value if _valid_timestamp(value) else None


def deadline_cutoff_results(context: ScenarioContext, cloud: Cloud) -> Mapping[str, Mapping[str, Any]]:
    if DEADLINE_REQUIREMENTS.get(context.scenario.scenario) != context.scenario.requirement_id:
        return {}
    witnesses = getattr(cloud, "cutoff_results", {})
    return {
        item["arn"]: witnesses[item["arn"]]
        for item in cloud.invocations
        if item.get("role") == "victim" and item.get("arn") in witnesses
    }


def _active_status_at_cutoff(
    history: Sequence[Mapping[str, Any]], cutoff_ms: float, witness_status: str | None, witnessed_at: Any = None
) -> str | None:
    """Project pause transitions and require a matching episode for a paused witness."""
    transitions = []
    pauses: list[tuple[float, float]] = []
    for event in history:
        kind = event.get("EventType")
        if kind not in ("ExecutionPaused", "ExecutionResumed"):
            continue
        at = _service_event_time(event)
        if at is None:
            return None
        if kind == "ExecutionPaused":
            details = event.get("ExecutionPausedDetails")
            requested = details.get("PauseRequestTimestamp") if isinstance(details, Mapping) else None
            try:
                request_ms = _milliseconds(requested)
            except ValueError, OverflowError:
                return None
            if not _valid_timestamp(request_ms) or request_ms > at:
                return None
            pauses.append((request_ms, at))
            if request_ms <= cutoff_ms < at:
                # A request is accepted before processing; no pause state is proven in that gap.
                return None
        transitions.append((at, "PAUSED" if kind == "ExecutionPaused" else "RUNNING"))
    if witness_status in ("PAUSING", "PAUSED"):
        if not _valid_timestamp(witnessed_at):
            return None
        observed_transitions = sorted(
            ((at, state) for at, state in transitions if at <= witnessed_at),
            key=lambda item: item[0],
        )
        last_state = observed_transitions[-1][1] if observed_transitions else "RUNNING"
        if witness_status == "PAUSING":
            matches = [pause for pause in pauses if pause[0] <= witnessed_at < pause[1]]
            if len(matches) != 1 or last_state != "RUNNING":
                return None
        elif last_state != "PAUSED":
            return None
    state = "RUNNING"
    for at, status in sorted(transitions, key=lambda item: item[0]):
        if at <= cutoff_ms:
            state = status
    return state


def _terminal_history_covers(
    history: Sequence[Mapping[str, Any]],
    result: Mapping[str, Any],
    requests: set[str],
    cutoff_ms: float,
    cutoff_result: Mapping[str, Any] | None,
) -> bool:
    status = result.get("Status")
    terminal_kind = _TERMINAL_EVENTS.get(status) if isinstance(status, str) else None
    if terminal_kind is None or not history or history[0].get("EventType") != "ExecutionStarted":
        return False
    if any(
        type(event.get("EventId")) is not int or event["EventId"] != number for number, event in enumerate(history, 1)
    ):
        return False
    terminal = [event for event in history if event.get("EventType") in _TERMINAL_EVENTS.values()]
    if len(terminal) != 1 or terminal[0]["EventType"] != terminal_kind:
        return False
    end = _terminal_end(history, result)
    if end is None:
        return False
    if cutoff_result is None:
        if end > cutoff_ms:
            return False
    else:
        witnessed = cutoff_result.get("Status")
        if cutoff_result.get("collectionError") or not (
            (
                witnessed in CUTOFF_ACTIVE_STATUSES
                and end > cutoff_ms
                and _active_status_at_cutoff(history, cutoff_ms, witnessed, cutoff_result.get("observedAtMs"))
                is not None
            )
            or witnessed == status
        ):
            return False
    completed = [details.get("RequestId") for details in _invocation_completions(history)]
    if any(not isinstance(request, str) or not request.strip() for request in completed):
        return False
    return len(set(completed)) == len(completed) and requests <= set(completed)


def terminal_history_pending(
    cloud: Cloud,
    ledger: Sequence[Mapping[str, Any]],
    cutoff_ms: float,
    excluded: set[str],
    cutoff_results: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[str]:
    if not cloud.invocations:
        return ["No admitted execution history was collected"]
    pending = []
    for invocation in cloud.invocations:
        arn = invocation.get("arn")
        requests = set(invocation.get("native_requests", []))
        requests.update(
            record["request"]
            for record in ledger
            if record.get("execution") == arn and isinstance(record.get("request"), str)
        )
        requests.difference_update(excluded)
        witness = cutoff_results.get(arn) if cutoff_results is not None else None
        if witness is not None and witness.get("collectionError"):
            pending.append(f"Could not establish the post-cutoff observation stop for {arn}")
        elif arn not in cloud.terminal_histories or not _terminal_history_covers(
            cloud.raw_histories.get(arn, []),
            cloud.execution_results.get(arn, {}),
            requests,
            cutoff_ms,
            witness,
        ):
            pending.append(
                f"Terminal service history does not cover the collection window for {arn or 'an unresolved launch'}"
            )
    return pending


def retirement_activity_after_cutoff(cloud: Cloud, cutoff_ms: float) -> bool:
    for record in cloud.events.values():
        observation = record.get("observation")
        if record.get("phase") != "CONFORMANCE" or not isinstance(observation, Mapping):
            continue
        at_ms = observation.get("atMs")
        if _valid_timestamp(at_ms) and at_ms > cutoff_ms:
            return True
    for history in cloud.raw_histories.values():
        for details in _invocation_completions(history):
            try:
                if _milliseconds(details.get("EndTimestamp")) > cutoff_ms:
                    return True
            except ValueError, OverflowError:
                continue
    return False


def _invalid_work_identity(event: Mapping[str, Any]) -> str | None:
    kind = event.get("kind")
    key = _WORK_IDENTITIES.get(kind) if isinstance(kind, str) else None
    if key is not None and (not isinstance(value := event.get(key), str) or not value.strip()):
        return key
    return None


def owned_observation(raw: Mapping[str, Any], observation: Mapping[str, Any]) -> dict[str, Any]:
    identity = {
        "requestId": raw.get("request"),
        "ownerRequestId": raw.get("request"),
        "executionArn": raw.get("execution"),
    }
    crossed = [key for key, value in identity.items() if key in observation and observation[key] != value]
    invalid_id = _invalid_work_identity(observation)
    if invalid_id is not None:
        message = (
            f"Raw CONFORMANCE record for {raw.get('request')} at sequence "
            f"{raw.get('sequence')} has invalid {invalid_id}"
        )
    elif crossed:
        message = f"Hook observation disagrees with ledger ownership: {', '.join(crossed)}"
    else:
        return {**observation, **identity}
    published_at = raw.get("time")
    return {
        **identity,
        "kind": "observation_error",
        "atMs": published_at * 1000 if _valid_timestamp(published_at) else 0,
        "message": message,
    }


@dataclass(frozen=True)
class CaseSources:
    context: ScenarioContext
    cloud: Cloud
    raw: tuple[Mapping[str, Any], ...]
    expected_outcome: Callable[[Mapping[str, Any], str], Mapping[str, Any]]
    worker_identity: Callable[[Mapping[str, Any]], str]
    worker_identity_key: str = "process"

    @classmethod
    def capture(
        cls, context: ScenarioContext, cloud: Cloud, *, raw: tuple[Mapping[str, Any], ...], **policies
    ) -> CaseSources:
        return cls(context, cloud, raw, **policies)

    def observations(self, *, pending: list[str] | None = None) -> tuple[list[dict[str, Any]], list[str]]:
        events: list[dict[str, Any]] = []
        errors: list[str] = []
        by_request: dict[str, list[int]] = defaultdict(list)
        sealed_requests: set[str] = set()
        for raw in self.raw:
            request = raw.get("request")
            if (
                raw.get("run") != self.context.run_id
                or raw.get("commit") != self.cloud.manifest["commit"]
                or raw.get("caseId") != self.context.scenario.case_id
            ):
                errors.append("Raw evidence does not belong to the admitted run/artifact/case")
                continue
            sequence = raw.get("sequence")
            if not isinstance(request, str) or not is_json_integer(sequence) or sequence < 1:
                errors.append("Raw evidence has no native request identity or source sequence")
                continue
            sequence = int(sequence)
            by_request[request].append(sequence)
            if raw.get("phase") == "SEALED":
                sealed_requests.add(request)
            # Collection reads the ledger with control validation disabled, and
            # only CONFORMANCE records become observations. A raw control
            # failure first seen here would otherwise leave complete evidence
            # with full watermarks despite a recorded control fault.
            if raw.get("phase") == "CONTROL_ERROR":
                errors.append(
                    f"Raw control failure for {raw.get('gate', 'unnamed control')}: {raw.get('error', 'unreported')}"
                )
            if raw.get("phase") != "CONFORMANCE":
                continue
            observation = raw.get("observation")
            if not isinstance(observation, Mapping):
                errors.append(
                    f"Raw CONFORMANCE record for {request} at sequence {sequence} lacks an observation object"
                )
                continue
            event = owned_observation(raw, observation)
            kind = event.get("kind")
            if not isinstance(kind, str) or not kind.strip() or not _valid_timestamp(event.get("atMs")):
                errors.append(f"Raw CONFORMANCE record for {request} at sequence {sequence} has invalid kind or atMs")
                continue
            publication_time = raw.get("time")
            if not _valid_timestamp(publication_time):
                errors.append(
                    f"Raw CONFORMANCE record for {request} at sequence {sequence} has invalid publication time"
                )
                continue
            event.update(sourceSequence=sequence, observedAtMs=publication_time * 1000)
            if "captureSequence" in event:
                capture = event["captureSequence"]
                if is_json_integer(capture) and capture > 0:
                    event["captureSequence"] = int(capture)
                else:
                    errors.append(
                        f"Raw CONFORMANCE record for {request} at sequence {sequence} has invalid capture sequence"
                    )
                    del event["captureSequence"]
            if "resources" in event:
                try:
                    event["resources"] = _resources(event["resources"])
                except KeyError, TypeError:
                    errors.append(f"Raw CONFORMANCE record for {request} at sequence {sequence} has invalid resources")
                    del event["resources"]
            if event.get("kind") == "wrapper_raise" and event.get("outcome") == "THREW":
                event["sourceOutcome"] = "THREW"
                event["outcome"] = "FAILED"  # Actual exception, not an inferred platform timeout.
            if event.get("kind") in {
                "observation_error",
                "control_error",
            }:
                errors.append(str(event.get("message", event.get("error", event["kind"]))))
            events.append(event)
        for request, sequences in by_request.items():
            if len(set(sequences)) != len(sequences):
                errors.append(f"Raw source publication contains duplicate sequence numbers for {request}")
            elif min(sequences) != 1 or max(sequences) != len(sequences):
                destination = errors if pending is None or request in sealed_requests else pending
                destination.append(f"Raw source publication is incomplete for {request}")
        return events, errors

    def histories(
        self, *, pending: list[str] | None = None, cutoff_ms: float | None = None, waking_ids: set[str] | None = None
    ) -> tuple[list[dict[str, Any]], dict[str, Any], list[str]]:
        """Normalize service history from service events alone.

        SDK checkpoint observations are the other side of the replay
        comparison, so none of the history's identity or output may come from
        them, or a corrupted checkpoint would compare equal to itself. An
        identity field the service omits, as for an unnamed root operation, is
        null.
        """
        histories: list[dict[str, Any]] = []
        expected: dict[str, Any] = {}
        errors: list[str] = []
        for item in self.cloud.invocations:
            arn = item.get("arn")
            if not arn or arn not in self.cloud.raw_histories or arn not in self.cloud.execution_results:
                (errors if pending is None else pending).append(
                    "An admitted execution is missing actual service history or state"
                )
                continue
            operations: dict[str, dict[str, Any]] = {}
            for raw in self.cloud.raw_histories[arn]:
                event_type = raw.get("EventType", "")
                kind = next(
                    (value for prefix, value in _OPERATION_TYPES.items() if event_type.startswith(prefix)),
                    None,
                )
                identity = raw.get("Id")
                if kind is None or not isinstance(identity, str):
                    continue
                if cutoff_ms is not None:
                    occurred = _service_event_time(raw)
                    if occurred is None:
                        (pending if pending is not None else errors).append(
                            f"Service operation {identity} has no proven event time in the observation window"
                        )
                        continue
                    if occurred > cutoff_ms or (
                        waking_ids is not None
                        and occurred == cutoff_ms
                        and (identity not in waking_ids or not event_type.endswith("Succeeded"))
                    ):
                        continue
                operation = operations.setdefault(
                    identity,
                    {
                        "id": identity,
                        "type": kind,
                        "name": raw.get("Name"),
                        "parentId": raw.get("ParentId"),
                        "status": "STARTED",
                    },
                )
                if "Name" in raw:
                    operation["name"] = raw["Name"]
                if "ParentId" in raw:
                    operation["parentId"] = raw["ParentId"]
                details = raw.get(event_type + "Details", {})
                if event_type.endswith("Succeeded"):
                    operation["status"] = "SUCCEEDED"
                    operation.pop("error", None)
                    # SDK snapshots are the other side of the replay comparison,
                    # so they cannot supply missing independent service output.
                    stored = details.get("Result")
                    if "Result" in details and (
                        not isinstance(stored, Mapping) or "Payload" in stored or stored.get("Truncated") is True
                    ):
                        _retain_service_payload(
                            operation,
                            "result",
                            stored,
                            f"Operation {identity}",
                            errors,
                            retain_encoding=self.context.scenario.scenario == "suspension-retry",
                        )
                    elif kind == "WAIT":
                        # WaitSucceeded has no Result field in the service model.
                        operation["result"] = None
                    # The suspension check orders a wait or callback's service
                    # completion between the suspending return and the resume.
                    if kind in ("WAIT", "CALLBACK") and (completed_at := _service_event_time(raw)) is not None:
                        operation["endedAtMs"] = completed_at
                elif event_type == "StepFailed" and "NextAttemptDelaySeconds" in details.get("RetryDetails", {}):
                    operation["status"] = "STARTED"
                elif event_type.endswith("Failed"):
                    operation["status"] = "FAILED"
                    if details.get("Error") is None:
                        errors.append(f"Service history is missing the error for operation {identity}")
                    else:
                        _retain_service_payload(operation, "error", details["Error"], f"Operation {identity}", errors)
                elif event_type.endswith(("TimedOut", "Stopped", "Cancelled")):
                    operation["status"] = next(
                        status
                        for suffix, status in (
                            ("TimedOut", "TIMED_OUT"),
                            ("Stopped", "STOPPED"),
                            ("Cancelled", "CANCELLED"),
                        )
                        if event_type.endswith(suffix)
                    )
            result = self.cloud.execution_results[arn]
            ended = _terminal_end(self.cloud.raw_histories[arn], result)
            cutoff_result = deadline_cutoff_results(self.context, self.cloud).get(arn)
            witness_status = cutoff_result.get("Status") if cutoff_result is not None else None
            status_at_read = result.get("Status")
            if cutoff_ms is not None and (
                status_at_read in CUTOFF_ACTIVE_STATUSES
                or (
                    status_at_read in _TERMINAL_EVENTS
                    and (
                        (ended is not None and ended > cutoff_ms)
                        or (cutoff_result is not None and (ended is None or witness_status in CUTOFF_ACTIVE_STATUSES))
                    )
                )
            ):
                result = {key: value for key, value in result.items() if key not in ("Status", "Result", "Error")}
                result["Status"] = _active_status_at_cutoff(
                    self.cloud.raw_histories[arn],
                    cutoff_ms,
                    witness_status or status_at_read,
                    cutoff_result.get("observedAtMs") if cutoff_result is not None else None,
                )
                if result["Status"] is None:
                    (pending if pending is not None else errors).append(
                        f"Service history does not establish the pause state at the observation cutoff for {arn}"
                    )
            started = result.get("StartTimestamp")
            if started is None:
                started = next(
                    (
                        event.get("EventTimestamp")
                        for event in self.cloud.raw_histories[arn]
                        if event.get("EventType") == "ExecutionStarted"
                    ),
                    None,
                )
            history: dict[str, Any] = {
                "executionArn": arn,
                "ownerCaseId": self.context.scenario.case_id,
                "status": result.get("Status"),
                "operations": list(operations.values()),
            }
            if started is not None:
                history["startedAtMs"] = _milliseconds(started)
            expectation = dict(self.expected_outcome(item, self.context.scenario.scenario))
            if "Result" in result:
                _retain_service_payload(history, "result", result["Result"], f"Execution {arn}", errors)
                raw = result["Result"]
                wire = raw.get("Payload") if isinstance(raw, Mapping) else raw
                if "result" in history and isinstance(wire, str) and _serialized_encoding(wire) == "json":
                    history["resultFingerprint"] = _json_result_fingerprint(wire)
            if result.get("Error") is not None:
                _retain_service_payload(history, "error", result["Error"], f"Execution {arn}", errors)
            histories.append(history)
            expected[arn] = expectation
        return histories, expected, errors

    def attempts(
        self,
        events: list[dict[str, Any]],
        *,
        pending: list[str] | None = None,
        cutoff_ms: float | None = None,
        excluded: set[str] | None = None,
        sealed: set[str] | None = None,
        undispatched: Collection[str] = (),
    ) -> tuple[list[dict[str, Any]], dict[str, str], list[str]]:
        attempts: list[dict[str, Any]] = []
        selected: dict[str, str] = {}
        errors: list[str] = []
        journal = {item.get("requestId"): item for item in self.context.attempts}
        victim = next(
            (entry for entry in journal.values() if entry.get("role") == "victim" and entry.get("attempt") == 1),
            {},
        )
        raw_by_request: dict[str, Mapping[str, Any]] = {}
        for raw in self.raw:
            if raw.get("phase") != "WRAPPER_ENTER":
                continue
            request = raw.get("request")
            if (
                not isinstance(request, str)
                or not request.strip()
                or not all(
                    isinstance(raw.get(key), str) and raw[key]
                    for key in ("marker", "execution", "environment", "process", self.worker_identity_key)
                )
                or not _valid_timestamp(raw.get("time"))
            ):
                errors.append(f"Invalid WRAPPER_ENTER identity or time at sequence {raw.get('sequence')}")
                continue
            raw_by_request[request] = raw
        # The ledger alone cannot reveal a native request that never
        # published. Without this, a retry whose original left no stream is
        # bound as the original, and the original goes unaudited.
        unpublished: dict[str, str | None] = {}
        for arn in dict.fromkeys(item.get("arn") for item in self.cloud.invocations):
            for service_event in self.cloud.raw_histories.get(arn, []) if arn else []:
                details = service_event.get("InvocationCompletedDetails")
                native = details.get("RequestId") if isinstance(details, Mapping) else None
                if (
                    isinstance(native, str)
                    and native not in raw_by_request
                    and native not in undispatched
                    and (excluded is None or native not in excluded)
                ):
                    unpublished[native] = service_error_type(details)
        for native, error_type in unpublished.items():
            reported = f" ({error_type})" if error_type else ""
            (errors if pending is None else pending).append(
                f"Service-recorded native request {native}{reported} has no lifecycle ledger stream"
            )
        for request, raw in raw_by_request.items():
            item = next(
                (item for item in self.cloud.invocations if item["marker"] == raw["marker"]),
                None,
            )
            if item is None or request not in journal or request not in self.cloud.request_roles:
                message = f"Native request {request} was not bound to an original launch"
                if (
                    _awaiting_bind(item, raw.get("execution"))
                    and pending is not None
                    and getattr(self.cloud, "binding_deferred", False) is True
                ):
                    # A budget-expiry refresh intentionally skips binding from a
                    # partial read (binding early could bind a retry as the
                    # original). While observation is still ongoing, that gap is
                    # incomplete evidence, not a collection error; a later
                    # complete refresh binds and drains the request. An unowned
                    # marker (no launch at all, e.g. one `Cloud.start` removed
                    # after a service rejection), a launch `_bind_requests`
                    # already skipped as conflicting, or a launch already bound
                    # to a *different* execution are never just "not yet bound" -
                    # those stay errors regardless of the deferral.
                    pending.append(message)
                else:
                    errors.append(message)
                continue
            owned = [event for event in events if event.get("requestId") == request]
            boundaries = [event for event in owned if event.get("kind") in {"wrapper_return", "wrapper_raise"}]
            deadline = [event for event in owned if event.get("kind") == "runtime_deadline"]
            observed_start = min([raw["time"] * 1000, *(event["atMs"] for event in owned)])
            started_at = observed_start
            start_source = "fixture"
            timeout = None
            native_failed = False
            native_end = None
            active = False
            history = self.cloud.raw_histories.get(raw["execution"], [])
            for service_event in history:
                details = service_event.get("InvocationCompletedDetails", {})
                if not isinstance(details, Mapping) or details.get("RequestId") != request:
                    continue
                # Fixture setup occurs after native admission. Only this
                # request's platform start establishes its full timeout window;
                # the durable execution start may belong to an earlier replay.
                try:
                    native_start = _milliseconds(details.get("StartTimestamp"))
                except ValueError, OverflowError:
                    native_start = None
                if native_start is None or not math.isfinite(native_start) or not 0 <= native_start <= observed_start:
                    errors.append(
                        f"Native request {request} has an invalid or missing InvocationCompletedDetails.StartTimestamp"
                    )
                else:
                    started_at = native_start
                    start_source = "platform"
                # The service writes the history event after the native invocation exits.
                try:
                    native_end = _milliseconds(details.get("EndTimestamp"))
                except ValueError, OverflowError:
                    native_end = None
                if native_end is None or not math.isfinite(native_end) or native_end < started_at:
                    errors.append(
                        f"Native request {request} has an invalid or missing InvocationCompletedDetails.EndTimestamp"
                    )
                    native_end = None
                    continue
                if cutoff_ms is not None and native_end > cutoff_ms:
                    proof = journal[request]
                    active = (
                        sealed is not None
                        and request in sealed
                        and started_at == proof.get("nativeStartedAtMs")
                        and native_end == proof.get("nativeEndedAtMs")
                        and service_native_intervals(history).get(request) == (started_at, native_end)
                        and active_retry_interval(proof, cutoff_ms, victim.get("executionArn"))
                    )
                    native_end = None
                    continue
                native_error = details.get("Error")
                if native_error is not None:
                    if isinstance(native_error, Mapping):
                        native_failed = True
                    else:
                        errors.append(f"Native request {request} has invalid InvocationCompletedDetails.Error")
                if is_platform_timeout(details):
                    timeout = {
                        "kind": "platform_timeout",
                        "requestId": request,
                        "ownerRequestId": request,
                        "executionArn": raw["execution"],
                        "atMs": native_end,
                        "source": "platform",
                        "sourceEventId": service_event.get("EventId"),
                    }
            if timeout is not None:
                events.append(timeout)
            role = self.cloud.request_roles[request]
            attempt: dict[str, Any] = {
                "requestId": request,
                "executionArn": raw["execution"],
                "environmentId": raw["environment"],
                "processId": raw["process"],
                "workerId": self.worker_identity(raw),
                "pid": raw.get("pid"),
                "role": role,
                "attempt": journal[request]["attempt"],
                "marker": raw["marker"],
                "fixture": item.get("payload", {}).get("scenario"),
                "startedAtMs": started_at,
                "startedAtMsSource": start_source,
            }
            if boundaries and native_end is not None:
                wrapper_outcome = boundaries[-1].get("outcome")
                if isinstance(wrapper_outcome, str):
                    attempt.update(outcome=wrapper_outcome, endedAtMs=native_end)
                else:
                    errors.append(f"Native request {request} has no valid wrapper outcome")
            if native_failed and native_end is not None:
                attempt.update(outcome="FAILED", endedAtMs=native_end)
            if timeout is not None:
                attempt.update(outcome="TIMED_OUT", endedAtMs=timeout["atMs"])
            if "endedAtMs" in attempt:
                attempt["endedAtMsSource"] = "platform"
            if deadline:
                deadline_at = deadline[0].get("deadlineAtMs")
                if _valid_timestamp(deadline_at):
                    attempt["deadlineAtMs"] = deadline_at
                else:
                    errors.append(f"Native request {request} has no valid runtime deadline")
            # A replace-worker runtime reports which worker capacity it took
            # over. Without it the shared validator rejects every conforming
            # replacement recovery, since worker_identity only names the
            # current worker.
            replaced = [
                event["replacesWorkerId"]
                for event in owned
                if event.get("kind") == "worker_recovered" and event.get("replacesWorkerId") is not None
            ]
            if replaced:
                attempt["replacesWorkerId"] = replaced[0]
            if active:
                attempt["activeAtObservationEnd"] = True
            if "outcome" not in attempt and not active:
                (errors if pending is None else pending).append(
                    f"Native request {request} has no observed completion boundary"
                )
            if journal[request]["attempt"] == 1:
                selected[role] = request
            attempts.append(attempt)
        return attempts, selected, errors


def _causal_order(events: list[dict[str, Any]]) -> None:
    requests = {event["requestId"] for event in events}
    for request in requests:
        owned = sorted(
            (event for event in events if event["requestId"] == request),
            key=lambda event: (
                event["atMs"],
                event.get("captureSequence", event.get("sourceSequence", -1)),
            ),
        )
        for sequence, event in enumerate(owned, 1):
            event["sequence"] = sequence


def _sdk_requests(events: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """The SDK requests ``build_evidence`` would derive from ``events``, for an in-progress quiescence check.

    Pure and side-effect free: unlike the loop that fills ``evidence["sdkRequests"]``,
    it does not record invalid-outcome errors. Callers that already hold the
    built evidence should pass its ``sdkRequests`` directly instead of calling this.
    """
    return [
        {
            **event,
            "method": "CheckpointDurableExecution"
            if event["kind"] == "checkpoint_call"
            else "GetDurableExecutionState",
            "outcome": event.get("outcome", "DISPATCHED"),
        }
        for event in events
        if event.get("kind") in {"checkpoint_call", "poll_call"}
    ]


def _quiescent(request: str, events: list[dict[str, Any]], sdk_requests: Sequence[Mapping[str, Any]] = ()) -> bool:
    local = [event for event in events if event.get("requestId") == request]
    if any(_invalid_work_identity(event) for event in local):
        return False
    if not any(event.get("kind") in {"wrapper_return", "wrapper_raise"} for event in local):
        return False
    if not any(event.get("kind") == "task_exit" and event.get("taskId") == "root" for event in local):
        return False
    for start, end, key in (
        ("task_start", "task_exit", "taskId"),
        ("io_start", "io_exit", "taskId"),
        ("checkpoint_call", "checkpoint_exit", "callId"),
        ("poll_call", "poll_exit", "callId"),
    ):
        starts = [event for event in local if event.get("kind") == start]
        ends = [event for event in local if event.get("kind") == end]
        begun = Counter(event.get(key) for event in starts)
        ended = Counter(event.get(key) for event in ends)
        for identity in begun - ended:
            own_starts = [event for event in starts if event.get(key) == identity]
            own_ends = [event for event in ends if event.get(key) == identity]
            if not rejected_call_settled(start, own_starts, own_ends, sdk_requests):
                return False
    return not any(event.get("kind") in {"observation_error", "control_error"} for event in local)


ReplayEffectPolicy = Callable[[Sequence[Mapping[str, Any]]], tuple[Mapping[str, int], Collection[str]]]


def _replay_effect_policy(
    policy: ReplayEffectPolicy,
    bodies: Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, int], Collection[str]]:
    counts, missing = policy(tuple(copy.deepcopy(body) for body in bodies))
    if (
        not isinstance(counts, Mapping)
        or any(
            not isinstance(key, str) or not key.strip() or type(value) is not int or value < 0
            for key, value in counts.items()
        )
        or not isinstance(missing, Collection)
        or isinstance(missing, (str, bytes, Mapping))
        or any(not isinstance(label, str) or not label.strip() for label in missing)
    ):
        raise ValueError("Replay effect policy returned malformed expectations")
    return counts, missing


def _replay_effects(
    raw: Sequence[Mapping[str, Any]],
    execution: str | None,
    bodies: list[dict[str, Any]],
    names: Mapping[str, str | None],
    expected: Mapping[str, int],
    errors: list[str],
) -> list[dict[str, Any]]:
    # Hook observations carry durable operation IDs. Raw fixture BODY rows, if
    # published, additionally witness effect totals but do not identify a child
    # operation when multiple branches use the same local name.
    observed = Counter(body["operationId"] for body in bodies)
    raw_counts: Counter[tuple[str, str, int]] = Counter()
    for record in raw:
        if not execution or record.get("execution") != execution or record.get("phase") != "BODY":
            continue
        request, name, attempt = (record.get(key) for key in ("request", "operation", "attempt"))
        if not isinstance(request, str) or not isinstance(name, str) or not is_json_integer(attempt) or attempt < 1:
            errors.append("Raw replay side effect is missing a valid request, operation name or attempt")
            continue
        raw_counts[request, name, int(attempt)] += 1
    hook_counts = Counter((body["requestId"], body.get("operation"), body["attempt"]) for body in bodies)
    if raw_counts and raw_counts != hook_counts:
        errors.append(
            "Raw replay side effects do not match fixture body observations by request, operation and attempt"
        )
    return [
        {
            "operationId": identity,
            "count": observed[identity],
            "expectedCount": expected.get(name, 0) if name is not None else 0,
        }
        for identity, name in names.items()
    ]


def replay_evidence(
    events: list[dict[str, Any]],
    histories: list[dict[str, Any]],
    selected: Mapping[str, str],
    *,
    raw: Sequence[Mapping[str, Any]] = (),
    expected_effects: Mapping[str, int] | None = None,
    effect_policy: ReplayEffectPolicy | None = None,
    expected_before: list[dict[str, Any]] | None = None,
    errors: list[str] | None = None,
    partial: bool = False,
) -> dict[str, Any]:
    """Compare actual invocation-start checkpoints with independent service history."""
    resume_id = selected.get("resume")
    starts = [
        event
        for event in events
        if resume_id is not None and event.get("kind") == "invocation_start" and event.get("requestId") == resume_id
    ]
    first_resume = starts[0] if len(starts) == 1 else {}
    victim = next((e for e in events if e.get("requestId") == selected.get("victim")), {})
    # Copy the collected history: sharing the list object would make the
    # validator's replay.after/history comparison an identity check that no
    # inconsistent snapshot could ever fail.
    after: list[dict[str, Any]] = deepcopy(
        next(
            (h["operations"] for h in histories if h["executionArn"] == victim.get("executionArn")),
            [],
        )
    )
    encoded_operations = {item["id"]: item for item in after if "resultEncoding" in item}
    diagnostics = errors if errors is not None else []
    if len(starts) > 1:
        diagnostics.append(f"Replay request {resume_id} has multiple invocation-start snapshots")
    before = []
    checkpoints = first_resume.get("operations", [])
    resume_id = first_resume.get("requestId")
    if not isinstance(checkpoints, list):
        diagnostics.append(f"Replay request {resume_id} has a malformed operations snapshot")
        checkpoints = []
    for index, saved in enumerate(checkpoints):
        if (
            not isinstance(saved, Mapping)
            or any(
                not isinstance(saved.get(key), str) or not saved[key].strip()
                for key in ("operationId", "operationType", "status")
            )
            or any(saved.get(key) is not None and not isinstance(saved[key], str) for key in ("name", "parentId"))
        ):
            diagnostics.append(f"Replay request {resume_id} has a malformed checkpoint at index {index}")
            continue
        operation_type = saved["operationType"]
        status = saved["status"]
        if operation_type == "EXECUTION" or status not in ("SUCCEEDED", "FAILED"):
            continue
        operation = {
            "id": saved["operationId"],
            "name": saved.get("name"),
            "type": operation_type,
            "parentId": saved.get("parentId"),
            "status": status,
        }
        if status == "SUCCEEDED":
            if (serialized := saved.get("serializedResult")) is not None:
                try:
                    operation["result"] = _decoded(serialized)
                    if operation["id"] in encoded_operations and isinstance(serialized, str):
                        operation["resultEncoding"] = _serialized_encoding(serialized)
                        if (
                            operation["resultEncoding"] == "json"
                            and "resultFingerprint" in encoded_operations[operation["id"]]
                        ):
                            operation["resultFingerprint"] = _json_result_fingerprint(serialized)
                except ValueError as error:
                    diagnostics.append(f"Replay request {resume_id} checkpoint {index}: {error}")
            elif operation_type == "WAIT":
                operation["result"] = None
        elif saved.get("error") is not None:
            operation["error"] = saved["error"]
        before.append(operation)
    # `events` keeps raw collection order, which for a batch first read together
    # follows the S3 request-key listing rather than execution chronology. Order
    # the user-code records causally so a retry's stored failure cannot appear
    # after its later success and reject a conforming SDK.
    bodies = []
    for event in events:
        if event.get("kind") != "body":
            continue
        identity, attempt = event.get("operationId"), event.get("attempt")
        if (
            not isinstance(identity, str)
            or not identity
            or not isinstance(event.get("operation"), str)
            or not isinstance(event.get("requestId"), str)
            or not is_json_integer(attempt)
            or attempt < 1
        ):
            diagnostics.append("Replay body observation has an invalid request, operation identity or attempt")
            continue
        bodies.append({**event, "attempt": int(attempt)})
    bodies.sort(key=lambda event: (event.get("atMs", 0), event.get("sequence", 0)))
    executions = [
        {
            "requestId": e["requestId"],
            "operationId": e["operationId"],
            "attemptId": f"{e['requestId']}:{e['attempt']}",
        }
        for e in bodies
    ]
    retries: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for body in bodies:
        if body.get("operation") != "retry":
            continue
        ending = next(
            (
                e
                for e in events
                if e.get("kind") == "task_exit"
                and e.get("operationId") == body["operationId"]
                and e.get("requestId") == body["requestId"]
                and e.get("attempt") == body["attempt"]
            ),
            {},
        )
        retries[body["operationId"]].append(
            {
                "attemptId": f"{body['requestId']}:{body['attempt']}",
                "requestId": body["requestId"],
                **({"outcome": ending["outcome"]} if "outcome" in ending else {}),
            }
        )
    completed_before_resume = {item["id"] for item in before}
    completed_with_result = {item["id"] for item in after if item.get("status") == "SUCCEEDED" and "result" in item}
    selected_retry = next(
        (
            identity
            for identity, attempts in retries.items()
            if identity not in completed_before_resume
            and identity in completed_with_result
            and len(attempts) >= 2
            and attempts[0].get("outcome") == "FAILED"
            and attempts[-1].get("outcome") == "SUCCEEDED"
        ),
        next(iter(retries), None),
    )
    attempts = retries[selected_retry] if selected_retry is not None else []
    result: dict[str, Any] = {"before": before, "after": after, "operationExecutions": executions}
    if expected_before is not None:
        result["expectedBefore"] = expected_before
    if selected_retry is not None:
        result["retry"] = {"operationId": selected_retry, "attempts": attempts}
    if not partial or effect_policy is not None:
        expected = _STANDARD_REPLAY_EFFECTS if expected_effects is None else expected_effects
        names = {body["operationId"]: None for body in bodies}
        names.update({operation["id"]: operation.get("name") for operation in before})
        names.update(
            {
                operation["id"]: operation.get("name")
                for operation in after
                if operation["id"] in names or operation.get("name") in expected
            }
        )
        if partial and effect_policy is not None:
            for body in bodies:
                if names[body["operationId"]] is None:
                    names[body["operationId"]] = body["operation"]
        if not partial:
            service_operations = {row["id"]: row.get("name") for row in after}
            for body in bodies:
                if service_operations.get(body["operationId"]) != body["operation"]:
                    diagnostics.append(f"Replay body {body['operationId']} lacks a matching service operation")
        policy_counts: Mapping[str, int] = {}
        policy_missing: Collection[str] = ()
        if effect_policy is not None:
            try:
                policy_counts, policy_missing = _replay_effect_policy(effect_policy, bodies)
            except Exception as error:
                diagnostics.append(f"Replay effect policy: {error}")
        if not partial:
            missing = {name for name, count in expected.items() if count > 0 and name not in names.values()}
            missing.update(policy_missing)
            if len(attempts) < 2:
                missing.add("retry")
            if missing:
                result["missingExpectedEffects"] = sorted(missing)
        effects = _replay_effects(
            () if partial else raw, victim.get("executionArn"), bodies, names, expected, diagnostics
        )
        observed_ids = {body["operationId"] for body in bodies}
        excess = []
        for row in effects:
            identity = row["operationId"]
            if identity in observed_ids and identity in policy_counts:
                row["expectedCount"] = min(row["expectedCount"], policy_counts[identity])
                if row["count"] > row["expectedCount"]:
                    excess.append(row)
        if not partial:
            result["sideEffects"] = effects
        elif excess:
            result["sideEffects"] = excess
    return result


def assertion_events(events, scenario, selected):
    """Expose one scenario-owned task boundary while retaining every task fact.

    The shared behavior contract's task_exit is singular. Other real tasks stay
    in the envelope with their source kind, and are restored for the separate
    all-task retirement audit.
    """
    if scenario not in {
        "normal-return",
        "normal-failure",
        "deadline-during-step",
        "peer-recovery",
        "warm-reuse",
    }:
        return events
    result = []
    for event in events:
        item = dict(event)
        target = (
            "late-callback" if scenario == "warm-reuse" and event.get("requestId") == selected.get("victim") else "root"
        )
        if item["kind"] in {"task_start", "task_exit"} and item.get("taskId") != target:
            item["sourceKind"] = item["kind"]
            item["kind"] = "observed_" + item["kind"]
        result.append(item)
    return result


def _undispatched_launches(context: ScenarioContext, cloud: Cloud) -> list[dict[str, Any]] | None:
    records = []
    rejected_markers = set()
    for path in sorted((cloud.artifacts_dir / "invocations").glob("*.json")):
        record = json.loads(path.read_text())
        payload = record.get("payload", {})
        marker, function = payload.get("marker"), record.get("function")
        execution_name = record.get("durableExecutionName")
        if (
            payload.get("run") != context.run_id
            or payload.get("caseId") != context.scenario.case_id
            or not isinstance(marker, str)
            or not marker.strip()
            or path.name != f"{marker}.json"
            or function != context.profile.resource_id
            or not isinstance(execution_name, str)
            or not execution_name.strip()
        ):
            return None
        state = record.get("dispatchState")
        identity = {
            "marker": marker,
            "functionArn": function,
            "durableExecutionName": execution_name,
            "dispatchState": state,
        }
        if state == "PREPARED" and not any(key in record for key in ("arn", "executionArn", "requestId")):
            records.append(identity)
        elif invoke_rejection_proven(record):
            rejection = {**identity, "rejectionCode": record["rejectionCode"], "retryAttempts": record["retryAttempts"]}
            observed = next((attempt for attempt in context.attempts if attempt.get("attemptId") == marker), None)
            if observed is None or any(observed.get(key) != value for key, value in rejection.items()):
                return None
            rejected_markers.add(marker)
            records.append(rejection)
        else:
            return None
    if len(rejected_markers) != len(context.attempts) or any(
        not invoke_rejection_proven(attempt) or attempt.get("attemptId") not in rejected_markers
        for attempt in context.attempts
    ):
        return None
    return records


def _replay_history_before_resume(
    sources: CaseSources,
    attempts: list[dict[str, Any]],
    selected: Mapping[str, str],
    errors: list[str],
    waking_ids: set[str] | None = None,
) -> list[dict[str, Any]] | None:
    resume_id = selected.get("resume")
    if resume_id is None:
        return None
    resume = next((entry for entry in attempts if entry.get("requestId") == resume_id), {})
    arn = resume.get("executionArn")
    native = sources.cloud.raw_histories.get(arn, []) if isinstance(arn, str) else []
    interval = service_native_intervals(native).get(resume_id) if resume_id is not None else None
    if not isinstance(arn, str) or interval is None or interval[0] != resume.get("startedAtMs"):
        errors.append("Replay history has no service-confirmed native resume start")
        return None
    native_start = interval[0]
    for event in native:
        kind = event.get("EventType")
        if not isinstance(kind, str) or not kind.startswith(tuple(_OPERATION_TYPES)):
            continue
        try:
            occurred = _milliseconds(event.get("EventTimestamp"))
        except ValueError, OverflowError:
            occurred = -1
        if not _valid_timestamp(occurred):
            errors.append("Replay service history has an operation with no valid event time")
            return None
    histories, _, projection_errors = sources.histories(
        cutoff_ms=native_start,
        waking_ids=waking_ids if waking_ids is not None else set(),
    )
    errors.extend(projection_errors)
    return [
        operation
        for history in histories
        if history["executionArn"] == arn
        for operation in history["operations"]
        if operation["status"] in ("SUCCEEDED", "FAILED")
    ]


def _public_suspension_histories(
    cloud: Cloud, ledger: Sequence[Mapping[str, Any]], victim_arn: str | None
) -> dict[str, set[str]]:
    if victim_arn is None:
        return {}
    mode = getattr(cloud, "suspension_mode", "callback")
    history = [dict(event) for event in cloud.raw_histories.get(victim_arn, [])]
    if mode == "callback":
        inherit_wait_for_callback_event_names(history)
    return suspension_resumptions([row for row in ledger if row.get("execution") == victim_arn], history, mode)


def _binding_expected_outcome(binding: FixtureBinding, item: Mapping[str, Any], scenario: str) -> dict[str, Any]:
    """Expected service outcome, keyed on the catalog scenario and role.

    Conformance-owned: never derived from an SDK fixture name, only from the
    catalog scenario the shared driver admitted and the launch's cohort role.
    """
    role = item.get("role")
    if scenario == "normal-failure" and role == "victim":
        return {
            "status": "FAILED",
            "error": {"type": binding.failure_error_type, "message": "expected:" + item["marker"]},
        }
    if scenario in {"deadline-during-step", "peer-recovery"} and role == "victim":
        # This fixture asserts native deadline/retry behavior. Its logical
        # execution may still be active or may finish after normal release.
        return {"allowedStatuses": ["RUNNING", "SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED"]}
    return {"status": "SUCCEEDED", "result": item["marker"]}


def _binding_replay_effect_policy(binding: FixtureBinding) -> ReplayEffectPolicy:
    """Generalize the standard ``replay_effects`` rule over an arbitrary binding.

    Builds one expected ``(operation, value)`` key per ``values`` entry a replay
    effect declares, or a single ``(operation, None)`` key when it declares none.
    For each key, the first durable operation ID observed owns its ``count``; any
    other ID sharing that key gets zero, and a body whose key is not expected also
    gets zero. An operation ID whose bodies report more than one key, or whose
    first body has an unexpected key, gets zero and owns no further key. Keys with
    no owner are reported missing, named ``operation`` or ``operation:value``.
    """
    expected: dict[tuple[str, int | None], int] = {
        (effect.operation, value): effect.count
        for effect in binding.replay_effects
        for value in (effect.values if effect.values is not None else (None,))
    }
    valued_operations = {effect.operation for effect in binding.replay_effects if effect.values is not None}

    def policy(bodies: Sequence[Mapping[str, Any]]) -> tuple[dict[str, int], list[str]]:
        owners: dict[tuple[str, int | None], str] = {}
        allowances: dict[str, int] = {}
        # Each ID's first body fixes the key (or None, for an unexpected key)
        # it may ever own; a later body carrying a different or unexpected key
        # cannot claim another key and instead zeroes the ID's allowance, so
        # the count check reports a side-effect count mismatch.
        first_keys: dict[str, tuple[str, int | None] | None] = {}
        for body in bodies:
            name, identity = body["operation"], body["operationId"]
            valued = name in valued_operations
            raw_value = body.get("value") if valued else None
            # A finite integral JSON number is the owner key regardless of its
            # wire spelling (``1``, ``1.0``, ``1e0``); this stores a canonical
            # int key. Python numeric equality already makes 1.0 and 1 the
            # same key, so acceptance is decided by ``valid`` below, not by
            # this normalization.
            value = int(raw_value) if valued and is_json_integer(raw_value) else raw_value
            key = (name, value)
            # ``valid`` keeps the comparison JSON-integer-strict: a bool, a
            # fractional value, a non-finite value or a string never satisfies
            # a declared integer allowance, even though some compare equal to
            # one in Python.
            valid = (not valued or is_json_integer(raw_value)) and key in expected
            if identity in first_keys:
                if not valid or key != first_keys[identity]:
                    allowances[identity] = 0
                continue
            first_keys[identity] = key if valid else None
            if not valid:
                allowances[identity] = 0
                continue
            owner = owners.setdefault(key, identity)
            allowances[identity] = expected[key] if identity == owner else 0
        missing = [
            f"{name}:{value}" if value is not None else name for name, value in expected if (name, value) not in owners
        ]
        return allowances, missing

    return policy


def binding_evidence_builder(binding: FixtureBinding, runtime: str) -> Callable[..., dict[str, Any]]:
    """Build an ``evidence_builder(context, cloud, *, phase)`` driven by a FixtureBinding.

    Replaces the SDK-supplied ``expected_outcome``/``worker_identity`` callbacks and
    the replay-effect policy with rules derived purely from the binding, keyed on
    the catalog scenario and role rather than any SDK fixture name.
    """
    expected_replay_effects = {effect.operation: effect.count for effect in binding.replay_effects}
    replay_effect_policy = _binding_replay_effect_policy(binding)

    def expected_outcome(item: Mapping[str, Any], scenario: str) -> dict[str, Any]:
        return _binding_expected_outcome(binding, item, scenario)

    def worker_identity(raw: Mapping[str, Any]) -> str:
        return raw[binding.worker_identity]

    def builder(context: ScenarioContext, cloud: Cloud, *, phase: str = "observations") -> dict[str, Any]:
        return build_evidence(
            context,
            cloud,
            runtime=runtime,
            expected_outcome=expected_outcome,
            worker_identity=worker_identity,
            worker_identity_key=binding.worker_identity,
            phase=phase,
            expected_replay_effects=expected_replay_effects,
            replay_effect_policy=replay_effect_policy,
        )

    return builder


def build_evidence(
    context: ScenarioContext,
    cloud: Cloud,
    *,
    runtime: str,
    expected_outcome: Callable[[Mapping[str, Any], str], Mapping[str, Any]],
    worker_identity: Callable[[Mapping[str, Any]], str],
    worker_identity_key: str = "process",
    phase: str = "observations",
    expected_replay_effects: Mapping[str, int] | None = None,
    replay_effect_policy: ReplayEffectPolicy | None = None,
) -> dict[str, Any]:
    final = phase == "final"
    partial = phase in ("partial", "retirement-partial")
    cutoff = cloud.observation_ended_at
    ended = (cutoff or 0) * 1000
    frozen = phase in ("observations", "partial") and cutoff is not None
    service = cloud.raw_histories
    all_ledger = tuple(cloud.events.values())
    excluded = observation_exclusions(context, cloud, all_ledger, ended if frozen else None)
    deferred_arns = {
        request: arn
        for arn, history in service.items()
        for request in service_native_intervals(history)
        if request in excluded
    }
    deferred_arns.update(
        {
            request: arn
            for entry in context.attempts
            if isinstance(request := entry.get("requestId"), str)
            and request in excluded
            and isinstance(arn := entry.get("executionArn"), str)
        }
    )
    deferred = [{"requestId": request, "executionArn": arn} for request, arn in sorted(deferred_arns.items())]
    ledger = tuple(
        record
        for record in all_ledger
        if not isinstance(request := record.get("request"), str) or request not in excluded
    )
    no_runtime = (
        not cloud.invocations
        and not all_ledger
        and not service
        and all(invoke_rejection_proven(attempt) for attempt in context.attempts)
    )
    control_only = None
    if (
        final
        and no_runtime
        and any(record["action"] == "hold" and record.get("state") != "REJECTED" for record in cloud.control_records)
    ):
        control_only = _undispatched_launches(context, cloud)
    sources = CaseSources.capture(
        context,
        cloud,
        raw=ledger,
        expected_outcome=expected_outcome,
        worker_identity=worker_identity,
        worker_identity_key=worker_identity_key,
    )
    pending: list[str] = []
    all_events, errors = sources.observations(pending=None if final else pending)
    events = [event for event in all_events if event["atMs"] <= ended] if frozen else all_events
    history_pending: list[str] = []
    known, missing, seal_errors = ledger_seal_status(
        ledger,
        service,
        cloud.invocations,
        excluded,
        defer_unbound=(not final) and getattr(cloud, "binding_deferred", False) is True,
    )
    errors.extend(seal_errors)
    # The service proves a physically undispatched request never reached a
    # handler, so it is exempt from the physical seal requirement; it still
    # counts as incomplete behavior evidence everywhere else, so it is never
    # computed outside the final (physical retirement) phase.
    undispatched = undispatched_requests(all_ledger, service, cloud.invocations) if final else {}
    missing -= undispatched.keys()
    sealed = (
        known - missing
        if frozen
        and DEADLINE_REQUIREMENTS.get(context.scenario.scenario) == context.scenario.requirement_id
        and not seal_errors
        else None
    )
    if final:
        histories, expected, history_errors = sources.histories()
        attempts, selected, attempt_errors = sources.attempts(events, undispatched=undispatched.keys())
    else:
        histories, expected, history_errors = sources.histories(
            pending=history_pending, cutoff_ms=ended if frozen else None
        )
        attempts, selected, attempt_errors = sources.attempts(
            events, pending=pending, cutoff_ms=ended if frozen else None, excluded=excluded, sealed=sealed
        )
    errors.extend(history_errors + attempt_errors)
    _causal_order(events)
    in_progress_sdk_requests = _sdk_requests(events)

    if missing:
        pending.append("Missing ledger seals or wrapper entries for native requests: " + ", ".join(sorted(missing)))
    if (
        not attempts
        or not known
        or not all(
            item.get("activeAtObservationEnd") is True
            or _quiescent(item["requestId"], events, in_progress_sdk_requests)
            for item in attempts
        )
    ):
        pending.append("Actual wrapper/task publication has not established a complete observation window")
    if cutoff is None:
        pending.append("No source collection cutoff was recorded")
        history_pending.append("No service history cutoff was recorded")
    latest = max([0, *(event["atMs"] for event in events), *(item.get("endedAtMs", 0) for item in attempts)])
    if final and ended < latest:
        errors.append("Retirement collection predates a retained observation")
    if partial and not frozen:
        ended = max(ended, latest)

    cutoff_results = deadline_cutoff_results(context, cloud) if frozen else None
    history_pending.extend(terminal_history_pending(cloud, ledger, ended, excluded, cutoff_results))

    watermarks: dict[str, float] = {}
    if final and control_only is None:
        errors.extend(pending + history_pending)
    if not partial and control_only is None and not errors:
        if not pending:
            watermarks.update(lifecycle=ended, sdkRequests=ended)
        if not history_pending:
            watermarks["history"] = ended
    pending.extend(history_pending)
    artifact = {
        "sdk": context.profile.artifact_id,
        "conformance": cloud.manifest.get("conformanceRevision") or "unverified",
    }
    profile = {
        "functionArn": context.profile.resource_id,
        "configurationDigest": context.profile.configuration_id,
        "nativeConcurrency": context.profile.native_concurrency,
        "timeoutMs": cloud.manifest["invocationTimeout"] * 1000,
    }
    if context.profile.recovery_policy:
        profile["recoveryPolicy"] = context.profile.recovery_policy
    evidence: dict[str, Any] = {
        "schemaVersion": 1,
        "runId": context.run_id,
        "caseId": context.scenario.case_id,
        "coverageProfile": context.scenario.coverage_profile,
        "scenario": context.scenario.scenario,
        "requirementId": context.scenario.requirement_id,
        "runtime": runtime,
        "artifact": artifact,
        "profile": profile,
        "attempts": attempts,
        "selectedRequests": selected,
        "events": assertion_events(events, context.scenario.scenario, selected),
        "histories": histories,
        "expectedHistories": expected,
        "observationEndedAtMs": ended,
        "sdkRequests": [],
        "collection": {
            "watermarks": watermarks,
            "evidenceUris": sorted(cloud.source_uris),
            **({"deferredServiceRequests": deferred} if deferred else {}),
            **({"pending": pending} if pending else {}),
        },
        "collectionErrors": errors,
    }
    evidence["budgets"] = dict(context.scenario.behavior_budgets)
    if context.scenario.scenario == "suspension-retry" and not no_runtime:
        complete_replay = not partial and not pending
        victim_arn = next(
            (item["executionArn"] for item in attempts if item["requestId"] == selected.get("victim")), None
        )
        # Clouds built by live drivers record the selected suspension mode.
        # Fixture-built clouds without a driver can still verify the first resume.
        suspension_mode = getattr(cloud, "suspension_mode", None)
        resumes = _public_suspension_histories(cloud, ledger, victim_arn) if complete_replay and suspension_mode else {}
        resume_id = selected.get("resume")
        projection_errors: list[str] = []
        expected_before = (
            _replay_history_before_resume(
                sources,
                attempts,
                selected,
                projection_errors,
                waking_ids=resumes.get(resume_id) if resume_id is not None else None,
            )
            if complete_replay
            else None
        )
        evidence["replay"] = replay_evidence(
            events,
            histories,
            selected,
            # Cross-check the complete raw/body streams during retirement if
            # the frozen window omits later captures.
            raw=ledger if len(events) == len(all_events) else (),
            expected_effects=expected_replay_effects,
            effect_policy=replay_effect_policy,
            expected_before=expected_before,
            errors=projection_errors,
            partial=not complete_replay,
        )
        if complete_replay and suspension_mode == "wait":
            evidence["replay"]["durableWaitObserved"] = bool(resumes)
        if complete_replay and resume_id is not None:
            matches = not suspension_mode or bool(resumes)
            for attempt in attempts:
                request = attempt["requestId"]
                if request == selected.get("victim") or attempt.get("executionArn") != victim_arn:
                    continue
                starts = [
                    event
                    for event in events
                    if event.get("requestId") == request and event.get("kind") == "invocation_start"
                ]
                if len(starts) != 1:
                    matches = False
                    continue
                resumed = {**selected, "resume": request}
                expected_checkpoints = _replay_history_before_resume(
                    sources, attempts, resumed, projection_errors, waking_ids=resumes.get(request)
                )
                actual = replay_evidence(events, histories, resumed, errors=projection_errors, partial=True)["before"]
                observed_ids = {row["id"]: row for row in actual}
                expected_ids = {row["id"]: row for row in expected_checkpoints or []}
                # The original first-resume and public-wakeup checks retain
                # their exact-set policy. Intermediate resumes compare all
                # supplied checkpoints and every observable root checkpoint.
                required = (
                    set(expected_ids)
                    if request == resume_id or request in resumes
                    else {identity for identity, row in expected_ids.items() if row.get("parentId") is None}
                )
                repeated_completed = any(
                    event.get("kind") == "body"
                    and event.get("requestId") == request
                    and isinstance(event.get("operationId"), str)
                    and event["operationId"] in expected_ids
                    for event in events
                )
                matches &= (
                    not repeated_completed
                    and expected_checkpoints is not None
                    and len(actual) == len(observed_ids)
                    and required <= observed_ids.keys()
                    and all(same_checkpoint(row, expected_ids.get(identity)) for identity, row in observed_ids.items())
                )
            if not projection_errors:
                evidence["replay"]["suspensionCheckpointMatches"] = matches
        if projection_errors:
            evidence["replay"]["projectionErrors"] = projection_errors
    for event in events:
        if event.get("kind") in {"checkpoint_call", "poll_call"}:
            # Legacy transport-boundary hooks only emit a call on dispatch.
            outcome = event.get("outcome", "DISPATCHED")
            if outcome not in ("DISPATCHED", "REJECTED"):
                errors.append(f"SDK request {event.get('callId')} has an invalid outcome")
            evidence["sdkRequests"].append(
                {
                    # Keep every event field, including the per-request
                    # `sequence` `_causal_order` assigned: `no_late_dispatch`
                    # needs it to order a same-millisecond dispatch against
                    # `wrapper_return`/`admission_closed`.
                    **event,
                    "method": "CheckpointDurableExecution"
                    if event["kind"] == "checkpoint_call"
                    else "GetDurableExecutionState",
                    "outcome": outcome,
                }
            )
    verified_executions = {
        arn: launch for launch in cloud.invocations if isinstance(arn := launch.get("arn"), str) and arn
    }
    request_ids = dict.fromkeys(item["requestId"] for item in attempts)
    for record in ledger:
        arn, request = record.get("execution"), record.get("request")
        if (
            isinstance(arn, str)
            and arn in verified_executions
            and isinstance(request, str)
            and request.strip()
            and record.get("marker") == verified_executions[arn]["marker"]
            and record.get("run") == context.run_id
            and record.get("caseId") == context.scenario.case_id
            and record.get("commit") == cloud.manifest["commit"]
        ):
            request_ids[request] = None
    for arn, launch in verified_executions.items():
        if not frozen:
            request_ids.update(dict.fromkeys(launch.get("native_requests", ())))
        for event in service.get(arn, ()):
            details = event.get("InvocationCompletedDetails")
            if not isinstance(details, Mapping) or not isinstance(request := details.get("RequestId"), str):
                continue
            if frozen:
                try:
                    if _milliseconds(details.get("StartTimestamp")) > ended:
                        continue
                except ValueError, OverflowError:
                    continue
            request_ids[request] = None
    created = [
        {"kind": "invocation", "id": request, "ownerCaseId": context.scenario.case_id} for request in request_ids
    ]
    created.extend(
        {"kind": "execution", "id": arn, "ownerCaseId": context.scenario.case_id} for arn in verified_executions
    )
    # Service callback starts establish ownership even when callback dispatch
    # failed before the driver received a response. Operation IDs avoid exposing
    # callback tokens in the retained inventory.
    callbacks = {}
    callback_completions: dict[str, list[dict[str, Any]]] = {}
    for arn in verified_executions:
        for event in service.get(arn, []):
            if frozen and ((occurred := _service_event_time(event)) is None or occurred > ended):
                continue
            operation_id = event.get("Id")
            if not isinstance(operation_id, str):
                continue
            identity = f"{arn}/callback/{operation_id}"
            if event.get("EventType", "").startswith("Callback"):
                callbacks[identity] = {
                    "kind": "callback",
                    "id": identity,
                    "ownerCaseId": context.scenario.case_id,
                    "executionArn": arn,
                    "operationId": operation_id,
                }
            if event.get("EventType") in {"CallbackSucceeded", "CallbackFailed", "CallbackTimedOut"}:
                callback_completions.setdefault(identity, []).append(event)
    created.extend(callbacks.values())
    controls = {
        record["gate"]: {
            "kind": "control",
            "id": f"s3://{cloud.manifest['bucket']}/{cloud.control_prefix}{record['gate']}",
            "gate": record["gate"],
            "ownerCaseId": context.scenario.case_id,
        }
        for record in cloud.control_records
        if record["action"] == "hold" and record.get("state") != "REJECTED"
    }
    created.extend(controls.values())
    evidence["resourcesCreated"] = created
    # Coordination barriers are not injected victim faults. Retain their full
    # journal separately; releasing a healthy peer is not evidence of victim cleanup.
    evidence["controlRecords"] = list(cloud.control_records)
    victim_faults = {
        item.get("payload", {}).get("gate")
        for item in cloud.invocations
        if item.get("role") == "victim" and context.scenario.scenario in _ACTIVE_FAULTS
    }
    faults = []
    for gate in controls:
        if gate not in victim_faults:
            continue
        fault: dict[str, Any] = {"id": gate, "ownerCaseId": context.scenario.case_id}
        receipts = [
            event["atMs"] for event in events if event.get("gateId") == gate and event.get("kind") == "fault_released"
        ]
        releases = [
            record
            for record in cloud.control_records
            if record["gate"] == gate
            and record["action"] == "release"
            and record.get("state", "ACKNOWLEDGED") == "ACKNOWLEDGED"
        ]
        if receipts:
            fault["releasedAtMs"] = min(receipts)
        if releases:
            release = releases[0]
            requested = release.get("started", release["time"]) * 1000
            if not receipts or requested <= min(receipts):
                fault.update(
                    releaseRequestedAtMs=requested,
                    releaseAcknowledgedAtMs=release["time"] * 1000,
                )
            fault.setdefault("releasedAtMs", release["time"] * 1000)
        faults.append(fault)
    evidence["faultsCreated"] = faults
    if phase == "final":
        retired = (cloud.retired_at or 0) * 1000
        settled = []
        for resource in created:
            item = {
                **resource,
                "settledAtMs": retired,
                "evidenceUri": (cloud.artifacts_dir / "events.json").resolve().as_uri(),
            }
            if resource["kind"] == "invocation":
                proof = undispatched.get(resource["id"])
                if proof is not None:
                    # A request proven undispatched settles with its proof fields;
                    # settledAtMs becomes its native rejection time, not the
                    # overall retirement time. Behavior validation must still
                    # reject this outcome, so a retry can never stand in for it.
                    item.update(outcome="UNDISPATCHED", **proof)
                    invocation = next((i for i in cloud.invocations if i.get("arn") == proof["executionArn"]), None)
                    if invocation is not None:
                        item["evidenceUri"] = (
                            (cloud.artifacts_dir / f"histories/{invocation['marker']}.json").resolve().as_uri()
                        )
                else:
                    item["outcome"] = (
                        "QUIESCENT" if _quiescent(resource["id"], events, evidence["sdkRequests"]) else "UNKNOWN"
                    )
            elif resource["kind"] == "execution":
                item["outcome"] = cloud.execution_results.get(resource["id"], {}).get("Status", "UNKNOWN")
                invocation = next((i for i in cloud.invocations if i.get("arn") == resource["id"]), None)
                if invocation is not None:
                    receipt = (cloud.artifacts_dir / f"executions/{invocation['marker']}.json").resolve().as_uri()
                    if receipt in cloud.source_uris:
                        item["evidenceUri"] = receipt
            elif resource["kind"] == "callback":
                completion = callback_completions.get(resource["id"], [])
                item["outcome"] = "UNKNOWN"
                if len(completion) == 1:
                    item["outcome"] = {
                        "CallbackSucceeded": "SUCCEEDED",
                        "CallbackFailed": "FAILED",
                        "CallbackTimedOut": "TIMED_OUT",
                    }[completion[0]["EventType"]]
                    try:
                        item["settledAtMs"] = _milliseconds(completion[0].get("EventTimestamp"))
                    except TypeError, ValueError:
                        item["outcome"] = "UNKNOWN"
                invocation = next((i for i in cloud.invocations if i.get("arn") == resource["executionArn"]), None)
                if invocation is not None:
                    item["evidenceUri"] = (
                        (cloud.artifacts_dir / f"histories/{invocation['marker']}.json").resolve().as_uri()
                    )
            elif resource["kind"] == "control":
                receipts = [
                    record
                    for record in cloud.control_records
                    if record["gate"] == resource["gate"]
                    and record["action"] == "delete"
                    and record.get("state") == "ACKNOWLEDGED"
                ]
                item["outcome"] = "DELETED" if receipts else "UNKNOWN"
                if receipts:
                    item["settledAtMs"] = receipts[-1]["time"] * 1000
                item["evidenceUri"] = (cloud.artifacts_dir / "control-records.json").resolve().as_uri()
            settled.append(item)
        evidence["retirement"] = {
            **({"mode": "controls-only", "undispatchedLaunches": control_only} if control_only is not None else {}),
            "retiredAtMs": retired,
            "resources": settled,
            "faults": [
                {
                    "id": fault["id"],
                    "ownerCaseId": context.scenario.case_id,
                    "releasedAtMs": fault.get("releaseAcknowledgedAtMs", fault.get("releasedAtMs", 0)),
                    "mode": "emergency"
                    if any(
                        event.get("gateId") == fault["id"]
                        and event.get("kind") in {"fault_released"}
                        and event.get("mode") == "emergency"
                        for event in events
                    )
                    else "normal",
                }
                for fault in faults
            ],
        }
    if errors:
        evidence["collection"]["watermarks"] = {}
    if not partial:
        cloud._save(f"shared-{phase}-evidence.json", evidence)
    return evidence
