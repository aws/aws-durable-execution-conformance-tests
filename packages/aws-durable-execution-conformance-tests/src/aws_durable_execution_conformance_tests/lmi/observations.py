# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Language-neutral source selection and runtime-specific placement models."""

import math
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from aws_durable_execution_conformance_tests.lmi.rules.values import UNDISPATCHED_ERROR_TYPES

from .errors import CollectionError, PlacementError

# The error type Lambda reports in InvocationCompletedDetails when the
# platform ends an invocation at its timeout, as observed in real LMI runs.
PLATFORM_TIMEOUT_ERROR_TYPES = frozenset({"Sandbox.Timedout"})


def timestamp_ms(value: Any) -> float:
    if isinstance(value, datetime):
        return value.timestamp() * 1000
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000
    if type(value) in (int, float):
        return float(value) * 1000
    raise ValueError("Service evidence is missing its actual timestamp")


def service_native_intervals(history: Sequence[Mapping[str, Any]]) -> dict[str, tuple[float, float]]:
    """Use only uniquely identified native completions with a valid lifetime."""
    reported: Counter[str] = Counter()
    intervals = {}
    for event in history:
        details = event.get("InvocationCompletedDetails")
        if event.get("EventType") != "InvocationCompleted" or not isinstance(details, Mapping):
            continue
        request = details.get("RequestId")
        if not isinstance(request, str) or not request.strip():
            continue
        reported[request] += 1
        try:
            start = timestamp_ms(details.get("StartTimestamp"))
            end = timestamp_ms(details.get("EndTimestamp"))
        except ValueError, OverflowError:
            continue
        if 0 <= start <= end < math.inf:
            intervals[request] = (start, end)
    return {request: interval for request, interval in intervals.items() if reported[request] == 1}


def _pending_native_decisions(
    ledger: Sequence[Mapping[str, Any]], history: Sequence[Mapping[str, Any]]
) -> list[tuple[float, float, float]]:
    intervals = service_native_intervals(history)
    entered = {
        request
        for event in ledger
        if event.get("phase") == "WRAPPER_ENTER" and isinstance(request := event.get("request"), str)
    }
    failed = {
        details.get("RequestId")
        for event in history
        if event.get("EventType") == "InvocationCompleted"
        and isinstance(details := event.get("InvocationCompletedDetails"), Mapping)
        and isinstance(details.get("RequestId"), str)
        and details.get("Error") is not None
    }
    decisions = []
    for event in ledger:
        request = event.get("request")
        if (
            not isinstance(request, str)
            or request not in intervals
            or request not in entered
            or request in failed
            or event.get("phase") != "WRAPPER_RETURN"
            or event.get("status") != "PENDING"
        ):
            continue
        start, end = intervals[request]
        observed = [
            observation
            for row in ledger
            if row.get("request") == request
            and row.get("phase") == "CONFORMANCE"
            and isinstance(observation := row.get("observation"), Mapping)
            and observation.get("kind") == "invocation_end"
        ]
        try:
            returned = timestamp_ms(event.get("time"))
            if observed:
                if len(observed) != 1 or observed[0].get("outcome") != "PENDING":
                    continue
                at = observed[0].get("atMs")
                if not isinstance(at, (int, float)) or isinstance(at, bool):
                    continue
                decision = float(at)
            else:
                decision = returned
        except KeyError, TypeError, ValueError, OverflowError:
            continue
        if 0 <= start <= decision <= returned <= end < math.inf:
            decisions.append((start, decision, end))
    return decisions


def suspension_resumptions(
    ledger: Sequence[Mapping[str, Any]], history: Sequence[Mapping[str, Any]], mode: str
) -> dict[str, set[str]]:
    """Map native resumes to public waits/callbacks that finished at their wakeup."""
    prefix = "Wait" if mode == "wait" else "Callback"
    start_kind, completion_kind = prefix + "Started", prefix + "Succeeded"
    intervals = service_native_intervals(history)
    entered = {
        request
        for event in ledger
        if event.get("phase") == "WRAPPER_ENTER" and isinstance(request := event.get("request"), str)
    }
    waits: dict[str, dict[str, float]] = {}
    duplicate_waits: set[str] = set()
    for event in history:
        kind, identity = event.get("EventType"), event.get("Id")
        if kind not in (start_kind, completion_kind) or not isinstance(identity, str) or not identity:
            continue
        try:
            at = timestamp_ms(event.get("EventTimestamp"))
        except ValueError, OverflowError:
            continue
        if mode != "wait" and kind == start_kind and event.get("Name") != "completion":
            continue
        if 0 <= at < math.inf:
            operation = waits.setdefault(identity, {})
            if kind in operation:
                duplicate_waits.add(identity)
            operation[kind] = at
    decisions = list(_pending_native_decisions(ledger, history))
    resumed: dict[str, set[str]] = {}
    for _, pending, end in decisions:
        candidates = sorted(
            (native_start, request)
            for request, (native_start, _) in intervals.items()
            if request in entered and native_start >= end
        )
        if not candidates or (len(candidates) > 1 and candidates[0][0] == candidates[1][0]):
            continue
        resumed_at, request = candidates[0]
        # The wait may have started in this invocation or in an earlier one that
        # also returned PENDING: a wait still outstanding across an intermediate
        # resume (another operation's retry, say) wakes the execution here.
        waking = {
            identity
            for identity, pair in waits.items()
            if identity not in duplicate_waits
            and pending < pair.get(completion_kind, -1) <= resumed_at
            and any(
                window_start <= pair.get(start_kind, -1) <= window_pending <= pending
                for window_start, window_pending, _ in decisions
            )
        }
        if waking:
            resumed.setdefault(request, set()).update(waking)
    return resumed


def wait_crossed_suspension(ledger: Sequence[Mapping[str, Any]], history: Sequence[Mapping[str, Any]]) -> bool:
    return bool(suspension_resumptions(ledger, history, "wait"))


def pending_callback(
    ledger: Sequence[Mapping[str, Any]], history: Sequence[Mapping[str, Any]], name: str
) -> str | None:
    """Identify one outstanding callback started by the request that actually suspended."""
    terminal = {
        identity
        for event in history
        if event.get("EventType") in ("CallbackSucceeded", "CallbackFailed", "CallbackTimedOut")
        and isinstance(identity := event.get("Id"), str)
    }
    decisions = _pending_native_decisions(ledger, history)
    candidates = []
    for event in history:
        identity, details = event.get("Id"), event.get("CallbackStartedDetails")
        if (
            event.get("EventType") != "CallbackStarted"
            or event.get("Name") != name
            or not isinstance(identity, str)
            or not identity
            or identity in terminal
            or not isinstance(details, Mapping)
            or not isinstance(callback := details.get("CallbackId"), str)
            or not callback.strip()
        ):
            continue
        try:
            started = timestamp_ms(event.get("EventTimestamp"))
        except ValueError, OverflowError:
            continue
        if any(start <= started <= decision for start, decision, _ in decisions):
            candidates.append(callback)
    return candidates[0] if len(candidates) == 1 else None


def service_error_type(details):
    """The structured error type of a service invocation completion, if any."""
    error = details.get("Error") if isinstance(details, Mapping) else None
    payload = error.get("Payload", error) if isinstance(error, Mapping) else None
    kind = payload.get("ErrorType") if isinstance(payload, Mapping) else None
    return kind if isinstance(kind, str) else None


def is_platform_timeout(details):
    """Classify by error type: messages and stack traces can mention timeouts."""
    return service_error_type(details) in PLATFORM_TIMEOUT_ERROR_TYPES


def is_undispatched_rejection(details):
    """Classify a platform rejection that proves a request never reached a handler.

    Platform rejections carry no stack trace, while handler-raised errors
    normally do; a handler that ran and raised a throttling exception must not
    qualify. So this requires the exact real payload shape (only ErrorType and
    ErrorMessage, both strings, and an untruncated error) and never matches on
    message text, ErrorData or a stack trace.
    """
    error = details.get("Error") if isinstance(details, Mapping) else None
    if not isinstance(error, Mapping) or error.get("Truncated", False) is not False:
        return False
    payload = error.get("Payload")
    if not isinstance(payload, Mapping) or set(payload) != {"ErrorType", "ErrorMessage"}:
        return False
    error_type, message = payload.get("ErrorType"), payload.get("ErrorMessage")
    return isinstance(error_type, str) and isinstance(message, str) and error_type in UNDISPATCHED_ERROR_TYPES


def select(events, phase, marker=None):
    return [e for e in events if e["phase"] == phase and (marker is None or e["marker"] == marker)]


def check_controls(events):
    errors = select(events, "CONTROL_ERROR")
    if errors:
        error = errors[0]
        raise CollectionError(f"Control {error['gate']} failed: {error['error']}")


def overlap(events, markers, count, environment=None, *, runtime, replacement=False):
    if runtime not in {"python", "java", "javascript"}:
        raise ValueError("Unknown runtime placement model")
    blocked = [e for e in select(events, "BLOCKED") if e["marker"] in markers]
    for start in blocked:
        env = start["environment"]
        if environment is not None and env != environment:
            continue
        active = []
        for candidate in blocked:
            if candidate["environment"] != env or candidate["time"] > start["time"]:
                continue
            alive = [
                e
                for e in events
                if e["phase"] in {"ALIVE", "EFFECT"}
                and all(e.get(k) == candidate.get(k) for k in ("request", "environment", "process", "gate"))
                and e["time"] >= candidate["time"]
            ]
            # Missing exit records (e.g. a retired process) do not prove liveness.
            if not alive or max(e["time"] for e in alive) < start["time"]:
                continue
            exits = [
                e
                for e in select(events, "IO_EXIT")
                if e["request"] == candidate["request"]
                and e.get("gate") == candidate.get("gate")
                and e["time"] >= candidate["time"]
            ]
            if not exits or min(e["time"] for e in exits) > start["time"]:
                active.append(candidate)
        if len({e["request"] for e in active}) >= count and len({e["execution"] for e in active}) >= count:
            processes = {e["process"] for e in active}
            if runtime == "python" and (len(processes) < count or len({e["pid"] for e in active}) < count):
                continue
            if (runtime == "java" or (runtime == "javascript" and not replacement)) and len(processes) != 1:
                continue
            if runtime == "javascript" and (
                any(not e.get("worker") for e in active)
                or (not replacement and len({e["worker"] for e in active}) != 1)
            ):
                continue
            return env
    raise PlacementError(f"No {count}-request {runtime} overlap in one execution environment")

    # A runtime-supported worker retirement can have no final diagnostic. Recovery
    # and full-capacity placement must therefore also be asserted by the driver.
