# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Language-neutral service-history and ledger-event helpers."""

import math
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from .errors import CollectionError

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


def service_error_type(details):
    """The structured error type of a service invocation completion, if any."""
    error = details.get("Error") if isinstance(details, Mapping) else None
    payload = error.get("Payload", error) if isinstance(error, Mapping) else None
    kind = payload.get("ErrorType") if isinstance(payload, Mapping) else None
    return kind if isinstance(kind, str) else None


def is_platform_timeout(details):
    """Classify by error type: messages and stack traces can mention timeouts."""
    return service_error_type(details) in PLATFORM_TIMEOUT_ERROR_TYPES


def select(events, phase, marker=None):
    return [e for e in events if e["phase"] == phase and (marker is None or e["marker"] == marker)]


def check_controls(events):
    errors = select(events, "CONTROL_ERROR")
    if errors:
        error = errors[0]
        raise CollectionError(f"Control {error['gate']} failed: {error['error']}")
