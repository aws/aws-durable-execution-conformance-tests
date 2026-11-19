# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: Apache-2.0
"""Language-neutral source selection and runtime-specific placement models."""

from collections.abc import Mapping

from .errors import CollectionError, PlacementError

# The error type Lambda reports in InvocationCompletedDetails when the
# platform ends an invocation at its timeout, as observed in real LMI runs.
PLATFORM_TIMEOUT_ERROR_TYPES = frozenset({"Sandbox.Timedout"})


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


def overlap(events, markers, count, environment=None, *, runtime):
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
            if runtime in {"java", "javascript"} and len(processes) != 1:
                continue
            if runtime == "javascript" and (
                any(not e.get("worker") for e in active) or len({e["worker"] for e in active}) != 1
            ):
                continue
            return env
    raise PlacementError(f"No {count}-request {runtime} overlap in one execution environment")

    # A runtime-supported worker retirement can have no final diagnostic. Recovery
    # and full-capacity placement must therefore also be asserted by the driver.
