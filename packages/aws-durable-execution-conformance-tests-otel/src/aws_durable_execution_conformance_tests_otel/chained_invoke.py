# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Observe child execution carriers through the existing public Lambda API."""

import re
from collections.abc import Mapping, Sequence
from typing import Any

from aws_durable_execution_conformance_tests.validate import get_durable_execution
from aws_durable_execution_conformance_tests_otel.model import ExecutionTraceContext


def execution_trace_context(response: Mapping[str, Any]) -> ExecutionTraceContext:
    """Require an actual, unambiguous Root/Parent/Sampled service observation."""
    header = response.get("TraceHeader")
    raw = header.get("XAmznTraceId") if isinstance(header, Mapping) else None
    if not isinstance(raw, str) or not raw:
        raise ValueError("GetDurableExecution is missing TraceHeader.XAmznTraceId")
    fields: dict[str, str] = {}
    for part in raw.split(";"):
        key, separator, value = part.strip().partition("=")
        if key not in {"Root", "Parent", "Sampled"}:
            continue
        if not separator or key in fields:
            raise ValueError(f"GetDurableExecution trace header has ambiguous {key}")
        fields[key] = value.strip()
    root = fields.get("Root", "")
    parent = fields.get("Parent", "")
    sampled = fields.get("Sampled", "")
    if not re.fullmatch(r"1-[0-9a-fA-F]{8}-[0-9a-fA-F]{24}", root) or int(root.replace("-", "")[1:], 16) == 0:
        raise ValueError("GetDurableExecution trace header has missing or invalid Root")
    if not re.fullmatch(r"[0-9a-fA-F]{16}", parent) or int(parent, 16) == 0:
        raise ValueError("GetDurableExecution trace header has missing or invalid Parent")
    if sampled not in {"0", "1"}:
        raise ValueError("GetDurableExecution trace header has missing or invalid Sampled")
    return ExecutionTraceContext(root.replace("-", "")[1:].lower(), parent.lower(), sampled == "1")


def load_execution_trace_contexts(
    assertions: Any,
    history: Mapping[str, Any],
    lambda_client: Any,
) -> dict[str, ExecutionTraceContext]:
    """Read only child ARNs present in actual ChainedInvokeStarted history."""
    if assertions is None:
        return {}
    if not isinstance(assertions, Sequence) or isinstance(assertions, (str, bytes)):
        raise ValueError("chained_invokes must be a sequence of mappings")
    children = [
        event.get("ChainedInvokeStartedDetails", {}).get("DurableExecutionArn")
        for event in history.get("Events", [])
        if event.get("EventType") == "ChainedInvokeStarted"
    ]
    observed: dict[str, ExecutionTraceContext] = {}
    for assertion in assertions:
        child = assertion.get("child_execution_arn") if isinstance(assertion, Mapping) else None
        if not isinstance(child, str) or children.count(child) != 1:
            raise ValueError(
                "chained_invokes child_execution_arn must identify exactly one actual ChainedInvokeStarted"
            )
        if child not in observed:
            response = get_durable_execution(child, lambda_client)
            if response is None:
                raise ValueError("Unable to read chained child GetDurableExecution trace header")
            observed[child] = execution_trace_context(response)
    return observed
