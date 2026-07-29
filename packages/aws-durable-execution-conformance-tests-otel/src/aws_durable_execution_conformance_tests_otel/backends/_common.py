# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Shared HTTP and trace-matching helpers for telemetry backends."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Mapping
from typing import Any, Protocol

from aws_durable_execution_conformance_tests_otel.model import (
    TelemetryQuery,
    Trace,
    normalize_id,
)
from aws_durable_execution_conformance_tests_otel.polling import BackendError
from aws_durable_execution_conformance_tests_otel.redaction import redact


class HttpClient(Protocol):
    def request_json(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        body: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        """Send an HTTP request and parse a JSON object."""


class JsonHttpClient:
    """Small stdlib JSON client that keeps headers out of diagnostics."""

    def request_json(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        body: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={
                "Accept": "application/json",
                **({"Content-Type": "application/json"} if body is not None else {}),
                **dict(headers or {}),
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                payload = json.loads(response.read() or b"{}")
        except (json.JSONDecodeError, urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            safe_url = redact(url)
            raise BackendError(f"Telemetry backend request to {safe_url!r} failed: {type(exc).__name__}") from exc
        if not isinstance(payload, Mapping):
            raise BackendError("Telemetry backend returned a non-object JSON response")
        return payload


def matching_trace(traces: list[Trace], query: TelemetryQuery) -> Trace | None:
    """Build a correlated view from the workflow and ambient Lambda traces."""

    def execution_arns(trace: Trace) -> set[str]:
        return {
            str(span.attributes["durable.execution.arn"])
            for span in trace.spans
            if "durable.execution.arn" in span.attributes
        }

    if query.trace_id:
        expected = normalize_id(query.trace_id, 32)
        primary = next((trace for trace in traces if trace.trace_id == expected), None)
        if primary is None:
            return None
    else:
        matching = [trace for trace in traces if query.execution_arn in execution_arns(trace)]
        if not matching:
            return None
        primary = next(
            (
                trace
                for trace in matching
                if any(
                    span.name == "Workflow" and span.attributes.get("durable.execution.arn") == query.execution_arn
                    for span in trace.spans
                )
            ),
            matching[0],
        )

    correlated_arns = set(query.execution_arns or (query.execution_arn,))
    correlated: list[Trace] = []
    correlated_trace_ids: set[str] = set()
    for trace in traces:
        if trace.trace_id not in correlated_trace_ids and execution_arns(trace) & correlated_arns:
            correlated.append(trace)
            correlated_trace_ids.add(trace.trace_id)

    ordered = [primary, *(trace for trace in correlated if trace is not primary)]
    correlated_spans = tuple(
        span
        for trace in ordered
        for span in trace.spans
        if trace is primary or span.attributes.get("durable.execution.arn") in correlated_arns
    )
    log_trace_ids: list[str] = []
    for trace in ordered:
        log_trace_ids.extend(trace_id for trace_id in trace.log_trace_ids if trace_id not in log_trace_ids)
    raw_artifacts = tuple(trace.raw_artifact for trace in ordered if trace.raw_artifact is not None)
    return Trace(
        trace_id=primary.trace_id,
        spans=correlated_spans,
        log_trace_ids=tuple(log_trace_ids),
        raw_artifact=raw_artifacts,
    )
