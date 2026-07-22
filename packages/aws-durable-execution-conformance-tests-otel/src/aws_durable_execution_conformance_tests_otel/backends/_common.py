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
    """Find the trace correlated with a backend-neutral query."""

    if query.trace_id:
        expected = normalize_id(query.trace_id, 32)
        return next((trace for trace in traces if trace.trace_id == expected), None)
    for trace in traces:
        for span in trace.spans:
            if query.execution_arn in {str(value) for value in span.attributes.values()}:
                return trace
    return None
