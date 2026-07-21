# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""AWS X-Ray telemetry backend."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from aws_durable_execution_conformance_tests_otel.backends._common import (
    matching_trace,
)
from aws_durable_execution_conformance_tests_otel.model import (
    Span,
    TelemetryQuery,
    Trace,
    normalize_id,
    parse_timestamp,
)
from aws_durable_execution_conformance_tests_otel.polling import (
    BackendError,
    PollingBackend,
)


def _metadata_attributes(metadata: Any) -> dict[str, Any]:
    if not isinstance(metadata, Mapping):
        return {}
    attributes: dict[str, Any] = {}
    for namespace, values in metadata.items():
        if not isinstance(values, Mapping):
            attributes[f"xray.{namespace}"] = values
        elif namespace == "default":
            attributes.update(values)
        else:
            attributes.update({f"xray.{namespace}.{key}": value for key, value in values.items()})
    return attributes


def normalize_xray(documents: Iterable[str | Mapping[str, Any]]) -> list[Trace]:
    """Normalize AWS X-Ray segment documents."""

    grouped: dict[str, list[Span]] = defaultdict(list)

    def ingest(
        segment: Mapping[str, Any],
        trace_id: str,
        service_name: str | None,
    ) -> None:
        span_id = normalize_id(segment.get("id"), 16)
        if span_id is None:
            return
        start = parse_timestamp(segment.get("start_time", 0))
        end = parse_timestamp(segment.get("end_time", segment.get("start_time", 0)))
        attributes = {
            **dict(segment.get("annotations", {})),
            **_metadata_attributes(segment.get("metadata")),
        }
        status = "ERROR" if any(segment.get(flag) for flag in ("error", "fault", "throttle")) else "OK"
        grouped[trace_id].append(
            Span(
                trace_id=trace_id,
                span_id=span_id,
                parent_span_id=normalize_id(segment.get("parent_id"), 16),
                name=str(segment.get("name", "")),
                start_time=start,
                end_time=end,
                status=status,
                attributes=attributes,
                service_name=service_name,
            )
        )
        for child in segment.get("subsegments", []):
            ingest(child, trace_id, service_name)

    raw_documents = list(documents)
    for document in raw_documents:
        segment = json.loads(document) if isinstance(document, str) else document
        trace_id = normalize_id(segment.get("trace_id"), 32)
        if trace_id is None:
            continue
        ingest(segment, trace_id, str(segment.get("name") or "") or None)
    return [
        Trace(trace_id=trace_id, spans=tuple(spans), raw_artifact=raw_documents) for trace_id, spans in grouped.items()
    ]


class XRayBackend(PollingBackend):
    name = "xray"

    def __init__(self, client: Any, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._client = client

    def _lookup(self, query: TelemetryQuery) -> Trace | None:
        try:
            trace_ids: list[str] = []
            if query.trace_id:
                trace_ids = [query.trace_id]
            else:
                response = self._client.get_trace_summaries(
                    StartTime=query.started_at,
                    EndTime=query.ended_at,
                    FilterExpression=f'service("{query.service_name}")',
                )
                trace_ids = [item["Id"] for item in response.get("TraceSummaries", []) if item.get("Id")]
            if not trace_ids:
                return None
            response = self._client.batch_get_traces(TraceIds=trace_ids[:5])
        except (BotoCoreError, ClientError) as exc:
            raise BackendError(f"X-Ray telemetry query failed: {type(exc).__name__}") from exc
        documents = [
            segment["Document"]
            for trace in response.get("Traces", [])
            for segment in trace.get("Segments", [])
            if segment.get("Document")
        ]
        return matching_trace(normalize_xray(documents), query)


class XRayBackendFactory:
    name = "xray"

    def create(
        self,
        options: Mapping[str, Any],
        *,
        region: str,
    ) -> PollingBackend:
        return XRayBackend(boto3.client("xray", region_name=region))
