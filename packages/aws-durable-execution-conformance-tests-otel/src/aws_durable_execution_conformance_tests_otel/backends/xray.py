# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""AWS X-Ray telemetry backend."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from aws_durable_execution_conformance_tests_otel.backends._common import (
    matching_trace,
)
from aws_durable_execution_conformance_tests_otel.model import TelemetryQuery, Trace
from aws_durable_execution_conformance_tests_otel.normalizers import normalize_xray
from aws_durable_execution_conformance_tests_otel.polling import (
    BackendError,
    PollingBackend,
)


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
