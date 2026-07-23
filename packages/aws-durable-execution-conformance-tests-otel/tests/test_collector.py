# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Amazon S3 OTLP file backend tests."""

from __future__ import annotations

import gzip
import json
from compression import zstd
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)

from aws_durable_execution_conformance_tests_otel.backends.collector import (
    CollectorBackend,
    CollectorBackendFactory,
)
from aws_durable_execution_conformance_tests_otel.model import TelemetryQuery
from aws_durable_execution_conformance_tests_otel.polling import (
    BackendError,
    PollingPolicy,
)

COLLECTOR_DIR = Path(__file__).resolve().parents[1] / "examples" / "collector"


class _Body:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self.closed = False

    def read(self) -> bytes:
        return self._payload

    def close(self) -> None:
        self.closed = True


class _S3:
    def __init__(
        self,
        objects: dict[str, tuple[bytes, str]],
        *,
        page_size: int = 1000,
    ) -> None:
        self.objects = objects
        self.page_size = page_size
        self.list_calls: list[dict[str, Any]] = []
        self.get_calls: list[dict[str, Any]] = []
        self.bodies: list[_Body] = []

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        self.list_calls.append(kwargs)
        keys = [key for key in sorted(self.objects) if key.startswith(str(kwargs["Prefix"]))]
        offset = int(kwargs.get("ContinuationToken", 0))
        page = keys[offset : offset + self.page_size]
        next_offset = offset + len(page)
        truncated = next_offset < len(keys)
        return {
            "Contents": [{"Key": key} for key in page],
            "IsTruncated": truncated,
            **({"NextContinuationToken": str(next_offset)} if truncated else {}),
        }

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        self.get_calls.append(kwargs)
        payload, content_encoding = self.objects[str(kwargs["Key"])]
        body = _Body(payload)
        self.bodies.append(body)
        return {
            "Body": body,
            **({"ContentEncoding": content_encoding} if content_encoding else {}),
        }


def _json_payload(
    *,
    span_id: str,
    execution_arn: str,
    invocation_id: str,
) -> dict[str, Any]:
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {
                            "key": "service.name",
                            "value": {"stringValue": "conformance"},
                        }
                    ]
                },
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "traceId": "1" * 32,
                                "spanId": span_id,
                                "name": "step",
                                "startTimeUnixNano": "1000000000",
                                "endTimeUnixNano": "2000000000",
                                "attributes": [
                                    {
                                        "key": "durable.execution.arn",
                                        "value": {"stringValue": execution_arn},
                                    },
                                    {
                                        "key": "faas.invocation_id",
                                        "value": {"stringValue": invocation_id},
                                    },
                                ],
                            }
                        ]
                    }
                ],
            }
        ]
    }


def _query(execution_arn: str = "arn:test") -> TelemetryQuery:
    now = datetime.now(UTC)
    return TelemetryQuery(execution_arn, "conformance", now, now)


def test_collector_merges_otlp_json_files_across_s3_pages() -> None:
    client = _S3(
        {
            "runs/current/year=2026/month=07/day=21/traces_100.json": (
                json.dumps(
                    _json_payload(
                        span_id="2" * 16,
                        execution_arn="arn:test",
                        invocation_id="invoke-1",
                    )
                ).encode(),
                "",
            ),
            "runs/current/year=2026/month=07/day=21/traces_101.json.gz": (
                gzip.compress(
                    json.dumps(
                        _json_payload(
                            span_id="3" * 16,
                            execution_arn="arn:test",
                            invocation_id="invoke-2",
                        )
                    ).encode()
                ),
                "gzip",
            ),
            "runs/current/year=2026/month=07/day=21/logs_102.json": (
                b'{"not": "trace data"}',
                "",
            ),
        },
        page_size=1,
    )
    backend = CollectorBackend(
        client,
        "telemetry-bucket",
        "runs/current/",
        sleep=lambda _seconds: None,
    )

    trace = backend.find_trace(
        _query(),
        PollingPolicy(timeout_seconds=1, interval_seconds=0, max_attempts=1),
    )

    assert {span.span_id for span in trace.spans} == {"2" * 16, "3" * 16}
    assert {span.attributes["faas.invocation_id"] for span in trace.spans} == {
        "invoke-1",
        "invoke-2",
    }
    assert client.list_calls[1]["ContinuationToken"] == "1"
    assert len(client.get_calls) == 2
    assert all("traces_" in call["Key"] for call in client.get_calls)
    assert all(call["Bucket"] == "telemetry-bucket" for call in client.get_calls)
    assert all(body.closed for body in client.bodies)


def test_collector_reads_otlp_protobuf_files() -> None:
    payload = ExportTraceServiceRequest()
    resource_spans = payload.resource_spans.add()
    resource_attr = resource_spans.resource.attributes.add()
    resource_attr.key = "service.name"
    resource_attr.value.string_value = "conformance"
    span = resource_spans.scope_spans.add().spans.add()
    span.trace_id = bytes.fromhex("4" * 32)
    span.span_id = bytes.fromhex("5" * 16)
    span.name = "protobuf span"
    span.kind = 1
    span.start_time_unix_nano = 1_000_000_000
    span.end_time_unix_nano = 2_000_000_000
    execution_attr = span.attributes.add()
    execution_attr.key = "durable.execution.arn"
    execution_attr.value.string_value = "arn:protobuf"
    client = _S3(
        {
            "otel/year=2026/month=07/day=21/traces_103.binpb.zst": (
                zstd.compress(payload.SerializeToString()),
                "zstd",
            )
        }
    )
    backend = CollectorBackend(
        client,
        "telemetry-bucket",
        "otel/",
        sleep=lambda _seconds: None,
    )

    trace = backend.find_trace(
        _query("arn:protobuf"),
        PollingPolicy(timeout_seconds=1, interval_seconds=0, max_attempts=1),
    )

    assert trace.trace_id == "4" * 32
    assert trace.spans[0].name == "protobuf span"
    assert trace.spans[0].kind == "INTERNAL"


def test_collector_only_downloads_each_immutable_s3_object_once() -> None:
    first_key = "otel/year=2026/month=07/day=21/traces_100.json"
    second_key = "otel/year=2026/month=07/day=21/traces_101.json"
    client = _S3(
        {
            first_key: (
                json.dumps(
                    _json_payload(
                        span_id="2" * 16,
                        execution_arn="arn:test",
                        invocation_id="invoke-1",
                    )
                ).encode(),
                "",
            ),
        }
    )
    backend = CollectorBackend(client, "telemetry-bucket", "otel/")

    first_trace = backend._lookup(_query())
    assert first_trace is not None
    assert len(client.get_calls) == 1

    assert backend._lookup(_query()) == first_trace
    assert len(client.get_calls) == 1

    client.objects[second_key] = (
        json.dumps(
            _json_payload(
                span_id="3" * 16,
                execution_arn="arn:test",
                invocation_id="invoke-2",
            )
        ).encode(),
        "",
    )
    complete_trace = backend._lookup(_query())

    assert complete_trace is not None
    assert {span.span_id for span in complete_trace.spans} == {"2" * 16, "3" * 16}
    assert [call["Key"] for call in client.get_calls] == [first_key, second_key]


def test_collector_rejects_s3_objects_without_readable_bodies() -> None:
    class _MissingBodyS3(_S3):
        def get_object(self, **kwargs: Any) -> dict[str, Any]:
            self.get_calls.append(kwargs)
            return {}

    client = _MissingBodyS3(
        {
            "otel/year=2026/month=07/day=21/traces_100.json": (
                b"unused",
                "",
            )
        }
    )
    backend = CollectorBackend(client, "telemetry-bucket", "otel/")

    with pytest.raises(BackendError, match="readable body"):
        backend._lookup(_query())


def test_collector_factory_requires_an_s3_backend_location() -> None:
    with pytest.raises(BackendError, match="s3://bucket/prefix"):
        CollectorBackendFactory().create(
            {"otel_backend_endpoint": "https://collector.example"},
            region="us-west-2",
        )


def test_custom_collector_layer_includes_the_s3_exporter_and_config() -> None:
    config = (COLLECTOR_DIR / "config.yaml").read_text(encoding="utf-8")
    build_script = (COLLECTOR_DIR / "build-lambda-layer.sh").read_text(encoding="utf-8")
    component = (COLLECTOR_DIR / "lambda" / "awss3.go").read_text(encoding="utf-8")

    assert "endpoint: localhost:4318" in config
    assert "marshaler: otlp_json" in config
    assert "compression: gzip" in config
    assert "awss3_version=v0.151.0" in build_script
    assert 'cp "$script_dir/config.yaml" "$collector_dir/config-s3.yaml"' in build_script
    assert "awss3exporter@$awss3_version" in build_script
    assert 'cd "$collector_dir"\n  go mod tidy' in build_script
    assert "lambdacomponents.exporter.awss3" in build_script
    assert "awss3exporter.NewFactory()" in component
