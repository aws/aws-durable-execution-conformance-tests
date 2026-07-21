# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Query traces written by the OpenTelemetry Collector AWS S3 exporter."""

from __future__ import annotations

import gzip
import json
import os
import urllib.parse
from collections.abc import Iterable, Mapping
from compression import zstd
from pathlib import PurePosixPath
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from google.protobuf.message import DecodeError

from aws_durable_execution_conformance_tests_otel.backends._common import (
    matching_trace,
)
from aws_durable_execution_conformance_tests_otel.model import TelemetryQuery, Trace
from aws_durable_execution_conformance_tests_otel.normalizers import (
    normalize_otlp_json,
    normalize_otlp_protobuf,
)
from aws_durable_execution_conformance_tests_otel.polling import (
    BackendError,
    PollingBackend,
)

_TRACE_SUFFIXES = (
    ".json",
    ".json.gz",
    ".json.zst",
    ".binpb",
    ".binpb.gz",
    ".binpb.zst",
)


def _parse_s3_uri(value: str) -> tuple[str, str]:
    parsed = urllib.parse.urlparse(value)
    if parsed.scheme != "s3" or not parsed.netloc or parsed.query or parsed.fragment:
        raise BackendError(
            "The collector backend requires an S3 location such as 's3://bucket/prefix' in --otel-backend-endpoint"
        )
    return parsed.netloc, urllib.parse.unquote(parsed.path.lstrip("/"))


def _is_trace_object(key: str) -> bool:
    name = PurePosixPath(key).name.lower()
    return "traces_" in name and name.endswith(_TRACE_SUFFIXES)


def _normalize_object(
    payload: bytes,
    *,
    key: str,
    content_encoding: str,
) -> list[Trace]:
    encoding = content_encoding.lower()
    lower_key = key.lower()
    if encoding == "gzip" or lower_key.endswith(".gz"):
        payload = gzip.decompress(payload)
    elif encoding == "zstd" or lower_key.endswith(".zst"):
        payload = zstd.decompress(payload)

    uncompressed_key = lower_key.removesuffix(".gz").removesuffix(".zst")
    if uncompressed_key.endswith(".json"):
        document = json.loads(payload)
        if not isinstance(document, Mapping):
            raise ValueError("OTLP JSON document must be an object")
        traces = normalize_otlp_json(document)
    else:
        traces = normalize_otlp_protobuf(payload)
    if not traces:
        raise ValueError("OTLP file contains no spans")
    return traces


def _merge_trace_files(
    files: Iterable[tuple[str, Iterable[Trace]]],
    *,
    bucket: str,
) -> list[Trace]:
    spans: dict[str, dict[str, Any]] = {}
    log_trace_ids: dict[str, list[str]] = {}
    source_keys: dict[str, list[str]] = {}
    for key, traces in files:
        for trace in traces:
            by_span = spans.setdefault(trace.trace_id, {})
            by_span.update({span.span_id: span for span in trace.spans})
            logs = log_trace_ids.setdefault(trace.trace_id, [])
            logs.extend(value for value in trace.log_trace_ids if value not in logs)
            keys = source_keys.setdefault(trace.trace_id, [])
            if key not in keys:
                keys.append(key)
    return [
        Trace(
            trace_id=trace_id,
            spans=tuple(by_span.values()),
            log_trace_ids=tuple(log_trace_ids[trace_id]),
            raw_artifact={
                "s3_bucket": bucket,
                "s3_keys": source_keys[trace_id],
            },
        )
        for trace_id, by_span in spans.items()
    ]


class CollectorBackend(PollingBackend):
    """Query AWS S3 exporter trace objects stored under an S3 prefix."""

    name = "collector"

    def __init__(
        self,
        client: Any,
        bucket: str,
        prefix: str = "",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._client = client
        self._bucket = bucket
        self._prefix = prefix

    def _lookup(self, query: TelemetryQuery) -> Trace | None:
        try:
            files = [(key, self._read_traces(key)) for key in self._list_keys()]
        except BackendError:
            raise
        except (BotoCoreError, ClientError) as exc:
            raise BackendError(f"S3 telemetry query failed: {type(exc).__name__}") from exc
        return matching_trace(
            _merge_trace_files(files, bucket=self._bucket),
            query,
        )

    def _list_keys(self) -> list[str]:
        keys: list[str] = []
        continuation_token: str | None = None
        while True:
            request: dict[str, Any] = {
                "Bucket": self._bucket,
                "Prefix": self._prefix,
            }
            if continuation_token:
                request["ContinuationToken"] = continuation_token
            response = self._client.list_objects_v2(**request)
            keys.extend(
                str(item["Key"])
                for item in response.get("Contents", [])
                if item.get("Key") and _is_trace_object(str(item["Key"]))
            )
            if not response.get("IsTruncated"):
                return keys
            continuation_token = response.get("NextContinuationToken")
            if not continuation_token:
                raise BackendError("S3 telemetry listing was truncated without a continuation token")

    def _read_traces(self, key: str) -> list[Trace]:
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        body = response["Body"]
        try:
            payload = body.read()
        finally:
            close = getattr(body, "close", None)
            if close is not None:
                close()
        if not isinstance(payload, bytes):
            raise BackendError(f"S3 telemetry object {key!r} returned a non-bytes body")
        try:
            return _normalize_object(
                payload,
                key=key,
                content_encoding=str(response.get("ContentEncoding", "")),
            )
        except (
            DecodeError,
            UnicodeDecodeError,
            gzip.BadGzipFile,
            json.JSONDecodeError,
            TypeError,
            ValueError,
            zstd.ZstdError,
        ) as exc:
            raise BackendError(
                f"S3 telemetry object {key!r} is not a supported AWS S3 exporter OTLP JSON or protobuf file"
            ) from exc


class CollectorBackendFactory:
    name = "collector"

    def create(
        self,
        options: Mapping[str, Any],
        *,
        region: str,
    ) -> PollingBackend:
        location = str(options.get("otel_backend_endpoint") or os.environ.get("OTEL_COLLECTOR_S3_URI") or "")
        bucket, prefix = _parse_s3_uri(location)
        return CollectorBackend(
            boto3.client("s3", region_name=region),
            bucket,
            prefix,
        )
