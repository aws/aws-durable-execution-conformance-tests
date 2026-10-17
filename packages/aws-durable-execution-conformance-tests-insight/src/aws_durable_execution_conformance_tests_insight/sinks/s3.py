# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""S3 sink -- reads canonical ``operations``-array records written by ``S3Exporter``.

``S3Exporter`` writes ``JSON.stringify(record)`` (the lossless ``operations``
array) to ``s3://bucket/prefix/<partition>/<executionName>.json``, overwriting
the same object per execution. This sink lists every object under the prefix,
fetches each JSON body, and keeps only records whose top-level ``executionArn``
equals the runner's ``execution_arn`` -- so partitioning is irrelevant and other
executions sharing the bucket are ignored.
"""

from __future__ import annotations

import json
import os
import urllib.parse
from collections.abc import Mapping
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from aws_durable_execution_conformance_tests_insight.model import InsightRecord, RecordQuery
from aws_durable_execution_conformance_tests_insight.normalizers import (
    NormalizationError,
    normalize_record,
)
from aws_durable_execution_conformance_tests_insight.polling import (
    PollingSink,
    SinkCapability,
    SinkError,
)


def _parse_s3_uri(value: str) -> tuple[str, str]:
    parsed = urllib.parse.urlparse(value)
    if parsed.scheme != "s3" or not parsed.netloc or parsed.query or parsed.fragment:
        raise SinkError("The s3 sink requires an S3 location such as 's3://bucket/prefix' in --insight-sink-endpoint")
    return parsed.netloc, urllib.parse.unquote(parsed.path.lstrip("/"))


class S3Sink(PollingSink):
    name = "s3"
    capability = SinkCapability.OPERATIONS_ARRAY

    def __init__(self, client: Any, bucket: str, prefix: str = "", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._client = client
        self._bucket = bucket
        self._prefix = prefix

    def _lookup(self, query: RecordQuery) -> list[InsightRecord] | None:
        try:
            payloads: list[Mapping[str, Any]] = []
            for key in self._list_keys():
                payload = self._read_object(key)
                if isinstance(payload, Mapping) and payload.get("executionArn") == query.execution_arn:
                    payloads.append(payload)
        except SinkError:
            raise
        except (BotoCoreError, ClientError) as exc:
            raise SinkError(f"S3 insight query failed: {type(exc).__name__}") from exc

        if not payloads:
            return None
        payloads.sort(key=lambda item: str(item.get("emittedAt", "")))
        try:
            return [normalize_record(payload) for payload in payloads]
        except NormalizationError as exc:
            raise SinkError(f"S3 insight object is not a Workflow Insight record: {exc}") from exc

    def _list_keys(self) -> list[str]:
        keys: list[str] = []
        continuation_token: str | None = None
        while True:
            request: dict[str, Any] = {"Bucket": self._bucket, "Prefix": self._prefix}
            if continuation_token:
                request["ContinuationToken"] = continuation_token
            response = self._client.list_objects_v2(**request)
            keys.extend(
                str(item["Key"])
                for item in response.get("Contents", [])
                if item.get("Key") and str(item["Key"]).endswith(".json")
            )
            if not response.get("IsTruncated"):
                return keys
            continuation_token = response.get("NextContinuationToken")
            if not continuation_token:
                raise SinkError("S3 insight listing was truncated without a continuation token")

    def _read_object(self, key: str) -> Any:
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        body = response.get("Body")
        read = getattr(body, "read", None)
        if not callable(read):
            raise SinkError(f"S3 insight object {key!r} did not return a readable body")
        try:
            payload = read()
        finally:
            close = getattr(body, "close", None)
            if callable(close):
                close()
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        try:
            return json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as exc:
            raise SinkError(f"S3 insight object {key!r} is not valid JSON") from exc


class S3SinkFactory:
    name = "s3"
    capability = SinkCapability.OPERATIONS_ARRAY
    client_services: tuple[str, ...] = ("s3",)

    def _location(self, options: Mapping[str, Any]) -> tuple[str, str]:
        location = str(options.get("insight_sink_endpoint") or os.environ.get("INSIGHT_S3_URI") or "")
        return _parse_s3_uri(location)

    def validate_configuration(self, args: Any) -> None:
        self._location(vars(args))

    def deployment_parameters(self, args: Any) -> dict[str, str]:
        bucket, prefix = self._location(vars(args))
        parameters = {"InsightSink": "s3", "InsightS3Bucket": bucket}
        if prefix:
            # S3Exporter concatenates the prefix verbatim (its default is
            # "workflow-insight/"), so ensure the injected prefix keeps keys
            # under the expected folder (and inside its IAM grant).
            parameters["InsightS3Prefix"] = prefix if prefix.endswith("/") else prefix + "/"
        return parameters

    def create(self, options: Mapping[str, Any], *, region: str, function_name: str) -> PollingSink:
        del function_name
        bucket, prefix = self._location(options)
        return S3Sink(boto3.client("s3", region_name=region), bucket, prefix)

    def create_with_clients(
        self,
        options: Mapping[str, Any],
        *,
        region: str,
        function_name: str,
        aws_clients: Mapping[str, Any],
    ) -> PollingSink:
        del function_name
        bucket, prefix = self._location(options)
        try:
            client = aws_clients["s3"]
        except KeyError:
            client = boto3.client("s3", region_name=region)
        return S3Sink(client, bucket, prefix)
