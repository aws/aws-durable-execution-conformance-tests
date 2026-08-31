# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""CloudWatch sink -- reads ``operationsByName`` records emitted by ``LambdaLogExporter``.

``LambdaLogExporter`` calls ``console.log(JSON.stringify(withOperationsByName(record)))``,
so each record is one JSON line in the function's own CloudWatch log group. This
sink runs ``FilterLogEvents`` over ``/aws/lambda/<function>``, decodes each event
message as JSON, and keeps only Workflow Insight records whose top-level
``executionArn`` equals the runner's ``execution_arn``. Because the log line
carries the name-keyed summary map (not the per-occurrence array), the sink
advertises ``OPERATIONS_BY_NAME`` and requirements needing the array are gated
out by the extension.

The log group is resolved from ``--insight-sink-endpoint`` when given, else via
the core runner's ``CloudWatchLogRetriever`` (CloudFormation logical->physical
resolution, the same model ``ExpectedLogs`` validation uses). Retrieval itself
stays sink-owned: the core's server-side ``$.executionArn`` filter assumes raw
top-level JSON, but nodejs18+ wraps ``console.log`` output in a structured
envelope, so this sink decodes the envelope and filters client-side.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from datetime import datetime
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from aws_durable_execution_conformance_tests.cloudwatch import (
    CloudWatchLogError,
    CloudWatchLogRetriever,
)
from aws_durable_execution_conformance_tests.config import STACK_NAME_PREFIX
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


def _epoch_ms(value: Any) -> int | None:
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    if isinstance(value, (int, float)):
        return int(value)
    return None


class CloudWatchSink(PollingSink):
    name = "cloudwatch"
    capability = SinkCapability.OPERATIONS_BY_NAME

    def __init__(self, client: Any, log_group: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._client = client
        self._log_group = log_group

    def _lookup(self, query: RecordQuery) -> list[InsightRecord] | None:
        request: dict[str, Any] = {"logGroupName": self._log_group}
        start_ms = _epoch_ms(query.started_at)
        if start_ms is not None:
            request["startTime"] = start_ms
        try:
            payloads: list[Mapping[str, Any]] = []
            next_token: str | None = None
            while True:
                if next_token:
                    request["nextToken"] = next_token
                response = self._client.filter_log_events(**request)
                for event in response.get("events", []):
                    payload = self._decode(event.get("message"))
                    if (
                        isinstance(payload, Mapping)
                        and payload.get("recordType") == "WorkflowInsight"
                        and payload.get("executionArn") == query.execution_arn
                    ):
                        payloads.append(payload)
                next_token = response.get("nextToken")
                if not next_token:
                    break
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code == "ResourceNotFoundException":
                # The function's log group is created lazily on its first log
                # write; absence means "no records yet", so let the poll retry.
                return None
            raise SinkError(f"CloudWatch insight query failed: {type(exc).__name__}") from exc
        except BotoCoreError as exc:
            raise SinkError(f"CloudWatch insight query failed: {type(exc).__name__}") from exc

        if not payloads:
            return None
        payloads.sort(key=lambda item: str(item.get("emittedAt", "")))
        try:
            return [normalize_record(payload) for payload in payloads]
        except NormalizationError as exc:
            raise SinkError(f"CloudWatch insight event is not a Workflow Insight record: {exc}") from exc

    @staticmethod
    def _decode(message: Any) -> Any:
        if not isinstance(message, str):
            return None
        try:
            payload = json.loads(message.strip())
        except json.JSONDecodeError:
            return None
        # Lambda's structured JSON logging (nodejs18+ default) wraps each
        # console.log line in an envelope: {"timestamp", "level", "requestId",
        # "message": "<the logged string>"}. Unwrap it so the insight record
        # emitted by LambdaLogExporter is visible at the top level.
        if isinstance(payload, Mapping) and "recordType" not in payload and isinstance(payload.get("message"), str):
            try:
                inner = json.loads(payload["message"].strip())
            except json.JSONDecodeError:
                return payload
            if isinstance(inner, Mapping):
                return inner
        return payload


def _resolve_log_group(
    options: Mapping[str, Any],
    function_name: str,
    aws_clients: Mapping[str, Any],
) -> str:
    """Resolve the function's log group, reusing the core runner's model.

    Precedence: explicit ``--insight-sink-endpoint`` / ``INSIGHT_LOG_GROUP``
    override, else the core's ``CloudWatchLogRetriever.get_log_group_name``
    (CloudFormation logical->physical resolution against the deployed stack,
    named ``{STACK_NAME_PREFIX}-{--name}`` exactly as the core deploys it).
    Resolution failures raise ``SinkError`` loudly -- a silent fallback to
    ``/aws/lambda/<logical-id>`` would poll a nonexistent group forever.
    """
    endpoint = options.get("insight_sink_endpoint") or os.environ.get("INSIGHT_LOG_GROUP")
    if endpoint:
        return str(endpoint)
    name = options.get("name")
    cloudformation = aws_clients.get("cloudformation")
    logs = aws_clients.get("logs")
    if not name or cloudformation is None:
        msg = (
            "cloudwatch sink cannot resolve the function log group: no "
            "--insight-sink-endpoint override and no stack name / "
            "cloudformation client available"
        )
        raise SinkError(msg)
    stack_name = f"{STACK_NAME_PREFIX}-{name}"
    retriever = CloudWatchLogRetriever(
        cloudformation_client=cloudformation,
        logs_client=logs,
    )
    try:
        return str(retriever.get_log_group_name(stack_name, function_name))
    except CloudWatchLogError as exc:
        raise SinkError(f"cloudwatch sink log-group resolution failed: {exc}") from exc


class CloudWatchSinkFactory:
    name = "cloudwatch"
    capability = SinkCapability.OPERATIONS_BY_NAME
    # "cloudformation" is required so _resolve_log_group can map the logical
    # resource id to the physical function name; without it the resolution
    # silently falls back to /aws/lambda/<logical-id>, which does not exist.
    client_services: tuple[str, ...] = ("logs", "cloudformation")

    def validate_configuration(self, args: Any) -> None:
        del args  # The function log group is discovered at validation time.

    def deployment_parameters(self, args: Any) -> dict[str, str]:
        del args  # LambdaLogExporter writes to the function's own log group.
        return {"InsightSink": "cloudwatch"}

    def create(self, options: Mapping[str, Any], *, region: str, function_name: str) -> PollingSink:
        log_group = _resolve_log_group(options, function_name, {})
        return CloudWatchSink(boto3.client("logs", region_name=region), log_group)

    def create_with_clients(
        self,
        options: Mapping[str, Any],
        *,
        region: str,
        function_name: str,
        aws_clients: Mapping[str, Any],
    ) -> PollingSink:
        log_group = _resolve_log_group(options, function_name, aws_clients)
        try:
            client = aws_clients["logs"]
        except KeyError:
            client = boto3.client("logs", region_name=region)
        return CloudWatchSink(client, log_group)
