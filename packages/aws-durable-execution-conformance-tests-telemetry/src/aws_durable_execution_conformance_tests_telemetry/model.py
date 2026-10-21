# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Data model for Durable Execution Telemetry records retrieved from CloudWatch Logs."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

RECORD_TYPE = "DurableExecutionTelemetry"


@dataclass(frozen=True)
class ExpectedExecution:
    """Identity fields the record must carry, derived from the execution ARN."""

    execution_arn: str
    function_name: str
    qualifier: str
    region: str
    account_id: str
    execution_name: str


def parse_execution_arn(arn: str) -> ExpectedExecution:
    """Derive the expected identity fields from a durable execution ARN.

    The ARN shape is
    ``arn:<partition>:lambda:<region>:<account>:function:<name>:<qualifier>/durable-execution/<execution>/<invocation>``.
    This parser is written independently of the plugin's parser so the
    conformance check exercises the plugin's output rather than mirroring it.

    Raises:
        ValueError: When the ARN has fewer than eight colon-separated parts,
            the resource path is not ``<qualifier>/durable-execution/<execution>/...``,
            or any of region, account, function name, qualifier, or execution
            name is empty.
    """
    parts = arn.split(":")
    if len(parts) < 8:
        raise ValueError(f"Execution ARN must have at least 8 colon-separated parts: {arn!r}")
    region = parts[3]
    account_id = parts[4]
    function_name = parts[6]
    resource = parts[7].split("/")
    if len(resource) < 3 or resource[1] != "durable-execution":
        raise ValueError(f"Execution ARN resource must be <qualifier>/durable-execution/<execution>/...: {arn!r}")
    qualifier = resource[0]
    execution_name = resource[2]
    for label, value in (
        ("region", region),
        ("account", account_id),
        ("function name", function_name),
        ("qualifier", qualifier),
        ("execution name", execution_name),
    ):
        if not value:
            raise ValueError(f"Execution ARN has an empty {label}: {arn!r}")
    return ExpectedExecution(
        execution_arn=arn,
        function_name=function_name,
        qualifier=qualifier,
        region=region,
        account_id=account_id,
        execution_name=execution_name,
    )


@dataclass(frozen=True)
class TelemetryRecord:
    """One parsed Durable Execution Telemetry record and the CloudWatch event it came from."""

    event: Mapping[str, Any]
    data: Mapping[str, Any]


def parse_record(event: Mapping[str, Any]) -> TelemetryRecord | None:
    """Parse one CloudWatch event into an :class:`TelemetryRecord`.

    Returns ``None`` when the message is not JSON, is not a JSON object, or its
    ``recordType`` is not ``DurableExecutionTelemetry``.
    """
    message = event.get("message")
    if not isinstance(message, str):
        return None

    try:
        parsed = json.loads(message)
    except ValueError:
        return None
    if not isinstance(parsed, dict) or parsed.get("recordType") != RECORD_TYPE:
        return None

    return TelemetryRecord(event=event, data=parsed)
