# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: Apache-2.0
"""Bounded AWS transport independent of SDK language and testing libraries."""

from __future__ import annotations

import json
import os
import time
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from .errors import CollectionError


def client(service, region=None):
    return boto3.client(
        service,
        region_name=region or os.environ["AWS_REGION"],
        config=Config(connect_timeout=3, read_timeout=10, retries={"total_max_attempts": 2}),
    )


class DurableFunctionRunner:
    """The small Lambda API surface used by shared scenario drivers."""

    def __init__(self, function_name: str, *, region: str, lambda_client: Any = None):
        self.function_name = function_name
        self.lambda_client = lambda_client if lambda_client is not None else client("lambda", region)

    def run_async(self, input=None) -> str:
        result = self.lambda_client.invoke(
            FunctionName=self.function_name, InvocationType="Event", Payload=json.dumps(input)
        )
        if result.get("StatusCode") != 202 or not result.get("DurableExecutionArn"):
            body = result.get("Payload")
            if body is not None:
                body.close()
            raise CollectionError("Invoke did not accept an identified durable execution")
        return result["DurableExecutionArn"]

    def wait_for_result(self, execution_arn: str, timeout: float = 60):
        deadline = time.monotonic() + timeout
        while True:
            result = self.lambda_client.get_durable_execution(DurableExecutionArn=execution_arn)
            if result["Status"] in {"SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED"}:
                return result
            if time.monotonic() >= deadline:
                raise TimeoutError("Durable execution did not reach a terminal state")
            time.sleep(min(0.5, max(0, deadline - time.monotonic())))

    def wait_for_callback(self, execution_arn: str, name: str, timeout: float = 60):
        deadline = time.monotonic() + timeout
        while True:
            marker = None
            try:
                while True:
                    request = {"DurableExecutionArn": execution_arn, "IncludeExecutionData": True}
                    if marker:
                        request["Marker"] = marker
                    page = self.lambda_client.get_durable_execution_history(**request)
                    for event in page.get("Events", []):
                        if event.get("EventType") == "CallbackStarted" and event.get("Name") == name:
                            callback = event.get("CallbackStartedDetails", {}).get("CallbackId")
                            if callback:
                                return callback
                    marker = page.get("NextMarker")
                    if not marker:
                        break
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Callback history did not complete before its deadline")
            except ClientError as error:
                if error.response["Error"]["Code"] != "ResourceNotFoundException":
                    raise
            if time.monotonic() >= deadline:
                raise TimeoutError("Callback did not become available")
            time.sleep(min(0.5, max(0, deadline - time.monotonic())))

    def send_callback_success(self, callback_id: str, result: bytes | None = None):
        self.lambda_client.send_durable_execution_callback_success(CallbackId=callback_id, Result=result)
