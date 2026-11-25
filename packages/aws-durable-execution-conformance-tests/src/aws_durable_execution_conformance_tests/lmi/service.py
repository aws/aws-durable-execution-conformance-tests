# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Bounded AWS transport independent of SDK language and testing libraries."""

from __future__ import annotations

import json
import os
import time
import uuid
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import (
    BotoCoreError,
    ChecksumError,
    ClientError,
    ConnectionError,
    FlexibleChecksumError,
    HTTPClientError,
    IncompleteReadError,
    ResponseStreamingError,
)

from aws_durable_execution_conformance_tests.validate import inherit_wait_for_callback_event_names

from .errors import CollectionError

TERMINAL_EXECUTION_STATUSES = frozenset({"SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED"})
OUTSTANDING_EXECUTION_STATUSES = ("RUNNING", "PAUSING", "PAUSED")


def client(service, region=None, *, single_attempt=False, connect_timeout=3, read_timeout=10):
    return boto3.client(
        service,
        region_name=region or os.environ["AWS_REGION"],
        config=Config(
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            retries={"total_max_attempts": 1 if single_attempt else 2},
        ),
    )


def definitive_s3_write_rejection(error: Exception, *, precondition: bool = False) -> bool:
    if not isinstance(error, ClientError):
        return False
    retries = error.response.get("ResponseMetadata", {}).get("RetryAttempts")
    if type(retries) is not int or retries != 0:
        return False
    code = error.response["Error"]["Code"]
    return code in {
        "AccessDenied",
        "ExpiredToken",
        "ExpiredTokenException",
        "InvalidToken",
        "InvalidRequest",
        "InvalidArgument",
        "NoSuchBucket",
    } or (precondition and code in {"PreconditionFailed", "ConditionalRequestConflict"})


def release_control(storage, bucket: str, key: str, *, allow_absent: bool = False) -> None:
    """Replace a held control conditionally so a late retry cannot recreate it."""
    request = {"Bucket": bucket, "Key": key}
    try:
        current = storage.get_object(**request)
    except ClientError as error:
        if not allow_absent or error.response["Error"]["Code"] not in {"NoSuchKey", "404", "NotFound"}:
            raise
        condition = {"IfNoneMatch": "*"}
    else:
        with current["Body"] as stream:
            state = stream.read()
        if state == b"release":
            return
        if state != b"hold":
            raise CollectionError(f"Unexpected content for control {key}")
        etag = current.get("ETag")
        if not isinstance(etag, str) or not etag:
            raise CollectionError(f"Cannot verify the held ETag for control {key}")
        condition = {"IfMatch": etag}
    for attempt in range(2):
        try:
            storage.put_object(**request, Body=b"release", **condition)
        except (BotoCoreError, ClientError) as error:
            observed = storage.get_object(**request)
            with observed["Body"] as stream:
                state = stream.read()
            if state == b"release":
                return
            if not (
                attempt == 0
                and "IfNoneMatch" in condition
                and state == b"hold"
                and (
                    retryable_read_error(error)
                    or (
                        isinstance(error, ClientError)
                        and error.response["Error"]["Code"] in {"PreconditionFailed", "ConditionalRequestConflict"}
                    )
                )
            ):
                raise
            etag = observed.get("ETag")
            if not isinstance(etag, str) or not etag:
                raise CollectionError(f"Cannot verify the held ETag for control {key}") from error
            condition = {"IfMatch": etag}
        else:
            return


def retryable_read_error(error: Exception) -> bool:
    if isinstance(
        error,
        (
            ConnectionError,
            HTTPClientError,
            IncompleteReadError,
            ResponseStreamingError,
            FlexibleChecksumError,
            ChecksumError,
        ),
    ):
        return True
    if not isinstance(error, ClientError):
        return False
    kind = error.response["Error"]["Code"]
    status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
    return (
        kind in {"ResourceNotFoundException", "TooManyRequestsException", "SlowDown", "RequestTimeout"}
        or kind.startswith("Throttl")
        or status >= 500
    )


def request_execution_stops(service, executions, *, timeout=30) -> dict[str, Exception]:
    """Retry service visibility independently for the executions the caller verified."""
    deadline = time.monotonic() + timeout
    pending = set(executions)
    errors: dict[str, Exception] = {}
    while pending:
        for arn in sorted(pending):
            try:
                status = service.get_durable_execution(DurableExecutionArn=arn)["Status"]
                if status == "PAUSING":
                    errors[arn] = CollectionError("Execution did not finish pausing before cleanup deadline")
                    continue
                if status in {"RUNNING", "PAUSED"}:
                    try:
                        service.stop_durable_execution(DurableExecutionArn=arn)
                    except ClientError as error:
                        if error.response["Error"]["Code"] != "InvalidParameterValueException":
                            raise
                        latest = service.get_durable_execution(DurableExecutionArn=arn)["Status"]
                        if latest not in TERMINAL_EXECUTION_STATUSES:
                            if latest == "PAUSING":
                                errors[arn] = error
                                continue
                            raise
            except Exception as error:
                errors[arn] = error
                if retryable_read_error(error):
                    continue
            else:
                errors.pop(arn, None)
            pending.remove(arn)
        remaining = deadline - time.monotonic()
        if not pending or remaining <= 0:
            return errors
        time.sleep(min(0.5, remaining))
    return errors


class DurableFunctionRunner:
    """The small Lambda API surface used by shared scenario drivers."""

    def __init__(self, function_name: str, *, region: str, lambda_client: Any = None):
        self.function_name = function_name
        self.lambda_client = lambda_client if lambda_client is not None else client("lambda", region)

    def run_async(self, input=None, *, execution_name: str | None = None) -> str:
        # Resolve once per logical invocation, before botocore can retry the
        # transport. A lost response must not create another durable execution.
        name = execution_name if execution_name is not None else uuid.uuid4().hex
        result = self.lambda_client.invoke(
            FunctionName=self.function_name,
            InvocationType="Event",
            Payload=json.dumps(input),
            DurableExecutionName=name,
        )
        if result.get("StatusCode") != 202 or not result.get("DurableExecutionArn"):
            body = result.get("Payload")
            if body is not None:
                body.close()
            raise CollectionError("Invoke did not accept an identified durable execution")
        return result["DurableExecutionArn"]

    def wait_for_result(self, execution_arn: str, timeout: float = 60):
        deadline = time.monotonic() + timeout
        transient = None
        while True:
            try:
                result = self.lambda_client.get_durable_execution(DurableExecutionArn=execution_arn)
            except Exception as error:
                if not retryable_read_error(error):
                    raise
                transient = error
            else:
                if result["Status"] in TERMINAL_EXECUTION_STATUSES:
                    return result
            if time.monotonic() >= deadline:
                raise TimeoutError("Durable execution did not reach a terminal state") from transient
            time.sleep(min(0.5, max(0, deadline - time.monotonic())))

    def wait_for_callback(self, execution_arn: str, name: str, timeout: float = 60):
        deadline = time.monotonic() + timeout
        transient = None
        completed: set[str] = set()
        while True:
            marker = None
            history: list[dict[str, Any]] = []
            try:
                while True:
                    request = {"DurableExecutionArn": execution_arn, "IncludeExecutionData": True}
                    if marker:
                        request["Marker"] = marker
                    page = self.lambda_client.get_durable_execution_history(**request)
                    events = page.get("Events", [])
                    history.extend(events)
                    completed.update(
                        identity
                        for event in events
                        if event.get("EventType") in {"CallbackSucceeded", "CallbackFailed", "CallbackTimedOut"}
                        and isinstance(identity := event.get("Id"), str)
                    )
                    if any(
                        event.get("EventType")
                        in {"ExecutionSucceeded", "ExecutionFailed", "ExecutionTimedOut", "ExecutionStopped"}
                        for event in events
                    ):
                        raise CollectionError("Execution completed before an outstanding callback could be obtained")

                    marker = page.get("NextMarker")
                    if not marker:
                        break
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Callback history did not complete before its deadline")
                inherit_wait_for_callback_event_names(history)
                candidates = [
                    callback
                    for event in history
                    if event.get("EventType") == "CallbackStarted"
                    and event.get("Name") == name
                    and isinstance(event.get("Id"), str)
                    and event["Id"] not in completed
                    and isinstance(callback := event.get("CallbackStartedDetails", {}).get("CallbackId"), str)
                    and callback
                ]
                if len(candidates) == 1:
                    return candidates[0]
            except Exception as error:
                if not retryable_read_error(error):
                    raise
                transient = error
            if time.monotonic() >= deadline:
                raise TimeoutError("Callback did not become available") from transient
            time.sleep(min(0.5, max(0, deadline - time.monotonic())))

    def send_callback_success(self, callback_id: str, result: bytes | None = None):
        if result is None:
            self.lambda_client.send_durable_execution_callback_success(CallbackId=callback_id)
        else:
            self.lambda_client.send_durable_execution_callback_success(CallbackId=callback_id, Result=result)
