# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Persistent retirement fences, independent of the workflow scheduling lock.

A fence is acquired before case admission and removed only after audited
quiescence. A retained fence requires external runtime reconciliation; terminal
durable execution status alone does not justify removing it.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Callable
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from .errors import ProvisioningError
from .service import definitely_not_sent, definitive_s3_write_rejection


class FenceRetryUncertain(ProvisioningError):
    """The transport did not provide the expected single-attempt receipt."""


def fence_key(resource_id: str) -> str:
    identity = hashlib.sha256(resource_id.encode()).hexdigest()
    return f"quarantine/{identity}.json"


def read_fence(storage: Any, bucket: str, resource_id: str) -> dict[str, Any] | None:
    try:
        response = storage.get_object(Bucket=bucket, Key=fence_key(resource_id))
    except ClientError as error:
        if error.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise
    with response["Body"] as stream:
        record = json.loads(stream.read())
    if not isinstance(record, dict) or record.get("resourceId") != resource_id or not record.get("runId"):
        raise ProvisioningError("Persistent retirement fence is malformed; external reconciliation is required")
    return {**record, "etag": response.get("ETag")}


def fence_matches(record, manifest, resource_id: str, token: str) -> bool:
    return (
        record is not None
        and record.get("resourceId") == resource_id
        and record.get("runId") == manifest["run"]
        and record.get("commit") == manifest["commit"]
        and record.get("token") == token
        and bool(record.get("etag"))
    )


def require_unfenced(storage: Any, bucket: str, resource_id: str) -> None:
    record = read_fence(storage, bucket, resource_id)
    if record is not None:
        raise ProvisioningError(
            f"Persistent target {resource_id} is quarantined by run {record['runId']}; "
            f"reconcile its runtime work before removing s3://{bucket}/{fence_key(resource_id)}"
        )


def acquire_fence(
    storage: Any,
    manifest: dict[str, Any],
    resource_id: str,
    *,
    token: str | None = None,
    before_put: Callable[[], None] | None = None,
    single_attempt: bool = False,
) -> str:
    require_unfenced(storage, manifest["bucket"], resource_id)
    token = token or uuid.uuid4().hex
    record = {
        "token": token,
        "resourceId": resource_id,
        "runId": manifest["run"],
        "commit": manifest["commit"],
        "evidencePrefix": f"runs/{manifest['run']}/",
    }
    if before_put is not None:
        before_put()
    try:
        response = storage.put_object(
            Bucket=manifest["bucket"],
            Key=fence_key(resource_id),
            Body=json.dumps(record).encode(),
            IfNoneMatch="*",
        )
    except (BotoCoreError, ClientError) as error:
        if definitely_not_sent(error, single_attempt=single_attempt):
            # The PUT never reached S3: no readback can prove or disprove a
            # write that was never attempted, and no state was persisted.
            raise
        if before_put is not None and isinstance(error, ClientError):
            retries = error.response.get("ResponseMetadata", {}).get("RetryAttempts")
            if retries is not None and (type(retries) is not int or retries != 0):
                raise FenceRetryUncertain("Fence acquisition lacks a confirmed single-attempt response") from error
        if before_put is not None and (not single_attempt or definitive_s3_write_rejection(error, precondition=True)):
            raise
        # With a known single submission, matching readback proves this
        # operation finished. An absent object cannot prove rejection.
        try:
            actual = read_fence(storage, manifest["bucket"], resource_id)
        except Exception as read_error:
            error.add_note(f"Could not reconcile acquiring fence: {read_error}")
            raise error from read_error
        if fence_matches(actual, manifest, resource_id, token):
            return token
        if before_put is not None:
            raise
        if isinstance(error, ClientError) and error.response["Error"]["Code"] in {
            "PreconditionFailed",
            "ConditionalRequestConflict",
        }:
            raise ProvisioningError("Persistent target acquired another retirement fence; admission refused") from error
        raise
    if before_put is not None:
        retries = response.get("ResponseMetadata", {}).get("RetryAttempts") if isinstance(response, dict) else None
        if type(retries) is not int or retries != 0:
            raise FenceRetryUncertain("Fence acquisition lacks a confirmed single-attempt response")
    return token


def release_fence(storage: Any, manifest: dict[str, Any], resource_id: str, token: str) -> None:
    record = read_fence(storage, manifest["bucket"], resource_id)
    if record is None or not fence_matches(record, manifest, resource_id, token):
        raise ProvisioningError("Refusing to clear an unverified persistent retirement fence")
    storage.delete_object(Bucket=manifest["bucket"], Key=fence_key(resource_id), IfMatch=record["etag"])
