# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Persistent retirement fences, independent of the workflow scheduling lock.

A fence is acquired before case admission and removed only after audited
quiescence. A retained fence requires external runtime reconciliation; terminal
durable execution status alone does not justify removing it.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from botocore.exceptions import ClientError

from .errors import ProvisioningError


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
    return record


def require_unfenced(storage: Any, bucket: str, resource_id: str) -> None:
    record = read_fence(storage, bucket, resource_id)
    if record is not None:
        raise ProvisioningError(
            f"Persistent target {resource_id} is quarantined by run {record['runId']}; "
            f"reconcile its runtime work before removing s3://{bucket}/{fence_key(resource_id)}"
        )


def acquire_fence(storage: Any, manifest: dict[str, Any], resource_id: str) -> None:
    require_unfenced(storage, manifest["bucket"], resource_id)
    record = {
        "resourceId": resource_id,
        "runId": manifest["run"],
        "commit": manifest["commit"],
        "evidencePrefix": f"runs/{manifest['run']}/",
    }
    try:
        storage.put_object(
            Bucket=manifest["bucket"],
            Key=fence_key(resource_id),
            Body=json.dumps(record).encode(),
            IfNoneMatch="*",
        )
    except ClientError as error:
        if error.response["Error"]["Code"] in {"PreconditionFailed", "ConditionalRequestConflict"}:
            raise ProvisioningError("Persistent target acquired another retirement fence; admission refused") from error
        raise


def release_fence(storage: Any, manifest: dict[str, Any], resource_id: str) -> None:
    record = read_fence(storage, manifest["bucket"], resource_id)
    if record is None or record["runId"] != manifest["run"] or record.get("commit") != manifest["commit"]:
        raise ProvisioningError("Refusing to clear another run's persistent retirement fence")
    storage.delete_object(Bucket=manifest["bucket"], Key=fence_key(resource_id))
