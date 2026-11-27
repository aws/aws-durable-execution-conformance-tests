# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
import io
import json
from typing import Any
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from aws_durable_execution_conformance_tests.lmi.errors import ProvisioningError
from aws_durable_execution_conformance_tests.lmi.quarantine import (
    FenceRetryUncertain,
    acquire_fence,
    fence_key,
    release_fence,
)


def test_lost_acquisition_acknowledgment_is_reconciled_and_delete_is_conditional():
    storage = Mock()
    manifest = {"bucket": "bucket", "run": "run", "commit": "commit"}
    record: dict[str, str] = {}

    def read(**_):
        if not record:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(json.dumps(record).encode()), "ETag": '"own-etag"'}

    def put(*, Body, **_):
        record.update(json.loads(Body))
        raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")

    storage.get_object.side_effect = read
    storage.put_object.side_effect = put
    token = acquire_fence(storage, manifest, "target")
    assert record["token"] == token
    release_fence(storage, manifest, "target", token)
    storage.delete_object.assert_called_once_with(Bucket="bucket", Key=fence_key("target"), IfMatch='"own-etag"')


def test_lost_delete_response_retry_cannot_delete_replacement_fence():
    storage = Mock()
    manifest = {"bucket": "bucket", "run": "run", "commit": "commit"}
    record = {"resourceId": "target", "runId": "run", "commit": "commit", "token": "original"}
    current: dict[str, Any] = {"record": record, "etag": '"original-etag"'}
    requests = []

    def read(**_):
        return {"Body": io.BytesIO(json.dumps(current["record"]).encode()), "ETag": current["etag"]}

    def delete_with_transport_retry(**request):
        requests.append(request)
        assert request["IfMatch"] == current["etag"]
        # The original delete commits, its response is lost, and another
        # owner acquires the same resource before the client's retry.
        current.update(record={**record, "token": "replacement"}, etag='"replacement-etag"')
        requests.append(request)
        if request["IfMatch"] != current["etag"]:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "DeleteObject")
        current.clear()

    storage.get_object.side_effect = read
    storage.delete_object.side_effect = delete_with_transport_retry
    with pytest.raises(ClientError, match="PreconditionFailed"):
        release_fence(storage, manifest, "target", "original")
    assert [request["IfMatch"] for request in requests] == ['"original-etag"', '"original-etag"']
    assert current["record"]["token"] == "replacement"
    with pytest.raises(ProvisioningError, match="unverified persistent retirement fence"):
        release_fence(storage, manifest, "target", "original")
    storage.delete_object.assert_called_once()


def test_known_single_attempt_fence_recovers_lost_ack_only_with_matching_record():
    from botocore.exceptions import ReadTimeoutError

    storage = Mock()
    manifest = {"bucket": "bucket", "run": "run", "commit": "commit"}
    record = {"resourceId": "target", "runId": "run", "commit": "commit", "token": "own"}
    storage.get_object.side_effect = [
        ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject"),
        {"Body": io.BytesIO(json.dumps(record).encode()), "ETag": '"own"'},
    ]
    storage.put_object.side_effect = ReadTimeoutError(endpoint_url="test")
    before_put = Mock()
    assert acquire_fence(storage, manifest, "target", token="own", before_put=before_put, single_attempt=True) == "own"
    before_put.assert_called_once()
    storage.put_object.assert_called_once()


def test_reported_retries_disable_single_attempt_readback():
    storage = Mock()
    manifest = {"bucket": "bucket", "run": "run", "commit": "commit"}
    storage.get_object.side_effect = ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
    storage.put_object.side_effect = ClientError(
        {"Error": {"Code": "PreconditionFailed"}, "ResponseMetadata": {"RetryAttempts": 1}}, "PutObject"
    )
    with pytest.raises(FenceRetryUncertain, match="single-attempt response"):
        acquire_fence(storage, manifest, "target", token="own", before_put=Mock(), single_attempt=True)
    storage.get_object.assert_called_once()
