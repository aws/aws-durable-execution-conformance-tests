# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: Apache-2.0
"""Lost Invoke responses must preserve one logical execution and launch journal."""

import io
import json
from typing import Any
from unittest.mock import Mock

import boto3
import pytest
from botocore.awsrequest import AWSResponse
from botocore.exceptions import ReadTimeoutError

from aws_durable_execution_conformance_tests.lmi import service
from aws_durable_execution_conformance_tests.lmi.cloud import Cloud
from aws_durable_execution_conformance_tests.lmi.errors import CollectionError

from .test_shared_adapter import FUNCTION, context, deployment, enter


@pytest.mark.parametrize("lose_all_responses", [False, True])
def test_botocore_retry_preserves_journaled_execution_and_lost_response_recovery(
    monkeypatch, tmp_path, lose_all_responses
):
    original_client = boto3.client
    monkeypatch.setattr(
        service.boto3,
        "client",
        lambda *args, **kwargs: original_client(
            *args, **kwargs, aws_access_key_id="test", aws_secret_access_key="test"
        ),
    )
    api = service.client("lambda", "us-west-2")
    monkeypatch.setattr("botocore.endpoint.time.sleep", lambda seconds: None)
    ctx = context()
    cloud = Cloud(
        deployment(),
        "c1",
        artifacts_dir=tmp_path,
        case_context=ctx,
        verify_profile=Mock(),
        client_factory=lambda name, region: api if name == "lambda" else Mock(),
    )
    # A real scenario marker can be longer than Lambda's execution-name limit.
    marker = "logical-launch-" + "m" * 100
    requests = []
    executions: dict[str, dict[str, Any]] = {}

    class Body(io.BytesIO):
        def stream(self, amt=1024, decode_content=False):
            yield self.read()

    def send(request):
        # This is the HTTP transport boundary, below botocore's retry machinery.
        requests.append(request)
        name = request.headers["X-Amz-Durable-Execution-Name"].decode()
        journal = json.loads((tmp_path / "invocations" / f"{marker}.json").read_text())
        assert journal["durableExecutionName"] == ctx.attempts[0]["durableExecutionName"] == name
        assert len(ctx.attempts) == 1
        # Model Lambda's named-execution deduplication after accepting the write.
        if name in executions:
            assert executions[name]["payload"] == request.body
        else:
            executions[name] = {"payload": request.body, "arn": FUNCTION + f"/durable-execution/{name}/id"}
        if len(requests) == 1 or lose_all_responses:
            raise ReadTimeoutError(endpoint_url=request.url)
        return AWSResponse(
            request.url,
            202,
            {"x-amz-durable-execution-arn": executions[name]["arn"], "content-length": "0"},
            Body(b""),
        )

    monkeypatch.setattr(api._endpoint.http_session, "send", send)
    if lose_all_responses:
        with pytest.raises(CollectionError, match="invocation API failed"):
            cloud.start("success", role="victim", marker=marker)
        assert cloud.invocations[0]["arn"] is None
    else:
        cloud.start("success", role="victim", marker=marker)
    assert len(requests) == 2
    assert len(executions) == len(ctx.attempts) == len(cloud.invocations) == 1
    name, accepted = next(iter(executions.items()))
    assert 1 <= len(name) <= 64 and name.isalnum()
    retained = json.loads((tmp_path / "invocations" / f"{marker}.json").read_text())
    assert retained["durableExecutionName"] == name
    if lose_all_responses:
        assert "error" in retained and "arn" not in retained
    else:
        assert retained["arn"] == accepted["arn"]

    # An actual native observation must still reconcile the original launch,
    # including when every response to the accepted Invoke was lost.
    observed = enter(cloud.invocations[0], "original-request", 1)
    observed["execution"] = accepted["arn"]
    cloud._bind_requests([observed])
    assert ctx.attempts[0]["requestId"] == "original-request"
    assert cloud.invocations[0]["arn"] == accepted["arn"]
    assert len(ctx.attempts) == 1


@pytest.mark.parametrize("changed", [None, "run", "case", "marker"])
def test_cloud_execution_names_are_stable_and_scoped_to_the_logical_launch(tmp_path, changed):
    names = []
    for index in range(2):
        manifest = deployment()
        case_id, marker = "case", "marker"
        if index:
            if changed == "run":
                manifest["run"] = "different-run"
            elif changed == "case":
                case_id = "different-case"
            elif changed == "marker":
                marker = "different-marker"
        api = Mock()
        api.invoke.side_effect = lambda **kwargs: {
            "StatusCode": 202,
            "DurableExecutionArn": f"{FUNCTION}/durable-execution/{kwargs['DurableExecutionName']}/id",
        }
        cloud = Cloud(
            manifest,
            "c1",
            artifacts_dir=tmp_path / str(index),
            case_context=context(case_id=case_id),
            verify_profile=Mock(),
            client_factory=lambda *_, api=api: api,
        )
        cloud.start("success", role="victim", marker=marker)
        names.append(api.invoke.call_args.kwargs["DurableExecutionName"])
    assert (names[0] == names[1]) is (changed is None)
