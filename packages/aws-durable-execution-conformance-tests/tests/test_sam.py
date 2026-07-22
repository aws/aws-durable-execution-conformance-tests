# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the SAM cleanup helper."""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

from aws_durable_execution_conformance_tests import sam
from aws_durable_execution_conformance_tests.sam import Deployer, delete_stack
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_delete_stack_returns_true_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, object] = {}

    class _FakeClient:
        def delete_stack(self, **kwargs: object) -> None:
            calls.update(kwargs)

    def _fake_boto3_client(service: str, region_name: str | None = None) -> _FakeClient:
        calls["service"] = service
        calls["region_name"] = region_name
        return _FakeClient()

    monkeypatch.setattr(sam.boto3, "client", _fake_boto3_client)

    assert delete_stack("my-stack", "us-west-2") is True
    assert calls["service"] == "cloudformation"
    assert calls["region_name"] == "us-west-2"
    assert calls["StackName"] == "my-stack"


def test_delete_stack_is_best_effort_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeClient:
        def delete_stack(self, **_kwargs: object) -> None:
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "no"}}, "DeleteStack")

    monkeypatch.setattr(sam.boto3, "client", lambda *_a, **_k: _FakeClient())

    # Never raises; returns False so the caller can note the best-effort miss.
    assert delete_stack("my-stack", "us-west-2") is False


def test_deploy_passes_but_redacts_secret_parameters(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    template = tmp_path / "template.yaml"
    template.write_text("Resources: {}\n", encoding="utf-8")
    commands: list[list[str]] = []

    def _run(command: list[str]) -> subprocess.CompletedProcess:
        commands.append(command)
        output = "deployed header-secret" if "deploy" in command else "built"
        return subprocess.CompletedProcess(command, 0, stdout=output, stderr="")

    monkeypatch.setattr(sam.SamExecutor, "run", _run)
    deployer = Deployer(str(template), build_dir=str(tmp_path / "build"))
    deployer.build()

    result = deployer.deploy(
        "stack",
        secret_parameter_overrides={"OtelExporterHeaders": "header-secret"},
    )

    assert "header-secret" in " ".join(commands[-1])
    assert "header-secret" not in result.command
    assert "header-secret" not in result.output
    assert "[REDACTED]" in result.command
    assert "[REDACTED]" in result.output


# region Invoker (direct boto3 invocation)


class _FakePayload:
    def __init__(self, body: bytes):
        self._body = body

    def read(self) -> bytes:
        return self._body


class _FakeCfnClient:
    """Fake CloudFormation client returning one Lambda resource page."""

    def __init__(self, resources: dict[str, str]):
        self._resources = resources
        self.list_calls = 0

    def get_paginator(self, name: str) -> object:
        assert name == "list_stack_resources"
        outer = self

        class _Paginator:
            def paginate(self, **_kwargs: object) -> list[dict]:
                outer.list_calls += 1
                return [
                    {
                        "StackResourceSummaries": [
                            {
                                "ResourceType": "AWS::Lambda::Function",
                                "LogicalResourceId": logical,
                                "PhysicalResourceId": physical,
                            }
                            for logical, physical in outer._resources.items()
                        ]
                    }
                ]

        return _Paginator()


class _FakeLambdaClient:
    def __init__(self, response: dict):
        self._response = response
        self.invocations: list[dict] = []

    def invoke(self, **kwargs: object) -> dict:
        self.invocations.append(kwargs)
        return self._response


def _make_invoker(response: dict, resources: dict[str, str] | None = None) -> tuple:
    cfn = _FakeCfnClient(resources or {"StepBasic": "physical-step-basic"})
    lam = _FakeLambdaClient(response)
    invoker = sam.Invoker(stack_name="my-stack", region="us-west-2", lambda_client=lam, cfn_client=cfn)
    return invoker, lam, cfn


def test_invoke_output_is_sam_compatible_json_with_arn() -> None:
    import json as _json

    response = {
        "StatusCode": 200,
        "DurableExecutionArn": "arn:aws:lambda:us-west-2:123:function:f:$LATEST/durable-execution/a/b",
        "ExecutedVersion": "$LATEST",
        "Payload": _FakePayload(b'"Hello, World!"'),
        "ResponseMetadata": {"HTTPHeaders": {}},
    }
    invoker, lam, _cfn = _make_invoker(response)

    result = invoker.invoke("StepBasic")

    assert result.success is True
    assert lam.invocations[0]["FunctionName"] == "physical-step-basic"
    assert lam.invocations[0]["Qualifier"] == "$LATEST"
    assert lam.invocations[0]["InvocationType"] == "RequestResponse"
    output = _json.loads(result.output)
    assert output["DurableExecutionArn"].endswith("/durable-execution/a/b")
    assert output["StatusCode"] == 200
    assert output["Payload"] == '"Hello, World!"'


def test_invoke_async_uses_event_type_and_returns_arn() -> None:
    import json as _json

    response = {
        "StatusCode": 202,
        "DurableExecutionArn": "arn:aws:lambda:us-west-2:123:function:f:$LATEST/durable-execution/c/d",
        "Payload": _FakePayload(b""),
        "ResponseMetadata": {"HTTPHeaders": {}},
    }
    invoker, lam, _cfn = _make_invoker(response)

    result = invoker.invoke_async("StepBasic")

    assert lam.invocations[0]["InvocationType"] == "Event"
    output = _json.loads(result.output)
    assert output["DurableExecutionArn"].endswith("/durable-execution/c/d")
    assert output["Payload"] == ""


def test_invoke_falls_back_to_arn_header_when_field_missing() -> None:
    import json as _json

    response = {
        "StatusCode": 200,
        "Payload": _FakePayload(b"{}"),
        "ResponseMetadata": {"HTTPHeaders": {"x-amz-durable-execution-arn": "arn:from-header"}},
    }
    invoker, _lam, _cfn = _make_invoker(response)

    output = _json.loads(invoker.invoke("StepBasic").output)

    assert output["DurableExecutionArn"] == "arn:from-header"


def test_invoke_resolves_stack_once_and_caches() -> None:
    response = {
        "StatusCode": 200,
        "Payload": _FakePayload(b"{}"),
        "ResponseMetadata": {"HTTPHeaders": {}},
    }
    invoker, _lam, cfn = _make_invoker(response)

    invoker.invoke("StepBasic")
    invoker.invoke("StepBasic")

    assert cfn.list_calls == 1


def test_invoke_unknown_logical_id_raises_invoke_error() -> None:
    import pytest as _pytest

    response = {"StatusCode": 200, "Payload": _FakePayload(b"{}"), "ResponseMetadata": {}}
    invoker, _lam, _cfn = _make_invoker(response)

    with _pytest.raises(sam.InvokeError, match="no Lambda function"):
        invoker.invoke("DoesNotExist")


def test_invoke_client_error_wrapped_as_invoke_error() -> None:
    import pytest as _pytest

    class _ErrLambdaClient:
        def invoke(self, **_kwargs: object) -> dict:
            raise ClientError(
                {"Error": {"Code": "TooManyRequestsException", "Message": "Rate exceeded"}},
                "Invoke",
            )

    cfn = _FakeCfnClient({"StepBasic": "physical-step-basic"})
    invoker = sam.Invoker(stack_name="my-stack", region="us-west-2", lambda_client=_ErrLambdaClient(), cfn_client=cfn)

    with _pytest.raises(sam.InvokeError, match="Rate exceeded"):
        invoker.invoke("StepBasic")


def test_invoke_function_error_surfaces_in_output() -> None:
    import json as _json

    response = {
        "StatusCode": 200,
        "FunctionError": "Unhandled",
        "Payload": _FakePayload(b'{"errorMessage": "boom", "errorType": "TypeError"}'),
        "ResponseMetadata": {"HTTPHeaders": {}},
    }
    invoker, _lam, _cfn = _make_invoker(response)

    result = invoker.invoke("StepBasic")

    # Parity with `sam remote invoke`: a handler-side error is not an invoke
    # failure -- the error payload and FunctionError marker pass through for
    # the validator/diagnostics to interpret.
    assert result.success is True
    output = _json.loads(result.output)
    assert output["FunctionError"] == "Unhandled"
    assert "boom" in output["Payload"]


def test_invoke_rejects_unsupported_parameters() -> None:
    import pytest as _pytest

    response = {"StatusCode": 200, "Payload": _FakePayload(b"{}"), "ResponseMetadata": {}}
    invoker, _lam, _cfn = _make_invoker(response)

    with _pytest.raises(sam.InvokeError, match="unsupported invoke parameter"):
        invoker.invoke("StepBasic", parameters=["ClientContext=abc"])


# endregion
