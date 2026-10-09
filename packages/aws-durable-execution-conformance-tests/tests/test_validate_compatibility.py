# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Regression coverage for callers predating configurable log polling."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from unittest.mock import Mock

import pytest
import yaml

import aws_durable_execution_conformance_tests.validate as validate_module
from aws_durable_execution_conformance_tests.clients import AwsClients
from aws_durable_execution_conformance_tests.config import DEFAULT_LOG_POLL_TIMEOUT_SECONDS
from aws_durable_execution_conformance_tests.sam import InvocationResult, Invoker
from aws_durable_execution_conformance_tests.validate import validate_description

if TYPE_CHECKING:
    from pathlib import Path


class _StubLambdaClient:
    def get_durable_execution_history(self, **_kwargs: Any) -> dict[str, Any]:
        return {"Events": [{"EventId": 1, "EventType": "ExecutionSucceeded"}]}


@pytest.mark.parametrize("async_invoke", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("positional_output_dir", [False, True], ids=["keyword", "positional"])
@pytest.mark.parametrize("timeout", [None, 45.5], ids=["default-timeout", "custom-timeout"])
def test_validate_description_preserves_existing_callers_and_forwards_log_timeout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    async_invoke: bool,
    positional_output_dir: bool,
    timeout: float | None,
) -> None:
    requirement = tmp_path / "test-1.yaml"
    requirement.write_text(
        yaml.safe_dump(
            {
                "AsyncInvoke": async_invoke,
                "AsyncConfig": {"PollIntervalSeconds": 0},
                "Input": {"value": "test"},
                "ExpectedExecutionHistory": [{"EventId": 1, "EventType": "ExecutionSucceeded"}],
                "ExpectedLogs": [{"match": {"message": "completed"}, "count": 1}],
            }
        ),
        encoding="utf-8",
    )
    invocation = InvocationResult(
        success=True,
        command="invoke",
        output=json.dumps({"DurableExecutionArn": "arn:test:execution"}),
        stderr="",
        function_name="TestFunction",
    )
    invoker = Invoker(stack_name="test-stack")
    invoke = Mock(return_value=invocation)
    invoke_async = Mock(return_value=invocation)
    monkeypatch.setattr(invoker, "invoke", invoke)
    monkeypatch.setattr(invoker, "invoke_async", invoke_async)
    validate_logs = Mock(return_value=[])
    monkeypatch.setattr(validate_module, "_validate_expected_logs", validate_logs)
    aws_clients = AwsClients({"lambda": _StubLambdaClient()})
    output_dir = tmp_path / "history"
    timeout_arguments: dict[str, Any] = {}
    if timeout is not None:
        timeout_arguments["log_poll_timeout_seconds"] = timeout

    if positional_output_dir:
        result = validate_description(
            "TestFunction",
            "test-1",
            str(requirement),
            invoker,
            str(tmp_path),
            "us-west-2",
            aws_clients,
            str(output_dir),
            **timeout_arguments,
        )
    else:
        result = validate_description(
            function_name="TestFunction",
            description_id="test-1",
            test_file=str(requirement),
            invoker=invoker,
            tmp_dir=str(tmp_path),
            region="us-west-2",
            aws_clients=aws_clients,
            output_dir=str(output_dir),
            **timeout_arguments,
        )

    assert result.passed, result.errors
    assert result.execution_arn == "arn:test:execution"
    assert json.loads((output_dir / "test-1.json").read_text(encoding="utf-8")) == result.execution_history
    assert json.loads((tmp_path / "test-1_event.json").read_text(encoding="utf-8")) == {"Input": {"value": "test"}}
    assert invoke.call_count == (0 if async_invoke else 1)
    assert invoke_async.call_count == (1 if async_invoke else 0)
    validate_logs.assert_called_once()
    assert validate_logs.call_args.kwargs["log_poll_timeout_seconds"] == (
        DEFAULT_LOG_POLL_TIMEOUT_SECONDS if timeout is None else timeout
    )
    assert validate_logs.call_args.kwargs["aws_clients"] is aws_clients
