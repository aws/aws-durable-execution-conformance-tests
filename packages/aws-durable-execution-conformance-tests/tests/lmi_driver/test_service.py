"""The shared transport has no dependency on a language SDK's testing library."""

import json
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from aws_durable_execution_conformance_tests.lmi import service
from aws_durable_execution_conformance_tests.lmi.service import DurableFunctionRunner

from .test_cloud import Clock


def test_invoke_preserves_the_fixture_payload_and_explicit_execution_name():
    api = Mock()
    api.invoke.return_value = {"StatusCode": 202, "DurableExecutionArn": "execution"}
    runner = DurableFunctionRunner("qualified-function", region="us-west-2", lambda_client=api)
    payload = {"caseId": "case", "scenario": "replay", "marker": "marker"}
    assert runner.run_async(input=payload, execution_name="journaled-launch") == "execution"
    api.invoke.assert_called_once_with(
        FunctionName="qualified-function",
        InvocationType="Event",
        Payload=json.dumps(payload),
        DurableExecutionName="journaled-launch",
    )


def test_unnamed_logical_calls_receive_distinct_execution_names():
    api = Mock()
    api.invoke.return_value = {"StatusCode": 202, "DurableExecutionArn": "execution"}
    runner = DurableFunctionRunner("qualified-function", region="us-west-2", lambda_client=api)
    runner.run_async(input={"same": "payload"})
    runner.run_async(input={"same": "payload"})
    names = [call.kwargs["DurableExecutionName"] for call in api.invoke.call_args_list]
    assert len(set(names)) == 2
    assert all(1 <= len(name) <= 64 and name.isalnum() for name in names)


def test_callback_search_retries_not_found_and_follows_pages(monkeypatch):
    clock, api = Clock(), Mock()
    monkeypatch.setattr(service, "time", clock)
    api.get_durable_execution_history.side_effect = [
        ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "History"),
        {
            "Events": [
                {"EventType": "CallbackStarted", "Name": "other", "CallbackStartedDetails": {"CallbackId": "wrong"}}
            ],
            "NextMarker": "page2",
        },
        {
            "Events": [
                {
                    "EventType": "CallbackStarted",
                    "Name": "completion",
                    "CallbackStartedDetails": {"CallbackId": "target"},
                }
            ]
        },
    ]
    runner = DurableFunctionRunner("function", region="us-west-2", lambda_client=api)
    assert runner.wait_for_callback("execution", "completion", timeout=5) == "target"
    assert api.get_durable_execution_history.call_args.kwargs["Marker"] == "page2"
    assert clock.now == 0.5
    runner.send_callback_success("target", b"marker")
    api.send_durable_execution_callback_success.assert_called_once_with(CallbackId="target", Result=b"marker")


@pytest.mark.parametrize("operation", ["result", "callback"])
def test_service_polling_has_a_finite_deadline(monkeypatch, operation):
    clock, api = Clock(), Mock()
    monkeypatch.setattr(service, "time", clock)
    api.get_durable_execution.return_value = {"Status": "RUNNING"}
    api.get_durable_execution_history.return_value = {"Events": []}
    runner = DurableFunctionRunner("function", region="us-west-2", lambda_client=api)
    with pytest.raises(TimeoutError):
        if operation == "result":
            runner.wait_for_result("execution", timeout=1)
        else:
            runner.wait_for_callback("execution", "completion", timeout=1)
    assert clock.now == 1


def test_permissions_fail_without_polling_again():
    api = Mock()
    api.get_durable_execution_history.side_effect = ClientError({"Error": {"Code": "AccessDeniedException"}}, "History")
    runner = DurableFunctionRunner("function", region="us-west-2", lambda_client=api)
    with pytest.raises(ClientError):
        runner.wait_for_callback("execution", "completion")
    api.get_durable_execution_history.assert_called_once()


@pytest.mark.parametrize("parent_first", [True, False])
def test_unnamed_callback_inherits_its_context_name_across_pages(parent_first):
    api = Mock()
    parent = {"Id": "context", "EventType": "ContextStarted", "SubType": "WaitForCallback", "Name": "completion"}
    child = {
        "Id": "callback",
        "ParentId": "context",
        "EventType": "CallbackStarted",
        "CallbackStartedDetails": {"CallbackId": "target"},
    }
    first, second = (parent, child) if parent_first else (child, parent)
    api.get_durable_execution_history.side_effect = [{"Events": [first], "NextMarker": "page2"}, {"Events": [second]}]
    runner = DurableFunctionRunner("function", region="us-west-2", lambda_client=api)
    assert runner.wait_for_callback("execution", "completion", timeout=5) == "target"
    assert api.get_durable_execution_history.call_count == 2
