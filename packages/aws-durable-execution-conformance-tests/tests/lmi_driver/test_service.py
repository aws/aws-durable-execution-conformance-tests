"""The shared transport has no dependency on a language SDK's testing library."""

import io
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
                    "Id": "callback",
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


def test_send_callback_success_omits_absent_result():
    api = Mock()
    runner = DurableFunctionRunner("function", region="us-west-2", lambda_client=api)

    runner.send_callback_success("target")

    api.send_durable_execution_callback_success.assert_called_once_with(CallbackId="target")


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


@pytest.mark.parametrize("code", ["ResourceNotFoundException", "TooManyRequestsException", "ServiceUnavailable"])
def test_result_poll_retries_transient_reads_until_terminal(monkeypatch, code):
    api = Mock()
    api.get_durable_execution.side_effect = [
        ClientError({"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": 503}}, "GetDurableExecution"),
        {"Status": "SUCCEEDED", "Result": "ok"},
    ]
    monkeypatch.setattr(service, "time", Clock())
    runner = DurableFunctionRunner("qualified-function", region="us-west-2", lambda_client=api)
    assert runner.wait_for_result("execution", timeout=1)["Result"] == "ok"
    assert api.get_durable_execution.call_count == 2


def test_cleanup_waits_for_pausing_then_stops_paused_execution(monkeypatch):
    api = Mock()
    api.get_durable_execution.side_effect = [{"Status": "PAUSING"}, {"Status": "PAUSED"}]
    monkeypatch.setattr(service, "time", Clock())
    assert service.request_execution_stops(api, ["execution"], timeout=3) == {}
    api.stop_durable_execution.assert_called_once_with(DurableExecutionArn="execution")


def test_callback_poll_retries_throttle_before_its_original_deadline(monkeypatch):
    api = Mock()
    api.get_durable_execution_history.side_effect = [
        ClientError({"Error": {"Code": "TooManyRequestsException"}}, "GetDurableExecutionHistory"),
        {
            "Events": [
                {
                    "EventType": "CallbackStarted",
                    "Id": "callback",
                    "Name": "completion",
                    "CallbackStartedDetails": {"CallbackId": "target"},
                }
            ]
        },
    ]
    monkeypatch.setattr(service, "time", Clock())
    assert (
        DurableFunctionRunner("function", region="us-west-2", lambda_client=api).wait_for_callback(
            "execution", "completion", timeout=1
        )
        == "target"
    )


def test_callback_waiter_skips_completed_id_on_a_later_page():
    api = Mock()
    api.get_durable_execution_history.side_effect = [
        {
            "Events": [
                {
                    "EventType": "CallbackStarted",
                    "Id": "old",
                    "Name": "completion",
                    "CallbackStartedDetails": {"CallbackId": "expired"},
                }
            ],
            "NextMarker": "next",
        },
        {
            "Events": [
                {"EventType": "CallbackTimedOut", "Id": "old"},
                {
                    "EventType": "CallbackStarted",
                    "Id": "new",
                    "Name": "completion",
                    "CallbackStartedDetails": {"CallbackId": "outstanding"},
                },
            ]
        },
    ]
    runner = DurableFunctionRunner("function", region="us-west-2", lambda_client=api)
    assert runner.wait_for_callback("execution", "completion") == "outstanding"


def test_release_reconciles_lost_result_without_unconditional_retry():
    storage = Mock()
    storage.get_object.side_effect = [
        {"Body": io.BytesIO(b"hold"), "ETag": '"held"'},
        {"Body": io.BytesIO(b"release"), "ETag": '"released"'},
    ]
    storage.put_object.side_effect = ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
    service.release_control(storage, "bucket", "key")
    storage.put_object.assert_called_once_with(Bucket="bucket", Key="key", Body=b"release", IfMatch='"held"')


def test_single_attempt_client_disables_botocore_retries(monkeypatch):
    factory = Mock()
    monkeypatch.setattr(service.boto3, "client", factory)
    service.client("s3", "us-west-2", single_attempt=True)
    assert factory.call_args.kwargs["config"].retries == {"total_max_attempts": 1}


def test_lambda_history_decoder_preserves_pause_request_timestamp():
    from botocore.parsers import create_parser
    from botocore.session import Session

    model = Session().get_service_model("lambda")
    operation = model.operation_model("GetDurableExecutionHistory")
    wire = {
        "Events": [
            {
                "EventType": "ExecutionPaused",
                "EventId": 1,
                "ExecutionPausedDetails": {"PauseRequestTimestamp": 123},
            }
        ]
    }
    response = {"status_code": 200, "headers": {}, "body": json.dumps(wire).encode()}
    parsed = create_parser(model.metadata["protocol"]).parse(response, operation.output_shape)
    assert parsed["Events"][0]["ExecutionPausedDetails"]["PauseRequestTimestamp"].timestamp() == 123


def test_stale_history_does_not_reopen_callback_seen_closed_before_failed_page(monkeypatch):
    api = Mock()
    old = {
        "EventType": "CallbackStarted",
        "Id": "old",
        "Name": "completion",
        "CallbackStartedDetails": {"CallbackId": "expired"},
    }
    new = {**old, "Id": "new", "CallbackStartedDetails": {"CallbackId": "available"}}
    api.get_durable_execution_history.side_effect = [
        {"Events": [old, {"EventType": "CallbackTimedOut", "Id": "old"}], "NextMarker": "later"},
        ClientError({"Error": {"Code": "ThrottlingException"}}, "GetDurableExecutionHistory"),
        {"Events": [old]},
        {"Events": [new]},
    ]
    monkeypatch.setattr(service, "time", Clock())
    runner = DurableFunctionRunner("function", region="us-west-2", lambda_client=api)
    assert runner.wait_for_callback("execution", "completion") == "available"
