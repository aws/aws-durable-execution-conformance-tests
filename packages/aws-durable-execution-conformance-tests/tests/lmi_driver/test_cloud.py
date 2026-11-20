import hashlib
import io
import json
from datetime import UTC, datetime
from typing import Any
from unittest.mock import Mock

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import ANY, Stubber

from aws_durable_execution_conformance_tests.lmi import cloud as module
from aws_durable_execution_conformance_tests.lmi.errors import CollectionError, PlacementError
from aws_durable_execution_conformance_tests.lmi.summary import summarize


@pytest.fixture
def driver(monkeypatch, tmp_path):
    lam = boto3.client(
        "lambda",
        region_name="us-west-2",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    driver = module.Cloud(
        {
            "functions": {"c1": "arn:aws:lambda:us-west-2:123456789012:function:lmi:$LATEST.PUBLISHED"},
            "region": "us-west-2",
            "run": "unit",
            "bucket": "unit-bucket",
            "commit": "commit",
            "invocationTimeout": 60,
            "concurrencies": {"c1": 1},
        },
        "c1",
        artifacts_dir=tmp_path,
        verify_profile=Mock(),
        client_factory=lambda *_: lam,
    )
    driver.s3 = Mock()
    driver.s3.put_object.return_value = {"ResponseMetadata": {"RetryAttempts": 0}}
    return driver


@pytest.mark.parametrize("configuration", ["c1", "c2"])
@pytest.mark.parametrize("scenario", ["success", "deadline"])
def test_scenarios_reuse_the_qualified_function_for_their_concurrency(driver, configuration, scenario):
    driver.configuration = configuration
    driver.manifest["concurrencies"] = module.CONCURRENCIES
    driver.manifest["functions"]["c2"] = "arn:aws:lambda:us-west-2:123456789012:function:lmi-c2:$LATEST.PUBLISHED"
    assert driver.concurrency == module.CONCURRENCIES[configuration]
    function = driver.manifest["functions"][configuration]
    identity = [driver.manifest["run"], configuration, None, "fixed"]
    name = hashlib.sha256(json.dumps(identity).encode()).hexdigest()
    arn = f"{function}/durable-execution/{name}/id"
    with Stubber(driver.lam) as stub:
        stub.add_response(
            "invoke",
            {"StatusCode": 202, "DurableExecutionArn": arn},
            {
                "FunctionName": driver.manifest["functions"][configuration],
                "InvocationType": "Event",
                "Payload": ANY,
                "DurableExecutionName": name,
            },
        )
        item = driver.start(scenario, gate="fault" if scenario == "deadline" else None, marker="fixed")
        assert item["arn"] == arn
        stub.assert_no_pending_responses()


def test_verify_rejects_a_deployment_missing_a_concurrency_configuration(driver):
    driver.manifest["concurrencies"] = module.CONCURRENCIES
    with pytest.raises(module.ProvisioningError, match="both c1 and c2"):
        driver.verify()


def test_settlement_releases_controls_stops_retries_and_waits_for_every_wrapper(driver):
    driver.invocations = [{"arn": "fault"}, {"arn": "completed"}]
    calls = []
    driver.release_all = Mock(side_effect=lambda: calls.append("release"))
    statuses = {"fault": "RUNNING", "completed": "SUCCEEDED"}
    driver.lam.get_durable_execution = Mock(side_effect=lambda **kw: {"Status": statuses[kw["DurableExecutionArn"]]})

    def stop(**kw):
        calls.append("stop")
        statuses[kw["DurableExecutionArn"]] = "STOPPED"

    driver.lam.stop_durable_execution = Mock(side_effect=stop)
    events = [
        {"phase": "WRAPPER_ENTER", "request": "first"},
        {"phase": "WRAPPER_ENTER", "request": "retry"},
        {"phase": "WRAPPER_RETURN", "request": "first"},
    ]
    driver.refresh = Mock(return_value=events)

    def poll(predicate, **_):
        # A terminal durable execution does not prove a stopped LMI worker.
        assert not predicate()
        events.append({"phase": "WRAPPER_RAISE", "request": "retry"})
        assert predicate()
        calls.append("settled")

    driver.poll = poll
    driver.settle_case()
    assert calls == ["release", "stop", "settled"]
    driver.lam.stop_durable_execution.assert_called_once_with(DurableExecutionArn="fault")


def test_settlement_drains_identified_work_even_with_an_unresolved_launch(driver):
    driver.invocations = [{"arn": "owned"}, {"arn": None}]
    driver.lam.get_durable_execution = Mock(side_effect=[{"Status": "RUNNING"}, {"Status": "STOPPED"}])
    driver.lam.stop_durable_execution = Mock()
    driver.refresh = Mock(return_value=[])
    driver.settle_case()
    driver.lam.stop_durable_execution.assert_called_once_with(DurableExecutionArn="owned")
    assert {call.kwargs["DurableExecutionArn"] for call in driver.lam.get_durable_execution.call_args_list} == {"owned"}


def test_settlement_reports_a_worker_still_running_after_release(driver):
    driver.invocations = [{"arn": "execution"}]
    driver.lam.get_durable_execution = Mock(return_value={"Status": "STOPPED"})
    driver.refresh = Mock(return_value=[{"phase": "WRAPPER_ENTER", "request": "stuck"}])

    def poll(predicate, **kwargs):
        assert not predicate()
        raise CollectionError(kwargs["message"])

    driver.poll = poll
    with pytest.raises(CollectionError, match="not ready for reuse"):
        driver.settle_case()


@pytest.mark.parametrize("status", ["SUCCEEDED", "RUNNING"])
def test_stop_race_is_ignored_only_after_confirmed_completion(driver, status):
    driver.invocations = [{"arn": "execution"}]
    driver.lam.get_durable_execution = Mock(side_effect=[{"Status": "RUNNING"}, {"Status": status}])
    driver.lam.stop_durable_execution = Mock(
        side_effect=ClientError(
            {"Error": {"Code": "InvalidParameterValueException"}},
            "StopDurableExecution",
        )
    )
    driver.refresh = Mock(return_value=[])
    if status == "RUNNING":
        with pytest.raises(ClientError):
            driver.settle_case()
    else:
        driver.settle_case()


def test_missing_execution_arn_is_collection_error_not_pass(driver):
    with Stubber(driver.lam) as stub:
        stub.add_response("invoke", {"StatusCode": 202})
        with pytest.raises(CollectionError, match="API failed"):
            driver.start("success")


@pytest.mark.parametrize("message", ["Runtime.Timeout", "Task timed out after 10 seconds"])
def test_requires_request_specific_service_timeout(driver, message):
    good = {
        "InvocationCompletedDetails": {
            "RequestId": "request",
            "Error": {"Payload": {"ErrorType": "Sandbox.Timedout", "ErrorMessage": message}},
        }
    }
    driver.history = Mock(return_value=[good])
    driver.poll = lambda predicate, **_: predicate() or (_ for _ in ()).throw(CollectionError())
    assert driver.platform_timeout({}, "request") == good
    for events in (
        [{"EventType": "ExecutionTimedOut"}],
        [
            {
                "InvocationCompletedDetails": {
                    "RequestId": "different",
                    "Error": {"Payload": {"ErrorType": "Sandbox.Timedout", "ErrorMessage": message}},
                }
            }
        ],
    ):
        driver.history.return_value = events
        with pytest.raises(CollectionError):
            driver.platform_timeout({}, "request")


def test_native_completion_polls_for_original_native_request(driver):
    def completed(request, end=0.060, event_type="InvocationCompleted"):
        details = {"RequestId": request, "StartTimestamp": 0.040, "EndTimestamp": end}
        return {"EventType": event_type, "InvocationCompletedDetails": details}

    original, retry = completed("original"), completed("retry", 0.070)
    logical_success = {"EventType": "ExecutionSucceeded"}
    item = {"arn": "arn", "native_requests": ["original", "retry"]}
    driver.raw_histories["arn"] = [logical_success]
    driver.history = Mock(side_effect=[[logical_success, retry], [logical_success, retry, original]])
    driver.wait = Mock()
    # Only the original request's own completion satisfies the wait, not a retry's.
    assert driver.native_completion(item, seconds=5) == (40, 60)
    assert driver.history.call_count == 2

    driver.raw_histories["arn"].append(original)
    driver.history.reset_mock()
    assert driver.native_completion(item, seconds=5) == (40, 60)
    driver.history.assert_not_called()


@pytest.mark.parametrize(
    "history",
    [
        pytest.param([], id="absent"),
        pytest.param(["duplicate"], id="duplicate"),
        pytest.param(["invalid-end"], id="invalid-end"),
    ],
)
def test_native_completion_budget_and_defective_records(driver, history):
    rows = {
        "duplicate": [
            {
                "EventType": "InvocationCompleted",
                "InvocationCompletedDetails": {"RequestId": "original", "StartTimestamp": 0.04, "EndTimestamp": end},
            }
            for end in (0.06, 0.07)
        ],
        "invalid-end": [
            {
                "EventType": "InvocationCompleted",
                "InvocationCompletedDetails": {"RequestId": "original", "StartTimestamp": 0.04, "EndTimestamp": None},
            }
        ],
    }
    driver.raw_histories["arn"] = [row for key in history for row in rows[key]]
    driver.history = Mock(return_value=driver.raw_histories["arn"])
    driver.poll = Mock(side_effect=PlacementError("expired"))
    item = {"arn": "arn", "native_requests": ["original"]}
    if history:
        # Retained history only grows, so a defective record is a collection
        # error now, not a peer expiry after polling.
        with pytest.raises(CollectionError, match="duplicate or malformed completion"):
            driver.native_completion(item, seconds=12.5, category=PlacementError, message="m")
        driver.poll.assert_not_called()
        return
    with pytest.raises(PlacementError):
        driver.native_completion(item, seconds=12.5, category=PlacementError, message="m")
    _, options = driver.poll.call_args
    assert options == {"seconds": 12.5, "category": PlacementError, "message": "m"}


def test_summary_never_claims_unexecuted_or_skipped_cloud_success(tmp_path):
    assert "did not run" in summarize(tmp_path)
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "run": "current",
                "commit": "sdk",
                "runtime": "python3.14",
                "concurrencies": {"c1": 1, "c2": 2},
            }
        )
    )
    shared = tmp_path / "shared/standard"
    shared.mkdir(parents=True)
    (shared / "report.json").write_text(
        json.dumps(
            {
                "run": {"name": "current"},
                "results": [
                    {"id": "lmi-lifecycle-1", "status": "FAILED"},
                    {"id": "lmi-lifecycle-2", "status": "UNCOVERED"},
                ],
                "ci": {"exit_code": 1},
            }
        )
    )
    summary = summarize(tmp_path)
    assert "lmi-lifecycle-1: FAILED" in summary
    assert "lmi-lifecycle-2: UNCOVERED" in summary
    assert "PASS" not in summary
    (tmp_path / "deploy-error.json").write_text(json.dumps({"type": "ProvisioningError"}))
    assert "ProvisioningError" in summarize(tmp_path)


def test_runs_share_functions_but_never_controls_or_event_prefixes(driver):
    other = module.Cloud(
        {**driver.manifest, "run": "other"},
        "c1",
        artifacts_dir=driver.artifacts_dir,
        verify_profile=Mock(),
        client_factory=lambda *_: driver.lam,
    )
    other.s3 = driver.s3
    objects = {}
    driver.s3.put_object.side_effect = lambda **kw: objects.update({kw["Key"]: kw["Body"]})
    driver.s3.get_object.side_effect = lambda **kw: {"Body": io.BytesIO(objects[kw["Key"]]), "ETag": '"hold"'}
    driver.hold("same-gate")
    other.hold("same-gate")
    driver.release("same-gate")
    assert objects == {
        "runs/unit/control/same-gate": b"release",
        "runs/other/control/same-gate": b"hold",
    }
    driver.s3.get_paginator.return_value.paginate.return_value = []
    driver.refresh(markers=[])
    other.refresh(markers=[])
    assert [c.kwargs["Prefix"] for c in driver.s3.get_paginator.return_value.paginate.call_args_list] == [
        "runs/unit/events/",
        "runs/other/events/",
    ]


def test_cleanup_discovery_ignores_other_runs_and_rejects_foreign_functions(driver, tmp_path):
    target = driver.manifest["functions"]["c1"]
    arn = target + "/durable-execution/case/id"
    payload = {"run": "unit", "marker": "case"}
    event = {**payload, "execution": arn, "phase": "WRAPPER_ENTER"}
    driver.refresh = Mock(return_value=[event])
    records = tmp_path / "invocations"
    records.mkdir()
    (records / "old.json").write_text(json.dumps({"payload": {"run": "previous", "marker": "old"}, "arn": "foreign"}))
    (records / "case.json").write_text(
        json.dumps({"payload": payload, "function": target, "durableExecutionName": "case"})
    )
    assert driver.run_invocations() == ([{"arn": arn, "marker": "case"}], [])
    driver.refresh.assert_called_once_with(markers=["case"], validate_controls=False)
    known_arn = target + "/durable-execution/known/id"
    (records / "known.json").write_text(
        json.dumps(
            {
                "payload": {**payload, "marker": "known"},
                "function": target,
                "durableExecutionName": "known",
                "arn": known_arn,
            }
        )
    )
    driver.refresh.return_value = [{**event, "execution": target + "/durable-execution/other/id"}]
    driver.lam.list_durable_executions_by_function = Mock(
        return_value={
            "DurableExecutions": [{"DurableExecutionName": "case", "DurableExecutionArn": arn}],
        }
    )
    driver.lam.get_durable_execution = Mock(return_value={"InputPayload": json.dumps(payload)})
    items, errors = driver.run_invocations()
    assert {item["arn"] for item in items} == {known_arn, arn}
    assert len(errors) == 1 and "journaled launch" in errors[0]
    driver.refresh.side_effect = ClientError({"Error": {"Code": "SlowDown"}}, "GetObject")
    items, errors = driver.run_invocations()
    assert {item["arn"] for item in items} == {known_arn, arn}
    assert len(errors) == 1 and "SlowDown" in errors[0]

    driver.lam.get_durable_execution.return_value = {"InputPayload": json.dumps({**payload, "marker": "other"})}
    items, errors = driver.run_invocations()
    assert {item["arn"] for item in items} == {known_arn}
    assert any("does not match the journaled launch" in error for error in errors)


def test_artifact_write_rejects_paths_outside_the_run_directory(driver, tmp_path):
    (driver.artifacts_dir / "linked").symlink_to(tmp_path.parent, target_is_directory=True)
    with pytest.raises(CollectionError, match="outside the run directory"):
        driver._save("linked/other.json", {"value": 1})


def test_polling_reads_only_current_case_and_surfaces_control_errors(driver):
    driver.invocations = [{"marker": "current"}]
    event = {
        "run": "unit",
        "commit": "commit",
        "marker": "current",
        "request": "r",
        "sequence": 1,
        "time": 1,
        "phase": "CONTROL_ERROR",
        "gate": "g",
        "error": "403 Forbidden",
    }
    key = "runs/unit/events/current/r/000001.json"
    driver.s3.get_paginator.return_value.paginate.return_value = [{"Contents": [{"Key": key}]}]
    driver.s3.get_object.side_effect = lambda **_: {"Body": io.BytesIO(json.dumps(event).encode())}
    with pytest.raises(CollectionError, match="403"):
        driver.refresh()
    driver.s3.get_paginator.return_value.paginate.assert_called_once_with(
        Bucket="unit-bucket", Prefix="runs/unit/events/current/"
    )
    driver.s3.get_object.assert_called_once_with(Bucket="unit-bucket", Key=key)
    # Full diagnostic collection preserves errors without turning itself into a
    # second failing test or omitting evidence from earlier cases.
    assert driver.refresh(markers=[]) == [event]
    driver.s3.get_paginator.return_value.paginate.assert_called_with(Bucket="unit-bucket", Prefix="runs/unit/events/")


class Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def throttle_error():
    return ClientError(
        {"Error": {"Code": "TooManyRequestsException", "Message": "Rate exceeded"}},
        "GetDurableExecutionHistory",
    )


def test_history_retries_preserve_pages_and_pace_requests(driver, monkeypatch):
    clock = Clock()
    monkeypatch.setattr(module, "time", clock)
    driver.lam.get_durable_execution_history = Mock(
        side_effect=[
            throttle_error(),
            {"Events": [{"EventId": 1}], "NextMarker": "page2"},
            {"Events": [{"EventId": 2}]},
        ]
    )
    assert driver.history({"arn": "execution", "marker": "case"}) == [
        {"EventId": 1},
        {"EventId": 2},
    ]
    calls = driver.lam.get_durable_execution_history.call_args_list
    assert calls[0] == calls[1]
    assert calls[2].kwargs["Marker"] == "page2"
    assert clock.now >= 2


def test_history_throttling_has_a_finite_budget(driver, monkeypatch):
    clock = Clock()
    monkeypatch.setattr(module, "time", clock)
    driver.lam.get_durable_execution_history = Mock(side_effect=throttle_error())
    with pytest.raises(CollectionError, match="throttle budget"):
        driver.history_page(DurableExecutionArn="execution")
    assert driver.lam.get_durable_execution_history.call_count == 5
    assert clock.now < 20


def test_history_does_not_retry_permission_errors_or_invocations(driver, monkeypatch):
    monkeypatch.setattr(module, "time", Clock())
    driver.lam.invoke = Mock(
        side_effect=lambda **kwargs: {
            "StatusCode": 202,
            "DurableExecutionArn": f"{kwargs['FunctionName']}/durable-execution/{kwargs['DurableExecutionName']}/id",
        }
    )
    item = driver.start("success")
    error = ClientError({"Error": {"Code": "AccessDeniedException"}}, "GetDurableExecutionHistory")
    driver.lam.get_durable_execution_history = Mock(side_effect=error)
    with pytest.raises(ClientError):
        item["runner"].lambda_client.get_durable_execution_history(DurableExecutionArn="execution")
    driver.lam.invoke.assert_called_once()
    driver.lam.get_durable_execution_history.assert_called_once()


def test_case_collection_is_scoped_but_final_collection_is_complete(driver):
    arn = driver.manifest["functions"]["c1"] + "/durable-execution/name/id"
    driver.invocations = [{"arn": arn, "marker": "case"}]
    driver.refresh = Mock(
        return_value=[
            {"execution": arn, "marker": "untrusted-marker", "request": "own", "time": 1},
            {"execution": arn, "marker": "untrusted-marker", "request": "own", "time": 3},
        ]
    )
    driver.history = Mock(return_value=[])
    driver.raw_histories[arn] = [
        {
            "InvocationCompletedDetails": {
                "RequestId": "own",
                "StartTimestamp": datetime.fromtimestamp(1, UTC),
                "EndTimestamp": datetime.fromtimestamp(3, UTC),
            }
        }
    ]
    driver.lam.get_durable_execution = Mock(
        return_value={"Status": "SUCCEEDED", "InputPayload": '{"run":"unit","marker":"untrusted-marker"}'}
    )
    driver.logs = Mock()
    driver.collect(full_run=False)
    driver.refresh.assert_called_once_with(markers=["case"], validate_controls=False)
    driver.history.assert_called_once()
    driver.logs.get_paginator.assert_not_called()
    driver.manifest["created"] = 0
    driver.lam.get_function_configuration = Mock(return_value={"LoggingConfig": {"LogGroup": "group"}})
    own = {"timestamp": 2000, "message": '{"record":{"requestId":"own"}}'}
    report = {"timestamp": 3001, "message": '{"record":{"requestId":"own"}}'}
    driver.logs.get_paginator.return_value.paginate.return_value = [
        {
            "events": [
                own,
                {"timestamp": 2000, "message": '{"requestId":"foreign"}'},
                report,
                {"timestamp": 5001, "message": '{"requestId":"own"}'},
                {"timestamp": 2100, "message": "unattributable output"},
            ]
        }
    ]
    driver.collect()
    assert json.loads((driver.artifacts_dir / "logs/c1.json").read_text()) == [own, report]
    driver.logs.get_paginator.return_value.paginate.assert_called_once_with(
        logGroupName="group", startTime=0, endTime=5001
    )
    driver.refresh.assert_called_with(markers=[], validate_controls=False)
    full_run_item = driver.history.call_args.args[0]
    assert full_run_item["arn"] == arn
    assert full_run_item["marker"] != "untrusted-marker"
    driver.logs.get_paginator.assert_called_once_with("filter_log_events")


def test_collection_reads_history_after_sampling_status(driver):
    item = {"arn": "execution", "marker": "case"}
    driver.invocations = [item]
    driver.refresh = Mock(return_value=[])
    reads = Mock()
    driver.lam.get_durable_execution = reads.status
    driver.history = reads.history
    reads.status.side_effect = [{"Status": "SUCCEEDED"}, {"Status": "RUNNING"}]

    driver.collect(full_run=False)
    assert [entry[0] for entry in reads.mock_calls] == ["status", "history"]
    assert driver.terminal_histories == {"execution"}
    driver.collect(full_run=False)
    assert driver.terminal_histories == set()


def test_summary_rejects_other_run_reports_and_requires_matching_cleanup(
    tmp_path,
):
    config = {
        "commit": "sdk",
        "runtime": "python3.14",
        "concurrencies": {"c1": 1, "c2": 2},
        "run": "current",
    }
    (tmp_path / "manifest.json").write_text(json.dumps(config))
    shared = tmp_path / "shared"
    (shared / "standard").mkdir(parents=True)
    (shared / "standard/report.json").write_text(
        json.dumps(
            {
                "run": {"name": "previous"},
                "results": [{"id": "lmi-lifecycle-1", "status": "PASSED"}],
            }
        )
    )
    receipt = shared / "cleanup.json"
    receipt.write_text(json.dumps({"runId": "previous", "status": "CLEANUP_REQUESTED", "errors": []}))
    summary = summarize(tmp_path)
    assert "another run" in summary and "PASSED" not in summary
    assert "Run cleanup not confirmed" in summary
    receipt.write_text(json.dumps({"runId": "current", "status": "CLEANUP_REQUESTED", "errors": []}))
    assert "Run-scoped cleanup requests completed" in summarize(tmp_path)


def test_summary_does_not_accept_a_report_without_deployment_identity(tmp_path):
    shared = tmp_path / "shared/standard"
    shared.mkdir(parents=True)
    (shared / "report.json").write_text(json.dumps({"results": [{"id": "lmi-lifecycle-1", "status": "PASSED"}]}))
    summary = summarize(tmp_path)
    assert "Deployment identity is missing" in summary
    assert "PASSED" not in summary


@pytest.mark.parametrize(
    ("error", "found"),
    [
        ({"Payload": {"ErrorType": "Sandbox.Timedout", "ErrorMessage": "Task timed out after 60.00 seconds"}}, True),
        ({"Payload": {"ErrorType": "DatabaseError", "ErrorMessage": "database timeout"}}, False),
    ],
)
def test_platform_timeout_wait_uses_the_structured_error_type(driver, error, found):
    event = {"InvocationCompletedDetails": {"RequestId": "request", "Error": error}}
    driver.history = lambda item: [event]
    driver.poll = lambda predicate, **options: predicate()
    assert (driver.platform_timeout({"arn": "execution"}, "request") is event) is found


@pytest.mark.parametrize(
    ("result", "phase"),
    [
        ({"Status": "SUCCEEDED", "Result": '"another-marker"'}, "WRAPPER_RETURN"),
        ({"Status": "FAILED", "Error": {"ErrorType": "Unexpected"}}, "WRAPPER_RAISE"),
    ],
)
def test_shared_finish_retains_unexpected_outcomes_for_validation(driver, result, phase):
    driver.case_context = Mock()
    driver.manifest["driverTimeout"] = 10
    driver.lam.get_durable_execution = Mock()
    driver.for_item = Mock(return_value=[{"phase": phase}])
    driver.history = Mock(return_value=[{"EventType": "ExecutionStarted"}])
    runner = Mock()
    runner.wait_for_result.return_value = result
    item = {"arn": "execution", "marker": "victim", "runner": runner}

    assert driver.finish(item) == [{"EventType": "ExecutionStarted"}]

    assert driver.execution_results["execution"] == result
    driver.lam.get_durable_execution.assert_not_called()


def test_hold_keeps_pending_intent_until_the_object_is_reconciled(driver, tmp_path):
    from botocore.exceptions import ReadTimeoutError

    driver.s3.put_object.side_effect = ReadTimeoutError(endpoint_url="test")
    driver.s3.get_object.side_effect = ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
    with pytest.raises(ClientError):
        driver.hold("peer")
    pending = json.loads((tmp_path / "control-records.json").read_text())
    assert pending[0]["state"] == "PENDING"
    assert "peer" in driver.gates
    driver.s3.put_object.side_effect = None
    assert driver.s3.get_object.call_count == 1
    driver.release_all()
    assert driver.s3.get_object.call_count == 2
    driver.s3.put_object.assert_called_with(
        Bucket="unit-bucket", Key="runs/unit/control/peer", Body=b"release", IfNoneMatch="*"
    )
    records = json.loads((tmp_path / "control-records.json").read_text())
    assert [(item["action"], item["state"]) for item in records] == [
        ("hold", "PENDING"),
        ("release", "ACKNOWLEDGED"),
    ]
    with pytest.raises(CollectionError, match="unresolved hold writes"):
        driver.delete_controls()
    driver.s3.delete_object.assert_not_called()


def test_cancelled_prepared_invocation_is_not_treated_as_dispatched(driver, monkeypatch):
    original_save = driver._save

    def save(relative, data, **kwargs):
        original_save(relative, data, **kwargs)
        if relative.startswith("invocations/"):
            driver.cancel()

    monkeypatch.setattr(driver, "_save", save)
    driver.lam.invoke = Mock()
    with pytest.raises(CollectionError, match="invocation API failed"):
        driver.start("success")
    driver.lam.invoke.assert_not_called()
    assert driver.invocations == []
    (journal,) = (driver.artifacts_dir / "invocations").glob("*.json")
    assert json.loads(journal.read_text())["dispatchState"] == "PREPARED"
    assert driver.run_invocations() == ([], [])


@pytest.mark.parametrize("malformed", [False, True])
def test_conflicting_recovered_arns_do_not_block_an_independently_owned_launch(driver, malformed):
    driver.case_context = Mock()
    target = driver.manifest["functions"][driver.configuration]
    launch = {
        "marker": "victim",
        "payload": {"run": "unit", "marker": "victim"},
        "execution_name": "victim",
        "arn": target + "/durable-execution/victim/first",
        "role": "victim",
        "launch_id": "victim",
        "native_requests": [],
        "resume_roles": (),
    }
    peer = {
        **launch,
        "marker": "peer",
        "payload": {"run": "unit", "marker": "peer"},
        "execution_name": "peer",
        "arn": None,
        "role": "peer",
        "launch_id": "peer",
        "native_requests": [],
    }
    driver.invocations = [launch, peer]
    event = {"phase": "WRAPPER_ENTER", "run": "unit", "time": 1, "sequence": 1}
    verified = target + "/durable-execution/peer/id"
    events = [
        {**event, "marker": "victim", "request": "one", "execution": launch["arn"]},
        {**event, "marker": "victim", "request": "two", "execution": target + "/durable-execution/victim/second"},
        {**event, "marker": "peer", "request": "three", "execution": verified},
    ]
    if malformed:
        events.insert(0, {**event, "marker": "victim", "request": "malformed"})
    match = "malformed native request or execution ARN" if malformed else "Multiple execution ARNs"
    with pytest.raises(CollectionError, match=match):
        driver._bind_requests(events)
    assert launch["arn"] is None
    assert peer["arn"] == verified
    driver.lam.get_durable_execution = Mock(side_effect=[{"Status": "RUNNING"}, {"Status": "STOPPED"}])
    driver.lam.stop_durable_execution = Mock()
    driver.manifest["driverTimeout"] = 0
    driver.refresh = Mock(side_effect=CollectionError(match))
    with pytest.raises(CollectionError, match=match):
        driver.settle_case()
    driver.lam.stop_durable_execution.assert_called_once_with(DurableExecutionArn=verified)


@pytest.mark.parametrize("custom_source", [False, True])
def test_full_collection_captures_acknowledged_execution_without_ledger(driver, tmp_path, custom_source):
    target = driver.manifest["functions"]["c1"]
    arn = target + "/durable-execution/fixture/id"
    coverage = tmp_path / "shared" / "standard"
    source = tmp_path / "custom-source" if custom_source else coverage / "source"
    if custom_source:
        coverage.mkdir(parents=True)
        (coverage / "manifest.json").write_text(json.dumps({"runId": "unit", "sdk": {"artifacts_dir": str(source)}}))
    journal = source / "cases" / "case" / "invocations" / "journal.json"
    journal.parent.mkdir(parents=True)
    journal.write_text(
        json.dumps(
            {
                "payload": {"run": "unit", "marker": "fixture"},
                "arn": arn,
                "function": target,
                "durableExecutionName": "fixture",
            }
        )
    )
    driver.refresh = Mock(return_value=[])
    driver.history = Mock(return_value=[])
    driver.lam.get_durable_execution = Mock(return_value={"Status": "FAILED"})
    driver.collect()
    driver.lam.get_durable_execution.assert_called_once_with(DurableExecutionArn=arn)
    assert driver.history.call_args.args[0]["arn"] == arn


@pytest.mark.parametrize("retries", [0, 1, None])
@pytest.mark.parametrize(
    "code",
    [
        "AccessDeniedException",
        "TooManyRequestsException",
        "DurableExecutionAlreadyStartedException",
        "ExpiredTokenException",
    ],
)
def test_invoke_rejection_only_clears_intent_without_retries(driver, retries, code, tmp_path):
    driver.lam.invoke = Mock(
        side_effect=ClientError(
            {
                "Error": {"Code": code},
                "ResponseMetadata": {} if retries is None else {"RetryAttempts": retries},
            },
            "Invoke",
        )
    )
    with pytest.raises(CollectionError, match="invocation API failed"):
        driver.start("fixture")
    (journal,) = (tmp_path / "invocations").glob("*.json")
    state = json.loads(journal.read_text())["dispatchState"]
    if code == "DurableExecutionAlreadyStartedException" and retries != 0:
        assert state == "CONFLICT"
        driver.lam.list_durable_executions_by_function = Mock()
        assert driver.run_invocations()[0] == []
        driver.lam.list_durable_executions_by_function.assert_not_called()
    else:
        assert state == ("REJECTED" if retries == 0 else "SUBMITTED")
    assert bool(driver.invocations) is (retries != 0)


def test_settlement_retries_visibility_independently_of_peer(driver):
    driver.invocations = [{"arn": "first"}, {"arn": "peer"}]
    driver.lam.get_durable_execution = Mock(
        side_effect=[
            ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "GetDurableExecution"),
            {"Status": "RUNNING"},
            {"Status": "RUNNING"},
            {"Status": "STOPPED"},
            {"Status": "STOPPED"},
        ]
    )
    driver.lam.stop_durable_execution = Mock()
    driver.refresh = Mock(return_value=[])
    driver.wait = Mock()
    driver.settle_case()
    assert [call.kwargs["DurableExecutionArn"] for call in driver.lam.stop_durable_execution.call_args_list] == [
        "peer",
        "first",
    ]


@pytest.mark.parametrize("stop_outcome", ["acknowledged", "raced", "denied"])
def test_observation_stop_preserves_the_running_witness(driver, stop_outcome):
    arn = "execution"
    item = {"arn": arn, "role": "victim"}
    driver.invocations = [item]
    driver.lam.get_durable_execution = Mock(side_effect=[{"Status": "RUNNING"}, {"Status": "SUCCEEDED"}])
    driver.lam.stop_durable_execution = Mock()
    if stop_outcome != "acknowledged":
        code = "InvalidParameterValueException" if stop_outcome == "raced" else "AccessDeniedException"
        driver.lam.stop_durable_execution.side_effect = ClientError({"Error": {"Code": code}}, "StopDurableExecution")
    driver.stop_victim_after_cutoff(item)
    assert driver.cutoff_results[arn]["Status"] == "RUNNING"
    assert ("collectionError" in driver.cutoff_results[arn]) is (stop_outcome == "denied")
    driver.lam.stop_durable_execution.assert_called_once_with(DurableExecutionArn=arn)
    (driver.artifacts_dir / "cutoff-executions.json").read_text()


def test_partial_ledger_read_revokes_only_the_affected_provisional_launch(driver):
    target = driver.manifest["functions"][driver.configuration]
    driver.case_context = Mock()
    owner = {"run": "unit", "caseId": "case", "commit": "commit"}
    items: list[dict[str, Any]] = [
        {
            "marker": marker,
            "payload": {**owner, "marker": marker},
            "execution_name": marker,
            "arn": f"{target}/durable-execution/{marker}/original",
            "role": marker,
            "launch_id": marker,
            "native_requests": [],
            "resume_roles": (),
        }
        for marker in ("victim", "peer")
    ]
    driver.invocations = items
    prefix = "runs/unit/events/"
    pages = {
        f"{prefix}victim/": [{"Contents": [{"Key": f"{prefix}victim/first"}, {"Key": f"{prefix}victim/late"}]}],
        f"{prefix}peer/": [{"Contents": [{"Key": f"{prefix}peer/first"}]}],
    }
    driver.s3.get_paginator.return_value.paginate.side_effect = lambda **kwargs: pages[kwargs["Prefix"]]

    def get_object(*, Key, **_):
        marker, name = Key.removeprefix(prefix).split("/")
        if marker == "victim" and name == "late":
            raise ClientError({"Error": {"Code": "SlowDown"}}, "GetObject")
        item = next(item for item in items if item["marker"] == marker)
        arn = item["arn"] if marker == "peer" else f"{target}/durable-execution/victim/conflict"
        row = {
            **owner,
            "marker": marker,
            "phase": "WRAPPER_ENTER",
            "request": marker,
            "time": 1,
            "sequence": 1,
            "execution": arn,
        }
        return {"Body": io.BytesIO(json.dumps(row).encode())}

    driver.s3.get_object.side_effect = get_object
    with pytest.raises(module.TransientCollectionError, match="victim"):
        driver.refresh()
    assert items[0]["arn"] is None
    assert items[1]["arn"].endswith("/peer/original")


def test_settlement_drains_execution_whose_acknowledgment_arrives_late(driver):
    driver.case_context = Mock()
    launch: dict[str, str | None] = {"arn": None}
    driver.invocations = [launch]
    refreshes = 0

    def refresh(**_):
        nonlocal refreshes
        refreshes += 1
        if refreshes == 2:
            launch["arn"] = "late"
        return []

    driver.refresh = refresh
    driver.lam.get_durable_execution = Mock(side_effect=[{"Status": "RUNNING"}, {"Status": "STOPPED"}])
    driver.lam.stop_durable_execution = Mock()
    driver.wait = Mock()
    driver.settle_case()
    driver.lam.stop_durable_execution.assert_called_once_with(DurableExecutionArn="late")


@pytest.mark.parametrize("crossed", [False, True])
def test_custom_shared_output_and_verified_ledger_supply_missing_log_timing(driver, tmp_path, crossed):
    target = driver.manifest["functions"]["c1"]
    arn = target + "/durable-execution/fixture/id"
    output = tmp_path / "custom-output"
    journal = output / "standard/source/cases/case/invocations/fixture.json"
    journal.parent.mkdir(parents=True)
    journal.write_text(
        json.dumps(
            {
                "payload": {"run": "unit", "marker": "fixture"},
                "arn": arn,
                "function": target,
                "durableExecutionName": "fixture",
            }
        )
    )
    driver.refresh = Mock(return_value=[{"execution": arn, "request": "own", "time": 10, "marker": "fixture"}])
    driver.history = Mock(return_value=[])
    driver.lam.get_durable_execution = Mock(return_value={"Status": "FAILED"})
    driver.lam.get_function_configuration = Mock(return_value={"LoggingConfig": {"LogGroup": "group"}})
    driver.logs = Mock()
    own = {"timestamp": 10000, "message": '{"requestId":"own"}'}
    driver.logs.get_paginator.return_value.paginate.return_value = [
        {"events": [own, {"timestamp": 10001, "message": '{"requestId":"wrong-case","message":"another case"}'}]}
    ]
    if crossed:
        clean = driver.refresh.return_value[0]
        driver.refresh.return_value.append({**clean, "caseId": "another-case", "request": "wrong-case"})
        with pytest.raises(CollectionError, match="conflicting fixture identities"):
            driver.collect(shared_output=output)
    else:
        driver.collect(shared_output=output)
    assert json.loads((driver.artifacts_dir / "logs/c1.json").read_text()) == [own]


def test_retirement_uses_service_lookup_when_no_wrapper_was_published(driver):
    driver.case_context = Mock()
    driver.refresh = Mock(return_value=[])
    target = driver.manifest["functions"][driver.configuration]
    function, qualifier = target.rsplit(":", 1)
    arn = target + "/durable-execution/fixture/id"
    driver.invocations = [{"arn": None, "execution_name": "fixture", "payload": {"marker": "fixture"}}]
    driver.lam.list_durable_executions_by_function = Mock(
        return_value={
            "DurableExecutions": [{"DurableExecutionName": "fixture", "DurableExecutionArn": arn}],
        }
    )
    driver.lam.get_durable_execution = Mock(
        side_effect=[{"InputPayload": '{"marker":"fixture"}'}, {"Status": "RUNNING"}, {"Status": "STOPPED"}]
    )
    driver.lam.stop_durable_execution = Mock()
    driver.settle_case()
    driver.lam.list_durable_executions_by_function.assert_called_once_with(
        FunctionName=function, Qualifier=qualifier, DurableExecutionName="fixture"
    )
    driver.lam.stop_durable_execution.assert_called_once_with(DurableExecutionArn=arn)
    assert driver.invocations[0]["arn"] == arn


def test_cutoff_stop_retains_running_witness_across_lost_stop_response(driver):
    target = driver.manifest["functions"][driver.configuration]
    launch = {"arn": target + "/durable-execution/victim/id", "role": "victim"}
    driver.invocations = [launch]
    driver.lam.get_durable_execution = Mock(side_effect=[{"Status": "RUNNING"}, {"Status": "STOPPED"}])
    driver.lam.stop_durable_execution = Mock(
        side_effect=ClientError({"Error": {"Code": "TooManyRequestsException"}}, "StopDurableExecution")
    )
    with pytest.raises(module.TransientCollectionError):
        driver.stop_victim_after_cutoff(launch)
    driver.stop_victim_after_cutoff(launch)
    witness = driver.cutoff_results[launch["arn"]]
    assert witness["Status"] == "RUNNING"
    assert witness["stopResolved"]
    assert "collectionError" not in witness
    driver.lam.stop_durable_execution.assert_called_once()


def test_live_drain_retries_stop_after_a_running_execution_begins_pausing(driver):
    driver.invocations = [{"arn": "execution"}]
    driver.lam.get_durable_execution = Mock(
        side_effect=[{"Status": status} for status in ("RUNNING", "PAUSING", "PAUSED", "STOPPED")]
    )
    driver.lam.stop_durable_execution = Mock(
        side_effect=[
            ClientError({"Error": {"Code": "InvalidParameterValueException"}}, "StopDurableExecution"),
            {},
        ]
    )
    driver.refresh = Mock(return_value=[])
    driver.wait = Mock()
    driver.settle_case()
    assert driver.lam.stop_durable_execution.call_count == 2
    assert all(
        call.kwargs["DurableExecutionArn"] == "execution" for call in driver.lam.stop_durable_execution.call_args_list
    )


def test_summary_does_not_report_acceptance_from_previous_manifest(tmp_path):
    from aws_durable_execution_conformance_tests.lmi.artifacts import case_key, save

    save(tmp_path / "manifest.json", {"run": "previous"})
    save(
        tmp_path / "shared/standard/report.json",
        {"run": {"name": "previous"}, "results": [{"id": "old-test", "status": "PASS"}]},
    )
    save(tmp_path / "cleanup" / case_key("new") / "cleanup.json", {"runId": "new", "status": "NO_CASES"})
    output = summarize(tmp_path, expected_run="new")
    assert "Current cloud acceptance is unavailable" in output
    assert "old-test" not in output
    assert "No scenario work was recorded" in output


@pytest.mark.parametrize("code", ["AccessDenied", "ExpiredToken"])
def test_first_attempt_denied_hold_is_a_rejected_receipt(driver, code):
    driver.s3.put_object.side_effect = ClientError(
        {"Error": {"Code": code}, "ResponseMetadata": {"RetryAttempts": 0}}, "PutObject"
    )
    with pytest.raises(ClientError, match=code):
        driver.hold("peer")
    assert driver.control_records[0]["state"] == "REJECTED"
    assert driver.gates == set()
    driver.s3.get_object.assert_not_called()
    driver.delete_controls()
    driver.s3.delete_object.assert_not_called()


@pytest.mark.parametrize("response", [{}, None, {"ResponseMetadata": {"RetryAttempts": False}}])
def test_hold_without_integer_zero_retry_receipt_cannot_be_deleted(driver, response):
    driver.s3.put_object.return_value = response
    driver.hold("gate")
    assert driver.control_records[0]["state"] == "PENDING"
    driver.s3.get_object.side_effect = lambda **_: {"Body": io.BytesIO(b"hold"), "ETag": '"held"'}
    driver.release_all()
    with pytest.raises(CollectionError, match="unresolved hold writes"):
        driver.delete_controls()
    driver.s3.delete_object.assert_not_called()


def test_same_function_mismatched_invoke_receipt_leaves_launch_submitted(driver):
    target = driver.manifest["functions"][driver.configuration]
    driver.lam.invoke = Mock(
        return_value={"StatusCode": 202, "DurableExecutionArn": target + "/durable-execution/foreign/id"}
    )
    with pytest.raises(CollectionError, match="invocation API failed"):
        driver.start("fixture", marker="marker")
    journal = json.loads((driver.artifacts_dir / "invocations/marker.json").read_text())
    assert journal["dispatchState"] == "SUBMITTED"
    assert "arn" not in journal
    assert driver.invocations[0]["arn"] is None


@pytest.mark.parametrize("marker", ["group/a", "group\\a", ".."])
def test_invalid_marker_is_rejected_before_control_or_invoke(driver, marker):
    driver.lam.invoke = Mock()
    with pytest.raises(CollectionError, match="single path segment"):
        driver.start("fixture", marker=marker, gate="fault")
    driver.s3.put_object.assert_not_called()
    driver.lam.invoke.assert_not_called()
    assert not (driver.artifacts_dir / "invocations").exists()


@pytest.mark.parametrize("phase", ["collection", "partialCollection"])
def test_optional_collection_returns_inside_outer_phase_budget(driver, monkeypatch, phase):
    import asyncio

    from aws_durable_execution_conformance_tests.lmi.coordinator import _bounded
    from aws_durable_execution_conformance_tests.lmi.scenarios import ScenarioDriver

    from .test_shared_partial_evidence import context

    ctx = context()
    driver.case_context = ctx
    driver.manifest["driverTimeout"] = 120
    scenario = ScenarioDriver(ctx, driver, runtime="python", fixtures={})
    read = Mock(return_value=False)
    monkeypatch.setattr(scenario, "seconds", lambda *_: 0.01)
    monkeypatch.setattr(scenario, "_collect_observation", read)

    async def exercise():
        residual: list[asyncio.Task[Any]] = []
        await _bounded(asyncio.to_thread(scenario.collect), phase, 0.5, ctx, residual)
        assert residual == []
        assert ctx._collection_deadline is None

    asyncio.run(exercise())
    read.assert_called_once()


def test_service_history_retains_native_completion_from_longer_snapshot(driver):
    arn = "execution"
    rows = [
        {"EventId": 1, "EventType": "ExecutionStarted"},
        {"EventId": 2, "EventType": "ExecutionStopped"},
        {"EventId": 3, "EventType": "InvocationCompleted", "InvocationCompletedDetails": {"RequestId": "later"}},
    ]
    driver.history_page = Mock(side_effect=[{"Events": rows}, {"Events": rows[:2]}])
    item = {"arn": arn, "marker": "marker"}
    assert driver.history(item) == rows
    assert driver.history(item) == rows
    assert driver.raw_histories[arn] == rows
    changed = {**rows[2], "InvocationCompletedDetails": {"RequestId": "different"}}
    driver.history_page.side_effect = [{"Events": [changed]}, {"Events": rows}]
    with pytest.raises(CollectionError, match="Conflicting service history"):
        driver.history(item)
    with pytest.raises(CollectionError, match="Conflicting service history"):
        driver.history(item)


def test_duplicate_launch_cannot_replace_accepted_receipt(tmp_path, driver):
    driver.lam.invoke = Mock(
        side_effect=lambda **kw: {
            "StatusCode": 202,
            "DurableExecutionArn": f"{kw['FunctionName']}/durable-execution/{kw['DurableExecutionName']}/id",
        }
    )
    launch = driver.start("fixture", marker="marker")
    journal = tmp_path / "invocations/marker.json"
    accepted = journal.read_bytes()
    with pytest.raises(CollectionError, match="already has a journal"):
        driver.start("fixture", marker="marker", gate="another")
    assert journal.read_bytes() == accepted
    assert json.loads(accepted)["arn"] == launch["arn"]
    driver.s3.put_object.assert_not_called()
    driver.lam.invoke.assert_called_once()


def test_collection_does_not_start_read_without_configured_attempt_allowance(driver):
    import time

    from botocore.config import Config

    from .test_shared_partial_evidence import context

    driver.case_context = context()
    driver.case_context._collection_deadline = time.monotonic() + 2
    driver.lam = Mock()
    driver.lam.meta.config = Config(connect_timeout=3, read_timeout=10, retries={"total_max_attempts": 2})
    with pytest.raises(module.CollectionBudgetExpired, match="Insufficient collection time"):
        driver.history({"arn": "execution", "marker": "one"})
    driver.lam.get_durable_execution_history.assert_not_called()


def test_in_flight_read_after_collection_deadline_still_requires_outer_reconciliation(driver):
    import asyncio
    from threading import Event

    from aws_durable_execution_conformance_tests.lmi.coordinator import PhaseTimeout, _bounded

    from .test_shared_partial_evidence import context

    ctx = context()
    driver.case_context = ctx
    driver.lam = Mock()
    started, allow_return, returned = Event(), Event(), Event()

    def reads():
        driver._check_read_budget(driver.lam)  # No advertised bound on this injected transport.
        started.set()
        allow_return.wait(2)
        returned.set()

    async def exercise():
        residual: list[asyncio.Task[Any]] = []
        try:
            with pytest.raises(PhaseTimeout):
                await _bounded(asyncio.to_thread(reads), "collection", 0.2, ctx, residual)
            assert started.is_set()
            assert not returned.is_set()
            assert len(residual) == 1
        finally:
            allow_return.set()
        assert await asyncio.to_thread(returned.wait, 1)

    asyncio.run(exercise())


def test_full_collection_refuses_foreign_service_input_despite_current_ledger(driver):
    target = driver.manifest["functions"]["c1"]
    arn = target + "/durable-execution/old/id"
    driver.refresh = Mock(
        return_value=[
            {"execution": arn, "run": "unit", "marker": "marker", "request": "request", "time": 1},
        ]
    )
    driver.lam.get_durable_execution = Mock(
        return_value={
            "Status": "SUCCEEDED",
            "Result": "foreign output",
            "InputPayload": '{"run":"old","marker":"marker"}',
        }
    )
    driver.history = Mock()
    driver._collect_logs = Mock()
    with pytest.raises(CollectionError):
        driver.collect()
    driver.lam.get_durable_execution.assert_called_once_with(DurableExecutionArn=arn, IncludeExecutionData=True)
    driver.history.assert_not_called()
    assert driver._collect_logs.call_args.args[2:] == ([], [])
    assert not (driver.artifacts_dir / "executions").exists()


def test_default_collection_narrows_attempts_and_socket_timeouts(driver, monkeypatch):
    import time

    from botocore.config import Config

    from .test_shared_partial_evidence import context

    driver.case_context = context()
    driver.case_context._collection_deadline = time.monotonic() + 6
    driver.single_attempt_fences = True
    transport = Mock()

    def create(_service, _region, *, single_attempt, connect_timeout, read_timeout):
        assert single_attempt
        assert 0 < connect_timeout <= 2
        assert 0 < read_timeout <= 2
        transport.meta.config = Config(
            connect_timeout=connect_timeout, read_timeout=read_timeout, retries={"total_max_attempts": 1}
        )
        return transport

    monkeypatch.setattr(module, "client", create)
    assert driver._read_transport("lambda", driver.lam) is transport


def test_s3_refresh_retains_completed_batch_and_preserves_budget_expiry(driver):
    prefix = "runs/unit/events/"
    keys = [f"{prefix}{i}.json" for i in range(9)]
    driver.s3.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": key} for key in keys], "IsTruncated": False},
    ]

    def read(*, Key, **_):
        record = {
            "run": "unit",
            "commit": "commit",
            "marker": "one",
            "request": "request",
            "phase": "WRAPPER_ENTER",
            "time": 1,
            "sequence": int(Key.split("/")[-1].split(".")[0]),
        }
        return {"Body": io.BytesIO(json.dumps(record).encode())}

    driver.s3.get_object.side_effect = read
    driver._read_transport = Mock(
        side_effect=[
            driver.s3,
            driver.s3,
            module.CollectionBudgetExpired("remaining collection margin"),
        ]
    )
    with pytest.raises(module.CollectionBudgetExpired, match="remaining collection margin"):
        driver.refresh(markers=[])
    assert driver.s3.get_object.call_count == 8
    assert len(json.loads((driver.artifacts_dir / "events.json").read_text())) == 8


def test_cutoff_stop_and_completion_race_use_deadline_transport(driver):
    target = driver.manifest["functions"][driver.configuration]
    launch = {"arn": target + "/durable-execution/victim/id", "role": "victim"}
    driver.invocations = [launch]
    transport = Mock()
    transport.get_durable_execution.side_effect = [{"Status": "RUNNING"}, {"Status": "STOPPED"}]
    transport.stop_durable_execution.side_effect = ClientError(
        {"Error": {"Code": "InvalidParameterValueException"}}, "StopDurableExecution"
    )
    driver._read_transport = Mock(return_value=transport)
    driver.lam = Mock()
    driver.stop_victim_after_cutoff(launch)
    assert driver.cutoff_results[launch["arn"]]["stopResolved"]
    assert transport.get_durable_execution.call_count == 2
    transport.stop_durable_execution.assert_called_once()
    driver.lam.get_durable_execution.assert_not_called()
    driver.lam.stop_durable_execution.assert_not_called()


def test_summary_reads_direct_run_matched_cli_report(tmp_path):
    saved = tmp_path / "shared"
    saved.mkdir()
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "run": "unit",
                "commit": "commit",
                "runtime": "python",
                "concurrencies": {"c1": 1},
            }
        )
    )
    (saved / "manifest.json").write_text(json.dumps({"runId": "unit"}))
    (saved / "report.json").write_text(
        json.dumps(
            {
                "run": {"name": "unit"},
                "results": [{"id": "fixture", "status": "PASSED"}],
                "ci": {"exit_code": 0},
            }
        )
    )
    assert "fixture: PASSED" in summarize(tmp_path)
