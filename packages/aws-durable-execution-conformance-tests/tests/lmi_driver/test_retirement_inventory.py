# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Every service callback and case barrier requires positive retirement evidence."""

import io
import json
from copy import deepcopy
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock, patch

import pytest
from botocore.exceptions import ClientError

from aws_durable_execution_conformance_tests.lmi.cloud import Cloud
from aws_durable_execution_conformance_tests.lmi.errors import CollectionError
from aws_durable_execution_conformance_tests.lmi.evidence import validate_retirement_evidence

from .support import build_evidence
from .test_shared_partial_evidence import context


def normalized(tmp_path, *, callback_outcome="SUCCEEDED", control_deleted=True, callback_timestamp=0.9):
    ctx = context()
    arn = "execution"
    attempt = {
        "requestId": "request",
        "executionArn": arn,
        "environmentId": "environment",
        "workerId": "worker",
        "processId": "process",
        "role": "victim",
        "attempt": 1,
        "startedAtMs": 100,
        "endedAtMs": 1000,
        "outcome": "SUCCEEDED",
    }
    events = [
        {
            "requestId": "request",
            "ownerRequestId": "request",
            "executionArn": arn,
            "sequence": sequence,
            "kind": kind,
            "atMs": when,
            **details,
        }
        for sequence, (kind, when, details) in enumerate(
            [
                ("wrapper_enter", 100, {}),
                ("task_start", 101, {"taskId": "root"}),
                ("task_exit", 999, {"taskId": "root"}),
                ("wrapper_return", 1000, {"outcome": "SUCCEEDED"}),
            ]
        )
    ]
    baseline = {
        "attempts": [attempt],
        "selectedRequests": {"victim": "request"},
        "events": events,
        "histories": [{"executionArn": arn, "ownerCaseId": "case", "status": "SUCCEEDED", "operations": []}],
    }
    raw = [{"EventType": "CallbackStarted", "Id": "callback-operation", "EventTimestamp": 0.3}]
    if callback_outcome:
        raw.append(
            {
                "EventType": {
                    "SUCCEEDED": "CallbackSucceeded",
                    "FAILED": "CallbackFailed",
                    "TIMED_OUT": "CallbackTimedOut",
                }[callback_outcome],
                "Id": "callback-operation",
                "EventTimestamp": callback_timestamp,
            }
        )
    raw = [
        {"EventType": "ExecutionStarted", "EventTimestamp": 0.1},
        *raw,
        {"EventType": "ExecutionSucceeded", "EventTimestamp": 1},
        {"EventType": "InvocationCompleted", "InvocationCompletedDetails": {"RequestId": "request"}},
    ]
    for event_id, event in enumerate(raw, 1):
        event["EventId"] = event_id
    ledger = [
        {"phase": "WRAPPER_ENTER", "request": "request", "sequence": 1},
        *(
            {
                "phase": "CONFORMANCE",
                "request": "request",
                "sequence": sequence + 1,
                "observation": {"captureSequence": sequence},
            }
            for sequence, _ in enumerate(events, 1)
        ),
        {
            "phase": "SEALED",
            "request": "request",
            "sequence": len(events) + 2,
            "sourceProtocol": 2,
            "throughCapture": len(events),
        },
    ]
    for row in ledger:
        row.update(marker="victim", execution=arn)
    records = [
        {"gate": "release-all", "action": "hold", "time": 0.1},
        {"gate": "release-all", "action": "release", "time": 20, "state": "ACKNOWLEDGED"},
    ]
    if control_deleted:
        records.append({"gate": "release-all", "action": "delete", "time": 20.5, "state": "ACKNOWLEDGED"})
    execution_receipt = tmp_path / "executions" / "victim.json"
    execution_receipt.parent.mkdir(exist_ok=True)
    execution_receipt.write_text('{"Status":"SUCCEEDED"}')
    cloud = SimpleNamespace(
        manifest={"bucket": "private", "invocationTimeout": 60},
        observation_ended_at=20,
        retired_at=21,
        source_uris={"memory://service", execution_receipt.as_uri()},
        raw_histories={arn: raw},
        execution_results={arn: {"Status": "SUCCEEDED", "EndTimestamp": 1}},
        terminal_histories={arn},
        events=dict(enumerate(ledger)),
        control_records=records,
        control_prefix="runs/run/cases/case/control/",
        invocations=[{"arn": arn, "marker": "victim", "native_requests": ["request"]}],
        artifacts_dir=tmp_path,
        _save=lambda *args: None,
    )
    sources = SimpleNamespace(
        observations=lambda **_: (deepcopy(baseline["events"]), []),
        histories=lambda: (baseline["histories"], {}, []),
        attempts=lambda _, **kwargs: (baseline["attempts"], baseline["selectedRequests"], []),
    )
    with patch("aws_durable_execution_conformance_tests.lmi.source_evidence.CaseSources.capture", return_value=sources):
        return build_evidence(ctx, cloud, phase="final")


@pytest.mark.parametrize("callback_outcome", ["SUCCEEDED", "FAILED", "TIMED_OUT"])
def test_callbacks_and_controls_are_inventoried_with_independent_receipts(tmp_path, callback_outcome):
    evidence = normalized(tmp_path, callback_outcome=callback_outcome)
    assert {item["kind"] for item in evidence["resourcesCreated"]} == {"invocation", "execution", "callback", "control"}
    execution = next(item for item in evidence["retirement"]["resources"] if item["kind"] == "execution")
    assert execution["evidenceUri"].endswith("/executions/victim.json")
    callback = next(item for item in evidence["retirement"]["resources"] if item["kind"] == "callback")
    assert callback["outcome"] == callback_outcome
    assert callback["settledAtMs"] == 900
    assert callback["evidenceUri"].endswith("histories/victim.json")
    control = next(item for item in evidence["retirement"]["resources"] if item["kind"] == "control")
    assert control["outcome"] == "DELETED"
    assert control["settledAtMs"] == 20500
    assert validate_retirement_evidence(evidence) == []


@pytest.mark.parametrize("missing", ["callback", "control"])
def test_unsettled_resources_fail_the_independent_retirement_audit(tmp_path, missing):
    evidence = normalized(
        tmp_path, callback_outcome=None if missing == "callback" else "SUCCEEDED", control_deleted=missing != "control"
    )
    resource = next(item for item in evidence["retirement"]["resources"] if item["kind"] == missing)
    assert resource["outcome"] == "UNKNOWN"
    errors = validate_retirement_evidence(evidence)
    assert any(
        ("callback completion" if missing == "callback" else "control object deletion") in error for error in errors
    )


@pytest.mark.parametrize("timestamp", [None, float("nan"), float("inf"), -1, 22])
def test_invalid_callback_completion_times_cannot_establish_retirement(tmp_path, timestamp):
    evidence = normalized(tmp_path, callback_timestamp=timestamp)
    assert validate_retirement_evidence(evidence)


def control_cloud(tmp_path):
    storage = Mock()
    storage.put_object.return_value = {"ResponseMetadata": {"RetryAttempts": 0}}
    storage.get_object.side_effect = lambda **_: {"Body": io.BytesIO(b"hold"), "ETag": '"hold"'}
    cloud = Cloud(
        {"run": "run", "region": "us-west-2", "bucket": "private"},
        artifacts_dir=tmp_path,
        verify_profile=Mock(),
        client_factory=lambda service, region: storage if service == "s3" else Mock(),
    )
    cloud.hold("release-all")
    cloud.release_all()
    return cloud, storage


def test_deleted_controls_have_verified_absence_and_persisted_receipts(tmp_path):
    cloud, storage = control_cloud(tmp_path)
    storage.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
    cloud.delete_controls()
    request = {"Bucket": "private", "Key": "runs/run/control/release-all"}
    storage.delete_object.assert_called_once_with(**request)
    storage.head_object.assert_called_once_with(**request)
    receipt = json.loads((tmp_path / "control-records.json").read_text())[-1]
    assert receipt["action"] == "delete"
    assert receipt["state"] == "ACKNOWLEDGED"
    assert receipt["started"] <= receipt["time"]
    cloud.delete_controls()
    storage.delete_object.assert_called_once()


@pytest.mark.parametrize("failure", ["retained", "access-denied", "transport"])
def test_failed_or_ambiguous_deletion_never_establishes_retirement(tmp_path, failure):
    cloud, storage = control_cloud(tmp_path)
    if failure == "access-denied":
        storage.head_object.side_effect = ClientError({"Error": {"Code": "AccessDenied"}}, "HeadObject")
    elif failure == "transport":
        storage.delete_object.side_effect = OSError("lost response")
    with pytest.raises((CollectionError, OSError)):
        cloud.delete_controls()
    receipt = json.loads((tmp_path / "control-records.json").read_text())[-1]
    assert receipt["action"] == "delete"
    assert receipt["state"] == "PENDING"
    assert "time" not in receipt


@pytest.mark.parametrize("dispatch", ["PREPARED", "REJECTED"])
def test_control_only_retirement_requires_no_dispatch_and_verified_deletes(tmp_path, monkeypatch, dispatch):
    from aws_durable_execution_conformance_tests.lmi.artifacts import case_key
    from aws_durable_execution_conformance_tests.lmi.evidence import validate_evidence
    from aws_durable_execution_conformance_tests.lmi.scenarios import ScenarioDriver
    from aws_durable_execution_conformance_tests.lmi.service import DurableFunctionRunner

    ctx = context()
    ctx.scenario = ctx.scenario.__class__(
        ctx.scenario.case_id,
        "lmi-lifecycle-5",
        ctx.profile.profile_id,
        "suspension-retry",
        roles=("victim", "resume"),
        coverage_profile="standard",
    )
    storage = Mock()
    storage.put_object.return_value = {"ResponseMetadata": {"RetryAttempts": 0}}
    storage.get_object.side_effect = lambda **_: {"Body": io.BytesIO(b"hold"), "ETag": '"hold"'}
    storage.get_paginator.return_value.paginate.return_value = []
    storage.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
    transport = Mock()
    cloud = Cloud(
        {
            "run": ctx.run_id,
            "region": "us-west-2",
            "bucket": "private",
            "commit": "sdk",
            "functions": {"c1": ctx.profile.resource_id},
            "invocationTimeout": 60,
        },
        "c1",
        artifacts_dir=tmp_path / case_key(ctx.scenario.case_id),
        verify_profile=Mock(),
        case_context=ctx,
        client_factory=lambda service, region: storage if service == "s3" else transport,
    )
    if dispatch == "PREPARED":
        monkeypatch.setattr(DurableFunctionRunner, "run_async", Mock(side_effect=OSError("before transport")))
    else:
        transport.invoke.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException"}, "ResponseMetadata": {"RetryAttempts": 0}}, "Invoke"
        )
    driver = ScenarioDriver(ctx, cloud, runtime="python", fixtures={"suspension-retry": "replay"})
    with pytest.raises(CollectionError, match="invocation API failed"):
        driver.start()
    assert driver.retire()
    evidence = build_evidence(ctx, cloud, phase="final")
    assert evidence["retirement"]["mode"] == "controls-only"
    assert evidence["retirement"]["undispatchedLaunches"][0]["dispatchState"] == dispatch
    assert evidence["collection"]["watermarks"] == {}
    assert not evidence["attempts"] and "replay" not in evidence
    assert validate_retirement_evidence(evidence) == []
    assert validate_evidence(evidence)
    superseded = deepcopy(evidence)
    next(row for row in superseded["controlRecords"] if row["action"] == "hold")["state"] = "SUPERSEDED"
    assert any("unresolved control receipt" in error for error in validate_retirement_evidence(superseded))
    evidence["controlRecords"].append({"action": "hold", "gate": "release-all", "state": "PENDING"})
    assert any("unresolved control receipt" in error for error in validate_retirement_evidence(evidence))
    assert storage.head_object.call_count == 1
    assert bool(ctx.attempts) is (dispatch == "REJECTED")
    if dispatch == "REJECTED":
        marker = ctx.attempts[0]["marker"]
        storage.get_paginator.return_value.paginate.assert_called_with(
            Bucket="private", Prefix=f"runs/{ctx.run_id}/events/{marker}/"
        )
        journal = cloud.artifacts_dir / "invocations" / f"{marker}.json"
        corrupted = json.loads(journal.read_text())
        corrupted["retryAttempts"] = 1
        journal.write_text(json.dumps(corrupted))
        assert validate_retirement_evidence(build_evidence(ctx, cloud, phase="final"))

    for mutation in ("crossed-control", "unknown-delete", "different-delete-time", "runtime-evidence", "deferred"):
        damaged = deepcopy(evidence)
        settlement = damaged["retirement"]["resources"][0]
        if mutation == "crossed-control":
            damaged["resourcesCreated"][0]["id"] = settlement["id"] = "s3://private/runs/other/control/release-all"
        elif mutation == "unknown-delete":
            damaged["controlRecords"][-1]["state"] = "PENDING"
        elif mutation == "different-delete-time":
            settlement["settledAtMs"] -= 0.1
        elif mutation == "deferred":
            damaged["collection"]["deferredServiceRequests"] = [{"requestId": "unsettled", "executionArn": "owned"}]
        else:
            damaged["replay"] = {}
        assert validate_retirement_evidence(damaged), mutation


def test_retirement_deletes_controls_only_after_runtime_quiescence(monkeypatch):
    from aws_durable_execution_conformance_tests.lmi.scenarios import ScenarioDriver

    actions = []
    cloud = SimpleNamespace(
        invocations=[],
        manifest={},
        refresh=lambda **kwargs: actions.append("refresh"),
        settle_case=lambda: actions.append("release-and-settle"),
        poll=lambda predicate, **kwargs: predicate(),
        collect=lambda **kwargs: actions.append("collect"),
        delete_controls=lambda: actions.append("delete"),
    )
    driver = object.__new__(ScenarioDriver)
    driver.cloud = cast(Any, cloud)

    def quiescent():
        actions.append("quiescent")
        return True

    monkeypatch.setattr(driver, "quiescent", quiescent)
    driver.retire()
    assert actions == ["release-and-settle", "collect", "quiescent", "delete"]
    assert cloud.observation_ended_at <= cloud.retired_at


def test_failed_settlement_does_not_delete_controls():
    from aws_durable_execution_conformance_tests.lmi.scenarios import ScenarioDriver

    cloud = SimpleNamespace(
        settle_case=Mock(side_effect=CollectionError("unresolved Invoke")),
        delete_controls=Mock(),
    )
    driver = object.__new__(ScenarioDriver)
    driver.cloud = cast(Any, cloud)
    with pytest.raises(CollectionError, match="unresolved Invoke"):
        driver.retire()
    cloud.settle_case.assert_called_once_with()
    cloud.delete_controls.assert_not_called()


def test_collection_cutoff_is_set_before_reading_sources(monkeypatch):
    from aws_durable_execution_conformance_tests.lmi.scenarios import ScenarioDriver

    times = iter([5.0, 6.0])
    monkeypatch.setattr(
        "aws_durable_execution_conformance_tests.lmi.scenarios.time",
        SimpleNamespace(time=lambda: next(times)),
    )
    cloud = SimpleNamespace(
        invocations=[],
        manifest={},
        refresh=Mock(),
        settle_case=Mock(),
        poll=lambda predicate, **kwargs: predicate(),
        delete_controls=Mock(),
    )

    def collect(**kwargs):
        assert cloud.observation_ended_at == 5
        cloud.delete_controls.assert_not_called()

    cloud.collect = collect
    driver = object.__new__(ScenarioDriver)
    driver.cloud = cast(Any, cloud)
    monkeypatch.setattr(driver, "quiescent", lambda: True)
    driver.retire()
    assert cloud.observation_ended_at == 5
    assert cloud.retired_at == 6


@pytest.mark.parametrize(
    ("scenario", "fixtures", "missing"),
    [
        ("peer-recovery", {"peer-recovery": "deadline", "recovery": "recovery"}, "peer"),
        ("peer-recovery", {"peer-recovery": "deadline", "peer": "peer"}, "recovery"),
        ("warm-reuse", {"warm-reuse": "owner"}, "warm"),
    ],
)
def test_missing_auxiliary_fixture_prevents_any_launch(scenario, fixtures, missing):
    from aws_durable_execution_conformance_tests.lmi.scenarios import ScenarioDriver

    cloud = Mock()
    ctx = cast(Any, SimpleNamespace(scenario=SimpleNamespace(scenario=scenario)))
    driver = ScenarioDriver(ctx, cloud, runtime="python", fixtures=fixtures)
    with pytest.raises(NotImplementedError, match=f"No SDK fixture for {missing}"):
        driver.start()
    cloud.start.assert_not_called()


def test_retirement_rereads_terminal_history_and_a_new_requests_seal(monkeypatch):
    from aws_durable_execution_conformance_tests.lmi.scenarios import ScenarioDriver

    times = iter([5.0, 6.0, 7.0, 8.0])
    monkeypatch.setattr(
        "aws_durable_execution_conformance_tests.lmi.scenarios.time",
        SimpleNamespace(time=lambda: next(times)),
    )
    arn, marker = "execution", "marker"

    def stream(request):
        owner = {"execution": arn, "marker": marker, "request": request}
        return [
            {**owner, "phase": "WRAPPER_ENTER", "sequence": 1},
            {**owner, "phase": "WRAPPER_RETURN", "sequence": 2},
            {
                **owner,
                "phase": "CONFORMANCE",
                "sequence": 3,
                "observation": {"kind": "task_start", "taskId": "root", "captureSequence": 1, "atMs": 3000},
            },
            {
                **owner,
                "phase": "CONFORMANCE",
                "sequence": 4,
                "observation": {"kind": "task_exit", "taskId": "root", "captureSequence": 2, "atMs": 4000},
            },
            {**owner, "phase": "SEALED", "sequence": 5, "sourceProtocol": 2, "throughCapture": 2},
        ]

    def completed(identifier, request):
        return {
            "EventType": "InvocationCompleted",
            "EventId": identifier,
            "InvocationCompletedDetails": {"RequestId": request, "StartTimestamp": 1, "EndTimestamp": 4},
        }

    actions = []
    cloud = SimpleNamespace(
        invocations=[{"arn": arn, "marker": marker, "native_requests": ["original"]}],
        manifest={},
        events=dict(enumerate(stream("original"))),
        raw_histories={},
        terminal_histories={arn},
        execution_results={arn: {"Status": "STOPPED", "EndTimestamp": 4}},
        settle_case=lambda: actions.append("settle"),
        delete_controls=lambda: actions.append("delete"),
    )
    cloud.refresh = lambda **_: list(cloud.events.values())

    def collect(**_):
        actions.append("collect")
        reads = actions.count("collect")
        history = [{"EventType": "ExecutionStarted", "EventId": 1}, completed(2, "original")]
        if reads >= 2:
            history += [{"EventType": "ExecutionStopped", "EventId": 3}, completed(4, "retry")]
        cloud.raw_histories[arn] = history
        if reads == 3:
            cloud.invocations[0]["native_requests"].append("retry")
            cloud.events.update(dict(enumerate(stream("retry"), len(cloud.events))))

    def poll(predicate, **_):
        assert not predicate()
        assert not predicate()
        assert predicate()

    cloud.collect = collect
    cloud.poll = poll
    driver = object.__new__(ScenarioDriver)
    driver.cloud = cast(Any, cloud)
    assert driver.retire()
    assert actions == ["settle", "collect", "collect", "collect", "delete"]
    assert cloud.observation_ended_at == 7
    assert cloud.retired_at == 8


def test_retirement_accepts_a_request_the_service_proves_undispatched_without_its_seal(monkeypatch):
    from aws_durable_execution_conformance_tests.lmi.scenarios import ScenarioDriver

    times = iter([5.0, 6.0])
    monkeypatch.setattr(
        "aws_durable_execution_conformance_tests.lmi.scenarios.time",
        SimpleNamespace(time=lambda: next(times)),
    )
    arn, marker = "execution", "marker"

    def stream(request):
        owner = {"execution": arn, "marker": marker, "request": request}
        return [
            {**owner, "phase": "WRAPPER_ENTER", "sequence": 1},
            {**owner, "phase": "WRAPPER_RETURN", "sequence": 2},
            {
                **owner,
                "phase": "CONFORMANCE",
                "sequence": 3,
                "observation": {"kind": "task_start", "taskId": "root", "captureSequence": 1, "atMs": 3000},
            },
            {
                **owner,
                "phase": "CONFORMANCE",
                "sequence": 4,
                "observation": {"kind": "task_exit", "taskId": "root", "captureSequence": 2, "atMs": 4000},
            },
            {**owner, "phase": "SEALED", "sequence": 5, "sourceProtocol": 2, "throughCapture": 2},
        ]

    def completed(identifier, request, *, error=None):
        details: dict[str, Any] = {"RequestId": request, "StartTimestamp": 1, "EndTimestamp": 4}
        if error is not None:
            details["Error"] = error
        return {"EventType": "InvocationCompleted", "EventId": identifier, "InvocationCompletedDetails": details}

    rejection = {
        "Payload": {"ErrorType": "TooManyRequestsException", "ErrorMessage": "capacity"},
        "Truncated": False,
    }
    actions = []
    cloud = SimpleNamespace(
        invocations=[{"arn": arn, "marker": marker, "native_requests": ["original"]}],
        manifest={},
        events=dict(enumerate(stream("original"))),
        raw_histories={
            arn: [
                {"EventType": "ExecutionStarted", "EventId": 1},
                completed(2, "original"),
                {"EventType": "ExecutionStopped", "EventId": 3},
                completed(4, "rejected", error=rejection),
            ]
        },
        terminal_histories={arn},
        execution_results={arn: {"Status": "STOPPED", "EndTimestamp": 4}},
        settle_case=lambda: actions.append("settle"),
        delete_controls=lambda: actions.append("delete"),
    )
    cloud.refresh = lambda **_: list(cloud.events.values())
    cloud.collect = lambda **_: actions.append("collect")
    cloud.poll = lambda predicate, **_: predicate() or pytest.fail("retirement readiness was not proven")
    driver = object.__new__(ScenarioDriver)
    driver.cloud = cast(Any, cloud)
    assert driver.retire()
    assert actions == ["settle", "collect", "delete"]
    assert cloud.retired_at == 6.0


@pytest.mark.parametrize("recovers", [True, False])
def test_retirement_retries_transient_reads_within_poll_budget(recovers):
    from aws_durable_execution_conformance_tests.lmi.errors import TransientCollectionError
    from aws_durable_execution_conformance_tests.lmi.scenarios import ScenarioDriver

    cloud = SimpleNamespace(
        invocations=[],
        events={},
        manifest={},
        settle_case=Mock(),
        refresh=Mock(return_value=[]),
        collect=Mock(
            side_effect=[TransientCollectionError("history not visible"), None]
            if recovers
            else TransientCollectionError("history not visible")
        ),
        delete_controls=Mock(),
    )

    def poll(predicate, *, category, message, **_):
        assert not predicate()
        if recovers:
            assert predicate()
        else:
            raise category(message)

    cloud.poll = poll
    driver = object.__new__(ScenarioDriver)
    driver.cloud = cast(Any, cloud)
    if recovers:
        assert driver.retire()
        assert cloud.collect.call_count == 2
        cloud.delete_controls.assert_called_once()
    else:
        with pytest.raises(CollectionError, match="history not visible"):
            driver.retire()
        cloud.delete_controls.assert_not_called()


def test_retirement_rereads_after_quiescence_binds_a_new_native_request(monkeypatch):
    from aws_durable_execution_conformance_tests.lmi.scenarios import ScenarioDriver

    requests = ["original"]
    active = {"arn": "owned", "native_requests": requests}
    driver = object.__new__(ScenarioDriver)
    cloud = SimpleNamespace(invocations=[active], events={}, collect=Mock())
    driver.cloud = cast(Any, cloud)

    def quiescent():
        requests.append("just-discovered")
        return True

    monkeypatch.setattr(driver, "quiescent", quiescent)
    assert not driver._retirement_ready()
    cloud.collect.assert_called_once()


def _undispatched_retirement_fixture():
    from pathlib import Path
    from runpy import run_path

    module = run_path(str(Path(__file__).parents[1] / "test_lmi_evidence.py"))
    value = module["evidence"]()
    module["complete_retirement_events"](value)
    resource = {"id": "rejected", "kind": "invocation", "ownerCaseId": value["caseId"]}
    value["resourcesCreated"].append(resource)
    value["retirement"]["resources"].append(
        {
            **resource,
            "executionArn": value["attempts"][0]["executionArn"],
            "serviceErrorType": "TooManyRequestsException",
            "sourceEventId": 99,
            "startedAtMs": 5,
            "settledAtMs": 6,
            "evidenceUri": value["collection"]["evidenceUris"][0],
            "outcome": "UNDISPATCHED",
        }
    )
    return value


def test_undispatched_settlement_with_a_consistent_proof_passes_independent_retirement():
    assert validate_retirement_evidence(_undispatched_retirement_fixture()) == []


@pytest.mark.parametrize(
    "mutate",
    [
        lambda resource, value: resource.update(serviceErrorType="ClientError"),
        lambda resource, value: resource.update(executionArn="arn:execution:foreign"),
        lambda resource, value: resource.pop("sourceEventId"),
        lambda resource, value: resource.update(startedAtMs=resource["settledAtMs"] + 1),
        lambda resource, value: resource.update(settledAtMs=value["retirement"]["retiredAtMs"] + 1000),
        lambda resource, value: resource.update(evidenceUri="s3://evidence/unretained.json"),
    ],
    ids=[
        "unlisted-error-type",
        "foreign-execution-arn",
        "missing-source-event-id",
        "started-after-settled",
        "settled-after-retired",
        "evidence-uri-not-retained",
    ],
)
def test_tampered_undispatched_proof_fields_fail_independent_retirement(mutate):
    value = _undispatched_retirement_fixture()
    resource = next(item for item in value["retirement"]["resources"] if item["id"] == "rejected")
    mutate(resource, value)
    assert (
        "INCOMPLETE: undispatched invocation rejected lacks a consistent service rejection proof"
        in validate_retirement_evidence(value)
    )


def test_duplicate_source_event_id_fails_independent_retirement():
    value = _undispatched_retirement_fixture()
    original = next(item for item in value["retirement"]["resources"] if item["id"] == "rejected")
    duplicate = {**original, "id": "rejected-again"}
    value["resourcesCreated"].append({"id": "rejected-again", "kind": "invocation", "ownerCaseId": value["caseId"]})
    value["retirement"]["resources"].append(duplicate)
    errors = validate_retirement_evidence(value)
    assert {
        "INCOMPLETE: undispatched invocation rejected lacks a consistent service rejection proof",
        "INCOMPLETE: undispatched invocation rejected-again lacks a consistent service rejection proof",
    } <= set(errors)


@pytest.mark.parametrize("field", ["requestId", "ownerRequestId", "observedDuringRequestId"])
def test_a_reference_anywhere_to_an_undispatched_request_fails_independent_retirement(field):
    value = _undispatched_retirement_fixture()
    owner = value["attempts"][0]
    value["events"].append(
        {
            "requestId": owner["requestId"],
            "ownerRequestId": owner["requestId"],
            "executionArn": owner["executionArn"],
            "kind": "task_start",
            "atMs": owner["startedAtMs"] + 1,
            "taskId": "scratch",
            field: "rejected",
        }
    )
    assert (
        "INCOMPLETE: undispatched invocation rejected lacks a consistent service rejection proof"
        in validate_retirement_evidence(value)
    )


def test_an_undispatched_id_equal_to_an_existing_attempt_fails_independent_retirement():
    value = _undispatched_retirement_fixture()
    victim = value["attempts"][0]
    value["resourcesCreated"] = [item for item in value["resourcesCreated"] if item["id"] != "rejected"]
    settled = next(item for item in value["retirement"]["resources"] if item["id"] == victim["requestId"])
    settled.update(
        outcome="UNDISPATCHED",
        executionArn=victim["executionArn"],
        serviceErrorType="TooManyRequestsException",
        sourceEventId=99,
        startedAtMs=5,
        settledAtMs=6,
    )
    value["retirement"]["resources"] = [item for item in value["retirement"]["resources"] if item["id"] != "rejected"]
    assert (
        f"INCOMPLETE: undispatched invocation {victim['requestId']} lacks a consistent service rejection proof"
        in validate_retirement_evidence(value)
    )
