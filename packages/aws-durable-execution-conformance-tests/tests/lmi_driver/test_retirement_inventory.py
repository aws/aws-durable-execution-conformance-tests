# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Every service callback and case barrier requires positive retirement evidence."""

import io
import json
from copy import deepcopy
from types import SimpleNamespace
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


def _undispatched_retirement_fixture(source_event_id=99):
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
            "sourceEventId": source_event_id,
            "startedAtMs": 5,
            "settledAtMs": 6,
            "evidenceUri": value["collection"]["evidenceUris"][0],
            "outcome": "UNDISPATCHED",
        }
    )
    return value


@pytest.mark.parametrize("source_event_id", [99, 99.0])
def test_undispatched_settlement_with_a_consistent_proof_passes_independent_retirement(source_event_id):
    # An integral JSON spelling of the history EventId is the same proof.
    assert validate_retirement_evidence(_undispatched_retirement_fixture(source_event_id)) == []


@pytest.mark.parametrize(
    "mutate",
    [
        lambda resource, value: resource.update(serviceErrorType="ClientError"),
        lambda resource, value: resource.update(executionArn="arn:execution:foreign"),
        lambda resource, value: resource.pop("sourceEventId"),
        lambda resource, value: resource.update(sourceEventId=99.5),
        lambda resource, value: resource.update(sourceEventId=True),
        lambda resource, value: resource.update(startedAtMs=resource["settledAtMs"] + 1),
        lambda resource, value: resource.update(settledAtMs=value["retirement"]["retiredAtMs"] + 1000),
        lambda resource, value: resource.update(evidenceUri="s3://evidence/unretained.json"),
    ],
    ids=[
        "unlisted-error-type",
        "foreign-execution-arn",
        "missing-source-event-id",
        "non-integral-source-event-id",
        "boolean-source-event-id",
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


def _undispatched_case(tmp_path, monkeypatch, dispatch):
    """Retire, through the Cloud transport alone, a case whose only launch never reached a handler.

    ``REJECTED``: Invoke returned a definite, unretried rejection. ``PREPARED``:
    the launch failed before the transport sent it. Either way the case only held
    and deleted its own controls.
    """
    import time

    from aws_durable_execution_conformance_tests.lmi.artifacts import case_key
    from aws_durable_execution_conformance_tests.lmi.coordinator import Scenario
    from aws_durable_execution_conformance_tests.lmi.service import DurableFunctionRunner

    ctx = context()
    ctx.scenario = Scenario(
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
    if dispatch == "PREPARED":
        monkeypatch.setattr(DurableFunctionRunner, "run_async", Mock(side_effect=OSError("before transport")))
    else:
        transport.invoke.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException"}, "ResponseMetadata": {"RetryAttempts": 0}}, "Invoke"
        )
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
    with pytest.raises(CollectionError, match="invocation API failed"):
        cloud.start("replay", role="victim", resume_roles=("resume",), dispatcher_payload=ctx.dispatcher("victim"))
    cloud.settle_case()
    cloud.observation_ended_at = time.time()
    cloud.collect(full_run=False)
    cloud.delete_controls()
    cloud.retired_at = time.time()
    (journal,) = (cloud.artifacts_dir / "invocations").glob("*.json")
    return ctx, cloud, journal


@pytest.mark.parametrize("dispatch", ["REJECTED", "PREPARED"])
def test_undispatched_launch_retires_with_controls_only_evidence(tmp_path, monkeypatch, dispatch):
    from aws_durable_execution_conformance_tests.lmi.evidence import validate_evidence

    ctx, cloud, journal = _undispatched_case(tmp_path, monkeypatch, dispatch)
    intent = json.loads(journal.read_text())
    assert bool(ctx.attempts) is (dispatch == "REJECTED")

    evidence = build_evidence(ctx, cloud, phase="final")

    launch = {
        "marker": intent["payload"]["marker"],
        "functionArn": ctx.profile.resource_id,
        "durableExecutionName": intent["durableExecutionName"],
        "dispatchState": dispatch,
    }
    if dispatch == "REJECTED":
        launch.update(rejectionCode="AccessDeniedException", retryAttempts=0)
    assert evidence["retirement"]["mode"] == "controls-only"
    assert evidence["retirement"]["undispatchedLaunches"] == [launch]
    assert evidence["attempts"] == [] and "replay" not in evidence
    assert evidence["collection"]["watermarks"] == {}
    assert [resource["kind"] for resource in evidence["resourcesCreated"]] == ["control"]
    assert [(row["action"], row["state"]) for row in evidence["controlRecords"]] == [
        ("hold", "ACKNOWLEDGED"),
        ("release", "ACKNOWLEDGED"),
        ("delete", "ACKNOWLEDGED"),
    ]
    assert validate_retirement_evidence(evidence) == []
    # Physical retirement only: the case observed no behavior.
    assert validate_evidence(evidence)

    # Every possibly held control needs a resolved receipt.
    superseded = deepcopy(evidence)
    next(row for row in superseded["controlRecords"] if row["action"] == "hold")["state"] = "SUPERSEDED"
    pending = deepcopy(evidence)
    pending["controlRecords"].append({"action": "hold", "gate": "release-all", "state": "PENDING"})
    for unresolved in (superseded, pending):
        assert "INCOMPLETE: controls-only retirement contains an unresolved control receipt" in (
            validate_retirement_evidence(unresolved)
        )
    for mutation, finding in (
        ("crossed-control", "controls-only retirement references a control outside the admitted case"),
        ("unknown-delete", "controls-only retirement contains an unresolved control receipt"),
        ("different-delete-time", "control release-all settlement does not match its retained deletion receipt"),
        ("runtime-evidence", "controls-only retirement cannot assert runtime observations"),
        ("deferred", "independent retirement evidence must settle every deferred service request"),
    ):
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
        assert f"INCOMPLETE: {finding}" in validate_retirement_evidence(damaged), mutation


@pytest.mark.parametrize(
    ("dispatch", "journaled"),
    [
        # The journal shows the service returned an execution for the launch.
        ("PREPARED", {"executionArn": "arn:execution:dispatched"}),
        # The journaled rejection no longer matches the attempt the coordinator recorded.
        ("REJECTED", {"rejectionCode": "ResourceNotFoundException"}),
        # A retried Invoke is not a definite rejection: the service may have run it.
        ("REJECTED", {"retryAttempts": 1}),
    ],
)
def test_dispatched_or_unproven_launch_is_not_controls_only(tmp_path, monkeypatch, dispatch, journaled):
    ctx, cloud, journal = _undispatched_case(tmp_path, monkeypatch, dispatch)
    journal.write_text(json.dumps({**json.loads(journal.read_text()), **journaled}))

    evidence = build_evidence(ctx, cloud, phase="final")

    assert "mode" not in evidence["retirement"]
    assert "undispatchedLaunches" not in evidence["retirement"]
    assert validate_retirement_evidence(evidence)


@pytest.mark.parametrize(
    ("retry_attempts", "accepted"), [(0, True), (0.0, True), (0.5, False), (True, False), (False, False), (None, False)]
)
def test_controls_only_rejection_accepts_an_integral_retry_count(tmp_path, monkeypatch, retry_attempts, accepted):
    ctx, cloud, _ = _undispatched_case(tmp_path, monkeypatch, "REJECTED")
    evidence = build_evidence(ctx, cloud, phase="final")
    launch = evidence["retirement"]["undispatchedLaunches"][0]
    if retry_attempts is None:
        del launch["retryAttempts"]
    else:
        launch["retryAttempts"] = retry_attempts

    errors = validate_retirement_evidence(evidence)

    if accepted:
        assert errors == []
    else:
        assert "INCOMPLETE: controls-only retirement has an unresolved or mismatched launch intent" in errors
