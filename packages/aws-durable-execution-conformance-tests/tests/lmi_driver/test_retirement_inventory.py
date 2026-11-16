# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Every service callback and case barrier requires positive retirement evidence."""

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
    records = [
        {"gate": "release-all", "action": "hold", "time": 0.1},
        {"gate": "release-all", "action": "release", "time": 20, "state": "ACKNOWLEDGED"},
    ]
    if control_deleted:
        records.append({"gate": "release-all", "action": "delete", "time": 20.5, "state": "ACKNOWLEDGED"})
    cloud = SimpleNamespace(
        manifest={"bucket": "private", "invocationTimeout": 60},
        observation_ended_at=20,
        retired_at=21,
        source_uris={"memory://service"},
        raw_histories={arn: raw},
        execution_results={arn: {"Status": "SUCCEEDED"}},
        control_records=records,
        control_prefix="runs/run/cases/case/control/",
        invocations=[{"arn": arn, "marker": "victim"}],
        artifacts_dir=tmp_path,
        _save=lambda *args: None,
    )
    sources = SimpleNamespace(
        observations=lambda: (deepcopy(baseline["events"]), []),
        histories=lambda _: (baseline["histories"], {}, []),
        attempts=lambda _: (baseline["attempts"], baseline["selectedRequests"], []),
    )
    with patch("aws_durable_execution_conformance_tests.lmi.source_evidence.CaseSources.capture", return_value=sources):
        return build_evidence(ctx, cloud, phase="final")


@pytest.mark.parametrize("callback_outcome", ["SUCCEEDED", "FAILED", "TIMED_OUT"])
def test_callbacks_and_controls_are_inventoried_with_independent_receipts(tmp_path, callback_outcome):
    evidence = normalized(tmp_path, callback_outcome=callback_outcome)
    assert {item["kind"] for item in evidence["resourcesCreated"]} == {"invocation", "execution", "callback", "control"}
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


def test_retirement_deletes_controls_only_after_runtime_quiescence():
    from aws_durable_execution_conformance_tests.lmi.scenarios import ScenarioDriver

    actions = []
    cloud = SimpleNamespace(
        invocations=[],
        manifest={},
        refresh=lambda **kwargs: actions.append("refresh"),
        settle_case=lambda: actions.append("release-and-settle"),
        poll=lambda predicate, **kwargs: actions.append("quiescent"),
        collect=lambda **kwargs: actions.append("collect"),
        delete_controls=lambda: actions.append("delete"),
    )
    driver = object.__new__(ScenarioDriver)
    driver.cloud = cast(Any, cloud)
    driver.retire()
    assert actions == ["refresh", "release-and-settle", "quiescent", "collect", "delete"]
    assert cloud.retired_at is not None
