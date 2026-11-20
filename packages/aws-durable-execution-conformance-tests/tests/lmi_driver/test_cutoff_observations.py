# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""An independently sealed deadline window can precede a service stop."""

import asyncio
import io
import time
from copy import deepcopy
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from aws_durable_execution_conformance_tests.lmi import cloud as cloud_module
from aws_durable_execution_conformance_tests.lmi import scenarios as scenarios_module
from aws_durable_execution_conformance_tests.lmi.artifacts import case_key
from aws_durable_execution_conformance_tests.lmi.cloud import Cloud
from aws_durable_execution_conformance_tests.lmi.coordinator import Profile, ResourceScheduler, Scenario, run_scenarios
from aws_durable_execution_conformance_tests.lmi.evidence import (
    validate_common_evidence,
    validate_evidence,
    validate_retirement_evidence,
)
from aws_durable_execution_conformance_tests.lmi.scenarios import ScenarioDriver
from aws_durable_execution_conformance_tests.lmi.source_evidence import build_evidence


@pytest.mark.parametrize(
    ("deny_observation_stop", "late_retry", "transient"),
    [
        (False, None, None),
        (True, None, None),
        (False, "collection", None),
        (False, "retirement", None),
        (False, "active", None),
        (False, "active-unsealed", None),
        (False, "active-no-completion", None),
        (False, "active-bad-final", None),
        (False, None, "status"),
        (False, None, "stop-response"),
    ],
)
def test_coordinator_preserves_running_cutoff_without_releasing_fault(
    deny_observation_stop, late_retry, transient, tmp_path, monkeypatch
):
    active = late_retry is not None and late_retry.startswith("active")
    invalid = active and late_retry != "active"
    clock = SimpleNamespace(now=100.0)
    clock_source = SimpleNamespace(time=lambda: clock.now, monotonic=time.monotonic)
    monkeypatch.setattr(cloud_module, "time", clock_source)
    monkeypatch.setattr(scenarios_module, "time", clock_source)

    target = "arn:aws:lambda:us-west-2:123456789012:function:lmi:$LATEST.PUBLISHED"
    profile = Profile("c1", target, 1, 1, "commit", "configuration")
    case = Scenario(
        "case",
        "lmi-lifecycle-3",
        "c1",
        "deadline-during-step",
        roles=("victim",),
        exclusive=True,
        coverage_profile="standard",
        behavior_budgets={"admissionMs": 20, "cleanupMs": 20, "observationGraceMs": 1},
    )
    actions = []
    storage = Mock()
    storage.get_object.side_effect = lambda **_: {"Body": io.BytesIO(b"hold"), "ETag": '"hold"'}
    storage.get_paginator.return_value.paginate.return_value = []

    def put_control(**kwargs):
        actions.append(kwargs["Body"])
        return {"ResponseMetadata": {"RetryAttempts": 0}}

    storage.put_object.side_effect = put_control
    storage.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
    transport = Mock()
    transport.invoke.side_effect = lambda **kwargs: {
        "StatusCode": 202,
        "DurableExecutionArn": f"{target}/durable-execution/{kwargs['DurableExecutionName']}/id",
    }
    state = {"Status": "RUNNING", "StartTimestamp": 100.0}
    history = [
        {"EventId": 1, "EventType": "ExecutionStarted", "EventTimestamp": 100.0},
        {
            "EventId": 2,
            "EventType": "InvocationCompleted",
            "EventTimestamp": 100.032,
            "InvocationCompletedDetails": {
                "RequestId": "native",
                "StartTimestamp": 100.010,
                "EndTimestamp": 100.032,
                "Error": {"Payload": {"ErrorType": "Sandbox.Timedout"}},
            },
        },
    ]

    def read_status(**_):
        if transient == "status" and transport.get_durable_execution.call_count == 1:
            raise ClientError({"Error": {"Code": "TooManyRequestsException"}}, "GetDurableExecution")
        return dict(state)

    transport.get_durable_execution.side_effect = read_status
    transport.get_durable_execution_history.side_effect = lambda **_: {"Events": list(history)}

    def stop(**_):
        actions.append("stop")
        if deny_observation_stop and actions.count("stop") == 1:
            raise ClientError({"Error": {"Code": "AccessDeniedException"}}, "StopDurableExecution")
        clock.now += 0.1
        state.update(Status="STOPPED", EndTimestamp=clock.now)
        if late_retry == "collection" or active:
            adapter.publish_retry()
        history.append({"EventId": len(history) + 1, "EventType": "ExecutionStopped", "EventTimestamp": clock.now})
        if transient == "stop-response" and actions.count("stop") == 1:
            raise EndpointConnectionError(endpoint_url="https://lambda.example.invalid")

    transport.stop_durable_execution.side_effect = stop
    manifest = {
        "run": "run",
        "region": "us-west-2",
        "bucket": "private",
        "commit": "commit",
        "conformanceRevision": "test-conformance",
        "functions": {"c1": target},
        "invocationTimeout": 0.02,
        "driverTimeout": 0.05 if transient else 0.001,
    }

    class Adapter:
        async def prepare(self, profile):
            pass

        async def finish(self, profile, *, quiescent):
            assert quiescent is not invalid

        async def start(self, context):
            self.cloud = Cloud(
                manifest,
                "c1",
                artifacts_dir=tmp_path / case_key(case.case_id),
                verify_profile=Mock(),
                case_context=context,
                client_factory=lambda service, region: storage if service == "s3" else transport,
            )
            monkeypatch.setattr(self.cloud, "history_page", transport.get_durable_execution_history)
            self.driver = ScenarioDriver(context, self.cloud, runtime="python", fixtures={case.scenario: "deadline"})
            self.driver.start()

        async def place(self, context):
            pass

        async def execute(self, context):
            item = self.driver.items["victim"]
            identity = {
                "run": "run",
                "caseId": case.case_id,
                "commit": "commit",
                "marker": item["marker"],
                "execution": item["arn"],
                "request": "native",
                "environment": "environment",
                "process": "worker",
            }
            observations: list[dict[str, Any]] = [
                {"kind": "wrapper_enter", "atMs": 100010},
                {"kind": "task_start", "taskId": "root", "atMs": 100011},
                {"kind": "work_in_flight", "atMs": 100030},
                {"kind": "admission_closed", "atMs": 100033},
                {"kind": "admission_probe", "probe": "operation", "outcome": "REJECTED", "atMs": 100034},
                {"kind": "admission_probe", "probe": "checkpoint", "outcome": "REJECTED", "atMs": 100035},
                {"kind": "task_exit", "taskId": "root", "atMs": 100040},
                {"kind": "wrapper_raise", "outcome": "FAILED", "atMs": 100042},
            ]
            raw = [
                {"phase": "WRAPPER_ENTER", "time": 100.010},
                *(
                    {
                        "phase": "CONFORMANCE",
                        "time": observation["atMs"] / 1000,
                        "observation": {**observation, "captureSequence": capture},
                    }
                    for capture, observation in enumerate(observations, 1)
                ),
                {"phase": "WRAPPER_RAISE", "time": 100.042},
                {"phase": "SEALED", "time": 100.043, "sourceProtocol": 2, "throughCapture": len(observations)},
            ]
            self.cloud.events = {
                str(sequence): {**identity, **event, "sequence": sequence} for sequence, event in enumerate(raw, 1)
            }
            clock.now = 100.2

        def publish_retry(self):
            owner = dict(self.cloud.events["1"], request="late-retry")
            entered = 100190 if active else 100220
            observations: list[dict[str, Any]] = [
                {"kind": "wrapper_enter", "atMs": entered},
                {"kind": "task_start", "taskId": "root", "atMs": entered + 1},
                {"kind": "checkpoint_call", "callId": "retry-call", "atMs": entered + 2},
                {"kind": "checkpoint_exit", "callId": "retry-call", "atMs": 100239},
                {"kind": "task_exit", "taskId": "root", "atMs": 100240},
                {"kind": "wrapper_return", "outcome": "SUCCEEDED", "atMs": 100241},
            ]
            raw = [
                {"phase": "WRAPPER_ENTER", "time": entered / 1000},
                *(
                    {
                        "phase": "CONFORMANCE",
                        "time": event["atMs"] / 1000,
                        "observation": {**event, "captureSequence": capture},
                    }
                    for capture, event in enumerate(observations, 1)
                ),
                {"phase": "WRAPPER_RETURN", "time": 100.242},
                {"phase": "SEALED", "time": 100.243, "sourceProtocol": 2, "throughCapture": len(observations)},
            ]
            if late_retry == "active-unsealed":
                raw.pop()
            self.cloud.events.update(
                {f"retry/{index}": {**owner, **event, "sequence": index} for index, event in enumerate(raw, 1)}
            )
            if late_retry == "active-no-completion":
                return
            history.append(
                {
                    "EventId": len(history) + 1,
                    "EventType": "InvocationCompleted",
                    "EventTimestamp": 100.25,
                    "InvocationCompletedDetails": {
                        "RequestId": "late-retry",
                        "StartTimestamp": (entered - 10) / 1000,
                        "EndTimestamp": 100.25,
                    },
                }
            )

        def evidence(self, context, phase):
            return build_evidence(
                context,
                self.cloud,
                runtime="python",
                phase=phase,
                worker_identity=lambda event: event["process"],
                expected_outcome=lambda *_: {"allowedStatuses": ["RUNNING", "STOPPED"]},
            )

        async def collect(self, context):
            self.driver.collect()
            assert b"release" not in actions
            return self.evidence(context, "observations")

        async def release(self, context):
            self.cloud.release_all()

        async def retire(self, context):
            if late_retry == "retirement":
                self.publish_retry()
            assert self.driver.retire()
            context.retirement_evidence = self.evidence(context, "final")
            if late_retry == "active-bad-final":
                retry = next(row for row in context.retirement_evidence["attempts"] if row["requestId"] == "late-retry")
                retry["endedAtMs"] = 100200
            retirement = context.retirement_evidence["retirement"]
            owned = {resource["id"] for resource in context.evidence["resourcesCreated"]}
            context.evidence["retirement"] = {
                **retirement,
                "resources": [item for item in retirement["resources"] if item["id"] in owned],
            }
            return True

    def validate(evidence, *, phase="final"):
        return validate_evidence(evidence, phase="observations" if phase == "observations" else "final")

    scheduler = ResourceScheduler()
    adapter = Adapter()
    (result,) = asyncio.run(
        run_scenarios([case], [profile], adapter, run_id="run", validator=validate, scheduler=scheduler)
    )
    if invalid:
        assert result.status != "PASSED", result.errors
        assert scheduler.quarantine_reason(target), result.errors
        if late_retry == "active-bad-final":
            assert any("actual service completion" in error for error in result.errors)
        else:
            assert all("activeAtObservationEnd" not in row for row in result.evidence["attempts"])
        return
    assert result.status == ("FAILED" if deny_observation_stop else "PASSED"), result.errors
    assert result.evidence["observationEndedAtMs"] == 100200
    assert result.evidence["histories"][0]["status"] == "RUNNING"
    assert result.retirement_evidence["histories"][0]["status"] == "STOPPED"
    assert actions.index("stop") < actions.index(b"release")
    assert actions.count("stop") == (2 if deny_observation_stop else 1)
    assert scheduler.quarantine_reason(target) is None, result.errors
    if late_retry is not None:
        frozen = {attempt["requestId"]: attempt for attempt in result.evidence["attempts"]}
        final = {attempt["requestId"]: attempt for attempt in result.retirement_evidence["attempts"]}
        assert set(final) == {"native", "late-retry"} and final["late-retry"]["endedAtMs"] == 100250
        assert "activeAtObservationEnd" not in final["late-retry"]
        journaled_retry = next(attempt for attempt in result.attempts if attempt["requestId"] == "late-retry")
        assert journaled_retry["nativeEndedAtMs"] == 100250
        if late_retry == "active":
            assert frozen["late-retry"]["activeAtObservationEnd"] is True
            assert frozen["late-retry"]["startedAtMs"] == journaled_retry["nativeStartedAtMs"] == 100180
            assert not ({"endedAtMs", "outcome"} & frozen["late-retry"].keys())
            assert [request["callId"] for request in result.evidence["sdkRequests"]] == ["retry-call"]
            offline = deepcopy(result.evidence)
            offline["retirementSnapshot"] = result.retirement_evidence
            assert validate_evidence(offline) == []
            alternate = deepcopy(offline)
            alternate["retirementSnapshot"]["artifact"]["diagnostic"] = "more metadata"
            for source in (alternate, alternate["retirementSnapshot"]):
                for row in source["events"]:
                    if row["requestId"] == "late-retry":
                        row["sequence"] = float(row["sequence"])
            assert validate_evidence(alternate) == []
            for key, invalid in (("runtime", "wrong-runtime"), ("coverageProfile", "wrong-coverage")):
                wrong_scope = deepcopy(offline)
                wrong_scope["retirementSnapshot"][key] = invalid
                assert any("different scope" in error for error in validate_evidence(wrong_scope))
            false_receipt = deepcopy(offline)
            arn = frozen["native"]["executionArn"]
            receipt = next(row for row in false_receipt["retirement"]["resources"] if row["id"] == arn)
            receipt["outcome"] = "SUCCEEDED"
            assert any("contradicts the audited settlement" in error for error in validate_evidence(false_receipt))
            contradicted = deepcopy(offline)
            victim = next(row for row in contradicted["retirementSnapshot"]["attempts"] if row["requestId"] == "native")
            victim["outcome"] = "SUCCEEDED"
            assert any("changed an original invocation's outcome" in error for error in validate_evidence(contradicted))
            late = deepcopy(offline)
            snapshot = late["retirementSnapshot"]
            scope = {
                "requestId": "late-retry",
                "ownerRequestId": "late-retry",
                "executionArn": frozen["late-retry"]["executionArn"],
                "callId": "late-dispatch",
            }
            sequence = max(event["sequence"] for event in snapshot["events"] if event["requestId"] == "late-retry")
            snapshot["events"].extend(
                [
                    {**scope, "kind": "poll_call", "atMs": 100246, "sequence": sequence + 1},
                    {**scope, "kind": "poll_exit", "atMs": 100247, "sequence": sequence + 2},
                ]
            )
            snapshot["sdkRequests"].append(
                {
                    **scope,
                    "method": "GetDurableExecutionState",
                    "outcome": "DISPATCHED",
                    "atMs": 100246,
                }
            )
            assert validate_retirement_evidence(snapshot) == []
            assert any("dispatched SDK work after admission closed" in error for error in validate_evidence(late))
            offline["sdkRequests"] = []
            offline["events"] = [event for event in offline["events"] if event.get("callId") != "retry-call"]
            assert any("pre-cutoff SDK requests" in error for error in validate_evidence(offline))
        else:
            assert set(frozen) == {"native"}
            assert journaled_retry["nativeStartedAtMs"] == 100210
            deferred = result.evidence["collection"]["deferredServiceRequests"]
            assert deferred == [{"requestId": "late-retry", "executionArn": journaled_retry["executionArn"]}]
            assert any("independent retirement audit" in error for error in validate_evidence(result.evidence))
            offline = deepcopy(result.evidence)
            offline["retirementSnapshot"] = {}
            assert validate_common_evidence(offline)
            offline["retirementSnapshot"] = deepcopy(result.retirement_evidence)
            assert validate_evidence(offline) == []
            assert validate_common_evidence(offline) == []
            concealed = deepcopy(offline)
            concealed["collection"].pop("deferredServiceRequests")
            assert any(
                "must match the frozen and deferred inventories" in error for error in validate_evidence(concealed)
            )
            retry = next(row for row in offline["retirementSnapshot"]["attempts"] if row["requestId"] == "late-retry")
            retry["startedAtMs"] = 100199
            assert any("started after the cutoff" in error for error in validate_evidence(offline))
            assert any("started after the cutoff" in error for error in validate_common_evidence(offline))
        assert result.evidence["selectedRequests"] == {"victim": "native"}
