# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Ownership, native-request journaling and cancellation of the shared driver."""

import asyncio
import contextlib
import io
import json
import time
from dataclasses import replace
from functools import partial
from pathlib import Path
from runpy import run_path
from threading import Event
from types import SimpleNamespace
from typing import Any, ClassVar
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError, CredentialRetrievalError, NoCredentialsError, ParamValidationError

from aws_durable_execution_conformance_tests.lmi import adapter as adapter_module
from aws_durable_execution_conformance_tests.lmi import cloud as cloud_module
from aws_durable_execution_conformance_tests.lmi.coordinator import (
    Profile,
    ResourceScheduler,
    Scenario,
    ScenarioContext,
)
from aws_durable_execution_conformance_tests.lmi.errors import (
    CollectionBudgetExpired,
    CollectionError,
    ProvisioningError,
)

from .support import FIXTURES, RUNTIME, ScenarioDriver, build_evidence, configuration_identity

run_and_adjudicate = run_path(str(Path(__file__).parents[1] / "test_lmi_coordinator.py"))["run_and_adjudicate"]

FUNCTION = "arn:aws:lambda:us-west-2:123456789012:function:lmi:$LATEST.PUBLISHED"
EXECUTION = FUNCTION + "/durable-execution/name/id"
PROFILE = Profile("native-1", FUNCTION, 1, 1, "sdk-sha", "configuration-sha")


def context(
    case_id="case",
    scenario="suspension-retry",
    roles=("victim", "resume"),
    *,
    native=1,
):
    profile = Profile(f"native-{native}", FUNCTION, native, native, "sdk-sha", "configuration-sha")
    return ScenarioContext(
        "run",
        Scenario(
            case_id,
            "lmi-lifecycle-5",
            profile.profile_id,
            scenario,
            roles=roles,
            coverage_profile="standard",
        ),
        profile,
    )


def deployment():
    return {
        **RUNTIME.to_manifest(),
        "functions": {"c1": FUNCTION, "c2": FUNCTION + "-c2"},
        "concurrencies": {"c1": 1, "c2": 2},
        "region": "us-west-2",
        "run": "run",
        "bucket": "private-evidence",
        "commit": "sdk-sha",
        "runtime": "python3.14",
        "role": "role",
        "provider": "provider",
        "codeSha256": "code",
        "invocationTimeout": 60,
        "executionTimeout": 240,
        "driverTimeout": 1,
    }


@pytest.fixture
def cloud(monkeypatch, tmp_path):
    clients = {name: Mock() for name in ("lambda", "s3", "logs")}
    clients["lambda"].invoke.side_effect = lambda **kwargs: {
        "StatusCode": 202,
        "DurableExecutionArn": f"{kwargs['FunctionName']}/durable-execution/{kwargs['DurableExecutionName']}/id",
    }
    clients["s3"].put_object.return_value = {"ResponseMetadata": {"RetryAttempts": 0}}
    clients["s3"].get_paginator.return_value.paginate.return_value = []
    return cloud_module.Cloud(
        deployment(),
        "c1",
        artifacts_dir=tmp_path / "case",
        case_context=context(),
        verify_profile=Mock(),
        client_factory=lambda service, region: clients[service],
    )


def enter(item, request, at):
    return {
        "phase": "WRAPPER_ENTER",
        "marker": item["marker"],
        "request": request,
        "execution": item.get("arn") or EXECUTION,
        "time": at,
        "sequence": 0,
        "run": "run",
        "commit": "sdk-sha",
        "runtime": "python3.14",
        "role": "role",
        "provider": "provider",
        "codeSha256": "code",
        "invocationTimeout": 60,
        "executionTimeout": 240,
    }


def refresh_entries(cloud, *events):
    cloud.events.update({f"events/{event['request']}": event for event in events})
    return cloud.refresh()


def test_direct_launch_is_journaled_before_public_runner_transport(cloud):
    observed = []

    def invoke(**kwargs):
        observed.append(kwargs)
        assert len(cloud.case_context.attempts) == 1
        assert "requestId" not in cloud.case_context.attempts[0]
        return {
            "StatusCode": 202,
            "DurableExecutionArn": f"{kwargs['FunctionName']}/durable-execution/{kwargs['DurableExecutionName']}/id",
        }

    cloud.lam.invoke.side_effect = invoke
    item = cloud.start(
        "replay",
        role="victim",
        resume_roles=("resume",),
        dispatcher_payload=cloud.case_context.dispatcher("victim"),
    )
    payload = json.loads(observed[0]["Payload"])
    assert observed[0]["FunctionName"] == FUNCTION
    assert payload["scenario"] == "replay"
    assert payload["sharedScenario"] == "suspension-retry"
    assert payload["caseId"] == "case"
    assert payload["controlPrefix"] == cloud.control_prefix
    assert "/cases/" in cloud.control_prefix
    assert item["native_requests"] == []
    refresh_entries(cloud, enter(item, "original", 1))
    assert cloud.case_context.attempts[0]["requestId"] == "original"


@pytest.mark.parametrize("coverage_profile", ["standard", "sdk-regression"])
@pytest.mark.parametrize("payload_profile", ["admitted", "substituted", "absent"])
def test_direct_launch_uses_the_admitted_coverage_profile(cloud, coverage_profile, payload_profile):
    cloud.case_context.scenario = replace(
        cloud.case_context.scenario,
        coverage_profile=coverage_profile,
        behavior_budgets={"settleMs": 10},
    )
    payload = cloud.case_context.dispatcher("victim")
    if payload_profile == "substituted":
        payload["coverageProfile"] = "another-profile"
    elif payload_profile == "absent":
        payload.pop("coverageProfile", None)
    item = cloud.start(
        "replay",
        role="victim",
        resume_roles=("resume",),
        dispatcher_payload=payload,
    )
    invoked = json.loads(cloud.lam.invoke.call_args.kwargs["Payload"])
    retained = json.loads((cloud.artifacts_dir / f"invocations/{item['marker']}.json").read_text())
    for observed in (invoked, item["payload"], retained["payload"]):
        assert observed["coverageProfile"] == coverage_profile
        assert observed["behaviorBudgets"] == {"settleMs": 10}
    assert cloud.case_context.scenario.coverage_profile == coverage_profile


def test_service_replays_are_journaled_without_another_invoke(cloud):
    item = cloud.start("replay", role="victim", resume_roles=("resume",))
    refresh_entries(
        cloud,
        enter(item, "first", 1),
        enter(item, "second", 2),
        enter(item, "third", 3),
    )
    assert cloud.lam.invoke.call_count == 1
    assert [(attempt["role"], attempt["attempt"], attempt["requestId"]) for attempt in cloud.case_context.attempts] == [
        ("victim", 1, "first"),
        ("resume", 1, "second"),
        ("resume", 2, "third"),
    ]
    assert cloud.request_roles == {
        "first": "victim",
        "second": "resume",
        "third": "resume",
    }
    cloud.refresh()
    assert len(cloud.case_context.attempts) == 3


def test_lost_invoke_response_retains_launch_and_recovers_identity(cloud):
    cloud.lam.invoke.side_effect = TimeoutError("response lost")
    with pytest.raises(CollectionError, match="API failed"):
        cloud.start("success", role="victim")
    item = cloud.invocations[0]
    assert item["arn"] is None
    record = json.loads((cloud.artifacts_dir / f"invocations/{item['marker']}.json").read_text())
    assert "response lost" in record["error"]
    event = enter(item, "original", 1)
    with pytest.raises(CollectionError, match="journaled launch"):
        refresh_entries(cloud, event)
    assert item["arn"] is None
    arn = FUNCTION + f"/durable-execution/{record['durableExecutionName']}/id"
    refresh_entries(cloud, {**event, "execution": arn})
    assert item["arn"] == arn
    assert cloud.case_context.attempts[0]["requestId"] == "original"
    assert cloud.lam.invoke.call_count == 1


def test_replacement_execution_cannot_rebind_a_launched_marker(cloud):
    item = cloud.start("success", role="victim")
    event = enter(item, "replacement", 1)
    event["execution"] = EXECUTION + "-other"
    with pytest.raises(CollectionError, match="original durable execution"):
        refresh_entries(cloud, event)


def test_case_controls_and_collection_cannot_touch_another_case(cloud, tmp_path):
    cloud.hold("own-gate")
    with pytest.raises(CollectionError, match="not owned"):
        cloud.release("another-gate")
    with pytest.raises(CollectionError, match="another case"):
        cloud.refresh(markers=["another-marker"])
    with pytest.raises(CollectionError, match="another case"):
        cloud.refresh(markers=[])
    with pytest.raises(CollectionError, match="only its own"):
        cloud.collect(full_run=True)
    cloud.s3.get_object.side_effect = lambda **_: {"Body": io.BytesIO(b"hold"), "ETag": '"hold"'}
    cloud.release_all()
    keys = [call.kwargs["Key"] for call in cloud.s3.put_object.call_args_list]
    assert all(key.startswith(cloud.control_prefix) for key in keys)
    assert not (tmp_path / "events.json").exists()


def test_unlaunched_case_does_not_scan_shared_event_prefix(cloud):
    assert cloud.refresh() == []
    cloud.s3.get_paginator.assert_not_called()


def test_cancellation_stops_waits_and_new_transports(cloud):
    cloud.cancel()
    with pytest.raises(CollectionError, match="cancelled"):
        cloud.wait(30)
    with pytest.raises(CollectionError, match="cancelled"):
        cloud.start("success", role="victim")
    cloud.lam.invoke.assert_not_called()
    cloud.begin_retirement()
    with pytest.raises(CollectionError, match="retiring"):
        cloud.start("success", role="victim")


class FakeCloud:
    instances: ClassVar[list] = []

    def __init__(self, manifest, configuration, *, artifacts_dir, case_context=None):
        self.manifest, self.configuration = manifest, configuration
        self.artifacts_dir, self.case_context = Path(artifacts_dir), case_context
        self.cancel_event = Event()
        self.actions = []
        self.invocations = []
        self.control_records = []
        self.s3: Any = Mock()
        self.s3.put_object.return_value = {"ResponseMetadata": {"RetryAttempts": 0}}
        self.s3.get_object.side_effect = ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        self.instances.append(self)

    def verify(self):
        self.actions.append("verify")

    def check_active(self):
        if self.cancel_event.is_set():
            raise CollectionError("cancelled")

    def cancel(self):
        self.cancel_event.set()

    def begin_retirement(self):
        self.actions.append("begin-retirement")
        self.cancel_event.clear()

    def release_all(self):
        self.actions.append("release")


def adapter(tmp_path, *, factory=FakeCloud):
    return adapter_module.SharedAdapter(
        {"runtime": "python", "sdk": {"deployment": deployment(), "artifacts_dir": str(tmp_path)}},
        evidence_builder=lambda context, cloud, *, phase: {
            "retirement": {"owner": context.scenario.case_id, "phase": phase}
        },
        cloud_factory=factory,
        driver_factory=partial(adapter_module.ScenarioDriver, runtime="python", fixtures=FIXTURES),
    )


def test_prepare_checks_the_actual_deployment_profile(tmp_path):
    value = adapter(tmp_path)
    from .support import configuration_identity

    valid = Profile(
        PROFILE.profile_id,
        PROFILE.resource_id,
        1,
        1,
        "sdk-sha",
        configuration_identity(deployment(), "c1"),
    )
    asyncio.run(value.prepare(valid))
    with pytest.raises(ProvisioningError, match="configuration identity"):
        asyncio.run(value.prepare(PROFILE))
    assert FakeCloud.instances[-1].actions == ["verify"]
    bad = Profile("bad", FUNCTION, 1, 2, "sdk-sha", "configuration-sha")
    with pytest.raises(ProvisioningError, match="Native concurrency"):
        asyncio.run(value.prepare(bad))


def test_finish_releases_fence_acquired_after_cancelled_preparation(tmp_path, monkeypatch):
    from aws_durable_execution_conformance_tests.lmi import quarantine

    entered, unblock = Event(), Event()
    actions: list[str] = []

    def acquire(*args, **kwargs):
        entered.set()
        assert unblock.wait(2)
        actions.append("acquire")
        return "owned-token"

    def release(storage, manifest, resource, token):
        assert token == "owned-token"
        (receipt,) = tmp_path.rglob("*-fence.json")
        assert json.loads(receipt.read_text())["state"] == "RETIRED"
        actions.append("release")
        raise ClientError({"Error": {"Code": "ExpiredToken"}}, "DeleteObject")

    monkeypatch.setattr(quarantine, "acquire_fence", acquire)
    monkeypatch.setattr(quarantine, "release_fence", release)

    async def exercise():
        value = adapter(tmp_path)
        valid = replace(PROFILE, configuration_id=configuration_identity(deployment(), "c1"))
        preparing = asyncio.create_task(value.prepare(valid))
        try:
            assert await asyncio.to_thread(entered.wait, 1)
            preparing.cancel()
            with pytest.raises(asyncio.CancelledError):
                await preparing
            finishing = asyncio.create_task(value.finish(valid, quiescent=True))
            await asyncio.sleep(0)
            assert not finishing.done()
        finally:
            unblock.set()
        with pytest.raises(ClientError, match="ExpiredToken"):
            await finishing
        (receipt,) = tmp_path.rglob("*-fence.json")
        assert json.loads(receipt.read_text())["token"] == "owned-token"
        assert actions == ["acquire", "release"]

    asyncio.run(exercise())


def test_profile_verification_holds_fence_and_releases_its_token_on_failure(tmp_path, monkeypatch):
    from aws_durable_execution_conformance_tests.lmi import quarantine

    events: list[str] = []

    def acquire(storage, manifest, resource, **kwargs):
        events.append("acquire")
        return "owned-token"

    def release(storage, manifest, resource, token):
        assert token == "owned-token"
        (receipt,) = tmp_path.rglob("*-fence.json")
        assert json.loads(receipt.read_text())["state"] == "NOT_ADMITTED"
        events.append("release")
        raise ClientError({"Error": {"Code": "ExpiredToken"}}, "DeleteObject")

    class InvalidCloud(FakeCloud):
        def verify(self):
            assert events == ["acquire"]
            events.append("verify")
            raise ProvisioningError("profile drifted")

    monkeypatch.setattr(quarantine, "acquire_fence", acquire)
    monkeypatch.setattr(quarantine, "release_fence", release)

    async def exercise():
        value = adapter(tmp_path, factory=InvalidCloud)
        valid = replace(PROFILE, configuration_id=configuration_identity(deployment(), "c1"))
        with pytest.raises(ProvisioningError, match="profile drifted"):
            await value.prepare(valid)
        await value.finish(valid, quiescent=True)

    asyncio.run(exercise())
    (receipt,) = tmp_path.rglob("*-fence.json")
    assert json.loads(receipt.read_text())["token"] == "owned-token"
    assert events == ["acquire", "verify", "release"]


def test_missing_fixture_does_not_create_retirement_evidence(tmp_path):
    async def exercise():
        value = adapter(tmp_path)
        value.driver_factory = partial(adapter_module.ScenarioDriver, runtime="python", fixtures={})
        ctx = context()
        with pytest.raises(NotImplementedError, match="fixture"):
            await value.start(ctx)
        await value.release(ctx)
        assert await value.retire(ctx)
        assert not ctx.evidence
        assert not ctx.retirement_evidence

    asyncio.run(exercise())


def test_no_work_start_failure_does_not_quarantine_a_shared_target(tmp_path, monkeypatch):
    """A proven-empty `start` failure must not fence the target for later cases.

    The coordinator still calls `collect_partial` for a case whose `start`
    raised before doing any work. Once `start` has deleted that empty case,
    `collect_partial` must not try to look it up again (which would raise and
    turn into a cleanup error, releasing the per-target lease non-quiescent
    and keeping the persistent fence even though nothing is actually wrong).
    """
    from aws_durable_execution_conformance_tests.lmi import quarantine, source_evidence

    release = Mock()
    monkeypatch.setattr(quarantine, "acquire_fence", lambda *args, **kwargs: "token")
    monkeypatch.setattr(quarantine, "release_fence", release)

    class ObservedCloud(FakeCloud):
        observation_ended_at: float
        retired_at: float | None = None
        source_uris: ClassVar[set[str]] = {"memory://source"}
        execution_results: ClassVar[dict[str, dict[str, str]]] = {EXECUTION: {"Status": "SUCCEEDED"}}
        terminal_histories: ClassVar[set[str]] = {EXECUTION}

        def _save(self, name, evidence):
            pass

    started = time.time() * 1000 - 100

    def snapshot_sources(ctx, cloud):
        owned = {"requestId": "request", "ownerRequestId": "request", "executionArn": EXECUTION}
        events = [
            {**owned, "kind": "wrapper_enter", "atMs": started},
            {**owned, "kind": "task_start", "taskId": "root", "atMs": started + 1},
            {**owned, "kind": "wrapper_return", "outcome": "SUCCEEDED", "atMs": started + 2},
            {**owned, "kind": "task_exit", "taskId": "root", "atMs": started + 3},
        ]
        raw_events = [
            {"phase": "WRAPPER_ENTER", "request": "request", "sequence": 1, "time": started / 1000},
            *(
                {
                    "phase": "CONFORMANCE",
                    "request": "request",
                    "sequence": capture + 1,
                    "observation": {"captureSequence": capture},
                }
                for capture, _ in enumerate(events, 1)
            ),
            {
                "phase": "SEALED",
                "request": "request",
                "sequence": len(events) + 2,
                "sourceProtocol": 2,
                "throughCapture": len(events),
            },
        ]
        for event in raw_events:
            event.update(marker="marker", execution=EXECUTION)
        cloud.events = dict(enumerate(raw_events, 1))
        cloud.raw_histories = {
            EXECUTION: [
                {"EventType": "ExecutionStarted", "EventId": 1},
                {"EventType": "ExecutionSucceeded", "EventId": 2, "EventTimestamp": started / 1000},
                {
                    "EventType": "InvocationCompleted",
                    "EventId": 3,
                    "InvocationCompletedDetails": {
                        "RequestId": "request",
                        "StartTimestamp": started / 1000,
                        "EndTimestamp": (started + 3) / 1000,
                    },
                },
            ]
        }
        attempt = {
            **owned,
            "environmentId": "environment",
            "workerId": "worker",
            "processId": "process",
            "role": "victim",
            "attempt": 1,
            "startedAtMs": started,
            "endedAtMs": started + 3,
            "outcome": "SUCCEEDED",
        }
        history = {
            "executionArn": EXECUTION,
            "ownerCaseId": ctx.scenario.case_id,
            "status": "SUCCEEDED",
            "operations": [],
        }
        return SimpleNamespace(
            observations=lambda **kwargs: (events, []),
            histories=lambda **kwargs: ([history], {}, []),
            attempts=lambda events, **kwargs: ([attempt], {"victim": "request"}, []),
        )

    class Driver:
        def __init__(self, case_context, cloud):
            self.context = case_context
            self.cloud = cloud

        def start(self):
            if self.context.scenario.case_id == "no-work":
                raise NotImplementedError("Missing SDK fixture: never dispatched")
            self.context.record_attempt("launch", requestId="request", executionArn=EXECUTION, role="victim")
            self.cloud.invocations.append({"arn": EXECUTION, "marker": "marker", "native_requests": ["request"]})

        def place(self):
            pass

        def execute(self):
            pass

        def collect(self):
            self.cloud.observation_ended_at = time.time()
            self.cloud.normalized_sources = snapshot_sources(self.context, self.cloud)

        def retire(self):
            self.cloud.observation_ended_at = time.time()
            self.cloud.retired_at = time.time()
            self.cloud.normalized_sources = snapshot_sources(self.context, self.cloud)
            return True

    monkeypatch.setattr(
        source_evidence.CaseSources,
        "capture",
        lambda ctx, cloud, **options: cloud.normalized_sources,
    )

    profile = replace(PROFILE, configuration_id=configuration_identity(deployment(), "c1"))
    no_work_case = Scenario(
        "no-work",
        "fixture-check",
        profile.profile_id,
        "completed-cohort",
        roles=("victim",),
        coverage_profile="fixture",
    )
    normal_case = Scenario(
        "normal", "fixture-check", profile.profile_id, "completed-cohort", roles=("victim",), coverage_profile="fixture"
    )
    value = adapter(tmp_path, factory=ObservedCloud)
    value.evidence_builder = build_evidence
    value.driver_factory = Driver  # type: ignore[assignment]
    scheduler = ResourceScheduler()

    results = asyncio.run(
        run_and_adjudicate([no_work_case, normal_case], [profile], value, run_id="run", scheduler=scheduler)
    )
    no_work_result = next(result for result in results if result.case_id == "no-work")
    normal_result = next(result for result in results if result.case_id == "normal")
    assert no_work_result.status == "NOT_IMPLEMENTED"
    assert normal_result.status not in ("INFRASTRUCTURE_ERROR", "NOT_IMPLEMENTED")
    assert scheduler.quarantine_reason(FUNCTION) is None, results
    release.assert_called_once()


def test_driver_factory_failure_before_registration_does_not_quarantine_a_shared_target(tmp_path, monkeypatch):
    """A `driver_factory` (or `cloud_factory`) failure before the case is even
    registered must not fence the target either: no Cloud or driver call ran,
    so this case did no work, exactly like a no-work `start()` failure.
    """
    from aws_durable_execution_conformance_tests.lmi import quarantine, source_evidence

    release = Mock()
    monkeypatch.setattr(quarantine, "acquire_fence", lambda *args, **kwargs: "token")
    monkeypatch.setattr(quarantine, "release_fence", release)

    class ObservedCloud(FakeCloud):
        observation_ended_at: float
        retired_at: float | None = None
        source_uris: ClassVar[set[str]] = {"memory://source"}
        execution_results: ClassVar[dict[str, dict[str, str]]] = {EXECUTION: {"Status": "SUCCEEDED"}}
        terminal_histories: ClassVar[set[str]] = {EXECUTION}

        def _save(self, name, evidence):
            pass

    started = time.time() * 1000 - 100

    def snapshot_sources(ctx, cloud):
        owned = {"requestId": "request", "ownerRequestId": "request", "executionArn": EXECUTION}
        events = [
            {**owned, "kind": "wrapper_enter", "atMs": started},
            {**owned, "kind": "task_start", "taskId": "root", "atMs": started + 1},
            {**owned, "kind": "wrapper_return", "outcome": "SUCCEEDED", "atMs": started + 2},
            {**owned, "kind": "task_exit", "taskId": "root", "atMs": started + 3},
        ]
        raw_events = [
            {"phase": "WRAPPER_ENTER", "request": "request", "sequence": 1, "time": started / 1000},
            *(
                {
                    "phase": "CONFORMANCE",
                    "request": "request",
                    "sequence": capture + 1,
                    "observation": {"captureSequence": capture},
                }
                for capture, _ in enumerate(events, 1)
            ),
            {
                "phase": "SEALED",
                "request": "request",
                "sequence": len(events) + 2,
                "sourceProtocol": 2,
                "throughCapture": len(events),
            },
        ]
        for event in raw_events:
            event.update(marker="marker", execution=EXECUTION)
        cloud.events = dict(enumerate(raw_events, 1))
        cloud.raw_histories = {
            EXECUTION: [
                {"EventType": "ExecutionStarted", "EventId": 1},
                {"EventType": "ExecutionSucceeded", "EventId": 2, "EventTimestamp": started / 1000},
                {
                    "EventType": "InvocationCompleted",
                    "EventId": 3,
                    "InvocationCompletedDetails": {
                        "RequestId": "request",
                        "StartTimestamp": started / 1000,
                        "EndTimestamp": (started + 3) / 1000,
                    },
                },
            ]
        }
        attempt = {
            **owned,
            "environmentId": "environment",
            "workerId": "worker",
            "processId": "process",
            "role": "victim",
            "attempt": 1,
            "startedAtMs": started,
            "endedAtMs": started + 3,
            "outcome": "SUCCEEDED",
        }
        history = {
            "executionArn": EXECUTION,
            "ownerCaseId": ctx.scenario.case_id,
            "status": "SUCCEEDED",
            "operations": [],
        }
        return SimpleNamespace(
            observations=lambda **kwargs: (events, []),
            histories=lambda **kwargs: ([history], {}, []),
            attempts=lambda events, **kwargs: ([attempt], {"victim": "request"}, []),
        )

    class Driver:
        def __init__(self, case_context, cloud):
            self.context = case_context
            self.cloud = cloud

        def start(self):
            self.context.record_attempt("launch", requestId="request", executionArn=EXECUTION, role="victim")
            self.cloud.invocations.append({"arn": EXECUTION, "marker": "marker", "native_requests": ["request"]})

        def place(self):
            pass

        def execute(self):
            pass

        def collect(self):
            self.cloud.observation_ended_at = time.time()
            self.cloud.normalized_sources = snapshot_sources(self.context, self.cloud)

        def retire(self):
            self.cloud.observation_ended_at = time.time()
            self.cloud.retired_at = time.time()
            self.cloud.normalized_sources = snapshot_sources(self.context, self.cloud)
            return True

    def driver_factory(context, cloud):
        if context.scenario.case_id == "no-work":
            # Neither Cloud nor the driver ran; this is the earliest a case
            # can fail, before `start()` even gets a chance to run.
            raise ValueError("driver construction failed")
        return Driver(context, cloud)

    monkeypatch.setattr(
        source_evidence.CaseSources,
        "capture",
        lambda ctx, cloud, **options: cloud.normalized_sources,
    )

    profile = replace(PROFILE, configuration_id=configuration_identity(deployment(), "c1"))
    no_work_case = Scenario(
        "no-work",
        "fixture-check",
        profile.profile_id,
        "completed-cohort",
        roles=("victim",),
        coverage_profile="fixture",
    )
    normal_case = Scenario(
        "normal", "fixture-check", profile.profile_id, "completed-cohort", roles=("victim",), coverage_profile="fixture"
    )
    value = adapter(tmp_path, factory=ObservedCloud)
    value.evidence_builder = build_evidence
    value.driver_factory = driver_factory  # type: ignore[assignment]
    scheduler = ResourceScheduler()

    results = asyncio.run(
        run_and_adjudicate([no_work_case, normal_case], [profile], value, run_id="run", scheduler=scheduler)
    )
    no_work_result = next(result for result in results if result.case_id == "no-work")
    normal_result = next(result for result in results if result.case_id == "normal")
    # ValueError (the factory's failure) adjudicates as INCOMPLETE_EVIDENCE.
    assert no_work_result.status == "INCOMPLETE_EVIDENCE"
    assert any("driver construction failed" in error for error in no_work_result.errors), no_work_result.errors
    assert normal_result.status not in ("INFRASTRUCTURE_ERROR", "NOT_IMPLEMENTED")
    assert scheduler.quarantine_reason(FUNCTION) is None, results
    release.assert_called_once()


def test_cloud_factory_failure_before_registration_does_not_quarantine_a_shared_target(tmp_path, monkeypatch):
    """Same shape as the `driver_factory` case above, but `cloud_factory` -
    tried first in `SharedAdapter.start` - is the one that fails before the
    case is ever registered. `no_work_cases` and `collect_partial` must
    treat this exactly like the `driver_factory` failure.
    """
    from aws_durable_execution_conformance_tests.lmi import quarantine, source_evidence

    release = Mock()
    monkeypatch.setattr(quarantine, "acquire_fence", lambda *args, **kwargs: "token")
    monkeypatch.setattr(quarantine, "release_fence", release)

    class ObservedCloud(FakeCloud):
        observation_ended_at: float
        retired_at: float | None = None
        source_uris: ClassVar[set[str]] = {"memory://source"}
        execution_results: ClassVar[dict[str, dict[str, str]]] = {EXECUTION: {"Status": "SUCCEEDED"}}
        terminal_histories: ClassVar[set[str]] = {EXECUTION}

        def _save(self, name, evidence):
            pass

    started = time.time() * 1000 - 100

    def snapshot_sources(ctx, cloud):
        owned = {"requestId": "request", "ownerRequestId": "request", "executionArn": EXECUTION}
        events = [
            {**owned, "kind": "wrapper_enter", "atMs": started},
            {**owned, "kind": "task_start", "taskId": "root", "atMs": started + 1},
            {**owned, "kind": "wrapper_return", "outcome": "SUCCEEDED", "atMs": started + 2},
            {**owned, "kind": "task_exit", "taskId": "root", "atMs": started + 3},
        ]
        raw_events = [
            {"phase": "WRAPPER_ENTER", "request": "request", "sequence": 1, "time": started / 1000},
            *(
                {
                    "phase": "CONFORMANCE",
                    "request": "request",
                    "sequence": capture + 1,
                    "observation": {"captureSequence": capture},
                }
                for capture, _ in enumerate(events, 1)
            ),
            {
                "phase": "SEALED",
                "request": "request",
                "sequence": len(events) + 2,
                "sourceProtocol": 2,
                "throughCapture": len(events),
            },
        ]
        for event in raw_events:
            event.update(marker="marker", execution=EXECUTION)
        cloud.events = dict(enumerate(raw_events, 1))
        cloud.raw_histories = {
            EXECUTION: [
                {"EventType": "ExecutionStarted", "EventId": 1},
                {"EventType": "ExecutionSucceeded", "EventId": 2, "EventTimestamp": started / 1000},
                {
                    "EventType": "InvocationCompleted",
                    "EventId": 3,
                    "InvocationCompletedDetails": {
                        "RequestId": "request",
                        "StartTimestamp": started / 1000,
                        "EndTimestamp": (started + 3) / 1000,
                    },
                },
            ]
        }
        attempt = {
            **owned,
            "environmentId": "environment",
            "workerId": "worker",
            "processId": "process",
            "role": "victim",
            "attempt": 1,
            "startedAtMs": started,
            "endedAtMs": started + 3,
            "outcome": "SUCCEEDED",
        }
        history = {
            "executionArn": EXECUTION,
            "ownerCaseId": ctx.scenario.case_id,
            "status": "SUCCEEDED",
            "operations": [],
        }
        return SimpleNamespace(
            observations=lambda **kwargs: (events, []),
            histories=lambda **kwargs: ([history], {}, []),
            attempts=lambda events, **kwargs: ([attempt], {"victim": "request"}, []),
        )

    class Driver:
        def __init__(self, case_context, cloud):
            self.context = case_context
            self.cloud = cloud

        def start(self):
            self.context.record_attempt("launch", requestId="request", executionArn=EXECUTION, role="victim")
            self.cloud.invocations.append({"arn": EXECUTION, "marker": "marker", "native_requests": ["request"]})

        def place(self):
            pass

        def execute(self):
            pass

        def collect(self):
            self.cloud.observation_ended_at = time.time()
            self.cloud.normalized_sources = snapshot_sources(self.context, self.cloud)

        def retire(self):
            self.cloud.observation_ended_at = time.time()
            self.cloud.retired_at = time.time()
            self.cloud.normalized_sources = snapshot_sources(self.context, self.cloud)
            return True

    def cloud_factory(deployment_config, key, *, artifacts_dir, case_context=None):
        # `prepare()` also calls this factory, for shared profile-level fence
        # verification, with no `case_context` at all; only a per-case start
        # for "no-work" should fail here.
        if case_context is not None and case_context.scenario.case_id == "no-work":
            # Cloud itself never got constructed, so no service I/O could
            # have run for this case at all.
            raise ValueError("cloud construction failed")
        return ObservedCloud(deployment_config, key, artifacts_dir=artifacts_dir, case_context=case_context)

    monkeypatch.setattr(
        source_evidence.CaseSources,
        "capture",
        lambda ctx, cloud, **options: cloud.normalized_sources,
    )

    profile = replace(PROFILE, configuration_id=configuration_identity(deployment(), "c1"))
    no_work_case = Scenario(
        "no-work",
        "fixture-check",
        profile.profile_id,
        "completed-cohort",
        roles=("victim",),
        coverage_profile="fixture",
    )
    normal_case = Scenario(
        "normal", "fixture-check", profile.profile_id, "completed-cohort", roles=("victim",), coverage_profile="fixture"
    )
    value = adapter(tmp_path, factory=cloud_factory)
    value.evidence_builder = build_evidence
    value.driver_factory = Driver  # type: ignore[assignment]
    scheduler = ResourceScheduler()

    results = asyncio.run(
        run_and_adjudicate([no_work_case, normal_case], [profile], value, run_id="run", scheduler=scheduler)
    )
    no_work_result = next(result for result in results if result.case_id == "no-work")
    normal_result = next(result for result in results if result.case_id == "normal")
    assert no_work_result.status == "INCOMPLETE_EVIDENCE"
    assert any("cloud construction failed" in error for error in no_work_result.errors), no_work_result.errors
    assert normal_result.status not in ("INFRASTRUCTURE_ERROR", "NOT_IMPLEMENTED")
    assert scheduler.quarantine_reason(FUNCTION) is None, results
    release.assert_called_once()


def test_newly_discovered_request_defers_binding_then_retirement_binds_and_drains_it(tmp_path, monkeypatch):
    """End-to-end regression for the budget-expiry binding deferral, using a
    REAL `Cloud` (mocked boto3 clients only, not `FakeCloud`): a refresh
    discovers a newly published native request's sealed stream under the
    owned launch's marker, but the collection budget expires right before
    `_bind_requests` runs. Observation collection must complete with the gap
    recorded as pending (not a collection error); a later, deadline-free
    retirement refresh then binds and drains the request for real, leaving
    nothing fenced.
    """
    from aws_durable_execution_conformance_tests.lmi import quarantine

    release = Mock()
    monkeypatch.setattr(quarantine, "acquire_fence", lambda *args, **kwargs: "token")
    monkeypatch.setattr(quarantine, "release_fence", release)

    clients = {name: Mock() for name in ("lambda", "s3", "logs")}
    clients["lambda"].invoke.side_effect = lambda **kwargs: {
        "StatusCode": 202,
        "DurableExecutionArn": f"{kwargs['FunctionName']}/durable-execution/{kwargs['DurableExecutionName']}/id",
    }
    clients["lambda"].get_durable_execution.return_value = {"Status": "SUCCEEDED"}
    clients["lambda"].get_durable_execution_history.return_value = {
        "Events": [
            {"EventId": 1, "EventType": "ExecutionStarted"},
            {"EventId": 2, "EventType": "ExecutionSucceeded", "EventTimestamp": 0.45},
            {
                "EventId": 3,
                "EventType": "InvocationCompleted",
                "InvocationCompletedDetails": {
                    "RequestId": "native-request-1",
                    "StartTimestamp": 0.1,
                    "EndTimestamp": 0.5,
                },
            },
        ]
    }
    clients["s3"].put_object.return_value = {"ResponseMetadata": {"RetryAttempts": 0}}
    clients["s3"].head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")

    marker, request = "launch", "native-request-1"
    event_keys = [f"runs/run/events/{marker}/{index}.json" for index in range(6)]
    clients["s3"].get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": key} for key in event_keys], "IsTruncated": False},
    ]

    execution_arn: dict[str, str] = {}

    def ledger_records():
        scope = {
            "request": request,
            "execution": execution_arn["value"],
            "marker": marker,
            "environment": "environment",
            "process": "process",
            "run": "run",
            "caseId": "case",
            "commit": "sdk-sha",
        }
        observations = [
            {"kind": "wrapper_enter", "atMs": 100},
            {"kind": "task_start", "taskId": "root", "atMs": 110},
            {"kind": "wrapper_return", "outcome": "SUCCEEDED", "atMs": 300},
            {"kind": "task_exit", "taskId": "root", "atMs": 400},
        ]
        return [
            {**scope, "phase": "WRAPPER_ENTER", "sequence": 1, "time": 0.1},
            *(
                {
                    **scope,
                    "phase": "CONFORMANCE",
                    "sequence": capture + 1,
                    "time": 0.2,
                    "observation": {**event, "captureSequence": capture},
                }
                for capture, event in enumerate(observations, 1)
            ),
            {
                **scope,
                "phase": "SEALED",
                "sequence": len(observations) + 2,
                "time": 0.2,
                "sourceProtocol": 2,
                "throughCapture": len(observations),
            },
        ]

    def get_object(*, Key, **_):
        if Key not in event_keys:
            # A control-gate confirmation read (e.g. for "release-all"): the
            # held body `hold()` would have written.
            return {"Body": io.BytesIO(b"hold"), "ETag": '"held"'}
        record = ledger_records()[event_keys.index(Key)]
        return {"Body": io.BytesIO(json.dumps(record).encode())}

    clients["s3"].get_object.side_effect = get_object

    bind_calls = {"count": 0}

    def cloud_factory(deployment_config, key, *, artifacts_dir, case_context=None):
        cloud = cloud_module.Cloud(
            deployment_config,
            key,
            artifacts_dir=artifacts_dir,
            case_context=case_context,
            verify_profile=Mock(),
            client_factory=lambda service, region: clients[service],
        )
        if case_context is not None:
            original_bind_requests = cloud._bind_requests

            def deferred_bind_requests(events, incomplete=None):
                bind_calls["count"] += 1
                if bind_calls["count"] == 1:
                    # Simulate the budget expiring exactly at `Cloud.refresh`'s
                    # pre-bind check: the newly read stream is already cached
                    # (self.events), but binding itself never runs this time.
                    raise CollectionBudgetExpired("pre-bind budget expired")
                return original_bind_requests(events, incomplete)

            cloud._bind_requests = deferred_bind_requests  # type: ignore[method-assign]
        return cloud

    class Driver:
        def __init__(self, case_context, cloud):
            self.context = case_context
            self.cloud = cloud

        def start(self):
            self.cloud.start("scenario", role="victim", marker=marker)
            execution_arn["value"] = self.cloud.invocations[0]["arn"]

        def place(self):
            pass

        def execute(self):
            pass

        def collect(self):
            self.cloud.observation_ended_at = time.time()
            # Adjudication still judges the retained observations; the
            # coordinator still audits retirement.
            with contextlib.suppress(CollectionBudgetExpired):
                self.cloud.collect(full_run=False)

        def retire(self):
            self.cloud.collect(full_run=False)
            self.cloud.delete_controls()
            self.cloud.retired_at = time.time()
            return True

    profile = replace(PROFILE, configuration_id=configuration_identity(deployment(), "c1"))
    case = Scenario("case", "fixture-check", profile.profile_id, "completed-cohort", roles=("victim",))
    value = adapter(tmp_path, factory=cloud_factory)
    value.evidence_builder = build_evidence
    value.driver_factory = Driver  # type: ignore[assignment]
    scheduler = ResourceScheduler()

    (result,) = asyncio.run(run_and_adjudicate([case], [profile], value, run_id="run", scheduler=scheduler))

    not_bound_markers = ("was not bound to an original launch", "inconsistent ledger, launch or service ownership")
    snapshots = [result.evidence, *(item for items in result.cleanup_evidence.values() for item in items)]
    for snapshot in snapshots:
        for error in snapshot.get("collectionErrors", []):
            assert not any(text in error for text in not_bound_markers), (snapshot.get("scenario"), error)
    assert bind_calls["count"] >= 2
    # Pin the deferral itself, not just the absence of a collection error:
    # the observation snapshot must have actually recorded the gap as pending.
    assert any("native-request-1" in entry for entry in result.evidence["collection"].get("pending", []))
    assert scheduler.quarantine_reason(FUNCTION) is None, result.errors
    release.assert_called_once()


@pytest.mark.parametrize("journaled", [False, True])
def test_preflight_value_error_does_not_retain_empty_case_but_keeps_a_prepared_intent(tmp_path, journaled):
    class Driver:
        def __init__(self, context, cloud):
            self.cloud = cloud

        def start(self):
            if journaled:
                intents = self.cloud.artifacts_dir / "invocations"
                intents.mkdir(parents=True)
                (intents / "prepared.json").write_text('{"dispatchState":"PREPARED"}')
            raise ValueError("invalid binding")

    async def exercise():
        value = adapter(tmp_path)
        value.driver_factory = Driver  # type: ignore[assignment]
        ctx = context()
        with pytest.raises(ValueError, match="invalid binding"):
            await value.start(ctx)
        assert (ctx.scenario.case_id in value.cases) is journaled
        if not journaled:
            assert await value.retire(ctx)
            assert not ctx.retirement_evidence

    asyncio.run(exercise())


@pytest.mark.parametrize("reject", [False, True])
def test_driver_threads_journal_on_the_coordinator_loop(tmp_path, monkeypatch, reject):
    """The adapter's view runs worker-thread journal calls on the coordinator loop."""
    import threading

    from aws_durable_execution_conformance_tests.lmi import quarantine
    from aws_durable_execution_conformance_tests.lmi.coordinator import run_scenarios

    monkeypatch.setattr(quarantine, "acquire_fence", lambda *args, **kwargs: "token")
    monkeypatch.setattr(quarantine, "release_fence", Mock())
    calls: list[str] = []

    class Driver:
        def __init__(self, case_context, cloud):
            self.context = case_context
            self.cloud = cloud

        def start(self):
            assert threading.current_thread() is not threading.main_thread()
            self.context.record_attempt("launch", role="victim")
            calls.append("dispatch")
            self.context.update_attempt("launch", requestId="request", executionArn=EXECUTION)
            self.context.record_service_attempt(
                "service-retry", original_request_id="request", execution_arn=EXECUTION, role="victim"
            )
            calls.append("bound")

        def place(self):
            pass

        def execute(self):
            pass

        def collect(self):
            pass

        def retire(self):
            return True

    sink_threads: set[threading.Thread] = set()

    def on_attempt(context):
        sink_threads.add(threading.current_thread())
        if reject:
            raise ValueError("journal refused the launch")

    profile = replace(PROFILE, configuration_id=configuration_identity(deployment(), "c1"))
    case = Scenario(
        "case", "fixture-check", profile.profile_id, "completed-cohort", roles=("victim",), coverage_profile="fixture"
    )
    value = adapter(tmp_path)
    value.driver_factory = Driver  # type: ignore[assignment]
    (result,) = asyncio.run(
        run_scenarios([case], [profile], value, run_id="run", scheduler=ResourceScheduler(), on_attempt=on_attempt)
    )
    # Worker threads journal through the adapter's view, so the sink runs on the
    # coordinator loop's thread.
    assert sink_threads == {threading.main_thread()}
    if reject:
        # The journal sink's failure reaches the worker before it dispatches anything.
        assert calls == []
        assert result.attempts == []
        assert any("journal refused the launch" in error for error in result.errors), result.errors
    else:
        assert calls == ["dispatch", "bound"]
        launch, retry = result.attempts
        assert (launch["requestId"], launch["executionArn"], "launchedAtMs" in launch) == ("request", EXECUTION, True)
        assert (retry["requestId"], retry["executionArn"], "discoveredAtMs" in retry) == (
            "service-retry",
            EXECUTION,
            True,
        )


def test_pending_observations_do_not_fence_after_audited_retirement(tmp_path, monkeypatch):
    from aws_durable_execution_conformance_tests.lmi import quarantine, source_evidence

    release = Mock()
    monkeypatch.setattr(quarantine, "acquire_fence", lambda *args, **kwargs: "token")
    monkeypatch.setattr(quarantine, "release_fence", release)

    class ObservedCloud(FakeCloud):
        observation_ended_at: float
        retired_at: float | None = None
        source_uris: ClassVar[set[str]] = {"memory://source"}
        execution_results: ClassVar[dict[str, dict[str, str]]] = {EXECUTION: {"Status": "SUCCEEDED"}}
        terminal_histories: ClassVar[set[str]] = {EXECUTION}

        def _save(self, name, evidence):
            pass

    class Driver:
        def __init__(self, case_context, cloud):
            self.context = case_context
            self.cloud = cloud

        def start(self):
            self.context.record_attempt("launch", requestId="request", executionArn=EXECUTION, role="victim")
            self.cloud.invocations.append({"arn": EXECUTION, "marker": "marker", "native_requests": ["request"]})

        def place(self):
            pass

        def execute(self):
            pass

        def collect(self):
            self.cloud.observation_ended_at = time.time()
            self.cloud.normalized_sources = snapshot_sources(self.context, self.cloud)

        def retire(self):
            self.cloud.observation_ended_at = time.time()
            self.cloud.retired_at = time.time()
            self.cloud.normalized_sources = snapshot_sources(self.context, self.cloud)
            return True

    started = time.time() * 1000 - 100

    def snapshot_sources(ctx, cloud):
        owned = {"requestId": "request", "ownerRequestId": "request", "executionArn": EXECUTION}
        events = [
            {**owned, "kind": "wrapper_enter", "atMs": started},
            {**owned, "kind": "task_start", "taskId": "root", "atMs": started + 1},
            {**owned, "kind": "wrapper_return", "outcome": "SUCCEEDED", "atMs": started + 2},
        ]
        if cloud.retired_at is not None:
            events.append({**owned, "kind": "task_exit", "taskId": "root", "atMs": started + 3})
        raw_events = [
            {"phase": "WRAPPER_ENTER", "request": "request", "sequence": 1, "time": started / 1000},
            *(
                {
                    "phase": "CONFORMANCE",
                    "request": "request",
                    "sequence": capture + 1,
                    "observation": {"captureSequence": capture},
                }
                for capture, _ in enumerate(events, 1)
            ),
        ]
        if cloud.retired_at is not None:
            raw_events.append(
                {
                    "phase": "SEALED",
                    "request": "request",
                    "sequence": len(raw_events) + 1,
                    "sourceProtocol": 2,
                    "throughCapture": len(events),
                }
            )
        for event in raw_events:
            event.update(marker="marker", execution=EXECUTION)
        cloud.events = dict(enumerate(raw_events, 1))
        cloud.raw_histories = {
            EXECUTION: [
                {"EventType": "ExecutionStarted", "EventId": 1},
                {"EventType": "ExecutionSucceeded", "EventId": 2, "EventTimestamp": started / 1000},
                {
                    "EventType": "InvocationCompleted",
                    "EventId": 3,
                    "InvocationCompletedDetails": {
                        "RequestId": "request",
                        "StartTimestamp": started / 1000,
                        "EndTimestamp": (started + 3) / 1000,
                    },
                },
            ]
        }
        attempt = {
            **owned,
            "environmentId": "environment",
            "workerId": "worker",
            "processId": "process",
            "role": "victim",
            "attempt": 1,
            "startedAtMs": started,
            "endedAtMs": started + 3,
            "outcome": "SUCCEEDED",
        }
        history = {
            "executionArn": EXECUTION,
            "ownerCaseId": ctx.scenario.case_id,
            "status": "SUCCEEDED",
            "operations": [],
        }
        return SimpleNamespace(
            observations=lambda **kwargs: (events, []),
            histories=lambda **kwargs: ([history], {}, []),
            attempts=lambda events, **kwargs: ([attempt], {"victim": "request"}, []),
        )

    monkeypatch.setattr(
        source_evidence.CaseSources,
        "capture",
        lambda ctx, cloud, **options: cloud.normalized_sources,
    )
    profile = replace(PROFILE, configuration_id=configuration_identity(deployment(), "c1"))
    case = Scenario(
        "case", "fixture-check", profile.profile_id, "completed-cohort", roles=("victim",), coverage_profile="fixture"
    )
    value = adapter(tmp_path, factory=ObservedCloud)
    value.evidence_builder = build_evidence
    value.driver_factory = Driver  # type: ignore[assignment]
    scheduler = ResourceScheduler()

    def validator(evidence, *, phase="final"):
        required = {"lifecycle", "sdkRequests", "history"}
        return (
            []
            if required == evidence["collection"]["watermarks"].keys()
            else ["INCOMPLETE: observation window still open"]
        )

    (result,) = asyncio.run(
        run_and_adjudicate([case], [profile], value, run_id="run", validator=validator, scheduler=scheduler)
    )
    assert "INCOMPLETE: observation window still open" in result.errors
    assert result.evidence["collectionErrors"] == []
    assert result.retirement_evidence["collection"]["watermarks"]
    assert scheduler.quarantine_reason(FUNCTION) is None, result.errors
    release.assert_called_once()


def test_undispatched_rejection_releases_the_fence_but_stays_incomplete_behavior(tmp_path, monkeypatch):
    from aws_durable_execution_conformance_tests.lmi import quarantine, source_evidence
    from aws_durable_execution_conformance_tests.lmi.adjudication import adjudicate_evidence

    release = Mock()
    monkeypatch.setattr(quarantine, "acquire_fence", lambda *args, **kwargs: "token")
    monkeypatch.setattr(quarantine, "release_fence", release)

    class ObservedCloud(FakeCloud):
        observation_ended_at: float
        retired_at: float | None = None
        source_uris: ClassVar[set[str]] = {"memory://source"}
        execution_results: ClassVar[dict[str, dict[str, str]]] = {EXECUTION: {"Status": "SUCCEEDED"}}
        terminal_histories: ClassVar[set[str]] = {EXECUTION}

        def _save(self, name, evidence):
            pass

    class Driver:
        def __init__(self, case_context, cloud):
            self.context = case_context
            self.cloud = cloud

        def start(self):
            self.context.record_attempt("launch", requestId="request", executionArn=EXECUTION, role="victim")
            self.cloud.invocations.append({"arn": EXECUTION, "marker": "marker", "native_requests": ["request"]})

        def place(self):
            pass

        def execute(self):
            pass

        def collect(self):
            self.cloud.observation_ended_at = time.time()
            self.cloud.normalized_sources = snapshot_sources(self.context, self.cloud)

        def retire(self):
            self.cloud.observation_ended_at = time.time()
            self.cloud.retired_at = time.time()
            self.cloud.normalized_sources = snapshot_sources(self.context, self.cloud)
            return True

    started = time.time() * 1000 - 100

    def snapshot_sources(ctx, cloud):
        owned = {"requestId": "request", "ownerRequestId": "request", "executionArn": EXECUTION}
        events = [
            {**owned, "kind": "wrapper_enter", "atMs": started},
            {**owned, "kind": "task_start", "taskId": "root", "atMs": started + 1},
            {**owned, "kind": "wrapper_return", "outcome": "SUCCEEDED", "atMs": started + 2},
        ]
        if cloud.retired_at is not None:
            events.append({**owned, "kind": "task_exit", "taskId": "root", "atMs": started + 3})
        raw_events = [
            {"phase": "WRAPPER_ENTER", "request": "request", "sequence": 1, "time": started / 1000},
            *(
                {
                    "phase": "CONFORMANCE",
                    "request": "request",
                    "sequence": capture + 1,
                    "observation": {"captureSequence": capture},
                }
                for capture, _ in enumerate(events, 1)
            ),
        ]
        if cloud.retired_at is not None:
            raw_events.append(
                {
                    "phase": "SEALED",
                    "request": "request",
                    "sequence": len(raw_events) + 1,
                    "sourceProtocol": 2,
                    "throughCapture": len(events),
                }
            )
        for event in raw_events:
            event.update(marker="marker", execution=EXECUTION)
        cloud.events = dict(enumerate(raw_events, 1))
        # A real run's Cloud._save retains the history URI as it is written;
        # this fixture's no-op _save does not, so register it explicitly.
        cloud.source_uris.add((cloud.artifacts_dir / "histories/marker.json").resolve().as_uri())
        cloud.raw_histories = {
            EXECUTION: [
                {"EventType": "ExecutionStarted", "EventId": 1},
                {"EventType": "ExecutionSucceeded", "EventId": 2, "EventTimestamp": started / 1000},
                {
                    "EventType": "InvocationCompleted",
                    "EventId": 3,
                    "InvocationCompletedDetails": {
                        "RequestId": "request",
                        "StartTimestamp": started / 1000,
                        "EndTimestamp": (started + 3) / 1000,
                    },
                },
                # The service also rejected a second native request on the same
                # launch before any handler ran; it never gets a ledger stream.
                {
                    "EventType": "InvocationCompleted",
                    "EventId": 4,
                    "InvocationCompletedDetails": {
                        "RequestId": "rejected",
                        "StartTimestamp": (started - 10) / 1000,
                        "EndTimestamp": (started - 5) / 1000,
                        "Error": {
                            "Payload": {"ErrorType": "TooManyRequestsException", "ErrorMessage": "capacity"},
                            "Truncated": False,
                        },
                    },
                },
            ]
        }
        attempt = {
            **owned,
            "environmentId": "environment",
            "workerId": "worker",
            "processId": "process",
            "role": "victim",
            "attempt": 1,
            "startedAtMs": started,
            "endedAtMs": started + 3,
            "outcome": "SUCCEEDED",
        }
        history = {
            "executionArn": EXECUTION,
            "ownerCaseId": ctx.scenario.case_id,
            "status": "SUCCEEDED",
            "operations": [],
        }
        return SimpleNamespace(
            observations=lambda **kwargs: (events, []),
            histories=lambda **kwargs: ([history], {}, []),
            attempts=lambda events, **kwargs: ([attempt], {"victim": "request"}, []),
        )

    monkeypatch.setattr(
        source_evidence.CaseSources,
        "capture",
        lambda ctx, cloud, **options: cloud.normalized_sources,
    )
    profile = replace(PROFILE, configuration_id=configuration_identity(deployment(), "c1"))
    case = Scenario(
        "case", "fixture-check", profile.profile_id, "completed-cohort", roles=("victim",), coverage_profile="fixture"
    )
    value = adapter(tmp_path, factory=ObservedCloud)
    value.evidence_builder = build_evidence
    value.driver_factory = Driver  # type: ignore[assignment]
    scheduler = ResourceScheduler()

    def validator(evidence, *, phase="final"):
        required = {"lifecycle", "sdkRequests", "history"}
        return (
            []
            if required == evidence["collection"]["watermarks"].keys()
            else ["INCOMPLETE: observation window still open"]
        )

    (result,) = asyncio.run(
        run_and_adjudicate([case], [profile], value, run_id="run", validator=validator, scheduler=scheduler)
    )
    assert "INCOMPLETE: observation window still open" in result.errors
    assert result.evidence["collectionErrors"] == []
    settlement = next(
        item for item in result.retirement_evidence["retirement"]["resources"] if item["id"] == "rejected"
    )
    assert settlement["outcome"] == "UNDISPATCHED"
    assert result.retirement_evidence["collection"]["watermarks"]
    assert scheduler.quarantine_reason(FUNCTION) is None, result.errors
    release.assert_called_once()

    status, findings, _ = adjudicate_evidence(
        "COLLECTED",
        [],
        result.evidence,
        retirement_evidence=result.retirement_evidence,
        cleanup_evidence=result.cleanup_evidence or {},
        standard=True,
    )
    assert status != "PASSED"
    assert any("service rejected native request rejected before its handler ran" in finding for finding in findings)


def test_foreign_target_prevents_any_driver_launch(tmp_path):
    value = adapter(tmp_path)
    ctx = context()
    ctx.profile = Profile("foreign", "arn:foreign", 1, 1, "sdk-sha", "configuration-sha")
    with pytest.raises(ProvisioningError, match="outside"):
        asyncio.run(value.start(ctx))
    assert value.cases == {}


def test_cancelled_worker_is_joined_before_case_cleanup(monkeypatch, tmp_path):
    entered, unblock, exited = Event(), Event(), Event()

    class Driver:
        def __init__(self, context, cloud, **kwargs):
            self.cloud = cloud

        def start(self):
            entered.set()
            unblock.wait(2)
            try:
                self.cloud.check_active()
            finally:
                exited.set()

        def retire(self):
            assert exited.is_set()
            return True

    monkeypatch.setattr(adapter_module, "ScenarioDriver", Driver)

    async def exercise():
        value = adapter(tmp_path)
        ctx = context()
        starting = asyncio.create_task(value.start(ctx))
        assert await asyncio.to_thread(entered.wait, 1)
        starting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await starting
        releasing = asyncio.create_task(value.release(ctx))
        await asyncio.sleep(0)
        assert "release" not in value.cases[ctx.scenario.case_id].driver.cloud.actions
        unblock.set()
        await releasing
        assert exited.is_set()
        assert await value.retire(ctx)
        assert ctx.evidence["retirement"] == {"owner": "case", "phase": "final"}

    asyncio.run(exercise())


def _reporting_shield(monkeypatch):
    """Make ``asyncio.shield`` behave as on Python 3.14.6, which reports any exception a
    shielded future finishes with after its waiter was cancelled."""
    real_shield = asyncio.shield

    def reporting_shield(inner: Any) -> Any:
        outer = real_shield(inner)

        def waiter_done(waiter: Any) -> None:
            if waiter.cancelled():

                def report(finished: Any) -> None:
                    if not finished.cancelled() and finished.exception() is not None:
                        error = finished.exception()
                        finished.get_loop().call_exception_handler(
                            {"message": f"{type(error).__name__} exception in shielded future", "exception": error}
                        )

                inner.add_done_callback(report)

        outer.add_done_callback(waiter_done)
        return outer

    monkeypatch.setattr(asyncio, "shield", reporting_shield)


def test_cancelled_case_worker_failure_is_not_reported_as_unhandled(monkeypatch, tmp_path):
    _reporting_shield(monkeypatch)
    entered, unblock = Event(), Event()

    class Driver:
        def __init__(self, context, cloud, **kwargs):
            self.cloud = cloud

        def start(self):
            entered.set()
            unblock.wait(2)
            # The cancelled case's cloud refuses further work.
            self.cloud.check_active()

    monkeypatch.setattr(adapter_module, "ScenarioDriver", Driver)

    async def exercise():
        reported: list[str] = []
        asyncio.get_running_loop().set_exception_handler(lambda loop, context: reported.append(context["message"]))
        value = adapter(tmp_path)
        ctx = context()
        starting = asyncio.create_task(value.start(ctx))
        assert await asyncio.to_thread(entered.wait, 1)
        starting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await starting
        # A cancelled release must not leave a reporting waiter on the worker either.
        releasing = asyncio.create_task(value.release(ctx))
        await asyncio.sleep(0)
        releasing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await releasing
        unblock.set()
        await value.release(ctx)
        await asyncio.sleep(0)
        return reported

    assert asyncio.run(exercise()) == []


def test_cancelled_preparation_failure_is_not_reported_as_unhandled(monkeypatch, tmp_path):
    from aws_durable_execution_conformance_tests.lmi import quarantine

    _reporting_shield(monkeypatch)
    entered, unblock = Event(), Event()

    def acquire(*args, **kwargs):
        entered.set()
        assert unblock.wait(2)
        return "owned-token"

    class InvalidCloud(FakeCloud):
        def verify(self):
            raise ProvisioningError("deployment drifted")

    monkeypatch.setattr(quarantine, "acquire_fence", acquire)
    monkeypatch.setattr(quarantine, "release_fence", lambda *args: None)

    async def exercise():
        reported: list[str] = []
        asyncio.get_running_loop().set_exception_handler(lambda loop, context: reported.append(context["message"]))
        value = adapter(tmp_path, factory=InvalidCloud)
        valid = replace(PROFILE, configuration_id=configuration_identity(deployment(), "c1"))
        preparing = asyncio.create_task(value.prepare(valid))
        assert await asyncio.to_thread(entered.wait, 1)
        preparing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await preparing
        # A cancelled finish must not leave a reporting waiter on the worker either.
        finishing = asyncio.create_task(value.finish(valid, quiescent=True))
        await asyncio.sleep(0)
        finishing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await finishing
        unblock.set()
        # Finish joins the preparation worker, whose verification failed after the cancellations.
        await value.finish(valid, quiescent=True)
        await asyncio.sleep(0)
        return reported

    assert asyncio.run(exercise()) == []


class ScriptCloud:
    def __init__(self, native=1):
        self.concurrency = native
        self.manifest = {"driverTimeout": 1, "invocationTimeout": 60}
        self.suspension_mode = "callback"
        self.actions = []
        self.invocations = []
        self.raw_histories = {}
        self.source_uris = set()

    def record_deadline_measurement(self, snapshot):
        self.actions.append(("measurement", snapshot))

    def wait_terminal(self, item):
        self.actions.append(("terminal", item["marker"]))
        return {"Status": "FAILED"}

    def refresh(self, **kwargs):
        return [event for item in self.invocations for event in self.for_item(item)]

    def start(self, fixture, **kwargs):
        item = {
            "marker": kwargs["role"],
            "arn": f"execution:{kwargs['role']}",
            "runner": Mock(),
        }
        item["runner"].wait_for_callback.return_value = "callback-id"
        self.actions.append(("start", fixture, kwargs))
        self.invocations.append(item)
        return item

    def phase(self, item, phase, **kwargs):
        self.actions.append(("phase", item["marker"], phase))
        return [
            {
                "phase": phase,
                "time": 0,
                "deadline": 0,
                "environment": "environment",
                "request": "request",
                "status": "PENDING",
            }
        ]

    def poll(self, predicate, **kwargs):
        self.actions.append(("poll", kwargs.get("message")))
        return predicate() or [{"phase": "PLACEMENT_ACCEPTED", "environment": "environment"}]

    def release(self, gate):
        self.actions.append(("release", gate))

    def finish(self, item, status="SUCCEEDED"):
        self.actions.append(("finish", item["marker"], status))

    def for_item(self, item):
        return [
            {
                "phase": "WRAPPER_RETURN",
                "status": "PENDING",
                "time": 1,
                "request": "first",
            },
            {"phase": "WRAPPER_ENTER", "request": "first"},
            {"phase": "WRAPPER_ENTER", "request": "second"},
            {"phase": "LATE_REJECTED"},
            {"phase": "ALIVE", "time": 2},
        ]

    def platform_timeout(self, item, request):
        self.actions.append(("platform-timeout", item["arn"]))

    def history(self, item):
        self.actions.append(("history", item["arn"]))

    def check_active(self):
        pass


@pytest.mark.parametrize(
    ("encoder", "encoded_marker"),
    [(None, b"victim"), (lambda marker: json.dumps(marker).encode(), b'"victim"')],
)
@pytest.mark.parametrize("response_lost", [None, "ServiceException", "CallbackTimeoutException"])
def test_callback_uses_same_execution_and_actual_pending_before_response(
    monkeypatch, encoder, encoded_marker, response_lost
):
    cloud = ScriptCloud()
    options = {"callback_encoder": encoder} if encoder is not None else {}
    driver = ScenarioDriver(context(), cloud, **options)
    driver.start()
    driver.place()
    runner = cloud.invocations[0]["runner"]
    if response_lost:
        runner.send_callback_success.side_effect = ClientError(
            {
                "Error": {"Code": response_lost},
                "ResponseMetadata": {"HTTPStatusCode": 503 if response_lost == "ServiceException" else 410},
            },
            "SendDurableExecutionCallbackSuccess",
        )
    first = {
        "EventType": "InvocationCompleted",
        "InvocationCompletedDetails": {
            "RequestId": "first",
            "StartTimestamp": 0,
            "EndTimestamp": 1.1,
        },
    }
    second = {
        "EventType": "InvocationCompleted",
        "InvocationCompletedDetails": {
            "RequestId": "second",
            "StartTimestamp": 2,
            "EndTimestamp": 3.1,
        },
    }
    checkpoint = {
        "EventType": "CallbackStarted",
        "Id": "operation",
        "Name": "completion",
        "EventTimestamp": 2.1,
        "CallbackStartedDetails": {"CallbackId": "callback-id"},
    }
    reads = 0

    def history(_):
        nonlocal reads
        reads += 1
        if reads == 1:
            return [first, checkpoint]
        if reads > 2:
            return [
                first,
                checkpoint,
                second,
                {
                    "EventType": "CallbackSucceeded",
                    "Id": "operation",
                    "CallbackSucceededDetails": {"Result": {"Payload": encoded_marker.decode(), "Truncated": False}},
                },
            ]
        return [first, checkpoint, second]

    def events(_):
        return [
            *ScriptCloud.for_item(cloud, None),
            {"phase": "WRAPPER_RETURN", "status": "PENDING", "time": 3, "request": "second"},
        ]

    def poll(predicate, **_):
        if not runner.send_callback_success.called:
            assert not predicate()  # The previous PENDING is only retry backoff.
            runner.send_callback_success.assert_not_called()
        assert predicate()

    monkeypatch.setattr(cloud, "history", history)
    monkeypatch.setattr(cloud, "for_item", events)
    monkeypatch.setattr(cloud, "poll", poll)
    driver.execute()
    runner.send_callback_success.assert_called_once_with("callback-id", result=encoded_marker)


def test_wait_only_suspension_never_sends_a_callback():
    # test_wait_crossed_suspension_uses_the_sdk_pending_decision pins the
    # service-side timer proof this mode relies on.
    cloud = ScriptCloud()
    driver = ScenarioDriver(context(), cloud, suspension_mode="wait")
    driver.start()
    driver.execute()
    assert cloud.suspension_mode == "wait"
    runner = cloud.invocations[0]["runner"]
    runner.send_callback_success.assert_not_called()
    assert ("finish", "victim", "SUCCEEDED") in cloud.actions


def _carryover_ledger_and_history(*, wakeup_at, waking_event_type, name=None):
    """Three sequential invocations: A (0-1, PENDING at 0.9), B (2-3, PENDING at
    2.9, another operation's retry), C (starts at 6). A wait or callback started
    during A can only be recognized as waking C if it survives, as outstanding,
    past B's own PENDING decision.
    """
    ledger = [
        {"phase": "WRAPPER_ENTER", "request": "A"},
        {"phase": "WRAPPER_ENTER", "request": "B"},
        {"phase": "WRAPPER_ENTER", "request": "C"},
        {"phase": "WRAPPER_RETURN", "status": "PENDING", "time": 0.9, "request": "A"},
        {"phase": "WRAPPER_RETURN", "status": "PENDING", "time": 2.9, "request": "B"},
    ]
    history: list[dict[str, Any]] = [
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {"RequestId": "A", "StartTimestamp": 0, "EndTimestamp": 1},
        },
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {"RequestId": "B", "StartTimestamp": 2, "EndTimestamp": 3},
        },
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {"RequestId": "C", "StartTimestamp": 6, "EndTimestamp": 7},
        },
        {
            "EventType": waking_event_type + "Started",
            "Id": "timer",
            "EventTimestamp": 0.5,
            **({"Name": name} if name else {}),
        },
        {"EventType": waking_event_type + "Succeeded", "Id": "timer", "EventTimestamp": wakeup_at},
    ]
    return ledger, history


def test_suspension_resumptions_carries_outstanding_wait_across_intermediate_decision():
    from aws_durable_execution_conformance_tests.lmi.observations import suspension_resumptions

    ledger, history = _carryover_ledger_and_history(wakeup_at=5, waking_event_type="Wait")
    assert suspension_resumptions(ledger, history, "wait") == {"C": {"timer"}}


def test_suspension_resumptions_carries_outstanding_callback_across_intermediate_decision():
    from aws_durable_execution_conformance_tests.lmi.observations import suspension_resumptions

    ledger, history = _carryover_ledger_and_history(wakeup_at=5, waking_event_type="Callback", name="completion")
    assert suspension_resumptions(ledger, history, "callback") == {"C": {"timer"}}


def test_suspension_resumptions_rejects_wait_from_a_request_that_never_went_pending():
    from aws_durable_execution_conformance_tests.lmi.observations import suspension_resumptions

    ledger: list[dict[str, Any]] = [
        {"phase": "WRAPPER_ENTER", "request": "X"},
        {"phase": "WRAPPER_ENTER", "request": "Y"},
        {"phase": "WRAPPER_ENTER", "request": "Z"},
        # X never returns PENDING: it is not a decision window at all.
        {"phase": "WRAPPER_RETURN", "status": "PENDING", "time": 2.9, "request": "Y"},
    ]
    history: list[dict[str, Any]] = [
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {"RequestId": "X", "StartTimestamp": 0, "EndTimestamp": 1},
        },
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {"RequestId": "Y", "StartTimestamp": 2, "EndTimestamp": 3},
        },
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {"RequestId": "Z", "StartTimestamp": 6, "EndTimestamp": 7},
        },
        # Started during X's run (0-1), which never went PENDING.
        {"EventType": "WaitStarted", "Id": "timer", "EventTimestamp": 0.5},
        # Completes between Y's PENDING decision (2.9) and Z's start (6), which
        # would otherwise satisfy the completion window for Y's decision.
        {"EventType": "WaitSucceeded", "Id": "timer", "EventTimestamp": 5},
    ]
    assert suspension_resumptions(ledger, history, "wait") == {}


def test_suspension_resumptions_rejects_wakeup_completing_after_resume_started():
    from aws_durable_execution_conformance_tests.lmi.observations import suspension_resumptions

    # Completes at 6.5, after C (the resume) already started at 6.
    ledger, history = _carryover_ledger_and_history(wakeup_at=6.5, waking_event_type="Wait")
    assert suspension_resumptions(ledger, history, "wait") == {}


def _teardown_ledger_and_history(*, decision_observed=True):
    """first returns PENDING at 1s and its invocation ends at 1.1s; second resumes at 2s.

    With ``decision_observed`` the SDK reports its PENDING decision at 900ms,
    so work completing during wrapper teardown follows the decision.
    """
    ledger: list[dict[str, Any]] = [
        {"phase": "WRAPPER_RETURN", "status": "PENDING", "time": 1, "request": "first"},
        {"phase": "WRAPPER_ENTER", "request": "first"},
        {"phase": "WRAPPER_ENTER", "request": "second"},
    ]
    if decision_observed:
        ledger.append(
            {
                "phase": "CONFORMANCE",
                "request": "first",
                "observation": {"kind": "invocation_end", "outcome": "PENDING", "atMs": 900},
            }
        )
    history: list[dict[str, Any]] = [
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {"RequestId": "first", "StartTimestamp": 0, "EndTimestamp": 1.1},
        },
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {"RequestId": "second", "StartTimestamp": 2, "EndTimestamp": 3},
        },
        {
            "EventType": "StepFailed",
            "Id": "retry",
            "StepFailedDetails": {"RetryDetails": {"NextAttemptDelaySeconds": 1}},
        },
    ]
    return ledger, history


@pytest.mark.parametrize(
    ("durable_wait", "decision_observed", "crossed"),
    [
        # The timer elapses during wrapper teardown, after the SDK's PENDING decision.
        (True, True, True),
        # Only a backoff suspended the execution.
        (False, True, False),
        # Without the SDK's decision the wrapper return bounds it, and the timer elapsed first.
        (True, False, False),
    ],
)
def test_wait_crossed_suspension_uses_the_sdk_pending_decision(durable_wait, decision_observed, crossed):
    from aws_durable_execution_conformance_tests.lmi.observations import wait_crossed_suspension

    ledger, history = _teardown_ledger_and_history(decision_observed=decision_observed)
    if durable_wait:
        history += [
            {"EventType": "WaitStarted", "Id": "timer", "EventTimestamp": 0.8},
            {"EventType": "WaitSucceeded", "Id": "timer", "EventTimestamp": 0.95},
        ]
    assert wait_crossed_suspension(ledger, history) is crossed


@pytest.mark.parametrize(
    ("started_at", "decision_observed", "selected"), [(0.85, True, True), (0.95, True, False), (0.95, False, True)]
)
def test_pending_callback_must_start_by_the_sdk_pending_decision(started_at, decision_observed, selected):
    from aws_durable_execution_conformance_tests.lmi.observations import pending_callback

    ledger, history = _teardown_ledger_and_history(decision_observed=decision_observed)
    history.append(
        {
            "EventType": "CallbackStarted",
            "Name": "completion",
            "Id": "callback",
            "EventTimestamp": started_at,
            "CallbackStartedDetails": {"CallbackId": "token"},
        }
    )
    assert pending_callback(ledger, history, "completion") == ("token" if selected else None)


def test_each_case_has_independent_artifact_namespace(monkeypatch, tmp_path):
    class Driver:
        def __init__(self, context, cloud, **kwargs):
            self.cloud = cloud

        def start(self):
            pass

    monkeypatch.setattr(adapter_module, "ScenarioDriver", Driver)

    async def exercise():
        value = adapter(tmp_path)
        await asyncio.gather(value.start(context("one")), value.start(context("two")))
        first = value.cases["one"].driver.cloud
        second = value.cases["two"].driver.cloud
        assert first.artifacts_dir != second.artifacts_dir
        assert first.case_context.scenario.case_id == "one"
        assert second.case_context.scenario.case_id == "two"
        with pytest.raises(CollectionError, match="replace"):
            await value.start(context("one"))

    asyncio.run(exercise())


def seal_fixture_events(events):
    events[:] = [event for event in events if event["phase"] != "SEALED"]
    captures = 0
    for sequence, event in enumerate(events, 1):
        event.update(sequence=sequence, marker="marker", execution="execution")
        if event["phase"] == "CONFORMANCE":
            captures += 1
            event["observation"]["captureSequence"] = captures
    events.append(
        {
            "phase": "SEALED",
            "request": "native",
            "sequence": len(events) + 1,
            "sourceProtocol": 2,
            "throughCapture": captures,
            "marker": "marker",
            "execution": "execution",
        }
    )


def settled_cloud(*, view=None):
    cloud = ScriptCloud()
    cloud.invocations = [{"arn": "execution", "marker": "marker", "native_requests": ["native"]}]
    events = [
        {"phase": "WRAPPER_ENTER", "request": "native"},
        {"phase": "WRAPPER_RETURN", "request": "native"},
        {
            "phase": "CONFORMANCE",
            "request": "native",
            "execution": "execution",
            "observation": {
                "kind": "task_start",
                "taskId": "root",
                "requestId": "native",
            },
        },
        {
            "phase": "CONFORMANCE",
            "request": "native",
            "execution": "execution",
            "observation": {
                "kind": "task_exit",
                "taskId": "root",
                "requestId": "native",
            },
        },
    ]
    seal_fixture_events(events)
    cloud.refresh = Mock(return_value=events)  # type: ignore[method-assign]
    driver = ScenarioDriver(context(), cloud)
    return driver, events


@pytest.mark.parametrize(
    "start,end,identity",
    [
        ("task_start", "task_exit", "taskId"),
        ("io_start", "io_exit", "taskId"),
        ("checkpoint_call", "checkpoint_exit", "callId"),
        ("poll_call", "poll_exit", "callId"),
    ],
)
def test_retirement_requires_every_real_task_and_call_to_exit(start, end, identity):
    driver, events = settled_cloud()
    assert driver.quiescent()
    event = {"requestId": "native", identity: "outstanding"}
    envelope = {"phase": "CONFORMANCE", "request": "native", "execution": "execution"}
    events.append({**envelope, "observation": {**event, "kind": start}})
    seal_fixture_events(events)
    assert not driver.quiescent()
    events.append({**envelope, "observation": {**event, "kind": end}})
    seal_fixture_events(events)
    assert driver.quiescent()


def test_missing_producer_seal_cannot_claim_physical_retirement():
    driver, events = settled_cloud()
    assert driver.quiescent()
    events.pop()
    assert not driver.quiescent()


def test_unidentified_launch_or_missing_root_hook_cannot_claim_retirement():
    driver, events = settled_cloud()
    driver.cloud.invocations[0]["native_requests"] = []
    assert not driver.quiescent()
    driver.cloud.invocations[0]["native_requests"] = ["native"]
    events[:] = [event for event in events if event.get("observation", {}).get("kind") != "task_exit"]
    seal_fixture_events(events)
    assert not driver.quiescent()


def test_wrong_function_response_is_never_adopted_for_case_cleanup(cloud):
    cloud.lam.invoke.side_effect = None
    cloud.lam.invoke.return_value = {
        "StatusCode": 202,
        "DurableExecutionArn": "arn:another-function/durable-execution/name/id",
    }
    with pytest.raises(CollectionError, match="API failed"):
        cloud.start("success", role="victim")
    assert cloud.invocations[0]["arn"] is None


def test_canonical_observations_keep_authoritative_outer_identity_and_hook_mismatch():
    from aws_durable_execution_conformance_tests.lmi.scenarios import canonical

    records = canonical(
        [
            {
                "phase": "CONFORMANCE",
                "request": "outer",
                "execution": "execution",
                "observation": {"kind": "task_exit", "taskId": "root"},
            },
            {
                "phase": "CONFORMANCE",
                "request": "outer",
                "execution": "execution",
                "time": 1,
                "observation": {
                    "kind": "invocation_start",
                    "requestId": "wrong-hook-request",
                    "ownerRequestId": "wrong-hook-request",
                    "executionArn": "wrong-execution",
                },
            },
        ]
    )
    assert records[0]["requestId"] == records[0]["ownerRequestId"] == "outer"
    assert records[0]["executionArn"] == "execution"
    assert records[1]["kind"] == "observation_error"
    assert records[1]["requestId"] == records[1]["ownerRequestId"] == "outer"
    assert records[1]["executionArn"] == "execution"
    assert "requestId, ownerRequestId, executionArn" in records[1]["message"]


def test_late_original_stream_is_rejected_across_refreshes(cloud):
    item = cloud.start("replay", role="victim", resume_roles=("resume",))
    refresh_entries(cloud, enter(item, "retry", 2))
    with pytest.raises(CollectionError, match="chronology"):
        refresh_entries(cloud, enter(item, "original", 1))
    assert cloud.case_context.attempts[0]["requestId"] == "retry"
    assert "original" not in cloud.request_roles
    with pytest.raises(CollectionError, match="chronology"):
        cloud.refresh()


def test_later_retry_remains_bindable_across_refreshes(cloud):
    item = cloud.start("replay", role="victim", resume_roles=("resume",))
    refresh_entries(cloud, enter(item, "original", 1))
    refresh_entries(cloud, enter(item, "retry", 2))
    assert [(item["role"], item["requestId"]) for item in cloud.case_context.attempts] == [
        ("victim", "original"),
        ("resume", "retry"),
    ]


def test_failed_retirement_fences_a_second_run_with_terminal_execution_and_live_task(tmp_path):
    from aws_durable_execution_conformance_tests.lmi.quarantine import fence_key, require_unfenced

    objects: dict[str, bytes] = {}

    class Storage:
        def get_object(self, *, Bucket, Key):
            if Key not in objects:
                raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
            return {"Body": io.BytesIO(objects[Key]), "ETag": '"fence-etag"'}

        def put_object(self, *, Bucket, Key, Body, IfNoneMatch):
            assert IfNoneMatch == "*"
            assert Key not in objects
            objects[Key] = Body
            return {"ResponseMetadata": {"RetryAttempts": 0}}

        def delete_object(self, *, Bucket, Key, IfMatch):
            assert IfMatch == '"fence-etag"'
            del objects[Key]

    storage = Storage()

    class PersistentCloud(FakeCloud):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.s3 = storage

    valid = replace(PROFILE, configuration_id=configuration_identity(deployment(), "c1"))
    first = adapter(tmp_path / "first", factory=PersistentCloud)
    asyncio.run(first.prepare(valid))
    # The durable execution is terminal, but its root task has not exited.
    driver, events = settled_cloud()
    task_exit = next(event for event in events if event.get("observation", {}).get("kind") == "task_exit")
    events.remove(task_exit)
    seal_fixture_events(events)
    assert not driver.quiescent()
    asyncio.run(first.finish(valid, quiescent=driver.quiescent()))
    record = json.loads(objects[fence_key(FUNCTION)])
    assert record["runId"] == "run"
    assert record["evidencePrefix"] == "runs/run/"
    second = adapter(tmp_path / "second", factory=PersistentCloud)
    second.deployment["run"] = "second"
    second_valid = replace(valid, configuration_id=configuration_identity(second.deployment, "c1"))
    with pytest.raises(ProvisioningError, match="quarantined by run run"):
        asyncio.run(second.prepare(second_valid))
    with pytest.raises(ProvisioningError, match="reconcile its runtime work"):
        require_unfenced(storage, deployment()["bucket"], FUNCTION)
    assert fence_key(FUNCTION) in objects
    # A positive audited retirement in the owning run permits reuse.
    events.append(task_exit)
    seal_fixture_events(events)
    assert driver.quiescent()
    asyncio.run(first.finish(valid, quiescent=driver.quiescent()))
    asyncio.run(second.prepare(second_valid))
    assert json.loads(objects[fence_key(FUNCTION)])["runId"] == "second"


def test_undispatched_rejection_does_not_block_a_quiescent_runs_persistent_fence_release(tmp_path):
    from aws_durable_execution_conformance_tests.lmi.quarantine import fence_key

    objects: dict[str, bytes] = {}

    class Storage:
        def get_object(self, *, Bucket, Key):
            if Key not in objects:
                raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
            return {"Body": io.BytesIO(objects[Key]), "ETag": '"fence-etag"'}

        def put_object(self, *, Bucket, Key, Body, IfNoneMatch):
            assert IfNoneMatch == "*"
            assert Key not in objects
            objects[Key] = Body
            return {"ResponseMetadata": {"RetryAttempts": 0}}

        def delete_object(self, *, Bucket, Key, IfMatch):
            assert IfMatch == '"fence-etag"'
            del objects[Key]

    storage = Storage()

    class PersistentCloud(FakeCloud):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.s3 = storage

    valid = replace(PROFILE, configuration_id=configuration_identity(deployment(), "c1"))
    first = adapter(tmp_path / "first", factory=PersistentCloud)
    asyncio.run(first.prepare(valid))

    # A fully quiescent run, except the service also rejected a second native
    # request before any handler ran. That proven rejection must not keep the
    # physical fence any more than a normal quiescent run would.
    driver, _events = settled_cloud()
    driver.cloud.raw_histories["execution"] = [
        {
            "EventType": "InvocationCompleted",
            "EventId": 4,
            "InvocationCompletedDetails": {
                "RequestId": "rejected",
                "StartTimestamp": 0.1,
                "EndTimestamp": 0.5,
                "Error": {
                    "Payload": {"ErrorType": "TooManyRequestsException", "ErrorMessage": "capacity"},
                    "Truncated": False,
                },
            },
        }
    ]
    assert driver.quiescent()
    asyncio.run(first.finish(valid, quiescent=driver.quiescent()))
    assert fence_key(FUNCTION) not in objects

    second = adapter(tmp_path / "second", factory=PersistentCloud)
    asyncio.run(second.prepare(valid))


def test_quiescent_finish_cannot_clear_a_foreign_run_fence(tmp_path):
    from aws_durable_execution_conformance_tests.lmi.quarantine import release_fence

    storage = Mock()
    storage.get_object.return_value = {
        "Body": io.BytesIO(
            json.dumps(
                {"resourceId": FUNCTION, "runId": "foreign", "commit": "sdk-sha", "token": "foreign-token"}
            ).encode()
        ),
        "ETag": '"fence-etag"',
    }
    with pytest.raises(ProvisioningError, match="unverified persistent retirement fence"):
        release_fence(storage, deployment(), FUNCTION, "owned-token")
    storage.delete_object.assert_not_called()


def test_retried_profile_fence_persists_token_without_verifying_or_deleting(tmp_path):
    from aws_durable_execution_conformance_tests.lmi.artifacts import case_key

    class RetriedCloud(FakeCloud):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.s3.put_object.return_value = {"ResponseMetadata": {"RetryAttempts": 1}}

    value = adapter(tmp_path, factory=RetriedCloud)
    valid = replace(PROFILE, configuration_id=configuration_identity(deployment(), "c1"))
    with pytest.raises(ProvisioningError, match="single-attempt response"):
        asyncio.run(value.prepare(valid))
    cloud = FakeCloud.instances[-1]
    journal = value.artifacts_dir / "configuration" / f"{case_key(FUNCTION)}-fence.json"
    saved = json.loads(journal.read_text())
    sent = json.loads(cloud.s3.put_object.call_args.kwargs["Body"])
    assert saved["token"] == sent["token"]
    assert saved["state"] == "ACQUIRING"
    assert "verify" not in cloud.actions
    asyncio.run(value.finish(valid, quiescent=True))
    cloud.s3.delete_object.assert_not_called()
    assert journal.is_file()


@pytest.mark.parametrize(
    "error,single_attempt,settled",
    [
        (NoCredentialsError(), False, True),
        (ParamValidationError(report="bad"), False, True),
        (CredentialRetrievalError(provider="sso", error_msg="expired"), True, True),
        (CredentialRetrievalError(provider="sso", error_msg="expired"), False, False),
    ],
)
def test_never_sent_profile_fence_put_settles_its_journal(tmp_path, error, single_attempt, settled):
    from aws_durable_execution_conformance_tests.lmi import runner
    from aws_durable_execution_conformance_tests.lmi.artifacts import case_key

    class UnsentCloud(FakeCloud):
        single_attempt_fences = single_attempt

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.s3.put_object.side_effect = error

    value = adapter(tmp_path, factory=UnsentCloud)
    valid = replace(PROFILE, configuration_id=configuration_identity(deployment(), "c1"))
    with pytest.raises(type(error)):
        asyncio.run(value.prepare(valid))
    cloud = FakeCloud.instances[-1]
    cloud.s3.put_object.assert_called_once()
    assert "verify" not in cloud.actions
    journal = value.artifacts_dir / "configuration" / f"{case_key(FUNCTION)}-fence.json"
    assert journal.is_file() is not settled
    errors = runner._recover_profile_fences(
        deployment(), value.artifacts_dir, {"c1": {"resource_id": FUNCTION}}, cloud.s3
    )
    if settled:
        # Offline cleanup finds nothing to reconcile, and a new attempt can reserve the profile again.
        assert errors == []
        asyncio.run(adapter(tmp_path).prepare(valid))
    else:
        # A retrying client may have sent an earlier attempt, so the outcome stays unresolved.
        assert json.loads(journal.read_text())["state"] == "ACQUIRING"
        assert errors == [f"{FUNCTION}: CollectionError: Profile fence acquisition outcome is unresolved"]
    cloud.s3.delete_object.assert_not_called()


def test_attempt_audit_failure_does_not_mark_invocation_submitted(cloud):
    def reject(_):
        (intent,) = (cloud.artifacts_dir / "invocations").glob("*.json")
        assert json.loads(intent.read_text())["dispatchState"] == "PREPARED"
        raise OSError("audit failed")

    cloud.case_context._on_attempt = reject
    with pytest.raises(CollectionError, match="invocation API failed"):
        cloud.start("fixture", role="victim")
    (intent,) = (cloud.artifacts_dir / "invocations").glob("*.json")
    assert json.loads(intent.read_text())["dispatchState"] == "PREPARED"
    assert cloud.case_context.attempts == []
    assert cloud.invocations == []
    cloud.lam.invoke.assert_not_called()


def test_profile_journal_race_preserves_other_writer_receipt(tmp_path, monkeypatch):
    actual_save = adapter_module.save

    def raced(path, value, **kwargs):
        if kwargs.get("exclusive"):
            actual_save(path, {**value, "token": "other"})
        return actual_save(path, value, **kwargs)

    monkeypatch.setattr(adapter_module, "save", raced)
    value = adapter(tmp_path)
    valid = replace(PROFILE, configuration_id=configuration_identity(deployment(), "c1"))
    with pytest.raises(ProvisioningError, match="Existing profile fence journal"):
        asyncio.run(value.prepare(valid))
    (receipt,) = tmp_path.rglob("*-fence.json")
    assert json.loads(receipt.read_text())["token"] == "other"
    FakeCloud.instances[-1].s3.put_object.assert_not_called()


def test_rejected_first_hold_leaves_only_prepared_intent(tmp_path):
    storage, function = Mock(), Mock()
    storage.put_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied"}, "ResponseMetadata": {"RetryAttempts": 0}}, "PutObject"
    )

    def factory(*args, **kwargs):
        return cloud_module.Cloud(
            *args,
            **kwargs,
            verify_profile=Mock(),
            client_factory=lambda service, _: storage if service == "s3" else function,
        )

    value = adapter(tmp_path, factory=factory)
    ctx = context()

    async def exercise():
        with pytest.raises(ClientError, match="AccessDenied"):
            await value.start(ctx)
        assert await value.retire(ctx)
        assert ctx.attempts == []
        (intent,) = tuple(tmp_path.rglob("invocations/*.json"))
        assert json.loads(intent.read_text())["dispatchState"] == "PREPARED"
        function.invoke.assert_not_called()
        storage.delete_object.assert_not_called()

    asyncio.run(exercise())


def test_profile_acquisition_lock_blocks_parallel_cleanup_before_admission(tmp_path):
    from aws_durable_execution_conformance_tests.lmi import runner

    profile = replace(PROFILE, configuration_id=configuration_identity(deployment(), "c1"))

    class ObservedCloud(FakeCloud):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

            def put(**_):
                errors = runner._recover_profile_fences(
                    deployment(),
                    value.artifacts_dir,
                    {profile.profile_id: {"resource_id": FUNCTION}},
                    self.s3,
                )
                assert len(errors) == 1 and "Journal ownership is busy" in errors[0]
                self.s3.delete_object.assert_not_called()
                return {"ResponseMetadata": {"RetryAttempts": 0}}

            self.s3.put_object.side_effect = put

    value = adapter(tmp_path, factory=ObservedCloud)
    asyncio.run(value.prepare(profile))
    assert FakeCloud.instances[-1].actions == ["verify"]
