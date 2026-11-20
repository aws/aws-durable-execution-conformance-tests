# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Failed observations stay failed; only independently proven cleanup permits reuse."""

import asyncio
from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from runpy import run_path
from typing import Any

import pytest

from aws_durable_execution_conformance_tests.lmi.coordinator import (
    Budgets,
    Profile,
    QuarantinedError,
    ResourceScheduler,
    Scenario,
    ScenarioContext,
    ScenarioResult,
    _event_frames_order_changed,
    run_scenarios,
)
from aws_durable_execution_conformance_tests.lmi.evidence import (
    EvidenceValidator,
    same_json,
    validate_common_evidence,
    validate_evidence,
    validate_retirement_evidence,
)
from aws_durable_execution_conformance_tests.lmi.offline import _cleanup_behavior_errors

PROFILE = Profile("native-1", "arn:function:c1", 1, 1, "sdk@sha256:tested", "sha256:configuration")
ORIGINAL_FAILURE = "ASSERTION: original SDK behavior failed"
PHASE_FAILURE = "An expected cohort member was not observed"


def _resequence(value: dict[str, Any]) -> None:
    for request in value["attempts"]:
        local = sorted(
            (event for event in value["events"] if event["requestId"] == request["requestId"]),
            key=lambda event: event["atMs"],
        )
        for sequence, event in enumerate(local):
            event["sequence"] = sequence


def _snapshot(context: ScenarioContext) -> dict[str, Any]:
    """Language-neutral physical retirement evidence, independent of SDK rules."""
    name = context.scenario.case_id
    request_id, arn, gate = f"request:{name}:victim", f"arn:execution:{name}:victim", f"gate:{name}"
    timed_out = context.scenario.scenario == "timed-out-cohort"
    wrapper_end = 660 if timed_out else 1000
    status = "TIMED_OUT" if timed_out else "SUCCEEDED"
    owner = {"requestId": request_id, "ownerRequestId": request_id, "executionArn": arn}
    events: list[dict[str, Any]] = []

    def event(kind: str, when: int, **details: Any) -> None:
        events.append({**owner, "kind": kind, "atMs": when, **details})

    event("wrapper_enter", 100)
    event("task_start", 101, taskId="root")
    event("io_start", 190, taskId=gate, gateId=gate)
    event("blocked", 200, gateId=gate)
    event("effect", 220, gateId=gate)
    event("fault_released", 630 if timed_out else 790, gateId=gate, mode="normal")
    event("io_exit", 640 if timed_out else 800, taskId=gate, gateId=gate)
    event("task_exit", 650 if timed_out else 999, taskId="root")
    event(
        "wrapper_raise" if timed_out else "wrapper_return", wrapper_end, outcome="FAILED" if timed_out else "SUCCEEDED"
    )
    if timed_out:
        event("platform_timeout", 600, source="platform")
    resources = [
        {"id": request_id, "kind": "invocation", "ownerCaseId": name},
        {"id": arn, "kind": "execution", "ownerCaseId": name},
    ]
    value = {
        "schemaVersion": 1,
        "runId": context.run_id,
        "caseId": name,
        "requirementId": context.scenario.requirement_id,
        "coverageProfile": context.scenario.coverage_profile,
        "runtime": "javascript",
        "artifact": {"sdk": context.profile.artifact_id, "conformance": "tested-validator"},
        "profile": {
            "functionArn": context.profile.resource_id,
            "configurationDigest": context.profile.configuration_id,
            "nativeConcurrency": 1,
            "timeoutMs": 500,
        },
        "attempts": [
            {
                "requestId": request_id,
                "executionArn": arn,
                "environmentId": "environment",
                "workerId": "worker",
                "processId": "process",
                "role": "victim",
                "attempt": 1,
                "startedAtMs": 100,
                "endedAtMs": 600 if timed_out else wrapper_end,
                "outcome": status,
            }
        ],
        "selectedRequests": {"victim": request_id},
        "events": events,
        "sdkRequests": [],
        "histories": [{"executionArn": arn, "ownerCaseId": name, "status": status, "operations": []}],
        "observationEndedAtMs": 20000,
        "collection": {
            "watermarks": {key: 20000 for key in ("lifecycle", "sdkRequests", "history")},
            "evidenceUris": ["memory://retained-raw-evidence"],
        },
        "resourcesCreated": resources,
        "faultsCreated": [{"id": gate, "ownerCaseId": name}],
        "retirement": {
            "retiredAtMs": 21000,
            "resources": [
                {
                    **resource,
                    "settledAtMs": 20900,
                    "evidenceUri": "memory://retirement-source",
                    "outcome": "QUIESCENT"
                    if resource["kind"] == "invocation"
                    else status
                    if resource["kind"] == "execution"
                    else "succeeded",
                }
                for resource in resources
            ],
            "faults": [{"id": gate, "ownerCaseId": name, "releasedAtMs": 630 if timed_out else 790, "mode": "normal"}],
        },
    }
    _resequence(value)
    return value


def _case(name: str, *, retry: bool = False) -> Scenario:
    return Scenario(
        name,
        "fixture-check",
        PROFILE.profile_id,
        "timed-out-cohort" if retry else "completed-cohort",
        roles=("victim", "retry") if retry else ("victim",),
        exclusive=True,
        coverage_profile="fixture-suite",
    )


def _validator(evidence: Mapping[str, Any], *, phase: str = "final") -> list[str]:
    # The behavior finding is deliberately identical in both phases. It must
    # never suppress a separate check of whether the owned work actually ended.
    return list(evidence.get("behaviorErrors", []))


def _service_fact_validator(evidence: Mapping[str, Any], *, phase: str = "final") -> list[str]:
    return validate_common_evidence(
        evidence, phase="observations" if phase == "observations" else "final", requirement_ids=("fixture-check",)
    ) + _validator(evidence, phase=phase)


class FailureAdapter:
    def __init__(self, *, phase: str = "execute", damage: str | None = None) -> None:
        self.phase = phase
        self.damage = damage
        self.steps: list[tuple[str, str]] = []
        self.started: list[str] = []
        self.finished: list[bool] = []
        self.snapshots: dict[str, dict[str, Any]] = {}

    async def prepare(self, profile: Profile) -> None:
        pass

    async def finish(self, profile: Profile, *, quiescent: bool) -> None:
        self.finished.append(quiescent)

    async def start(self, context: ScenarioContext) -> None:
        name = context.scenario.case_id
        self.started.append(name)
        self.snapshots[name] = _snapshot(context)
        for attempt in self.snapshots[name]["attempts"]:
            context.record_attempt(attempt["requestId"], role=attempt["role"], requestId=attempt["requestId"])
        if name == "failed" and self.damage == "unknown-launch":
            context.record_attempt("transport-without-native-response", role="victim")

    async def place(self, context: ScenarioContext) -> None:
        if context.scenario.case_id == "failed" and self.phase == "placement":
            raise RuntimeError(PHASE_FAILURE)

    async def execute(self, context: ScenarioContext) -> None:
        if context.scenario.case_id == "failed" and self.phase == "execute":
            raise RuntimeError(PHASE_FAILURE)

    def observations(self, context: ScenarioContext) -> dict[str, Any]:
        observed = deepcopy(self.snapshots[context.scenario.case_id])
        observed.pop("retirement")
        observed["capturedBeforeCleanup"] = True
        observed["behaviorErrors"] = [ORIGINAL_FAILURE] if context.scenario.case_id == "failed" else []
        return observed

    async def collect(self, context: ScenarioContext) -> Mapping[str, Any]:
        self.steps.append((context.scenario.case_id, "collect"))
        if context.scenario.case_id == "failed" and self.phase == "collection":
            raise RuntimeError(PHASE_FAILURE)
        return self.observations(context)

    async def collect_partial(self, context: ScenarioContext) -> Any:
        self.steps.append((context.scenario.case_id, "partial"))
        if self.damage == "malformed-partial-object":
            return []
        observed = self.observations(context)
        if self.damage == "malformed-partial-attempts":
            observed["attempts"] = None
        return observed

    async def release(self, context: ScenarioContext) -> None:
        self.steps.append((context.scenario.case_id, "release"))
        context.evidence["capturedBeforeCleanup"] = False
        context.evidence["behaviorErrors"] = []

    async def retire(self, context: ScenarioContext) -> bool:
        self.steps.append((context.scenario.case_id, "retire"))
        final = deepcopy(self.snapshots[context.scenario.case_id])
        context.evidence["retirement"] = deepcopy(final["retirement"])
        if context.scenario.case_id == "failed":
            self.damage_retirement(final)
        if self.damage == "missing-audit" and context.scenario.case_id == "failed":
            return True
        if self.damage == "malformed-final" and context.scenario.case_id == "failed":
            context.retirement_evidence = []  # type: ignore[assignment]
        else:
            context.retirement_evidence = final
        return True

    def damage_retirement(self, final: dict[str, Any]) -> None:
        if self.damage in {"missing-io-exit", "missing-root-exit"}:
            kind = {
                "missing-io-exit": "io_exit",
                "missing-root-exit": "task_exit",
            }[self.damage]
            final["events"] = [event for event in final["events"] if event["kind"] != kind]
        elif self.damage == "dropped-known-start":
            final["events"] = [event for event in final["events"] if event["kind"] not in {"io_start", "io_exit"}]
        elif self.damage == "dropped-known-fault":
            final["faultsCreated"] = []
            final["retirement"]["faults"] = []
        elif self.damage == "active-execution":
            final["histories"][0]["status"] = "RUNNING"
        elif self.damage == "reversed-equal-time-pair":
            start = next(event for event in final["events"] if event["kind"] == "io_start")
            end = next(event for event in final["events"] if event["kind"] == "io_exit")
            end["atMs"] = start["atMs"]
            _resequence(final)
            start["sequence"], end["sequence"] = end["sequence"], start["sequence"]


def _run(
    adapter: FailureAdapter, *, retry: bool = False, validator: EvidenceValidator = _validator
) -> tuple[list[ScenarioResult], ResourceScheduler]:
    scheduler = ResourceScheduler()
    cases = [_case("failed", retry=retry), _case("next")]
    results = asyncio.run(
        run_scenarios(cases, [PROFILE], adapter, run_id="run", validator=validator, scheduler=scheduler)
    )
    return results, scheduler


@pytest.mark.parametrize("dispatch", ["PREPARED", "REJECTED", "SUBMITTED", "MIXED"])
@pytest.mark.parametrize("standard_replay", [False, True])
def test_controls_only_audit_reconciles_original_launch_journal(dispatch: str, standard_replay: bool) -> None:
    from aws_durable_execution_conformance_tests.lmi.artifacts import case_key

    class ControlsOnlyAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            if context.scenario.case_id != "failed":
                await super().start(context)
                return
            self.started.append("failed")
            final = _snapshot(context)
            rejected = {
                "marker": "fixture",
                "functionArn": context.profile.resource_id,
                "durableExecutionName": "fixture-name",
                "dispatchState": "REJECTED",
                "rejectionCode": "AccessDeniedException",
                "retryAttempts": 0,
            }
            if dispatch in ("REJECTED", "MIXED"):
                context.record_attempt("fixture", role="victim", source="driver-invoke", **rejected)
            if dispatch in ("SUBMITTED", "MIXED"):
                context.record_attempt("uncertain", role="victim", dispatchState="SUBMITTED")
            undispatched = (
                [
                    {key: value for key, value in rejected.items() if key not in {"rejectionCode", "retryAttempts"}}
                    | {"dispatchState": "PREPARED"}
                ]
                if dispatch == "PREPARED"
                else [rejected]
                if dispatch in ("REJECTED", "MIXED")
                else []
            )
            scope = case_key(context.scenario.case_id)
            uri = f"file:///local/artifacts/cases/{scope}/control-records.json"
            control = {
                "kind": "control",
                "id": f"s3://bucket/runs/{context.run_id}/cases/{scope}/control/release-all",
                "ownerCaseId": context.scenario.case_id,
                "gate": "release-all",
            }
            for key in ("attempts", "events", "histories", "sdkRequests", "faultsCreated"):
                final[key] = []
            final["selectedRequests"] = {}
            final["collection"] = {"watermarks": {}, "evidenceUris": [uri]}
            final["resourcesCreated"] = [control]
            final["controlRecords"] = [
                {"gate": "release-all", "action": action, "state": "ACKNOWLEDGED", "started": at - 0.1, "time": at}
                for action, at in (("hold", 1), ("release", 2), ("delete", 3))
            ]
            final["retirement"].update(
                mode="controls-only",
                undispatchedLaunches=undispatched,
                faults=[],
                resources=[{**control, "outcome": "DELETED", "settledAtMs": 3000, "evidenceUri": uri}],
            )
            self.snapshots["failed"] = final
            raise RuntimeError(PHASE_FAILURE)

    adapter = ControlsOnlyAdapter()
    if standard_replay:
        scheduler = ResourceScheduler()
        case = Scenario(
            "failed",
            "lmi-lifecycle-5",
            PROFILE.profile_id,
            "suspension-retry",
            roles=("victim", "resume"),
            exclusive=True,
            coverage_profile="standard",
        )
        results = asyncio.run(
            run_scenarios(
                [case, _case("next")], [PROFILE], adapter, run_id="run", validator=_validator, scheduler=scheduler
            )
        )
    else:
        results, scheduler = _run(adapter)
    failed, following = results
    reusable = dispatch in ("PREPARED", "REJECTED")
    assert any(PHASE_FAILURE in error for error in failed.errors)
    assert failed.status == "INFRASTRUCTURE_ERROR"
    assert (scheduler.quarantine_reason(PROFILE.resource_id) is None) is reusable
    assert following.status == ("PASSED" if reusable else "INFRASTRUCTURE_ERROR"), failed.errors
    assert adapter.finished == [reusable]
    if dispatch == "REJECTED":
        assert failed.attempts[0]["dispatchState"] == "REJECTED"


@pytest.mark.parametrize("retry_attempts", [0, 1])
def test_runtime_retirement_ignores_only_proven_rejected_peer_attempts(retry_attempts: int) -> None:
    class RejectedPeerAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            if context.scenario.case_id == "failed":
                context.record_attempt(
                    "peer-marker",
                    role="peer",
                    marker="peer-marker",
                    source="driver-invoke",
                    functionArn=context.profile.resource_id,
                    durableExecutionName="peer-name",
                    dispatchState="REJECTED",
                    rejectionCode="AccessDeniedException",
                    retryAttempts=retry_attempts,
                )
                raise RuntimeError(PHASE_FAILURE)

    adapter = RejectedPeerAdapter()
    scheduler = ResourceScheduler()
    first = replace(_case("failed"), roles=("victim", "peer"))
    failed, following = asyncio.run(
        run_scenarios(
            [first, _case("next")], [PROFILE], adapter, run_id="run", validator=_validator, scheduler=scheduler
        )
    )
    assert failed.status == "INFRASTRUCTURE_ERROR"
    assert any(PHASE_FAILURE in error for error in failed.errors)
    assert failed.attempts[-1]["dispatchState"] == "REJECTED"
    assert following.status == ("PASSED" if retry_attempts == 0 else "INFRASTRUCTURE_ERROR"), failed.errors
    assert (scheduler.quarantine_reason(PROFILE.resource_id) is None) is (retry_attempts == 0)


def test_driver_incomplete_observation_window_does_not_quarantine_a_drained_target() -> None:
    from types import SimpleNamespace
    from typing import cast
    from unittest.mock import patch

    from aws_durable_execution_conformance_tests.lmi.cloud import Cloud
    from aws_durable_execution_conformance_tests.lmi.source_evidence import build_evidence

    class PendingWindowAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            baseline = super().observations(context)
            histories = baseline["histories"]
            histories[0]["result"] = None
            self.snapshots[context.scenario.case_id]["histories"][0]["result"] = None
            expected = {histories[0]["executionArn"]: {"status": "SUCCEEDED", "result": None}}
            if context.scenario.case_id != "failed":
                return {**baseline, "expectedHistories": expected}

            cloud = SimpleNamespace(
                observation_ended_at=20,
                source_uris={"memory://raw"},
                manifest={"invocationTimeout": 0.5, "conformanceRevision": "tested-validator"},
                control_records=[],
                invocations=[
                    {"arn": histories[0]["executionArn"], "native_requests": [baseline["attempts"][0]["requestId"]]}
                ],
                events={},
                raw_histories={},
                terminal_histories=set(),
                _save=lambda *args: None,
            )
            events = [event for event in baseline["events"] if event["kind"] != "io_exit"]
            events.extend(
                {
                    **events[0],
                    "kind": kind,
                    "atMs": at,
                    "callId": "closed-call",
                    **extra,
                }
                for kind, at, extra in (
                    ("checkpoint_call", 1100, {"outcome": "REJECTED"}),
                    ("checkpoint_exit", 1200, {}),
                )
            )
            self.snapshots[context.scenario.case_id]["events"].extend(deepcopy(events[-2:]))
            _resequence(self.snapshots[context.scenario.case_id])
            sources = SimpleNamespace(
                observations=lambda **_: (events, []),
                histories=lambda **_: (histories, expected, []),
                attempts=lambda _, **kwargs: (baseline["attempts"], baseline["selectedRequests"], []),
            )
            with patch(
                "aws_durable_execution_conformance_tests.lmi.source_evidence.CaseSources.capture", return_value=sources
            ):
                result = build_evidence(
                    context,
                    cast(Cloud, cloud),
                    runtime="javascript",
                    expected_outcome=lambda *_: {},
                    worker_identity=lambda _: "",
                )
            self.snapshots[context.scenario.case_id]["sdkRequests"] = deepcopy(result["sdkRequests"])
            return result

    adapter = PendingWindowAdapter(phase="assertion")
    first_and_following, scheduler = _run(adapter, validator=_service_fact_validator)
    first, following = first_and_following
    assert first.evidence["collectionErrors"] == []
    assert first.evidence["collection"]["pending"]
    assert first.evidence["collection"]["watermarks"] == {}
    assert first.evidence["sdkRequests"][0]["outcome"] == "REJECTED"
    assert first.status == "INCOMPLETE_EVIDENCE", first.errors
    assert any("INCOMPLETE: lifecycle collection does not cover" in error for error in first.errors)
    assert following.status == "PASSED", " | ".join(first.errors)
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None
    assert adapter.finished == [True]


def _add_retained_resource(value: dict[str, Any], kind: str, outcome: str) -> None:
    resource = {"id": f"{kind}:{value['caseId']}", "kind": kind, "ownerCaseId": value["caseId"]}
    value["resourcesCreated"].append(resource)
    value["retirement"]["resources"].append(
        {**resource, "settledAtMs": 20900, "outcome": outcome, "evidenceUri": "memory://provider-completion"}
    )


def _complete_fixture_sdk_calls(value: dict[str, Any]) -> None:
    value["events"] = [
        event for event in value["events"] if not str(event.get("callId", "")).startswith("fixture-dispatch-")
    ]
    for index, request in enumerate(value["sdkRequests"]):
        if request["outcome"] != "DISPATCHED":
            continue
        request["callId"] = f"fixture-dispatch-{index}"
        kind = "poll" if request["method"] == "GetDurableExecutionState" else "checkpoint"
        value["events"].extend([{**request, "kind": f"{kind}_call"}, {**request, "kind": f"{kind}_exit"}])
    _resequence(value)


def _standard_retirement_snapshot(factory: str, runtime: str = "javascript", *, requirement: int = 2) -> dict[str, Any]:
    fixtures = run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))
    value = (
        fixtures[factory](requirement, runtime)
        if factory == "evidence"
        else fixtures[factory]()
        if factory in ("replay_evidence", "failure_evidence")
        else fixtures[factory](runtime=runtime)
    )
    for attempt in value["attempts"]:
        owner = {key: attempt[key] for key in ("requestId", "executionArn")}
        owner["ownerRequestId"] = attempt["requestId"]
        local = [event for event in value["events"] if event["requestId"] == attempt["requestId"]]
        value["events"].extend(
            [
                {**owner, "kind": "wrapper_enter", "atMs": attempt["startedAtMs"]},
                {**owner, "kind": "task_start", "taskId": "root", "atMs": attempt["startedAtMs"] + 0.1},
            ]
        )
        exits = [event for event in local if event["kind"] == "task_exit"]
        if exits:
            for event in exits:
                event["taskId"] = "root"
        else:
            value["events"].append({**owner, "kind": "task_exit", "taskId": "root", "atMs": attempt["endedAtMs"] - 0.1})
        if not any(event["kind"] == "wrapper_return" for event in local):
            value["events"].append({**owner, "kind": "wrapper_return", "atMs": attempt["endedAtMs"]})
    _complete_fixture_sdk_calls(value)
    assert validate_evidence(value) == []
    assert validate_retirement_evidence(value) == []
    return value


def _run_standard_cleanup_discovery(
    value: dict[str, Any],
    mutate: Callable[[dict[str, Any]], None],
    *,
    phase: str,
    path: str,
    originally_failed: bool = False,
    quiescent: bool = True,
    observe_retirement: bool = False,
    retirement_valid: bool = True,
    service_resume: bool = False,
) -> ScenarioResult:
    deployment = value["profile"]
    profile = Profile(
        "standard",
        deployment["functionArn"],
        deployment["nativeConcurrency"],
        deployment["nativeConcurrency"],
        value["artifact"]["sdk"],
        deployment["configurationDigest"],
        recovery_policy=deployment.get("recoveryPolicy"),
    )
    case = Scenario(
        value["caseId"],
        value["requirementId"],
        profile.profile_id,
        "fixture",
        roles=tuple(value["selectedRequests"]),
        exclusive=True,
        coverage_profile=value.get("coverageProfile", "standard"),
        behavior_budgets=value.get("budgets", {}),
    )
    finished: list[bool] = []

    class Adapter:
        async def prepare(self, profile: Profile) -> None:
            pass

        async def finish(self, profile: Profile, *, quiescent: bool) -> None:
            finished.append(quiescent)

        async def start(self, context: ScenarioContext) -> None:
            for attempt in value["attempts"]:
                if service_resume and attempt["role"] == "resume":
                    continue
                context.record_attempt(
                    attempt["requestId"],
                    role=attempt["role"],
                    requestId=attempt["requestId"],
                    executionArn=attempt["executionArn"],
                )

        async def place(self, context: ScenarioContext) -> None:
            pass

        async def execute(self, context: ScenarioContext) -> None:
            pass

        async def collect(self, context: ScenarioContext) -> Mapping[str, Any]:
            if service_resume:
                original = value["selectedRequests"]["victim"]
                resume = next(attempt for attempt in value["attempts"] if attempt["role"] == "resume")
                with pytest.raises(ValueError, match="existing journaled execution"):
                    context.record_service_attempt(
                        resume["requestId"], original_request_id=original, execution_arn="unrelated", role="resume"
                    )
                context.record_service_attempt(
                    resume["requestId"],
                    original_request_id=original,
                    execution_arn=resume["executionArn"],
                    role="resume",
                )
            observed = deepcopy(value)
            if not observe_retirement:
                observed.pop("retirement")
            assert bool(validator(observed, phase="observations")) is originally_failed
            return observed

        async def release(self, context: ScenarioContext) -> None:
            if phase in ("release", "both"):
                mutate(context.evidence)

        async def retire(self, context: ScenarioContext) -> bool:
            if phase in ("retire", "both"):
                mutate(context.evidence)
            final = deepcopy(context.evidence)
            if phase == "snapshot":
                mutate(final)
            final["retirement"] = deepcopy(value["retirement"])
            if phase == "retirement":
                mutate(final)
            _resequence(final)
            if path == "independent":
                context.retirement_evidence = final
            else:
                context.evidence = final
            context.evidence["retirement"] = deepcopy(value["retirement"])
            return True

    def validator(evidence: Mapping[str, Any], *, phase: str = "final") -> list[str]:
        if value.get("coverageProfile", "standard") != "standard":
            return validate_common_evidence(
                evidence,
                phase="observations" if phase == "observations" else "final",
                requirement_ids=(value["requirementId"],),
            )
        return validate_evidence(evidence, phase="observations" if phase == "observations" else "final")

    scheduler = ResourceScheduler()

    async def exercise() -> ScenarioResult:
        results = await run_scenarios(
            [case], [profile], Adapter(), run_id=value["runId"], validator=validator, scheduler=scheduler
        )
        # A conformance failure is distinct from residual physical work.
        if quiescent:
            lease = await scheduler.acquire(profile.resource_id, "subsequent-case", exclusive=True, timeout=0.1)
            await lease.release(quiescent=True)
        else:
            with pytest.raises(QuarantinedError):
                await scheduler.acquire(profile.resource_id, "subsequent-case", exclusive=True, timeout=0.1)
        return results[0]

    result = asyncio.run(exercise())
    assert finished == [quiescent]
    assert (validate_retirement_evidence(result.retirement_evidence or result.evidence) == []) is retirement_valid
    return result


@pytest.mark.parametrize(
    ("phase", "path"), [("release", "normal"), ("retire", "normal"), ("retirement", "independent")]
)
@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        (("schemaVersion",), 2),
        (("schemaVersion",), True),
        (("observationEndedAtMs",), False),
        (("observationEndedAtMs",), -1),
        (("observationEndedAtMs",), float("nan")),
        (("observationEndedAtMs",), float("inf")),
        (("collection",), None),
        (("collection", "watermarks"), []),
        (("collection", "watermarks", "lifecycle"), False),
        (("collection", "watermarks", "history"), -1),
        (("collection", "evidenceUris"), None),
        (("collection", "evidenceUris"), [False]),
    ],
)
def test_cleanup_rejects_supplied_malformed_envelope_metadata(
    phase: str, path: str, field: tuple[str, ...], invalid: Any
) -> None:
    value = _standard_retirement_snapshot("evidence")

    def mutate(snapshot: dict[str, Any]) -> None:
        target = snapshot
        for key in field[:-1]:
            target = target[key]
        target[field[-1]] = invalid

    result = _run_standard_cleanup_discovery(
        value, mutate, phase=phase, path=path, quiescent=False, retirement_valid=path == "normal"
    )
    assert result.status == "INFRASTRUCTURE_ERROR", result.errors
    assert any(".".join(field) in error for error in result.errors), result.errors
    assert result.evidence["schemaVersion"] == 1
    assert result.evidence["observationEndedAtMs"] == 200


@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize("field", ["schemaVersion", "observationEndedAtMs", "collection"])
def test_cleanup_envelope_correction_cannot_erase_malformed_metadata(path: str, field: str) -> None:
    value = _standard_retirement_snapshot("evidence")
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        snapshot[field] = None if calls == 1 else deepcopy(value[field])

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path=path, quiescent=False)
    assert result.status == "INFRASTRUCTURE_ERROR", result.errors
    assert any(field in error for error in result.errors), result.errors
    assert result.cleanup_evidence["release"][0][field] is None


@pytest.mark.parametrize(
    ("phase", "path"), [("release", "normal"), ("retire", "normal"), ("retirement", "independent")]
)
def test_retirement_must_cover_metadata_only_cleanup_window(phase: str, path: str) -> None:
    value = _standard_retirement_snapshot("evidence")

    def mutate(snapshot: dict[str, Any]) -> None:
        snapshot["observationEndedAtMs"] = 300
        snapshot["collection"]["watermarks"] = dict.fromkeys(snapshot["collection"]["watermarks"], 300)

    result = _run_standard_cleanup_discovery(
        value, mutate, phase=phase, path=path, quiescent=False, retirement_valid=path == "normal"
    )
    assert result.status == "INFRASTRUCTURE_ERROR", result.errors
    assert any("observation window" in error for error in result.errors), result.errors
    assert result.evidence["observationEndedAtMs"] == 200


@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize("shortfall", ["retirement", "watermarks"])
def test_cleanup_observation_bounds_preserve_exact_integer_timestamps(path: str, shortfall: str) -> None:
    value = _standard_retirement_snapshot("evidence")
    end = 2**53 + 1

    def mutate(snapshot: dict[str, Any]) -> None:
        snapshot["observationEndedAtMs"] = end
        snapshot["collection"]["watermarks"] = dict.fromkeys(
            snapshot["collection"]["watermarks"], end - 1 if shortfall == "watermarks" else end
        )
        value["retirement"]["retiredAtMs"] = end - 1 if shortfall == "retirement" else end + 1

    result = _run_standard_cleanup_discovery(
        value,
        mutate,
        phase="release",
        path=path,
        quiescent=False,
        retirement_valid=path == "normal" or shortfall == "watermarks",
    )
    assert result.status == "INFRASTRUCTURE_ERROR", result.errors
    assert any("observation window" in error for error in result.errors), result.errors


@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize("retired_at", [220, 320])
def test_retirement_covers_every_retained_cleanup_window_after_correction(path: str, retired_at: int) -> None:
    value = _standard_retirement_snapshot("evidence")
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        end = 300 if calls == 1 else 200
        snapshot["observationEndedAtMs"] = end
        snapshot["collection"]["watermarks"] = dict.fromkeys(snapshot["collection"]["watermarks"], end)
        value["retirement"]["retiredAtMs"] = retired_at

    covered = retired_at >= 300
    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path=path, quiescent=covered)
    assert result.status == ("PASSED" if covered else "INFRASTRUCTURE_ERROR"), result.errors
    assert result.cleanup_evidence["release"][0]["observationEndedAtMs"] == 300
    if not covered:
        assert any("observation window" in error for error in result.errors), result.errors


@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize("complete_coverage", [False, True])
def test_cleanup_watermark_completion_must_cover_retained_window(path: str, complete_coverage: bool) -> None:
    value = _standard_retirement_snapshot("evidence")
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        snapshot["observationEndedAtMs"] = 300
        if calls == 2 and complete_coverage:
            snapshot["collection"]["watermarks"] = dict.fromkeys(snapshot["collection"]["watermarks"], 300)
        value["retirement"]["retiredAtMs"] = 320

    result = _run_standard_cleanup_discovery(
        value,
        mutate,
        phase="both",
        path=path,
        quiescent=complete_coverage,
        retirement_valid=complete_coverage or path == "normal",
    )
    assert result.status == ("PASSED" if complete_coverage else "INFRASTRUCTURE_ERROR"), result.errors
    if not complete_coverage:
        assert any("observation window" in error for error in result.errors), result.errors


@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize("partial", [{}, {"watermarks": {}}, {"watermarks": {"lifecycle": 100}}, {"evidenceUris": []}])
def test_cleanup_accepts_absent_and_partial_envelope_metadata(path: str, partial: dict[str, Any]) -> None:
    value = _standard_retirement_snapshot("evidence")
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            snapshot.pop("schemaVersion")
            snapshot.pop("observationEndedAtMs")
            snapshot["collection"] = deepcopy(partial)
        else:
            for field in ("schemaVersion", "observationEndedAtMs", "collection"):
                snapshot[field] = deepcopy(value[field])

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path=path)
    assert result.status == "PASSED", result.errors


@pytest.mark.parametrize("phase", ["release", "retire", "snapshot"])
@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize(
    "change", ["completed-success", "completed-failure", "count", "count-and-oracle", "incomplete-entry", "peer-entry"]
)
def test_cleanup_replay_discoveries_use_frozen_operations_and_count_oracles(phase: str, path: str, change: str) -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    frozen_replay = deepcopy(value["replay"])
    rejected = change not in {"incomplete-entry", "peer-entry"}

    def mutate(snapshot: dict[str, Any]) -> None:
        if change.startswith("count"):
            count = next(item for item in snapshot["replay"]["sideEffects"] if item["operationId"] == "done")
            count["count"] = 2
            if change == "count-and-oracle":
                count["expectedCount"] = 2
        else:
            snapshot["replay"]["operationExecutions"].append(
                {
                    "requestId": "request-peer" if change == "peer-entry" else "request-resume",
                    "operationId": "not-checkpointed"
                    if change == "incomplete-entry"
                    else "failed"
                    if change == "completed-failure"
                    else "done",
                }
            )

    probe = deepcopy(value)
    mutate(probe)
    if change not in {"count-and-oracle", "incomplete-entry", "peer-entry"}:
        assert validate_evidence(probe)
    else:
        assert validate_evidence(probe) == []
    result = _run_standard_cleanup_discovery(value, mutate, phase=phase, path=path)
    assert result.status == ("FAILED" if rejected else "PASSED"), result.errors
    assert result.evidence["replay"] == frozen_replay
    if rejected:
        assert any("completed user code" in error or "side-effect" in error for error in result.errors), result.errors


def test_missing_expected_effect_is_behavior_failure_not_retirement_failure() -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    value["replay"]["missingExpectedEffects"] = ["fixture-success"]
    assert any(
        error == "ASSERTION: replay omitted the expected side-effect operation fixture-success"
        for error in validate_evidence(value)
    )
    assert validate_retirement_evidence(value) == []


@pytest.mark.parametrize("phase", ["release", "retire", "snapshot"])
@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize(
    "change", ["reverse", "operation", "first", "last", "request", "missing-entry", "duplicate", "metadata", "partial"]
)
def test_cleanup_retry_observations_preserve_frozen_identity_and_order(phase: str, path: str, change: str) -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    frozen = deepcopy(value["replay"])
    rejected = change not in {"metadata", "partial"}

    def mutate(snapshot: dict[str, Any]) -> None:
        retry = snapshot["replay"]["retry"]
        attempts = retry["attempts"]
        if change == "reverse":
            attempts.reverse()
        elif change == "operation":
            retry["operationId"] = "other-operation"
        elif change == "first":
            attempts[0]["outcome"] = "SUCCEEDED"
        elif change == "last":
            attempts[-1]["outcome"] = "FAILED"
        elif change == "request":
            attempts[0]["requestId"] = "request-resume"
            next(item for item in snapshot["replay"]["operationExecutions"] if item.get("attemptId") == "retry-1")[
                "requestId"
            ] = "request-resume"
        elif change == "missing-entry":
            attempts.insert(1, {"attemptId": "uncorroborated", "requestId": "request-victim", "outcome": "FAILED"})
        elif change == "duplicate":
            attempts.insert(1, deepcopy(attempts[0]))
        elif change == "metadata":
            attempts[0]["evidenceUri"] = "memory://retry-observation"
        else:
            snapshot["replay"]["retry"] = {"attempts": [{"attemptId": "retry-2"}]}

    probe = deepcopy(value)
    mutate(probe)
    if rejected and change != "request":
        assert validate_evidence(probe)
    result = _run_standard_cleanup_discovery(value, mutate, phase=phase, path=path)
    assert result.status == ("FAILED" if rejected else "PASSED"), result.errors
    assert result.evidence["replay"] == frozen
    if rejected:
        assert any("retry" in error for error in result.errors), result.errors


@pytest.mark.parametrize("path", ["normal", "independent"])
def test_cleanup_retry_order_conflict_survives_restoration(path: str) -> None:
    value = _standard_retirement_snapshot("replay_evidence")

    def mutate(snapshot: dict[str, Any]) -> None:
        snapshot["replay"]["retry"]["attempts"].reverse()

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path=path)
    assert result.status == "FAILED", result.errors
    assert result.evidence["replay"] == value["replay"]


@pytest.mark.parametrize(
    "position,outcome,rejected",
    [(0, "SUCCEEDED", True), (2, "FAILED", True), (0, "FAILED", False), (1, "FAILED", False), (2, "SUCCEEDED", False)],
)
def test_new_cleanup_retry_attempts_preserve_standard_endpoint_semantics(
    position: int, outcome: str, rejected: bool
) -> None:
    value = _standard_retirement_snapshot("replay_evidence")

    def mutate(snapshot: dict[str, Any]) -> None:
        attempt = {"attemptId": "additional", "requestId": "request-resume", "outcome": outcome}
        snapshot["replay"]["retry"]["attempts"].insert(position, attempt)
        snapshot["replay"]["operationExecutions"].append({**attempt, "operationId": "retry"})

    probe = deepcopy(value)
    mutate(probe)
    assert bool(validate_evidence(probe)) is rejected
    result = _run_standard_cleanup_discovery(value, mutate, phase="snapshot", path="independent")
    assert result.status == ("FAILED" if rejected else "PASSED"), result.errors


def test_cleanup_retry_correlation_can_be_supplied_after_the_attempt_fragment() -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        attempt = {"attemptId": "additional", "requestId": "request-victim", "outcome": "FAILED"}
        if calls == 1:
            snapshot["replay"]["retry"]["attempts"].insert(1, attempt)
        else:
            snapshot["replay"]["operationExecutions"].append({**attempt, "operationId": "retry"})

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path="independent")
    assert result.status == "PASSED", result.errors


@pytest.mark.parametrize("cycle", [False, True])
def test_cleanup_retry_partial_orders_are_combined_without_inventing_edges(cycle: bool) -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    extra = {"attemptId": "additional", "requestId": "request-victim", "outcome": "FAILED"}
    a, b = deepcopy(value["replay"]["retry"]["attempts"])
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            snapshot["replay"]["operationExecutions"].append({**extra, "operationId": "retry"})
        fragment = ([extra, a] if calls == 1 else [b, extra]) if cycle else ([a, extra] if calls == 1 else [extra, b])
        snapshot["replay"]["retry"] = {"attempts": deepcopy(fragment)}

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path="independent")
    assert result.status == ("FAILED" if cycle else "PASSED"), result.errors


def test_cleanup_retry_unbound_native_reference_requires_reconciliation() -> None:
    value = _standard_retirement_snapshot("replay_evidence")

    def mutate(snapshot: dict[str, Any]) -> None:
        snapshot["replay"]["retry"]["attempts"].insert(
            1, {"attemptId": "unbound", "requestId": "unknown-native", "outcome": "FAILED"}
        )

    result = _run_standard_cleanup_discovery(value, mutate, phase="retire", path="normal", quiescent=False)
    assert result.status != "PASSED"
    assert any(error.startswith("INCOMPLETE:") and "native request" in error for error in result.errors), result.errors


@pytest.mark.parametrize("phase", ["release", "retire", "snapshot"])
@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize("view", ["before", "after"])
@pytest.mark.parametrize(
    "change",
    [
        "result",
        "number-kind",
        "error",
        "id",
        "type",
        "name",
        "parentId",
        "status",
        "duplicate",
        "metadata",
        "partial",
        "subset",
    ],
)
def test_cleanup_checkpoint_views_preserve_completed_facts(phase: str, path: str, view: str, change: str) -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    frozen = deepcopy(value)
    rejected = change not in {"metadata", "partial", "subset"}

    def mutate(snapshot: dict[str, Any]) -> None:
        operations = snapshot["replay"][view]
        operation = next(item for item in operations if item["id"] == ("failed" if change == "error" else "done"))
        if change in {"result", "error"}:
            operation[change] = {"rewritten": [2, 1]}
        elif change == "number-kind":
            operation["result"] = {"value": True}
        elif change == "id":
            operation["id"] = "renamed-id"
        elif change == "type":
            operation["type"] = "CONTEXT"
        elif change == "name":
            operation["name"] = "renamed-operation"
        elif change == "parentId":
            operation["parentId"] = "other-parent"
        elif change == "status":
            operation["status"] = "PENDING"
        elif change == "duplicate":
            operations.append(deepcopy(operation))
        elif change == "metadata":
            operation["endedAtMs"] = 30
        elif change == "partial":
            snapshot["replay"][view] = [{"id": "failed", "error": deepcopy(value["replay"]["before"][1]["error"])}]
        else:
            snapshot["replay"][view] = [deepcopy(value["replay"]["before"][1])]

    probe = deepcopy(value)
    mutate(probe)
    if rejected:
        assert validate_evidence(probe)
    result = _run_standard_cleanup_discovery(value, mutate, phase=phase, path=path)
    assert result.status == ("FAILED" if rejected else "PASSED"), result.errors
    assert result.evidence["replay"] == frozen["replay"]
    assert result.evidence["histories"] == frozen["histories"]


@pytest.mark.parametrize("view", ["before", "after"])
@pytest.mark.parametrize("path", ["normal", "independent"])
def test_cleanup_checkpoint_contradiction_survives_restoration(view: str, path: str) -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            snapshot["replay"][view][0]["result"] = "rewritten"
        else:
            snapshot["replay"][view] = deepcopy(value["replay"][view])

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path=path)
    assert result.status == "FAILED", result.errors
    assert result.evidence["replay"] == value["replay"]
    assert result.evidence["histories"] == value["histories"]


@pytest.mark.parametrize("mode", ["retry", "later-code", "conflicting-views"])
@pytest.mark.parametrize("path", ["normal", "independent"])
def test_new_cleanup_checkpoint_claims_must_agree_with_known_replay_constraints(mode: str, path: str) -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    if mode == "later-code":
        operation = {
            "id": "additional",
            "name": "additional",
            "type": "STEP",
            "parentId": None,
            "status": "SUCCEEDED",
            "result": "before",
        }
        value["replay"]["after"].append(deepcopy(operation))
        next(item for item in value["histories"] if item["executionArn"] == "arn:execution:victim")[
            "operations"
        ].append(deepcopy(operation))
        value["replay"]["operationExecutions"].append({"requestId": "request-resume", "operationId": "additional"})
        value["replay"]["sideEffects"].append({"operationId": "additional", "count": 1, "expectedCount": 1})
    assert validate_evidence(value) == []

    def mutate(snapshot: dict[str, Any]) -> None:
        if mode == "retry":
            operation = next(item for item in snapshot["replay"]["after"] if item["id"] == "retry")
            snapshot["replay"]["before"].append(deepcopy(operation))
        else:
            operation = {
                "id": "additional",
                "name": "additional",
                "type": "STEP",
                "parentId": None,
                "status": "SUCCEEDED",
                "result": "before",
            }
            snapshot["replay"]["before"].append(operation)
            if mode == "conflicting-views":
                snapshot["replay"]["after"].append({**operation, "result": "after"})

    probe = deepcopy(value)
    mutate(probe)
    assert validate_evidence(probe)
    result = _run_standard_cleanup_discovery(value, mutate, phase="snapshot", path=path)
    assert result.status == "FAILED", result.errors
    assert result.evidence["replay"] == value["replay"]
    assert result.evidence["histories"] == value["histories"]


@pytest.mark.parametrize("initial", ["STARTED", "PENDING", "READY"])
@pytest.mark.parametrize("later", ["READY", "SUCCEEDED"])
def test_cleanup_after_checkpoint_can_complete_without_rewriting_frozen_state(initial: str, later: str) -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    operation = {
        "id": "incomplete",
        "name": "incomplete",
        "type": "STEP",
        "parentId": None,
        "status": initial,
        "result": {"poll": 1},
    }
    value["replay"]["after"].append(deepcopy(operation))
    history = next(item for item in value["histories"] if item["executionArn"] == "arn:execution:victim")
    history["operations"].append(deepcopy(operation))
    assert validate_evidence(value) == []
    frozen = deepcopy(value)

    def mutate(snapshot: dict[str, Any]) -> None:
        for current in snapshot["replay"]["after"]:
            if current["id"] == "incomplete":
                current.update(status=later, result={"poll": 2})
        for current in next(item for item in snapshot["histories"] if item["executionArn"] == "arn:execution:victim")[
            "operations"
        ]:
            if current["id"] == "incomplete":
                current.update(status=later, result={"poll": 2})

    result = _run_standard_cleanup_discovery(value, mutate, phase="retire", path="independent")
    assert result.status == "PASSED", result.errors
    assert result.evidence["replay"] == frozen["replay"]
    assert result.evidence["histories"] == frozen["histories"]


@pytest.mark.parametrize("path", ["normal", "independent"])
def test_cleanup_new_after_checkpoint_can_gain_service_correlation_later(path: str) -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    operation = {
        "id": "discovered",
        "name": "discovered",
        "type": "STEP",
        "parentId": None,
        "status": "SUCCEEDED",
        "result": "stored",
    }
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            snapshot["replay"]["after"].append(deepcopy(operation))
        else:
            history = next(item for item in snapshot["histories"] if item["executionArn"] == "arn:execution:victim")
            history["operations"].append(deepcopy(operation))

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path=path)
    assert result.status == "PASSED", result.errors
    assert result.evidence["replay"] == value["replay"]
    assert result.evidence["histories"] == value["histories"]


@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize("phase", ["release", "retire", "snapshot", "both"])
@pytest.mark.parametrize("pending_at", [39, 40, 40.5])
def test_cleanup_pending_timing_uses_the_frozen_wrapper_return(path: str, phase: str, pending_at: float) -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    frozen_events = deepcopy(value["events"])
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        event = next(item for item in snapshot["events"] if item["kind"] == "pending")
        event["atMs"] = 35 if phase == "both" and calls > 1 else pending_at

    probe = deepcopy(value)
    mutate(probe)
    assert bool(validate_evidence(probe)) is (pending_at > 40)
    calls = 0
    result = _run_standard_cleanup_discovery(value, mutate, phase=phase, path=path)
    assert result.status == ("FAILED" if pending_at > 40 else "PASSED"), result.errors
    assert result.evidence["events"] == frozen_events


@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize(
    ("field", "changed"),
    [("ownerRequestId", "other"), ("executionArn", "other"), ("atMs", None), ("atMs", True), ("atMs", -1)],
)
def test_cleanup_pending_invalid_facts_survive_restoration(path: str, field: str, changed: Any) -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    original = next(item for item in value["events"] if item["kind"] == "pending")
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        event = next(item for item in snapshot["events"] if item["kind"] == "pending")
        event[field] = changed if calls == 1 else original[field]

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path=path, quiescent=False)
    assert result.status != "PASSED"
    assert any(error.startswith("INCOMPLETE: cleanup PENDING event") for error in result.errors), result.errors


@pytest.mark.parametrize("path", ["normal", "independent"])
def test_cleanup_pending_partial_metadata_can_be_completed(path: str) -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    original = next(item for item in value["events"] if item["kind"] == "pending")
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        event = next(item for item in snapshot["events"] if item["kind"] == "pending")
        for key in ("ownerRequestId", "executionArn", "atMs"):
            if calls == 1:
                event.pop(key)
            else:
                event[key] = original[key]

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path=path)
    assert result.status == "PASSED", result.errors


@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize("start", [39, 40, 41, 45])
def test_cleanup_resume_admission_uses_the_frozen_pending_return(path: str, start: int) -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    frozen_attempts = deepcopy(value["attempts"])

    def mutate(snapshot: dict[str, Any]) -> None:
        next(item for item in snapshot["attempts"] if item["role"] == "resume")["startedAtMs"] = start

    probe = deepcopy(value)
    mutate(probe)
    assert bool(validate_evidence(probe)) is (start < 45)
    result = _run_standard_cleanup_discovery(value, mutate, phase="snapshot", path=path)
    assert result.status == ("FAILED" if start < 45 else "PASSED"), result.errors
    assert result.evidence["attempts"] == frozen_attempts


@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize("phase", ["release", "retire", "snapshot", "both"])
@pytest.mark.parametrize(
    ("factory", "kind", "field", "changed", "rejected"),
    [
        ("evidence", "root_completed", "outcome", "FAILED", True),
        ("evidence", "root_completed", "atMs", 41, True),
        ("evidence", "root_completed", "atMs", 39, False),
        ("deadline_evidence", "platform_timeout", "source", "sdk", True),
        ("deadline_evidence", "platform_timeout", "source", "platform", False),
        ("deadline_evidence", "platform_timeout", "atMs", 29, True),
        ("deadline_evidence", "platform_timeout", "atMs", 31, False),
        ("recovery_evidence", "worker_recovered", "atMs", 46, True),
        ("recovery_evidence", "worker_recovered", "atMs", 44, False),
        ("evidence", "barrier_arrived", "atMs", 41, True),
        ("evidence", "barrier_arrived", "atMs", 19, False),
        ("recovery_evidence", "replacement", "replacesWorkerId", "unrelated-worker", True),
        ("recovery_evidence", "replacement", "replacesWorkerId", "process-1", False),
    ],
)
def test_cleanup_singleton_facts_refute_frozen_measurements(
    path: str, phase: str, factory: str, kind: str, field: str, changed: Any, rejected: bool
) -> None:
    value = _standard_retirement_snapshot(
        factory, "python" if kind == "replacement" else "javascript", requirement=1 if kind == "barrier_arrived" else 2
    )
    if kind == "replacement":
        value["profile"]["recoveryPolicy"] = "replace-worker"
        recovery = next(item for item in value["attempts"] if item["role"] == "recovery")
        recovery.update(workerId="replacement-worker", processId="replacement-process", replacesWorkerId="process-1")
    assert validate_evidence(value) == []
    frozen = deepcopy(value)
    collection, identity, selected = (
        ("attempts", "role", "recovery") if kind == "replacement" else ("events", "kind", kind)
    )
    original = next(item for item in value[collection] if item[identity] == selected)
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        target = next(item for item in snapshot[collection] if item[identity] == selected)
        target[field] = original[field] if phase == "both" and calls > 1 else changed

    probe = deepcopy(value)
    mutate(probe)
    assert bool(validate_evidence(probe)) is rejected
    calls = 0
    result = _run_standard_cleanup_discovery(value, mutate, phase=phase, path=path)
    assert result.status == ("FAILED" if rejected else "PASSED"), result.errors
    assert result.evidence[collection] == frozen[collection]


@pytest.mark.parametrize("kind", ["barrier_arrived", "platform_timeout"])
def test_cleanup_singleton_timing_allows_completed_native_intervals(kind: str) -> None:
    value = (
        _standard_retirement_snapshot("evidence", requirement=1)
        if kind == "barrier_arrived"
        else _standard_retirement_snapshot("deadline_evidence")
    )
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        event = next(item for item in snapshot["events"] if item["kind"] == kind)
        event["atMs"] = 41 if kind == "barrier_arrived" else 29
        if calls == 2:
            victim = next(item for item in snapshot["attempts"] if item["role"] == "victim")
            victim["endedAtMs" if kind == "barrier_arrived" else "startedAtMs"] = 45 if kind == "barrier_arrived" else 9

    if kind == "barrier_arrived":
        provisional = deepcopy(value)
        mutate(provisional)
        assert any(
            "barrier lies outside the established native overlap" in error
            for error in _cleanup_behavior_errors(value, provisional, standard=True, final=True)
        )
        calls = 0

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path="independent")
    assert result.status == "PASSED", result.errors


@pytest.mark.parametrize("kind", ["peer_progress", "admission_closed"])
def test_cleanup_positive_event_discovery_does_not_erase_prior_evidence(kind: str) -> None:
    value = _standard_retirement_snapshot("recovery_evidence")

    def mutate(snapshot: dict[str, Any]) -> None:
        event = next(
            item
            for item in snapshot["events"]
            if item["kind"] == kind and (kind != "peer_progress" or item["atMs"] == 44)
        )
        event["atMs"] = 46 if kind == "peer_progress" else 34

    result = _run_standard_cleanup_discovery(value, mutate, phase="snapshot", path="normal")
    assert result.status == "PASSED", result.errors


@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize("late_at", [64, 70])
def test_cleanup_late_activity_cannot_outlive_the_frozen_task_exit(path: str, late_at: int) -> None:
    value = _standard_retirement_snapshot("warm_evidence", "python")
    frozen_events = deepcopy(value["events"])
    victim = next(item for item in value["attempts"] if item["role"] == "victim")
    owner = {key: victim[key] for key in ("requestId", "executionArn")}
    owner["ownerRequestId"] = victim["requestId"]

    def mutate(snapshot: dict[str, Any]) -> None:
        snapshot["events"].extend(
            [
                {**owner, "kind": "task_start", "taskId": "discovered-child", "atMs": 55},
                {**owner, "kind": "late_activity", "atMs": late_at, "observedDuringRequestId": "request-warm"},
                {**owner, "kind": "task_exit", "taskId": "discovered-child", "atMs": late_at + 0.5},
            ]
        )

    probe = deepcopy(value)
    next(item for item in probe["events"] if item["kind"] == "late_activity")["atMs"] = late_at
    assert bool(validate_evidence(probe)) is (late_at > 65)
    result = _run_standard_cleanup_discovery(value, mutate, phase="retire", path=path)
    assert result.status == ("FAILED" if late_at > 65 else "PASSED"), result.errors
    assert result.evidence["events"] == frozen_events


def test_cleanup_replay_binding_can_arrive_in_later_retirement_evidence() -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    entry = {"requestId": "discovered-native", "operationId": "not-checkpointed"}
    discovered = {"replay": {"operationExecutions": [entry]}}
    binding = {
        "attempts": [{"requestId": "discovered-native", "executionArn": "arn:execution:victim"}],
        "retirement": deepcopy(value["retirement"]),
    }
    binding["retirement"]["resources"].append(
        {
            "id": "discovered-native",
            "kind": "invocation",
            "ownerCaseId": value["caseId"],
            "settledAtMs": 209,
            "outcome": "QUIESCENT",
            "evidenceUri": "memory://retirement",
        }
    )
    assert _cleanup_behavior_errors(value, discovered, standard=True) == []
    assert _cleanup_behavior_errors(value, binding, standard=True, known_snapshots=[discovered], final=True) == []
    unresolved = _cleanup_behavior_errors(value, {}, standard=True, known_snapshots=[discovered], final=True)
    assert any(error.startswith("INCOMPLETE:") and "native request" in error for error in unresolved), unresolved
    entry["operationId"] = "done"
    completed = _cleanup_behavior_errors(value, binding, standard=True, known_snapshots=[discovered], final=True)
    assert any("completed user code" in error for error in completed), completed
    contradictory = {"attempts": [{"requestId": "discovered-native", "executionArn": "arn:execution:peer"}]}
    conflicts = _cleanup_behavior_errors(
        value, contradictory, standard=True, known_snapshots=[discovered, binding], final=True
    )
    assert any(error.startswith("INCOMPLETE:") and "conflicting execution binding" in error for error in conflicts)


@pytest.mark.parametrize("path", ["normal", "independent"])
def test_unbound_cleanup_replay_entries_require_reconciliation(path: str) -> None:
    value = _standard_retirement_snapshot("replay_evidence")

    def mutate(snapshot: dict[str, Any]) -> None:
        snapshot["replay"]["operationExecutions"].append(
            {"requestId": "unknown-native", "operationId": "not-checkpointed"}
        )

    result = _run_standard_cleanup_discovery(value, mutate, phase="release", path=path, quiescent=False)
    assert result.status != "PASSED"
    assert any(error.startswith("INCOMPLETE:") and "native request" in error for error in result.errors), result.errors


@pytest.mark.parametrize("phase", ["retire", "snapshot"])
@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize(
    ("kind", "outcome", "when", "closure_owner", "rejected"),
    [
        ("sdk", "DISPATCHED", 39, "victim", True),
        ("probe", "ACCEPTED", 39, "victim", True),
        ("sdk", "REJECTED", 39, "victim", False),
        ("sdk", "DISPATCHED", 37, "victim", False),
        ("probe", "ACCEPTED", 37, "victim", False),
        ("sdk", "DISPATCHED", 39, "peer", False),
        ("probe", "ACCEPTED", 39, "peer", False),
    ],
)
def test_discovered_earlier_closure_rechecks_previously_observed_admission(
    phase: str, path: str, kind: str, outcome: str, when: int, closure_owner: str, rejected: bool
) -> None:
    value = _standard_retirement_snapshot("evidence")
    attempts = {attempt["role"]: attempt for attempt in value["attempts"]}
    victim = attempts["victim"]
    if kind == "sdk":
        value["sdkRequests"][0].update(atMs=when, outcome=outcome)
        _complete_fixture_sdk_calls(value)
    else:
        value["events"].append(
            {
                "kind": "admission_probe",
                "probe": "operation",
                "outcome": outcome,
                "atMs": when,
                "requestId": victim["requestId"],
                "ownerRequestId": victim["requestId"],
                "executionArn": victim["executionArn"],
            }
        )
        _resequence(value)
    original_requests, original_events = deepcopy(value["sdkRequests"]), deepcopy(value["events"])

    def mutate(snapshot: dict[str, Any]) -> None:
        owner = attempts[closure_owner]
        snapshot["events"].append(
            {
                "kind": "admission_closed",
                "atMs": 38,
                "requestId": owner["requestId"],
                "ownerRequestId": owner["requestId"],
                "executionArn": owner["executionArn"],
            }
        )
        _resequence(snapshot)

    result = _run_standard_cleanup_discovery(value, mutate, phase=phase, path=path)
    assert result.status == ("FAILED" if rejected else "PASSED"), result.errors
    assert result.evidence["sdkRequests"] == original_requests
    assert result.evidence["events"] == original_events


@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize("kind", ["sdk", "probe"])
def test_cleanup_admission_combines_closure_and_activity_from_separate_snapshots(path: str, kind: str) -> None:
    value = _standard_retirement_snapshot("evidence")
    victim = value["attempts"][0]
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        owner = {
            "requestId": victim["requestId"],
            "ownerRequestId": victim["requestId"],
            "executionArn": victim["executionArn"],
        }
        if calls == 1:
            snapshot["events"].append({**owner, "kind": "admission_closed", "atMs": 38})
        else:
            snapshot["events"] = [event for event in snapshot["events"] if event["kind"] != "admission_closed"]
            if kind == "sdk":
                snapshot["sdkRequests"].append(
                    {**owner, "method": "CheckpointDurableExecution", "outcome": "DISPATCHED", "atMs": 39}
                )
                _complete_fixture_sdk_calls(snapshot)
            else:
                snapshot["events"].append(
                    {**owner, "kind": "admission_probe", "probe": "operation", "outcome": "ACCEPTED", "atMs": 39}
                )
        _resequence(snapshot)

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path=path)
    assert calls == 2
    assert result.status == "FAILED", result.errors
    assert not any(event["kind"] == "admission_closed" for event in result.retirement_evidence["events"])
    assert any(
        event["kind"] == "admission_closed"
        for snapshot in result.cleanup_evidence["release"]
        for event in snapshot["events"]
    )


@pytest.mark.parametrize("phase", ["release", "retire"])
@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize(
    ("kind", "outcome", "when", "owner", "early_closure", "rejected"),
    [
        ("sdk", "DISPATCHED", 50, "victim", False, True),
        ("sdk", "DISPATCHED", 40, "victim", False, True),
        ("sdk", "DISPATCHED", 39, "victim", False, False),
        ("sdk", "REJECTED", 50, "victim", False, False),
        ("sdk", "DISPATCHED", 41, "peer", False, False),
        ("sdk", "DISPATCHED", 39, "victim", True, True),
        ("operation", "ACCEPTED", 50, "victim", False, False),
        ("checkpoint", "ACCEPTED", 50, "victim", False, True),
        ("operation", "REJECTED", 50, "victim", False, False),
        ("operation", "ACCEPTED", 39, "victim", False, False),
        ("operation", "ACCEPTED", 41, "peer", False, False),
    ],
)
def test_cleanup_discovered_admission_violations_fail_behavior_but_allow_audited_retirement(
    phase: str, path: str, kind: str, outcome: str, when: int, owner: str, early_closure: bool, rejected: bool
) -> None:
    value = _standard_retirement_snapshot("evidence")
    selected = next(attempt for attempt in value["attempts"] if attempt["role"] == owner)
    if early_closure:
        value["events"].append(
            {
                "kind": "admission_closed",
                "atMs": 38,
                "requestId": selected["requestId"],
                "ownerRequestId": selected["requestId"],
                "executionArn": selected["executionArn"],
            }
        )
        _resequence(value)
    original_requests = deepcopy(value["sdkRequests"])

    def mutate(snapshot: dict[str, Any]) -> None:
        record = {
            "requestId": selected["requestId"],
            "ownerRequestId": selected["requestId"],
            "executionArn": selected["executionArn"],
            "atMs": when,
            "outcome": outcome,
        }
        if kind == "sdk":
            snapshot["sdkRequests"].append({**record, "method": "CheckpointDurableExecution"})
            _complete_fixture_sdk_calls(snapshot)
        else:
            snapshot["events"].append({**record, "kind": "admission_probe", "probe": kind})
        if early_closure:
            next(event for event in snapshot["events"] if event["kind"] == "admission_closed")["atMs"] = 41
        _resequence(snapshot)

    result = _run_standard_cleanup_discovery(value, mutate, phase=phase, path=path)
    assert result.status == ("FAILED" if rejected else "PASSED"), result.errors
    assert result.evidence["sdkRequests"] == original_requests
    if rejected:
        assert any("admission" in error.lower() for error in result.errors), result.errors


@pytest.mark.parametrize(
    ("phase", "path"), [("release", "normal"), ("retire", "normal"), ("retirement", "independent")]
)
@pytest.mark.parametrize("mode", ["normal", "emergency"])
@pytest.mark.parametrize(
    ("factory", "released", "failed"),
    [
        ("deadline_evidence", 34, True),
        ("deadline_evidence", 35, True),
        ("deadline_evidence", 36, False),
        ("recovery_evidence", 44, True),
        ("recovery_evidence", 45, True),
        ("recovery_evidence", 46, False),
    ],
)
def test_retained_retirement_fault_releases_refute_measurement_even_after_overwrite(
    phase: str, path: str, mode: str, factory: str, released: int, failed: bool
) -> None:
    value = _standard_retirement_snapshot(factory, "python")
    original_faults = deepcopy(value["faultsCreated"])

    def mutate(snapshot: dict[str, Any]) -> None:
        fault = value["faultsCreated"][0]
        snapshot.setdefault("retirement", {})["faults"] = [
            {"id": fault["id"], "ownerCaseId": fault["ownerCaseId"], "releasedAtMs": released, "mode": mode}
        ]

    result = _run_standard_cleanup_discovery(value, mutate, phase=phase, path=path)
    assert result.status == ("FAILED" if failed else "PASSED"), result.errors
    assert result.evidence["faultsCreated"] == original_faults
    if failed:
        assert any("fault release" in error for error in result.errors), result.errors
    if phase == "retirement":
        assert result.retirement_evidence["retirement"]["faults"][0]["releasedAtMs"] == released
    else:
        assert result.evidence["retirement"]["faults"][0]["releasedAtMs"] == 210
        assert any(
            fault.get("releasedAtMs") == released
            for snapshot in result.cleanup_evidence[phase]
            for fault in snapshot.get("retirement", {}).get("faults", [])
        )


def test_early_retirement_release_cannot_be_erased_from_original_observations() -> None:
    value = _standard_retirement_snapshot("deadline_evidence")
    value["retirement"]["faults"][0]["releasedAtMs"] = 34

    def mutate(snapshot: dict[str, Any]) -> None:
        snapshot["retirement"]["faults"][0]["releasedAtMs"] = 210
        value["retirement"]["faults"][0]["releasedAtMs"] = 210

    result = _run_standard_cleanup_discovery(value, mutate, phase="release", path="normal", observe_retirement=True)
    assert result.status == "FAILED", result.errors
    assert any("fault release" in error for error in result.errors)


def _cleanup_time_record(value: dict[str, Any], kind: str) -> dict[str, Any]:
    if kind == "sdk_request":
        return value["sdkRequests"][0]
    return next(item for item in value["events"] if item["kind"] == kind)


@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize(
    ("factory", "kind"),
    [
        ("evidence", "admission_probe"),
        ("deadline_evidence", "admission_closed"),
        ("recovery_evidence", "peer_progress"),
        ("evidence", "sdk_request"),
    ],
)
@pytest.mark.parametrize("invalid", [False, True, None, "45", -1, float("inf"), float("-inf"), float("nan")])
def test_cleanup_record_invalid_time_survives_restoration(path: str, factory: str, kind: str, invalid: Any) -> None:
    value = _standard_retirement_snapshot(factory)
    original = _cleanup_time_record(value, kind)["atMs"]
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        _cleanup_time_record(snapshot, kind)["atMs"] = invalid if calls == 1 else original

    probe = deepcopy(value)
    mutate(probe)
    assert any(error.startswith("INCOMPLETE:") and ".atMs" in error for error in validate_evidence(probe))
    calls = 0
    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path=path, quiescent=False)
    assert result.status == "INFRASTRUCTURE_ERROR", result.errors
    assert any(error.startswith("INCOMPLETE:") and ".atMs" in error for error in result.errors), result.errors


@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize(
    "during,observed,rejected",
    [(False, 5, True), (False, 205, True), (False, 45, False), (True, 11, True), (True, 43, True), (True, 20, False)],
)
def test_cleanup_record_time_respects_all_known_intervals(
    path: str, during: bool, observed: int, rejected: bool
) -> None:
    value = _standard_retirement_snapshot("evidence")
    kind = "barrier_arrived" if during else "admission_probe"
    record = _cleanup_time_record(value, kind)
    original = record["atMs"]
    if during:
        record["observedDuringRequestId"] = "request-peer"
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        _cleanup_time_record(snapshot, kind)["atMs"] = observed if calls == 1 else original

    probe = deepcopy(value)
    mutate(probe)
    assert bool(validate_evidence(probe)) is rejected
    calls = 0
    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path=path, quiescent=during or not rejected)
    expected = "FAILED" if during and rejected else "INFRASTRUCTURE_ERROR" if rejected else "PASSED"
    assert result.status == expected, result.errors


@pytest.mark.parametrize("completion", ["native-start", "observation-end", "during-start", "during-end"])
def test_cleanup_record_time_allows_later_interval_completion(completion: str) -> None:
    value = _standard_retirement_snapshot("evidence")
    during = completion.startswith("during-")
    kind = "barrier_arrived" if during else "admission_probe"
    record = _cleanup_time_record(value, kind)
    original = record["atMs"]
    if during:
        record["observedDuringRequestId"] = "request-peer"
    observed = {"native-start": 5, "observation-end": 205, "during-start": 11, "during-end": 43}[completion]
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        _cleanup_time_record(snapshot, kind)["atMs"] = observed if calls == 1 else original
        if calls == 2:
            if completion == "observation-end":
                snapshot["observationEndedAtMs"] = 208
                snapshot["collection"]["watermarks"] = dict.fromkeys(("lifecycle", "sdkRequests", "history"), 208)
            else:
                native = next(item for item in snapshot["attempts"] if item["role"] == ("peer" if during else "victim"))
                native["endedAtMs" if completion == "during-end" else "startedAtMs"] = {
                    "native-start": 4,
                    "during-start": 10,
                    "during-end": 44,
                }[completion]

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path="independent")
    assert result.status == "PASSED", result.errors


@pytest.mark.parametrize("complete_window", [False, True])
def test_cleanup_record_time_keeps_integer_boundary_precision(complete_window: bool) -> None:
    value = _standard_retirement_snapshot("evidence")
    boundary = 2**53
    value["observationEndedAtMs"] = boundary
    value["collection"]["watermarks"] = dict.fromkeys(("lifecycle", "sdkRequests", "history"), boundary)
    value["retirement"]["retiredAtMs"] = boundary + 4
    for resource in value["retirement"]["resources"]:
        resource["settledAtMs"] = boundary + 2
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        _cleanup_time_record(snapshot, "admission_probe")["atMs"] = boundary + 1 if calls == 1 else 45
        if calls == 2 and complete_window:
            snapshot["observationEndedAtMs"] = boundary + 1
            snapshot["collection"]["watermarks"] = dict.fromkeys(("lifecycle", "sdkRequests", "history"), boundary + 1)

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path="independent", quiescent=complete_window)
    assert result.status == ("PASSED" if complete_window else "INFRASTRUCTURE_ERROR"), result.errors


@pytest.mark.parametrize("missing", [False, True])
@pytest.mark.parametrize("kind", ["admission_probe", "admission_closed", "peer_progress"])
def test_cleanup_record_time_allows_provisional_and_valid_values(kind: str, missing: bool) -> None:
    factory = {"admission_closed": "deadline_evidence", "peer_progress": "recovery_evidence"}.get(kind, "evidence")
    value = _standard_retirement_snapshot(factory)
    original = _cleanup_time_record(value, kind)["atMs"]
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        record = _cleanup_time_record(snapshot, kind)
        if calls == 1 and missing:
            record.pop("atMs")
        else:
            record["atMs"] = float(original)

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path="independent")
    assert result.status == "PASSED", result.errors


@pytest.mark.parametrize("observed", [False, 5, 205])
def test_cleanup_record_time_checks_also_apply_to_custom_catalogs(observed: Any) -> None:
    value = _standard_retirement_snapshot("evidence")
    value["coverageProfile"] = "sdk-local"
    value["requirementId"] = "sdk-local-timestamp-window"
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        _cleanup_time_record(snapshot, "admission_probe")["atMs"] = observed if calls == 1 else 45

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path="independent", quiescent=False)
    assert result.status == "INFRASTRUCTURE_ERROR", result.errors


@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize("location", ["faultsCreated", "retirement"])
@pytest.mark.parametrize("restoration", ["remove", "replace"])
@pytest.mark.parametrize("invalid", [False, True, None, "210", -1, float("inf"), float("-inf"), float("nan")])
def test_cleanup_invalid_fault_release_timestamps_survive_restoration(
    path: str, location: str, restoration: str, invalid: Any
) -> None:
    value = _standard_retirement_snapshot("deadline_evidence")
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        fault = snapshot["faultsCreated"][0] if location == "faultsCreated" else snapshot["retirement"]["faults"][0]
        if calls == 1:
            fault["releasedAtMs"] = invalid
        elif restoration == "replace":
            fault["releasedAtMs"] = 210
        else:
            fault.pop("releasedAtMs")

    probe = deepcopy(value)
    mutate(probe)
    assert any(error.startswith("INCOMPLETE:") and "releasedAtMs" in error for error in validate_evidence(probe))
    calls = 0
    result = _run_standard_cleanup_discovery(
        value, mutate, phase="both", path=path, quiescent=False, observe_retirement=True
    )
    assert result.status == "INFRASTRUCTURE_ERROR", result.errors
    assert any(error.startswith("INCOMPLETE:") and "releasedAtMs" in error for error in result.errors), result.errors
    assert result.evidence["faultsCreated"] == value["faultsCreated"]


@pytest.mark.parametrize(
    ("location", "phase", "path", "retirement_valid"),
    [
        ("faultsCreated", "release", "normal", False),
        ("faultsCreated", "retire", "normal", False),
        ("faultsCreated", "snapshot", "independent", False),
        ("retirement", "release", "normal", True),
        ("retirement", "retire", "normal", True),
        ("retirement", "retirement", "independent", False),
    ],
)
def test_cleanup_invalid_fault_release_timestamps_are_checked_in_each_phase(
    location: str, phase: str, path: str, retirement_valid: bool
) -> None:
    value = _standard_retirement_snapshot("deadline_evidence")

    def mutate(snapshot: dict[str, Any]) -> None:
        fault = snapshot["faultsCreated"][0] if location == "faultsCreated" else snapshot["retirement"]["faults"][0]
        fault["releasedAtMs"] = False

    result = _run_standard_cleanup_discovery(
        value,
        mutate,
        phase=phase,
        path=path,
        quiescent=False,
        observe_retirement=True,
        retirement_valid=retirement_valid,
    )
    assert result.status == "INFRASTRUCTURE_ERROR", result.errors
    assert any(error.startswith("INCOMPLETE:") and "releasedAtMs" in error for error in result.errors), result.errors


@pytest.mark.parametrize("location", ["faultsCreated", "retirement"])
@pytest.mark.parametrize("released", [None, 210, 210.5])
def test_cleanup_missing_fault_release_timestamp_can_be_completed(location: str, released: float | None) -> None:
    value = _standard_retirement_snapshot("deadline_evidence")
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        fault = snapshot["faultsCreated"][0] if location == "faultsCreated" else snapshot["retirement"]["faults"][0]
        if calls == 1 or released is None:
            fault.pop("releasedAtMs", None)
        else:
            fault["releasedAtMs"] = released

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path="independent", observe_retirement=True)
    assert result.status == "PASSED", result.errors


def test_cleanup_invalid_fault_release_is_rejected_outside_deadline_scenarios() -> None:
    value = _standard_retirement_snapshot("evidence")
    fault = {"id": "auxiliary-fault", "ownerCaseId": value["caseId"]}
    value["faultsCreated"].append(fault)
    value["retirement"]["faults"].append({**fault, "releasedAtMs": 210, "mode": "normal"})
    assert validate_evidence(value) == []
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        observed = snapshot["faultsCreated"][0]
        if calls == 1:
            observed["releasedAtMs"] = False
        else:
            observed.pop("releasedAtMs")

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path="normal", quiescent=False)
    assert result.status == "INFRASTRUCTURE_ERROR", result.errors


def test_retirement_release_discovery_preserves_an_existing_sdk_failure() -> None:
    value = _standard_retirement_snapshot("deadline_evidence")
    value["sdkRequests"][0]["atMs"] = 32
    _complete_fixture_sdk_calls(value)
    original_requests = deepcopy(value["sdkRequests"])

    def mutate(snapshot: dict[str, Any]) -> None:
        snapshot["retirement"]["faults"][0]["releasedAtMs"] = 34

    result = _run_standard_cleanup_discovery(
        value, mutate, phase="retirement", path="independent", originally_failed=True
    )
    assert result.status == "FAILED"
    assert any("admission closed" in error for error in result.errors)
    assert any("fault release" in error for error in result.errors)
    assert result.evidence["sdkRequests"] == original_requests


@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize(
    ("factory", "runtime", "change", "rejected"),
    [
        ("deadline_evidence", "javascript", "fault-before-exit", True),
        ("recovery_evidence", "python", "fault-before-recovery", True),
        ("recovery_evidence", "python", "fault-after-recovery", False),
        ("warm_evidence", "python", "victim-end-overlap", True),
        ("warm_evidence", "python", "warm-start-before-wrapper", True),
        ("warm_evidence", "python", "warm-start-equal-wrapper", True),
        ("warm_evidence", "python", "victim-end-equal", False),
        ("recovery_evidence", "python", "recovery-before-timeout", True),
        ("recovery_evidence", "python", "victim-end-overlap", True),
        ("recovery_evidence", "python", "victim-end-equal", False),
        ("recovery_evidence", "javascript", "victim-end-overlap", False),
        ("recovery_evidence", "python", "replacement-victim-overlap", False),
    ],
)
def test_cleanup_native_completion_and_fault_facts_can_refute_frozen_standard_measurements(
    path: str, factory: str, runtime: str, change: str, rejected: bool
) -> None:
    value = _standard_retirement_snapshot(factory, runtime)
    if change == "replacement-victim-overlap":
        value["profile"]["recoveryPolicy"] = "replace-worker"
        attempts = {attempt["role"]: attempt for attempt in value["attempts"]}
        attempts["recovery"].update(
            workerId="replacement-worker",
            processId="replacement-process",
            replacesWorkerId=attempts["victim"]["workerId"],
        )
    original_attempts = deepcopy(value["attempts"])

    def mutate(snapshot: dict[str, Any]) -> None:
        attempts = {attempt["role"]: attempt for attempt in snapshot["attempts"]}
        if change.startswith("fault-"):
            snapshot["faultsCreated"][0]["releasedAtMs"] = {
                "fault-before-exit": 34,
                "fault-before-recovery": 44,
                "fault-after-recovery": 46,
            }[change]
        elif change == "warm-start-before-wrapper":
            attempts["warm"]["startedAtMs"] = 39
        elif change == "warm-start-equal-wrapper":
            attempts["warm"]["startedAtMs"] = 40
        elif change == "recovery-before-timeout":
            attempts["recovery"]["startedAtMs"] = 29
        else:
            following = attempts["warm"] if "warm" in attempts else attempts["recovery"]
            attempts["victim"]["endedAtMs"] = following["startedAtMs"] + (10 if change.endswith("overlap") else 0)

    changed = deepcopy(value)
    mutate(changed)
    rewrites_platform = any(
        previous.get(f"{field}Source") == "platform" and previous[field] != current[field]
        for previous, current in zip(original_attempts, changed["attempts"], strict=True)
        for field in ("startedAtMs", "endedAtMs")
    )
    result = _run_standard_cleanup_discovery(value, mutate, phase="retire", path=path, quiescent=not rewrites_platform)
    expected = "INFRASTRUCTURE_ERROR" if rewrites_platform else "FAILED" if rejected else "PASSED"
    assert result.status == expected, result.errors
    assert result.evidence["attempts"] == original_attempts


@pytest.mark.parametrize("change", ["dispatch", "probe", "early-fault", "warm-overlap", "recovery-before-timeout"])
def test_retirement_only_discoveries_are_checked_against_frozen_measurements(change: str) -> None:
    factory = (
        "evidence"
        if change in ("dispatch", "probe")
        else "warm_evidence"
        if change == "warm-overlap"
        else "recovery_evidence"
    )
    value = _standard_retirement_snapshot(factory, "python")

    def mutate(snapshot: dict[str, Any]) -> None:
        attempts = {attempt["role"]: attempt for attempt in snapshot["attempts"]}
        victim = attempts["victim"]
        if change in ("dispatch", "probe"):
            record = {
                "requestId": victim["requestId"],
                "ownerRequestId": victim["requestId"],
                "executionArn": victim["executionArn"],
                "atMs": 50,
            }
            if change == "dispatch":
                snapshot["sdkRequests"].append(
                    {**record, "method": "CheckpointDurableExecution", "outcome": "DISPATCHED"}
                )
                _complete_fixture_sdk_calls(snapshot)
            else:
                snapshot["events"].append(
                    {**record, "kind": "admission_probe", "probe": "checkpoint", "outcome": "ACCEPTED"}
                )
        elif change == "early-fault":
            snapshot["faultsCreated"][0]["releasedAtMs"] = 44
        elif change == "warm-overlap":
            victim["endedAtMs"] = 60
        else:
            attempts["recovery"]["startedAtMs"] = 29

    rewrites_platform = change in ("warm-overlap", "recovery-before-timeout")
    result = _run_standard_cleanup_discovery(
        value, mutate, phase="snapshot", path="independent", quiescent=not rewrites_platform
    )
    assert result.status == ("INFRASTRUCTURE_ERROR" if rewrites_platform else "FAILED"), result.errors


def test_cleanup_admission_discovery_keeps_an_original_failed_observation() -> None:
    value = _standard_retirement_snapshot("evidence")
    victim = value["attempts"][0]
    bad = {
        "requestId": victim["requestId"],
        "ownerRequestId": victim["requestId"],
        "executionArn": victim["executionArn"],
        "atMs": 50,
        "method": "CheckpointDurableExecution",
        "outcome": "DISPATCHED",
    }
    value["sdkRequests"].append(bad)
    _complete_fixture_sdk_calls(value)
    original_requests = deepcopy(value["sdkRequests"])

    def mutate(snapshot: dict[str, Any]) -> None:
        snapshot["sdkRequests"].append({**bad, "atMs": 51})
        _complete_fixture_sdk_calls(snapshot)

    result = _run_standard_cleanup_discovery(value, mutate, phase="retire", path="independent", originally_failed=True)
    assert result.status == "FAILED"
    assert result.evidence["sdkRequests"] == original_requests


class ReplacingEvidenceAdapter(FailureAdapter):
    def __init__(self, phase: str, inventory: str, outcome: str, complete_retirement: bool) -> None:
        super().__init__(phase="placement" if phase == "partial" else "assertion")
        self.discovery_phase = phase
        self.inventory = inventory
        self.outcome = outcome
        self.complete_retirement = complete_retirement

    def discover(self, context: ScenarioContext, phase: str) -> None:
        if context.scenario.case_id != "failed" or phase != self.discovery_phase:
            return
        if self.inventory == "control":
            context.evidence.setdefault("resourcesCreated", []).append(
                {"id": "control:failed", "kind": "control", "ownerCaseId": "failed"}
            )
        else:
            context.evidence.setdefault("faultsCreated", []).append({"id": "new-fault", "ownerCaseId": "failed"})
        # The discovery exists only on the object supplied to this method;
        # neither the replacement nor the collector's response contains it.
        context.evidence = self.observations(context)
        if self.outcome == "exception":
            raise RuntimeError("adapter failed after replacing its input evidence")
        if self.outcome == "cancelled":
            raise asyncio.CancelledError

    async def start(self, context: ScenarioContext) -> None:
        await super().start(context)
        self.discover(context, "start")
        context.evidence = self.observations(context)

    async def place(self, context: ScenarioContext) -> None:
        self.discover(context, "place")
        await super().place(context)

    async def execute(self, context: ScenarioContext) -> None:
        self.discover(context, "execute")
        await super().execute(context)

    async def collect(self, context: ScenarioContext) -> Mapping[str, Any]:
        self.discover(context, "normal")
        return await super().collect(context)

    async def collect_partial(self, context: ScenarioContext) -> Any:
        self.discover(context, "partial")
        return await super().collect_partial(context)

    async def retire(self, context: ScenarioContext) -> bool:
        await super().retire(context)
        if context.scenario.case_id == "failed" and self.complete_retirement:
            final = context.retirement_evidence
            if self.inventory == "control":
                _add_retained_resource(final, "control", "DELETED")
            else:
                final["faultsCreated"].append({"id": "new-fault", "ownerCaseId": "failed"})
                final["retirement"]["faults"].append(
                    {"id": "new-fault", "ownerCaseId": "failed", "releasedAtMs": 20900, "mode": "normal"}
                )
        return True


@pytest.mark.parametrize("phase", ["normal", "partial", "start", "place", "execute"])
@pytest.mark.parametrize("inventory", ["control", "fault"])
@pytest.mark.parametrize("outcome", ["response", "exception"])
@pytest.mark.parametrize("complete_retirement", [False, True])
def test_phase_input_objects_retain_discoveries_when_context_is_replaced(
    phase: str, inventory: str, outcome: str, complete_retirement: bool
) -> None:
    adapter = ReplacingEvidenceAdapter(phase, inventory, outcome, complete_retirement)
    (failed, following), scheduler = _run(adapter)
    records = failed.evidence["resourcesCreated" if inventory == "control" else "faultsCreated"]
    assert any(item["id"] == ("control:failed" if inventory == "control" else "new-fault") for item in records)
    assert failed.status != "PASSED"
    assert ORIGINAL_FAILURE in failed.errors
    if complete_retirement and (phase != "partial" or outcome == "response"):
        assert following.status == "PASSED", following.errors
        assert adapter.finished == [True]
        assert scheduler.quarantine_reason(PROFILE.resource_id) is None
    else:
        assert following.status == "INFRASTRUCTURE_ERROR", following.errors
        assert adapter.started == ["failed"]
        assert adapter.finished == [False]
        assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("phase", ["normal", "partial"])
def test_replaced_collector_input_keeps_newest_returned_completion_and_sequence(phase: str) -> None:
    class EnrichingCollectorAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            if phase == "normal":
                observed["behaviorErrors"] = []
            return observed

        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            context.evidence = self.observations(context)
            if context.scenario.case_id == "failed":
                context.evidence["attempts"][0].update(startedAtMs=150, endedAtMs=800)
                context.evidence["histories"][0].update(status="RUNNING", result=None)

        def completed(self, context: ScenarioContext) -> dict[str, Any]:
            context.evidence["resourcesCreated"].append(
                {"id": "control:failed", "kind": "control", "ownerCaseId": "failed"}
            )
            context.evidence = self.observations(context)
            final = deepcopy(self.snapshots["failed"])
            _add_retained_resource(final, "control", "DELETED")
            final["histories"][0]["result"] = {"completed": True}
            for event in final["events"]:
                event["sequence"] += 1000
            self.snapshots["failed"] = final
            return self.observations(context)

        async def collect(self, context: ScenarioContext) -> Mapping[str, Any]:
            if context.scenario.case_id == "failed":
                return self.completed(context)
            return await super().collect(context)

        async def collect_partial(self, context: ScenarioContext) -> Any:
            return self.completed(context)

    adapter = EnrichingCollectorAdapter(phase="assertion" if phase == "normal" else "placement")
    (collected, following), scheduler = _run(adapter)
    assert not any("omitted or changed retained" in error for error in collected.errors), collected.errors
    assert any(item["id"] == "control:failed" for item in collected.evidence["resourcesCreated"])
    assert len(collected.evidence["histories"]) == 1
    assert collected.evidence["histories"][0]["result"] == {"completed": True}
    assert collected.evidence["attempts"][0]["startedAtMs"] == 100
    assert collected.evidence["attempts"][0]["endedAtMs"] == 1000
    assert all(event["sequence"] >= 1000 for event in collected.evidence["events"])
    assert len({event["sequence"] for event in collected.evidence["events"]}) == len(collected.evidence["events"])
    if phase == "normal":
        assert collected.status == "PASSED", collected.errors
    else:
        assert ORIGINAL_FAILURE in collected.errors
    assert following.status == "PASSED", following.errors
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


@pytest.mark.parametrize("phase", ["normal", "partial"])
@pytest.mark.parametrize("complete_retirement", [False, True])
def test_cancelled_collectors_retain_discoveries_from_replaced_input_objects(
    phase: str, complete_retirement: bool
) -> None:
    adapter = ReplacingEvidenceAdapter(phase, "control", "cancelled", complete_retirement)
    scheduler = ResourceScheduler()
    emitted: list[ScenarioResult] = []

    async def exercise() -> None:
        try:
            await run_scenarios(
                [_case("failed")],
                [PROFILE],
                adapter,
                run_id="run",
                validator=_validator,
                scheduler=scheduler,
                on_result=emitted.append,
            )
        except asyncio.CancelledError:
            assert phase == "normal"

    asyncio.run(exercise())
    failed = emitted[-1]
    assert any(item["id"] == "control:failed" for item in failed.evidence["resourcesCreated"])
    assert failed.status != "PASSED"
    assert not scheduler._resources[PROFILE.resource_id].owners
    if not complete_retirement or phase == "partial":
        assert adapter.finished == [False]
        assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize(
    "kind,outcome",
    [("callback", "SUCCEEDED"), ("callback", "FAILED"), ("callback", "TIMED_OUT"), ("control", "DELETED")],
)
def test_settled_callback_and_control_resources_allow_failed_target_reuse(kind: str, outcome: str) -> None:
    class RetainedResourceAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            _add_retained_resource(self.snapshots[context.scenario.case_id], kind, outcome)

    adapter = RetainedResourceAdapter(phase="assertion")
    (failed, following), scheduler = _run(adapter)
    assert failed.status == "FAILED"
    assert ORIGINAL_FAILURE in failed.errors
    assert validate_retirement_evidence(failed.retirement_evidence) == []
    assert following.status == "PASSED", following.errors
    assert adapter.started == ["failed", "next"]
    assert adapter.finished == [True]
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


@pytest.mark.parametrize("kind,outcome", [("callback", "PENDING"), ("callback", "QUIESCENT"), ("control", "CREATED")])
def test_retained_resources_require_terminal_outcomes_in_both_audits(kind: str, outcome: str) -> None:
    value = _snapshot(ScenarioContext("run", _case("failed"), PROFILE))
    _add_retained_resource(value, kind, outcome)
    value["expectedHistories"] = {"arn:execution:failed:victim": {"status": "SUCCEEDED", "result": None}}
    assert any(kind in error for error in validate_retirement_evidence(value))
    assert any(kind in error for error in validate_common_evidence(value, requirement_ids=("fixture-check",)))


@pytest.mark.parametrize("kind,outcome", [("callback", "SUCCEEDED"), ("control", "DELETED")])
def test_additional_resource_completion_still_requires_retained_provider_evidence(kind: str, outcome: str) -> None:
    value = _snapshot(ScenarioContext("run", _case("failed"), PROFILE))
    _add_retained_resource(value, kind, outcome)
    value["retirement"]["resources"][-1].pop("evidenceUri")
    assert any("evidenceUri" in error for error in validate_retirement_evidence(value))


def test_unknown_resource_kind_cannot_claim_independent_retirement() -> None:
    value = _snapshot(ScenarioContext("run", _case("failed"), PROFILE))
    _add_retained_resource(value, "unrecognized", "SUCCEEDED")
    assert any("no completion contract" in error for error in validate_retirement_evidence(value))


@pytest.mark.parametrize("phase", ["placement", "execute", "collection", "assertion"])
def test_failed_case_keeps_observations_and_error_while_audited_cleanup_permits_reuse(phase: str) -> None:
    adapter = FailureAdapter(phase=phase)
    results, scheduler = _run(adapter)
    failed, following = results
    assert failed.status != "PASSED"
    assert ORIGINAL_FAILURE in failed.errors
    if phase != "assertion":
        assert PHASE_FAILURE in failed.errors[0]
        assert adapter.steps.index(("failed", "partial")) < adapter.steps.index(("failed", "release"))
    else:
        assert ("failed", "partial") not in adapter.steps
    assert failed.evidence["capturedBeforeCleanup"] is True
    assert failed.evidence["behaviorErrors"] == [ORIGINAL_FAILURE]
    assert "behaviorErrors" not in failed.retirement_evidence
    assert validate_retirement_evidence(failed.retirement_evidence) == []
    assert following.status == "PASSED", following.errors
    assert adapter.started == ["failed", "next"]
    assert adapter.steps.index(("failed", "retire")) < adapter.steps.index(("next", "collect"))
    assert adapter.finished == [True]
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


def test_missing_unlaunched_retry_remains_failed_but_does_not_invent_a_live_owner() -> None:
    adapter = FailureAdapter()
    results, scheduler = _run(adapter, retry=True)
    failed, following = results
    assert PHASE_FAILURE in failed.errors[0]
    assert failed.status != "PASSED"
    assert len(failed.attempts) == len(failed.retirement_evidence["attempts"]) == 1
    assert set(failed.retirement_evidence["selectedRequests"]) == {"victim"}
    assert following.status == "PASSED", following.errors
    assert adapter.finished == [True]
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


@pytest.mark.parametrize("phase", ["execute", "assertion"])
@pytest.mark.parametrize(
    "damage",
    [
        "missing-audit",
        "missing-io-exit",
        "missing-root-exit",
        "unknown-launch",
        "dropped-known-start",
        "dropped-known-fault",
        "malformed-final",
        "active-execution",
        "reversed-equal-time-pair",
    ],
)
def test_failed_case_cannot_repeat_its_original_error_to_bypass_retirement(phase: str, damage: str) -> None:
    adapter = FailureAdapter(phase=phase, damage=damage)
    results, scheduler = _run(adapter)
    failed, following = results
    assert failed.status != "PASSED"
    assert ORIGINAL_FAILURE in failed.errors
    if phase == "assertion" and damage == "missing-audit":
        # Normal retirement still carries full physical evidence. An SDK
        # assertion alone does not require a second retirement snapshot.
        assert failed.status == "FAILED"
        assert following.status == "PASSED", following.errors
        assert adapter.finished == [True]
        assert scheduler.quarantine_reason(PROFILE.resource_id) is None
        return
    assert any("retirement" in error.lower() for error in failed.errors)
    assert following.status == "INFRASTRUCTURE_ERROR"
    assert any("QuarantinedError" in error for error in following.errors)
    assert adapter.started == ["failed"]
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("damage", ["malformed-partial-object", "malformed-partial-attempts"])
def test_malformed_partial_evidence_cannot_release_the_target(damage: str) -> None:
    adapter = FailureAdapter(damage=damage)
    results, scheduler = _run(adapter)
    assert results[0].status != "PASSED"
    assert PHASE_FAILURE in results[0].errors[0]
    assert results[1].status == "INFRASTRUCTURE_ERROR"
    assert adapter.started == ["failed"]
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("cleanup_phase", ["release", "retire"])
def test_reordered_cleanup_inventories_do_not_require_a_new_independent_audit(cleanup_phase: str) -> None:
    class ReorderedAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            observed["behaviorErrors"] = []
            # Normal collection does not require the semantic sequence metadata
            # that the stronger independent physical retirement audit needs.
            for event in observed["events"]:
                event.pop("sequence")
            return observed

        def reorder(self, context: ScenarioContext) -> None:
            for inventory in ("attempts", "resourcesCreated", "faultsCreated", "histories", "sdkRequests", "events"):
                context.evidence[inventory].reverse()

        async def release(self, context: ScenarioContext) -> None:
            await super().release(context)
            if cleanup_phase == "release":
                self.reorder(context)

        async def retire(self, context: ScenarioContext) -> bool:
            if cleanup_phase == "retire":
                self.reorder(context)
            context.evidence["retirement"] = deepcopy(self.snapshots[context.scenario.case_id]["retirement"])
            return True

    adapter = ReorderedAdapter(phase="assertion")
    (observed, following), scheduler = _run(adapter)
    assert observed.status == following.status == "PASSED", observed.errors
    assert observed.retirement_evidence.get("retirement") == observed.evidence.get("retirement")
    assert observed.cleanup_evidence[cleanup_phase]
    assert adapter.finished == [True]
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


class CleanupIdentityAdapter(FailureAdapter):
    def __init__(
        self, phase: str, path: tuple[str, ...], value: Any, *, restore: bool = False, mutate_context: bool = False
    ) -> None:
        super().__init__(phase="assertion")
        self.cleanup_phase, self.path, self.value = phase, path, value
        self.restore, self.mutate_context = restore, mutate_context
        self.original_run_id = ""

    async def start(self, context: ScenarioContext) -> None:
        await super().start(context)
        self.original_run_id = context.run_id
        snapshot = self.snapshots[context.scenario.case_id]
        snapshot["histories"][0]["result"] = None
        snapshot["profile"]["recoveryPolicy"] = "reuse-worker"
        snapshot["artifact"]["build"] = 1

    def observations(self, context: ScenarioContext) -> dict[str, Any]:
        observed = super().observations(context)
        observed["behaviorErrors"] = []
        observed["expectedHistories"] = {
            history["executionArn"]: {"status": "SUCCEEDED", "result": None} for history in observed["histories"]
        }
        return observed

    def change_identity(self, context: ScenarioContext) -> None:
        if not self.path:
            context.evidence["artifact"] = {}
            context.evidence["profile"] = {}
            return
        target = context.evidence
        for key in self.path[:-1]:
            target = target[key]
        target[self.path[-1]] = deepcopy(self.value)
        if self.mutate_context:
            context.run_id = self.value

    async def release(self, context: ScenarioContext) -> None:
        await super().release(context)
        if context.scenario.case_id == "failed" and self.cleanup_phase == "release":
            self.change_identity(context)

    async def retire(self, context: ScenarioContext) -> bool:
        if context.scenario.case_id != "failed":
            return await super().retire(context)
        if self.cleanup_phase == "retire":
            self.change_identity(context)
        if self.restore:
            context.run_id = self.original_run_id
            context.evidence = self.observations(context)
        context.evidence["retirement"] = deepcopy(self.snapshots["failed"]["retirement"])
        return True


@pytest.mark.parametrize("phase", ["release", "retire"])
@pytest.mark.parametrize("restore", [False, True])
@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("runId",), "other-run"),
        (("caseId",), "other-case"),
        (("requirementId",), "other-requirement"),
        (("coverageProfile",), "other-suite"),
        (("runtime",), "python"),
        (("artifact", "sdk"), "sdk@other"),
        (("artifact", "conformance"), "other-validator"),
        (("artifact", "build"), True),
        (("profile", "functionArn"), "arn:function:other"),
        (("profile", "configurationDigest"), "sha256:other"),
        (("profile", "nativeConcurrency"), True),
        (("profile", "recoveryPolicy"), "replace-worker"),
        (("profile", "timeoutMs"), 501),
        (("artifact",), None),
        (("profile",), []),
    ],
)
def test_cleanup_cannot_change_reported_case_or_deployment_identity(
    phase: str, restore: bool, path: tuple[str, ...], value: Any
) -> None:
    adapter = CleanupIdentityAdapter(phase, path, value, restore=restore)
    profile = replace(PROFILE, recovery_policy="reuse-worker")
    scheduler = ResourceScheduler()
    failed, following = asyncio.run(
        run_scenarios(
            [_case("failed"), _case("next")],
            [profile],
            adapter,
            run_id="run",
            validator=_service_fact_validator,
            scheduler=scheduler,
        )
    )
    assert failed.status == "INFRASTRUCTURE_ERROR", failed.errors
    assert any(".".join(path) in error for error in failed.errors), failed.errors
    assert failed.evidence["artifact"]["sdk"] == profile.artifact_id
    assert failed.evidence["profile"]["configurationDigest"] == profile.configuration_id
    assert following.status == "INFRASTRUCTURE_ERROR", following.errors
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(profile.resource_id)


@pytest.mark.parametrize("phase", ["release", "retire"])
@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("artifact", "sdk"), PROFILE.artifact_id),
        (("artifact", "extra"), {"new": True}),
        (("profile", "extra"), "new"),
        ((), None),
    ],
)
def test_cleanup_accepts_unchanged_and_completed_identity_metadata(
    phase: str, path: tuple[str, ...], value: Any
) -> None:
    adapter = CleanupIdentityAdapter(phase, path, value, restore=True)
    profile = replace(PROFILE, recovery_policy="reuse-worker")
    scheduler = ResourceScheduler()
    result, following = asyncio.run(
        run_scenarios(
            [_case("failed"), _case("next")],
            [profile],
            adapter,
            run_id="run",
            validator=_service_fact_validator,
            scheduler=scheduler,
        )
    )
    assert result.status == following.status == "PASSED", result.errors
    assert result.retirement_evidence.get("retirement") == result.evidence.get("retirement")
    assert adapter.finished == [True]
    assert scheduler.quarantine_reason(profile.resource_id) is None


@pytest.mark.parametrize("phase", ["release", "retire"])
def test_cleanup_identity_checks_use_frozen_admission_when_context_changes(phase: str) -> None:
    adapter = CleanupIdentityAdapter(phase, ("runId",), "other-run", restore=True, mutate_context=True)
    failed, following = asyncio.run(
        run_scenarios(
            [_case("failed"), _case("next")],
            [PROFILE],
            adapter,
            run_id="run",
            validator=_service_fact_validator,
        )
    )
    assert failed.status == following.status == "INFRASTRUCTURE_ERROR", failed.errors
    assert any("runId" in error for error in failed.errors), failed.errors
    assert adapter.finished == [False]


def test_cleanup_recovery_policy_uses_declared_admission_when_not_observed_earlier() -> None:
    class InitiallyUndeclaredAdapter(CleanupIdentityAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            self.snapshots[context.scenario.case_id]["profile"].pop("recoveryPolicy")

    adapter = InitiallyUndeclaredAdapter("retire", ("profile", "recoveryPolicy"), "replace-worker")
    profile = replace(PROFILE, recovery_policy="reuse-worker")
    failed, following = asyncio.run(
        run_scenarios(
            [_case("failed"), _case("next")],
            [profile],
            adapter,
            run_id="run",
            validator=_service_fact_validator,
        )
    )
    assert failed.status == following.status == "INFRASTRUCTURE_ERROR", failed.errors
    assert "recoveryPolicy" not in failed.evidence["profile"]
    assert any("profile.recoveryPolicy" in error for error in failed.errors), failed.errors
    assert adapter.finished == [False]


def test_cleanup_identity_failure_does_not_block_other_profiles() -> None:
    other = replace(PROFILE, profile_id="other", resource_id="arn:function:other")
    finished: dict[str, bool] = {}
    first_admitted = asyncio.Event()

    class IsolatedAdapter(CleanupIdentityAdapter):
        async def prepare(self, profile: Profile) -> None:
            if profile.profile_id == other.profile_id:
                await first_admitted.wait()

        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            if context.scenario.case_id == "failed":
                first_admitted.set()

        async def finish(self, profile: Profile, *, quiescent: bool) -> None:
            finished[profile.profile_id] = quiescent

    adapter = IsolatedAdapter("release", ("artifact", "sdk"), "sdk@other", restore=True)
    scheduler = ResourceScheduler()
    failed, following, independent = asyncio.run(
        run_scenarios(
            [_case("failed"), _case("next"), replace(_case("independent"), profile_id=other.profile_id)],
            [PROFILE, other],
            adapter,
            run_id="run",
            validator=_service_fact_validator,
            scheduler=scheduler,
            scenario_concurrency=1,
        )
    )
    assert failed.status == following.status == "INFRASTRUCTURE_ERROR", failed.errors
    assert independent.status == "PASSED", independent.errors
    assert finished == {PROFILE.profile_id: False, other.profile_id: True}
    assert scheduler.quarantine_reason(PROFILE.resource_id)
    assert scheduler.quarantine_reason(other.resource_id) is None


def test_duplicate_native_ownership_quarantines_before_profile_finish() -> None:
    class DuplicateAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            request = "first-request"
            name = context.scenario.case_id
            original = _snapshot(context)
            old_request = original["attempts"][0]["requestId"]
            for record in (
                *original["attempts"],
                *original["events"],
                *original["resourcesCreated"],
                *original["retirement"]["resources"],
                original["selectedRequests"],
            ):
                for field, value in record.items():
                    if value == old_request:
                        record[field] = request
            self.snapshots[name] = original
            context.record_attempt(request, role="victim", requestId=request)

    adapter = DuplicateAdapter()
    scheduler = ResourceScheduler()
    results = asyncio.run(
        run_scenarios(
            [_case("first"), _case("second")],
            [PROFILE],
            adapter,
            run_id="run",
            validator=_service_fact_validator,
            scheduler=scheduler,
            scenario_concurrency=1,
        )
    )
    for result in results:
        assert result.status == "FAILED", result.errors
        assert "INCOMPLETE: requestId first-request is claimed by multiple cases" in result.errors
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


class IndependentCleanupIdentityAdapter(CleanupIdentityAdapter):
    async def retire(self, context: ScenarioContext) -> bool:
        final = deepcopy(self.snapshots[context.scenario.case_id])
        if context.scenario.case_id == "failed":
            target = final
            for key in self.path[:-1]:
                target = target[key]
            target[self.path[-1]] = deepcopy(self.value)
            if self.mutate_context:
                context.profile = replace(context.profile, recovery_policy=self.value)
        context.retirement_evidence = final
        context.evidence["retirement"] = deepcopy(self.snapshots[context.scenario.case_id]["retirement"])
        return True


@pytest.mark.parametrize(
    "path,value",
    [
        (("artifact", "sdk"), "sdk@other"),
        (("profile", "configurationDigest"), "sha256:other"),
        (("profile", "nativeConcurrency"), 2),
        (("profile", "timeoutMs"), 501),
        (("profile", "recoveryPolicy"), "replace-worker"),
    ],
)
def test_independent_retirement_cannot_change_frozen_deployment_identity(path: tuple[str, ...], value: Any) -> None:
    adapter = IndependentCleanupIdentityAdapter("independent", path, value)
    profile = replace(PROFILE, recovery_policy="reuse-worker")
    scheduler = ResourceScheduler()
    failed, following = asyncio.run(
        run_scenarios(
            [_case("failed"), _case("next")],
            [profile],
            adapter,
            run_id="run",
            validator=_service_fact_validator,
            scheduler=scheduler,
        )
    )
    assert failed.status == "INFRASTRUCTURE_ERROR", failed.errors
    assert any("Cleanup retirement snapshot" in error and ".".join(path) in error for error in failed.errors), (
        failed.errors
    )
    assert failed.evidence["profile"]["timeoutMs"] == 500
    assert failed.evidence["profile"]["recoveryPolicy"] == "reuse-worker"
    assert following.status == "INFRASTRUCTURE_ERROR"
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(profile.resource_id)


def test_independent_retirement_policy_cannot_be_authorized_by_mutating_context() -> None:
    class MissingObservedPolicyAdapter(IndependentCleanupIdentityAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            self.snapshots[context.scenario.case_id]["profile"].pop("recoveryPolicy")

    adapter = MissingObservedPolicyAdapter(
        "independent", ("profile", "recoveryPolicy"), "replace-worker", mutate_context=True
    )
    profile = replace(PROFILE, recovery_policy="reuse-worker")
    failed, following = asyncio.run(
        run_scenarios(
            [_case("failed"), _case("next")],
            [profile],
            adapter,
            run_id="run",
            validator=_service_fact_validator,
        )
    )
    assert failed.status == following.status == "INFRASTRUCTURE_ERROR", failed.errors
    assert "recoveryPolicy" not in failed.evidence["profile"]
    assert any("Cleanup retirement snapshot: profile.recoveryPolicy" in error for error in failed.errors)
    assert adapter.finished == [False]


@pytest.mark.parametrize(
    "path,value",
    [
        (("artifact", "extra"), {"new": True}),
        (("profile", "extra"), "new"),
        (("profile", "recoveryPolicy"), "reuse-worker"),
    ],
)
def test_independent_retirement_can_fill_unobserved_identity_metadata(path: tuple[str, ...], value: Any) -> None:
    class MissingObservedPolicyAdapter(IndependentCleanupIdentityAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            self.snapshots[context.scenario.case_id]["profile"].pop("recoveryPolicy")

    adapter = MissingObservedPolicyAdapter("independent", path, value)
    profile = replace(PROFILE, recovery_policy="reuse-worker")
    scheduler = ResourceScheduler()
    result, following = asyncio.run(
        run_scenarios(
            [_case("failed"), _case("next")],
            [profile],
            adapter,
            run_id="run",
            validator=_service_fact_validator,
            scheduler=scheduler,
        )
    )
    assert result.status == following.status == "PASSED", result.errors
    assert validate_retirement_evidence(result.retirement_evidence) == []
    assert adapter.finished == [True]
    assert scheduler.quarantine_reason(profile.resource_id) is None


@pytest.mark.parametrize("cleanup_phase", ["release", "retire"])
@pytest.mark.parametrize("clear_later", [False, True])
@pytest.mark.parametrize(
    ("kind", "payload", "valid"),
    [
        ("collectionErrors", ["history query failed"], False),
        ("collectionErrors", None, False),
        ("collectionErrors", "history query failed", False),
        ("collectionErrors", {}, False),
        ("collectionErrors", [""], False),
        ("collectionErrors", ["   "], False),
        ("collectionErrors", [None], False),
        ("collectionErrors", [False], False),
        ("collectionErrors", [], True),
        ("observation_error", None, False),
        ("control_error", None, False),
    ],
)
def test_cleanup_source_failures_remain_visible_and_quarantine_only_their_profile(
    cleanup_phase: str, clear_later: bool, kind: str, payload: Any, valid: bool
) -> None:
    other = replace(PROFILE, profile_id="other", resource_id="arn:function:other")
    first_admitted = asyncio.Event()
    finished: dict[str, bool] = {}

    class SourceErrorAdapter(FailureAdapter):
        async def prepare(self, profile: Profile) -> None:
            if profile.profile_id == other.profile_id:
                await first_admitted.wait()

        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            self.snapshots[context.scenario.case_id]["histories"][0]["result"] = None
            if context.scenario.case_id == "failed":
                first_admitted.set()

        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            observed["behaviorErrors"] = []
            observed["expectedHistories"] = {
                history["executionArn"]: {"status": "SUCCEEDED", "result": None} for history in observed["histories"]
            }
            return observed

        def report_failure(self, context: ScenarioContext) -> None:
            if kind == "collectionErrors":
                context.evidence["collectionErrors"] = deepcopy(payload)
            else:
                attempt = context.evidence["attempts"][0]
                context.evidence["events"].append(
                    {
                        "kind": kind,
                        "requestId": attempt["requestId"],
                        "ownerRequestId": attempt["requestId"],
                        "executionArn": attempt["executionArn"],
                        "atMs": 1500,
                        "sequence": max(event["sequence"] for event in context.evidence["events"]) + 1,
                    }
                )

        async def release(self, context: ScenarioContext) -> None:
            await super().release(context)
            if context.scenario.case_id == "failed" and cleanup_phase == "release":
                self.report_failure(context)

        async def retire(self, context: ScenarioContext) -> bool:
            if context.scenario.case_id != "failed":
                return await super().retire(context)
            if cleanup_phase == "retire":
                self.report_failure(context)
            if clear_later:
                context.evidence = self.observations(context)
            context.evidence["retirement"] = deepcopy(self.snapshots["failed"]["retirement"])
            return True

        async def finish(self, profile: Profile, *, quiescent: bool) -> None:
            finished[profile.profile_id] = quiescent

    adapter = SourceErrorAdapter(phase="assertion")
    scheduler = ResourceScheduler()
    cases = [_case("failed"), _case("next"), replace(_case("unrelated"), profile_id=other.profile_id)]
    first, following, unrelated = asyncio.run(
        run_scenarios(
            cases,
            [PROFILE, other],
            adapter,
            run_id="run",
            validator=_service_fact_validator,
            scheduler=scheduler,
            scenario_concurrency=1,
        )
    )
    assert first.evidence.get("collectionErrors", []) == []
    assert not any(event["kind"] in ("observation_error", "control_error") for event in first.evidence["events"])
    snapshots = first.cleanup_evidence[cleanup_phase]
    if kind == "collectionErrors":
        assert any("collectionErrors" in snapshot and snapshot["collectionErrors"] == payload for snapshot in snapshots)
    else:
        assert any(event["kind"] == kind for snapshot in snapshots for event in snapshot.get("events", []))
    assert unrelated.status == "PASSED", unrelated.errors
    assert finished[other.profile_id] is True
    assert scheduler.quarantine_reason(other.resource_id) is None
    if valid:
        assert first.status == following.status == "PASSED", first.errors
        assert finished[PROFILE.profile_id] is True
        assert scheduler.quarantine_reason(PROFILE.resource_id) is None
    else:
        assert first.status == "INFRASTRUCTURE_ERROR", first.errors
        assert any(kind in error for error in first.errors), first.errors
        assert following.status == "INFRASTRUCTURE_ERROR", following.errors
        assert finished[PROFILE.profile_id] is False
        assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("cleanup_phase", ["release", "retire"])
@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize("outcome", ["return", "raise", "replace-none"])
@pytest.mark.parametrize("complete_retirement", [False, True])
@pytest.mark.parametrize("original_failed", [False, True])
def test_cleanup_discoveries_require_retirement_without_rewriting_behavior(
    cleanup_phase: str, path: str, outcome: str, complete_retirement: bool, original_failed: bool
) -> None:
    checked_phases: list[str] = []

    def validate_frozen(evidence: Mapping[str, Any], *, phase: str = "final") -> list[str]:
        if evidence["caseId"] == "failed":
            checked_phases.append(phase)
            assert evidence["behaviorErrors"] == ([ORIGINAL_FAILURE] if original_failed else [])
        return _validator(evidence, phase=phase)

    class DiscoveryAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            if not original_failed:
                observed["behaviorErrors"] = []
            return observed

        def discover(self, context: ScenarioContext) -> None:
            context.evidence["resourcesCreated"].append(
                {"id": "control:failed", "kind": "control", "ownerCaseId": "failed"}
            )
            if outcome == "raise":
                raise RuntimeError("cleanup failed after discovering a control")
            if outcome == "replace-none":
                context.evidence = None  # type: ignore[assignment]

        async def release(self, context: ScenarioContext) -> None:
            await super().release(context)
            if context.scenario.case_id == "failed" and cleanup_phase == "release":
                self.discover(context)

        async def retire(self, context: ScenarioContext) -> bool:
            if context.scenario.case_id != "failed":
                return await super().retire(context)
            self.steps.append(("failed", "retire"))
            final = deepcopy(self.snapshots["failed"])
            if complete_retirement:
                _add_retained_resource(final, "control", "DELETED")
            if path == "independent":
                context.retirement_evidence = deepcopy(final)
            if cleanup_phase == "retire":
                self.discover(context)
            if outcome != "replace-none" or cleanup_phase != "retire":
                context.evidence = final
            return True

    adapter = DiscoveryAdapter(phase="assertion")
    (observed, following), scheduler = _run(adapter, validator=validate_frozen)
    assert observed.evidence["behaviorErrors"] == ([ORIGINAL_FAILURE] if original_failed else [])
    assert all(resource["id"] != "control:failed" for resource in observed.evidence["resourcesCreated"])
    if original_failed:
        assert observed.status == "FAILED"
        assert ORIGINAL_FAILURE in observed.errors
    valid_cleanup = complete_retirement and outcome == "return"
    if valid_cleanup:
        assert validate_retirement_evidence(observed.retirement_evidence) == []
        assert following.status == "PASSED", following.errors
        assert adapter.finished == [True]
        assert scheduler.quarantine_reason(PROFILE.resource_id) is None
        if not original_failed:
            assert observed.status == "PASSED", observed.errors
            assert checked_phases == ["observations", "final"]
            original_retirement = observed.evidence["retirement"]
            full_retirement = observed.retirement_evidence["retirement"]
            assert original_retirement["retiredAtMs"] == full_retirement["retiredAtMs"]
            assert all(resource["id"] != "control:failed" for resource in original_retirement["resources"])
            assert any(resource["id"] == "control:failed" for resource in full_retirement["resources"])
    else:
        assert observed.status != "PASSED"
        assert following.status == "INFRASTRUCTURE_ERROR", following.errors
        assert adapter.started == ["failed"]
        assert adapter.finished == [False]
        assert scheduler.quarantine_reason(PROFILE.resource_id)
    assert any(
        resource.get("id") == "control:failed"
        for snapshot in observed.cleanup_evidence[cleanup_phase]
        for resource in snapshot.get("resourcesCreated", [])
    )


@pytest.mark.parametrize("cleanup_phase", ["release", "retire"])
@pytest.mark.parametrize("inventory", ["attempts", "faultsCreated", "histories", "events", "sdkRequests"])
def test_cleanup_cannot_omit_other_discovered_ownership_or_activity(cleanup_phase: str, inventory: str) -> None:
    class DiscoveryAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            observed["behaviorErrors"] = []
            return observed

        def discover(self, context: ScenarioContext) -> None:
            attempt = context.evidence["attempts"][0]
            record = {
                "requestId": attempt["requestId"],
                "ownerRequestId": attempt["requestId"],
                "executionArn": attempt["executionArn"],
                "atMs": 1500,
            }
            extra = {
                "attempts": {**attempt, "requestId": "new-request", "executionArn": "new-execution"},
                "faultsCreated": {"id": "new-fault", "ownerCaseId": "failed"},
                "histories": {"executionArn": "new-execution", "ownerCaseId": "failed", "status": "RUNNING"},
                "events": {**record, "kind": "io_start", "taskId": "cleanup-io"},
                "sdkRequests": {**record, "method": "CheckpointDurableExecution", "outcome": "REJECTED"},
            }
            context.evidence[inventory].append(extra[inventory])

        async def release(self, context: ScenarioContext) -> None:
            await super().release(context)
            if context.scenario.case_id == "failed" and cleanup_phase == "release":
                self.discover(context)

        async def retire(self, context: ScenarioContext) -> bool:
            if context.scenario.case_id != "failed":
                return await super().retire(context)
            if cleanup_phase == "retire":
                self.discover(context)
            context.evidence = deepcopy(self.snapshots["failed"])
            return True

    adapter = DiscoveryAdapter(phase="assertion")
    (failed, following), scheduler = _run(adapter)
    assert failed.status == "INFRASTRUCTURE_ERROR", failed.errors
    assert any(
        len(snapshot.get(inventory, [])) > len(failed.evidence[inventory])
        for snapshot in failed.cleanup_evidence[cleanup_phase]
    )
    assert following.status == "INFRASTRUCTURE_ERROR"
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("cleanup_phase", ["release", "retire"])
def test_cleanup_discoveries_without_original_work_still_require_an_audit(cleanup_phase: str) -> None:
    class EmptyOriginalAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            if context.scenario.case_id == "failed":
                self.started.append("failed")
                raise RuntimeError("failed before launching work")
            await super().start(context)

        def discover(self, context: ScenarioContext) -> None:
            context.evidence["resourcesCreated"] = [{"id": "new-control", "kind": "control", "ownerCaseId": "failed"}]

        async def release(self, context: ScenarioContext) -> None:
            if context.scenario.case_id != "failed":
                await super().release(context)
            elif cleanup_phase == "release":
                self.discover(context)

        async def retire(self, context: ScenarioContext) -> bool:
            if context.scenario.case_id != "failed":
                return await super().retire(context)
            if cleanup_phase == "retire":
                self.discover(context)
            context.evidence = {}
            return True

    adapter = EmptyOriginalAdapter(phase="assertion")
    (failed, following), scheduler = _run(adapter)
    assert failed.evidence == {}
    assert failed.attempts == []
    assert any(snapshot.get("resourcesCreated") for snapshot in failed.cleanup_evidence[cleanup_phase])
    assert following.status == "INFRASTRUCTURE_ERROR", following.errors
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("cleanup_phase", ["release", "retire"])
@pytest.mark.parametrize("raises", [False, True])
@pytest.mark.parametrize("original_failed", [False, True])
@pytest.mark.parametrize("malformed", [None, [], ["retirement"], "retirement", 42])
def test_malformed_cleanup_context_preserves_results_and_quarantines_only_its_target(
    cleanup_phase: str, raises: bool, original_failed: bool, malformed: Any
) -> None:
    other = replace(PROFILE, profile_id="other", resource_id="arn:function:other")
    finished: dict[str, bool] = {}
    failed_admitted = asyncio.Event()

    class CorruptingAdapter(FailureAdapter):
        async def prepare(self, profile: Profile) -> None:
            if profile.profile_id == other.profile_id:
                await failed_admitted.wait()

        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            if context.scenario.case_id == "failed":
                failed_admitted.set()

        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            if not original_failed:
                observed["behaviorErrors"] = []
            return observed

        def corrupt(self, context: ScenarioContext) -> None:
            context.evidence = deepcopy(malformed)
            if raises:
                raise RuntimeError("cleanup failed after corrupting context evidence")

        async def release(self, context: ScenarioContext) -> None:
            if context.scenario.case_id == "failed" and cleanup_phase == "release":
                self.steps.append(("failed", "release"))
                self.corrupt(context)
            else:
                await super().release(context)

        async def retire(self, context: ScenarioContext) -> bool:
            if context.scenario.case_id != "failed":
                return await super().retire(context)
            self.steps.append(("failed", "retire"))
            # An independently valid snapshot and True result cannot excuse
            # malformed cleanup output or bypass case-lease reconciliation.
            context.retirement_evidence = deepcopy(self.snapshots["failed"])
            if cleanup_phase == "retire":
                self.corrupt(context)
            return True

        async def finish(self, profile: Profile, *, quiescent: bool) -> None:
            finished[profile.profile_id] = quiescent

    adapter = CorruptingAdapter(phase="assertion")
    scheduler = ResourceScheduler()
    emitted: list[ScenarioResult] = []
    cases = [_case("failed"), _case("next"), replace(_case("unrelated"), profile_id=other.profile_id)]
    failed, following, unrelated = asyncio.run(
        run_scenarios(
            cases,
            [PROFILE, other],
            adapter,
            run_id="run",
            validator=_validator,
            scheduler=scheduler,
            scenario_concurrency=1,
            on_result=emitted.append,
        )
    )
    assert failed.status == ("FAILED" if original_failed else "INFRASTRUCTURE_ERROR")
    assert any("Cleanup evidence retention" in error for error in failed.errors), failed.errors
    assert failed.evidence["capturedBeforeCleanup"] is True
    assert failed.evidence["behaviorErrors"] == ([ORIGINAL_FAILURE] if original_failed else [])
    assert validate_retirement_evidence(failed.retirement_evidence) == []
    assert following.status == "INFRASTRUCTURE_ERROR", following.errors
    assert any("QuarantinedError" in error for error in following.errors)
    assert unrelated.status == "PASSED", unrelated.errors
    assert adapter.started == ["failed", "unrelated"]
    assert {result.case_id for result in emitted} == {"failed", "next", "unrelated"}
    assert finished == {PROFILE.profile_id: False, other.profile_id: True}
    assert scheduler.quarantine_reason(PROFILE.resource_id)
    assert scheduler.quarantine_reason(other.resource_id) is None
    assert scheduler._resources[PROFILE.resource_id].owners == {}
    assert scheduler._active["run"] == 0


@pytest.mark.parametrize("cancel_run", [False, True])
def test_interrupted_phase_cannot_use_a_cleanup_snapshot_to_bypass_quarantine(cancel_run: bool) -> None:
    async def exercise() -> None:
        entered = asyncio.Event()

        class BlockingAdapter(FailureAdapter):
            async def execute(self, context: ScenarioContext) -> None:
                entered.set()
                await asyncio.Event().wait()

        adapter = BlockingAdapter()
        scheduler = ResourceScheduler()
        observed: list[ScenarioResult] = []
        running = asyncio.create_task(
            run_scenarios(
                [_case("failed")],
                [PROFILE],
                adapter,
                run_id="run",
                validator=_validator,
                scheduler=scheduler,
                budgets=Budgets(execution=0.05 if not cancel_run else 10),
                on_result=observed.append,
            )
        )
        await entered.wait()
        if cancel_run:
            running.cancel()
            with pytest.raises(asyncio.CancelledError):
                await running
        else:
            results = await running
            assert results[0].status != "PASSED"
            assert any("exceeded" in error for error in results[0].errors)
        assert observed and observed[0].status != "PASSED"
        assert ("failed", "partial") not in adapter.steps
        assert ("failed", "release") in adapter.steps
        assert ("failed", "retire") in adapter.steps
        assert adapter.finished == [False]
        assert scheduler.quarantine_reason(PROFILE.resource_id)

    asyncio.run(exercise())


def test_client_launch_discovered_after_collection_fails_offline_after_safe_retirement() -> None:
    class LateRequestAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            observed["behaviorErrors"] = []
            return observed

        async def retire(self, context: ScenarioContext) -> bool:
            await super().retire(context)
            if context.scenario.case_id != "failed":
                return True
            # An internally complete final audit cannot replace the passing
            # case's earlier observation window with newly launched work.
            extra = _snapshot(ScenarioContext(context.run_id, _case("retirement-only"), PROFILE))
            request = extra["attempts"][0]
            request["attempt"] = 2
            request["startedAtMs"] += 20_000
            request["endedAtMs"] += 20_000
            context.record_attempt(request["requestId"], role="victim", requestId=request["requestId"])
            for event in extra["events"]:
                event["atMs"] += 20_000
            for key in ("histories", "resourcesCreated", "faultsCreated"):
                for item in extra[key]:
                    item["ownerCaseId"] = context.scenario.case_id
            for key, field in (("resources", "settledAtMs"), ("faults", "releasedAtMs")):
                for item in extra["retirement"][key]:
                    item["ownerCaseId"] = context.scenario.case_id
                    item[field] += 20_000
            final = context.retirement_evidence
            for key in ("attempts", "events", "histories", "resourcesCreated", "faultsCreated"):
                final[key].extend(extra[key])
            final["observationEndedAtMs"] = 40_000
            final["collection"]["watermarks"] = {name: 40_000 for name in final["collection"]["watermarks"]}
            for key in ("resources", "faults"):
                final["retirement"][key].extend(extra["retirement"][key])
            final["retirement"]["retiredAtMs"] = 41_000
            context.evidence["retirement"] = deepcopy(final["retirement"])
            assert validate_retirement_evidence(final) == []
            return True

    adapter = LateRequestAdapter(phase="none")
    results, scheduler = _run(adapter)
    assert results[0].status == "INCOMPLETE_EVIDENCE"
    assert any("client-journaled launches" in error for error in results[0].errors)
    assert len(results[0].attempts) == 2
    assert len(results[0].evidence["attempts"]) == 1
    assert len(results[0].retirement_evidence["attempts"]) == 2
    assert results[1].status == "PASSED"
    assert adapter.started == ["failed", "next"]
    assert adapter.finished == [True]
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


@pytest.mark.parametrize("original_failed", [False, True])
@pytest.mark.parametrize(
    ("observed_outcome", "retired_outcome"),
    [("SUCCEEDED", "FAILED"), ("FAILED", "SUCCEEDED"), ("PENDING", "SUCCEEDED"), ("TIMED_OUT", "FAILED")],
)
def test_retirement_cannot_change_an_observed_native_invocation_outcome(
    original_failed: bool, observed_outcome: str, retired_outcome: str
) -> None:
    class ChangedOutcomeAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            snapshot = self.snapshots[context.scenario.case_id]
            snapshot["histories"][0]["result"] = None
            if context.scenario.case_id == "failed":
                snapshot["attempts"][0]["outcome"] = observed_outcome

        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            if not original_failed:
                observed["behaviorErrors"] = []
            observed["expectedHistories"] = {
                history["executionArn"]: {"status": "SUCCEEDED", "result": None} for history in observed["histories"]
            }
            return observed

        def damage_retirement(self, final: dict[str, Any]) -> None:
            final["attempts"][0]["outcome"] = retired_outcome

    adapter = ChangedOutcomeAdapter(phase="assertion")
    (observed, following), scheduler = _run(adapter, validator=_service_fact_validator)
    assert observed.status == ("FAILED" if original_failed else "INFRASTRUCTURE_ERROR"), observed.errors
    assert any("original invocation's outcome" in error for error in observed.errors), observed.errors
    assert observed.evidence["attempts"][0]["outcome"] == observed_outcome
    assert validate_retirement_evidence(observed.retirement_evidence) == []
    assert following.status == "INFRASTRUCTURE_ERROR", following.errors
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("phase", ["normal", "partial"])
def test_retirement_can_complete_a_previously_absent_native_outcome(phase: str) -> None:
    class CompletedOutcomeAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            context.evidence = self.observations(context)
            if context.scenario.case_id == "failed":
                context.evidence["attempts"][0].pop("outcome")

        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            if phase == "normal":
                observed["behaviorErrors"] = []
            return observed

    adapter = CompletedOutcomeAdapter(phase="assertion" if phase == "normal" else "placement")
    (observed, following), scheduler = _run(adapter)
    assert observed.evidence["attempts"][0]["outcome"] == "SUCCEEDED"
    assert following.status == "PASSED", following.errors
    if phase == "normal":
        assert observed.status == "PASSED", observed.errors
    else:
        assert ORIGINAL_FAILURE in observed.errors
    assert adapter.finished == [True]
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


def test_pending_invocation_and_its_resume_keep_independent_native_outcomes() -> None:
    class ResumingAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            snapshot = self.snapshots[context.scenario.case_id]
            snapshot["histories"][0]["result"] = None
            if context.scenario.case_id != "failed":
                return
            victim = snapshot["attempts"][0]
            victim["outcome"] = "PENDING"
            resumed = {
                **victim,
                "requestId": "resumed-request",
                "role": "resume",
                "outcome": "SUCCEEDED",
                "startedAtMs": 1100,
                "endedAtMs": 2000,
            }
            context.record_attempt(resumed["requestId"], role="resume", requestId=resumed["requestId"])
            snapshot["attempts"].append(resumed)
            snapshot["selectedRequests"]["resume"] = resumed["requestId"]
            snapshot["events"].extend(
                [
                    {
                        **event,
                        "requestId": resumed["requestId"],
                        "ownerRequestId": resumed["requestId"],
                        "atMs": event["atMs"] + 1000,
                    }
                    for event in list(snapshot["events"])
                ]
            )
            resource = {"id": resumed["requestId"], "kind": "invocation", "ownerCaseId": "failed"}
            snapshot["resourcesCreated"].append(resource)
            snapshot["retirement"]["resources"].append(
                {
                    **resource,
                    "settledAtMs": 20900,
                    "outcome": "QUIESCENT",
                    "evidenceUri": "memory://resumed-native-completion",
                }
            )

        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            observed["behaviorErrors"] = []
            observed["expectedHistories"] = {
                history["executionArn"]: {"status": "SUCCEEDED", "result": None} for history in observed["histories"]
            }
            return observed

    adapter = ResumingAdapter(phase="assertion")
    scheduler = ResourceScheduler()
    cases = [replace(_case("failed"), roles=("victim", "resume")), _case("next")]
    observed, following = asyncio.run(
        run_scenarios(
            cases,
            [PROFILE],
            adapter,
            run_id="run",
            validator=_service_fact_validator,
            scheduler=scheduler,
        )
    )
    assert observed.status == following.status == "PASSED", observed.errors
    before = [(item["requestId"], item["outcome"]) for item in observed.evidence["attempts"]]
    after = [(item["requestId"], item["outcome"]) for item in observed.retirement_evidence["attempts"]]
    assert before == after == [("request:failed:victim", "PENDING"), ("resumed-request", "SUCCEEDED")]
    assert adapter.finished == [True]
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


@pytest.mark.parametrize("path", ["normal", "independent", "failed"])
@pytest.mark.parametrize(
    "change",
    [
        "execution-result",
        "execution-error",
        "execution-status",
        "execution-null",
        "operation-result",
        "operation-error",
        "operation-status",
        "operation-null",
        "operation-drop",
        "operation-name",
        "operation-type",
        "operation-parentId",
    ],
)
def test_retirement_preserves_observed_terminal_service_facts(path: str, change: str) -> None:
    initial: dict[str, Any] = {"status": "SUCCEEDED", "result": "A", "operations": []}
    if change == "execution-error":
        initial = {"status": "FAILED", "error": {"message": "A"}, "operations": []}
    elif change == "execution-null":
        initial["result"] = None
    elif change.startswith("operation"):
        operation: dict[str, Any] = {
            "id": "step",
            "name": "stable-step",
            "type": "STEP",
            "parentId": None,
            "status": "SUCCEEDED",
            "result": "A",
        }
        if change == "operation-error":
            operation.update(status="FAILED", error={"value": True})
            operation.pop("result")
        elif change == "operation-null":
            operation["result"] = None
        initial["operations"] = [operation]

    class ChangedTerminalAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            snapshot = self.snapshots[context.scenario.case_id]
            snapshot["histories"][0].update(deepcopy(initial))
            for resource in snapshot["retirement"]["resources"]:
                if resource["kind"] == "execution":
                    resource["outcome"] = initial["status"]

        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            if path != "failed":
                observed["behaviorErrors"] = []
            observed["expectedHistories"] = {
                history["executionArn"]: deepcopy(initial) for history in observed["histories"]
            }
            return observed

        async def release(self, context: ScenarioContext) -> None:
            await super().release(context)
            if context.scenario.case_id == "failed":
                context.evidence["resourcesCreated"].append(
                    {"id": "control:failed", "kind": "control", "ownerCaseId": "failed"}
                )

        async def retire(self, context: ScenarioContext) -> bool:
            if context.scenario.case_id != "failed":
                return await super().retire(context)
            final = deepcopy(self.snapshots["failed"])
            _add_retained_resource(final, "control", "DELETED")
            history = final["histories"][0]
            if change == "operation-drop":
                history["operations"] = []
            else:
                target = history["operations"][0] if change.startswith("operation") else history
                if change.endswith("result"):
                    target["result"] = "B"
                elif change.endswith("error"):
                    target["error"] = {"value": 1} if change.startswith("operation") else {"message": "B"}
                elif change.endswith("null"):
                    target.pop("result")
                elif change.removeprefix("operation-") in ("name", "type", "parentId"):
                    target[change.removeprefix("operation-")] = "different-identity"
                else:
                    target.update(status="FAILED", error={"message": "B"})
                    target.pop("result")
            for resource in final["retirement"]["resources"]:
                if resource["kind"] == "execution":
                    resource["outcome"] = history["status"]
            if path == "normal":
                context.evidence = final
            else:
                context.retirement_evidence = final
                context.evidence["retirement"] = deepcopy(final["retirement"])
            return True

    adapter = ChangedTerminalAdapter(phase="assertion")
    (observed, following), scheduler = _run(adapter, validator=_service_fact_validator)
    assert observed.status != "PASSED", observed.errors
    assert any("Retirement" in error and "terminal" in error for error in observed.errors), observed.errors
    assert validate_retirement_evidence(observed.retirement_evidence) == []
    if path == "failed":
        assert ORIGINAL_FAILURE in observed.errors
    assert following.status == "INFRASTRUCTURE_ERROR", following.errors
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("path", ["normal", "independent", "failed"])
@pytest.mark.parametrize(
    "progress", ["RUNNING", "PENDING", "operation-STARTED", "operation-PENDING", "operation-READY", "completion-only"]
)
def test_retirement_allows_actual_service_progress_and_additional_history_fields(path: str, progress: str) -> None:
    initial: dict[str, Any] = {"status": "RUNNING", "result": None, "operations": []}
    final_history: dict[str, Any] = {"status": "SUCCEEDED", "result": "completed", "operations": []}
    if progress in ("RUNNING", "PENDING"):
        initial["status"] = progress
    elif progress.startswith("operation-"):
        status = progress.removeprefix("operation-")
        operation = {"id": "step", "name": "stable-step", "type": "STEP", "parentId": None, "status": status}
        initial["operations"] = [{**operation, "error" if status == "PENDING" else "result": {"attempt": 1}}]
        final_history["operations"] = [{**operation, "status": "SUCCEEDED", "result": {"attempt": 3}}]
    else:
        first = {"id": "first", "name": "first", "type": "STEP", "parentId": None, "status": "SUCCEEDED"}
        second = {
            "id": "second",
            "name": "second",
            "type": "STEP",
            "parentId": None,
            "status": "FAILED",
            "error": {"message": "known"},
        }
        initial = {"status": "SUCCEEDED", "operations": [first, second]}
        final_history["operations"] = [
            deepcopy(second),
            {**first, "result": None},
            {**first, "id": "new", "name": "new", "result": "later-collected"},
        ]

    class ProgressingAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            if context.scenario.case_id == "failed":
                self.snapshots["failed"]["histories"][0].update(deepcopy(initial))

        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            if path != "failed":
                observed["behaviorErrors"] = []
            observed["expectedHistories"] = {
                history["executionArn"]: {"allowedStatuses": ["RUNNING", "PENDING", "SUCCEEDED"]}
                for history in observed["histories"]
            }
            return observed

        async def release(self, context: ScenarioContext) -> None:
            await super().release(context)
            if context.scenario.case_id == "failed":
                context.evidence["resourcesCreated"].append(
                    {"id": "control:failed", "kind": "control", "ownerCaseId": "failed"}
                )

        async def retire(self, context: ScenarioContext) -> bool:
            if context.scenario.case_id != "failed":
                return await super().retire(context)
            final = deepcopy(self.snapshots["failed"])
            final["histories"][0].update(deepcopy(final_history))
            _add_retained_resource(final, "control", "DELETED")
            if path == "normal":
                context.evidence = final
            else:
                context.retirement_evidence = final
                context.evidence["retirement"] = deepcopy(final["retirement"])
            return True

    adapter = ProgressingAdapter(phase="assertion")
    (observed, following), scheduler = _run(adapter, validator=_service_fact_validator)
    assert observed.status == ("FAILED" if path == "failed" else "PASSED"), observed.errors
    assert validate_retirement_evidence(observed.retirement_evidence) == []
    assert following.status == "PASSED", following.errors
    assert adapter.finished == [True]
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


@pytest.mark.parametrize("status", ["STARTED", "PENDING", "READY"])
@pytest.mark.parametrize("field", ["name", "type", "parentId"])
def test_active_operations_keep_their_observed_identity_during_retirement(status: str, field: str) -> None:
    class ChangedIdentityAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            self.snapshots[context.scenario.case_id]["histories"][0].update(
                status="RUNNING",
                operations=[{"id": "step", "name": None, "type": "STEP", "parentId": None, "status": status}],
            )

        async def retire(self, context: ScenarioContext) -> bool:
            await super().retire(context)
            history = context.retirement_evidence["histories"][0]
            history["status"] = "SUCCEEDED"
            history["operations"][0].update(status="SUCCEEDED", result="completed")
            if context.scenario.case_id == "failed":
                history["operations"][0][field] = "different-identity"
            return True

    adapter = ChangedIdentityAdapter(phase="assertion")
    (failed, following), scheduler = _run(adapter)
    assert any("Retirement changed operation" in error and field in error for error in failed.errors), failed.errors
    assert validate_retirement_evidence(failed.retirement_evidence) == []
    assert following.status == "INFRASTRUCTURE_ERROR"
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


def test_retirement_can_fill_previously_unobserved_operation_identity_fields() -> None:
    class CompletedIdentityAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            history = self.snapshots[context.scenario.case_id]["histories"][0]
            history["result"] = "completion-result"
            if context.scenario.case_id == "failed":
                history["operations"] = [
                    {
                        "id": "step",
                        "name": None,
                        "type": "STEP",
                        "parentId": None,
                        "status": "SUCCEEDED",
                        "result": None,
                    }
                ]
                context.evidence = {
                    "histories": [
                        {
                            "executionArn": history["executionArn"],
                            "ownerCaseId": "failed",
                            "status": "RUNNING",
                            "operations": [{"id": "step", "status": "STARTED"}],
                        }
                    ]
                }

        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            observed["behaviorErrors"] = []
            observed["expectedHistories"] = {
                history["executionArn"]: {"status": "SUCCEEDED", "result": "completion-result"}
                for history in observed["histories"]
            }
            return observed

    adapter = CompletedIdentityAdapter(phase="assertion")
    (completed, following), scheduler = _run(adapter, validator=_service_fact_validator)
    assert completed.status == following.status == "PASSED", completed.errors
    assert adapter.finished == [True]
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


@pytest.mark.parametrize("phase", ["normal", "partial"])
@pytest.mark.parametrize("initial", ["sdk-only", "empty", "extra"])
def test_retirement_can_complete_provisional_artifact_metadata(phase: str, initial: str) -> None:
    class ArtifactAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            if phase == "normal":
                observed["behaviorErrors"] = []
            return observed

        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            if context.scenario.case_id == "failed":
                self.snapshots["failed"]["artifact"].update(extra={"source": "fixture"}, compiler="python")
                context.evidence = self.observations(context)
                context.evidence["artifact"] = {} if initial == "empty" else {"sdk": context.profile.artifact_id}
                if initial == "extra":
                    context.evidence["artifact"]["extra"] = {"source": "fixture"}

    adapter = ArtifactAdapter(phase="assertion" if phase == "normal" else "placement")
    (completed, following), scheduler = _run(adapter)
    if phase == "normal":
        assert completed.status == "PASSED", completed.errors
    else:
        assert ORIGINAL_FAILURE in completed.errors
    assert completed.evidence["artifact"]["conformance"] == "tested-validator"
    assert completed.retirement_evidence["artifact"]["extra"] == {"source": "fixture"}
    assert validate_retirement_evidence(completed.retirement_evidence) == []
    assert following.status == "PASSED", following.errors
    assert adapter.finished == [True]
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


@pytest.mark.parametrize("field", ["sdk", "conformance", "extra"])
@pytest.mark.parametrize("change", ["replace", "drop"])
def test_completed_artifacts_preserve_every_previously_declared_field(field: str, change: str) -> None:
    class ChangedArtifactAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            self.snapshots[context.scenario.case_id]["artifact"]["extra"] = True

        def damage_retirement(self, final: dict[str, Any]) -> None:
            if change == "drop":
                final["artifact"].pop(field)
            else:
                # Python equality treats True and 1 as equal; persisted JSON does not.
                final["artifact"][field] = 1 if field == "extra" else "different-revision"

    adapter = ChangedArtifactAdapter(phase="assertion")
    (failed, following), scheduler = _run(adapter)
    assert any(f"artifact.{field}" in error for error in failed.errors), failed.errors
    assert following.status == "INFRASTRUCTURE_ERROR", following.errors
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


def test_retirement_cannot_drop_a_declared_null_artifact_field() -> None:
    class NullArtifactAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            self.snapshots[context.scenario.case_id]["artifact"]["extra"] = None

        def damage_retirement(self, final: dict[str, Any]) -> None:
            final["artifact"].pop("extra")

    (failed, following), scheduler = _run(NullArtifactAdapter(phase="assertion"))
    assert any("artifact.extra" in error for error in failed.errors), failed.errors
    assert following.status == "INFRASTRUCTURE_ERROR"
    assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("source", ["original", "final"])
@pytest.mark.parametrize("malformed", [None, [], 42])
def test_retirement_artifact_completion_rejects_malformed_objects(source: str, malformed: Any) -> None:
    class MalformedArtifactAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            if context.scenario.case_id == "failed" and source == "original":
                context.evidence = self.observations(context)
                context.evidence["artifact"] = malformed

        def damage_retirement(self, final: dict[str, Any]) -> None:
            if source == "final":
                final["artifact"] = malformed

    adapter = MalformedArtifactAdapter(phase="assertion")
    (failed, following), scheduler = _run(adapter)
    assert any("artifact" in error.lower() for error in failed.errors), failed.errors
    assert following.status == "INFRASTRUCTURE_ERROR"
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


def test_artifact_completion_does_not_allow_changing_the_observed_runtime() -> None:
    class ChangedRuntimeAdapter(FailureAdapter):
        def damage_retirement(self, final: dict[str, Any]) -> None:
            final["runtime"] = "python"

    (failed, following), scheduler = _run(ChangedRuntimeAdapter(phase="assertion"))
    assert any("Retirement runtime differs" in error for error in failed.errors), failed.errors
    assert following.status == "INFRASTRUCTURE_ERROR"
    assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize(
    ("native_start", "native_end", "contains_original"),
    [(90, 1000, True), (50, 1000, True), (90, 1200, True), (50, 1200, True), (95, 1200, False), (50, 999, False)],
)
def test_retirement_can_complete_but_not_shrink_provisional_native_intervals(
    native_start: int, native_end: int, contains_original: bool
) -> None:
    class NativeIntervalAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            self.snapshots[context.scenario.case_id]["attempts"][0]["startedAtMs"] = 90

        def damage_retirement(self, final: dict[str, Any]) -> None:
            final["attempts"][0].update(startedAtMs=native_start, endedAtMs=native_end)

    adapter = NativeIntervalAdapter()
    (failed, following), scheduler = _run(adapter)
    assert failed.status != "PASSED"
    assert ORIGINAL_FAILURE in failed.errors
    assert failed.evidence["attempts"][0]["startedAtMs"] == 90
    assert failed.evidence["attempts"][0]["endedAtMs"] == 1000
    assert failed.retirement_evidence["attempts"][0]["startedAtMs"] == native_start
    assert failed.retirement_evidence["attempts"][0]["endedAtMs"] == native_end
    assert validate_retirement_evidence(failed.retirement_evidence) == []
    if contains_original:
        assert following.status == "PASSED", following.errors
        assert adapter.started == ["failed", "next"]
        assert adapter.finished == [True]
        assert scheduler.quarantine_reason(PROFILE.resource_id) is None
    else:
        assert following.status == "INFRASTRUCTURE_ERROR", following.errors
        assert adapter.started == ["failed"]
        assert adapter.finished == [False]
        assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("identity", ["executionArn", "environmentId", "workerId", "processId", "role", "attempt"])
def test_completing_native_intervals_cannot_change_invocation_identity(identity: str) -> None:
    class ChangedIdentityAdapter(FailureAdapter):
        def damage_retirement(self, final: dict[str, Any]) -> None:
            final["attempts"][0].update(startedAtMs=50, endedAtMs=1200)
            final["attempts"][0][identity] = 2 if identity == "attempt" else "different-identity"

    adapter = ChangedIdentityAdapter()
    (failed, following), scheduler = _run(adapter)
    assert any(f"original invocation's {identity}" in error for error in failed.errors), failed.errors
    assert following.status == "INFRASTRUCTURE_ERROR", following.errors
    assert adapter.started == ["failed"]
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("source", ["original", "final"])
@pytest.mark.parametrize("endpoint", ["startedAtMs", "endedAtMs"])
@pytest.mark.parametrize("timestamp", [False, -1, float("inf"), float("nan"), "100"])
def test_native_interval_completion_rejects_malformed_timestamps(source: str, endpoint: str, timestamp: Any) -> None:
    class MalformedIntervalAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            if context.scenario.case_id == "failed" and source == "original":
                observed["attempts"][0][endpoint] = timestamp
            return observed

        def damage_retirement(self, final: dict[str, Any]) -> None:
            final["attempts"][0].update(startedAtMs=50, endedAtMs=1200)
            if source == "final":
                final["attempts"][0][endpoint] = timestamp

    adapter = MalformedIntervalAdapter()
    (failed, following), scheduler = _run(adapter)
    assert failed.status != "PASSED"
    assert following.status == "INFRASTRUCTURE_ERROR", following.errors
    assert adapter.started == ["failed"]
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("source", ["native-end", "event", "sdk-request"])
@pytest.mark.parametrize("settled_at", [18999, 19000])
def test_completed_native_intervals_still_settle_after_original_activity(source: str, settled_at: int) -> None:
    class OriginalActivityAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            if context.scenario.case_id != "failed":
                return
            snapshot = self.snapshots[context.scenario.case_id]
            attempt = snapshot["attempts"][0]
            if source == "native-end":
                attempt["endedAtMs"] = 19000
            else:
                record = {
                    "requestId": attempt["requestId"],
                    "ownerRequestId": attempt["requestId"],
                    "executionArn": attempt["executionArn"],
                    "atMs": 19000,
                }
                if source == "event":
                    snapshot["events"].append({**record, "kind": "late_activity"})
                    _resequence(snapshot)
                else:
                    snapshot["sdkRequests"].append(
                        {**record, "method": "CheckpointDurableExecution", "outcome": "REJECTED"}
                    )

        def damage_retirement(self, final: dict[str, Any]) -> None:
            final["attempts"][0]["startedAtMs"] = 50
            final["retirement"]["resources"][0]["settledAtMs"] = settled_at

    adapter = OriginalActivityAdapter()
    (failed, following), scheduler = _run(adapter)
    assert failed.status != "PASSED"
    assert ORIGINAL_FAILURE in failed.errors
    if settled_at < 19000:
        assert any("settled before its originally observed activity" in error for error in failed.errors), failed.errors
        assert following.status == "INFRASTRUCTURE_ERROR", following.errors
        assert adapter.finished == [False]
        assert scheduler.quarantine_reason(PROFILE.resource_id)
    else:
        assert following.status == "PASSED", following.errors
        assert adapter.started == ["failed", "next"]
        assert adapter.finished == [True]
        assert scheduler.quarantine_reason(PROFILE.resource_id) is None


def _same_time_pair(value: dict[str, Any], start_kind: str, end_kind: str) -> tuple[dict[str, Any], dict[str, Any]]:
    if start_kind in {"checkpoint_call", "poll_call"}:
        for event in value["events"]:
            if event["kind"] in {"io_start", "io_exit"}:
                event["kind"] = start_kind if event["kind"] == "io_start" else end_kind
                event["callId"] = "observed-call"
    start = next(event for event in value["events"] if event["kind"] == start_kind)
    end = next(event for event in value["events"] if event["kind"] == end_kind)
    end["atMs"] = start["atMs"]
    _resequence(value)
    return start, end


@pytest.mark.parametrize(
    ("start_kind", "end_kind"),
    [
        ("task_start", "task_exit"),
        ("io_start", "io_exit"),
        ("checkpoint_call", "checkpoint_exit"),
        ("poll_call", "poll_exit"),
        ("wrapper_enter", "wrapper_return"),
        ("wrapper_enter", "wrapper_raise"),
    ],
)
def test_retirement_cannot_reorder_observed_equal_time_boundaries(start_kind: str, end_kind: str) -> None:
    class ReorderedAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            if context.scenario.case_id == "failed":
                _same_time_pair(self.snapshots[context.scenario.case_id], start_kind, end_kind)

        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            if context.scenario.case_id == "failed":
                start = next(event for event in observed["events"] if event["kind"] == start_kind)
                end = next(event for event in observed["events"] if event["kind"] == end_kind)
                start["sequence"], end["sequence"] = end["sequence"], start["sequence"]
            return observed

    adapter = ReorderedAdapter()
    (failed, following), scheduler = _run(adapter, retry=end_kind == "wrapper_raise")
    assert ORIGINAL_FAILURE in failed.errors
    assert validate_retirement_evidence(failed.retirement_evidence) == []
    assert any("semantic order" in error for error in failed.errors), failed.errors
    assert following.status == "INFRASTRUCTURE_ERROR", following.errors
    assert adapter.started == ["failed"]
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("completion", ["renumbered", "added-event", "missing-sequences", "partial-sequences"])
def test_retirement_preserves_relative_order_without_freezing_sequence_numbers(completion: str) -> None:
    class CompletedOrderAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            if context.scenario.case_id == "failed":
                _same_time_pair(self.snapshots[context.scenario.case_id], "io_start", "io_exit")

        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            if context.scenario.case_id == "failed":
                for event in observed["events"]:
                    if completion == "missing-sequences" or (
                        completion == "partial-sequences" and event["kind"] == "io_exit"
                    ):
                        event.pop("sequence")
            return observed

        def damage_retirement(self, final: dict[str, Any]) -> None:
            if completion == "added-event":
                end = next(event for event in final["events"] if event["kind"] == "io_exit")
                final["events"].insert(final["events"].index(end), {**end, "kind": "late_activity"})
                _resequence(final)
            for event in final["events"]:
                event["sequence"] = 10 + event["sequence"] * 3

    adapter = CompletedOrderAdapter()
    (failed, following), scheduler = _run(adapter)
    assert ORIGINAL_FAILURE in failed.errors
    assert validate_retirement_evidence(failed.retirement_evidence) == []
    assert following.status == "PASSED", following.errors
    assert adapter.started == ["failed", "next"]
    assert adapter.finished == [True]
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


class StaleSnapshotAdapter(FailureAdapter):
    """Observes late activity that its retirement snapshot does not cover."""

    def __init__(self, *, late_at: int) -> None:
        super().__init__(phase="execute")
        self.late_at = late_at

    def observations(self, context: ScenarioContext) -> dict[str, Any]:
        observed = super().observations(context)
        if context.scenario.case_id == "failed":
            victim = observed["attempts"][0]
            owner = {key: victim[key] for key in ("requestId", "executionArn")}
            observed["events"].append(
                {**owner, "ownerRequestId": victim["requestId"], "kind": "late_activity", "atMs": self.late_at}
            )
        return observed


@pytest.mark.parametrize(("late_at", "reason"), [(25000, "predates"), (20500, "omitted or changed")])
def test_retirement_snapshot_must_cover_previously_observed_activity(late_at: int, reason: str) -> None:
    # The snapshot retires at 21000. Activity observed at 25000 makes it stale,
    # and earlier activity it omits contradicts the frozen observations.
    results, scheduler = _run(StaleSnapshotAdapter(late_at=late_at))
    failed = results[0]
    assert any(reason in error for error in failed.errors), failed.errors
    assert scheduler.quarantine_reason(PROFILE.resource_id) is not None


@pytest.mark.parametrize(
    "damage", ["dropped-dispatch", "changed-dispatch", "early-settlement", "changed-completion", "dropped-late-event"]
)
def test_retirement_preserves_sdk_requests_and_invocation_completion(damage: str) -> None:
    class LostActivityAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            if context.scenario.case_id == "failed":
                attempt = observed["attempts"][0]
                if damage == "changed-completion":
                    attempt["endedAtMs"] = 19000
                elif damage == "dropped-late-event":
                    observed["events"].append(
                        {
                            "requestId": attempt["requestId"],
                            "ownerRequestId": attempt["requestId"],
                            "executionArn": attempt["executionArn"],
                            "kind": "effect",
                            "atMs": 19000,
                        }
                    )
                else:
                    observed["sdkRequests"] = [
                        {
                            "requestId": attempt["requestId"],
                            "ownerRequestId": attempt["requestId"],
                            "executionArn": attempt["executionArn"],
                            "atMs": 19000,
                            "method": "CheckpointDurableExecution",
                            "outcome": "DISPATCHED",
                        }
                    ]
            return observed

        async def retire(self, context: ScenarioContext) -> bool:
            await super().retire(context)
            if context.scenario.case_id == "failed":
                final = context.retirement_evidence
                if damage == "early-settlement":
                    final["sdkRequests"] = deepcopy(self.observations(context)["sdkRequests"])
                elif damage == "changed-dispatch":
                    final["sdkRequests"] = deepcopy(self.observations(context)["sdkRequests"])
                    final["sdkRequests"][0]["outcome"] = "REJECTED"
                final["retirement"]["resources"][0]["settledAtMs"] = 19500 if damage == "changed-dispatch" else 15000
                # All except the unchanged SDK dispatch form internally valid
                # snapshots; only the original observations expose their lies.
                if damage != "early-settlement":
                    assert validate_retirement_evidence(final) == []
            return True

    adapter = LostActivityAdapter(phase="assertion")
    (failed, following), scheduler = _run(adapter)
    assert failed.status == "FAILED"
    assert any("retirement" in error.lower() for error in failed.errors), failed.errors
    assert following.status == "INFRASTRUCTURE_ERROR", following.errors
    assert adapter.started == ["failed"]
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("provide_audit", [False, True])
def test_unjournaled_collected_attempts_cannot_release_the_target(provide_audit: bool) -> None:
    class UnjournaledAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            self.started.append(context.scenario.case_id)
            self.snapshots[context.scenario.case_id] = _snapshot(context)

        async def retire(self, context: ScenarioContext) -> bool:
            await super().retire(context)
            if not provide_audit:
                context.retirement_evidence = {}
            return True

    adapter = UnjournaledAdapter(phase="assertion")
    (failed, following), scheduler = _run(adapter)
    assert failed.status == "INCOMPLETE_EVIDENCE"
    assert failed.attempts == []
    assert failed.evidence["attempts"]
    assert any("identified invocation request" in error for error in failed.errors)
    assert any("retirement" in error.lower() for error in failed.errors), failed.errors
    assert following.status == "INFRASTRUCTURE_ERROR", following.errors
    assert adapter.started == ["failed"]
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("damage", [None, "dropped-known-start", "dropped-known-fault"])
def test_early_failure_freezes_context_observations_without_partial_collection(damage: str | None) -> None:
    class ContextEvidenceAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            context.evidence = self.observations(context)

        async def release(self, context: ScenarioContext) -> None:
            await super().release(context)
            context.evidence["events"].clear()
            context.evidence["faultsCreated"].clear()

    adapter = ContextEvidenceAdapter(phase="placement", damage=damage)
    adapter.collect_partial = None  # type: ignore[assignment, method-assign]
    (failed, following), scheduler = _run(adapter)
    assert PHASE_FAILURE in failed.errors[0]
    assert failed.evidence["capturedBeforeCleanup"] is True
    assert failed.evidence["behaviorErrors"] == [ORIGINAL_FAILURE]
    assert any(event["kind"] == "io_start" for event in failed.evidence["events"])
    assert failed.evidence["faultsCreated"]
    assert ("failed", "partial") not in adapter.steps
    if damage is None:
        assert following.status == "PASSED", following.errors
        assert adapter.finished == [True]
        assert scheduler.quarantine_reason(PROFILE.resource_id) is None
    else:
        assert following.status == "INFRASTRUCTURE_ERROR", following.errors
        assert adapter.started == ["failed"]
        assert adapter.finished == [False]
        assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("inventory", ["faultsCreated", "histories", "resourcesCreated", "events", "sdkRequests"])
def test_prelaunch_owned_work_requires_an_independent_retirement_audit(inventory: str) -> None:
    class PrelaunchFailureAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            if context.scenario.case_id != "failed":
                await super().start(context)
                return
            self.started.append(context.scenario.case_id)
            snapshot = _snapshot(context)
            if inventory == "sdkRequests":
                snapshot[inventory] = [{"requestId": "unidentified-dispatch", "atMs": 200}]
            context.evidence = {inventory: deepcopy(snapshot[inventory])}
            raise RuntimeError(PHASE_FAILURE)

        async def release(self, context: ScenarioContext) -> None:
            await super().release(context)
            if context.scenario.case_id == "failed":
                context.evidence.clear()

        async def retire(self, context: ScenarioContext) -> bool:
            if context.scenario.case_id == "failed":
                self.steps.append(("failed", "retire"))
                return True
            return await super().retire(context)

    adapter = PrelaunchFailureAdapter()
    (failed, following), scheduler = _run(adapter)
    assert failed.attempts == []
    assert failed.evidence[inventory]
    assert PHASE_FAILURE in failed.errors[0]
    assert any("independent retirement evidence" in error for error in failed.errors), failed.errors
    assert following.status == "INFRASTRUCTURE_ERROR", following.errors
    assert adapter.started == ["failed"]
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("collection", ["reduced-response", "in-place", "exception", "cancelled"])
@pytest.mark.parametrize("complete_retirement", [False, True])
def test_partial_collection_cannot_erase_previously_retained_observations(
    collection: str, complete_retirement: bool
) -> None:
    class PartialLossAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            context.evidence = self.observations(context)

        async def collect_partial(self, context: ScenarioContext) -> Any:
            self.steps.append((context.scenario.case_id, "partial"))
            captured = self.observations(context) if collection == "reduced-response" else context.evidence
            captured["events"] = [item for item in captured["events"] if item["kind"] not in {"io_start", "io_exit"}]
            captured["faultsCreated"] = []
            captured["behaviorErrors"] = []
            if collection == "exception":
                raise RuntimeError("partial collection failed after mutation")
            if collection == "cancelled":
                raise asyncio.CancelledError
            return captured

        async def retire(self, context: ScenarioContext) -> bool:
            await super().retire(context)
            if context.scenario.case_id == "failed" and not complete_retirement:
                final = context.retirement_evidence
                final["events"] = [item for item in final["events"] if item["kind"] not in {"io_start", "io_exit"}]
                final["faultsCreated"] = []
                final["retirement"]["faults"] = []
                assert validate_retirement_evidence(final) == []
            return True

    adapter = PartialLossAdapter(phase="placement")
    (failed, following), scheduler = _run(adapter)
    assert failed.status != "PASSED"
    assert PHASE_FAILURE in failed.errors[0]
    assert ORIGINAL_FAILURE in failed.errors
    assert failed.evidence["behaviorErrors"] == [ORIGINAL_FAILURE]
    assert any(item["kind"] == "io_start" for item in failed.evidence["events"])
    assert failed.evidence["faultsCreated"]
    if complete_retirement and collection in {"reduced-response", "in-place"}:
        assert following.status == "PASSED", following.errors
        assert adapter.finished == [True]
        assert scheduler.quarantine_reason(PROFILE.resource_id) is None
    else:
        assert following.status == "INFRASTRUCTURE_ERROR", following.errors
        assert adapter.started == ["failed"]
        assert adapter.finished == [False]
        assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("collection", ["response", "exception", "cancelled"])
@pytest.mark.parametrize("complete_retirement", [False, True])
def test_partial_collection_retains_new_context_work_even_when_it_is_absent_from_the_response(
    collection: str, complete_retirement: bool
) -> None:
    class AdditionalContextAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            context.evidence = self.observations(context)

        async def collect_partial(self, context: ScenarioContext) -> Any:
            context.evidence["resourcesCreated"].append(
                {"id": "control:failed", "kind": "control", "ownerCaseId": "failed"}
            )
            if collection == "exception":
                raise RuntimeError("partial collection failed after retaining a control")
            if collection == "cancelled":
                raise asyncio.CancelledError
            return self.observations(context)

        async def retire(self, context: ScenarioContext) -> bool:
            await super().retire(context)
            if context.scenario.case_id == "failed" and complete_retirement:
                _add_retained_resource(context.retirement_evidence, "control", "DELETED")
            return True

    adapter = AdditionalContextAdapter(phase="placement")
    (failed, following), scheduler = _run(adapter)
    assert ORIGINAL_FAILURE in failed.errors
    assert any(item["id"] == "control:failed" for item in failed.evidence["resourcesCreated"])
    if complete_retirement and collection == "response":
        assert following.status == "PASSED", following.errors
        assert scheduler.quarantine_reason(PROFILE.resource_id) is None
    else:
        assert following.status == "INFRASTRUCTURE_ERROR", following.errors
        assert adapter.finished == [False]
        assert scheduler.quarantine_reason(PROFILE.resource_id)


def test_partial_collection_cannot_hide_an_execution_known_only_from_its_history() -> None:
    class HistoryInventoryAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            context.evidence = self.observations(context)
            if context.scenario.case_id == "failed":
                context.evidence["histories"].append(
                    {"executionArn": "arn:execution:unresolved", "ownerCaseId": "failed", "status": "RUNNING"}
                )

    (failed, following), scheduler = _run(HistoryInventoryAdapter(phase="placement"))
    assert any(item["executionArn"] == "arn:execution:unresolved" for item in failed.evidence["histories"])
    assert any("original histories entry" in error for error in failed.errors), failed.errors
    assert following.status == "INFRASTRUCTURE_ERROR", following.errors
    assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("collection", ["reduced-response", "in-place", "exception"])
@pytest.mark.parametrize("complete_retirement", [False, True])
def test_normal_collection_cannot_erase_context_observations_before_a_failed_audit(
    collection: str, complete_retirement: bool
) -> None:
    class NormalLossAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            context.evidence = self.observations(context)

        async def collect(self, context: ScenarioContext) -> Mapping[str, Any]:
            if context.scenario.case_id != "failed":
                return await super().collect(context)
            captured = self.observations(context) if collection == "reduced-response" else context.evidence
            captured["events"] = [item for item in captured["events"] if item["kind"] not in {"io_start", "io_exit"}]
            captured["faultsCreated"] = []
            captured["behaviorErrors"] = []
            if collection == "exception":
                raise RuntimeError("normal collection failed after mutation")
            return captured

        async def collect_partial(self, context: ScenarioContext) -> Any:
            return context.evidence

        async def retire(self, context: ScenarioContext) -> bool:
            await super().retire(context)
            if context.scenario.case_id == "failed" and not complete_retirement:
                final = context.retirement_evidence
                final["events"] = [item for item in final["events"] if item["kind"] not in {"io_start", "io_exit"}]
                final["faultsCreated"] = []
                final["retirement"]["faults"] = []
            return True

    adapter = NormalLossAdapter(phase="none")
    (failed, following), scheduler = _run(adapter)
    assert failed.status != "PASSED"
    assert ORIGINAL_FAILURE in failed.errors
    assert failed.evidence["behaviorErrors"] == [ORIGINAL_FAILURE]
    assert any(item["kind"] == "io_start" for item in failed.evidence["events"])
    assert failed.evidence["faultsCreated"]
    if collection != "exception":
        assert failed.status == "INCOMPLETE_EVIDENCE"
        assert any("Collection omitted or changed" in error for error in failed.errors)
    if complete_retirement:
        assert following.status == "PASSED", following.errors
        assert adapter.finished == [True]
        assert scheduler.quarantine_reason(PROFILE.resource_id) is None
    else:
        assert following.status == "INFRASTRUCTURE_ERROR", following.errors
        assert adapter.started == ["failed"]
        assert adapter.finished == [False]
        assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("history_status", ["RUNNING", "PENDING"])
def test_normal_collection_can_enrich_provisional_context_observations(history_status: str) -> None:
    class EnrichingAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            context.evidence = self.observations(context)
            if context.scenario.case_id == "failed":
                context.evidence["observationEndedAtMs"] = 1500
                context.evidence["collection"]["watermarks"] = dict.fromkeys(
                    context.evidence["collection"]["watermarks"], 1500
                )
                context.evidence["attempts"][0].update(startedAtMs=150, endedAtMs=800)
                context.evidence["histories"][0]["status"] = history_status

        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            value = super().observations(context)
            value["behaviorErrors"] = []
            return value

    adapter = EnrichingAdapter(phase="none")
    (enriched, following), scheduler = _run(adapter)
    assert enriched.status == following.status == "PASSED", enriched.errors
    assert len(enriched.evidence["attempts"]) == len(enriched.evidence["histories"]) == 1
    assert enriched.evidence["attempts"][0]["startedAtMs"] == 100
    assert enriched.evidence["attempts"][0]["endedAtMs"] == 1000
    assert enriched.evidence["histories"][0]["status"] == "SUCCEEDED"
    assert enriched.evidence["observationEndedAtMs"] == 20000
    assert all(value == 20000 for value in enriched.evidence["collection"]["watermarks"].values())
    assert adapter.finished == [True]
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


@pytest.mark.parametrize("phase", ["normal", "partial"])
@pytest.mark.parametrize("enrichment", ["history-result", "operation-result", "event-sequence"])
def test_collection_accepts_completion_and_semantic_event_resequencing(phase: str, enrichment: str) -> None:
    class CompletingAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            value = super().observations(context)
            if phase == "normal":
                value["behaviorErrors"] = []
            return value

        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            context.evidence = self.observations(context)
            if context.scenario.case_id != "failed":
                return
            before = context.evidence
            final = self.snapshots[context.scenario.case_id]
            if enrichment == "history-result":
                before["histories"][0].update(status="RUNNING", result=None)
                final["histories"][0]["result"] = {"completed": True}
            elif enrichment == "operation-result":
                operation = {"id": "step", "name": "work", "type": "STEP", "parentId": None, "status": "STARTED"}
                before["histories"][0]["operations"] = [{**operation, "result": None}]
                final["histories"][0]["operations"] = [
                    {**operation, "status": "SUCCEEDED", "result": {"completed": True}}
                ]
            else:
                owner = {key: final["attempts"][0][key] for key in ("requestId", "executionArn")}
                final["events"].append({**owner, "ownerRequestId": owner["requestId"], "kind": "effect", "atMs": 100.5})
                _resequence(final)

    adapter = CompletingAdapter(phase="none" if phase == "normal" else "placement")
    (collected, following), scheduler = _run(adapter)
    if phase == "normal":
        assert collected.status == "PASSED", collected.errors
    else:
        assert collected.status != "PASSED"
        assert ORIGINAL_FAILURE in collected.errors
    assert not any("omitted or changed retained" in error for error in collected.errors), collected.errors
    if enrichment == "history-result":
        assert collected.evidence["histories"][0]["result"] == {"completed": True}
    elif enrichment == "operation-result":
        operations = collected.evidence["histories"][0]["operations"]
        assert len(operations) == 1
        assert operations[0]["status"] == "SUCCEEDED"
        assert operations[0]["result"] == {"completed": True}
    else:
        assert len(collected.evidence["events"]) == len(adapter.snapshots["failed"]["events"])
        assert len({event["sequence"] for event in collected.evidence["events"]}) == len(collected.evidence["events"])
    assert following.status == "PASSED", following.errors
    assert adapter.finished == [True]
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


@pytest.mark.parametrize("scope", ["history", "operation"])
@pytest.mark.parametrize("original_result", [None, {"completed": "original"}])
def test_collection_cannot_replace_a_completed_outcome(scope: str, original_result: Any) -> None:
    class CompletedOutcomeAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            value = super().observations(context)
            value["behaviorErrors"] = []
            return value

        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            context.evidence = self.observations(context)
            if context.scenario.case_id != "failed":
                return
            before, after = context.evidence["histories"][0], self.snapshots["failed"]["histories"][0]
            if scope == "operation":
                operation = {"id": "step", "name": "work", "type": "STEP", "parentId": None, "status": "SUCCEEDED"}
                before["operations"], after["operations"] = [deepcopy(operation)], [deepcopy(operation)]
                before, after = before["operations"][0], after["operations"][0]
            before["result"] = deepcopy(original_result)
            after["result"] = {"completed": "substituted"}

    (collected, _), _scheduler = _run(CompletedOutcomeAdapter(phase="none"))
    assert collected.status == "INCOMPLETE_EVIDENCE", collected.errors
    assert any("omitted or changed retained" in error for error in collected.errors)
    result = collected.evidence["histories"][0]
    if scope == "operation":
        assert len(result["operations"]) == 1
        result = result["operations"][0]
    assert result["result"] == original_result


@pytest.mark.parametrize("repair_original_order", [False, True])
def test_normal_collection_cannot_reverse_observed_order_without_an_independent_audit(
    repair_original_order: bool,
) -> None:
    class ReorderedAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            value = super().observations(context)
            value["behaviorErrors"] = []
            return value

        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            final = self.snapshots[context.scenario.case_id]
            start = next(event for event in final["events"] if event["kind"] == "io_start")
            end = next(event for event in final["events"] if event["kind"] == "io_exit")
            end["atMs"] = start["atMs"]
            _resequence(final)
            context.evidence = self.observations(context)
            changed = context.evidence if repair_original_order else final
            start = next(event for event in changed["events"] if event["kind"] == "io_start")
            end = next(event for event in changed["events"] if event["kind"] == "io_exit")
            start["sequence"], end["sequence"] = end["sequence"], start["sequence"]

        async def retire(self, context: ScenarioContext) -> bool:
            context.evidence["retirement"] = deepcopy(self.snapshots[context.scenario.case_id]["retirement"])
            return True

    adapter = ReorderedAdapter(phase="none")
    (collected, following), scheduler = _run(adapter)
    assert collected.status == "INCOMPLETE_EVIDENCE", collected.errors
    assert any("omitted or changed retained" in error for error in collected.errors)
    assert any("independent retirement evidence" in error for error in collected.errors)
    assert following.status == "INFRASTRUCTURE_ERROR", following.errors
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


def test_event_resequencing_cannot_collapse_duplicate_observations() -> None:
    class DuplicateEventAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            value = super().observations(context)
            value["behaviorErrors"] = []
            return value

        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            context.evidence = self.observations(context)
            if context.scenario.case_id == "failed":
                observed = next(event for event in context.evidence["events"] if event["kind"] == "effect")
                context.evidence["events"].append(deepcopy(observed))
                _resequence(context.evidence)

    (collected, _), _scheduler = _run(DuplicateEventAdapter(phase="none"))
    assert collected.status == "INCOMPLETE_EVIDENCE", collected.errors
    assert sum(event["kind"] == "effect" for event in collected.evidence["events"]) == 2
    assert any("omitted or changed retained" in error for error in collected.errors)


@pytest.mark.parametrize("phase", ["normal", "partial"])
@pytest.mark.parametrize("scope", ["history", "operation"])
@pytest.mark.parametrize("field", ["result", "error"])
@pytest.mark.parametrize(
    ("original", "substituted"),
    [
        ([1, 2], [2, 1]),
        ([1], [1, 2]),
        ({"a": 1}, {"a": 1, "b": 2}),
        ({"a": [1, 2]}, {"a": [2, 1]}),
        ({"a": [1]}, {"a": [True]}),
    ],
)
def test_checkpoint_outcome_json_is_retained_atomically(
    phase: str, scope: str, field: str, original: Any, substituted: Any
) -> None:
    class OutcomeMutationAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            value = super().observations(context)
            if phase == "normal":
                value["behaviorErrors"] = []
            return value

        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            context.evidence = self.observations(context)
            if context.scenario.case_id != "failed":
                return
            status = "SUCCEEDED" if field == "result" else "FAILED"
            before, after = context.evidence["histories"][0], self.snapshots["failed"]["histories"][0]
            if scope == "operation":
                operation = {"id": "step", "name": "work", "type": "STEP", "parentId": None, "status": status}
                before["operations"], after["operations"] = [deepcopy(operation)], [deepcopy(operation)]
                before, after = before["operations"][0], after["operations"][0]
            else:
                before["status"] = after["status"] = status
                for resource in self.snapshots["failed"]["retirement"]["resources"]:
                    if resource["kind"] == "execution":
                        resource["outcome"] = status
            before[field], after[field] = deepcopy(original), deepcopy(substituted)

    (collected, _), _scheduler = _run(OutcomeMutationAdapter(phase="none" if phase == "normal" else "placement"))
    assert collected.status != "PASSED", collected.errors
    assert any("omitted or changed retained" in error for error in collected.errors)
    outcome = collected.evidence["histories"][0]
    if scope == "operation":
        assert len(outcome["operations"]) == 1
        outcome = outcome["operations"][0]
    assert same_json(outcome[field], original), outcome[field]


@pytest.mark.parametrize("phase", ["normal", "partial"])
@pytest.mark.parametrize("inventory", ["faultsCreated", "resourcesCreated"])
def test_collection_enriches_owned_inventory_without_duplicating_identity(phase: str, inventory: str) -> None:
    class InventoryEnrichmentAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            value = super().observations(context)
            if phase == "normal":
                value["behaviorErrors"] = []
            return value

        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            context.evidence = self.observations(context)
            if context.scenario.case_id == "failed":
                record = self.snapshots["failed"][inventory][0]
                if inventory == "faultsCreated":
                    record["releasedAtMs"] = 790
                else:
                    record["evidenceUri"] = "memory://resource-observation"

    adapter = InventoryEnrichmentAdapter(phase="none" if phase == "normal" else "placement")
    (collected, following), scheduler = _run(adapter)
    assert not any("omitted or changed retained" in error for error in collected.errors), collected.errors
    if phase == "normal":
        assert collected.status == "PASSED", collected.errors
    else:
        assert collected.status != "PASSED"
        assert ORIGINAL_FAILURE in collected.errors
    assert collected.evidence[inventory] == adapter.snapshots["failed"][inventory]
    assert following.status == "PASSED", following.errors
    assert adapter.finished == [True]
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


@pytest.mark.parametrize("phase", ["normal", "partial"])
@pytest.mark.parametrize(
    ("inventory", "field", "substituted"),
    [
        ("faultsCreated", "ownerCaseId", "other-case"),
        ("faultsCreated", "id", "other-fault"),
        ("faultsCreated", "releasedAtMs", 900),
        ("resourcesCreated", "ownerCaseId", "other-case"),
        ("resourcesCreated", "id", "other-resource"),
        ("resourcesCreated", "kind", "control"),
        ("resourcesCreated", "evidenceUri", "memory://substituted-source"),
    ],
)
def test_inventory_enrichment_cannot_replace_known_identity_or_metadata(
    phase: str, inventory: str, field: str, substituted: Any
) -> None:
    class ConflictingInventoryAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            value = super().observations(context)
            if phase == "normal":
                value["behaviorErrors"] = []
            return value

        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            context.evidence = self.observations(context)
            if context.scenario.case_id != "failed":
                return
            if field == "releasedAtMs":
                context.evidence[inventory][0][field] = 790
            elif field == "evidenceUri":
                context.evidence[inventory][0][field] = "memory://original-source"
            self.original = deepcopy(context.evidence[inventory][0])
            self.snapshots["failed"][inventory][0][field] = substituted

    adapter = ConflictingInventoryAdapter(phase="none" if phase == "normal" else "placement")
    (collected, _), _scheduler = _run(adapter)
    assert collected.status != "PASSED", collected.errors
    assert any("omitted or changed retained" in error for error in collected.errors)
    original = next(record for record in collected.evidence[inventory] if record["id"] == adapter.original["id"])
    assert original[field] == adapter.original[field]


@pytest.mark.parametrize("inventory", ["faultsCreated", "resourcesCreated"])
def test_inventory_identity_matching_preserves_observed_multiplicity(inventory: str) -> None:
    class DuplicateInventoryAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            value = super().observations(context)
            value["behaviorErrors"] = []
            return value

        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            context.evidence = self.observations(context)
            if context.scenario.case_id == "failed":
                context.evidence[inventory].append(deepcopy(context.evidence[inventory][0]))

    adapter = DuplicateInventoryAdapter(phase="none")
    (collected, _), _scheduler = _run(adapter)
    assert collected.status == "INCOMPLETE_EVIDENCE", collected.errors
    assert len(collected.evidence[inventory]) == len(adapter.snapshots["failed"][inventory]) + 1
    assert any("omitted or changed retained" in error for error in collected.errors)


@pytest.mark.parametrize("phase", ["normal", "partial"])
@pytest.mark.parametrize(
    ("declared", "substituted", "actual"),
    [
        ({"status": "SUCCEEDED", "result": [1, 2]}, {"status": "SUCCEEDED", "result": [2, 1]}, {"result": [2, 1]}),
        ({"status": "SUCCEEDED", "result": [1]}, {"status": "SUCCEEDED", "result": [1, 2]}, {"result": [1, 2]}),
        (
            {"status": "SUCCEEDED", "result": {"a": 1}},
            {"status": "SUCCEEDED", "result": {"a": 1, "b": 2}},
            {"result": {"a": 1, "b": 2}},
        ),
        (
            {"status": "FAILED", "error": {"type": "Failure"}},
            {"status": "FAILED", "error": {"type": "Failure", "details": "changed"}},
            {"status": "FAILED", "error": {"type": "Failure", "details": "changed"}},
        ),
        (
            {"status": "SUCCEEDED", "result": None},
            {"status": "FAILED", "result": None},
            {"status": "FAILED", "result": None},
        ),
        (
            {"allowedStatuses": ["SUCCEEDED"], "result": None},
            {"allowedStatuses": ["SUCCEEDED", "FAILED"], "result": None},
            {"status": "FAILED", "result": None},
        ),
        (
            {"status": "SUCCEEDED", "result": {"value": 1}},
            {"status": "SUCCEEDED", "result": {"value": True}},
            {"result": {"value": True}},
        ),
    ],
)
def test_collection_cannot_rewrite_a_predeclared_history_expectation(
    phase: str, declared: dict[str, Any], substituted: dict[str, Any], actual: dict[str, Any]
) -> None:
    class RewrittenExpectationAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            history = self.snapshots[context.scenario.case_id]["histories"][0]
            history.update(actual if context.scenario.case_id == "failed" else {"result": None})
            if context.scenario.case_id == "failed":
                context.evidence = {"expectedHistories": {history["executionArn"]: deepcopy(declared)}}
            for resource in self.snapshots[context.scenario.case_id]["retirement"]["resources"]:
                if resource["kind"] == "execution":
                    resource["outcome"] = history["status"]

        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            value = super().observations(context)
            arn = value["histories"][0]["executionArn"]
            expected = substituted if context.scenario.case_id == "failed" else {"status": "SUCCEEDED", "result": None}
            value["expectedHistories"] = {arn: deepcopy(expected)}
            return value

    adapter = RewrittenExpectationAdapter(phase="none" if phase == "normal" else "placement")
    results = asyncio.run(
        run_scenarios(
            [_case("failed"), _case("next")],
            [PROFILE],
            adapter,
            run_id="run",
            validator=lambda evidence, *, phase="final": validate_common_evidence(
                evidence,
                phase="observations" if phase == "observations" else "final",
                requirement_ids=("fixture-check",),
            ),
        )
    )
    collected, following = results
    assert collected.status != "PASSED", collected.errors
    assert any("omitted or changed retained" in error for error in collected.errors)
    assert any(
        "fixture expectation" in error or "outside the declared allowedStatuses" in error for error in collected.errors
    )
    arn = collected.evidence["histories"][0]["executionArn"]
    assert same_json(collected.evidence["expectedHistories"][arn], declared)
    assert following.status == "PASSED", following.errors


@pytest.mark.parametrize("phase", ["normal", "partial"])
def test_collection_can_add_an_expectation_for_a_newly_observed_execution(phase: str) -> None:
    class AdditionalExecutionAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            final = self.snapshots[context.scenario.case_id]
            final["histories"][0]["result"] = None
            original_arn = final["histories"][0]["executionArn"]
            context.evidence = {"expectedHistories": {original_arn: {"status": "SUCCEEDED", "result": None}}}
            if context.scenario.case_id != "failed":
                return
            extra = _snapshot(ScenarioContext(context.run_id, _case("additional"), PROFILE))
            request = extra["attempts"][0]
            request["attempt"] = 2
            request["startedAtMs"] += 2000
            request["endedAtMs"] += 2000
            context.record_attempt(
                request["requestId"],
                role="victim",
                requestId=request["requestId"],
                executionArn=request["executionArn"],
            )
            for event in extra["events"]:
                event["atMs"] += 2000
            extra["histories"][0]["result"] = None
            for key in ("resourcesCreated", "faultsCreated", "histories"):
                for item in extra[key]:
                    item["ownerCaseId"] = context.scenario.case_id
            for key in ("resources", "faults"):
                for item in extra["retirement"][key]:
                    item["ownerCaseId"] = context.scenario.case_id
                    if key == "faults":
                        item["releasedAtMs"] += 2000
                final["retirement"][key].extend(extra["retirement"][key])
            for key in ("attempts", "events", "histories", "resourcesCreated", "faultsCreated"):
                final[key].extend(extra[key])

        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            value = super().observations(context)
            value["expectedHistories"] = {
                history["executionArn"]: {"status": "SUCCEEDED", "result": None} for history in value["histories"]
            }
            return value

    adapter = AdditionalExecutionAdapter(phase="none" if phase == "normal" else "placement")
    collected, following = asyncio.run(
        run_scenarios(
            [_case("failed"), _case("next")],
            [PROFILE],
            adapter,
            run_id="run",
            validator=lambda evidence, *, phase="final": validate_common_evidence(
                evidence,
                phase="observations" if phase == "observations" else "final",
                requirement_ids=("fixture-check",),
            ),
        )
    )
    if phase == "normal":
        assert collected.status == "PASSED", collected.errors
    else:
        assert collected.status != "PASSED"
    assert not any("omitted or changed retained" in error for error in collected.errors)
    assert len(collected.evidence["expectedHistories"]) == len(collected.evidence["histories"]) == 2
    assert following.status == "PASSED", following.errors
    assert adapter.finished == [True]


@pytest.mark.parametrize("phase", ["normal", "partial"])
@pytest.mark.parametrize("initial", ["STARTED", "PENDING", "READY"])
@pytest.mark.parametrize("terminal", ["SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT", "STOPPED"])
def test_collection_accepts_real_operation_state_completion(phase: str, initial: str, terminal: str) -> None:
    outcome_field = "result" if terminal == "SUCCEEDED" else "error"
    outcome = {"terminal": terminal}

    class OperationCompletionAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            value = super().observations(context)
            if phase == "normal":
                value["behaviorErrors"] = []
            return value

        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            context.evidence = self.observations(context)
            if context.scenario.case_id == "failed":
                operation = {"id": "step", "name": "work", "type": "STEP", "parentId": None}
                context.evidence["histories"][0]["operations"] = [{**operation, "status": initial, outcome_field: None}]
                self.snapshots["failed"]["histories"][0]["operations"] = [
                    {**operation, "status": terminal, outcome_field: outcome}
                ]

    adapter = OperationCompletionAdapter(phase="none" if phase == "normal" else "placement")
    (collected, following), scheduler = _run(adapter)
    if phase == "normal":
        assert collected.status == "PASSED", collected.errors
    else:
        assert collected.status != "PASSED"
        assert ORIGINAL_FAILURE in collected.errors
    assert not any("omitted or changed retained" in error for error in collected.errors), collected.errors
    operations = collected.evidence["histories"][0]["operations"]
    assert len(operations) == 1
    assert operations[0]["status"] == terminal
    assert operations[0][outcome_field] == outcome
    assert following.status == "PASSED", following.errors
    assert adapter.finished == [True]
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


@pytest.mark.parametrize("original", ["SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT", "STOPPED", "RUNNING", "UNKNOWN"])
@pytest.mark.parametrize("keep_status", [False, True])
def test_collection_cannot_advance_a_terminal_or_unknown_operation_state(original: str, keep_status: bool) -> None:
    unknown = original in {"RUNNING", "UNKNOWN"}
    updated = original if keep_status else "SUCCEEDED" if unknown else "STARTED"
    outcome_field = "result" if original == "SUCCEEDED" or unknown else "error"
    original_outcome = None if unknown else {"value": "original"}

    class InvalidProgressAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            value = super().observations(context)
            value["behaviorErrors"] = []
            return value

        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            context.evidence = self.observations(context)
            if context.scenario.case_id == "failed":
                operation = {"id": "step", "name": "work", "type": "STEP", "parentId": None}
                context.evidence["histories"][0]["operations"] = [
                    {**operation, "status": original, outcome_field: deepcopy(original_outcome)}
                ]
                self.snapshots["failed"]["histories"][0]["operations"] = [
                    {**operation, "status": updated, outcome_field: {"value": "changed"}}
                ]

    (collected, _), _scheduler = _run(InvalidProgressAdapter(phase="none"))
    assert collected.status == "INCOMPLETE_EVIDENCE", collected.errors
    operation = collected.evidence["histories"][0]["operations"][0]
    assert operation["status"] == original
    assert same_json(operation[outcome_field], original_outcome)


@pytest.mark.parametrize("phase", ["normal", "partial"])
@pytest.mark.parametrize("kind", ["poll-result", "retry-error"])
def test_collection_retains_evolving_nonterminal_operation_outcomes(phase: str, kind: str) -> None:
    class IntermediateOutcomeAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            value = super().observations(context)
            if phase == "normal":
                value["behaviorErrors"] = []
            return value

        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            context.evidence = self.observations(context)
            if context.scenario.case_id == "failed":
                operation = {"id": "step", "name": "work", "type": "STEP", "parentId": None}
                status, field = ("STARTED", "result") if kind == "poll-result" else ("PENDING", "error")
                context.evidence["histories"][0]["operations"] = [
                    {**operation, "status": status, field: {"attempt": 1, "values": [1]}}
                ]
                self.snapshots["failed"]["histories"][0]["operations"] = [
                    {**operation, "status": "SUCCEEDED", "result": {"attempt": 3, "values": [3]}}
                ]

        async def collect(self, context: ScenarioContext) -> Mapping[str, Any]:
            if context.scenario.case_id == "failed":
                operation = context.evidence["histories"][0]["operations"][0]
                operation["status"] = "READY"
                field = "result" if kind == "poll-result" else "error"
                operation[field] = {"attempt": 2, "values": [2]}
            return self.observations(context)

        async def collect_partial(self, context: ScenarioContext) -> Mapping[str, Any]:
            return await self.collect(context)

    adapter = IntermediateOutcomeAdapter(phase="none" if phase == "normal" else "placement")
    (collected, following), scheduler = _run(adapter)
    if phase == "normal":
        assert collected.status == "PASSED", collected.errors
    else:
        assert collected.status != "PASSED"
        assert ORIGINAL_FAILURE in collected.errors
    assert not any("omitted or changed retained" in error for error in collected.errors), collected.errors
    operations = collected.evidence["histories"][0]["operations"]
    assert len(operations) == 1
    assert operations[0]["status"] == "SUCCEEDED"
    assert operations[0]["result"] == {"attempt": 3, "values": [3]}
    assert "error" not in operations[0]
    assert following.status == "PASSED", following.errors
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


@pytest.mark.parametrize("phase", ["place", "execute", "collect", "partial"])
@pytest.mark.parametrize("scope", ["execution", "operation", "expectation"])
@pytest.mark.parametrize("field", ["result", "error"])
def test_retained_immutable_conflicts_survive_a_later_matching_collection(phase: str, scope: str, field: str) -> None:
    original, conflicting = {"value": [1, 2]}, {"value": [2, 1]}

    class ConflictingContextAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            final = self.snapshots[context.scenario.case_id]
            history = final["histories"][0]
            history["result"] = None
            if context.scenario.case_id == "failed":
                target = history
                status = "SUCCEEDED" if field == "result" else "FAILED"
                if scope == "operation":
                    target = {"id": "step", "name": "work", "type": "STEP", "parentId": None}
                    history["operations"] = [target]
                target.update(status=status, **{field: deepcopy(original)})
                for resource in final["retirement"]["resources"]:
                    if resource["kind"] == "execution":
                        resource["outcome"] = history["status"]
            context.evidence = self.observations(context)

        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            value = super().observations(context)
            history = value["histories"][0]
            expected = {key: deepcopy(item) for key, item in history.items() if key in {"status", "result", "error"}}
            value["expectedHistories"] = {history["executionArn"]: expected}
            return value

        def conflict(self, context: ScenarioContext) -> None:
            value = context.evidence
            history = value["histories"][0]
            if scope == "operation":
                target = history["operations"][0]
            elif scope == "expectation":
                target = value["expectedHistories"][history["executionArn"]]
            else:
                target = history
            target[field] = deepcopy(conflicting)

        async def place(self, context: ScenarioContext) -> None:
            if context.scenario.case_id == "failed" and phase == "place":
                self.conflict(context)
            await super().place(context)

        async def execute(self, context: ScenarioContext) -> None:
            if context.scenario.case_id == "failed":
                if phase == "execute":
                    self.conflict(context)
                elif phase == "place":
                    context.evidence = self.observations(context)

        async def collect(self, context: ScenarioContext) -> Mapping[str, Any]:
            if context.scenario.case_id == "failed" and phase == "collect":
                self.conflict(context)
            context.evidence = self.observations(context)
            return self.observations(context)

        async def collect_partial(self, context: ScenarioContext) -> Mapping[str, Any]:
            self.conflict(context)
            context.evidence = self.observations(context)
            return self.observations(context)

    adapter = ConflictingContextAdapter(phase="placement" if phase == "partial" else "none")
    collected, following = asyncio.run(
        run_scenarios(
            [_case("failed"), _case("next")],
            [PROFILE],
            adapter,
            run_id="run",
            validator=lambda evidence, *, phase="final": validate_common_evidence(
                evidence,
                phase="observations" if phase == "observations" else "final",
                requirement_ids=("fixture-check",),
            ),
        )
    )
    assert collected.status != "PASSED", collected.errors
    assert any("conflict with retained facts" in error for error in collected.errors), collected.errors
    assert validate_common_evidence(collected.evidence, requirement_ids=("fixture-check",)) == []
    if scope == "expectation":
        # A business-expectation conflict does not contradict the fetched
        # service history used to prove physical retirement.
        assert following.status == "PASSED", following.errors
        assert adapter.finished == [True]
    else:
        assert any("Retirement changed terminal" in error for error in collected.errors), collected.errors
        assert following.status == "INFRASTRUCTURE_ERROR", following.errors
        assert adapter.finished == [False]


@pytest.mark.parametrize("phase", ["place", "collect"])
def test_provisional_context_omissions_do_not_invent_immutable_conflicts(phase: str) -> None:
    class IncompleteContextAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            self.snapshots[context.scenario.case_id]["histories"][0]["result"] = None
            context.evidence = self.observations(context)

        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            value = super().observations(context)
            arn = value["histories"][0]["executionArn"]
            value["expectedHistories"] = {arn: {"status": "SUCCEEDED", "result": None}}
            return value

        async def place(self, context: ScenarioContext) -> None:
            if phase == "place":
                context.evidence = {}

        async def collect(self, context: ScenarioContext) -> Mapping[str, Any]:
            if phase == "collect":
                context.evidence = {}
            return self.observations(context)

    results = asyncio.run(
        run_scenarios(
            [_case("failed")],
            [PROFILE],
            IncompleteContextAdapter(phase="none"),
            run_id="run",
            validator=lambda evidence, *, phase="final": validate_common_evidence(
                evidence,
                phase="observations" if phase == "observations" else "final",
                requirement_ids=("fixture-check",),
            ),
        )
    )
    assert results[0].status == "PASSED", results[0].errors


@pytest.mark.parametrize("damage", ["identity", "event-order"])
def test_retained_identity_and_event_order_conflicts_survive_restoration(damage: str) -> None:
    class RestoredContextAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            value = super().observations(context)
            value["behaviorErrors"] = []
            return value

        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            if damage == "event-order":
                value = self.snapshots[context.scenario.case_id]
                start = next(event for event in value["events"] if event["kind"] == "io_start")
                end = next(event for event in value["events"] if event["kind"] == "io_exit")
                end["atMs"] = start["atMs"]
                _resequence(value)
            context.evidence = self.observations(context)

        async def place(self, context: ScenarioContext) -> None:
            if context.scenario.case_id != "failed":
                return
            if damage == "identity":
                context.evidence["attempts"][0]["workerId"] = "substituted-worker"
            else:
                start = next(event for event in context.evidence["events"] if event["kind"] == "io_start")
                end = next(event for event in context.evidence["events"] if event["kind"] == "io_exit")
                start["sequence"], end["sequence"] = end["sequence"], start["sequence"]

        async def execute(self, context: ScenarioContext) -> None:
            context.evidence = self.observations(context)

    (collected, following), scheduler = _run(RestoredContextAdapter(phase="none"))
    assert collected.status != "PASSED"
    path = "attempts.workerId" if damage == "identity" else "events.sequence"
    assert any("conflict with retained facts" in error and path in error for error in collected.errors), (
        collected.errors
    )
    assert following.status == "INFRASTRUCTURE_ERROR", following.errors
    assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("discover_control", [False, True])
def test_rereading_provisional_fields_does_not_reverse_observed_progress(discover_control: bool) -> None:
    class ReplacedProvisionalAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            value = super().observations(context)
            value["behaviorErrors"] = []
            return value

        async def start(self, context: ScenarioContext) -> None:
            provided = context.evidence
            await super().start(context)
            operation = {"id": "step", "name": "work", "type": "STEP", "parentId": None}
            self.snapshots[context.scenario.case_id]["histories"][0]["operations"] = [
                {**operation, "status": "SUCCEEDED", "result": {"poll": 2}}
            ]
            context.evidence = self.observations(context)
            context.evidence["histories"][0].update(status="RUNNING")
            context.evidence["histories"][0]["operations"] = [{**operation, "status": "STARTED", "result": {"poll": 1}}]
            # Both objects have been observed before place provides completion.
            provided.update(deepcopy(context.evidence))
            self.original_input = provided

        async def place(self, context: ScenarioContext) -> None:
            # The coordinator later revisits the object supplied to place even
            # though this replacement already retained its monotonic completion.
            context.evidence = self.observations(context)
            if discover_control:
                self.original_input["resourcesCreated"].append(
                    {
                        "id": f"control:{context.scenario.case_id}",
                        "kind": "control",
                        "ownerCaseId": context.scenario.case_id,
                    }
                )
                _add_retained_resource(self.snapshots[context.scenario.case_id], "control", "DELETED")

    (collected, following), scheduler = _run(ReplacedProvisionalAdapter(phase="none"))
    assert collected.status == following.status == "PASSED", collected.errors
    assert not any("conflict with retained facts" in error for error in collected.errors)
    assert collected.evidence["histories"][0]["operations"][0]["result"] == {"poll": 2}
    if discover_control:
        assert any(resource["kind"] == "control" for resource in collected.evidence["resourcesCreated"])
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


@pytest.mark.parametrize("scope", ["execution", "operation"])
@pytest.mark.parametrize("field", ["result", "error"])
@pytest.mark.parametrize("outcome", [None, {"value": "original"}])
@pytest.mark.parametrize("rewrite", [False, True])
def test_status_only_completion_retains_present_outcomes_before_a_later_phase(
    scope: str, field: str, outcome: Any, rewrite: bool
) -> None:
    final_outcome = {"value": "rewritten"} if rewrite else outcome
    terminal_status = "SUCCEEDED" if field == "result" else "FAILED"

    class StatusOnlyCompletionAdapter(FailureAdapter):
        def target(self, value: dict[str, Any]) -> dict[str, Any]:
            history = value["histories"][0]
            return history["operations"][0] if scope == "operation" else history

        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            final = self.snapshots[context.scenario.case_id]
            history = final["histories"][0]
            if scope == "operation":
                history["result"] = None
                history["operations"] = [{"id": "step", "name": "work", "type": "STEP", "parentId": None}]
            self.target(final).update(status=terminal_status, **{field: deepcopy(final_outcome)})
            for resource in final["retirement"]["resources"]:
                if resource["kind"] == "execution":
                    resource["outcome"] = history["status"]
            context.evidence = self.observations(context)
            self.target(context.evidence).update(
                status="STARTED" if scope == "operation" else "RUNNING", **{field: deepcopy(outcome)}
            )

        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            value = super().observations(context)
            history = value["histories"][0]
            expected = {
                key: deepcopy(item)
                for key, item in history.items()
                if key in {"status", "result", "error", "operations"}
            }
            value["expectedHistories"] = {history["executionArn"]: expected}
            return value

        async def place(self, context: ScenarioContext) -> None:
            # Only status changes on the supplied object; the current outcome
            # remains explicitly present, including a meaningful JSON null.
            self.target(context.evidence)["status"] = terminal_status

        async def execute(self, context: ScenarioContext) -> None:
            if rewrite:
                self.target(context.evidence)[field] = deepcopy(final_outcome)

        async def retire(self, context: ScenarioContext) -> bool:
            # Exercise the normal retirement-record path, without an independent
            # full snapshot that might incidentally expose the lost observation.
            context.evidence["retirement"] = deepcopy(self.snapshots[context.scenario.case_id]["retirement"])
            return True

    adapter = StatusOnlyCompletionAdapter(phase="none")
    scheduler = ResourceScheduler()
    (collected,) = asyncio.run(
        run_scenarios(
            [_case("failed")],
            [PROFILE],
            adapter,
            run_id="run",
            scheduler=scheduler,
            validator=lambda evidence, *, phase="final": validate_common_evidence(
                evidence,
                phase="observations" if phase == "observations" else "final",
                requirement_ids=("fixture-check",),
            ),
        )
    )
    retained = adapter.target(collected.evidence)
    assert field in retained
    assert same_json(retained[field], outcome)
    if rewrite:
        assert collected.status != "PASSED", collected.errors
        assert any("conflict with retained facts" in error for error in collected.errors), collected.errors
        assert scheduler.quarantine_reason(PROFILE.resource_id)
    else:
        assert collected.status == "PASSED", collected.errors
        assert scheduler.quarantine_reason(PROFILE.resource_id) is None


class ProvisionalOwnershipAdapter(FailureAdapter):
    def __init__(self, inventory: str, missing: tuple[str, ...], phase: str = "none") -> None:
        super().__init__(phase=phase)
        self.inventory, self.missing = inventory, missing

    async def start(self, context: ScenarioContext) -> None:
        await super().start(context)
        value = self.snapshots[context.scenario.case_id]
        for history in value["histories"]:
            history["result"] = None
        row = deepcopy(value[self.inventory][0])
        for name in self.missing:
            row.pop(name)
        context.evidence = {self.inventory: [row]}

    def observations(self, context: ScenarioContext) -> dict[str, Any]:
        value = super().observations(context)
        value["behaviorErrors"] = []
        value["expectedHistories"] = {
            history["executionArn"]: {"status": history["status"], "result": None} for history in value["histories"]
        }
        return value


@pytest.mark.parametrize("phase", ["none", "placement"])
@pytest.mark.parametrize(
    "inventory,missing",
    [
        ("histories", ("ownerCaseId", "status", "operations", "result")),
        ("resourcesCreated", ("kind", "ownerCaseId")),
        ("resourcesCreated", ("ownerCaseId",)),
        ("resourcesCreated", ("kind",)),
        ("faultsCreated", ("ownerCaseId",)),
    ],
)
def test_retirement_completes_provisional_ownership_metadata(
    phase: str, inventory: str, missing: tuple[str, ...]
) -> None:
    adapter = ProvisionalOwnershipAdapter(inventory, missing, phase)
    (collected, following), scheduler = _run(adapter, validator=_service_fact_validator)
    if phase == "none":
        assert collected.status == "PASSED", collected.errors
    else:
        assert collected.status != "PASSED"
        assert PHASE_FAILURE in collected.errors[0]
    assert validate_retirement_evidence(collected.retirement_evidence) == []
    assert following.status == "PASSED", following.errors
    assert adapter.started == ["failed", "next"]
    assert adapter.finished == [True]
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


@pytest.mark.parametrize(
    "inventory,field",
    [
        ("histories", "ownerCaseId"),
        ("resourcesCreated", "ownerCaseId"),
        ("resourcesCreated", "kind"),
        ("faultsCreated", "ownerCaseId"),
    ],
)
@pytest.mark.parametrize("known", [None, "different-value"])
def test_retirement_cannot_replace_explicit_ownership_fields(inventory: str, field: str, known: Any) -> None:
    class KnownOwnershipAdapter(ProvisionalOwnershipAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            if context.scenario.case_id == "failed":
                context.evidence[self.inventory][0][field] = known

    adapter = KnownOwnershipAdapter(inventory, ())
    (collected, following), scheduler = _run(adapter, validator=_service_fact_validator)
    assert collected.status != "PASSED"
    assert any(f"original {inventory} entry" in error for error in collected.errors), collected.errors
    assert following.status == "INFRASTRUCTURE_ERROR"
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("inventory", ["histories", "resourcesCreated", "faultsCreated"])
@pytest.mark.parametrize("malformed", ["missing", None, "", [], {}])
def test_retirement_requires_identifiable_provisional_inventory_rows(inventory: str, malformed: Any) -> None:
    class UnidentifiedOwnershipAdapter(ProvisionalOwnershipAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            if context.scenario.case_id == "failed":
                identifier = "executionArn" if inventory == "histories" else "id"
                row = context.evidence[self.inventory][0]
                if malformed == "missing":
                    row.pop(identifier)
                else:
                    row[identifier] = malformed

    adapter = UnidentifiedOwnershipAdapter(inventory, ())
    (collected, following), scheduler = _run(adapter, validator=_service_fact_validator)
    assert collected.status != "PASSED"
    assert any("stable" in error and inventory in error for error in collected.errors), collected.errors
    assert following.status == "INFRASTRUCTURE_ERROR"
    assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize(
    "inventory,field", [("histories", "ownerCaseId"), ("resourcesCreated", "kind"), ("faultsCreated", "ownerCaseId")]
)
def test_final_retirement_ownership_still_must_be_complete(inventory: str, field: str) -> None:
    class IncompleteFinalOwnershipAdapter(ProvisionalOwnershipAdapter):
        async def retire(self, context: ScenarioContext) -> bool:
            await super().retire(context)
            if context.scenario.case_id == "failed":
                context.retirement_evidence[self.inventory][0].pop(field)
                assert validate_retirement_evidence(context.retirement_evidence)
            return True

    adapter = IncompleteFinalOwnershipAdapter(inventory, (field,))
    (collected, following), scheduler = _run(adapter, validator=_service_fact_validator)
    assert collected.status != "PASSED"
    assert following.status == "INFRASTRUCTURE_ERROR"
    assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize(
    ("field", "supplied", "canonical_prefix", "unresolved"),
    [
        ("kind", None, "INCOMPLETE:", True),
        ("kind", "", "INCOMPLETE:", True),
        ("kind", " ", "INCOMPLETE:", True),
        ("kind", 42, "INCOMPLETE:", True),
        ("requestId", "request-unknown", "INCOMPLETE:", True),
        ("requestId", None, "INCOMPLETE:", True),
        ("requestId", "", "INCOMPLETE:", True),
        ("requestId", False, "INCOMPLETE:", True),
        ("requestId", [], "INCOMPLETE:", True),
        ("ownerRequestId", "request-peer", "ASSERTION:", False),
        ("ownerRequestId", "request-unknown", "ASSERTION:", True),
        ("ownerRequestId", None, "ASSERTION:", True),
        ("ownerRequestId", "", "ASSERTION:", True),
        ("ownerRequestId", 42, "ASSERTION:", True),
        ("executionArn", "arn:execution:peer", "ASSERTION:", False),
        ("executionArn", "arn:execution:unknown", "ASSERTION:", True),
        ("executionArn", None, "ASSERTION:", True),
        ("executionArn", "", "ASSERTION:", True),
        ("executionArn", 42, "ASSERTION:", True),
        ("observedDuringRequestId", "request-unknown", "INCOMPLETE:", True),
        ("observedDuringRequestId", "", "INCOMPLETE:", True),
        ("observedDuringRequestId", False, "INCOMPLETE:", True),
        ("observedDuringRequestId", [], "INCOMPLETE:", True),
        ("observedDuringRequestId", "request-peer", "ASSERTION:", False),
    ],
)
def test_cleanup_common_record_event_fields_survive_restoration(
    path: str, field: str, supplied: Any, canonical_prefix: str, unresolved: bool
) -> None:
    value = _standard_retirement_snapshot("evidence")
    original = deepcopy(value["events"])
    probe_index = next(index for index, event in enumerate(original) if event["kind"] == "admission_probe")
    damaged = deepcopy(value)
    damaged["events"][probe_index][field] = supplied
    canonical = validate_common_evidence(damaged, phase="observations")
    assert canonical and all(error.startswith(canonical_prefix) for error in canonical), canonical
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            snapshot["events"][probe_index][field] = supplied
        else:
            snapshot["events"] = deepcopy(original)

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path=path, quiescent=not unresolved)
    assert result.status == ("INFRASTRUCTURE_ERROR" if unresolved else "FAILED"), result.errors
    # Unknown or malformed ownership additionally prevents physical retirement,
    # even where the common validator reports an attribution assertion.
    expected_prefix = "INCOMPLETE:" if unresolved else "ASSERTION:"
    assert any(error.startswith(expected_prefix) for error in result.errors), result.errors
    assert result.evidence["events"] == original
    assert same_json(result.cleanup_evidence["release"][0]["events"][probe_index][field], supplied)


@pytest.mark.parametrize(
    ("field", "supplied", "prefix"),
    [
        ("kind", None, "INCOMPLETE:"),
        ("requestId", "request-unknown", "INCOMPLETE:"),
        ("ownerRequestId", "request-peer", "ASSERTION:"),
        ("executionArn", "arn:execution:peer", "ASSERTION:"),
        ("observedDuringRequestId", "request-unknown", "INCOMPLETE:"),
    ],
)
def test_cleanup_common_record_independent_snapshot_rejects_invalid_event_fields(
    field: str, supplied: Any, prefix: str
) -> None:
    value = _standard_retirement_snapshot("evidence")
    original = deepcopy(value["events"])

    def mutate(snapshot: dict[str, Any]) -> None:
        next(event for event in snapshot["events"] if event["kind"] == "admission_probe")[field] = supplied
        canonical = validate_common_evidence(snapshot, phase="observations")
        assert canonical and all(error.startswith(prefix) for error in canonical), canonical

    result = _run_standard_cleanup_discovery(
        value, mutate, phase="retirement", path="independent", quiescent=False, retirement_valid=False
    )
    statuses = {"FAILED", "INFRASTRUCTURE_ERROR"} if prefix == "ASSERTION:" else {"INFRASTRUCTURE_ERROR"}
    assert result.status in statuses, result.errors
    assert any(error.startswith(prefix) for error in result.errors), result.errors
    assert result.evidence["events"] == original


@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize("field", ["kind", "requestId", "ownerRequestId", "executionArn", "observedDuringRequestId"])
def test_cleanup_common_record_absent_event_fields_remain_provisional(path: str, field: str) -> None:
    value = _standard_retirement_snapshot("evidence")
    original = deepcopy(value["events"])
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            event = next(event for event in snapshot["events"] if event["kind"] == "admission_probe")
            event.pop(field, None)
        else:
            snapshot["events"] = deepcopy(original)

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path=path)
    assert result.status == "PASSED", result.errors
    assert result.evidence["events"] == original


@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize("phase", ["both", "snapshot"])
@pytest.mark.parametrize("kind", ["effect_observed", "adapter.custom.diagnostic"])
def test_cleanup_common_record_accepts_custom_diagnostic_kinds(path: str, phase: str, kind: str) -> None:
    value = _standard_retirement_snapshot("evidence")
    original = deepcopy(value["events"])
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            event = deepcopy(next(event for event in snapshot["events"] if event["kind"] == "admission_probe"))
            event["kind"] = kind
            snapshot["events"].append(event)
            _resequence(snapshot)
            assert validate_evidence(snapshot, phase="observations") == []
        else:
            snapshot["events"] = deepcopy(original)

    result = _run_standard_cleanup_discovery(value, mutate, phase=phase, path=path)
    assert result.status == "PASSED", result.errors
    assert result.evidence["events"] == original


@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize("phase", ["both", "snapshot"])
def test_cleanup_common_record_accepts_unassigned_observation_correlation(path: str, phase: str) -> None:
    value = _standard_retirement_snapshot("evidence")
    original = deepcopy(value["events"])
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            next(event for event in snapshot["events"] if event["kind"] == "admission_probe")[
                "observedDuringRequestId"
            ] = None
            assert validate_common_evidence(snapshot, phase="observations") == []
        else:
            snapshot["events"] = deepcopy(original)

    result = _run_standard_cleanup_discovery(value, mutate, phase=phase, path=path)
    assert result.status == "PASSED", result.errors
    assert result.evidence["events"] == original


@pytest.mark.parametrize("path", ["normal", "independent"])
@pytest.mark.parametrize("unknown", ["requestId", "ownerRequestId", "executionArn"])
def test_cleanup_common_record_cannot_drop_custom_event_with_unknown_owner(path: str, unknown: str) -> None:
    value = _standard_retirement_snapshot("evidence")
    original = deepcopy(value["events"])
    event = {
        "kind": "effect_observed",
        "requestId": "request-victim",
        "ownerRequestId": "request-victim",
        "executionArn": "arn:execution:victim",
        "atMs": 25,
    }
    event[unknown] = "arn:execution:unknown" if unknown == "executionArn" else "request-unknown"
    damaged = deepcopy(value)
    damaged["events"].append(event)
    canonical = validate_common_evidence(damaged, phase="observations")
    expected_prefix = "INCOMPLETE:" if unknown == "requestId" else "ASSERTION:"
    assert canonical and all(error.startswith(expected_prefix) for error in canonical), canonical
    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        snapshot["events"] = [*deepcopy(original), deepcopy(event)] if calls == 1 else deepcopy(original)

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path=path, quiescent=False)
    assert result.status == "INFRASTRUCTURE_ERROR", result.errors
    assert any(error.startswith("INCOMPLETE:") for error in result.errors), result.errors
    assert result.evidence["events"] == original
    assert result.cleanup_evidence["release"][0]["events"][-1] == event


def test_cleanup_common_record_late_native_binding_can_establish_physical_retirement() -> None:
    value = _standard_retirement_snapshot("evidence")
    deployment = value["profile"]
    profile = Profile(
        "standard", deployment["functionArn"], 2, 2, value["artifact"]["sdk"], deployment["configurationDigest"]
    )
    case = Scenario(
        value["caseId"], value["requirementId"], profile.profile_id, "fixture", roles=tuple(value["selectedRequests"])
    )
    event = {
        "kind": "effect_observed",
        "requestId": "request-late",
        "ownerRequestId": "request-late",
        "executionArn": "arn:execution:late",
        "atMs": 25,
    }
    final = deepcopy(value)
    request = {
        **value["attempts"][0],
        "requestId": event["requestId"],
        "executionArn": event["executionArn"],
        "attempt": 2,
        "startedAtMs": 20,
        "endedAtMs": 30,
    }
    final["attempts"].append(request)
    owner = {key: event[key] for key in ("requestId", "ownerRequestId", "executionArn")}
    final["events"].extend(
        [
            {**owner, "kind": "wrapper_enter", "atMs": 20},
            {**owner, "kind": "task_start", "taskId": "root", "atMs": 20.1},
            event,
            {**owner, "kind": "task_exit", "taskId": "root", "atMs": 29.9},
            {**owner, "kind": "wrapper_return", "atMs": 30},
        ]
    )
    final["histories"].append(
        {
            "executionArn": event["executionArn"],
            "ownerCaseId": value["caseId"],
            "status": "SUCCEEDED",
            "result": None,
            "operations": [],
        }
    )
    final["expectedHistories"][event["executionArn"]] = {"status": "SUCCEEDED", "result": None}
    for kind, identifier, outcome in (
        ("invocation", event["requestId"], "QUIESCENT"),
        ("execution", event["executionArn"], "SUCCEEDED"),
    ):
        resource = {"id": identifier, "kind": kind, "ownerCaseId": value["caseId"]}
        final["resourcesCreated"].append(resource)
        final["retirement"]["resources"].append(
            {
                **resource,
                "outcome": outcome,
                "settledAtMs": 100,
                "evidenceUri": f"s3://raw/{value['runId']}/{value['caseId']}/{identifier}",
            }
        )
    _resequence(final)
    assert validate_common_evidence(final) == []
    assert validate_retirement_evidence(final) == []
    finished: list[bool] = []

    class Adapter:
        async def prepare(self, profile: Profile) -> None:
            pass

        async def finish(self, profile: Profile, *, quiescent: bool) -> None:
            finished.append(quiescent)

        async def start(self, context: ScenarioContext) -> None:
            for attempt in value["attempts"]:
                context.record_attempt(
                    attempt["requestId"],
                    role=attempt["role"],
                    requestId=attempt["requestId"],
                    executionArn=attempt["executionArn"],
                )
            context.record_attempt("late-launch", role="victim")

        async def place(self, context: ScenarioContext) -> None:
            pass

        async def execute(self, context: ScenarioContext) -> None:
            pass

        async def collect(self, context: ScenarioContext) -> Mapping[str, Any]:
            observed = deepcopy(value)
            observed.pop("retirement")
            assert validate_evidence(observed, phase="observations") == []
            return observed

        async def release(self, context: ScenarioContext) -> None:
            context.evidence["events"].append(deepcopy(event))
            canonical = validate_common_evidence(context.evidence, phase="observations")
            assert canonical == ["INCOMPLETE: event references an unrecorded request"]

        async def retire(self, context: ScenarioContext) -> bool:
            context.update_attempt("late-launch", requestId=event["requestId"], executionArn=event["executionArn"])
            context.retirement_evidence = deepcopy(final)
            return True

    scheduler = ResourceScheduler()

    def validator(evidence: Mapping[str, Any], *, phase: str = "final") -> list[str]:
        return validate_evidence(evidence, phase="observations" if phase == "observations" else "final")

    async def exercise() -> ScenarioResult:
        (result,) = await run_scenarios(
            [case], [profile], Adapter(), run_id=value["runId"], validator=validator, scheduler=scheduler
        )
        lease = await scheduler.acquire(profile.resource_id, "subsequent-case", exclusive=True, timeout=0.1)
        await lease.release(quiescent=True)
        return result

    result = asyncio.run(exercise())
    assert result.status == "INCOMPLETE_EVIDENCE", result.errors
    assert "Every launched attempt must retain an identified invocation request" in result.errors
    assert finished == [True]
    assert result.evidence["events"] == value["events"]
    assert validate_retirement_evidence(result.retirement_evidence) == []
    assert any(attempt.get("requestId") == event["requestId"] for attempt in result.attempts)


def test_in_place_retirement_cannot_omit_a_known_post_cutoff_retry() -> None:
    class InPlaceAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            observed["behaviorErrors"] = []
            return observed

        async def retire(self, context: ScenarioContext) -> bool:
            await super().retire(context)
            if context.scenario.case_id == "failed":
                context.retirement_evidence = {}
                context.record_attempt(
                    "late-retry",
                    role="victim",
                    source="service-observed",
                    requestId="late-request",
                    executionArn=self.snapshots["failed"]["attempts"][0]["executionArn"],
                    nativeStartedAtMs=20100,
                )
            return True

    results, scheduler = _run(InPlaceAdapter(phase="assertion"))
    first, following = results
    assert first.status == following.status == "INFRASTRUCTURE_ERROR"
    assert any("has no independent retirement evidence" in error for error in first.errors)
    assert scheduler.quarantine_reason(PROFILE.resource_id)


@pytest.mark.parametrize("contradiction", ["expectedBefore", "durableWaitObserved", "suspensionCheckpointMatches"])
def test_retirement_replay_contradictions_fail_behavior_without_holding_lease(contradiction):
    value = _standard_retirement_snapshot("replay_evidence")

    def mutate(snapshot):
        replay = snapshot["replay"]
        if contradiction == "expectedBefore":
            extra = dict(replay["before"][0], id="unreported-completed-operation")
            replay[contradiction] = [*replay["before"], extra]
        else:
            replay[contradiction] = False

    result = _run_standard_cleanup_discovery(value, mutate, phase="snapshot", path="independent")
    assert result.status == "FAILED", result.errors
    assert any("cleanup service history" in error for error in result.errors), result.errors

    offline = deepcopy(value)
    snapshot = deepcopy(value)
    mutate(snapshot)
    offline["retirementSnapshot"] = snapshot
    assert any("cleanup service history" in error for error in validate_evidence(offline))


def test_cleanup_wrapper_raise_is_an_admission_boundary_for_late_poll():
    original = _standard_retirement_snapshot("deadline_evidence")
    snapshot = deepcopy(original)
    retry = dict(snapshot["attempts"][0], requestId="late-retry", attempt=2, startedAtMs=215, endedAtMs=217)
    snapshot["attempts"].append(retry)
    scope = {
        "requestId": "late-retry",
        "ownerRequestId": "late-retry",
        "executionArn": retry["executionArn"],
    }
    snapshot["events"].append({**scope, "kind": "wrapper_raise", "atMs": 217})
    snapshot["sdkRequests"].append(
        {
            **scope,
            "method": "GetDurableExecutionState",
            "outcome": "DISPATCHED",
            "atMs": 218,
        }
    )
    errors = _cleanup_behavior_errors(original, snapshot, standard=True)
    assert any(error.startswith("ASSERTION:") and "dispatch" in error for error in errors), errors


def test_in_place_retirement_must_audit_service_only_deferred_request():
    class DeferredAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            observed["behaviorErrors"] = []
            if context.scenario.case_id == "failed":
                observed["collection"]["deferredServiceRequests"] = [
                    {
                        "requestId": "not-yet-published",
                        "executionArn": observed["attempts"][0]["executionArn"],
                    }
                ]
            return observed

        async def retire(self, context: ScenarioContext) -> bool:
            await super().retire(context)
            if context.scenario.case_id == "failed":
                context.retirement_evidence = {}
            return True

    (first, next_case), scheduler = _run(DeferredAdapter(phase="assertion"))
    assert first.status == next_case.status == "INFRASTRUCTURE_ERROR"
    assert scheduler.quarantine_reason(PROFILE.resource_id)
    assert any("independent retirement evidence" in error for error in first.errors)


@pytest.mark.parametrize("deferred", [None, {}, [None], [{"requestId": "", "executionArn": "owned"}]])
def test_malformed_deferred_requests_keep_the_target_fenced(deferred: Any) -> None:
    class MalformedDeferralAdapter(FailureAdapter):
        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            if context.scenario.case_id == "failed":
                observed["collection"]["deferredServiceRequests"] = deferred
            return observed

    adapter = MalformedDeferralAdapter(phase="assertion")
    (first, _), scheduler = _run(adapter)
    assert first.status != "PASSED"
    assert any("Deferred service request reconciliation" in error for error in first.errors), first.errors
    assert scheduler.quarantine_reason(PROFILE.resource_id)
    assert adapter.finished == [False]


def test_unreleased_case_lease_keeps_the_target_fenced(monkeypatch: pytest.MonkeyPatch) -> None:
    import aws_durable_execution_conformance_tests.lmi.coordinator as coordinator

    def fail(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("audit crashed before lease release")

    monkeypatch.setattr(coordinator, "_ownership_claims", fail)
    adapter = FailureAdapter(phase="assertion")
    scheduler = ResourceScheduler()
    with pytest.raises(RuntimeError, match="audit crashed"):
        asyncio.run(
            run_scenarios(
                [_case("failed")], [PROFILE], adapter, run_id="run", validator=_validator, scheduler=scheduler
            )
        )
    reason = scheduler.quarantine_reason(PROFILE.resource_id)
    assert reason and "was not released" in reason
    assert adapter.finished == [False]


def test_no_work_missing_fixture_permits_later_case_on_same_target():
    class MissingThenSupported(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            if context.scenario.case_id == "failed":
                raise NotImplementedError("fixture unavailable")
            await super().start(context)

        async def collect_partial(self, context: ScenarioContext) -> Any:
            if context.scenario.case_id == "failed":
                return {}
            return await super().collect_partial(context)

        async def retire(self, context: ScenarioContext) -> bool:
            if context.scenario.case_id == "failed":
                return True
            return await super().retire(context)

    (missing, supported), scheduler = _run(MissingThenSupported(phase="assertion"))
    assert missing.status == "NOT_IMPLEMENTED" and not missing.retirement_evidence
    assert supported.status == "PASSED", supported.errors
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


def test_retirement_only_replay_projection_errors_do_not_hold_lease():
    value = _standard_retirement_snapshot("replay_evidence")

    def mutate(snapshot):
        snapshot["replay"]["projectionErrors"] = ["retry chronology unavailable"]

    result = _run_standard_cleanup_discovery(value, mutate, phase="snapshot", path="independent")

    assert result.status == "INCOMPLETE_EVIDENCE", result.errors
    assert any("retry chronology unavailable" in error for error in result.errors)


def test_offline_retirement_allows_only_the_selected_disposal_operation_probe():
    original = _standard_retirement_snapshot("evidence")
    snapshot = deepcopy(original)
    victim = snapshot["attempts"][0]
    probe = {
        "requestId": victim["requestId"],
        "ownerRequestId": victim["requestId"],
        "executionArn": victim["executionArn"],
        "kind": "admission_probe",
        "probe": "operation",
        "outcome": "ACCEPTED",
        "atMs": victim["endedAtMs"] + 0.1,
    }
    snapshot["events"].append(probe)
    _resequence(snapshot)
    original["retirementSnapshot"] = snapshot
    assert validate_evidence(original) == []

    probe["probe"] = "checkpoint"
    assert any("admitted a checkpoint probe after admission closed" in error for error in validate_evidence(original))

    # A closure visible only in observations still bounds the detached probe.
    original["events"].append(
        {
            **{key: probe[key] for key in ("requestId", "ownerRequestId", "executionArn")},
            "kind": "admission_closed",
            "atMs": victim["endedAtMs"] - 0.2,
        }
    )
    _resequence(original)
    probe["atMs"] = victim["endedAtMs"] - 0.1
    _resequence(snapshot)
    assert validate_retirement_evidence(snapshot) == []
    assert any("admitted a checkpoint probe after admission closed" in error for error in validate_evidence(original))


def test_offline_retirement_rejects_a_changed_service_checkpoint_result():
    value = _standard_retirement_snapshot("replay_evidence")
    snapshot = deepcopy(value)
    victim = value["selectedRequests"]["victim"]
    arn = next(attempt["executionArn"] for attempt in value["attempts"] if attempt["requestId"] == victim)
    history = next(row for row in snapshot["histories"] if row["executionArn"] == arn)
    operation = next(row for row in history["operations"] if "result" in row)
    operation["result"] = {"unexpected": True}
    value["retirementSnapshot"] = snapshot
    assert any("has contradictory observations" in error for error in validate_evidence(value))


@pytest.mark.parametrize("change", ["work", "result", "body", "effects", "decimal"])
def test_offline_retirement_reconciles_frozen_work_and_replay(change):
    value = _standard_retirement_snapshot("replay_evidence")
    victim = value["selectedRequests"]["victim"]
    arn = next(attempt["executionArn"] for attempt in value["attempts"] if attempt["requestId"] == victim)
    history = next(row for row in value["histories"] if row["executionArn"] == arn)
    if change == "work":
        scope = {"requestId": victim, "ownerRequestId": victim, "executionArn": arn, "callId": "observed-io"}
        value["events"].extend(
            [
                {**scope, "kind": "io_start", "atMs": 35},
                {**scope, "kind": "io_exit", "atMs": 36},
            ]
        )
        _resequence(value)
    snapshot = deepcopy(value)
    if change == "work":
        snapshot["events"] = [row for row in snapshot["events"] if row.get("callId") != "observed-io"]
        expected = "previously observed work"
    elif change == "result":
        next(row for row in snapshot["histories"] if row["executionArn"] == arn)["result"] = "changed"
        expected = "terminal execution"
    elif change == "body":
        snapshot["replay"]["operationExecutions"].append(
            {"operationId": value["replay"]["before"][0]["id"], "requestId": value["selectedRequests"]["resume"]}
        )
        expected = "repeated completed user code"
    elif change == "effects":
        snapshot["replay"]["sideEffects"][0]["count"] += 1
        expected = "differs from the frozen fixture oracle"
    else:
        source_operation = next(
            row for row in history["operations"] if row.get("status") == "SUCCEEDED" and "result" in row
        )
        operation_id = source_operation["id"]
        for node in (*value["replay"]["before"], *value["replay"]["after"], source_operation):
            if node["id"] == operation_id:
                node.update(result=1.0, resultEncoding="json", resultFingerprint="a" * 64)
        snapshot = deepcopy(value)
        next(row for row in snapshot["replay"]["after"] if row["id"] == operation_id)["resultFingerprint"] = "b" * 64
        expected = "contradictory observations"
    assert validate_retirement_evidence(snapshot) == []
    value["retirementSnapshot"] = snapshot
    assert any(expected in error for error in validate_evidence(value)), validate_evidence(value)


def test_live_disposal_accepts_a_strictly_later_probe_in_a_partial_snapshot():
    original = _standard_retirement_snapshot("evidence")
    victim = original["attempts"][0]
    fragment = {
        "attempts": [],
        "events": [
            {
                "requestId": victim["requestId"],
                "kind": "admission_probe",
                "probe": "operation",
                "outcome": "ACCEPTED",
                "atMs": victim["endedAtMs"] + 1,
            }
        ],
    }
    assert _cleanup_behavior_errors(original, fragment, standard=True) == []
    fragment["events"][0].pop("outcome")
    assert any("admitted" in error for error in _cleanup_behavior_errors(original, fragment, standard=True))


@pytest.mark.parametrize(("count", "expected", "fails"), [(1, 0, True), (1, 1, False), (0, 1, False)])
def test_late_fixture_policy_assertion_keeps_retirement_verified(count, expected, fails):
    value = _standard_retirement_snapshot("replay_evidence")

    def mutate(snapshot):
        snapshot["replay"]["sideEffects"].append({"operationId": "new", "count": count, "expectedCount": expected})

    result = _run_standard_cleanup_discovery(value, mutate, phase="snapshot", path="independent")
    assert result.status == ("FAILED" if fails else "PASSED"), result.errors
    snapshot = deepcopy(value)
    mutate(snapshot)
    value["retirementSnapshot"] = snapshot
    assert any("exceeds the fixture oracle" in error for error in validate_evidence(value)) is fails


def test_offline_retirement_cannot_drop_an_aliased_child_task():
    original = _standard_retirement_snapshot("evidence")
    snapshot = deepcopy(original)
    victim = original["attempts"][0]
    scope = {
        "requestId": victim["requestId"],
        "ownerRequestId": victim["requestId"],
        "executionArn": victim["executionArn"],
        "taskId": "child",
    }
    original["events"].extend(
        [
            {**scope, "kind": "observed_task_start", "sourceKind": "task_start", "atMs": victim["startedAtMs"] + 1},
            {**scope, "kind": "observed_task_exit", "sourceKind": "task_exit", "atMs": victim["endedAtMs"] - 1},
        ]
    )
    _resequence(original)
    original["retirementSnapshot"] = snapshot

    assert validate_retirement_evidence(snapshot) == []
    assert any("previously observed work" in error for error in validate_evidence(original))


def test_saved_replay_counts_accept_integral_json_floats():
    value = _standard_retirement_snapshot("replay_evidence")
    snapshot = deepcopy(value)
    for capture in (value, snapshot):
        for row in capture["replay"]["sideEffects"]:
            row["count"] = float(row["count"])
            row["expectedCount"] = float(row["expectedCount"])
    value["retirementSnapshot"] = snapshot

    assert validate_evidence(value) == []


def test_cleanup_replay_rejects_undeclared_retry_body() -> None:
    value = _standard_retirement_snapshot("replay_evidence")

    def mutate(snapshot: dict[str, Any]) -> None:
        resume = next(row for row in snapshot["attempts"] if row["role"] == "resume")
        snapshot["events"].append(
            {
                "kind": "body",
                "requestId": resume["requestId"],
                "ownerRequestId": resume["requestId"],
                "executionArn": resume["executionArn"],
                "operationId": "retry",
                "attemptId": "extra",
                "atMs": 60,
            }
        )
        _resequence(snapshot)

    result = _run_standard_cleanup_discovery(value, mutate, phase="release", path="independent")
    assert result.status == "FAILED", result.errors
    assert any("retry body does not match a declared attempt" in error for error in result.errors)


def test_cleanup_replay_tied_body_and_pending_allow_renumbering() -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    pending = next(event for event in value["events"] if event["kind"] == "pending")
    body = next(event for event in value["events"] if event.get("operationId") == "done")
    body["atMs"] = pending["atMs"]
    _resequence(value)

    def mutate(snapshot: dict[str, Any]) -> None:
        owner = {key: body[key] for key in ("requestId", "ownerRequestId", "executionArn")}
        snapshot["events"].extend({**owner, "kind": "fixture_observation", "atMs": at} for at in (15, 16))
        _resequence(snapshot)

    result = _run_standard_cleanup_discovery(value, mutate, phase="release", path="independent")
    assert result.status == "PASSED", result.errors


@pytest.mark.parametrize(
    ("operation_id", "end", "diagnostic"),
    [
        ("done", 20, "completed replay body occurred after recorded completion"),
        ("done", 40, "replay.before contains an operation that completed after suspension"),
        ("retry", 48, "retry owner was admitted after recorded operation completion"),
    ],
)
def test_cleanup_reconciles_service_checkpoint_completion_times(operation_id: str, end: int, diagnostic: str) -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    snapshot = deepcopy(value)
    inventories = [snapshot["replay"]["before"], snapshot["replay"]["after"]]
    inventories.extend(history["operations"] for history in snapshot["histories"])
    for operations in inventories:
        for operation in operations:
            if operation["id"] == operation_id:
                operation["endedAtMs"] = end
    assert any(diagnostic in error for error in _cleanup_behavior_errors(value, snapshot, standard=True, final=True))


def test_cleanup_overlap_rejects_additional_record_marker() -> None:
    value = _standard_retirement_snapshot("evidence", requirement=1)
    snapshot = deepcopy(value)
    history = next(row for row in snapshot["histories"] if row["executionArn"] == "arn:execution:victim")
    marker = next(row for row in history["operations"] if row["name"] == "record-marker")
    history["operations"].append({**marker, "id": "second-marker"})
    assert any(
        "more than one record-marker" in error
        for error in _cleanup_behavior_errors(value, snapshot, standard=True, final=True)
    )


def test_cleanup_retry_endpoints_are_checked_across_partial_orders() -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    snapshot = deepcopy(value)
    retry = snapshot["replay"]["retry"]
    last = {"attemptId": "additional", "requestId": "request-resume", "outcome": "FAILED"}
    retry["attempts"] = [retry["attempts"][-1], last]
    snapshot["replay"]["operationExecutions"].append({**last, "operationId": "retry"})
    assert any(
        "do not start with failure and end with success" in error
        for error in _cleanup_behavior_errors(value, snapshot, standard=True, final=True)
    )


@pytest.mark.parametrize("changed_time", [False, True])
def test_cleanup_retry_uses_canonical_numeric_body_identity(changed_time: bool) -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    resume = next(row for row in value["attempts"] if row["role"] == "resume")
    bodies = []
    for number, declared in enumerate(value["replay"]["retry"]["attempts"], 1):
        prior_id = declared["attemptId"]
        declared.update(attemptId=f"{resume['requestId']}:{number}", requestId=resume["requestId"])
        next(row for row in value["replay"]["operationExecutions"] if row.get("attemptId") == prior_id).update(declared)
        bodies.append(
            {
                "kind": "body",
                "requestId": resume["requestId"],
                "ownerRequestId": resume["requestId"],
                "executionArn": resume["executionArn"],
                "atMs": 55,
                "operationId": "retry",
                "attempt": number,
            }
        )
    value["events"].extend(bodies)
    _resequence(value)
    snapshot = deepcopy(value)
    if changed_time:
        next(row for row in snapshot["events"] if row.get("attempt") == 2)["atMs"] = 60
    _resequence(snapshot)
    findings = _cleanup_behavior_errors(value, snapshot, standard=True, final=True)
    assert any("retry body changed its observed timestamp" in error for error in findings) is changed_time
    if not changed_time:
        assert findings == []


def test_cleanup_deadline_rejects_earlier_admission_closure() -> None:
    value = _standard_retirement_snapshot("deadline_evidence")
    snapshot = deepcopy(value)
    next(row for row in snapshot["events"] if row["kind"] == "admission_closed")["atMs"] = 29
    assert any(
        "admission closed before the observed platform timeout" in error
        for error in _cleanup_behavior_errors(value, snapshot, standard=True, final=True)
    )


def test_cleanup_reconciles_original_invocation_and_ordinal_order() -> None:
    value = _standard_retirement_snapshot("evidence")
    victim = next(row for row in value["attempts"] if row["role"] == "victim")
    value["attempts"].append(
        {**victim, "requestId": "request-victim-retry", "attempt": 2, "startedAtMs": 60, "endedAtMs": 80}
    )
    snapshot = deepcopy(value)
    snapshot["attempts"][-1]["startedAtMs"] = 9
    findings = _cleanup_behavior_errors(value, snapshot, standard=True, final=True)
    assert any("ordinals contradict admission order" in error for error in findings)
    assert any("selected victim is not the original invocation" in error for error in findings)


@pytest.mark.parametrize("role_name", ["recovery", "victim"])
def test_cleanup_rechecks_tied_original_admissions(role_name: str) -> None:
    value = _standard_retirement_snapshot("recovery_evidence" if role_name == "recovery" else "replay_evidence")
    selected = next(row for row in value["attempts"] if row["role"] == role_name)
    value["attempts"].append(
        {
            **selected,
            "requestId": "later-native",
            "attempt": 2,
            "startedAtMs": 60,
            "startedAtMsSource": "fixture",
            "endedAtMs": 90,
        }
    )
    snapshot = deepcopy(value)
    snapshot["attempts"][-1].update(startedAtMs=selected["startedAtMs"], startedAtMsSource="platform")
    findings = _cleanup_behavior_errors(value, snapshot, standard=True, final=True)
    assert any(f"tied native admissions cannot establish the original {role_name}" in error for error in findings)


def test_cleanup_rechecks_peer_progress_against_corrected_recovery() -> None:
    value = _standard_retirement_snapshot("recovery_evidence")
    snapshot = deepcopy(value)
    next(row for row in snapshot["events"] if row["kind"] == "worker_recovered")["atMs"] = 43
    findings = _cleanup_behavior_errors(value, snapshot, standard=True, final=True)
    assert any("healthy peer lacks progress across the timeout and recovery" in error for error in findings)


def test_cleanup_retry_checks_transitive_body_order_across_fragments() -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    resume = next(row for row in value["attempts"] if row["role"] == "resume")
    retry = value["replay"]["retry"]
    first, middle = retry["attempts"]
    first["requestId"] = resume["requestId"]
    next(row for row in value["replay"]["operationExecutions"] if row.get("attemptId") == first["attemptId"]).update(
        first
    )
    # The middle attempt deliberately has no timed body observation.
    owner = {
        "requestId": resume["requestId"],
        "ownerRequestId": resume["requestId"],
        "executionArn": resume["executionArn"],
    }
    value["events"].append(
        {**owner, "kind": "body", "operationId": "retry", "attemptId": first["attemptId"], "atMs": 60}
    )
    _resequence(value)
    snapshot = deepcopy(value)
    last = {"attemptId": "last", "requestId": resume["requestId"], "outcome": "SUCCEEDED"}
    snapshot["replay"]["retry"]["attempts"] = [middle, last]
    snapshot["replay"]["operationExecutions"].append({**last, "operationId": "retry"})
    snapshot["events"].append({**owner, "kind": "body", "operationId": "retry", "attemptId": "last", "atMs": 55})
    _resequence(snapshot)
    findings = _cleanup_behavior_errors(value, snapshot, standard=True, final=True)
    assert any("retry attempt order contradicts timed user-code entries" in error for error in findings)


def test_cleanup_retry_cannot_certify_ambiguous_endpoints() -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    snapshot = deepcopy(value)
    first = snapshot["replay"]["retry"]["attempts"][0]
    unplaced = {"attemptId": "unplaced", "requestId": "request-resume", "outcome": "FAILED"}
    snapshot["replay"]["retry"]["attempts"] = [first, unplaced]
    snapshot["replay"]["operationExecutions"].append({**unplaced, "operationId": "retry"})
    findings = _cleanup_behavior_errors(value, snapshot, standard=True, final=True)
    assert any("retry order does not establish unique first and last attempts" in error for error in findings)


def test_cleanup_rejects_completed_replay_bodies_after_observed_pending() -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    snapshot = deepcopy(value)
    next(event for event in snapshot["events"] if event["kind"] == "pending")["atMs"] = 20
    _resequence(snapshot)
    findings = _cleanup_behavior_errors(value, snapshot, standard=True, final=True)
    assert any("reports repeated completed user code for done" in error for error in findings)
    assert any("reports repeated completed user code for failed" in error for error in findings)


def test_cleanup_ambiguous_retry_endpoints_do_not_block_verified_retirement() -> None:
    value = _standard_retirement_snapshot("replay_evidence")

    def mutate(snapshot: dict[str, Any]) -> None:
        first = snapshot["replay"]["retry"]["attempts"][0]
        unplaced = {"attemptId": "unplaced", "requestId": "request-resume", "outcome": "FAILED"}
        snapshot["replay"]["retry"]["attempts"] = [first, unplaced]
        snapshot["replay"]["operationExecutions"].append({**unplaced, "operationId": "retry"})

    result = _run_standard_cleanup_discovery(value, mutate, phase="snapshot", path="independent")
    assert result.status == "INCOMPLETE_EVIDENCE", result.errors
    assert any("retry order does not establish unique first and last attempts" in error for error in result.errors)


@pytest.mark.parametrize(
    ("witnessed", "count", "untimed_pending", "status"),
    [
        (False, 1, False, "INCOMPLETE_EVIDENCE"),
        (True, 1, False, "PASSED"),
        (True, 2, False, "FAILED"),
        (True, None, False, "INCOMPLETE_EVIDENCE"),
        (True, 1, True, "PASSED"),
    ],
)
def test_cleanup_new_before_step_requires_an_observed_original_body(
    witnessed: bool, count: int | None, untimed_pending: bool, status: str
) -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    operation = {
        "id": "extra",
        "name": "extra",
        "type": "STEP",
        "parentId": None,
        "status": "SUCCEEDED",
        "result": "ok",
    }
    value["replay"]["after"].append(deepcopy(operation))
    history = next(row for row in value["histories"] if row["executionArn"] == "arn:execution:victim")
    history["operations"].append(deepcopy(operation))

    calls = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        if untimed_pending:
            pending = next(event for event in snapshot["events"] if event["kind"] == "pending")
            if calls == 1:
                pending.pop("atMs")
                return
            pending["atMs"] = next(event["atMs"] for event in value["events"] if event["kind"] == "pending")
        snapshot["replay"]["before"].append(deepcopy(operation))
        if count is not None:
            snapshot["replay"]["sideEffects"].append({"operationId": "extra", "count": count, "expectedCount": 1})
        if witnessed:
            victim = next(row for row in snapshot["attempts"] if row["role"] == "victim")
            owner = {key: victim[key] for key in ("requestId", "executionArn")}
            snapshot["replay"]["operationExecutions"].append({**owner, "operationId": "extra"})
            snapshot["events"].append(
                {**owner, "ownerRequestId": victim["requestId"], "kind": "body", "operationId": "extra", "atMs": 25}
            )

    result = _run_standard_cleanup_discovery(
        value, mutate, phase="both" if untimed_pending else "snapshot", path="independent"
    )
    assert result.status == status, result.errors
    if count is None:
        assert any("lacks side-effect count coverage for extra" in error for error in result.errors)
    elif count == 2:
        assert any("side-effect count for extra differs" in error for error in result.errors)
    assert (
        any("completed replay step lacks observed user-code execution" in error for error in result.errors)
        is not witnessed
    )


def test_cleanup_recovery_rechecks_declared_observation_grace() -> None:
    value = _standard_retirement_snapshot("recovery_evidence")
    value["observationEndedAtMs"] = 100
    value["budgets"]["observationGraceMs"] = 55
    snapshot = deepcopy(value)
    next(row for row in snapshot["events"] if row["kind"] == "worker_recovered")["atMs"] = 46
    findings = _cleanup_behavior_errors(value, snapshot, standard=True, final=True)
    assert any("observation window lacks the declared grace after recovery" in error for error in findings)


def test_cleanup_rejects_malformed_interior_retry_outcome() -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    snapshot = deepcopy(value)
    malformed = {"attemptId": "malformed", "requestId": "request-resume", "outcome": False}
    snapshot["replay"]["retry"]["attempts"].insert(1, malformed)
    snapshot["replay"]["operationExecutions"].append({**malformed, "operationId": "retry"})
    findings = _cleanup_behavior_errors(value, snapshot, standard=True, final=True)
    assert any("attempt malformed outcome must be a non-empty string" in error for error in findings)


def test_cleanup_rechecks_marker_dispatch_at_corrected_overlap_boundary() -> None:
    value = _standard_retirement_snapshot("evidence", requirement=1)
    snapshot = deepcopy(value)
    victim = next(row for row in snapshot["attempts"] if row["role"] == "victim")
    victim.update(endedAtMs=26, endedAtMsSource="platform")
    findings = _cleanup_behavior_errors(value, snapshot, standard=True, final=True)
    assert any("overlap peer lacks a marker checkpoint dispatch inside native overlap" in error for error in findings)
    assert not any("barrier lies outside" in error for error in findings)


def test_cleanup_deadline_rechecks_grace_after_corrected_timeout() -> None:
    value = _standard_retirement_snapshot("deadline_evidence")
    value["budgets"]["observationGraceMs"] = 170
    snapshot = deepcopy(value)
    next(row for row in snapshot["events"] if row["kind"] == "platform_timeout")["atMs"] = 30.5
    findings = _cleanup_behavior_errors(value, snapshot, standard=True, final=True)
    assert any("observation window lacks the declared grace after timeout" in error for error in findings)


def test_cleanup_recovery_admission_is_checked_against_corrected_timeout() -> None:
    value = _standard_retirement_snapshot("recovery_evidence")
    next(row for row in value["attempts"] if row["role"] == "recovery")["startedAtMs"] = 30.25
    snapshot = deepcopy(value)
    next(row for row in snapshot["events"] if row["kind"] == "platform_timeout")["atMs"] = 30.5
    findings = _cleanup_behavior_errors(value, snapshot, standard=True, final=True)
    assert any("places recovery before the victim timeout" in error for error in findings)


def test_incomplete_replay_inventory_does_not_block_verified_retirement() -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    operation = {
        "id": "extra",
        "name": "extra",
        "type": "STEP",
        "parentId": None,
        "status": "SUCCEEDED",
        "result": "ok",
    }
    value["replay"]["after"].append(deepcopy(operation))
    history = next(row for row in value["histories"] if row["executionArn"] == "arn:execution:victim")
    history["operations"].append(deepcopy(operation))

    def mutate(snapshot: dict[str, Any]) -> None:
        history = next(row for row in snapshot["histories"] if row["executionArn"] == "arn:execution:victim")
        for rows in (snapshot["replay"]["after"], history["operations"]):
            next(row for row in rows if row["id"] == "extra")["endedAtMs"] = 25

    result = _run_standard_cleanup_discovery(value, mutate, phase="snapshot", path="independent")
    assert result.status == "INCOMPLETE_EVIDENCE", result.errors
    assert any("omits an operation known to have completed before suspension" in error for error in result.errors)


def test_independent_retirement_archives_the_adapters_unmodified_native_source() -> None:
    value = _standard_retirement_snapshot("warm_evidence")
    victim = next(row for row in value["attempts"] if row["role"] == "victim")

    def mutate(snapshot: dict[str, Any]) -> None:
        attempt = next(row for row in snapshot["attempts"] if row["requestId"] == victim["requestId"])
        attempt.pop("endedAtMsSource")

    result = _run_standard_cleanup_discovery(value, mutate, phase="retirement", path="independent")
    assert result.status == "PASSED", result.errors
    archived = next(row for row in result.retirement_evidence["attempts"] if row["requestId"] == victim["requestId"])
    assert "endedAtMsSource" not in archived


def test_cleanup_deadline_rejects_late_in_flight_task_observation() -> None:
    value = _standard_retirement_snapshot("deadline_evidence")

    def mutate(snapshot: dict[str, Any]) -> None:
        next(event for event in snapshot["events"] if event["kind"] == "work_in_flight")["atMs"] = 36

    result = _run_standard_cleanup_discovery(value, mutate, phase="snapshot", path="independent")
    assert result.status == "FAILED", result.errors
    assert any("in-flight work follows the controlled task exit" in error for error in result.errors)
    assert any("timeout contradicts the frozen in-flight work or task exit" in error for error in result.errors)


def test_completed_service_operation_requires_in_window_replay_after_entry() -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    operation = {
        "id": "extra-retirement-step",
        "name": "extra-retirement-step",
        "type": "STEP",
        "parentId": None,
        "status": "SUCCEEDED",
        "result": "ok",
        "endedAtMs": 60,
    }

    def mutate(snapshot: dict[str, Any]) -> None:
        history = next(row for row in snapshot["histories"] if row["executionArn"] == "arn:execution:victim")
        history["operations"].append(deepcopy(operation))

    result = _run_standard_cleanup_discovery(value, mutate, phase="snapshot", path="independent")
    assert result.status == "INCOMPLETE_EVIDENCE", result.errors
    assert any(
        "replay.after omits a completed service operation inside the observation window" in error
        for error in result.errors
    )

    snapshot = deepcopy(value)
    mutate(snapshot)
    history = next(row for row in snapshot["histories"] if row["executionArn"] == "arn:execution:victim")
    history["operations"][-1]["endedAtMs"] = value["observationEndedAtMs"] + 1
    assert not any(
        "replay.after omits a completed service operation" in error
        for error in _cleanup_behavior_errors(value, snapshot, standard=True, final=True)
    )


@pytest.mark.parametrize("provisional_start", [39, 42])
def test_platform_resume_admission_supersedes_earlier_fixture_estimates(provisional_start: int) -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    resume = next(row for row in value["attempts"] if row["role"] == "resume")
    resume["startedAtMsSource"] = "platform"
    reads = 0

    def mutate(snapshot: dict[str, Any]) -> None:
        nonlocal reads
        reads += 1
        row = next(row for row in snapshot["attempts"] if row["role"] == "resume")
        if reads == 1:
            row.update(startedAtMs=provisional_start, startedAtMsSource="fixture")
        else:
            row.update(startedAtMs=resume["startedAtMs"], startedAtMsSource="platform")

    result = _run_standard_cleanup_discovery(value, mutate, phase="both", path="independent")
    assert result.status == "PASSED", result.errors


def test_service_resume_is_journaled_when_discovered() -> None:
    result = _run_standard_cleanup_discovery(
        _standard_retirement_snapshot("replay_evidence"),
        lambda _snapshot: None,
        phase="snapshot",
        path="independent",
        service_resume=True,
    )
    assert result.status == "PASSED", result.errors


@pytest.mark.parametrize("settled_at", [50, 210])
def test_cleanup_checks_history_expectations_only_for_in_window_settlement(settled_at: int) -> None:
    value = _standard_retirement_snapshot("deadline_evidence")
    arn = value["histories"][0]["executionArn"]
    value["histories"][0]["status"] = "RUNNING"
    value["expectedHistories"][arn] = {"allowedStatuses": ["RUNNING", "FAILED", "TIMED_OUT", "STOPPED"]}

    def mutate(snapshot: dict[str, Any]) -> None:
        snapshot["histories"][0]["status"] = "SUCCEEDED"
        resource = next(row for row in snapshot["retirement"]["resources"] if row["id"] == arn)
        resource["settledAtMs"] = settled_at

    result = _run_standard_cleanup_discovery(value, mutate, phase="retirement", path="independent")
    assert result.status == ("FAILED" if settled_at == 50 else "PASSED"), result.errors
    assert any("outside the declared allowedStatuses" in error for error in result.errors) == (settled_at == 50)


def test_cleanup_rejects_stale_replay_after_for_in_window_checkpoint() -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    operation = {"id": "extra", "name": "extra", "type": "STEP", "parentId": None, "status": "PENDING"}
    value["replay"]["after"].append(deepcopy(operation))
    value["histories"][0]["operations"].append(deepcopy(operation))

    def mutate(snapshot: dict[str, Any]) -> None:
        snapshot["histories"][0]["operations"][-1].update(status="SUCCEEDED", result="ok", endedAtMs=60)

    result = _run_standard_cleanup_discovery(value, mutate, phase="snapshot", path="independent")
    assert result.status == "FAILED", result.errors
    assert any("replay.after differs from a completed service operation" in error for error in result.errors)


def test_independent_retirement_preserves_an_observed_terminal_callback_outcome() -> None:
    value = _standard_retirement_snapshot("warm_evidence")
    _add_retained_resource(value, "callback", "SUCCEEDED")
    value["retirement"]["resources"][-1]["settledAtMs"] = 210

    def mutate(snapshot: dict[str, Any]) -> None:
        snapshot["retirement"]["resources"][-1]["outcome"] = "FAILED"

    result = _run_standard_cleanup_discovery(
        value, mutate, phase="retirement", path="independent", observe_retirement=True, quiescent=False
    )
    assert result.status != "PASSED"
    assert any("changed the terminal outcome of observed resource callback:" in error for error in result.errors)


@pytest.mark.parametrize("observed_kind", ["task_start", "observed_task_start"])
def test_cleanup_deadline_rejects_task_start_after_in_flight(observed_kind: str) -> None:
    value = _standard_retirement_snapshot("deadline_evidence")

    def mutate(snapshot: dict[str, Any]) -> None:
        work = next(row for row in snapshot["events"] if row["kind"] == "work_in_flight")
        task = next(row for row in snapshot["events"] if row["kind"] == "task_start")
        task.update(atMs=work["atMs"] + 0.1, kind=observed_kind)
        if observed_kind != "task_start":
            task["sourceKind"] = "task_start"

    result = _run_standard_cleanup_discovery(value, mutate, phase="snapshot", path="independent", quiescent=False)
    assert any("started after it was observed in flight" in error for error in result.errors)


def test_cleanup_deadline_rejects_over_budget_admission_close() -> None:
    value = _standard_retirement_snapshot("deadline_evidence")

    def mutate(snapshot: dict[str, Any]) -> None:
        timeout = next(row for row in snapshot["events"] if row["kind"] == "platform_timeout")
        closed = next(row for row in snapshot["events"] if row["kind"] == "admission_closed")
        closed["atMs"] = timeout["atMs"] + value["budgets"]["admissionMs"] + 1

    result = _run_standard_cleanup_discovery(value, mutate, phase="snapshot", path="independent")
    assert result.status == "FAILED", result.errors
    assert any("contradicts the frozen admissionMs measurement" in error for error in result.errors)


@pytest.mark.parametrize("partial", [True, False])
def test_collection_reports_reversed_event_order(partial: bool) -> None:
    class SequenceAdapter(FailureAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            if context.scenario.case_id == "failed":
                _same_time_pair(self.snapshots["failed"], "io_start", "io_exit")
                context.evidence = super().observations(context)

        def observations(self, context: ScenarioContext) -> dict[str, Any]:
            observed = super().observations(context)
            if context.scenario.case_id == "failed":
                start = next(row for row in observed["events"] if row["kind"] == "io_start")
                end = next(row for row in observed["events"] if row["kind"] == "io_exit")
                start["sequence"], end["sequence"] = end["sequence"], start["sequence"]
            return observed

    (result, _), _ = _run(SequenceAdapter(phase="execute" if partial else ""))
    message = "Partial collection omitted" if partial else "Collection omitted"
    assert any(message in error for error in result.errors)


def test_event_order_comparison_ignores_inherited_sequences() -> None:
    first = {"requestId": "victim", "atMs": 10, "kind": "task_start", "taskId": "work"}
    second = {**first, "kind": "task_exit"}
    prior = {"events": [{**first, "sequence": 0}, {**second, "sequence": 1}]}
    partial = {"events": [{**first, "sequence": 100}, second]}
    final = {"events": [{**first, "sequence": 100}, {**second, "sequence": 101}]}
    assert not _event_frames_order_changed(prior, partial)
    assert not _event_frames_order_changed(partial, final)


def test_cleanup_defers_same_time_poll_until_semantic_order_is_known() -> None:
    value = _standard_retirement_snapshot("warm_evidence")
    returned = next(event for event in value["events"] if event["kind"] == "wrapper_return")
    provisional = deepcopy(value)
    provisional["sdkRequests"].append(
        {
            **{key: returned[key] for key in ("requestId", "executionArn", "atMs")},
            "method": "GetDurableExecutionState",
            "outcome": "DISPATCHED",
            "callId": "same-time-poll",
        }
    )
    final = deepcopy(provisional)
    closed = next(event for event in final["events"] if event["kind"] == "wrapper_return")
    poll = final["sdkRequests"][-1]
    poll["sequence"] = 0
    closed["sequence"] = 1
    diagnostic = "dispatched SDK work after admission closed"
    assert not any(diagnostic in error for error in _cleanup_behavior_errors(value, provisional, standard=True))
    assert not any(
        diagnostic in error
        for error in _cleanup_behavior_errors(value, final, standard=True, known_snapshots=[provisional], final=True)
    )
    poll["sequence"] = 2
    assert any(
        diagnostic in error
        for error in _cleanup_behavior_errors(value, final, standard=True, known_snapshots=[provisional], final=True)
    )


def test_cleanup_recovery_rejects_two_recoveries_in_one_snapshot() -> None:
    original = _standard_retirement_snapshot("recovery_evidence")
    snapshot = deepcopy(original)
    recovery = next(row for row in snapshot["events"] if row["kind"] == "worker_recovered")
    snapshot["events"].append({**recovery, "atMs": recovery["atMs"] + 0.5})
    assert any(
        "needs exactly one worker_recovered" in failure
        for failure in _cleanup_behavior_errors(original, snapshot, standard=True, final=True)
    )


def test_offline_disposal_uses_verified_retirement_attempt_count() -> None:
    from aws_durable_execution_conformance_tests.lmi.evidence import validate_retirement_evidence
    from aws_durable_execution_conformance_tests.lmi.offline import evaluate_evidence

    original = _standard_retirement_snapshot("failure_evidence")
    first = original["attempts"][0]
    arn = first["executionArn"]
    original["histories"][0]["status"] = "RUNNING"
    original["expectedHistories"][arn] = {"allowedStatuses": ["RUNNING", "FAILED"]}
    snapshot = deepcopy(original)
    snapshot["histories"][0]["status"] = "SUCCEEDED"
    snapshot["observationEndedAtMs"] = 206
    snapshot["collection"]["watermarks"] = {key: 206 for key in snapshot["collection"]["watermarks"]}
    retry_id = "service-retry"
    snapshot["attempts"].append(
        {**first, "requestId": retry_id, "attempt": 2, "startedAtMs": 201, "endedAtMs": 205, "outcome": "SUCCEEDED"}
    )
    for event in ("wrapper_enter", "task_start", "task_exit", "wrapper_return"):
        template = next(row for row in original["events"] if row["kind"] == event)
        snapshot["events"].append(
            {
                **template,
                "requestId": retry_id,
                "ownerRequestId": retry_id,
                "atMs": 201 if event in ("wrapper_enter", "task_start") else 205,
            }
        )
    invocation = {"kind": "invocation", "id": retry_id, "ownerCaseId": original["caseId"]}
    snapshot["resourcesCreated"].append(invocation)
    snapshot["retirement"]["resources"].append(
        {**invocation, "settledAtMs": 208, "outcome": "QUIESCENT", "evidenceUri": "memory://service"}
    )
    next(row for row in snapshot["retirement"]["resources"] if row["id"] == arn).update(outcome="SUCCEEDED")
    _resequence(snapshot)
    assert validate_retirement_evidence(snapshot) == []
    errors = evaluate_evidence(
        original,
        retirement_evidence=snapshot,
        cleanup_evidence={},
        validator=validate_evidence,
        standard=True,
    )
    assert errors == []


@pytest.mark.parametrize("cleanup_history", ["status-only", "contradicting-result"])
def test_partial_cleanup_history_only_fails_on_supplied_contradictions(cleanup_history: str) -> None:
    from aws_durable_execution_conformance_tests.lmi.offline import evaluate_evidence

    original = _standard_retirement_snapshot("evidence")
    # Executions that settle inside the observation window are rechecked
    # against their fixture expectations in every cleanup snapshot.
    for row in original["retirement"]["resources"]:
        if row["kind"] == "execution":
            row["settledAtMs"] = original["observationEndedAtMs"] - 10
    assert validate_evidence(original) == []
    frame = deepcopy(original)
    frame["histories"] = [
        {"executionArn": history["executionArn"], "status": history["status"]} for history in frame["histories"]
    ]
    if cleanup_history == "contradicting-result":
        frame["histories"][0]["result"] = {"marker": "unexpected"}
    errors = evaluate_evidence(
        original,
        retirement_evidence={},
        cleanup_evidence={"retire": [frame]},
        validator=validate_evidence,
        standard=True,
    )
    if cleanup_history == "status-only":
        assert errors == []
    else:
        arn = frame["histories"][0]["executionArn"]
        assert f"ASSERTION: service history {arn}.result differs from the fixture expectation" in errors


@pytest.mark.parametrize("frozen_status", ["SUCCEEDED", "RUNNING"])
@pytest.mark.parametrize("cleanup_result", ["omitted", "expected", "unexpected"])
def test_in_window_terminal_history_must_supply_declared_fields(frozen_status: str, cleanup_result: str) -> None:
    arn = "arn:execution:victim"
    declared = {"marker": "victim"}
    original = {
        "expectedHistories": {arn: {"allowedStatuses": ["RUNNING", "SUCCEEDED"], "result": declared}},
        "observationEndedAtMs": 200,
        # A nonterminal frozen view can match the declared result without
        # proving the result of the terminal execution.
        "histories": [{"executionArn": arn, "status": frozen_status, "result": declared}],
    }
    row: dict[str, Any] = {"executionArn": arn, "status": "SUCCEEDED"}
    if cleanup_result != "omitted":
        row["result"] = declared if cleanup_result == "expected" else {"marker": "unexpected"}
    snapshot = {
        "histories": [row],
        "retirement": {"resources": [{"kind": "execution", "id": arn, "outcome": "SUCCEEDED", "settledAtMs": 190}]},
    }
    findings = _cleanup_behavior_errors(original, snapshot, standard=True, final=True)
    contradiction = f"ASSERTION: service history {arn}.result differs from the fixture expectation"
    missing = f"INCOMPLETE: terminal service history {arn} lacks declared result"
    assert (contradiction in findings) is (cleanup_result == "unexpected")
    assert (missing in findings) is (cleanup_result == "omitted" and frozen_status == "RUNNING")
