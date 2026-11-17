# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Failed observations stay failed; only independently proven cleanup permits reuse."""

import asyncio
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import replace
from typing import Any

import pytest

from aws_durable_execution_conformance_tests.lmi.coordinator import (
    Budgets,
    Profile,
    ResourceScheduler,
    Scenario,
    ScenarioContext,
    ScenarioResult,
    run_scenarios,
)
from aws_durable_execution_conformance_tests.lmi.evidence import (
    EvidenceValidator,
    same_json,
    validate_common_evidence,
    validate_retirement_evidence,
)

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


def _add_retained_resource(value: dict[str, Any], kind: str, outcome: str) -> None:
    resource = {"id": f"{kind}:{value['caseId']}", "kind": kind, "ownerCaseId": value["caseId"]}
    value["resourcesCreated"].append(resource)
    value["retirement"]["resources"].append(
        {**resource, "settledAtMs": 20900, "outcome": outcome, "evidenceUri": "memory://provider-completion"}
    )


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
    assert observed.retirement_evidence == {}
    assert observed.cleanup_evidence[cleanup_phase]
    assert adapter.finished == [True]
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


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
    valid_cleanup = complete_retirement and outcome == "return" and (path == "independent" or not original_failed)
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


def test_passing_case_cannot_add_a_new_native_request_only_during_retirement() -> None:
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
    assert results[0].status == "INFRASTRUCTURE_ERROR"
    assert any("journaled launches" in error for error in results[0].errors)
    assert len(results[0].attempts) == 2
    assert len(results[0].evidence["attempts"]) == 1
    assert len(results[0].retirement_evidence["attempts"]) == 2
    assert results[1].status == "INFRASTRUCTURE_ERROR"
    assert adapter.started == ["failed"]
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


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
