# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Failed observations stay failed; only independently proven cleanup permits reuse."""

import asyncio
from collections.abc import Mapping
from copy import deepcopy
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
from aws_durable_execution_conformance_tests.lmi.evidence import validate_retirement_evidence

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
    if context.scenario.otel_view:
        for kind, started, completed in (("export", 300, 350), ("flush", 310, 360)):
            identity = f"{kind}:{name}:victim"
            details = {f"{kind}Id": identity, "providerId": "provider", "exporterId": "exporter"}
            event(f"{kind}_started", started, **details)
            event(f"{kind}_completed", completed, outcome="succeeded", **details)
            resources.append({"id": identity, "kind": kind, "ownerCaseId": name})
        event("export_quiescence", wrapper_end + 1, pendingExportIds=[], pendingFlushIds=[], publicationCount=0)
    value = {
        "schemaVersion": 1,
        "runId": context.run_id,
        "caseId": name,
        "cohortId": context.scenario.cohort_id,
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
    if context.scenario.otel_view:
        value["otelView"] = context.scenario.otel_view
    _resequence(value)
    return value


def _case(name: str, *, retry: bool = False, telemetry: bool = False) -> Scenario:
    return Scenario(
        name,
        "fixture-check",
        PROFILE.profile_id,
        "timed-out-cohort" if retry else "completed-cohort",
        roles=("victim", "retry") if retry else ("victim",),
        exclusive=True,
        coverage_profile="fixture-suite",
        otel_view="invocation" if telemetry else None,
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
        if self.damage in {"missing-io-exit", "missing-root-exit", "pending-export", "pending-flush"}:
            kind = {
                "missing-io-exit": "io_exit",
                "missing-root-exit": "task_exit",
                "pending-export": "export_completed",
                "pending-flush": "flush_completed",
            }[self.damage]
            final["events"] = [event for event in final["events"] if event["kind"] != kind]
        elif self.damage == "dropped-known-start":
            final["events"] = [event for event in final["events"] if event["kind"] not in {"io_start", "io_exit"}]
        elif self.damage == "dropped-known-fault":
            final["faultsCreated"] = []
            final["retirement"]["faults"] = []
        elif self.damage == "dropped-known-export":
            final["resourcesCreated"] = [item for item in final["resourcesCreated"] if item["kind"] != "export"]
            final["retirement"]["resources"] = [
                item for item in final["retirement"]["resources"] if item["kind"] != "export"
            ]
            final["events"] = [
                event for event in final["events"] if event["kind"] not in {"export_started", "export_completed"}
            ]
        elif self.damage == "active-execution":
            final["histories"][0]["status"] = "RUNNING"
        elif self.damage == "unpublished-observations":
            next(event for event in final["events"] if event["kind"] == "export_quiescence")["publicationCount"] = 1
        elif self.damage == "reversed-equal-time-pair":
            start = next(event for event in final["events"] if event["kind"] == "io_start")
            end = next(event for event in final["events"] if event["kind"] == "io_exit")
            end["atMs"] = start["atMs"]
            _resequence(final)
            start["sequence"], end["sequence"] = end["sequence"], start["sequence"]


def _run(
    adapter: FailureAdapter, *, retry: bool = False, telemetry: bool = False
) -> tuple[list[ScenarioResult], ResourceScheduler]:
    scheduler = ResourceScheduler()
    cases = [_case("failed", retry=retry, telemetry=telemetry), _case("next", telemetry=telemetry)]
    results = asyncio.run(
        run_scenarios(cases, [PROFILE], adapter, run_id="run", validator=_validator, scheduler=scheduler)
    )
    return results, scheduler


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


@pytest.mark.parametrize(
    "damage", ["pending-export", "pending-flush", "unpublished-observations", "dropped-known-export"]
)
def test_failed_case_retains_telemetry_ownership_until_work_and_publication_complete(damage: str) -> None:
    adapter = FailureAdapter(phase="assertion", damage=damage)
    results, scheduler = _run(adapter, telemetry=True)
    assert ORIGINAL_FAILURE in results[0].errors
    assert results[1].status == "INFRASTRUCTURE_ERROR"
    assert adapter.started == ["failed"]
    assert adapter.finished == [False]
    assert scheduler.quarantine_reason(PROFILE.resource_id)


def test_settled_telemetry_does_not_turn_the_original_failure_into_success() -> None:
    adapter = FailureAdapter(phase="assertion")
    results, scheduler = _run(adapter, telemetry=True)
    assert results[0].status == "FAILED"
    assert ORIGINAL_FAILURE in results[0].errors
    assert results[1].status == "PASSED", results[1].errors
    assert adapter.finished == [True]
    assert scheduler.quarantine_reason(PROFILE.resource_id) is None


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
