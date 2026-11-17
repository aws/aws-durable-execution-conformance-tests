# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Replay observations preserve retry order and cumulative side-effect counts."""

import asyncio
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from runpy import run_path
from typing import Any

import pytest

from aws_durable_execution_conformance_tests.lmi.coordinator import (
    Profile,
    ResourceScheduler,
    Scenario,
    ScenarioContext,
    ScenarioResult,
    run_scenarios,
)
from aws_durable_execution_conformance_tests.lmi.evidence import validate_evidence


def _replay_evidence(count: int = 2) -> dict[str, Any]:
    fixture = run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))
    value = fixture["replay_evidence"]()
    attempts = [
        {
            "attemptId": f"retry-{index + 1}",
            "requestId": "request-resume" if index == count - 1 else "request-victim",
            "outcome": "SUCCEEDED" if index == count - 1 else "FAILED",
        }
        for index in range(count)
    ]
    value["replay"]["retry"]["attempts"] = attempts
    executions = value["replay"]["operationExecutions"]
    executions[:] = [item for item in executions if item["operationId"] != "retry"]
    executions.extend({**item, "operationId": "retry"} for item in attempts)
    count_record = next(item for item in value["replay"]["sideEffects"] if item["operationId"] == "retry")
    count_record.update(count=count, expectedCount=count)
    assert validate_evidence(value) == []
    return value


class RetryObservationAdapter:
    def __init__(
        self,
        collected: dict[str, Any],
        snapshots: list[list[dict[str, Any]]],
        *,
        reverse_in_collection: bool = False,
        partial: bool = False,
    ) -> None:
        self.collected = collected
        self.snapshots = snapshots
        self.reverse_in_collection = reverse_in_collection
        self.partial = partial

    async def prepare(self, profile: Profile) -> None:
        pass

    async def finish(self, profile: Profile, *, quiescent: bool) -> None:
        pass

    async def start(self, context: ScenarioContext) -> None:
        for attempt in self.collected["attempts"]:
            context.record_attempt(
                attempt["requestId"],
                role=attempt["role"],
                requestId=attempt["requestId"],
                executionArn=attempt["executionArn"],
            )
        context.evidence = {"replay": {"retry": {"operationId": "retry", "attempts": deepcopy(self.snapshots[0])}}}

    async def place(self, context: ScenarioContext) -> None:
        if len(self.snapshots) > 1:
            context.evidence["replay"]["retry"]["attempts"] = deepcopy(self.snapshots[1])
        if self.partial:
            raise RuntimeError("collect partial retry observations")

    async def execute(self, context: ScenarioContext) -> None:
        if len(self.snapshots) > 2:
            context.evidence["replay"]["retry"]["attempts"] = deepcopy(self.snapshots[2])

    async def collect(self, context: ScenarioContext) -> Mapping[str, Any]:
        if self.reverse_in_collection:
            context.evidence["replay"]["retry"]["attempts"].reverse()
        observed = deepcopy(self.collected)
        observed.pop("retirement")
        return observed

    async def collect_partial(self, context: ScenarioContext) -> Mapping[str, Any]:
        return await self.collect(context)

    async def release(self, context: ScenarioContext) -> None:
        pass

    async def retire(self, context: ScenarioContext) -> bool:
        # No independent snapshot: the original failure must survive before
        # normal retirement could allow a passing case to release its lease.
        context.evidence["retirement"] = deepcopy(self.collected["retirement"])
        return True


def _run(adapter: RetryObservationAdapter) -> tuple[ScenarioResult, ResourceScheduler]:
    value = adapter.collected
    profile = Profile(
        "native-2",
        value["profile"]["functionArn"],
        2,
        2,
        value["artifact"]["sdk"],
        value["profile"]["configurationDigest"],
    )
    case = Scenario(
        value["caseId"],
        value["requirementId"],
        profile.profile_id,
        "suspension-retry",
        roles=tuple(value["selectedRequests"]),
        slots=2,
        exclusive=True,
    )
    scheduler = ResourceScheduler()
    (result,) = asyncio.run(
        run_scenarios(
            [case],
            [profile],
            adapter,
            run_id=value["runId"],
            scheduler=scheduler,
            validator=lambda evidence, *, phase="final": validate_evidence(
                evidence, phase="observations" if phase == "observations" else "final"
            ),
        )
    )
    return result, scheduler


@pytest.mark.parametrize("source", ["initial", "in-place", "collector", "partial"])
def test_retry_order_conflict_survives_corrected_collection(source: str) -> None:
    value = _replay_evidence()
    attempts = value["replay"]["retry"]["attempts"]
    reversed_attempts = list(reversed(attempts))
    invalid = deepcopy(value)
    invalid["replay"]["retry"]["attempts"] = reversed_attempts
    assert any("failed attempt followed by success" in error for error in validate_evidence(invalid))
    snapshots = [reversed_attempts] if source == "initial" else [attempts]
    if source == "in-place":
        snapshots.extend([reversed_attempts, attempts])
    result, scheduler = _run(
        RetryObservationAdapter(
            value,
            snapshots,
            reverse_in_collection=source in {"collector", "partial"},
            partial=source == "partial",
        )
    )
    assert result.status != "PASSED", result.errors
    assert any("retry attempt order" in error for error in result.errors), result.errors
    assert scheduler.quarantine_reason(value["profile"]["functionArn"])


@pytest.mark.parametrize("known", [(1, 2, 3), (0, 3), (0, 1)])
def test_retry_observations_can_add_records_before_between_or_after_known_attempts(known: tuple[int, ...]) -> None:
    value = _replay_evidence(4)
    attempts = value["replay"]["retry"]["attempts"]
    result, _ = _run(RetryObservationAdapter(value, [[attempts[index] for index in known]]))
    assert result.status == "PASSED", result.errors
    assert result.evidence["replay"]["retry"]["attempts"] == attempts


@pytest.mark.parametrize("first,second", [(0, 1), (1, 0)])
def test_disjoint_retry_fragments_do_not_imply_an_order(first: int, second: int) -> None:
    value = _replay_evidence()
    attempts = value["replay"]["retry"]["attempts"]
    result, _ = _run(RetryObservationAdapter(value, [[attempts[first]], [attempts[second]]]))
    assert result.status == "PASSED", result.errors


def test_overlapping_retry_fragments_can_omit_provisional_records() -> None:
    value = _replay_evidence(4)
    attempts = value["replay"]["retry"]["attempts"]
    result, _ = _run(RetryObservationAdapter(value, [[attempts[index] for index in (0, 2, 3)], attempts[:3]]))
    assert result.status == "PASSED", result.errors


def test_retry_attempt_metadata_enrichment_preserves_one_record_per_identity() -> None:
    value = _replay_evidence()
    known = deepcopy(value["replay"]["retry"]["attempts"])
    for attempt in value["replay"]["retry"]["attempts"]:
        attempt["evidenceUri"] = f"memory://{attempt['attemptId']}"
    result, _ = _run(RetryObservationAdapter(value, [known]))
    assert result.status == "PASSED", result.errors
    assert result.evidence["replay"]["retry"]["attempts"] == value["replay"]["retry"]["attempts"]


def test_retry_attempt_multiplicity_is_not_erased_by_collection() -> None:
    value = _replay_evidence()
    attempts = value["replay"]["retry"]["attempts"]
    result, _ = _run(RetryObservationAdapter(value, [[attempts[0], attempts[0], attempts[1]]]))
    assert result.status != "PASSED"
    assert any("distinct observed attempts" in error for error in result.errors), result.errors
    assert sum(attempt["attemptId"] == "retry-1" for attempt in result.evidence["replay"]["retry"]["attempts"]) == 2


@pytest.mark.parametrize("field,replacement", [("requestId", "request-resume"), ("outcome", "SUCCEEDED")])
def test_retry_attempt_enrichment_cannot_replace_request_or_outcome(field: str, replacement: str) -> None:
    value = _replay_evidence(3)
    known = deepcopy(value["replay"]["retry"]["attempts"])
    value["replay"]["retry"]["attempts"][1][field] = replacement
    for execution in value["replay"]["operationExecutions"]:
        if execution.get("attemptId") == "retry-2":
            execution[field] = replacement
    assert validate_evidence(value) == []
    result, _ = _run(RetryObservationAdapter(value, [known]))
    assert result.status != "PASSED"
    retained = next(item for item in result.evidence["replay"]["retry"]["attempts"] if item["attemptId"] == "retry-2")
    assert retained[field] == known[1][field]


class SideEffectObservationAdapter(RetryObservationAdapter):
    def __init__(self, collected: dict[str, Any], initial: list[dict[str, Any]], *, partial: bool = False) -> None:
        super().__init__(collected, [deepcopy(collected["replay"]["retry"]["attempts"])], partial=partial)
        self.initial_effects = initial

    async def start(self, context: ScenarioContext) -> None:
        await super().start(context)
        context.evidence["replay"]["sideEffects"] = deepcopy(self.initial_effects)


def _effect(value: dict[str, Any]) -> dict[str, Any]:
    return next(item for item in value["replay"]["sideEffects"] if item["operationId"] == "done")


@pytest.mark.parametrize("partial", [False, True])
def test_side_effect_observations_can_reach_the_predeclared_count(partial: bool) -> None:
    value = _replay_evidence()
    _effect(value).update(count=2, expectedCount=2)
    known = {"operationId": "done", "count": 1, "expectedCount": 2}
    assert validate_evidence(value) == []
    result, _ = _run(SideEffectObservationAdapter(value, [known], partial=partial))
    if partial:
        assert result.status != "PASSED"
    else:
        assert result.status == "PASSED", result.errors
    assert validate_evidence(result.evidence) == [], result.errors
    assert not any("side-effect" in error or "omitted or changed retained" in error for error in result.errors)
    assert _effect(result.evidence) == {"operationId": "done", "count": 2, "expectedCount": 2}


def test_side_effect_source_metadata_can_enrich_the_same_counter() -> None:
    value = _replay_evidence()
    known = deepcopy(_effect(value))
    _effect(value)["evidenceUri"] = "memory://side-effect-count"
    result, _ = _run(SideEffectObservationAdapter(value, [known]))
    assert result.status == "PASSED", result.errors
    assert _effect(result.evidence) == _effect(value)
    assert sum(item["operationId"] == "done" for item in result.evidence["replay"]["sideEffects"]) == 1


@pytest.mark.parametrize("change", ["decrease", "expectation"])
def test_side_effect_progress_cannot_decrease_or_change_the_declared_expectation(change: str) -> None:
    value = _replay_evidence()
    known = {"operationId": "done", "count": 2 if change == "decrease" else 1, "expectedCount": 1}
    if change == "expectation":
        _effect(value).update(count=2, expectedCount=2)
    assert validate_evidence(value) == []
    result, _ = _run(SideEffectObservationAdapter(value, [known]))
    assert result.status != "PASSED", result.errors
    assert _effect(result.evidence)["expectedCount"] == 1
    assert _effect(result.evidence)["count"] == 2
    assert any("side-effect count differs" in error for error in result.errors), result.errors


@pytest.mark.parametrize("source", ["context", "collector"])
@pytest.mark.parametrize("conflicting", [False, True])
def test_side_effect_snapshots_cannot_repeat_an_operation_identity(source: str, conflicting: bool) -> None:
    value = _replay_evidence()
    known = deepcopy(_effect(value))
    duplicate = {**known, "count": 2, "expectedCount": 2} if conflicting else deepcopy(known)
    initial = [known, duplicate] if source == "context" else [known]
    if source == "collector":
        value["replay"]["sideEffects"].append(duplicate)
    assert validate_evidence(value) == []
    result, _ = _run(SideEffectObservationAdapter(value, initial))
    assert result.status != "PASSED", result.errors
    assert any("duplicate side-effect operationId" in error for error in result.errors), result.errors
    assert sum(item["operationId"] == "done" for item in result.evidence["replay"]["sideEffects"]) == 2


@pytest.mark.parametrize("field", ["count", "expectedCount"])
@pytest.mark.parametrize("invalid", [1.0, True, -1, float("inf"), "1"])
def test_invalid_counter_observations_cannot_be_erased_by_valid_collection(field: str, invalid: Any) -> None:
    value = _replay_evidence()
    known = deepcopy(_effect(value))
    known[field] = invalid
    result, _ = _run(SideEffectObservationAdapter(value, [known]))
    assert result.status != "PASSED", result.errors
    assert any(f"side-effect {field} must be a non-negative integer" in error for error in result.errors), result.errors


def _inventory(value: dict[str, Any], name: str) -> list[dict[str, Any]]:
    if name == "operations":
        return next(history["operations"] for history in value["histories"] if history["operations"])
    if name == "retry":
        return value["replay"]["retry"]["attempts"]
    if name == "sideEffects":
        return value["replay"]["sideEffects"]
    return value[name]


def _replay_with_fault() -> dict[str, Any]:
    value = _replay_evidence()
    fault = {"id": "fault-1", "ownerCaseId": value["caseId"]}
    value["faultsCreated"] = [fault]
    value["retirement"]["faults"] = [{**fault, "releasedAtMs": 205, "mode": "normal"}]
    assert validate_evidence(value) == []
    return value


@pytest.mark.parametrize("phase", ["place", "execute"])
@pytest.mark.parametrize(
    "inventory", ["attempts", "histories", "operations", "faultsCreated", "resourcesCreated", "retry", "sideEffects"]
)
def test_in_place_identity_occurrence_increases_survive_later_deduplication(phase: str, inventory: str) -> None:
    value = _replay_with_fault()
    invalid = deepcopy(value)
    rows = _inventory(invalid, inventory)
    rows.insert(1, deepcopy(rows[0]))
    if inventory != "sideEffects":
        assert validate_evidence(invalid)

    class DuplicateDuringPhaseAdapter(RetryObservationAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            context.evidence = deepcopy(self.collected)
            context.evidence.pop("retirement")

        def duplicate(self, context: ScenarioContext) -> None:
            rows = _inventory(context.evidence, inventory)
            rows.insert(1, deepcopy(rows[0]))

        async def place(self, context: ScenarioContext) -> None:
            if phase == "place":
                self.duplicate(context)

        async def execute(self, context: ScenarioContext) -> None:
            if phase == "execute":
                self.duplicate(context)

    result, scheduler = _run(DuplicateDuringPhaseAdapter(value, [value["replay"]["retry"]["attempts"]]))
    assert result.status != "PASSED", result.errors
    assert len(_inventory(result.evidence, inventory)) == len(_inventory(value, inventory)) + 1
    if inventory == "retry":
        assert any("duplicate retry attemptId" in error for error in result.errors), result.errors
        assert any("distinct observed attempts" in error for error in validate_evidence(result.evidence))
    assert scheduler.quarantine_reason(value["profile"]["functionArn"])


@pytest.mark.parametrize(
    "inventory", ["attempts", "histories", "operations", "faultsCreated", "resourcesCreated", "retry", "sideEffects"]
)
def test_metadata_updates_do_not_increase_identity_occurrences(inventory: str) -> None:
    value = _replay_with_fault()
    _inventory(value, inventory)[0]["evidenceUri"] = "memory://additional-observation"
    if inventory == "operations":
        operation_id = _inventory(value, inventory)[0]["id"]
        for operation in [*value["replay"]["before"], *value["replay"]["after"]]:
            if operation["id"] == operation_id:
                operation["evidenceUri"] = "memory://additional-observation"
    assert validate_evidence(value) == []

    class MetadataDuringPhaseAdapter(RetryObservationAdapter):
        async def start(self, context: ScenarioContext) -> None:
            await super().start(context)
            context.evidence = deepcopy(self.collected)
            context.evidence.pop("retirement")
            _inventory(context.evidence, inventory)[0].pop("evidenceUri")

        async def place(self, context: ScenarioContext) -> None:
            _inventory(context.evidence, inventory)[0]["evidenceUri"] = "memory://additional-observation"

    result, _ = _run(MetadataDuringPhaseAdapter(value, [value["replay"]["retry"]["attempts"]]))
    assert result.status == "PASSED", result.errors
    assert len(_inventory(result.evidence, inventory)) == len(_inventory(value, inventory))


def test_new_unique_retry_observation_is_retained_without_replaying_other_fields() -> None:
    value = _replay_evidence(3)
    attempts = value["replay"]["retry"]["attempts"]
    result, _ = _run(RetryObservationAdapter(value, [[attempts[0], attempts[2]], attempts]))
    assert result.status == "PASSED", result.errors
    assert result.evidence["replay"]["retry"]["attempts"] == attempts
