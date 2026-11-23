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
)
from aws_durable_execution_conformance_tests.lmi.evidence import validate_evidence

run_and_adjudicate = run_path(str(Path(__file__).with_name("test_lmi_coordinator.py")))["run_and_adjudicate"]


def _replay_evidence(count: int = 2) -> dict[str, Any]:
    fixture = run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))
    value = fixture["replay_evidence"]()
    fixture["complete_sdk_dispatches"](value)
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
        run_and_adjudicate(
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
    # Retry order is an adjudication verdict. Only the interrupted capture lacks
    # the physical retirement required to reuse the target.
    assert bool(scheduler.quarantine_reason(value["profile"]["functionArn"])) is (source == "partial")


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
    assert not any("side-effect" in error for error in result.errors)
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
    expected = {
        "decrease": "ASSERTION: observed side-effect count differs from earlier observations for done",
        "expectation": "ASSERTION: declared side-effect expectation for done changed across observations",
    }[change]
    assert expected in result.errors, result.errors


@pytest.mark.parametrize("source", ["context", "collector"])
@pytest.mark.parametrize("conflicting", [False, True])
def test_side_effect_snapshots_cannot_repeat_an_operation_identity(source: str, conflicting: bool) -> None:
    value = _replay_evidence()
    known = deepcopy(_effect(value))
    duplicate = {**known, "count": 2, "expectedCount": 2} if conflicting else deepcopy(known)
    initial = [known, duplicate] if source == "context" else [known]
    if source == "collector":
        value["replay"]["sideEffects"].append(duplicate)
        assert "INCOMPLETE: side-effect counts must be unique per operation" in validate_evidence(value)
    else:
        assert validate_evidence(value) == []
    result, _ = _run(SideEffectObservationAdapter(value, initial))
    assert result.status != "PASSED", result.errors
    assert any("duplicate side-effect operationId" in error for error in result.errors), result.errors


@pytest.mark.parametrize("field", ["count", "expectedCount"])
@pytest.mark.parametrize("invalid", [1.5, True, -1, float("inf"), "1"])
def test_invalid_counter_observations_cannot_be_erased_by_valid_collection(field: str, invalid: Any) -> None:
    value = _replay_evidence()
    known = deepcopy(_effect(value))
    known[field] = invalid
    result, _ = _run(SideEffectObservationAdapter(value, [known]))
    assert result.status != "PASSED", result.errors
    assert any(f"side-effect {field} must be a non-negative integer" in error for error in result.errors), result.errors


def _inventory(value: dict[str, Any], name: str) -> list[dict[str, Any]]:
    if name in {"before", "after"}:
        return value["replay"][name]
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


def _checkpoint_views(value: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        *value["replay"]["before"],
        *value["replay"]["after"],
        *(operation for history in value["histories"] for operation in history["operations"]),
    ]


class CheckpointObservationAdapter(RetryObservationAdapter):
    def __init__(
        self, value: dict[str, Any], snapshot: str, initial: list[dict[str, Any]], mode: str = "normal"
    ) -> None:
        super().__init__(value, [value["replay"]["retry"]["attempts"]], partial=mode == "partial")
        self.snapshot = snapshot
        self.initial = initial
        self.mode = mode

    async def start(self, context: ScenarioContext) -> None:
        await super().start(context)
        context.evidence["replay"][self.snapshot] = deepcopy(self.initial)

    async def place(self, context: ScenarioContext) -> None:
        if self.mode == "phase":
            context.evidence["replay"][self.snapshot] = deepcopy(self.collected["replay"][self.snapshot])
        await super().place(context)


@pytest.mark.parametrize("snapshot", ["before", "after"])
@pytest.mark.parametrize("mode", ["normal", "partial", "phase"])
@pytest.mark.parametrize("timestamp", ["startedAtMs", "endedAtMs"])
@pytest.mark.parametrize("operation_id", ["done", "failed"])
def test_checkpoint_snapshots_can_fill_missing_operation_metadata(
    snapshot: str, mode: str, timestamp: str, operation_id: str
) -> None:
    value = _replay_evidence()
    for operation in _checkpoint_views(value):
        if operation["id"] == operation_id:
            operation[timestamp] = 10 if timestamp == "startedAtMs" else 24
    assert validate_evidence(value) == []
    initial = list(reversed(deepcopy(value["replay"][snapshot])))
    next(operation for operation in initial if operation["id"] == operation_id).pop(timestamp)
    result, _ = _run(CheckpointObservationAdapter(value, snapshot, initial, mode))
    if mode == "partial":
        assert result.status != "PASSED"
    else:
        assert result.status == "PASSED", result.errors
    assert validate_evidence(result.evidence) == [], result.errors
    assert result.evidence["replay"][snapshot] == value["replay"][snapshot]


@pytest.mark.parametrize("mode", ["normal", "partial", "phase"])
@pytest.mark.parametrize("status", ["STARTED", "PENDING", "READY"])
def test_after_snapshot_can_complete_an_unfinished_operation(mode: str, status: str) -> None:
    value = _replay_evidence()
    initial = deepcopy(value["replay"]["after"])
    retry = next(operation for operation in initial if operation["id"] == "retry")
    retry["status"] = status
    if status == "PENDING":
        retry.pop("result")
        retry["error"] = {"type": "TransientError"}
    else:
        retry["result"] = {"poll": 1}
    result, _ = _run(CheckpointObservationAdapter(value, "after", initial, mode))
    if mode != "partial":
        assert result.status == "PASSED", result.errors
    assert validate_evidence(result.evidence) == [], result.errors
    assert result.evidence["replay"]["after"] == value["replay"]["after"]


class OperationEntryAdapter(RetryObservationAdapter):
    def __init__(self, value: dict[str, Any], initial: list[dict[str, Any]], mode: str = "normal") -> None:
        super().__init__(value, [value["replay"]["retry"]["attempts"]], partial=mode == "partial")
        self.initial_entries, self.mode = initial, mode

    async def start(self, context: ScenarioContext) -> None:
        await super().start(context)
        context.evidence["replay"]["operationExecutions"] = deepcopy(self.initial_entries)

    async def place(self, context: ScenarioContext) -> None:
        if self.mode == "phase":
            context.evidence["replay"]["operationExecutions"] = deepcopy(
                self.collected["replay"]["operationExecutions"]
            )
        await super().place(context)


@pytest.mark.parametrize("mode", ["normal", "partial", "phase"])
@pytest.mark.parametrize("operation_id", ["done", "retry"])
def test_operation_entry_can_gain_missing_outcome_metadata(mode: str, operation_id: str) -> None:
    value = _replay_evidence()
    entry = next(item for item in value["replay"]["operationExecutions"] if item["operationId"] == operation_id)
    if operation_id == "done":
        entry["outcome"] = "SUCCEEDED"
    original = deepcopy(entry)
    original.pop("outcome")
    assert validate_evidence(value) == []
    result, _ = _run(OperationEntryAdapter(value, [original], mode))
    if mode != "partial":
        assert result.status == "PASSED", result.errors
    assert validate_evidence(result.evidence) == [], result.errors
    assert result.evidence["replay"]["operationExecutions"] == value["replay"]["operationExecutions"]


def test_operation_entry_enrichment_matches_occurrences_without_stealing_known_metadata() -> None:
    value = _replay_evidence()
    bare = {"requestId": "request-victim", "operationId": "uncheckpointed"}
    failed = {**bare, "outcome": "FAILED"}
    value["replay"]["operationExecutions"].extend(
        [{**failed, "evidenceUri": "memory://failed"}, {**bare, "outcome": "SUCCEEDED"}]
    )
    assert validate_evidence(value) == []
    result, _ = _run(OperationEntryAdapter(value, [bare, failed]))
    assert result.status == "PASSED", result.errors
    assert result.evidence["replay"]["operationExecutions"] == value["replay"]["operationExecutions"]


def test_new_retry_entries_remain_distinct_on_the_same_request_and_operation() -> None:
    value = _replay_evidence(3)
    original = deepcopy(
        next(item for item in value["replay"]["operationExecutions"] if item.get("attemptId") == "retry-1")
    )
    original.pop("outcome")
    result, _ = _run(OperationEntryAdapter(value, [original], "phase"))
    assert result.status == "PASSED", result.errors
    entries = [item for item in result.evidence["replay"]["operationExecutions"] if item["operationId"] == "retry"]
    assert [item["attemptId"] for item in entries] == ["retry-1", "retry-2", "retry-3"]


def test_operation_entry_metadata_does_not_hide_completed_code_on_resume() -> None:
    value = _replay_evidence()
    original = {"requestId": "request-resume", "operationId": "done"}
    value["replay"]["operationExecutions"].append({**original, "outcome": "SUCCEEDED"})
    result, _ = _run(OperationEntryAdapter(value, [original]))
    assert result.status != "PASSED"
    assert any("repeated completed user code" in error for error in result.errors), result.errors


@pytest.mark.parametrize("field", ["count", "expectedCount"])
def test_integral_json_side_effect_observations_are_valid(field: str) -> None:
    value = _replay_evidence()
    known = deepcopy(_effect(value))
    known[field] = float(known[field])
    result, _ = _run(SideEffectObservationAdapter(value, [known]))
    assert result.status == "PASSED", result.errors


def test_missing_side_effect_observation_does_not_block_target_reuse() -> None:
    value = _replay_evidence()
    known = deepcopy(_effect(value))
    value["replay"]["sideEffects"].remove(_effect(value))
    result, scheduler = _run(SideEffectObservationAdapter(value, [known]))
    assert result.status != "PASSED", result.errors
    assert scheduler.quarantine_reason(value["profile"]["functionArn"]) is None
