# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Collected replay evidence cannot rewrite previously observed retry order."""

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
