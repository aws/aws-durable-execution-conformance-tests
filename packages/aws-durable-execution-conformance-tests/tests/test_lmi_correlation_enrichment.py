# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Late-work correlation can be completed without replacing known observations."""

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
from aws_durable_execution_conformance_tests.lmi.evidence import validate_evidence, validate_retirement_evidence


def _warm_evidence() -> dict[str, Any]:
    fixture = run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))
    value = fixture["warm_evidence"]()
    spare = fixture["attempt"]("spare", start=55, end=75, process="process-3")
    value["attempts"].append(spare)
    value["selectedRequests"]["spare"] = spare["requestId"]
    fixture["refresh_histories"](value)
    fixture["record_owned_resources"](value)
    victim = value["attempts"][0]
    value["sdkRequests"].append(
        fixture["event"](
            victim,
            "request",
            60,
            method="CheckpointDurableExecution",
            outcome="REJECTED",
            observedDuringRequestId="request-warm",
        )
    )
    # Supply an independent physical retirement audit as well as the standard
    # warm-reuse assertions, so richer cleanup records exercise the same rules.
    for attempt in value["attempts"]:
        local = [event for event in value["events"] if event["requestId"] == attempt["requestId"]]
        value["events"].append(fixture["event"](attempt, "wrapper_enter", attempt["startedAtMs"]))
        value["events"].append(fixture["event"](attempt, "task_start", attempt["startedAtMs"] + 0.1, taskId="root"))
        exits = [event for event in local if event["kind"] == "task_exit"]
        if exits:
            exits[0]["taskId"] = "root"
        else:
            value["events"].append(fixture["event"](attempt, "task_exit", attempt["endedAtMs"], taskId="root"))
        if not any(event["kind"] == "wrapper_return" for event in local):
            value["events"].append(fixture["event"](attempt, "wrapper_return", attempt["endedAtMs"]))
    for index, request in enumerate(value["sdkRequests"]):
        if request["outcome"] != "DISPATCHED":
            continue
        request["callId"] = f"dispatch-{index}"
        kind = "poll" if request["method"] == "GetDurableExecutionState" else "checkpoint"
        value["events"].extend([{**request, "kind": f"{kind}_call"}, {**request, "kind": f"{kind}_exit"}])
    _resequence(value)
    assert validate_evidence(value) == []
    assert validate_retirement_evidence(value) == []
    return value


def _resequence(value: dict[str, Any]) -> None:
    for attempt in value["attempts"]:
        events = sorted(
            (event for event in value["events"] if event["requestId"] == attempt["requestId"]),
            key=lambda event: event["atMs"],
        )
        for index, event in enumerate(events):
            event["sequence"] = index


class CorrelationAdapter:
    def __init__(
        self, value: dict[str, Any], inventory: str, initial: list[dict[str, Any]], mode: str = "normal"
    ) -> None:
        self.value, self.inventory, self.initial, self.mode = value, inventory, initial, mode

    async def prepare(self, profile: Profile) -> None:
        pass

    async def finish(self, profile: Profile, *, quiescent: bool) -> None:
        pass

    async def start(self, context: ScenarioContext) -> None:
        for attempt in self.value["attempts"]:
            context.record_attempt(
                attempt["requestId"],
                role=attempt["role"],
                requestId=attempt["requestId"],
                executionArn=attempt["executionArn"],
            )
        context.evidence = {self.inventory: deepcopy(self.initial)}

    async def place(self, context: ScenarioContext) -> None:
        if self.mode == "phase":
            context.evidence[self.inventory] = deepcopy(self.value[self.inventory])
        if self.mode == "partial":
            raise RuntimeError("collect partial warm-reuse evidence")

    async def execute(self, context: ScenarioContext) -> None:
        pass

    async def collect(self, context: ScenarioContext) -> Mapping[str, Any]:
        value = deepcopy(self.value)
        value.pop("retirement")
        return value

    async def collect_partial(self, context: ScenarioContext) -> Mapping[str, Any]:
        return await self.collect(context)

    async def release(self, context: ScenarioContext) -> None:
        pass

    async def retire(self, context: ScenarioContext) -> bool:
        context.evidence["retirement"] = deepcopy(self.value["retirement"])
        context.retirement_evidence = deepcopy(self.value)
        return True


def _run(adapter: CorrelationAdapter) -> tuple[ScenarioResult, ResourceScheduler]:
    value = adapter.value
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
        "warm-reuse",
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


@pytest.mark.parametrize("inventory", ["events", "sdkRequests"])
@pytest.mark.parametrize("mode", ["normal", "partial", "phase"])
@pytest.mark.parametrize("null_placeholder", [False, True])
def test_late_work_can_gain_observed_during_correlation(inventory: str, mode: str, null_placeholder: bool) -> None:
    value = _warm_evidence()
    record = next(item for item in value[inventory] if item.get("observedDuringRequestId") == "request-warm")
    original = deepcopy(record)
    original.pop("observedDuringRequestId")
    if null_placeholder:
        original["observedDuringRequestId"] = None
    if inventory == "events":
        original.pop("sequence")
    result, scheduler = _run(CorrelationAdapter(value, inventory, [original], mode))
    if mode != "partial":
        assert result.status == "PASSED", result.errors
    else:
        assert result.status != "PASSED"
    assert validate_evidence(result.evidence) == [], result.errors
    assert len(result.evidence[inventory]) == len(value[inventory])
    assert scheduler.quarantine_reason(value["profile"]["functionArn"]) is None


@pytest.mark.parametrize("inventory", ["events", "sdkRequests"])
def test_known_late_work_correlation_cannot_be_reassigned(inventory: str) -> None:
    value = _warm_evidence()
    original = deepcopy(
        next(item for item in value[inventory] if item.get("observedDuringRequestId") == "request-warm")
    )
    original["observedDuringRequestId"] = "request-spare"
    result, scheduler = _run(CorrelationAdapter(value, inventory, [original]))
    assert result.status != "PASSED", result.errors
    assert any(item.get("observedDuringRequestId") == "request-spare" for item in result.evidence[inventory])
    assert scheduler.quarantine_reason(value["profile"]["functionArn"])


def test_known_sdk_correlation_cannot_be_reset_to_null() -> None:
    value = _warm_evidence()
    request = next(item for item in value["sdkRequests"] if item.get("observedDuringRequestId") == "request-warm")
    original = deepcopy(request)
    request["observedDuringRequestId"] = None
    assert validate_evidence(value) == []
    result, _ = _run(CorrelationAdapter(value, "sdkRequests", [original]))
    assert result.status != "PASSED", result.errors
    assert any(item.get("observedDuringRequestId") == "request-warm" for item in result.evidence["sdkRequests"])


@pytest.mark.parametrize("inventory", ["events", "sdkRequests"])
@pytest.mark.parametrize("missing_occurrence", [False, True])
def test_correlation_enrichment_keeps_each_occurrence(inventory: str, missing_occurrence: bool) -> None:
    value = _warm_evidence()
    record = deepcopy(next(item for item in value[inventory] if item.get("observedDuringRequestId") == "request-warm"))
    if inventory == "events":
        record["kind"] = "effect"
    else:
        value[inventory].remove(
            next(item for item in value[inventory] if item.get("observedDuringRequestId") == "request-warm")
        )
    record.pop("sequence", None)
    uncorrelated = deepcopy(record)
    uncorrelated.pop("observedDuringRequestId")
    other = {**record, "observedDuringRequestId": "request-spare"}
    value[inventory].append(deepcopy(record))
    if not missing_occurrence:
        value[inventory].append(other)
    _resequence(value)
    assert validate_evidence(value) == []
    result, _ = _run(CorrelationAdapter(value, inventory, [uncorrelated, record]))
    if missing_occurrence:
        assert result.status != "PASSED"
        assert len(result.evidence[inventory]) == len(value[inventory]) + 1
    else:
        assert result.status == "PASSED", result.errors
        assert len(result.evidence[inventory]) == len(value[inventory])


@pytest.mark.parametrize("reverse", [False, True])
def test_event_correlation_enrichment_preserves_known_equal_time_order(reverse: bool) -> None:
    value = _warm_evidence()
    late = next(event for event in value["events"] if event["kind"] == "late_activity")
    owner = {key: late[key] for key in ("requestId", "ownerRequestId", "executionArn")}
    value["events"].extend(
        [
            {**owner, "kind": "io_start", "atMs": 45, "taskId": "late-io"},
            {**owner, "kind": "io_exit", "atMs": 60, "taskId": "late-io", "observedDuringRequestId": "request-warm"},
        ]
    )
    _resequence(value)
    ended = next(event for event in value["events"] if event["kind"] == "io_exit")
    original = sorted([deepcopy(late), deepcopy(ended)], key=lambda event: event["sequence"])
    for event in original:
        event.pop("observedDuringRequestId")
    if reverse:
        late["sequence"], ended["sequence"] = ended["sequence"], late["sequence"]
    assert validate_evidence(value) == []
    assert validate_retirement_evidence(value) == []
    result, scheduler = _run(CorrelationAdapter(value, "events", original))
    if reverse:
        assert result.status != "PASSED", result.errors
        assert any("order" in error for error in result.errors), result.errors
        assert scheduler.quarantine_reason(value["profile"]["functionArn"])
    else:
        assert result.status == "PASSED", result.errors
        assert scheduler.quarantine_reason(value["profile"]["functionArn"]) is None
