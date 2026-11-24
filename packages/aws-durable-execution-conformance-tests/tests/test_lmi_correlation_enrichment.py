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
)
from aws_durable_execution_conformance_tests.lmi.evidence import validate_evidence, validate_retirement_evidence

run_and_adjudicate = run_path(str(Path(__file__).with_name("test_lmi_coordinator.py")))["run_and_adjudicate"]


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
