# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Composed exporter validation and coordinator retirement for both modes."""

from functools import lru_cache
from pathlib import Path
from runpy import run_path

import pytest

from aws_durable_execution_conformance_tests.lmi.app import validate
from aws_durable_execution_conformance_tests.lmi.matrix import describe_case, required_cases


@lru_cache(maxsize=1)
def builder():
    path = (
        Path(__file__).resolve().parents[2] / "aws-durable-execution-conformance-tests-otel/tests/test_lmi_exports.py"
    )
    return run_path(str(path))["build_evidence"]


@pytest.mark.parametrize("runtime", ["javascript", "java", "python"])
@pytest.mark.parametrize("case", required_cases("java", "lifecycle-otel", "export-lifecycle"), ids=describe_case)
def test_all_modes_use_the_shared_validator_for_every_runtime(runtime, case):
    data = builder()(case.scenario, runtime, case.native_concurrency, case.otel_view, case.export_mode)
    assert validate(data, phase="observations") == []
    assert validate(data, phase="final") == []


@pytest.mark.parametrize("complete", [True, False])
def test_pending_export_holds_deployment_ownership_until_proven_retirement(complete):
    import asyncio
    from copy import deepcopy

    from aws_durable_execution_conformance_tests.lmi.coordinator import (
        Profile,
        QuarantinedError,
        ResourceScheduler,
        Scenario,
        run_scenarios,
    )

    async def exercise():
        data = builder()("held-export", "java", 1, "invocation", "async-bounded-flush")
        if not complete:
            data["events"] = [
                event for event in data["events"] if event["kind"] not in {"export_completed", "flush_completed"}
            ]
        profile = Profile(
            "native-1",
            data["profile"]["functionArn"],
            1,
            1,
            data["artifact"]["sdk"],
            data["profile"]["configurationDigest"],
        )
        scenario = Scenario(
            data["caseId"],
            data["requirementId"],
            profile.profile_id,
            data["scenario"],
            cohort_id=data["cohortId"],
            roles=tuple(data["selectedRequests"]),
            exclusive=True,
            otel_view=data["otelView"],
            coverage_profile="export-lifecycle",
            export_mode=data["exportMode"],
            behavior_budgets=data["exportLifecycle"]["budgets"],
        )
        wrapper_returned, release_export = asyncio.Event(), asyncio.Event()
        scheduler = ResourceScheduler()
        finished = []
        deployment_entered = []

        class Adapter:
            async def prepare(self, profile):
                pass

            async def start(self, context):
                assert context.dispatcher("victim")["exportMode"] == "async-bounded-flush"
                for attempt in data["attempts"]:
                    context.record_attempt(attempt["requestId"], role=attempt["role"], requestId=attempt["requestId"])

            async def place(self, context):
                pass

            async def execute(self, context):
                # Native work returned, but the process-owned exporter still owns work.
                await context.release_capacity()
                wrapper_returned.set()
                await release_export.wait()

            async def collect(self, context):
                return deepcopy(data)

            async def release(self, context):
                pass

            async def retire(self, context):
                context.evidence["retirement"] = deepcopy(data["retirement"])
                return True  # Evidence, not this assertion alone, must establish retirement.

            async def finish(self, profile, *, quiescent):
                finished.append(quiescent)

        async def deployment():
            async with scheduler.deployment(profile.resource_id, "next-artifact", timeout=2):
                deployment_entered.append(True)

        running = asyncio.create_task(
            run_scenarios(
                [scenario], [profile], Adapter(), run_id=data["runId"], validator=validate, scheduler=scheduler
            )
        )
        await wrapper_returned.wait()
        updating = asyncio.create_task(deployment())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert deployment_entered == []
        release_export.set()
        result = (await running)[0]
        if complete:
            await updating
            assert result.status == "PASSED", result.errors
            assert deployment_entered == [True]
        else:
            with pytest.raises(QuarantinedError):
                await updating
            assert result.status == "INFRASTRUCTURE_ERROR"
            assert any("retirement" in error for error in result.errors)
            assert deployment_entered == []
        assert finished == [complete]

    asyncio.run(exercise())
