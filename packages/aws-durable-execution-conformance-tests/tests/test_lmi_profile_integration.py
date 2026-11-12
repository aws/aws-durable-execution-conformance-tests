# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Run every matrix cell through the composed common/lifecycle/OTel validators."""

from functools import lru_cache
from pathlib import Path
from runpy import run_path

import pytest

from aws_durable_execution_conformance_tests.lmi.app import validate
from aws_durable_execution_conformance_tests.lmi.matrix import describe_case, required_cases


@lru_cache(maxsize=2)
def builder(telemetry=False):
    core = Path(__file__).with_name("test_lmi_lifecycle_regressions.py")
    otel = (
        Path(__file__).resolve().parents[2]
        / "aws-durable-execution-conformance-tests-otel/tests/test_lmi_otel_regressions.py"
    )
    return run_path(str(otel if telemetry else core))["build_evidence"]


@pytest.mark.parametrize("case", required_cases("python", "lifecycle-otel", "python-lifecycle"), ids=describe_case)
def test_each_python_matrix_cell_passes_composed_validation(case):
    if case.otel_view is None:
        evidence = builder()(case.scenario, native=case.native_concurrency)
    else:
        evidence = builder(True)(case.scenario, native=case.native_concurrency, view=case.otel_view)
    assert evidence["coverageProfile"] == "python-lifecycle"
    assert evidence["scenario"] == case.scenario
    assert evidence["profile"]["nativeConcurrency"] == case.native_concurrency
    assert evidence.get("otelView") == case.otel_view
    assert set(case.budget_names) <= set(evidence["regression"]["budgets"])
    assert set(case.roles) <= set(evidence["selectedRequests"])
    assert validate(evidence, phase="observations") == []
    assert validate(evidence, phase="final") == []


@pytest.mark.parametrize(
    "scenario,native,view",
    [
        ("warm-lifecycle", 1, None),
        ("timeout-retry", 2, None),
        ("otel-warm-lifecycle", 2, "execution"),
    ],
)
@pytest.mark.parametrize("mutation", [None, "factory", "scenario"])
def test_profile_coordinator_can_journal_every_planned_native_request(tmp_path, scenario, native, view, mutation):
    import asyncio
    import sys
    from copy import deepcopy
    from types import SimpleNamespace
    from unittest.mock import patch

    from aws_durable_execution_conformance_tests.lmi.app import live_results

    evidence = builder(True)(scenario, native=native, view=view) if view else builder()(scenario, native=native)
    plan = {
        "schemaVersion": 1,
        "runId": evidence["runId"],
        "runtime": "python",
        "coverageProfile": "python-lifecycle",
        "capabilityProfile": "lifecycle-otel" if view else "lifecycle",
        "profiles": [
            {
                "profile_id": f"native-{n}",
                "native_concurrency": n,
                "capacity": n,
                "resource_id": evidence["profile"]["functionArn"] if n == native else "arn:other-function",
                "artifact_id": evidence["artifact"]["sdk"],
                "configuration_id": evidence["profile"]["configurationDigest"],
                "recovery_policy": evidence["profile"].get("recoveryPolicy", "reuse-worker"),
            }
            for n in (1, 2)
        ],
        "scenarios": [
            {
                "case_id": evidence["caseId"],
                "requirement_id": evidence["requirementId"],
                "profile_id": f"native-{native}",
                "scenario": scenario,
                "otel_view": view,
                "behavior_budgets": evidence["regression"]["budgets"],
            }
        ],
    }
    if mutation:
        plan["scenarios"][0]["behavior_budgets"] = deepcopy(evidence["regression"]["budgets"])
        plan["scenarios"][0]["behavior_budgets"]["observationGraceMs"] /= 2
    declared = deepcopy(plan)
    finished = []

    class Adapter:
        async def prepare(self, profile):
            pass

        async def finish(self, profile, *, quiescent):
            finished.append(quiescent)

        async def start(self, context):
            if mutation == "scenario":
                context.scenario.behavior_budgets["observationGraceMs"] = evidence["regression"]["budgets"][
                    "observationGraceMs"
                ]
            for attempt in evidence["attempts"]:
                if mutation is None:
                    assert context.dispatcher(attempt["role"])["behaviorBudgets"] == evidence["regression"]["budgets"]
                context.record_attempt(
                    attempt["requestId"],
                    role=attempt["role"],
                    requestId=attempt["requestId"],
                    attempt=attempt["attempt"],
                )

        async def place(self, context):
            pass

        async def execute(self, context):
            pass

        async def collect(self, context):
            return deepcopy(evidence)

        async def release(self, context):
            pass

        async def retire(self, context):
            context.evidence["retirement"] = deepcopy(evidence["retirement"])
            return True

    def create(adapter_manifest):
        if mutation == "factory":
            adapter_manifest["scenarios"][0]["behavior_budgets"]["observationGraceMs"] = evidence["regression"][
                "budgets"
            ]["observationGraceMs"]
        return Adapter()

    with patch.dict(sys.modules, {"matrix_fixture_adapter": SimpleNamespace(create=create)}):
        results = asyncio.run(live_results(plan, "matrix_fixture_adapter:create", audit_dir=tmp_path))
    assert plan == declared, "adapter or Scenario mutated the declared acceptance snapshot"
    if mutation is None:
        assert results[0]["status"] == "PASSED", results[0]["errors"]
    else:
        assert results[0]["status"] != "PASSED"
        assert any("budgets" in error for error in results[0]["errors"])
    assert len(results[0]["attempts"]) == len(evidence["attempts"])
    assert finished == [True]
