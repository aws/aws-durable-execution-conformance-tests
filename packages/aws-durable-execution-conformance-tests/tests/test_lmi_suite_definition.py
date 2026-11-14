# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: Apache-2.0
"""Caller-owned suites reuse infrastructure without joining the shared catalog."""

import asyncio
import json
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from runpy import run_path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from aws_durable_execution_conformance_tests.lmi.app import (
    SuiteDefinition,
    build_report,
    coverage_inventory,
    evidence_results,
    live_results,
    load_manifest,
)
from aws_durable_execution_conformance_tests.lmi.catalog import requirements
from aws_durable_execution_conformance_tests.lmi.evidence import validate_common_evidence
from aws_durable_execution_conformance_tests.lmi.matrix import CaseRequirement

LOCAL_ID = "sdk-check-1"


def local_validate(evidence: Mapping[str, Any], *, phase: str = "final") -> list[str]:
    errors = validate_common_evidence(
        evidence, phase="observations" if phase == "observations" else "final", requirement_ids=(LOCAL_ID,)
    )
    if evidence.get("sdkObserved") is not True:
        errors.append("ASSERTION: caller-owned observation is missing")
    return errors


def definition() -> SuiteDefinition:
    return SuiteDefinition(
        coverage_profile="sdk-regression",
        suite_name="sdk-checks",
        requirements={LOCAL_ID: {"description": "SDK-owned observation"}},
        cases=(
            CaseRequirement(
                LOCAL_ID,
                "sdk-observation",
                2,
                ("victim", "peer"),
                False,
                2,
                budget_names=("settleMs", "allowance"),
            ),
        ),
        validator=local_validate,
        evidence_policy_key="sdkPolicy",
        nonnegative_budgets=frozenset({"allowance"}),
    )


def plan_and_evidence() -> tuple[dict[str, Any], dict[str, Any]]:
    value = run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))["evidence"]()
    value.update(requirementId=LOCAL_ID, coverageProfile="sdk-regression", scenario="sdk-observation", sdkObserved=True)
    value["sdkPolicy"] = {"budgets": {"settleMs": 10, "allowance": 0}}
    plan = {
        "schemaVersion": 1,
        "runId": value["runId"],
        "runtime": value["runtime"],
        "coverageProfile": "sdk-regression",
        "capabilityProfile": "lifecycle",
        "profiles": [
            {
                "profile_id": "c1",
                "resource_id": "arn:other",
                "capacity": 1,
                "native_concurrency": 1,
                "artifact_id": "other",
                "configuration_id": "other",
            },
            {
                "profile_id": "c2",
                "resource_id": value["profile"]["functionArn"],
                "capacity": 2,
                "native_concurrency": 2,
                "artifact_id": value["artifact"]["sdk"],
                "configuration_id": value["profile"]["configurationDigest"],
            },
        ],
        "scenarios": [
            {
                "case_id": value["caseId"],
                "requirement_id": LOCAL_ID,
                "profile_id": "c2",
                "scenario": value["scenario"],
                "behavior_budgets": deepcopy(value["sdkPolicy"]["budgets"]),
                "evidence_file": "evidence.json",
            }
        ],
    }
    return plan, value


def test_local_rules_require_an_explicit_definition_and_do_not_enter_the_catalog(tmp_path: Path) -> None:
    plan, value = plan_and_evidence()
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError, match="coverage profile"):
        load_manifest(path)
    assert load_manifest(path, suite=definition()) == plan
    assert LOCAL_ID not in requirements("lifecycle", runtime="javascript")
    (tmp_path / "evidence.json").write_text(json.dumps(value))
    results = evidence_results(plan, tmp_path, suite=definition())
    assert results[0]["status"] == "PASSED"
    report = build_report(plan, results, suite=definition())
    assert report.exit_code() == 0
    assert report.run.suites == ["sdk-checks"]
    assert coverage_inventory(plan, suite=definition())["missing"] == []
    assert build_report(plan, [], suite=definition()).exit_code() == 1
    value["sdkObserved"] = False
    (tmp_path / "evidence.json").write_text(json.dumps(value))
    assert evidence_results(plan, tmp_path, suite=definition())[0]["status"] == "FAILED"


def test_local_definition_cannot_be_used_under_a_different_profile(tmp_path: Path) -> None:
    plan, _ = plan_and_evidence()
    plan["coverageProfile"] = "standard"
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError, match="definition does not match"):
        load_manifest(path, suite=definition())


@pytest.mark.parametrize("bad", [False, -1, float("inf")])
def test_local_budget_allowance_is_still_numeric_finite_and_nonnegative(tmp_path: Path, bad: Any) -> None:
    plan, _ = plan_and_evidence()
    plan["scenarios"][0]["behavior_budgets"]["allowance"] = bad
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError, match="behavior budgets"):
        load_manifest(path, suite=definition())


@pytest.mark.parametrize("mutate", [False, True])
def test_local_live_suite_preserves_budget_snapshot_and_before_dispatch_journal(tmp_path: Path, mutate: bool) -> None:
    plan, value = plan_and_evidence()
    if mutate:
        plan["scenarios"][0]["behavior_budgets"]["settleMs"] = 5
    original = deepcopy(plan)

    class Adapter:
        async def prepare(self, profile: Any) -> None:
            pass

        async def finish(self, profile: Any, *, quiescent: bool) -> None:
            pass

        async def start(self, context: Any) -> None:
            for attempt in value["attempts"]:
                context.record_attempt(attempt["requestId"], role=attempt["role"], requestId=attempt["requestId"])
                assert attempt["requestId"] in (tmp_path / "attempts.jsonl").read_text()

        async def place(self, context: Any) -> None:
            pass

        async def execute(self, context: Any) -> None:
            pass

        async def collect(self, context: Any) -> dict[str, Any]:
            return deepcopy(value)

        async def release(self, context: Any) -> None:
            pass

        async def retire(self, context: Any) -> bool:
            context.evidence["retirement"] = deepcopy(value["retirement"])
            return True

    def factory(manifest: dict[str, Any]) -> Adapter:
        manifest["scenarios"][0]["behavior_budgets"]["settleMs"] = 10
        return Adapter()

    with patch.dict("sys.modules", {"local_suite_adapter": SimpleNamespace(create=factory)}):
        results = asyncio.run(live_results(plan, "local_suite_adapter:create", audit_dir=tmp_path, suite=definition()))
    assert plan == original
    if mutate:
        assert results[0]["status"] != "PASSED"
        assert any("budgets" in error for error in results[0]["errors"])
    else:
        assert results[0]["status"] == "PASSED", results[0]["errors"]


@pytest.mark.parametrize("reserved", ["profile", "report", "requirement"])
def test_caller_owned_rules_cannot_masquerade_as_shared_conformance(tmp_path: Path, reserved: str) -> None:
    from dataclasses import replace

    plan, _ = plan_and_evidence()
    suite = definition()
    if reserved == "profile":
        plan["coverageProfile"] = "standard"
        suite = replace(suite, coverage_profile="standard")
    elif reserved == "report":
        suite = replace(suite, suite_name="lmi-lifecycle")
    else:
        plan["scenarios"][0]["requirement_id"] = "lmi-lifecycle-1"
        suite = replace(
            suite,
            requirements={"lmi-lifecycle-1": {"description": "local substitute"}},
            cases=(replace(suite.cases[0], requirement_id="lmi-lifecycle-1"),),
        )
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError, match=r"shared|namespace"):
        load_manifest(path, suite=suite)
