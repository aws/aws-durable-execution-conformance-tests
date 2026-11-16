# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: Apache-2.0
"""Caller-owned suites reuse infrastructure without joining the shared catalog."""

import json
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from runpy import run_path
from typing import Any

import pytest

from aws_durable_execution_conformance_tests.lmi.app import (
    SuiteDefinition,
    build_report,
    coverage_inventory,
    evidence_results,
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


@pytest.mark.parametrize("role_source", ["manifest", "suite"])
def test_local_retained_evidence_must_cover_every_declared_cohort_role(tmp_path: Path, role_source: str) -> None:
    plan, value = plan_and_evidence()
    suite = definition()
    roles = ("victim", "peer", "spare")
    if role_source == "manifest":
        plan["scenarios"][0]["roles"] = list(roles)
    else:
        suite = replace(suite, cases=(replace(suite.cases[0], roles=roles),))
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    assert load_manifest(path, suite=suite) == plan
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(json.dumps(value))
    result = evidence_results(plan, tmp_path, suite=suite)[0]
    assert result["status"] == "FAILED"
    assert any("declared cohort role" in error for error in result["errors"]), result["errors"]

    fixtures = run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))
    spare = fixtures["attempt"]("spare")
    value["attempts"].append(spare)
    value["selectedRequests"]["spare"] = spare["requestId"]
    fixtures["refresh_histories"](value)
    fixtures["record_owned_resources"](value)
    evidence_path.write_text(json.dumps(value))
    result = evidence_results(plan, tmp_path, suite=suite)[0]
    assert result["status"] == "PASSED", result["errors"]


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


@pytest.mark.parametrize("reserved", ["profile", "report", "requirement"])
def test_caller_owned_rules_cannot_masquerade_as_shared_conformance(tmp_path: Path, reserved: str) -> None:
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
