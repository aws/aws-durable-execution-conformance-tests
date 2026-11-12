# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The Python profile must exercise every declared native/view/behavior slot."""

import json
from copy import deepcopy
from pathlib import Path
from runpy import run_path
from unittest.mock import patch

import pytest

from aws_durable_execution_conformance_tests.lmi.app import (
    _identity_errors,
    build_report,
    coverage_inventory,
    load_manifest,
    validate,
)
from aws_durable_execution_conformance_tests.lmi.matrix import required_cases
from aws_durable_execution_conformance_tests.report import ReportStatus


def manifest(capability="lifecycle-otel"):
    plan = run_path(str(Path(__file__).with_name("test_lmi_app.py")))["manifest"]()
    plan.update(coverageProfile="python-lifecycle", capabilityProfile=capability)
    for profile in plan["profiles"]:
        profile["recovery_policy"] = "reuse-worker"
    plan["scenarios"] = [
        {
            "case_id": f"case-{index}",
            "requirement_id": case.requirement_id,
            "scenario": case.scenario,
            "profile_id": f"native-{case.native_concurrency}",
            "behavior_budgets": {name: (0 if name.startswith("max") else 1000) for name in case.budget_names},
            **({"otel_view": case.otel_view} if case.otel_view else {}),
        }
        for index, case in enumerate(required_cases("python", capability, "python-lifecycle"))
    ]
    return plan


def results(plan):
    return [
        {
            "case_id": case["case_id"],
            "requirement_id": case["requirement_id"],
            "profile_id": case["profile_id"],
            "status": "PASSED",
            "errors": [],
        }
        for case in plan["scenarios"]
    ]


@pytest.mark.parametrize("capability,count", [("lifecycle", 28), ("lifecycle-otel", 76)])
def test_full_python_matrix_is_explicit_and_reported(tmp_path, capability, count):
    plan = manifest(capability)
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan))
    assert load_manifest(path) == plan
    assert len(coverage_inventory(plan)["required"]) == count
    assert coverage_inventory(plan)["missing"] == []
    assert build_report(plan, results(plan)).exit_code() == 0


@pytest.mark.parametrize("slot", range(76))
def test_removing_any_required_matrix_slot_blocks_acceptance(slot):
    plan = manifest()
    removed = plan["scenarios"].pop(slot)
    report = build_report(plan, results(plan))
    assert report.exit_code() == 1
    entry = next(entry for entry in report.entries if entry.id == removed["requirement_id"])
    assert entry.status == ReportStatus.UNCOVERED
    assert any("Missing mandatory matrix slot" in error for error in entry.errors)


@pytest.mark.parametrize(
    "predicate",
    [
        lambda case: case["profile_id"] == "native-1",
        lambda case: case.get("otel_view") == "execution",
        lambda case: case["scenario"] == "runtime-carrier-unsampled",
        lambda case: case["scenario"] == "export-error-recovery",
        lambda case: case["scenario"] == "held-export",
        lambda case: not case.get("otel_view"),
    ],
)
def test_declared_but_unexercised_profiles_or_paths_never_pass(predicate):
    plan = manifest()
    plan["scenarios"] = [case for case in plan["scenarios"] if not predicate(case)]
    assert build_report(plan, results(plan)).exit_code() == 1


def test_duplicate_c2_cases_cannot_fill_c1_coverage():
    plan = manifest()
    plan["scenarios"] = [case for case in plan["scenarios"] if case["profile_id"] == "native-2"]
    copies = deepcopy(plan["scenarios"])
    for case in copies:
        case["case_id"] += "-duplicate"
    plan["scenarios"].extend(copies)
    assert len(plan["scenarios"]) == 76
    assert build_report(plan, results(plan)).exit_code() == 1


def test_every_gap_and_failed_variant_stays_blocking():
    plan = manifest()
    passed = results(plan)
    for status in ("FAILED", "NOT_IMPLEMENTED", "INCOMPLETE_EVIDENCE", "INFRASTRUCTURE_ERROR"):
        current = deepcopy(passed)
        current[0].update(status=status, errors=["original fixture did not pass"])
        assert build_report(plan, current).exit_code() == 1
    assert build_report(plan, passed[1:]).exit_code() == 1


@pytest.mark.parametrize(
    "mutation",
    [
        lambda plan: plan.update(runtime="java"),
        lambda plan: plan.update(coverageProfile="python-ish"),
        lambda plan: plan["scenarios"][0].update(scenario="made-up-selector"),
        lambda plan: plan["scenarios"][0].update(otel_view="wrong-view"),
        lambda plan: plan["scenarios"][0].update(coverage_profile="standard"),
    ],
)
def test_invalid_profile_is_rejected_before_admission(tmp_path, mutation):
    plan = manifest()
    mutation(plan)
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError):
        load_manifest(path)


def test_evidence_identity_includes_behavior_and_view():
    plan = manifest()
    case = next(case for case in plan["scenarios"] if case.get("otel_view"))
    evidence = {"coverageProfile": "standard", "scenario": "wrong-behavior", "otelView": "wrong-view"}
    errors = _identity_errors(plan, case, evidence)
    assert any("coverageProfile" in error for error in errors)
    assert any("scenario" in error for error in errors)
    assert any("OTel view" in error for error in errors)


def test_otel_validates_lifecycle_requirements_too():
    evidence = {
        "coverageProfile": "python-lifecycle",
        "runtime": "python",
        "requirementId": "lmi-lifecycle-11",
        "scenario": "timeout-retry",
        "otelView": "execution",
    }
    with (
        patch("aws_durable_execution_conformance_tests.lmi.evidence.validate_common_evidence", return_value=[]),
        patch(
            "aws_durable_execution_conformance_tests.lmi.lifecycle_regressions.validate_lifecycle_regressions",
            return_value=[],
        ),
        patch(
            "aws_durable_execution_conformance_tests_otel.lmi_regressions.validate_lifecycle_telemetry",
            return_value=["ASSERTION: missing replay Workflow"],
        ) as telemetry,
    ):
        assert validate(evidence) == ["ASSERTION: missing replay Workflow"]
        telemetry.assert_called_once_with(evidence, phase="final")


def test_observed_budgets_cannot_be_inflated_after_the_run():
    plan = manifest()
    case = plan["scenarios"][0]
    evidence = {
        "coverageProfile": "python-lifecycle",
        "scenario": case["scenario"],
        "regression": {"budgets": {"observationGraceMs": 999999}},
    }
    assert any("budgets" in error for error in _identity_errors(plan, case, evidence))


@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        {"observationGraceMs": True},
        {"observationGraceMs": float("inf")},
        {"observationGraceMs": 0},
        {"observationGraceMs": -1},
    ],
)
def test_behavior_budget_configuration_is_required_and_finite(tmp_path, value):
    plan = manifest()
    plan["scenarios"][0]["behavior_budgets"] = value
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError):
        load_manifest(path)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda rows: rows[0].update(profile_id="wrong-profile"),
        lambda rows: rows[0].update(errors=["unresolved assertion"]),
        lambda rows: rows.append(deepcopy(rows[0])),
        lambda rows: rows[0].update(case_id="unadmitted-case"),
    ],
)
def test_strict_report_rejects_misbound_or_contradictory_results(mutation):
    plan = manifest()
    rows = results(plan)
    mutation(rows)
    assert build_report(plan, rows).exit_code() == 1


@pytest.mark.parametrize("policy", [None, "unknown", True])
def test_recovery_policy_must_be_declared_before_execution(tmp_path, policy):
    plan = manifest()
    plan["profiles"][0]["recovery_policy"] = policy
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError):
        load_manifest(path)
