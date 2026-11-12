# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Mode-specific acceptance cannot be satisfied by another exporter configuration."""

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


def manifest(runtime="java"):
    plan = run_path(str(Path(__file__).with_name("test_lmi_app.py")))["manifest"]()
    plan.update(runtime=runtime, coverageProfile="export-lifecycle", capabilityProfile="lifecycle-otel")
    plan["scenarios"] = [
        {
            "case_id": f"export-{index}",
            "requirement_id": case.requirement_id,
            "scenario": case.scenario,
            "profile_id": f"native-{case.native_concurrency}",
            "otel_view": case.otel_view,
            "export_mode": case.export_mode,
            "behavior_budgets": {name: 1000 for name in case.budget_names},
        }
        for index, case in enumerate(required_cases(runtime, "lifecycle-otel", "export-lifecycle"))
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


@pytest.mark.parametrize("runtime", ["javascript", "java", "python"])
def test_both_modes_are_available_independent_of_language(tmp_path, runtime):
    plan = manifest(runtime)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    assert load_manifest(path) == plan
    assert len(coverage_inventory(plan)["required"]) == 24
    assert coverage_inventory(plan)["missing"] == []
    assert build_report(plan, results(plan)).exit_code() == 0


@pytest.mark.parametrize("slot", range(24))
def test_removing_any_mode_view_native_or_outcome_slot_blocks_acceptance(slot):
    plan = manifest()
    plan["scenarios"].pop(slot)
    report = build_report(plan, results(plan))
    assert report.exit_code() == 1
    assert report.entries[0].status == ReportStatus.UNCOVERED
    assert any("export=" in error for error in report.entries[0].errors)


def test_duplicate_sync_success_cannot_replace_async_coverage():
    plan = manifest()
    plan["scenarios"] = [case for case in plan["scenarios"] if case["export_mode"] == "sync"]
    repeated = deepcopy(plan["scenarios"])
    for case in repeated:
        case["case_id"] += "-repeated"
    plan["scenarios"].extend(repeated)
    assert len(plan["scenarios"]) == 24
    assert build_report(plan, results(plan)).exit_code() == 1


@pytest.mark.parametrize(
    "mutation",
    [
        lambda plan: plan["scenarios"][0].pop("export_mode"),
        lambda plan: plan["scenarios"][0].update(export_mode="java"),
        lambda plan: plan["scenarios"][0].update(export_mode=True),
        lambda plan: plan.update(capabilityProfile="lifecycle"),
        lambda plan: next(case for case in plan["scenarios"] if case["export_mode"] == "async-bounded-flush")[
            "behavior_budgets"
        ].pop("flushWaitMs"),
        lambda plan: next(case for case in plan["scenarios"] if case["scenario"] == "held-export").update(
            exclusive=False
        ),
    ],
)
def test_invalid_mode_configuration_is_rejected_before_adapter_load(tmp_path, mutation):
    plan = manifest()
    mutation(plan)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError):
        load_manifest(path)


def test_mode_and_budgets_are_bound_to_the_admitted_case():
    plan = manifest()
    case = next(case for case in plan["scenarios"] if case["export_mode"] == "sync")
    evidence = {
        "coverageProfile": "export-lifecycle",
        "exportMode": "async-bounded-flush",
        "exportLifecycle": {"budgets": {"flushWaitMs": 999999}},
    }
    errors = _identity_errors(plan, case, evidence)
    assert any("export mode" in error for error in errors)
    assert any("budgets" in error for error in errors)


def test_common_export_profile_uses_its_own_validator():
    value = {"coverageProfile": "export-lifecycle", "runtime": "java", "requirementId": "lmi-lifecycle-16"}
    with (
        patch("aws_durable_execution_conformance_tests.lmi.evidence.validate_common_evidence", return_value=[]),
        patch(
            "aws_durable_execution_conformance_tests_otel.lmi_exports.validate_export_lifecycle",
            return_value=["INCOMPLETE: pending export"],
        ) as export,
    ):
        assert validate(value) == ["INCOMPLETE: pending export"]
        export.assert_called_once_with(value, phase="final")
