# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: Apache-2.0
"""Caller-owned suites reuse infrastructure without joining the shared catalog."""

import asyncio
import json
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from runpy import run_path
from types import MappingProxyType
from typing import Any
from unittest.mock import MagicMock

import pytest

from aws_durable_execution_conformance_tests.lmi.app import (
    SuiteDefinition,
    build_report,
    collect_and_adjudicate,
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


@pytest.mark.parametrize("entry_point", ["manifest", "coverage", "report"])
def test_suite_rejects_a_mandatory_case_missing_from_its_requirement_catalog(tmp_path: Path, entry_point: str) -> None:
    plan, _ = plan_and_evidence()
    original = definition()
    missing_case = replace(original.cases[0], requirement_id="sdk-check-2", scenario="additional-check")
    suite = replace(original, cases=(*original.cases, missing_case))
    with pytest.raises(ValueError, match=r"case requirement.*catalog"):
        if entry_point == "manifest":
            path = tmp_path / "manifest.json"
            path.write_text(json.dumps(plan))
            load_manifest(path, suite=suite)
        elif entry_point == "coverage":
            coverage_inventory(plan, suite=suite)
        else:
            build_report(plan, [{**plan["scenarios"][0], "status": "PASSED", "errors": []}], suite=suite)


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
    with pytest.raises(ValueError, match=r"behavior budgets|JSON numbers must be finite"):
        load_manifest(path, suite=definition())


@pytest.mark.parametrize(
    ("declared", "observed", "matches"),
    [(0, False, False), (1, True, False), (0, 0, True), (0, 0.0, True), (0.0, 0, True), (1, 1.0, True), (1.0, 1, True)],
)
def test_caller_owned_budget_binding_preserves_json_types(
    tmp_path: Path, declared: int | float, observed: Any, matches: bool
) -> None:
    plan, value = plan_and_evidence()
    plan["scenarios"][0]["behavior_budgets"]["allowance"] = declared
    value["sdkPolicy"]["budgets"]["allowance"] = observed
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    (tmp_path / "evidence.json").write_text(json.dumps(value))
    suite = definition()
    admitted = load_manifest(path, suite=suite)

    results = evidence_results(admitted, tmp_path, suite=suite)

    assert results[0]["status"] == ("PASSED" if matches else "FAILED")
    if not matches:
        assert any("behavior budgets" in error for error in results[0]["errors"])
    assert build_report(admitted, results, suite=suite).exit_code() == (0 if matches else 1)


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("exclusive", False, "exclusive resource ownership"),
        ("slots", 1, "required cohort"),
        ("roles", ["victim"], "required cohort roles"),
    ],
)
def test_local_collection_suite_enforces_admission_before_constructing_its_adapter(
    tmp_path: Path, field: str, value: Any, reason: str
) -> None:
    plan, _ = plan_and_evidence()
    suite = definition()
    suite = replace(suite, cases=(replace(suite.cases[0], exclusive=True),))
    plan["scenarios"][0][field] = value
    load_adapter = MagicMock(side_effect=AssertionError("Invalid manifest constructed an adapter"))
    with pytest.raises(ValueError, match=reason):
        asyncio.run(collect_and_adjudicate(plan, audit_dir=tmp_path, suite=suite, adapter_factory=load_adapter))
    load_adapter.assert_not_called()


@pytest.mark.parametrize("mutate", [False, True])
def test_local_collection_suite_preserves_budget_snapshot_and_before_dispatch_journal(
    tmp_path: Path, mutate: bool
) -> None:
    plan, value = plan_and_evidence()
    fixtures = run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))
    fixtures["complete_sdk_dispatches"](value)
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

    suite = definition()
    suite = replace(
        suite,
        requirements=MappingProxyType({key: MappingProxyType(row) for key, row in suite.requirements.items()}),
    )
    results = asyncio.run(collect_and_adjudicate(plan, audit_dir=tmp_path, suite=suite, adapter_factory=factory))
    assert plan == original
    if mutate:
        assert results[0]["status"] != "PASSED"
        assert any("budgets" in error for error in results[0]["errors"])
    else:
        assert results[0]["status"] == "PASSED", results[0]["errors"]


@pytest.mark.parametrize("declared", [False, True])
def test_local_collection_suite_declares_service_role_transitions(tmp_path: Path, declared: bool) -> None:
    plan, value = plan_and_evidence()
    suite = definition()
    if declared:
        suite = replace(suite, service_role_transitions={LOCAL_ID: frozenset({("victim", "peer")})})
    victim = value["attempts"][0]
    outcomes: list[str] = []

    class Adapter:
        async def prepare(self, profile: Any) -> None:
            pass

        async def finish(self, profile: Any, *, quiescent: bool) -> None:
            pass

        async def start(self, context: Any) -> None:
            context.record_attempt(
                victim["requestId"],
                role=victim["role"],
                requestId=victim["requestId"],
                executionArn=victim["executionArn"],
            )
            try:
                context.record_service_attempt(
                    "request-continued",
                    original_request_id=victim["requestId"],
                    execution_arn=victim["executionArn"],
                    role="peer",
                )
            except ValueError as error:
                outcomes.append(str(error))
            else:
                outcomes.append("journaled")

        async def place(self, context: Any) -> None:
            pass

        async def execute(self, context: Any) -> None:
            pass

        async def collect(self, context: Any) -> dict[str, Any]:
            return deepcopy(value)

        async def release(self, context: Any) -> None:
            pass

        async def retire(self, context: Any) -> bool:
            return True

    asyncio.run(
        collect_and_adjudicate(plan, audit_dir=tmp_path, suite=suite, adapter_factory=lambda manifest: Adapter())
    )

    journal = [json.loads(line) for line in (tmp_path / "attempts.jsonl").read_text().splitlines()]
    discovered = [row for entry in journal for row in entry["attempts"] if "discoveredAtMs" in row]
    if declared:
        assert outcomes == ["journaled"]
        assert [(row["requestId"], row["role"]) for row in discovered] == [("request-continued", "peer")]
    else:
        assert outcomes == ["Service attempt role does not match its original invocation"]
        assert discovered == []


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


@pytest.mark.parametrize("minimum", [1, 3])
def test_suite_slots_reject_overlap_without_shadowing_disjoint_exact_slot(tmp_path: Path, minimum: int) -> None:
    plan, _ = plan_and_evidence()
    suite = definition()
    exact = suite.cases[0]
    wildcard = replace(exact, native_concurrency=None, minimum_native_concurrency=minimum, roles=("warm",))
    suite = replace(suite, cases=(wildcard, exact))
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    if minimum == 1:
        with pytest.raises(ValueError, match="case definitions overlap"):
            load_manifest(path, suite=suite)
        with pytest.raises(ValueError, match="case definitions overlap"):
            coverage_inventory(plan, suite=suite)
    else:
        assert load_manifest(path, suite=suite) == plan
        assert len(coverage_inventory(plan, suite=suite)["missing"]) == 1


@pytest.mark.parametrize("observed", [None, 10, 1000])
def test_suite_without_policy_key_binds_top_level_budgets(tmp_path: Path, observed: int | None) -> None:
    plan, value = plan_and_evidence()
    suite = replace(definition(), evidence_policy_key=None)
    if observed is not None:
        value["budgets"] = {"settleMs": observed, "allowance": 0}
    (tmp_path / "evidence.json").write_text(json.dumps(value))
    result = evidence_results(plan, tmp_path, suite=suite)[0]
    assert result["status"] == ("PASSED" if observed == 10 else "FAILED")
    if observed != 10:
        assert result["errors"] == ["INCOMPLETE: observed behavior budgets do not match the declared case budgets"]


def test_empty_local_suite_cannot_report_passing_coverage() -> None:
    plan, _ = plan_and_evidence()
    plan["scenarios"] = []
    suite = replace(definition(), requirements={}, cases=())
    with pytest.raises(ValueError, match="must declare at least one mandatory case"):
        build_report(plan, [], suite=suite)
