# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Strict coverage and explicit LMI selection cannot silently accept missing work."""

import json
from unittest.mock import patch

import pytest

from aws_durable_execution_conformance_tests.lmi.app import build_report, evidence_results, load_manifest, run
from aws_durable_execution_conformance_tests.lmi.catalog import requirements
from aws_durable_execution_conformance_tests.report import (
    FAIL_ON_FAILED,
    FAIL_ON_STRICT,
    Report,
    ReportEntry,
    ReportStatus,
    RunMetadata,
)


def manifest():
    return {
        "schemaVersion": 1,
        "runId": "run-1",
        "runtime": "python",
        "capabilityProfile": "lifecycle",
        "profiles": [
            {
                "profile_id": f"native-{n}",
                "resource_id": f"arn:function:{n}",
                "capacity": n,
                "native_concurrency": n,
                "artifact_id": "sdk-commit",
                "configuration_id": f"config-{n}",
            }
            for n in (1, 2)
        ],
        "scenarios": [
            {"case_id": name, "requirement_id": req, "profile_id": "native-2", "scenario": name}
            for req, definition in requirements("lifecycle").items()
            for name in definition["requiredScenarios"]
        ],
    }


def passed_results(plan):
    return [
        {"case_id": case["case_id"], "requirement_id": case["requirement_id"], "status": "PASSED", "errors": []}
        for case in plan["scenarios"]
    ]


def test_strict_acceptance_requires_every_mandatory_requirement():
    plan = manifest()
    results = passed_results(plan)
    assert build_report(plan, results).exit_code() == 0
    for index in range(len(results)):
        incomplete = results[:index] + results[index + 1 :]
        # Missing even one invocation in a multi-case requirement must not disappear.
        assert build_report(plan, incomplete).exit_code() == 1


@pytest.mark.parametrize("status", ["FAILED", "INCOMPLETE_EVIDENCE", "INFRASTRUCTURE_ERROR", "NOT_IMPLEMENTED"])
def test_later_success_cannot_replace_failure_or_gap(status):
    plan = manifest()
    results = passed_results(plan)
    results.append(
        {**results[0], "case_id": "original-failed-attempt", "status": status, "errors": ["original failure"]}
    )
    assert build_report(plan, results).exit_code() == 1


def test_missing_success_or_failure_variant_is_uncovered():
    plan = manifest()
    plan["scenarios"] = [case for case in plan["scenarios"] if case["scenario"] != "normal-failure"]
    assert build_report(plan, passed_results(plan)).exit_code() == 1


@pytest.mark.parametrize(
    "scope,key,value",
    [
        ("run", "capabilityProfile", "lifecycle-otel"),
        ("run", "coverageProfile", "export-lifecycle"),
        ("case", "requirement_id", "lmi-lifecycle-7"),
        ("case", "requirement_id", "lmi-lifecycle-8"),
        ("case", "requirement_id", "lmi-lifecycle-16"),
        ("case", "otel_view", "invocation"),
        ("case", "export_mode", "sync"),
    ],
)
def test_deferred_telemetry_configuration_cannot_claim_lifecycle_acceptance(tmp_path, scope, key, value):
    plan = manifest()
    target = plan if scope == "run" else plan["scenarios"][0]
    target[key] = value
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError):
        load_manifest(path)


def test_strict_policy_does_not_change_ordinary_nonblocking_gaps():
    entry = ReportEntry("requirement", "suite", ReportStatus.NOT_IMPLEMENTED)
    metadata = RunMetadata("run", "template", "region")
    assert Report(metadata, [entry], fail_on=FAIL_ON_FAILED).exit_code() == 0
    assert Report(metadata, [entry], fail_on=FAIL_ON_STRICT).exit_code() == 1


@pytest.mark.parametrize(
    "mutation",
    [
        lambda plan: plan.update(schemaVersion=True),
        lambda plan: plan.update(runtime="unknown"),
        lambda plan: plan.update(scenarioConcurrency=0),
        lambda plan: plan.update(budgets={"placement": float("nan")}),
        lambda plan: plan["profiles"].pop(),
        lambda plan: plan["profiles"][1].update(native_concurrency=1),
        lambda plan: plan["profiles"].append({**plan["profiles"][1], "profile_id": "extra", "resource_id": "extra"}),
        lambda plan: plan["scenarios"].append(plan["scenarios"][0]),
        lambda plan: plan["scenarios"][0].update(requirement_id="not-an-id"),
        lambda plan: plan["scenarios"][0].update(profile_id="not-a-profile"),
    ],
)
def test_invalid_plan_is_rejected_before_loading_adapter(tmp_path, mutation):
    plan = manifest()
    mutation(plan)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError):
        load_manifest(path)


def test_valid_plan_can_exceed_native_invocation_concurrency(tmp_path):
    plan = manifest()
    plan["scenarioConcurrency"] = 16
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    assert load_manifest(path) == plan


def test_evidence_cannot_be_substituted_between_cases_or_artifacts(tmp_path):
    plan = manifest()
    case = plan["scenarios"][0]
    case["evidence_file"] = "evidence.json"
    evidence = {
        "caseId": "other-case",
        "runId": "other-run",
        "runtime": "python",
        "requirementId": case["requirement_id"],
        "profile": {"functionArn": "other-function"},
        "artifact": {"sdk": "other-commit"},
    }
    (tmp_path / "evidence.json").write_text(json.dumps(evidence))
    with patch("aws_durable_execution_conformance_tests.lmi.app.validate", return_value=[]):
        result = evidence_results(plan, tmp_path)[0]
    assert result["status"] == "FAILED"
    assert any("artifact" in error for error in result["errors"])
    assert any("caseId" in error for error in result["errors"])


def test_cli_writes_blocking_reports_for_empty_coverage(tmp_path):
    plan = manifest()
    plan["scenarios"] = []
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    with pytest.raises(SystemExit) as exc:
        run(["--manifest", str(path), "--output", str(tmp_path / "out")])
    assert exc.value.code == 1
    report = json.loads((tmp_path / "out/report.json").read_text())
    assert report["summary"]["uncovered"] == 6
    assert report["ci"]["exit_code"] == 1
    assert (tmp_path / "out/report.xml").is_file()


def test_dispatch_is_opt_in():
    from aws_durable_execution_conformance_tests.app import run as main

    with patch("aws_durable_execution_conformance_tests.lmi.app.run") as lmi:
        main(["lmi-lifecycle", "--help"])
        lmi.assert_called_once_with(["--help"])


@pytest.mark.parametrize(
    "changes",
    [
        {"exclusive": False},
        {"exclusive": "false"},
        {"profile_id": "native-1"},
        {"slots": 1},
        {"roles": ["victim"]},
        {"scenario": "some-other-fixture"},
    ],
)
def test_recovery_cannot_weaken_its_measurement_window(tmp_path, changes):
    plan = manifest()
    recovery = next(case for case in plan["scenarios"] if case["scenario"] == "peer-recovery")
    recovery.update(changes)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError):
        load_manifest(path)


def test_normal_failure_cannot_reuse_success_evidence():
    from aws_durable_execution_conformance_tests.lmi.app import _identity_errors

    plan = manifest()
    scenario = next(case for case in plan["scenarios"] if case["scenario"] == "normal-failure")
    value = {"selectedRequests": {"victim": "r"}, "attempts": [{"requestId": "r", "outcome": "SUCCEEDED"}]}
    assert any(
        "requires the selected victim outcome FAILED" in error for error in _identity_errors(plan, scenario, value)
    )


@pytest.mark.parametrize("complete", [True, False])
def test_live_cli_uses_catalog_scheduling_and_persists_before_dispatch(tmp_path, complete):
    import asyncio
    import sys
    from copy import deepcopy
    from pathlib import Path
    from runpy import run_path
    from types import SimpleNamespace

    from aws_durable_execution_conformance_tests.lmi.app import live_results

    evidence = run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))["evidence"]

    value = evidence()
    if not complete:
        value.pop("collection")
        value.pop("retirement")
    finished = []
    plan = manifest()
    plan["runtime"] = value["runtime"]
    plan["scenarios"] = [plan["scenarios"][0]]
    scenario = plan["scenarios"][0]
    scenario["case_id"] = value["caseId"]
    profile = plan["profiles"][1]
    profile.update(
        resource_id=value["profile"]["functionArn"],
        artifact_id=value["artifact"]["sdk"],
        configuration_id=value["profile"]["configurationDigest"],
    )

    class Adapter:
        async def prepare(self, profile):
            pass

        async def finish(self, profile, *, quiescent):
            finished.append(quiescent)

        async def start(self, context):
            assert context.scenario.slots == 2
            assert context.scenario.roles == ["victim", "peer"]
            for attempt in value["attempts"]:
                context.record_attempt(attempt["requestId"], role=attempt["role"], requestId=attempt["requestId"])
                # This is where a real adapter sends the request. The audit is already durable.
                assert attempt["requestId"] in (tmp_path / "attempts.jsonl").read_text()

        async def place(self, context):
            pass

        async def execute(self, context):
            pass

        async def collect(self, context):
            return deepcopy(value)

        async def release(self, context):
            pass

        async def retire(self, context):
            if complete:
                context.evidence["retirement"] = deepcopy(value["retirement"])
            return True

    with patch.dict(sys.modules, {"fixture": SimpleNamespace(create=lambda manifest: Adapter())}):
        results = asyncio.run(live_results(plan, "fixture:create", audit_dir=tmp_path))
    expected_status = "PASSED" if complete else "FAILED"
    assert results[0]["status"] == expected_status, results
    assert finished == [complete]
    if not complete:
        assert any("retirement" in error for error in results[0]["errors"])
    audit = [json.loads(line) for line in (tmp_path / "results.jsonl").read_text().splitlines()]
    assert audit[-1]["status"] == expected_status
    assert len(audit[-1]["attempts"]) == 2


def test_output_reuse_preserves_previous_run(tmp_path):
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest()))
    output = tmp_path / "out"
    output.mkdir()
    original = output / "evidence.json"
    original.write_text("retained original evidence")
    with pytest.raises(SystemExit) as error:
        run(["--manifest", str(path), "--output", str(output)])
    assert error.value.code == 2
    assert original.read_text() == "retained original evidence"


def test_python_regressions_are_not_a_builtin_shared_profile(tmp_path):
    plan = manifest()
    plan.update(coverageProfile="python-lifecycle", scenarios=[])
    for profile in plan["profiles"]:
        profile["recovery_policy"] = "reuse-worker"
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError, match="coverage profile"):
        load_manifest(path)
