# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Strict coverage and explicit LMI selection cannot silently accept missing work."""

import asyncio
import errno
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from runpy import run_path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from aws_durable_execution_conformance_tests.lmi.app import (
    adjudicate_collected_evidence,
    build_report,
    collect_and_adjudicate,
    load_manifest,
    run,
)
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
            {
                "case_id": definition["scenario"],
                "requirement_id": req,
                "profile_id": "native-2",
                "scenario": definition["scenario"],
                # Track the catalog so a newly declared budget cannot be
                # left undeclared by this fixture.
                **(
                    {"behavior_budgets": {name: 5 for name in definition["behaviorBudgets"]}}
                    if definition.get("behaviorBudgets")
                    else {}
                ),
            }
            for req, definition in requirements("lifecycle").items()
        ],
    }


def coordinator_capture(plan, evidence_by_case):
    """Return a coordinator-shaped capture with one row per admitted case."""
    rows = []
    for case in plan["scenarios"]:
        row = {"case_id": case["case_id"], "requirement_id": case["requirement_id"], "profile_id": case["profile_id"]}
        if "not_implemented" in case:
            row.update(status="NOT_IMPLEMENTED", errors=[case["not_implemented"]])
        elif case["case_id"] in evidence_by_case:
            evidence = evidence_by_case[case["case_id"]]
            row.update(
                status="COLLECTED",
                errors=[],
                evidence=evidence,
                retirement_evidence=deepcopy(evidence),
                cleanup_evidence={},
                observation_evidence=[],
                attempts=[],
            )
        else:
            row.update(status="INCOMPLETE_EVIDENCE", errors=["INCOMPLETE: no observations collected"])
        rows.append(row)
    return rows


def run_collected_text(directory, plan, raw_evidence, capsys):
    """Adjudicate a one-case capture whose evidence is raw JSON text through the CLI.

    Returns the published results, or the CLI's error output if it rejected the run.
    """
    directory.mkdir(parents=True, exist_ok=True)
    (case,) = plan["scenarios"]
    row = {
        "case_id": case["case_id"],
        "requirement_id": case["requirement_id"],
        "profile_id": case["profile_id"],
        "status": "COLLECTED",
        "errors": [],
        "retirement_evidence": {},
        "cleanup_evidence": {},
        "observation_evidence": [],
        "attempts": [],
    }
    collected = directory / "collected-evidence.json"
    collected.write_text(json.dumps([row])[:-2] + ', "evidence": ' + raw_evidence + "}]")
    source = directory / "manifest.json"
    source.write_text(json.dumps(plan))
    output = directory / "out"
    with pytest.raises(SystemExit) as exc:
        run(["--manifest", str(source), "--collected-evidence", str(collected), "--output", str(output)])
    captured = capsys.readouterr()
    if exc.value.code == 2:
        return captured.err
    return json.loads((output / "evidence.json").read_text(), parse_float=Decimal)


def passed_results(plan):
    return [
        {"case_id": case["case_id"], "requirement_id": case["requirement_id"], "status": "PASSED", "errors": []}
        for case in plan["scenarios"]
    ]


@pytest.mark.parametrize("encoding", [".0", "e0"])
def test_manifest_normalizes_integral_json_numbers_before_typed_consumers(tmp_path, encoding):
    plan = manifest()
    plan["scenarioConcurrency"] = 8
    for scenario in plan["scenarios"]:
        scenario["slots"] = 2
    raw = re.sub(
        r'("(?:schemaVersion|capacity|native_concurrency|scenarioConcurrency|slots)"\s*:\s*)(\d+)',
        lambda match: f"{match[1]}{match[2]}{encoding}",
        json.dumps(plan),
    )
    assert type(json.loads(raw)["schemaVersion"]) is float
    path = tmp_path / "manifest.json"
    path.write_text(raw)

    admitted = load_manifest(path)

    assert type(admitted["schemaVersion"]) is int
    assert type(admitted["scenarioConcurrency"]) is int
    assert all(
        type(profile[key]) is int for profile in admitted["profiles"] for key in ("capacity", "native_concurrency")
    )
    assert all(type(scenario["slots"]) is int for scenario in admitted["scenarios"])
    assert build_report(admitted, passed_results(admitted)).exit_code() == 0


@pytest.mark.parametrize(
    "path",
    [
        ("schemaVersion",),
        ("profiles", 0, "capacity"),
        ("profiles", 1, "native_concurrency"),
        ("scenarioConcurrency",),
        ("scenarios", 0, "slots"),
    ],
)
@pytest.mark.parametrize("invalid", [True, False, 0.5, float("inf"), float("nan")])
def test_manifest_integer_fields_reject_booleans_fractions_and_nonfinite_values(tmp_path, path, invalid):
    plan = manifest()
    target = plan
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = invalid
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(plan))
    with pytest.raises(ValueError):
        load_manifest(manifest_path)


def test_manifest_preserves_large_integer_capacity_without_float_conversion(tmp_path):
    plan = manifest()
    plan["profiles"][1]["capacity"] = 10**1000
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    admitted = load_manifest(path)
    assert admitted["profiles"][1]["capacity"] == 10**1000


@pytest.mark.parametrize("behavior_budget", [False, True])
def test_manifest_rejects_budgets_that_overflow_float(tmp_path, behavior_budget):
    plan, _ = timed_plan(3)
    if behavior_budget:
        plan["scenarios"][0]["behavior_budgets"]["admissionMs"] = 10**400
    else:
        plan["budgets"] = {"execution": 10**400}
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError, match="budgets must"):
        load_manifest(path)


def test_manifest_default_profiles_are_independent_of_order(tmp_path):
    plan = manifest()
    extra = {
        **plan["profiles"][1],
        "profile_id": "extra",
        "resource_id": "arn:extra",
        "incompatibilityReason": "different worker configuration",
    }
    plan["profiles"].insert(0, extra)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    assert load_manifest(path) == plan


def test_manifest_rejects_evidence_files(tmp_path):
    plan = manifest()
    plan["scenarios"][0]["evidence_file"] = "evidence.json"
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError, match="evidence_file is not supported"):
        load_manifest(path)


def test_strict_acceptance_requires_every_mandatory_requirement():
    plan = manifest()
    results = passed_results(plan)
    assert build_report(plan, results).exit_code() == 0
    for index in range(len(results)):
        incomplete = results[:index] + results[index + 1 :]
        # Every missing required case must remain visible in its requirement report.
        assert build_report(plan, incomplete).exit_code() == 1


@pytest.mark.parametrize("status", ["FAILED", "INCOMPLETE_EVIDENCE", "INFRASTRUCTURE_ERROR", "NOT_IMPLEMENTED"])
def test_later_success_cannot_replace_failure_or_gap(status):
    plan = manifest()
    results = passed_results(plan)
    results.append(
        {**results[0], "case_id": "original-failed-attempt", "status": status, "errors": ["original failure"]}
    )
    assert build_report(plan, results).exit_code() == 1


@pytest.mark.parametrize("missing_id", ["lmi-lifecycle-2", "lmi-lifecycle-7"])
def test_missing_disposal_requirement_is_reported_independently(missing_id):
    plan = manifest()
    plan["scenarios"] = [case for case in plan["scenarios"] if case["requirement_id"] != missing_id]
    report = build_report(plan, passed_results(plan))
    assert report.exit_code() == 1
    statuses = {entry.id: entry.status for entry in report.entries}
    assert statuses.pop(missing_id) == ReportStatus.UNCOVERED
    assert all(status == ReportStatus.PASSED for status in statuses.values())


@pytest.mark.parametrize(
    "scope,key,value",
    [
        ("run", "capabilityProfile", "lifecycle-otel"),
        ("run", "coverageProfile", "export-lifecycle"),
        ("case", "scenario", "otel-attribution"),
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
        lambda plan: plan["scenarios"][0].update(slots=1),
        lambda plan: plan["scenarios"][0].update(roles=["victim"]),
        lambda plan: next(case for case in plan["scenarios"] if case["requirement_id"] == "lmi-lifecycle-4").update(
            exclusive=False
        ),
    ],
)
def test_invalid_plan_is_rejected_before_loading_adapter(tmp_path, mutation):
    plan = manifest()
    mutation(plan)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError):
        load_manifest(path)
    with patch("aws_durable_execution_conformance_tests.lmi.app.importlib") as adapter_imports:
        load_adapter = adapter_imports.import_module
        load_adapter.side_effect = AssertionError("Invalid manifest imported an adapter")
        with pytest.raises(ValueError):
            asyncio.run(collect_and_adjudicate(plan, "fixture:create", audit_dir=tmp_path))
        load_adapter.assert_not_called()


def test_valid_plan_can_exceed_native_invocation_concurrency(tmp_path):
    plan = manifest()
    plan["scenarioConcurrency"] = 16
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    assert load_manifest(path) == plan


@pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity"])
def test_collected_evidence_rejects_nonfinite_json_numbers(tmp_path, capsys, literal):
    plan = manifest()
    plan["scenarios"] = plan["scenarios"][:1]
    errors = run_collected_text(tmp_path, plan, '{"result": [' + literal + "]}", capsys)
    assert "JSON numbers must be finite" in errors


def test_evidence_cannot_be_substituted_between_cases_or_artifacts():
    plan = manifest()
    case = plan["scenarios"][0]
    evidence = {
        "caseId": "other-case",
        "runId": "other-run",
        "runtime": "python",
        "requirementId": case["requirement_id"],
        "profile": {"functionArn": "other-function"},
        "artifact": {"sdk": "other-commit"},
    }
    result = adjudicate_collected_evidence(plan, coordinator_capture(plan, {case["case_id"]: evidence}))[0]
    assert result["status"] == "INCOMPLETE_EVIDENCE", result["errors"]
    assert "INCOMPLETE: evidence SDK artifact does not match the deployed revision" in result["errors"]
    assert "INCOMPLETE: evidence caseId does not match the admitted case" in result["errors"]


def test_collected_evidence_must_cover_every_declared_cohort_role():
    """A manifest may declare roles beyond the requirement's defaults."""
    plan = manifest()
    case = next(item for item in plan["scenarios"] if item["scenario"] == "native-overlap")
    plan["scenarios"] = [case]
    case["roles"] = ["victim", "peer", "spare"]
    profile = next(item for item in plan["profiles"] if item["profile_id"] == case["profile_id"])
    evidence = {
        "caseId": case["case_id"],
        "runId": plan["runId"],
        "runtime": plan["runtime"],
        "requirementId": case["requirement_id"],
        "profile": {
            "functionArn": profile["resource_id"],
            "configurationDigest": profile["configuration_id"],
            "nativeConcurrency": profile["native_concurrency"],
        },
        "artifact": {"sdk": profile["artifact_id"]},
        "selectedRequests": {"victim": "request-1", "peer": "request-2"},
    }
    result = adjudicate_collected_evidence(plan, coordinator_capture(plan, {case["case_id"]: evidence}))[0]
    assert result["status"] != "PASSED"
    assert any("declared cohort role" in error for error in result["errors"]), result["errors"]
    evidence["selectedRequests"]["spare"] = "request-3"
    result = adjudicate_collected_evidence(plan, coordinator_capture(plan, {case["case_id"]: evidence}))[0]
    assert not any("declared cohort role" in error for error in result["errors"]), result["errors"]


def test_cli_writes_blocking_reports_for_empty_coverage(tmp_path):
    plan = manifest()
    plan["scenarios"] = []
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    collected = tmp_path / "collected-evidence.json"
    collected.write_text("[]")
    with pytest.raises(SystemExit) as exc:
        run(["--manifest", str(path), "--collected-evidence", str(collected), "--output", str(tmp_path / "out")])
    assert exc.value.code == 1
    report = json.loads((tmp_path / "out/report.json").read_text())
    assert report["summary"]["uncovered"] == 7
    assert report["ci"]["exit_code"] == 1
    assert (tmp_path / "out/report.xml").is_file()


@pytest.mark.parametrize("reason", ["", "handler unavailable"])
def test_cli_reports_not_implemented_with_optional_reason(tmp_path, capsys, reason):
    plan = manifest()
    for case in plan["scenarios"]:
        case["not_implemented"] = reason
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    output = tmp_path / "out"

    # Every case is a declared skip, so the runner never loads the adapter.
    with pytest.raises(SystemExit) as exc:
        run(["--manifest", str(path), "--adapter", "unused_adapter:create", "--output", str(output)])
    assert exc.value.code == 1
    console = capsys.readouterr().out
    assert "Not implemented (declared SDK gap):" in console
    assert "non-blocking" not in console
    assert "Exit code: 1 (fail-on: strict)" in console

    results = json.loads((output / "evidence.json").read_text())
    assert len(results) == len(plan["scenarios"])
    assert all(result["status"] == "NOT_IMPLEMENTED" and result["errors"] == [reason] for result in results)
    report = json.loads((output / "report.json").read_text())
    assert report["summary"]["not_implemented"] == 7
    assert report["summary"]["failed"] == report["summary"]["uncovered"] == 0
    assert report["ci"]["fail_on"] == FAIL_ON_STRICT
    assert report["ci"]["exit_code"] == 1
    assert all(entry["status"] == "NOT_IMPLEMENTED" for entry in report["results"])

    junit = ET.parse(output / "report.xml").getroot()
    assert junit.attrib["tests"] == junit.attrib["skipped"] == "7"
    assert junit.attrib["failures"] == "0"
    for testcase in junit.findall("testcase"):
        skipped = testcase.find("skipped")
        assert skipped is not None
        assert skipped.attrib["message"].startswith("NOT_IMPLEMENTED")


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


@pytest.mark.parametrize(
    "failure",
    [
        None,
        "missing-evidence",
        "collectionErrors",
        "observation_error",
        "control_error",
        "non-json",
        "missing-sdk-completion",
    ],
)
def test_collection_cli_uses_catalog_scheduling_and_persists_before_dispatch(tmp_path, failure):
    import asyncio
    from copy import deepcopy
    from pathlib import Path
    from runpy import run_path
    from types import SimpleNamespace

    from aws_durable_execution_conformance_tests.lmi.app import collect_and_adjudicate

    fixtures = run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))
    value = fixtures["evidence"]()
    if failure != "missing-sdk-completion":
        fixtures["complete_sdk_dispatches"](value)
    if failure == "missing-evidence":
        value.pop("collection")
        value.pop("retirement")
    elif failure == "collectionErrors":
        value["collectionErrors"] = ["SDK request source collection failed"]
    elif failure == "non-json":
        value["diagnostic"] = b"not JSON"
    elif failure not in (None, "missing-sdk-completion"):
        value["events"].append(fixtures["event"](value["attempts"][0], failure, 38))
    finished = []
    plan = manifest()
    plan["runtime"] = value["runtime"]
    plan["scenarios"] = [plan["scenarios"][0], dict(plan["scenarios"][1], not_implemented="fixture skip")]
    scenario = plan["scenarios"][0]
    scenario.update(case_id=value["caseId"], fixtureOptions={"enabled": True})
    profile = plan["profiles"][1]
    profile.update(
        region="fixture-region",
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
            assert context.scenario.roles == ("victim", "peer")
            for attempt in value["attempts"]:
                context.record_attempt(
                    attempt["requestId"],
                    role=attempt["role"],
                    requestId=attempt["requestId"],
                    statusCounts={200: 1},
                )
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
            if "retirement" in value:
                context.evidence["retirement"] = deepcopy(value["retirement"])
            return True

    def factory(received):
        assert received == plan
        return Adapter()

    with patch.dict(sys.modules, {"fixture": SimpleNamespace(create=factory)}):
        results = asyncio.run(collect_and_adjudicate(plan, "fixture:create", audit_dir=tmp_path))
    expected_status = "PASSED" if failure is None else "INFRASTRUCTURE_ERROR"
    assert results[0]["status"] == expected_status, results
    assert finished == [failure in (None, "non-json")]
    if failure not in (None, "non-json"):
        assert any("retirement" in error for error in results[0]["errors"])
    if failure in {"collectionErrors", "observation_error", "control_error"}:
        assert any(failure in error for error in results[0]["errors"])
    audit = [json.loads(line) for line in (tmp_path / "results.jsonl").read_text().splitlines()]
    active = [row for row in audit if row["case_id"] == scenario["case_id"]][-1]
    assert active["status"] == expected_status
    if failure == "non-json":
        assert "Could not serialize case evidence" in active["errors"][-1]
        assert active == results[0]
        assert "evidence" not in active
        assert json.dumps(results)
    else:
        assert len(active["attempts"]) == 2
        assert active["attempts"][0]["statusCounts"] == {"200": 1}
    attempts = [json.loads(line) for line in (tmp_path / "attempts.jsonl").read_text().splitlines()]
    assert attempts[0]["attempts"][0]["statusCounts"] == {"200": 1}
    assert audit[0] == results[1]
    assert audit[0]["status"] == "NOT_IMPLEMENTED"
    assert audit[0]["profile_id"] == plan["scenarios"][1]["profile_id"]
    if failure is None:
        from aws_durable_execution_conformance_tests.lmi.app import _dump_json, adjudicate_collected_evidence

        captured = json.loads((tmp_path / "collected-evidence.json").read_text())
        assert captured[0]["status"] == "COLLECTED"
        assert json.loads(_dump_json(adjudicate_collected_evidence(plan, captured))) == json.loads(_dump_json(results))


def test_collection_cli_round_trips_through_collected_evidence(tmp_path, capsys):
    fixtures = run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))
    value = fixtures["evidence"]()
    fixtures["complete_sdk_dispatches"](value)
    plan = manifest()
    plan["runtime"] = value["runtime"]
    plan["scenarios"] = [plan["scenarios"][0], dict(plan["scenarios"][1], not_implemented="fixture skip")]
    plan["scenarios"][0].update(case_id=value["caseId"], fixtureOptions={"enabled": True})
    plan["profiles"][1].update(
        region="fixture-region",
        resource_id=value["profile"]["functionArn"],
        artifact_id=value["artifact"]["sdk"],
        configuration_id=value["profile"]["configurationDigest"],
    )
    source = tmp_path / "plan.json"
    source.write_text(json.dumps(plan))
    collection, adjudication = tmp_path / "collection", tmp_path / "adjudication"

    class Adapter:
        async def prepare(self, profile):
            pass

        async def finish(self, profile, *, quiescent):
            pass

        async def start(self, context):
            for attempt in value["attempts"]:
                context.record_attempt(attempt["requestId"], role=attempt["role"], requestId=attempt["requestId"])

        async def place(self, context):
            pass

        async def execute(self, context):
            pass

        async def collect(self, context):
            return deepcopy(value)

        async def release(self, context):
            pass

        async def retire(self, context):
            context.evidence["retirement"] = deepcopy(value["retirement"])
            return True

    adapters = {"fixture": SimpleNamespace(create=lambda received: Adapter())}
    with patch.dict(sys.modules, adapters), pytest.raises(SystemExit) as collected:
        run(["--manifest", str(source), "--adapter", "fixture:create", "--output", str(collection)])
    with pytest.raises(SystemExit) as adjudicated:
        run(
            [
                "--manifest",
                str(collection / "manifest.json"),
                "--collected-evidence",
                str(collection / "collected-evidence.json"),
                "--output",
                str(adjudication),
            ]
        )
    capsys.readouterr()

    outputs = {"attempts.jsonl", "results.jsonl", "collected-evidence.json", "evidence.json", "coverage.json"}
    assert outputs | {"manifest.json", "report.json", "report.xml"} <= {path.name for path in collection.iterdir()}
    results = json.loads((collection / "evidence.json").read_text())
    assert [row["status"] for row in results] == ["PASSED", "NOT_IMPLEMENTED"], results
    assert json.loads((adjudication / "evidence.json").read_text()) == results
    assert not outputs & {path.name for path in adjudication.iterdir()} - {"evidence.json", "coverage.json"}
    assert adjudicated.value.code == collected.value.code


@pytest.mark.parametrize("rollback_fails", [False, True])
def test_audit_append_repairs_partial_tail_before_the_next_record(tmp_path, rollback_fails):
    from aws_durable_execution_conformance_tests.lmi.app import _append_audit

    journal = tmp_path / "results.jsonl"
    _append_audit(journal, {"first": True})
    with journal.open("a+b", buffering=0) as stream:
        short_writer = MagicMock(wraps=stream)
        short_writer.__enter__.return_value = short_writer
        short_writer.write.side_effect = lambda row: stream.write(row[:5])
        if rollback_fails:
            short_writer.truncate.side_effect = OSError("rollback failed")
        with patch.object(Path, "open", return_value=short_writer), pytest.raises(OSError):
            _append_audit(journal, {"partial": True})
    _append_audit(journal, {"last": True})
    assert [json.loads(line) for line in journal.read_text().splitlines()] == [{"first": True}, {"last": True}]


@pytest.mark.parametrize("exists", [False, True])
def test_new_journal_and_directories_are_synced_before_dispatch(tmp_path, monkeypatch, exists):
    from aws_durable_execution_conformance_tests.lmi import app

    synced: list[Path] = []
    monkeypatch.setattr(app, "_sync_directory", synced.append)
    audit_dir = tmp_path / "new" / "audit"
    if exists:
        # An earlier attempt may have created the directory without syncing it.
        audit_dir.mkdir(parents=True)
    app._make_durable_directory(audit_dir)
    assert synced == list(audit_dir.parents)
    synced.clear()
    journal = audit_dir / "attempts.jsonl"
    app._append_audit(journal, {"first": True})
    app._append_audit(journal, {"second": True})
    # Every append re-syncs the journal's directory before it can authorize dispatch.
    assert synced == [audit_dir, audit_dir]


def test_directory_sync_is_best_effort_only_for_unwritable_existing_ancestors(tmp_path, monkeypatch):
    from aws_durable_execution_conformance_tests.lmi import app

    def fail_at(parent: Path, error: OSError):
        def sync(path: Path) -> None:
            if path == parent:
                raise error

        return sync

    writable = {"value": False}
    real_access = os.access
    monkeypatch.setattr(
        app.os,
        "access",
        lambda path, mode: writable["value"] if Path(path) == tmp_path.parent else real_access(path, mode),
    )
    # An existing ancestor this process cannot write cannot hold an entry it created.
    monkeypatch.setattr(app, "_sync_directory", fail_at(tmp_path.parent, PermissionError("cannot open")))
    app._make_durable_directory(tmp_path / "kept")
    # A writable ancestor must sync, even if a retry finds the directory already there.
    writable["value"] = True
    with pytest.raises(PermissionError):
        app._make_durable_directory(tmp_path / "kept")
    monkeypatch.setattr(app, "_sync_directory", fail_at(tmp_path.parent, OSError(errno.EIO, "I/O error")))
    with pytest.raises(OSError, match="I/O error"):
        app._make_durable_directory(tmp_path / "kept")
    # tmp_path / "new" is created now, so failing to sync tmp_path is fatal.
    monkeypatch.setattr(app, "_sync_directory", fail_at(tmp_path, PermissionError("cannot open")))
    with pytest.raises(PermissionError):
        app._make_durable_directory(tmp_path / "new" / "audit")


@pytest.mark.skipif(os.name == "nt", reason="Windows cannot fsync a directory")
@pytest.mark.parametrize(("code", "raises"), [(errno.EINVAL, False), (errno.EIO, True)])
def test_directory_fsync_ignores_only_unsupported_operation_errors(tmp_path, monkeypatch, code, raises):
    from aws_durable_execution_conformance_tests.lmi import app

    closed: list[int] = []
    real_close = os.close

    def fsync(descriptor: int) -> None:
        raise OSError(code, os.strerror(code))

    def close(descriptor: int) -> None:
        closed.append(descriptor)
        real_close(descriptor)

    monkeypatch.setattr(app.os, "fsync", fsync)
    monkeypatch.setattr(app.os, "close", close)
    if raises:
        with pytest.raises(OSError):
            app._sync_directory(tmp_path)
    else:
        app._sync_directory(tmp_path)
    assert len(closed) == 1


def test_failed_journal_directory_sync_aborts_the_append(tmp_path, monkeypatch):
    from aws_durable_execution_conformance_tests.lmi import app

    def fail(path: Path) -> None:
        raise OSError("directory sync failed")

    monkeypatch.setattr(app, "_sync_directory", fail)
    with pytest.raises(OSError, match="directory sync failed"):
        app._append_audit(tmp_path / "attempts.jsonl", {"first": True})


@pytest.mark.parametrize("mode", ["collection", "adjudication"])
def test_every_cli_mode_claims_the_output_directory_exclusively(tmp_path, monkeypatch, capsys, mode):
    source = tmp_path / "manifest.json"
    source.write_text(json.dumps(manifest()))
    output = tmp_path / "out"
    output.mkdir()
    # Another run claims the directory after this one saw it empty.
    (output / "manifest.json").write_text(json.dumps(manifest()) + "\n")
    listing = Path.iterdir
    monkeypatch.setattr(Path, "iterdir", lambda path: iter(()) if path == output else listing(path))
    selected = ["--adapter", "unused_module:factory"] if mode == "collection" else ["--collected-evidence", str(source)]
    with pytest.raises(SystemExit) as error:
        run(["--manifest", str(source), "--output", str(output), *selected])
    assert error.value.code == 2
    assert "manifest.json" in capsys.readouterr().err
    assert sorted(path.name for path in listing(output)) == ["manifest.json"]


def test_collection_circular_case_evidence_does_not_abort_other_reports(tmp_path):
    from aws_durable_execution_conformance_tests.lmi.app import collect_and_adjudicate
    from aws_durable_execution_conformance_tests.lmi.coordinator import ScenarioResult

    cyclic: list[Any] = []
    cyclic.append(cyclic)

    async def run_cases(cases, profiles, adapter, *, on_result, **kwargs):
        outcomes = [
            ScenarioResult(
                case.case_id,
                case.requirement_id,
                case.profile_id,
                "COLLECTED",
                [],
                {"diagnostic": cyclic} if index == 0 else {},
                [],
                {},
            )
            for index, case in enumerate(cases)
        ]
        for outcome in outcomes:
            on_result(outcome)
        return outcomes

    plan = manifest()
    plan["scenarios"] = [plan["scenarios"][0], dict(plan["scenarios"][0], case_id="other-case")]
    with (
        patch.dict("sys.modules", {"fixture": SimpleNamespace(create=lambda _: object())}),
        patch("aws_durable_execution_conformance_tests.lmi.coordinator.run_scenarios", side_effect=run_cases),
    ):
        results = asyncio.run(collect_and_adjudicate(plan, "fixture:create", audit_dir=tmp_path))
    assert [result["status"] for result in results] == ["INFRASTRUCTURE_ERROR", "INCOMPLETE_EVIDENCE"]
    assert "Could not serialize case evidence" in results[0]["errors"][0]
    rows = [json.loads(line) for line in (tmp_path / "results.jsonl").read_text().splitlines()]
    assert list({row["case_id"]: row for row in rows}.values()) == results


def test_cancelled_collection_still_adjudicates_emitted_cases(tmp_path):
    import asyncio
    from dataclasses import dataclass
    from types import SimpleNamespace

    from aws_durable_execution_conformance_tests.lmi.app import collect_and_adjudicate

    @dataclass
    class Outcome:
        case_id: str
        requirement_id: str
        profile_id: str
        status: str
        errors: list
        retirement_evidence: dict

    async def run_cases(cases, profiles, adapter, *, on_result, **kwargs):
        for case in cases:
            on_result(Outcome(case.case_id, case.requirement_id, case.profile_id, "COLLECTED", [], {}))
        raise asyncio.CancelledError

    plan = manifest()
    plan["scenarios"] = [plan["scenarios"][0], dict(plan["scenarios"][0], case_id="other-case")]
    with (
        patch.dict("sys.modules", {"fixture": SimpleNamespace(create=lambda _: object())}),
        patch("aws_durable_execution_conformance_tests.lmi.coordinator.run_scenarios", side_effect=run_cases),
        pytest.raises(asyncio.CancelledError),
    ):
        asyncio.run(collect_and_adjudicate(plan, "fixture:create", audit_dir=tmp_path))
    # Cases emitted before the cancellation still get adjudicated final rows.
    audit = [json.loads(line) for line in (tmp_path / "results.jsonl").read_text().splitlines()]
    final = audit[-2:]
    assert {row["case_id"] for row in final} == {plan["scenarios"][0]["case_id"], "other-case"}
    assert [row["status"] for row in final] == ["INCOMPLETE_EVIDENCE", "INCOMPLETE_EVIDENCE"], final
    assert all("INCOMPLETE: no observations collected" in row["errors"] for row in final), final
    captured = json.loads((tmp_path / "collected-evidence.json").read_text())
    assert [row["status"] for row in captured] == ["COLLECTED", "COLLECTED"], captured


def test_predeclared_skip_is_journaled_even_when_active_work_is_cancelled(tmp_path):
    from aws_durable_execution_conformance_tests.lmi.app import collect_and_adjudicate

    async def cancelled(*args, **kwargs):
        raise asyncio.CancelledError

    plan = manifest()
    skipped = dict(plan["scenarios"][1], not_implemented="skip reason")
    plan["scenarios"] = [plan["scenarios"][0], skipped]
    with (
        patch.dict("sys.modules", {"fixture": SimpleNamespace(create=lambda _: object())}),
        patch("aws_durable_execution_conformance_tests.lmi.coordinator.run_scenarios", side_effect=cancelled),
        pytest.raises(asyncio.CancelledError),
    ):
        asyncio.run(collect_and_adjudicate(plan, "fixture:create", audit_dir=tmp_path))
    assert [json.loads(line) for line in (tmp_path / "results.jsonl").read_text().splitlines()] == [
        {
            "case_id": skipped["case_id"],
            "requirement_id": skipped["requirement_id"],
            "profile_id": skipped["profile_id"],
            "status": "NOT_IMPLEMENTED",
            "errors": ["skip reason"],
        }
    ]


def test_empty_adapter_option_is_rejected(tmp_path, capsys):
    source = tmp_path / "manifest.json"
    source.write_text(json.dumps(manifest()))
    with pytest.raises(SystemExit) as error:
        run(["--manifest", str(source), "--output", str(tmp_path / "out"), "--adapter", ""])
    assert error.value.code == 2
    assert "--adapter must be a local Python module:factory" in capsys.readouterr().err


def test_cli_requires_an_evidence_source(tmp_path, capsys):
    source = tmp_path / "manifest.json"
    source.write_text(json.dumps(manifest()))
    with pytest.raises(SystemExit) as error:
        run(["--manifest", str(source), "--output", str(tmp_path / "out")])
    assert error.value.code == 2
    assert "one of the arguments --collected-evidence --adapter is required" in capsys.readouterr().err
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("policy", [None, "reuse-worker", "replace-worker", "reuse"])
def test_unused_profile_recovery_policy_matches_collection_and_adjudication_admission(tmp_path, policy):
    from aws_durable_execution_conformance_tests.lmi.app import collect_and_adjudicate

    plan = manifest()
    plan["scenarios"] = []
    plan["profiles"][0]["recovery_policy"] = policy
    source = tmp_path / "manifest.json"
    source.write_text(json.dumps(plan))
    if policy == "reuse":
        with pytest.raises(ValueError, match="Unknown recovery policy"):
            load_manifest(source)
        with pytest.raises(ValueError, match="Unknown recovery policy"):
            asyncio.run(collect_and_adjudicate(plan, "fixture:create", audit_dir=tmp_path))
    else:
        assert load_manifest(source) == plan
        assert asyncio.run(collect_and_adjudicate(plan, "fixture:create", audit_dir=tmp_path)) == []


def test_collection_skips_the_adapter_when_every_case_is_not_implemented(tmp_path):
    from aws_durable_execution_conformance_tests.lmi.app import collect_and_adjudicate

    plan = manifest()
    plan["scenarios"] = [dict(row, not_implemented="fixture skip") for row in plan["scenarios"]]
    with patch("aws_durable_execution_conformance_tests.lmi.app.importlib") as imports:
        results = asyncio.run(collect_and_adjudicate(plan, "fixture:create", audit_dir=tmp_path))
    imports.import_module.assert_not_called()
    cases = [row["case_id"] for row in plan["scenarios"]]
    assert [(row["case_id"], row["status"]) for row in results] == [(case, "NOT_IMPLEMENTED") for case in cases]
    journal = [json.loads(line) for line in (tmp_path / "results.jsonl").read_text().splitlines()]
    assert sorted(row["case_id"] for row in journal if row["status"] == "NOT_IMPLEMENTED") == sorted(cases)


def test_output_reuse_preserves_previous_run(tmp_path):
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest()))
    collected = tmp_path / "collected-evidence.json"
    collected.write_text("[]")
    output = tmp_path / "out"
    output.mkdir()
    original = output / "evidence.json"
    original.write_text("retained original evidence")
    with pytest.raises(SystemExit) as error:
        run(["--manifest", str(path), "--collected-evidence", str(collected), "--output", str(output)])
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


def timed_plan(requirement: int) -> tuple[dict[str, Any], dict[str, Any]]:
    fixtures = run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))
    value = fixtures["deadline_evidence" if requirement == 3 else "recovery_evidence"]()
    plan = manifest()
    plan["runtime"] = value["runtime"]
    plan["scenarios"] = [case for case in plan["scenarios"] if case["requirement_id"] == value["requirementId"]]
    case = plan["scenarios"][0]
    case.update(
        case_id=value["caseId"],
        behavior_budgets=deepcopy(value["budgets"]),
        roles=list(value["selectedRequests"]),
        slots=2,
    )
    plan["profiles"][1].update(
        resource_id=value["profile"]["functionArn"],
        artifact_id=value["artifact"]["sdk"],
        configuration_id=value["profile"]["configurationDigest"],
    )
    return plan, value


@pytest.mark.parametrize("requirement", [3, 4])
@pytest.mark.parametrize(("declared", "observed", "matches"), [(1, True, False), (1, 1.0, True), (1.0, 1, True)])
def test_standard_extension_budget_binding_preserves_json_types(tmp_path, requirement, declared, observed, matches):
    plan, value = timed_plan(requirement)
    plan["scenarios"][0]["behavior_budgets"]["extensionAllowance"] = declared
    value["budgets"]["extensionAllowance"] = observed
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    admitted = load_manifest(path)

    result = adjudicate_collected_evidence(admitted, coordinator_capture(admitted, {value["caseId"]: value}))[0]

    assert result["status"] == ("PASSED" if matches else "INCOMPLETE_EVIDENCE"), result["errors"]
    if not matches:
        assert any("behavior budgets" in error for error in result["errors"])


@pytest.mark.parametrize("requirement", [3, 4])
@pytest.mark.parametrize("missing", ["all", "admissionMs", "cleanupMs"])
def test_standard_deadline_cases_require_predeclared_budgets(tmp_path, requirement, missing):
    plan, _ = timed_plan(requirement)
    if missing == "all":
        plan["scenarios"][0].pop("behavior_budgets")
    else:
        plan["scenarios"][0]["behavior_budgets"].pop(missing)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError, match="scenario must declare behavior budgets"):
        load_manifest(path)
    with patch("aws_durable_execution_conformance_tests.lmi.app.importlib") as adapter_imports:
        load_adapter = adapter_imports.import_module
        with pytest.raises(ValueError, match="scenario must declare behavior budgets"):
            asyncio.run(collect_and_adjudicate(plan, "fixture:create", audit_dir=tmp_path))
        load_adapter.assert_not_called()


@pytest.mark.parametrize("requirement", [3, 4])
@pytest.mark.parametrize("tightened", [None, "admissionMs", "cleanupMs"])
def test_standard_evidence_cannot_weaken_declared_timing_budgets(tmp_path, requirement, tightened):
    plan, value = timed_plan(requirement)
    if tightened is not None:
        plan["scenarios"][0]["behavior_budgets"][tightened] = 0.5
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    admitted = load_manifest(path)
    result = adjudicate_collected_evidence(admitted, coordinator_capture(admitted, {value["caseId"]: value}))[0]
    assert result["status"] == ("PASSED" if tightened is None else "INCOMPLETE_EVIDENCE"), result
    if tightened is not None:
        assert any("behavior budgets" in error for error in result["errors"])


@pytest.mark.parametrize("requirement", [3, 4])
@pytest.mark.parametrize(("released", "expected_status"), [(230, "INCOMPLETE_EVIDENCE"), (215, "PASSED")])
def test_final_validation_rejects_observed_fault_release_after_retirement(requirement, released, expected_status):
    plan, value = timed_plan(requirement)
    value["faultsCreated"][0]["releasedAtMs"] = released
    assert value["retirement"]["retiredAtMs"] == 220
    assert value["retirement"]["faults"][0]["releasedAtMs"] == 210

    result = adjudicate_collected_evidence(plan, coordinator_capture(plan, {value["caseId"]: value}))[0]

    assert result["status"] == expected_status
    assert ("INCOMPLETE: observed fault release occurred after retirement ended" in result["errors"]) == (
        released == 230
    )
    if released == 215:
        assert result["errors"] == []


@pytest.mark.parametrize("omit_declared", [False, True])
def test_budget_binding_permits_extensions_but_requires_declared_keys(omit_declared):
    plan, value = timed_plan(3)
    plan["scenarios"][0]["behavior_budgets"]["extraBudget"] = 1
    value["budgets"]["adapterMetadata"] = None
    if not omit_declared:
        value["budgets"]["extraBudget"] = 1
    result = adjudicate_collected_evidence(plan, coordinator_capture(plan, {value["caseId"]: value}))[0]
    assert result["status"] == ("INCOMPLETE_EVIDENCE" if omit_declared else "PASSED"), result["errors"]


def test_collected_evidence_preserves_large_integral_decimal_values(tmp_path, capsys):
    plan = manifest()
    plan["scenarios"] = plan["scenarios"][:1]
    results = run_collected_text(tmp_path, plan, '{"result":9007199254740993.0}', capsys)
    assert results[0]["evidence"]["result"] == 9007199254740993


def test_collected_evidence_round_trips_high_precision_json_numbers(tmp_path, capsys):
    plan = manifest()
    plan["scenarios"] = plan["scenarios"][:1]
    raw_number = "0.100000000000000000001"
    results = run_collected_text(tmp_path, plan, '{"result":' + raw_number + "}", capsys)
    assert results[0]["evidence"]["result"] == Decimal(raw_number)


def test_collected_evidence_json_accepts_exact_integral_exponents_without_float_overflow(tmp_path, capsys):
    plan = manifest()
    plan["scenarios"] = plan["scenarios"][:1]
    assert run_collected_text(tmp_path / "large", plan, '{"result":1e400}', capsys)[0]["evidence"]["result"] == 10**400
    assert run_collected_text(tmp_path / "zero", plan, '{"result":0e9999}', capsys)[0]["evidence"]["result"] == 0
    errors = run_collected_text(tmp_path / "huge", plan, '{"result":1e9999}', capsys)
    assert "JSON integral number exceeds the supported digit limit" in errors


def test_manifest_rejects_whitespace_only_extra_role(tmp_path):
    plan = manifest()
    case = next(item for item in plan["scenarios"] if item["requirement_id"] == "lmi-lifecycle-1")
    case["roles"] = ["victim", "peer", " "]
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(plan))
    with pytest.raises(ValueError, match="distinct required cohort roles"):
        load_manifest(path)


@pytest.mark.parametrize("requirement", [3, 4])
@pytest.mark.parametrize("source", ["matching", "evidence", "factory", "scenario"])
def test_standard_collection_timing_budgets_preserve_the_admitted_snapshot(tmp_path, requirement, source):
    plan, value = timed_plan(requirement)
    fixtures = run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))
    fixtures["complete_sdk_dispatches"](value)
    if source != "matching":
        # Keep every declared budget name so the admitted snapshot stays valid
        # and only its values disagree with the evidence.
        budgets = plan["scenarios"][0]["behavior_budgets"]
        plan["scenarios"][0]["behavior_budgets"] = {name: 0.5 for name in budgets}
    original = deepcopy(plan)

    class Adapter:
        async def prepare(self, profile):
            pass

        async def finish(self, profile, *, quiescent):
            pass

        async def start(self, context):
            if source == "scenario":
                context.scenario.behavior_budgets.update(value["budgets"])
            for attempt in value["attempts"]:
                context.record_attempt(attempt["requestId"], role=attempt["role"], requestId=attempt["requestId"])

        async def place(self, context):
            pass

        async def execute(self, context):
            pass

        async def collect(self, context):
            return deepcopy(value)

        async def release(self, context):
            pass

        async def retire(self, context):
            context.evidence["retirement"] = deepcopy(value["retirement"])
            return True

    def factory(received):
        if source == "factory":
            received["scenarios"][0]["behavior_budgets"].update(value["budgets"])
        return Adapter()

    with patch.dict("sys.modules", {"fixture": SimpleNamespace(create=factory)}):
        result = asyncio.run(collect_and_adjudicate(plan, "fixture:create", audit_dir=tmp_path))[0]
    assert plan == original
    if source == "matching":
        assert result["status"] == "PASSED", result
    else:
        assert result["status"] != "PASSED", result
        assert any("behavior budgets" in error for error in result["errors"])


def test_active_retry_requires_an_independent_retirement_audit():
    plan, value = timed_plan(3)
    active = dict(value["attempts"][0], requestId="in-flight", attempt=2, activeAtObservationEnd=True)
    active.pop("endedAtMs")
    active.pop("outcome")
    value["attempts"].append(active)
    (row,) = coordinator_capture(plan, {value["caseId"]: value})
    row["retirement_evidence"] = {}
    result = adjudicate_collected_evidence(plan, [row])[0]
    assert result["status"] == "INCOMPLETE_EVIDENCE", result["errors"]
    assert any("independent retirement audit" in error for error in result["errors"]), result["errors"]
    assert result["evidence"]["attempts"][-1] == active


@pytest.mark.parametrize("matches", [False, True])
def test_collected_evidence_binds_conformance_revision(matches):
    plan, value = timed_plan(3)
    revision = value["artifact"]["conformance"]
    plan["sdk"] = {"deployment": {"conformanceRevision": revision if matches else "wrong-revision"}}

    result = adjudicate_collected_evidence(plan, coordinator_capture(plan, {value["caseId"]: value}))[0]

    assert result["status"] == ("PASSED" if matches else "INCOMPLETE_EVIDENCE"), result["errors"]
    if not matches:
        assert "INCOMPLETE: evidence conformance revision does not match the admitted plan" in result["errors"]


def test_collection_cli_persists_canonical_binding_directory(tmp_path, monkeypatch):
    from aws_durable_execution_conformance_tests.lmi import app
    from aws_durable_execution_conformance_tests.lmi.artifacts import sdk_artifact_directory

    plan = manifest()
    plan["sdk"] = {"artifacts_dir": "case-journals"}
    path = tmp_path / "input.json"
    path.write_text(json.dumps(plan))
    working = tmp_path / "original-cwd"
    working.mkdir()
    output = tmp_path / "test-output"
    monkeypatch.chdir(working)

    async def inspect(manifest, adapter, *, audit_dir):
        expected = str(working / "case-journals")
        assert manifest["sdk"]["artifacts_dir"] == expected
        assert json.loads((audit_dir / "manifest.json").read_text())["sdk"]["artifacts_dir"] == expected
        raise RuntimeError("validated before dispatch")

    monkeypatch.setattr(app, "collect_and_adjudicate", inspect)
    with pytest.raises(RuntimeError, match="validated before dispatch"):
        app.run(["--manifest", str(path), "--adapter", "fixture:adapter", "--output", str(output)])
    monkeypatch.chdir(tmp_path)
    old_plan = {"sdk": {"artifacts_dir": "case-journals"}}
    with pytest.raises(ValueError, match="original location is unresolved"):
        sdk_artifact_directory(output, old_plan)


def test_collection_audit_directory_is_claimed_before_adapter_load(tmp_path):
    from aws_durable_execution_conformance_tests.lmi.app import collect_and_adjudicate

    (tmp_path / "collected-evidence.json").write_text("previous capture")
    with (
        patch("aws_durable_execution_conformance_tests.lmi.app.importlib") as imports,
        pytest.raises(ValueError, match="already contains lifecycle evidence"),
    ):
        asyncio.run(collect_and_adjudicate(manifest(), "fixture:create", audit_dir=tmp_path))
    imports.import_module.assert_not_called()
    assert (tmp_path / "collected-evidence.json").read_text() == "previous capture"
    assert not (tmp_path / "attempts.jsonl").exists()


@pytest.mark.parametrize("existing", ["different-manifest", "earlier-report"])
def test_collection_refuses_a_used_directory_and_releases_its_claim(tmp_path, existing):
    from aws_durable_execution_conformance_tests.lmi.app import collect_and_adjudicate

    plan = manifest()
    if existing == "different-manifest":
        other = deepcopy(plan)
        other["runId"] = "another-run"
        (tmp_path / "manifest.json").write_text(json.dumps(other))
        expected = "different manifest"
    else:
        (tmp_path / "manifest.json").write_text(json.dumps(plan))
        (tmp_path / "evidence.json").write_text("[]")
        expected = "already contains lifecycle evidence"
    before = {path.name: path.read_text() for path in tmp_path.iterdir()}
    with (
        patch("aws_durable_execution_conformance_tests.lmi.app.importlib") as imports,
        pytest.raises(ValueError, match=expected),
    ):
        asyncio.run(collect_and_adjudicate(plan, "fixture:create", audit_dir=tmp_path))
    imports.import_module.assert_not_called()
    assert {path.name: path.read_text() for path in tmp_path.iterdir()} == before


@pytest.mark.parametrize("failure", ["serialization", "write"])
def test_collection_leaves_no_partial_manifest(tmp_path, monkeypatch, failure):
    from aws_durable_execution_conformance_tests.lmi import app

    if failure == "serialization":

        def fail(value, **kwargs):
            raise TypeError("not serializable")

        monkeypatch.setattr(app, "_dump_json", fail)
        expected: type[Exception] = TypeError
    else:
        opened = Path.open

        def open_failing(path, mode="r", *args, **kwargs):
            stream = opened(path, mode, *args, **kwargs)
            if path.name == "manifest.json" and mode == "x":
                stream.write = MagicMock(side_effect=OSError("disk full"))
            return stream

        monkeypatch.setattr(Path, "open", open_failing)
        expected = OSError
    with (
        patch("aws_durable_execution_conformance_tests.lmi.app.importlib") as imports,
        pytest.raises(expected),
    ):
        asyncio.run(app.collect_and_adjudicate(manifest(), "fixture:create", audit_dir=tmp_path))
    imports.import_module.assert_not_called()
    assert list(tmp_path.iterdir()) == []


def test_collection_accepts_caller_owned_artifacts_beside_its_manifest(tmp_path):
    from aws_durable_execution_conformance_tests.lmi.app import collect_and_adjudicate

    plan = manifest()
    (tmp_path / "manifest.json").write_text(json.dumps(plan))
    (tmp_path / "sdk-artifacts").mkdir()
    with (
        patch("aws_durable_execution_conformance_tests.lmi.app.importlib") as imports,
        pytest.raises(RuntimeError, match="adapter loaded"),
    ):
        imports.import_module.side_effect = RuntimeError("adapter loaded")
        asyncio.run(collect_and_adjudicate(plan, "fixture:create", audit_dir=tmp_path))
    imports.import_module.assert_called_once()


def test_adjudication_validates_manifest_admission():
    from aws_durable_execution_conformance_tests.lmi.app import adjudicate_collected_evidence

    plan, _ = timed_plan(3)
    plan["scenarios"][0].pop("behavior_budgets")
    with pytest.raises(ValueError, match="scenario must declare behavior budgets"):
        adjudicate_collected_evidence(plan, [])


@pytest.mark.parametrize("attempts", ["missing", ["not-an-object"]])
def test_adjudication_requires_recorded_attempts(attempts):
    from aws_durable_execution_conformance_tests.lmi.app import adjudicate_collected_evidence

    plan, value = timed_plan(3)
    case = plan["scenarios"][0]
    capture = {
        **{key: case[key] for key in ("case_id", "requirement_id", "profile_id")},
        "status": "COLLECTED",
        "errors": [],
        "evidence": value,
    }
    if attempts != "missing":
        capture["attempts"] = attempts
    result = adjudicate_collected_evidence(plan, [capture])[0]
    assert result["status"] == "INCOMPLETE_EVIDENCE", result["errors"]
    assert "INCOMPLETE: malformed captured evidence: recorded attempts must be an array of objects" in result["errors"]


def test_adjudication_reports_wrong_victim_outcome_as_failed():
    from aws_durable_execution_conformance_tests.lmi.app import adjudicate_collected_evidence

    fixtures = run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))
    value = fixtures["evidence"](7)
    plan = manifest()
    plan["runtime"] = value["runtime"]
    case = next(case for case in plan["scenarios"] if case["scenario"] == "normal-failure")
    plan["scenarios"] = [case]
    case.update(case_id=value["caseId"], roles=list(value["selectedRequests"]))
    profile = plan["profiles"][1]
    profile.update(
        resource_id=value["profile"]["functionArn"],
        configuration_id=value["profile"]["configurationDigest"],
        artifact_id=value["artifact"]["sdk"],
    )
    capture = {
        **{key: case[key] for key in ("case_id", "requirement_id", "profile_id")},
        "status": "COLLECTED",
        "errors": [],
        "evidence": value,
        "attempts": [{**row, "launchedAtMs": 1} for row in value["attempts"]],
    }
    result = adjudicate_collected_evidence(plan, [capture])[0]
    assert result["status"] == "FAILED", result["errors"]
    assert "ASSERTION: normal-failure requires the selected victim outcome FAILED" in result["errors"]


def test_adjudication_accepts_service_retry_proven_during_retirement():
    from aws_durable_execution_conformance_tests.lmi.app import adjudicate_collected_evidence
    from aws_durable_execution_conformance_tests.lmi.evidence import validate_retirement_evidence

    fixtures = run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))
    value, snapshot = fixtures["lifecycle_7_retirement_retry"]()
    plan = manifest()
    plan["runtime"] = value["runtime"]
    case = next(case for case in plan["scenarios"] if case["scenario"] == "normal-failure")
    plan["scenarios"] = [case]
    case.update(case_id=value["caseId"], roles=list(value["selectedRequests"]))
    plan["profiles"][1].update(
        resource_id=value["profile"]["functionArn"],
        configuration_id=value["profile"]["configurationDigest"],
        artifact_id=value["artifact"]["sdk"],
    )
    retry = next(row for row in snapshot["attempts"] if row["requestId"] == "service-retry")
    journal = [{**row, "launchedAtMs": 1} for row in value["attempts"]]
    journal.append(
        {key: retry[key] for key in ("requestId", "executionArn", "role", "attempt")} | {"discoveredAtMs": 207}
    )
    capture = {
        **{key: case[key] for key in ("case_id", "requirement_id", "profile_id")},
        "status": "COLLECTED",
        "errors": [],
        "evidence": value,
        "attempts": journal,
        "retirement_evidence": snapshot,
    }
    assert validate_retirement_evidence(snapshot) == []
    result = adjudicate_collected_evidence(plan, [capture])[0]
    # The CLI selects the shared rules, so a service retry proven during
    # retirement is accepted as the single disposal invocation's successor.
    assert result["status"] == "PASSED", result["errors"]
