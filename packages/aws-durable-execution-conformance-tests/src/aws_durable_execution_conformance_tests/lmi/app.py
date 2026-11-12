# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Run SDK-owned LMI adapters or validate retained evidence with strict coverage."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import math
import os
import sys
from collections.abc import Mapping
from dataclasses import asdict
from pathlib import Path
from typing import Any

from aws_durable_execution_conformance_tests.lmi.catalog import CAPABILITIES, RUNTIMES, requirements
from aws_durable_execution_conformance_tests.report import (
    FAIL_ON_STRICT,
    Report,
    ReportEntry,
    ReportStatus,
    RunMetadata,
)
from aws_durable_execution_conformance_tests.reporters import render_console, write_report


def load_manifest(path: Path) -> dict[str, Any]:
    """Validate the run inventory before loading an adapter or admitting work."""
    manifest = json.loads(path.read_text())
    if (
        not isinstance(manifest, dict)
        or type(manifest.get("schemaVersion")) is not int
        or manifest["schemaVersion"] != 1
    ):
        raise ValueError("manifest.schemaVersion must be 1")
    if manifest.get("runtime") not in RUNTIMES:
        raise ValueError(f"runtime must be one of {RUNTIMES}")
    if manifest.get("capabilityProfile") not in CAPABILITIES:
        raise ValueError(f"capabilityProfile must be one of {CAPABILITIES}")
    if not isinstance(manifest.get("runId"), str) or not manifest["runId"].strip():
        raise ValueError("runId must be a non-empty string")
    profiles = manifest.get("profiles")
    if not isinstance(profiles, list) or len(profiles) < 2:
        raise ValueError("profiles must include persistent native concurrency 1 and 2 targets")
    ids: set[str] = set()
    resources: set[str] = set()
    for profile in profiles:
        if not isinstance(profile, dict):
            raise ValueError("profile must be an object")
        for key in ("profile_id", "resource_id", "artifact_id", "configuration_id"):
            if not isinstance(profile.get(key), str) or not profile[key].strip():
                raise ValueError(f"profile.{key} must be a non-empty string")
        if profile["profile_id"] in ids or profile["resource_id"] in resources:
            raise ValueError("profile IDs and function resources must be unique")
        ids.add(profile["profile_id"])
        resources.add(profile["resource_id"])
        for key in ("capacity", "native_concurrency"):
            if type(profile.get(key)) is not int or profile[key] < 1:
                raise ValueError(f"profile.{key} must be a positive integer")
    if not {1, 2} <= {profile["native_concurrency"] for profile in profiles}:
        raise ValueError("profiles must include native concurrency 1 and 2")
    # Extra profiles must have an incompatibility, even if their concurrency is 1 or 2.
    default_profiles: set[int] = set()
    for profile in profiles:
        concurrency = profile["native_concurrency"]
        if (concurrency not in (1, 2) or concurrency in default_profiles) and (
            not isinstance(profile.get("incompatibilityReason"), str) or not profile["incompatibilityReason"].strip()
        ):
            raise ValueError("additional profiles require an incompatibilityReason")
        default_profiles.add(concurrency)
    count = manifest.get("scenarioConcurrency", 8)
    if type(count) is not int or count < 1:
        raise ValueError("scenarioConcurrency must be a positive integer")
    budgets = manifest.get("budgets", {})
    if not isinstance(budgets, dict):
        raise ValueError("budgets must be an object")
    if set(budgets) - {"provisioning", "lease", "placement", "execution", "collection", "release", "retirement"}:
        raise ValueError("unknown budget phase")
    for value in budgets.values():
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            raise ValueError("budgets must contain finite positive seconds")
    scenarios = manifest.get("scenarios")
    if not isinstance(scenarios, list):
        raise ValueError("scenarios must be a list (empty means uncovered)")
    catalog = requirements(manifest["capabilityProfile"])
    case_ids: set[str] = set()
    for scenario in scenarios:
        if not isinstance(scenario, dict):
            raise ValueError("scenario must be an object")
        for key in ("case_id", "requirement_id", "profile_id", "scenario"):
            if not isinstance(scenario.get(key), str) or not scenario[key].strip():
                raise ValueError(f"scenario.{key} must be a non-empty string")
        if scenario["case_id"] in case_ids:
            raise ValueError("case IDs must be unique; retain retries in the original case evidence")
        case_ids.add(scenario["case_id"])
        if scenario["requirement_id"] not in catalog:
            raise ValueError(f"requirement not applicable to selected capability: {scenario['requirement_id']}")
        if scenario["profile_id"] not in ids:
            raise ValueError(f"unknown profile: {scenario['profile_id']}")
        definition = catalog[scenario["requirement_id"]]
        profile = next(profile for profile in profiles if profile["profile_id"] == scenario["profile_id"])
        if scenario["scenario"] not in definition["requiredScenarios"]:
            raise ValueError("scenario selector does not match its requirement")
        if profile["native_concurrency"] < definition["minimumNativeConcurrency"]:
            raise ValueError("scenario requires a higher deployed native concurrency")
        if "exclusive" in scenario and type(scenario["exclusive"]) is not bool:
            raise ValueError("exclusive must be a boolean")
        if definition["scheduling"] == "exclusive" and scenario.get("exclusive") is False:
            raise ValueError("this measurement requires exclusive resource ownership")
        slots = scenario.get("slots", definition["minimumNativeConcurrency"])
        if type(slots) is not int or not definition["minimumNativeConcurrency"] <= slots <= profile["capacity"]:
            raise ValueError("scenario slots must reserve the required cohort within profile capacity")
        roles = scenario.get("roles", definition["roles"])
        if (
            not isinstance(roles, list)
            or not all(isinstance(role, str) and role for role in roles)
            or len(set(roles)) != len(roles)
            or not set(definition["roles"]) <= set(roles)
        ):
            raise ValueError("scenario must include distinct required cohort roles")
        if scenario.get("otel_view") not in (None, "invocation", "execution"):
            raise ValueError("otel_view must be invocation or execution")
        if "not_implemented" in scenario and (
            not isinstance(scenario["not_implemented"], str) or not scenario["not_implemented"].strip()
        ):
            raise ValueError("not_implemented must explain the missing SDK capability")
    return manifest


def validate(evidence: Mapping[str, Any], *, phase: str = "final") -> list[str]:
    from aws_durable_execution_conformance_tests.lmi.evidence import validate_evidence

    if phase not in ("observations", "final"):
        return ["INCOMPLETE: unknown evidence validation phase"]
    errors = validate_evidence(evidence, phase="observations" if phase == "observations" else "final")
    if evidence.get("requirementId") in ("lmi-lifecycle-7", "lmi-lifecycle-8"):
        try:
            from aws_durable_execution_conformance_tests_otel.lmi import validate_lmi_telemetry
        except ImportError:
            errors.append("INCOMPLETE: install the OTel extension to validate this capability profile")
        else:
            errors.extend(validate_lmi_telemetry(evidence, phase=phase))
    return errors


def _identity_errors(
    manifest: Mapping[str, Any], scenario: Mapping[str, Any], evidence: Mapping[str, Any]
) -> list[str]:
    errors = []
    expected = {
        "runId": manifest["runId"],
        "runtime": manifest["runtime"],
        "caseId": scenario["case_id"],
        "cohortId": scenario.get("cohort_id", "cohort-1"),
        "requirementId": scenario["requirement_id"],
    }
    for key, value in expected.items():
        if evidence.get(key) != value:
            errors.append(f"INCOMPLETE: evidence {key} does not match the admitted case")
    if scenario["scenario"] in ("normal-return", "normal-failure"):
        outcome = "SUCCEEDED" if scenario["scenario"] == "normal-return" else "FAILED"
        selected = evidence.get("selectedRequests", {})
        attempts = evidence.get("attempts", [])
        if (
            not isinstance(selected, Mapping)
            or not isinstance(attempts, list)
            or not any(
                isinstance(attempt, Mapping)
                and attempt.get("requestId") == selected.get("victim")
                and attempt.get("outcome") == outcome
                for attempt in attempts
            )
        ):
            errors.append(f"ASSERTION: {scenario['scenario']} requires the selected victim outcome {outcome}")
    profile = next(profile for profile in manifest["profiles"] if profile["profile_id"] == scenario["profile_id"])
    observed = evidence.get("profile", {})
    artifact = evidence.get("artifact", {})
    if not isinstance(observed, Mapping) or not isinstance(artifact, Mapping):
        return [*errors, "INCOMPLETE: artifact/profile identity is missing"]
    for key, value in {
        "functionArn": profile["resource_id"],
        "configurationDigest": profile["configuration_id"],
        "nativeConcurrency": profile["native_concurrency"],
    }.items():
        if observed.get(key) != value:
            errors.append(f"INCOMPLETE: evidence profile.{key} does not match the deployed profile")
    if artifact.get("sdk") != profile["artifact_id"]:
        errors.append("INCOMPLETE: evidence SDK artifact does not match the deployed revision")
    return errors


def evidence_results(manifest: Mapping[str, Any], base: Path) -> list[dict[str, Any]]:
    results = []
    for scenario in manifest["scenarios"]:
        result: dict[str, Any] = {"case_id": scenario["case_id"], "requirement_id": scenario["requirement_id"]}
        if "not_implemented" in scenario:
            result.update(status="NOT_IMPLEMENTED", errors=[scenario["not_implemented"]])
        elif not scenario.get("evidence_file"):
            result.update(status="INCOMPLETE_EVIDENCE", errors=["INCOMPLETE: missing evidence_file"])
        else:
            try:
                evidence = json.loads((base / scenario["evidence_file"]).read_text())
                if not isinstance(evidence, dict):
                    raise ValueError("evidence must be an object")
                errors = _identity_errors(manifest, scenario, evidence) + validate(evidence)
                result.update(evidence=evidence, errors=errors, status="FAILED" if errors else "PASSED")
            except (OSError, ValueError, TypeError) as exc:
                result.update(status="INCOMPLETE_EVIDENCE", errors=[f"INCOMPLETE: {exc}"])
        results.append(result)
    return results


def _append_audit(path: Path, record: Mapping[str, Any]) -> None:
    """Persist before dispatch; sink failure must abort the launch."""
    with path.open("a") as stream:
        stream.write(json.dumps(record) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


async def live_results(manifest: Mapping[str, Any], adapter_name: str, *, audit_dir: Path) -> list[dict[str, Any]]:
    from aws_durable_execution_conformance_tests.lmi.coordinator import Budgets, Profile, Scenario, run_scenarios

    module_name, separator, factory_name = adapter_name.partition(":")
    if not separator or not factory_name:
        raise ValueError("--adapter must be a local Python module:factory")
    factory = getattr(importlib.import_module(module_name), factory_name)
    adapter = factory(manifest)
    profiles = [
        Profile(**{key: value for key, value in profile.items() if key != "incompatibilityReason"})
        for profile in manifest["profiles"]
    ]
    active = [scenario for scenario in manifest["scenarios"] if "not_implemented" not in scenario]
    catalog = requirements(manifest["capabilityProfile"])
    scenarios = []
    for scenario in active:
        definition = catalog[scenario["requirement_id"]]
        configured = {key: value for key, value in scenario.items() if key != "evidence_file"}
        configured.setdefault("exclusive", definition["scheduling"] == "exclusive")
        configured.setdefault("slots", definition["minimumNativeConcurrency"])
        configured.setdefault("roles", definition["roles"])
        scenarios.append(Scenario(**configured))
    expected = {scenario["case_id"]: scenario for scenario in active}

    def bound_validator(evidence: Mapping[str, Any], *, phase: str = "final") -> list[str]:
        scenario = expected.get(evidence.get("caseId", ""))
        if scenario is None:
            return ["INCOMPLETE: adapter returned evidence for an unadmitted case"]
        return _identity_errors(manifest, scenario, evidence) + validate(evidence, phase=phase)

    outcomes = await run_scenarios(
        scenarios,
        profiles,
        adapter,
        run_id=manifest["runId"],
        validator=bound_validator,
        scenario_concurrency=manifest.get("scenarioConcurrency", 8),
        budgets=Budgets(**manifest.get("budgets", {})),
        on_attempt=lambda context: _append_audit(
            audit_dir / "attempts.jsonl",
            {
                "runId": context.run_id,
                "caseId": context.scenario.case_id,
                "profileId": context.profile.profile_id,
                "attempts": context.attempts,
            },
        ),
        on_result=lambda result: _append_audit(audit_dir / "results.jsonl", asdict(result)),
    )
    results = [asdict(outcome) for outcome in outcomes]
    results.extend(
        {
            "case_id": scenario["case_id"],
            "requirement_id": scenario["requirement_id"],
            "status": "NOT_IMPLEMENTED",
            "errors": [scenario["not_implemented"]],
        }
        for scenario in manifest["scenarios"]
        if "not_implemented" in scenario
    )
    return results


def build_report(manifest: Mapping[str, Any], results: list[dict[str, Any]]) -> Report:
    report = Report(
        run=RunMetadata(
            name=manifest["runId"],
            template="LMI persistent profiles",
            region=manifest.get("region", ""),
            language=manifest["runtime"],
            suites=["lmi-lifecycle"],
        ),
        fail_on=FAIL_ON_STRICT,
    )
    for requirement_id, requirement in requirements(manifest["capabilityProfile"]).items():
        cases = [result for result in results if result.get("requirement_id") == requirement_id]
        status = ReportStatus.PASSED
        errors: list[str] = []
        if not cases:
            status = ReportStatus.UNCOVERED
        elif all(case["status"] == "NOT_IMPLEMENTED" for case in cases):
            status = ReportStatus.NOT_IMPLEMENTED
            errors = [f"{case['case_id']}: {error}" for case in cases for error in case.get("errors", [])]
        elif any(case["status"] != "PASSED" for case in cases):
            status = ReportStatus.FAILED
            errors = [
                f"{case['case_id']} [{case['status']}]: {error}"
                for case in cases
                if case["status"] != "PASSED"
                for error in case.get("errors", []) or ["case did not pass"]
            ]
        expected_cases = {
            scenario["case_id"] for scenario in manifest["scenarios"] if scenario["requirement_id"] == requirement_id
        }
        missing_cases = expected_cases - {case["case_id"] for case in cases}
        if missing_cases and status == ReportStatus.PASSED:
            status = ReportStatus.UNCOVERED
            errors.append(f"Missing admitted cases: {', '.join(sorted(missing_cases))}")
        configured = {
            scenario["scenario"] for scenario in manifest["scenarios"] if scenario["requirement_id"] == requirement_id
        }
        missing = set(requirement["requiredScenarios"]) - configured
        if missing and status == ReportStatus.PASSED:
            status = ReportStatus.UNCOVERED
            errors.append(f"Missing mandatory scenarios: {', '.join(sorted(missing))}")
        report.add(
            ReportEntry(
                id=requirement_id,
                suite="lmi-lifecycle",
                status=status,
                description=requirement["description"],
                errors=errors,
                reason="; ".join(errors) if status == ReportStatus.NOT_IMPLEMENTED else None,
            )
        )
    if manifest["capabilityProfile"] == "lifecycle":
        report.warnings.append(
            "lmi-lifecycle-7 and -8 are not applicable to the lifecycle-only profile; OTel is not accepted."
        )
    return report


def run(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--adapter", help="SDK-owned local module:factory; omit to validate retained evidence")
    parser.add_argument("--output", type=Path, default=Path("lmi-output"))
    args = parser.parse_args(argv)
    try:
        manifest = load_manifest(args.manifest)
        args.output.mkdir(parents=True, exist_ok=True)
        if any(args.output.iterdir()):
            raise ValueError("output directory must be empty; preserve previous run evidence")
        (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        results = (
            asyncio.run(live_results(manifest, args.adapter, audit_dir=args.output))
            if args.adapter
            else evidence_results(manifest, args.manifest.parent)
        )
        report = build_report(manifest, results)
    except (OSError, ValueError, TypeError, ImportError, AttributeError) as exc:
        parser.error(str(exc))
    (args.output / "evidence.json").write_text(json.dumps(results, indent=2) + "\n")
    print(render_console(report))
    for report_format in ("json", "junit"):
        write_report(report, report_format, str(args.output / "report"))
    sys.exit(report.exit_code())


if __name__ == "__main__":
    run()
