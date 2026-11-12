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
from copy import deepcopy
from dataclasses import asdict
from functools import lru_cache
from pathlib import Path
from typing import Any

from aws_durable_execution_conformance_tests.lmi.catalog import CAPABILITIES, RUNTIMES, requirements
from aws_durable_execution_conformance_tests.lmi.matrix import CaseRequirement, missing_cases, required_cases
from aws_durable_execution_conformance_tests.report import (
    FAIL_ON_STRICT,
    Report,
    ReportEntry,
    ReportStatus,
    RunMetadata,
)
from aws_durable_execution_conformance_tests.reporters import render_console, write_report


def _catalog(manifest: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return requirements(
        manifest["capabilityProfile"],
        runtime=manifest["runtime"],
        coverage_profile=manifest.get("coverageProfile", "standard"),
    )


@lru_cache(maxsize=16)
def _profile_cases(runtime: str, capability: str, profile: str) -> tuple[CaseRequirement, ...]:
    # Installed requirement resources are immutable during a run.
    return tuple(required_cases(runtime, capability, profile))


def _required(manifest: Mapping[str, Any]) -> list[CaseRequirement]:
    return list(
        _profile_cases(manifest["runtime"], manifest["capabilityProfile"], manifest.get("coverageProfile", "standard"))
    )


def _case_definition(
    manifest: Mapping[str, Any],
    scenario: Mapping[str, Any],
    profile: Mapping[str, Any],
) -> CaseRequirement:
    for required in _required(manifest):
        if required.requirement_id != scenario["requirement_id"] or required.scenario != scenario["scenario"]:
            continue
        if required.native_concurrency is not None and (
            required.native_concurrency != profile["native_concurrency"]
            or required.otel_view != scenario.get("otel_view")
        ):
            continue
        return required
    raise ValueError("scenario, native concurrency and OTel view do not match the selected coverage matrix")


def coverage_inventory(manifest: Mapping[str, Any]) -> dict[str, Any]:
    profiles = {profile["profile_id"]: profile for profile in manifest["profiles"]}
    observed = [
        (
            case["requirement_id"],
            case["scenario"],
            profiles[case["profile_id"]]["native_concurrency"],
            case.get("otel_view") if manifest.get("coverageProfile", "standard") != "standard" else None,
        )
        for case in manifest["scenarios"]
    ]
    required = _required(manifest)
    missing = missing_cases(required, observed)
    return {
        "coverageProfile": manifest.get("coverageProfile", "standard"),
        "required": [asdict(case) for case in required],
        "missing": [asdict(case) for case in missing],
    }


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
    # Resolve the profile before any adapter import, deployment or admission.
    _required(manifest)
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
        if manifest.get("coverageProfile", "standard") == "python-lifecycle" and profile.get("recovery_policy") not in (
            "reuse-worker",
            "replace-worker",
        ):
            raise ValueError("Python lifecycle profiles require an explicit recovery_policy")
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
    catalog = _catalog(manifest)
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
        profile = next(profile for profile in profiles if profile["profile_id"] == scenario["profile_id"])
        definition = _case_definition(manifest, scenario, profile)
        if profile["native_concurrency"] < definition.minimum_native_concurrency:
            raise ValueError("scenario requires a higher deployed native concurrency")
        if "exclusive" in scenario and type(scenario["exclusive"]) is not bool:
            raise ValueError("exclusive must be a boolean")
        if definition.exclusive and scenario.get("exclusive") is False:
            raise ValueError("this measurement requires exclusive resource ownership")
        slots = scenario.get("slots", definition.minimum_native_concurrency)
        if type(slots) is not int or not definition.minimum_native_concurrency <= slots <= profile["capacity"]:
            raise ValueError("scenario slots must reserve the required cohort within profile capacity")
        roles = scenario.get("roles", list(definition.roles))
        if (
            not isinstance(roles, list)
            or not all(isinstance(role, str) and role for role in roles)
            or len(set(roles)) != len(roles)
            or not set(definition.roles) <= set(roles)
        ):
            raise ValueError("scenario must include distinct required cohort roles")
        if scenario.get("otel_view") not in (None, "invocation", "execution"):
            raise ValueError("otel_view must be invocation or execution")
        if "coverage_profile" in scenario:
            raise ValueError("coverageProfile is a run-level property and cannot be overridden by a case")
        behavior_budgets = scenario.get("behavior_budgets", {})
        if not isinstance(behavior_budgets, dict):
            raise ValueError("behavior_budgets must be an object")
        if not set(definition.budget_names) <= set(behavior_budgets):
            raise ValueError(f"scenario must declare behavior budgets: {', '.join(definition.budget_names)}")
        for name, value in behavior_budgets.items():
            nonnegative = name in {"maxThreadGrowth", "maxFdGrowth", "maxRssGrowthBytes"}
            if (
                type(value) not in (int, float)
                or not math.isfinite(value)
                or value < 0
                or (not nonnegative and value == 0)
            ):
                raise ValueError("behavior budgets must be finite positive numbers (resource growth may be zero)")
        if "not_implemented" in scenario and (
            not isinstance(scenario["not_implemented"], str) or not scenario["not_implemented"].strip()
        ):
            raise ValueError("not_implemented must explain the missing SDK capability")
    return manifest


def validate(evidence: Mapping[str, Any], *, phase: str = "final") -> list[str]:
    from aws_durable_execution_conformance_tests.lmi.evidence import validate_common_evidence, validate_evidence

    if phase not in ("observations", "final"):
        return ["INCOMPLETE: unknown evidence validation phase"]
    profile = evidence.get("coverageProfile", "standard")
    if profile == "python-lifecycle":
        from aws_durable_execution_conformance_tests.lmi.lifecycle_regressions import validate_lifecycle_regressions

        errors = validate_common_evidence(
            evidence,
            phase="observations" if phase == "observations" else "final",
            requirement_ids=tuple(f"lmi-lifecycle-{index}" for index in range(1, 16)),
        )
        errors.extend(validate_lifecycle_regressions(evidence, phase=phase))
        needs_otel = evidence.get("otelView") is not None
    elif profile == "standard":
        errors = validate_evidence(evidence, phase="observations" if phase == "observations" else "final")
        needs_otel = evidence.get("requirementId") in ("lmi-lifecycle-7", "lmi-lifecycle-8")
    else:
        return ["INCOMPLETE: unknown evidence coverage profile"]
    if needs_otel:
        try:
            if profile == "python-lifecycle":
                from aws_durable_execution_conformance_tests_otel.lmi_regressions import validate_lifecycle_telemetry

                errors.extend(validate_lifecycle_telemetry(evidence, phase=phase))
            else:
                from aws_durable_execution_conformance_tests_otel.lmi import validate_lmi_telemetry

                errors.extend(validate_lmi_telemetry(evidence, phase=phase))
        except ImportError:
            errors.append("INCOMPLETE: install the OTel extension to validate this capability profile")
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
    coverage_profile = manifest.get("coverageProfile", "standard")
    if evidence.get("coverageProfile", "standard") != coverage_profile:
        errors.append("INCOMPLETE: evidence coverageProfile does not match the admitted run")
    if coverage_profile == "python-lifecycle":
        if evidence.get("scenario") != scenario["scenario"]:
            errors.append("INCOMPLETE: evidence scenario does not match the admitted variant")
        if evidence.get("otelView") != scenario.get("otel_view"):
            errors.append("INCOMPLETE: evidence OTel view does not match the admitted matrix slot")
        regression = evidence.get("regression")
        if not isinstance(regression, Mapping) or regression.get("budgets") != scenario.get("behavior_budgets", {}):
            errors.append("INCOMPLETE: observed behavior budgets do not match the declared case budgets")
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
    if coverage_profile == "python-lifecycle" and observed.get("recoveryPolicy") != profile.get("recovery_policy"):
        errors.append("INCOMPLETE: observed recovery policy does not match the declared profile")
    return errors


def evidence_results(manifest: Mapping[str, Any], base: Path) -> list[dict[str, Any]]:
    results = []
    for scenario in manifest["scenarios"]:
        result: dict[str, Any] = {
            "case_id": scenario["case_id"],
            "requirement_id": scenario["requirement_id"],
            "profile_id": scenario["profile_id"],
        }
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

    # Validation keeps its own snapshot. Adapter and Scenario objects must not
    # be able to change the expected budgets, profile, or coverage after admission.
    manifest = deepcopy(dict(manifest))
    module_name, separator, factory_name = adapter_name.partition(":")
    if not separator or not factory_name:
        raise ValueError("--adapter must be a local Python module:factory")
    factory = getattr(importlib.import_module(module_name), factory_name)
    adapter = factory(deepcopy(manifest))
    profiles = [
        Profile(**{key: value for key, value in profile.items() if key != "incompatibilityReason"})
        for profile in manifest["profiles"]
    ]
    active = [scenario for scenario in manifest["scenarios"] if "not_implemented" not in scenario]
    profile_map = {profile["profile_id"]: profile for profile in manifest["profiles"]}
    scenarios = []
    for scenario in active:
        definition = _case_definition(manifest, scenario, profile_map[scenario["profile_id"]])
        configured = deepcopy({key: value for key, value in scenario.items() if key != "evidence_file"})
        configured.setdefault("exclusive", definition.exclusive)
        configured.setdefault("slots", definition.minimum_native_concurrency)
        configured.setdefault("roles", list(definition.roles))
        configured["coverage_profile"] = manifest.get("coverageProfile", "standard")
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
    declared = {case["case_id"]: case for case in manifest["scenarios"]}
    seen: set[str] = set()
    integrity_errors = []
    for result in results:
        case_id = result.get("case_id")
        case = declared.get(case_id) if isinstance(case_id, str) else None
        if not isinstance(case_id, str) or case is None or case_id in seen:
            integrity_errors.append(f"Unadmitted or duplicate result: {case_id}")
        else:
            seen.add(case_id)
            if (
                result.get("requirement_id") != case["requirement_id"]
                or result.get("profile_id", case["profile_id"]) != case["profile_id"]
            ):
                integrity_errors.append(f"Result identity does not match admitted case {case_id}")
        if result.get("status") == "PASSED" and result.get("errors"):
            integrity_errors.append(f"Passing result retains assertion errors: {case_id}")
    if integrity_errors:
        report.add(
            ReportEntry(
                id="lmi-run",
                suite="lmi-lifecycle",
                status=ReportStatus.FAILED,
                description="Run result ownership and consistency",
                errors=integrity_errors,
            )
        )
    inventory = coverage_inventory(manifest)
    for requirement_id, requirement in _catalog(manifest).items():
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
        missing = [case for case in inventory["missing"] if case["requirement_id"] == requirement_id]
        if missing:
            errors.extend(
                f"Missing mandatory matrix slot: {case['scenario']} / native={case['native_concurrency']} / "
                f"OTel={case['otel_view'] or 'disabled'}"
                for case in missing
            )
            if status == ReportStatus.PASSED:
                status = ReportStatus.UNCOVERED
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
    report.warnings.append(
        f"Coverage profile: {inventory['coverageProfile']}; "
        f"{len(inventory['required'])} mandatory matrix slots, {len(inventory['missing'])} absent from the manifest."
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
    (args.output / "coverage.json").write_text(json.dumps(coverage_inventory(manifest), indent=2) + "\n")
    print(render_console(report))
    for report_format in ("json", "junit"):
        write_report(report, report_format, str(args.output / "report"))
    sys.exit(report.exit_code())


if __name__ == "__main__":
    run()
