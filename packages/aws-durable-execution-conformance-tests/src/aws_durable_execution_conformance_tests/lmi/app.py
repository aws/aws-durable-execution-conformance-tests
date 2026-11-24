# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Validate retained LMI lifecycle evidence with strict coverage."""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from itertools import combinations
from pathlib import Path
from typing import Any

from aws_durable_execution_conformance_tests.lmi.catalog import CAPABILITIES, COVERAGE_PROFILES, RUNTIMES, requirements
from aws_durable_execution_conformance_tests.lmi.evidence import EvidenceValidator, is_json_integer, same_json
from aws_durable_execution_conformance_tests.lmi.matrix import CaseRequirement, missing_cases, required_cases
from aws_durable_execution_conformance_tests.report import (
    FAIL_ON_STRICT,
    Report,
    ReportEntry,
    ReportStatus,
    RunMetadata,
)
from aws_durable_execution_conformance_tests.reporters import render_console, write_report


@dataclass(frozen=True)
class SuiteDefinition:
    """Caller-owned rules using the common coordinator, evidence and reporting.

    Passing a definition is an explicit programmatic API operation. It does not add
    requirements or a coverage profile to the shared CLI/catalog or its wheels.
    The adapter only receives a separate copy of the run manifest.
    """

    coverage_profile: str
    suite_name: str
    requirements: Mapping[str, Mapping[str, Any]]
    cases: tuple[CaseRequirement, ...]
    validator: EvidenceValidator
    evidence_policy_key: str | None = None
    nonnegative_budgets: frozenset[str] = frozenset()


def _check_suite(manifest: Mapping[str, Any], suite: SuiteDefinition) -> None:
    if not suite.coverage_profile or manifest.get("coverageProfile", "standard") != suite.coverage_profile:
        raise ValueError("suite definition does not match the declared coverageProfile")
    if not suite.suite_name:
        raise ValueError("suite definition must have a report suite name")
    if suite.coverage_profile in COVERAGE_PROFILES:
        raise ValueError("caller-owned rules cannot replace a shared coverage profile")
    if suite.suite_name == "lmi-lifecycle" or any(
        not isinstance(name, str) or name.startswith("lmi-lifecycle-") for name in suite.requirements
    ):
        raise ValueError("caller-owned rules must use their own report and requirement namespace")
    if not suite.cases:
        raise ValueError("suite definition must declare at least one mandatory case")
    if any(case.requirement_id not in suite.requirements for case in suite.cases):
        raise ValueError("every case requirement must exist in the suite requirement catalog")

    for left, right in combinations(suite.cases, 2):
        concurrency = left.native_concurrency if left.native_concurrency is not None else right.native_concurrency
        if concurrency is None:
            concurrency = max(left.minimum_native_concurrency, right.minimum_native_concurrency)
        key = (left.requirement_id, left.scenario, concurrency)
        if left.matches(key) and right.matches(key):
            raise ValueError("suite case definitions overlap in the coverage matrix")


def _catalog(manifest: Mapping[str, Any], suite: SuiteDefinition | None = None) -> dict[str, dict[str, Any]]:
    if suite is not None:
        _check_suite(manifest, suite)
        return {key: deepcopy(dict(value)) for key, value in suite.requirements.items()}
    return requirements(
        manifest["capabilityProfile"],
        runtime=manifest["runtime"],
        coverage_profile=manifest.get("coverageProfile", "standard"),
    )


@lru_cache(maxsize=16)
def _profile_cases(runtime: str, capability: str, profile: str) -> tuple[CaseRequirement, ...]:
    # Installed requirement resources are immutable during a run.
    return tuple(required_cases(runtime, capability, profile))


def _required(manifest: Mapping[str, Any], suite: SuiteDefinition | None = None) -> list[CaseRequirement]:
    if suite is not None:
        _check_suite(manifest, suite)
        return list(suite.cases)
    return list(
        _profile_cases(manifest["runtime"], manifest["capabilityProfile"], manifest.get("coverageProfile", "standard"))
    )


def _case_definition(
    manifest: Mapping[str, Any],
    scenario: Mapping[str, Any],
    profile: Mapping[str, Any],
    suite: SuiteDefinition | None = None,
) -> CaseRequirement:
    key = (scenario["requirement_id"], scenario["scenario"], profile["native_concurrency"])
    for required in _required(manifest, suite):
        if required.matches(key):
            return required
    raise ValueError("scenario and native concurrency do not match the selected coverage matrix")


def coverage_inventory(manifest: Mapping[str, Any], *, suite: SuiteDefinition | None = None) -> dict[str, Any]:
    profiles = {profile["profile_id"]: profile for profile in manifest["profiles"]}
    observed = [
        (
            case["requirement_id"],
            case["scenario"],
            profiles[case["profile_id"]]["native_concurrency"],
        )
        for case in manifest["scenarios"]
    ]
    required = _required(manifest, suite)
    missing = missing_cases(required, observed)
    return {
        "coverageProfile": manifest.get("coverageProfile", "standard"),
        "required": [asdict(case) for case in required],
        "missing": [asdict(case) for case in missing],
    }


def _finite_budget(value: Any) -> bool:
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def _validate_behavior_budgets(
    scenario: Mapping[str, Any], definition: CaseRequirement, suite: SuiteDefinition | None
) -> None:
    behavior_budgets = scenario.get("behavior_budgets", {})
    if not isinstance(behavior_budgets, dict):
        raise ValueError("behavior_budgets must be an object")
    if not set(definition.budget_names) <= set(behavior_budgets):
        raise ValueError(f"scenario must declare behavior budgets: {', '.join(definition.budget_names)}")
    for name, value in behavior_budgets.items():
        nonnegative = suite is not None and name in suite.nonnegative_budgets
        if not _finite_budget(value) or value < 0 or (not nonnegative and value == 0):
            raise ValueError("behavior budgets must be finite positive numbers (resource growth may be zero)")


def _finite_json_float(raw: str) -> int | float | Decimal:
    try:
        exact = Decimal(raw)
    except InvalidOperation as error:
        raise ValueError("JSON number cannot be represented without precision loss") from error
    if not exact.is_finite():
        raise ValueError("JSON numbers must be finite")
    if exact == exact.to_integral_value():
        limit = sys.get_int_max_str_digits() or sys.int_info.default_max_str_digits
        if exact and exact.adjusted() >= limit:
            raise ValueError("JSON integral number exceeds the supported digit limit")
        return int(exact)
    value = float(exact)
    return value if math.isfinite(value) and exact == Decimal(str(value)) else exact


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(), parse_float=_finite_json_float, parse_constant=_finite_json_float)


def _dump_json(value: Any, depth: int = 0) -> str:
    """Write retained JSON without rounding exact decimal payloads."""
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("JSON numbers must be finite")
        return str(value)
    if not isinstance(value, (dict, list, tuple)) or not value:
        return json.dumps(value, allow_nan=False)
    inner = "\n" + "  " * (depth + 1)
    outer = "\n" + "  " * depth
    if isinstance(value, dict):
        items = [f"{json.dumps(key)}: {_dump_json(item, depth + 1)}" for key, item in value.items()]
        return "{" + inner + ("," + inner).join(items) + outer + "}"
    return "[" + inner + ("," + inner).join(_dump_json(item, depth + 1) for item in value) + outer + "]"


def load_manifest(path: Path, *, suite: SuiteDefinition | None = None) -> dict[str, Any]:
    """Validate the run inventory before loading an adapter or admitting work."""
    manifest = _read_json(path)
    if (
        not isinstance(manifest, dict)
        or not is_json_integer(manifest.get("schemaVersion"))
        or manifest["schemaVersion"] != 1
    ):
        raise ValueError("manifest.schemaVersion must be 1")
    manifest["schemaVersion"] = int(manifest["schemaVersion"])
    if manifest.get("runtime") not in RUNTIMES:
        raise ValueError(f"runtime must be one of {RUNTIMES}")
    if manifest.get("capabilityProfile") not in CAPABILITIES:
        raise ValueError(f"capabilityProfile must be one of {CAPABILITIES}")
    if not isinstance(manifest.get("runId"), str) or not manifest["runId"].strip():
        raise ValueError("runId must be a non-empty string")
    # Resolve the profile before any adapter import, deployment or admission.
    _required(manifest, suite)
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
            if not is_json_integer(profile.get(key)) or profile[key] < 1:
                raise ValueError(f"profile.{key} must be a positive integer")
            profile[key] = int(profile[key])
    if not {1, 2} <= {profile["native_concurrency"] for profile in profiles}:
        raise ValueError("profiles must include native concurrency 1 and 2")
    # A reason identifies an additional target regardless of inventory order.
    default_profiles: set[int] = set()
    for profile in profiles:
        concurrency = profile["native_concurrency"]
        reason = profile.get("incompatibilityReason")
        if reason is not None:
            if not isinstance(reason, str) or not reason.strip():
                raise ValueError("incompatibilityReason must be a non-empty string")
        elif concurrency not in (1, 2) or concurrency in default_profiles:
            raise ValueError("additional profiles require an incompatibilityReason")
        else:
            default_profiles.add(concurrency)
    if default_profiles != {1, 2}:
        raise ValueError("profiles must include default native concurrency 1 and 2 targets")
    count = manifest.get("scenarioConcurrency", 8)
    if not is_json_integer(count) or count < 1:
        raise ValueError("scenarioConcurrency must be a positive integer")
    if "scenarioConcurrency" in manifest:
        manifest["scenarioConcurrency"] = int(count)
    budgets = manifest.get("budgets", {})
    if not isinstance(budgets, dict):
        raise ValueError("budgets must be an object")
    if set(budgets) - {"provisioning", "lease", "placement", "execution", "collection", "release", "retirement"}:
        raise ValueError("unknown budget phase")
    for value in budgets.values():
        if not _finite_budget(value) or value <= 0:
            raise ValueError("budgets must contain finite positive seconds")
    scenarios = manifest.get("scenarios")
    if not isinstance(scenarios, list):
        raise ValueError("scenarios must be a list (empty means uncovered)")
    catalog = _catalog(manifest, suite)
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
        definition = _case_definition(manifest, scenario, profile, suite)
        if profile["native_concurrency"] < definition.minimum_native_concurrency:
            raise ValueError("scenario requires a higher deployed native concurrency")
        if "exclusive" in scenario and type(scenario["exclusive"]) is not bool:
            raise ValueError("exclusive must be a boolean")
        if definition.exclusive and scenario.get("exclusive") is False:
            raise ValueError("this measurement requires exclusive resource ownership")
        slots = scenario.get("slots", definition.minimum_native_concurrency)
        if "slots" in scenario and is_json_integer(slots):
            slots = scenario["slots"] = int(slots)
        if type(slots) is not int or not definition.minimum_native_concurrency <= slots <= profile["capacity"]:
            raise ValueError("scenario slots must reserve the required cohort within profile capacity")
        roles = scenario.get("roles", list(definition.roles))
        if (
            not isinstance(roles, list)
            or not all(isinstance(role, str) and role.strip() for role in roles)
            or len(set(roles)) != len(roles)
            or not set(definition.roles) <= set(roles)
        ):
            raise ValueError("scenario must include distinct required cohort roles")
        if "otel_view" in scenario or "export_mode" in scenario:
            raise ValueError("telemetry options are not supported by the LMI lifecycle runner")
        if "coverage_profile" in scenario:
            raise ValueError("coverageProfile is a run-level property and cannot be overridden by a case")
        _validate_behavior_budgets(scenario, definition, suite)
        if "not_implemented" in scenario:
            if not isinstance(scenario["not_implemented"], str):
                raise ValueError("not_implemented must be a string")
            if scenario.get("evidence_file"):
                raise ValueError("a scenario cannot declare both evidence_file and not_implemented")
    return manifest


def validate(evidence: Mapping[str, Any], *, phase: str = "final") -> list[str]:
    from aws_durable_execution_conformance_tests.lmi.evidence import validate_evidence

    if evidence.get("coverageProfile", "standard") != "standard":
        return ["INCOMPLETE: unknown evidence coverage profile"]
    if phase not in ("observations", "final"):
        return ["INCOMPLETE: unknown evidence validation phase"]
    return validate_evidence(evidence, phase="observations" if phase == "observations" else "final")


def _identity_errors(
    manifest: Mapping[str, Any],
    scenario: Mapping[str, Any],
    evidence: Mapping[str, Any],
    *,
    suite: SuiteDefinition | None = None,
) -> list[str]:
    errors = []
    expected = {
        "runId": manifest["runId"],
        "runtime": manifest["runtime"],
        "caseId": scenario["case_id"],
        "requirementId": scenario["requirement_id"],
    }
    for key, value in expected.items():
        if evidence.get(key) != value:
            errors.append(f"INCOMPLETE: evidence {key} does not match the admitted case")
    coverage_profile = manifest.get("coverageProfile", "standard")
    if evidence.get("coverageProfile", "standard") != coverage_profile:
        errors.append("INCOMPLETE: evidence coverageProfile does not match the admitted run")
    profile = next(profile for profile in manifest["profiles"] if profile["profile_id"] == scenario["profile_id"])
    definition = _case_definition(manifest, scenario, profile, suite)
    # A manifest may declare roles beyond the requirement's defaults, and
    # retained evidence must cover every declared role for every suite.
    declared = set(scenario.get("roles", definition.roles))
    selected_roles = evidence.get("selectedRequests")
    if not isinstance(selected_roles, Mapping) or set(selected_roles) != declared:
        errors.append("INCOMPLETE: selectedRequests does not cover every declared cohort role")
    policy_key = suite.evidence_policy_key if suite is not None else None
    if suite is not None and evidence.get("scenario") != scenario["scenario"]:
        errors.append("INCOMPLETE: evidence scenario does not match the admitted variant")
    policy = evidence if policy_key is None else evidence.get(policy_key)
    declared_budgets = scenario.get("behavior_budgets", {})
    if policy_key is not None or definition.budget_names or declared_budgets:
        observed_budgets = policy.get("budgets") if isinstance(policy, Mapping) else None
        if not isinstance(observed_budgets, Mapping) or any(
            name not in observed_budgets or not same_json(observed_budgets[name], value)
            for name, value in declared_budgets.items()
        ):
            errors.append("INCOMPLETE: observed behavior budgets do not match the declared case budgets")
    if suite is None and coverage_profile == "standard" and scenario["scenario"] in ("normal-return", "normal-failure"):
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
    if profile.get("recovery_policy") is not None and observed.get("recoveryPolicy") != profile["recovery_policy"]:
        errors.append("INCOMPLETE: observed recovery policy does not match the declared profile")
    return errors


def evidence_results(
    manifest: Mapping[str, Any], base: Path, *, suite: SuiteDefinition | None = None
) -> list[dict[str, Any]]:
    _required(manifest, suite)
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
                evidence = _read_json(base / scenario["evidence_file"])
                if not isinstance(evidence, dict):
                    raise ValueError("evidence must be an object")
                errors = _identity_errors(manifest, scenario, evidence, suite=suite) + list(
                    (suite.validator if suite else validate)(evidence)
                )
                result.update(evidence=evidence, errors=errors, status="FAILED" if errors else "PASSED")
            except (OSError, ValueError, TypeError) as exc:
                result.update(status="INCOMPLETE_EVIDENCE", errors=[f"INCOMPLETE: {exc}"])
        results.append(result)
    claims: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for result in results:
        case_evidence = result.get("evidence", {})
        attempts = case_evidence.get("attempts", [])
        identities = {
            (field, attempt[field])
            for attempt in (attempts if isinstance(attempts, list) else [])
            if isinstance(attempt, Mapping)
            for field in ("requestId", "executionArn")
            if isinstance(attempt.get(field), str) and attempt[field].strip()
        }
        for collection in ("resourcesCreated", "faultsCreated"):
            resources = case_evidence.get(collection, [])
            if not isinstance(resources, list):
                continue
            for resource in resources:
                if not isinstance(resource, Mapping) or not isinstance(resource.get("id"), str):
                    continue
                kind = "fault" if collection == "faultsCreated" else resource.get("kind")
                if kind in ("callback", "control", "fault") and resource["id"].strip():
                    identities.add((kind, resource["id"]))
        for claim in sorted(identities):
            claims.setdefault(claim, []).append(result)
    for (field, resource_id), owners in claims.items():
        if len(owners) > 1:
            for owner in owners:
                owner["errors"].append(f"INCOMPLETE: {field} {resource_id} is claimed by multiple cases")
                owner["status"] = "FAILED"
    return results


def build_report(
    manifest: Mapping[str, Any], results: list[dict[str, Any]], *, suite: SuiteDefinition | None = None
) -> Report:
    suite_name = suite.suite_name if suite is not None else "lmi-lifecycle"
    report = Report(
        run=RunMetadata(
            name=manifest["runId"],
            template="LMI persistent profiles",
            region=manifest.get("region", ""),
            language=manifest["runtime"],
            suites=[suite_name],
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
                suite=suite_name,
                status=ReportStatus.FAILED,
                description="Run result ownership and consistency",
                errors=integrity_errors,
            )
        )
    inventory = coverage_inventory(manifest, suite=suite)
    for requirement_id, requirement in _catalog(manifest, suite).items():
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
                f"Missing mandatory matrix slot: {case['scenario']} / native={case['native_concurrency']}"
                for case in missing
            )
            if status == ReportStatus.PASSED:
                status = ReportStatus.UNCOVERED
        report.add(
            ReportEntry(
                id=requirement_id,
                suite=suite_name,
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
    return report


def run(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="durable-execution-conformance lmi-lifecycle", description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("lmi-output"))
    args = parser.parse_args(argv)
    try:
        manifest = load_manifest(args.manifest)
        args.output.mkdir(parents=True, exist_ok=True)
        if any(args.output.iterdir()):
            raise ValueError("output directory must be empty; preserve previous run evidence")
        serialized_manifest = _dump_json(manifest) + "\n"
        with (args.output / "manifest.json").open("x") as output:
            output.write(serialized_manifest)
        results = evidence_results(manifest, args.manifest.parent)
        report = build_report(manifest, results)
    except (OSError, ValueError, TypeError) as exc:
        parser.error(str(exc))
    (args.output / "evidence.json").write_text(_dump_json(results) + "\n")
    (args.output / "coverage.json").write_text(json.dumps(coverage_inventory(manifest), indent=2) + "\n")
    print(render_console(report))
    for report_format in ("json", "junit"):
        write_report(report, report_format, str(args.output / "report"))
    sys.exit(report.exit_code())


if __name__ == "__main__":
    run()
