# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Collect LMI lifecycle evidence and adjudicate it, with strict coverage."""

from __future__ import annotations

import argparse
import asyncio
import errno
import importlib
import json
import math
import os
import sys
from collections.abc import Iterable, Mapping
from copy import deepcopy
from dataclasses import asdict, dataclass, field, fields, replace
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from itertools import combinations
from pathlib import Path
from typing import Any, BinaryIO

from aws_durable_execution_conformance_tests.lmi.catalog import CAPABILITIES, COVERAGE_PROFILES, RUNTIMES, requirements
from aws_durable_execution_conformance_tests.lmi.evidence import (
    RECOVERY_POLICIES,
    EvidenceValidator,
    is_json_integer,
    same_json,
)
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
    service_role_transitions: Mapping[str, frozenset[tuple[str, str]]] = field(default_factory=dict)


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


def _dump_json(value: Any, depth: int = 0, *, compact: bool = False) -> str:
    """Write retained JSON without rounding exact decimal payloads."""
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("JSON numbers must be finite")
        return str(value)
    if not isinstance(value, (dict, list, tuple)) or not value:
        return json.dumps(value, allow_nan=False)
    inner = "" if compact else "\n" + "  " * (depth + 1)
    outer = "" if compact else "\n" + "  " * depth
    if isinstance(value, dict):
        keys: set[str] = set()
        items = []
        for key, item in value.items():
            if not isinstance(key, str):
                if key is not None and not isinstance(key, (bool, int, float)):
                    raise ValueError("JSON object keys must be strings or scalar JSON values")
                key = json.dumps(key, allow_nan=False)
            if key in keys:
                raise ValueError(f"JSON object keys collide after encoding: {key!r}")
            keys.add(key)
            items.append(f"{json.dumps(key)}: {_dump_json(item, depth + 1, compact=compact)}")
        return "{" + inner + ("," + inner).join(items) + outer + "}"
    return (
        "[" + inner + ("," + inner).join(_dump_json(item, depth + 1, compact=compact) for item in value) + outer + "]"
    )


def _validate_manifest(manifest: Any, *, suite: SuiteDefinition | None = None) -> dict[str, Any]:
    """Validate the run inventory before loading an adapter or admitting work."""
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
        if (policy := profile.get("recovery_policy")) is not None and policy not in RECOVERY_POLICIES:
            raise ValueError("Unknown recovery policy")
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
        if "evidence_file" in scenario:
            raise ValueError("evidence_file is not supported; collect evidence with --adapter")
        if "not_implemented" in scenario and not isinstance(scenario["not_implemented"], str):
            raise ValueError("not_implemented must be a string")
    return manifest


def load_manifest(path: Path, *, suite: SuiteDefinition | None = None) -> dict[str, Any]:
    """Load a manifest using the same admission rules as direct collection runs."""
    return _validate_manifest(_read_json(path), suite=suite)


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
    # A manifest may declare roles beyond the requirement's defaults. Collected
    # evidence must cover the declared role set for every suite.
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


def _audit_append_boundary(stream: BinaryIO) -> int:
    end = stream.seek(0, os.SEEK_END)
    if not end:
        return 0
    stream.seek(end - 1)
    if stream.read(1) == b"\n":
        return end

    # Repair a partial tail left by an earlier write whose rollback failed.
    offset = end
    while offset:
        start = max(0, offset - 8192)
        stream.seek(start)
        last_newline = stream.read(offset - start).rfind(b"\n")
        if last_newline >= 0:
            offset = start + last_newline + 1
            break
        offset = start
    stream.truncate(offset)
    return offset


def _append_audit(path: Path, record: Mapping[str, Any]) -> None:
    """Persist before dispatch; sink failure must abort the launch."""
    _append_audit_row(path, _dump_json(record, compact=True))


def _sync_directory(path: Path) -> None:
    """Persist a directory's entries, such as a newly created journal file."""
    if os.name == "nt":
        return  # Windows cannot open a directory for fsync.
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    except OSError as exc:
        # Some filesystems cannot fsync a directory at all; nothing more is possible.
        if exc.errno not in (errno.EINVAL, errno.EBADF, errno.ENOTSUP, errno.EOPNOTSUPP):
            raise
    finally:
        os.close(descriptor)


def _write_new_manifest(path: Path, manifest: Mapping[str, Any]) -> None:
    """Create ``path`` exclusively with the manifest, leaving nothing on failure."""
    document = _dump_json(manifest) + "\n"
    created = False
    try:
        with path.open("x") as stream:
            created = True
            stream.write(document)
    except BaseException:
        if created:
            path.unlink(missing_ok=True)
        raise


# Files an earlier collection or adjudication wrote into an output directory.
_RUN_OUTPUTS = (
    "attempts.jsonl",
    "results.jsonl",
    "collected-evidence.json",
    "evidence.json",
    "coverage.json",
    "report.json",
    "report.xml",
)


def _make_durable_directory(path: Path) -> None:
    """Create a directory and persist every entry on its path.

    Every ancestor is synced, not only those created now, so a directory that an
    earlier failed attempt created still gets a durable name. A directory this
    process can change must sync; one it cannot write, such as ``/``, cannot hold
    an entry this run created, so its sync is best effort.
    """
    created = {directory for directory in (path, *path.parents) if not directory.exists()}
    path.mkdir(parents=True, exist_ok=True)
    for directory in (path, *path.parents):
        if directory.parent == directory:
            break
        try:
            _sync_directory(directory.parent)
        except OSError:
            if directory in created or os.access(directory.parent, os.W_OK):
                raise


def _append_audit_row(path: Path, document: str) -> None:
    row = (document + "\n").encode("utf-8")
    with path.open("a+b", buffering=0) as stream:
        try:
            boundary = _audit_append_boundary(stream)
        except OSError as exc:
            raise OSError(f"Could not repair audit record boundary in {path}") from exc
        try:
            if stream.write(row) != len(row):
                raise OSError(f"Short audit write to {path}")
        except OSError as exc:
            try:
                stream.truncate(boundary)
            except OSError as repair_error:
                repair_error.add_note(f"Failed to restore audit record boundary in {path}")
                raise repair_error from exc
            raise
        # A complete row remains a possible durable claim if sync raises.
        os.fsync(stream.fileno())
    # A row is durable only once the journal's name is. Sync on every append so
    # a failed earlier sync cannot leave a later row authorizing dispatch.
    _sync_directory(path.parent)


def _result_for_audit(result: dict[str, Any]) -> tuple[dict[str, Any], str]:
    try:
        return result, _dump_json(result, compact=True)
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        fallback = {
            "case_id": result["case_id"],
            "requirement_id": result["requirement_id"],
            "profile_id": result["profile_id"],
            "status": "INFRASTRUCTURE_ERROR" if result["status"] in ("PASSED", "COLLECTED") else result["status"],
            "errors": [*result["errors"], f"Could not serialize case evidence: {exc}"],
        }
    return fallback, _dump_json(fallback, compact=True)


async def collect_and_adjudicate(
    manifest: Mapping[str, Any], adapter_name: str, *, audit_dir: Path, suite: SuiteDefinition | None = None
) -> list[dict[str, Any]]:
    from aws_durable_execution_conformance_tests.lmi.coordinator import (
        Budgets,
        Profile,
        Scenario,
        ScenarioResult,
        run_scenarios,
    )

    # Validation keeps its own snapshot. Adapter and Scenario objects must not
    # be able to change the expected budgets, profile, or coverage after admission.
    if suite is not None:
        suite = replace(suite, requirements={key: deepcopy(dict(row)) for key, row in suite.requirements.items()})
    manifest = _validate_manifest(deepcopy(dict(manifest)), suite=suite)
    module_name, separator, factory_name = adapter_name.partition(":")
    if not separator or not factory_name:
        raise ValueError("--adapter must be a local Python module:factory")
    profiles = [
        Profile(**{item.name: profile[item.name] for item in fields(Profile) if item.name in profile})
        for profile in manifest["profiles"]
    ]
    active = [scenario for scenario in manifest["scenarios"] if "not_implemented" not in scenario]
    profile_map = {profile["profile_id"]: profile for profile in manifest["profiles"]}
    scenarios = []
    for scenario in active:
        definition = _case_definition(manifest, scenario, profile_map[scenario["profile_id"]], suite)
        configured = deepcopy({item.name: scenario[item.name] for item in fields(Scenario) if item.name in scenario})
        configured.setdefault("exclusive", definition.exclusive)
        configured.setdefault("slots", definition.minimum_native_concurrency)
        configured.setdefault("roles", list(definition.roles))
        configured["coverage_profile"] = manifest.get("coverageProfile", "standard")
        configured["service_role_transitions"] = (
            frozenset(suite.service_role_transitions.get(scenario["requirement_id"], ())) if suite else frozenset()
        )
        scenarios.append(Scenario(**configured))
    _make_durable_directory(audit_dir)
    result_journal = audit_dir / "results.jsonl"
    attempt_journal = audit_dir / "attempts.jsonl"
    capture_path = audit_dir / "collected-evidence.json"
    # A manifest and caller-owned artifacts may precede collection; the outputs of
    # an earlier collection or adjudication may not.
    # Look names up in the filesystem, which may ignore case, and count dangling links.
    if any(os.path.lexists(audit_dir / name) for name in _RUN_OUTPUTS):
        raise ValueError("audit directory already contains lifecycle evidence")
    manifest_path = audit_dir / "manifest.json"
    # Claim this run's output before any adapter work, then bind the manifest
    # under that claim. Simultaneous callers targeting the same directory cannot
    # interleave their journals or record another plan's manifest.
    capture_path.touch(exist_ok=False)
    try:
        if manifest_path.exists():
            if not same_json(_read_json(manifest_path), manifest):
                raise ValueError("audit directory contains a different manifest")
        else:
            _write_new_manifest(manifest_path, manifest)
    except BaseException:
        capture_path.unlink(missing_ok=True)
        raise
    publications: dict[str, str] = {}
    emitted: dict[str, ScenarioResult] = {}

    def publish(case_id: str, document: str) -> None:
        _append_audit_row(result_journal, document)
        publications[case_id] = document

    def result_fields(result: ScenarioResult) -> dict[str, Any]:
        return {item.name: getattr(result, item.name) for item in fields(result)}

    def write_result(result: ScenarioResult) -> None:
        emitted[result.case_id] = result
        raw = result_fields(result)
        _, document = _result_for_audit(raw)
        publish(result.case_id, document)

    def publish_final(result: dict[str, Any], document: str) -> None:
        if publications.get(result["case_id"]) == document:
            return
        try:
            publish(result["case_id"], document)
        except OSError as exc:
            if result["status"] in ("PASSED", "NOT_IMPLEMENTED"):
                result["status"] = "INFRASTRUCTURE_ERROR"
            result["errors"].append(f"Could not publish final case result: {exc}")

    skipped_results: list[dict[str, Any]] = []
    for scenario in manifest["scenarios"]:
        if "not_implemented" in scenario:
            skipped = {
                "case_id": scenario["case_id"],
                "requirement_id": scenario["requirement_id"],
                "profile_id": scenario["profile_id"],
                "status": "NOT_IMPLEMENTED",
                "errors": [scenario["not_implemented"]],
            }
            publish_final(skipped, _dump_json(skipped, compact=True))
            skipped_results.append(skipped)

    def reconcile_results(outcomes: Iterable[ScenarioResult]) -> list[dict[str, Any]]:
        results = [_result_for_audit(result_fields(outcome))[0] for outcome in outcomes]
        results.extend(skipped_results)
        capture_path.write_text(_dump_json(results) + "\n")
        results = adjudicate_collected_evidence(manifest, _read_json(capture_path), suite=suite)
        for index, result in enumerate(results):
            results[index], document = _result_for_audit(result)
            publish_final(results[index], document)
        return results

    if not scenarios:
        return reconcile_results(())
    factory = getattr(importlib.import_module(module_name), factory_name)
    adapter = factory(deepcopy(manifest))
    try:
        outcomes = await run_scenarios(
            scenarios,
            profiles,
            adapter,
            run_id=manifest["runId"],
            scenario_concurrency=manifest.get("scenarioConcurrency", 8),
            budgets=Budgets(**manifest.get("budgets", {})),
            on_attempt=lambda context: _append_audit(
                attempt_journal,
                {
                    "runId": context.run_id,
                    "caseId": context.scenario.case_id,
                    "profileId": context.profile.profile_id,
                    "attempts": context.attempts,
                },
            ),
            on_result=write_result,
        )
    except asyncio.CancelledError as cancelled:
        try:
            reconcile_results(emitted.values())
        except (OSError, ValueError, TypeError) as error:
            cancelled.add_note(f"Could not preserve cancelled run evidence: {error}")
        raise
    return reconcile_results(outcomes)


def adjudicate_collected_evidence(
    manifest: Mapping[str, Any],
    collected: Any,
    *,
    suite: SuiteDefinition | None = None,
) -> list[dict[str, Any]]:
    """Adjudicate captured cases without an adapter or an acquired target."""
    from aws_durable_execution_conformance_tests.lmi.adjudication import adjudicate_evidence

    if not isinstance(collected, list):
        raise ValueError("collected evidence must be an array of case results")
    manifest = _validate_manifest(deepcopy(dict(manifest)), suite=suite)
    expected = {case["case_id"]: case for case in manifest["scenarios"]}
    results = []
    for raw in collected:
        if (
            not isinstance(raw, dict)
            or not isinstance(raw.get("case_id"), str)
            or not isinstance(raw.get("errors"), list)
            or not all(isinstance(error, str) for error in raw["errors"])
        ):
            raise ValueError("collected evidence must contain case result objects with errors")
        case = expected.get(raw.get("case_id"))
        if case is None:
            raise ValueError("collected evidence contains an unadmitted case")
        if raw.get("status") not in (
            "COLLECTED",
            "FAILED",
            "INFRASTRUCTURE_ERROR",
            "INCOMPLETE_EVIDENCE",
            "NOT_IMPLEMENTED",
        ):
            raise ValueError("collected case has an invalid collection status")
        if raw.get("profile_id") != case["profile_id"] or raw.get("requirement_id") != case["requirement_id"]:
            raise ValueError("collected case does not match its manifest identity")
        if "not_implemented" in case and raw["status"] not in ("NOT_IMPLEMENTED", "INFRASTRUCTURE_ERROR"):
            raise ValueError("collected evidence conflicts with a manifest-declared skip")
        result = deepcopy(raw)
        evidence = result.get("evidence")
        if result["status"] == "COLLECTED" and not evidence:
            result["status"] = "INCOMPLETE_EVIDENCE"
            result["errors"].append("INCOMPLETE: no observations collected")
        try:
            if evidence:
                if not isinstance(evidence, Mapping):
                    raise ValueError("case observations must be an object")
                findings = _identity_errors(manifest, case, evidence, suite=suite)
                retirement = result.get("retirement_evidence", {})
                cleanup = result.get("cleanup_evidence", {})
                observations = result.get("observation_evidence", [])
                attempts = result.get("attempts")
                if (
                    not isinstance(retirement, Mapping)
                    or not isinstance(cleanup, Mapping)
                    or any(
                        not isinstance(frames, list) or not all(isinstance(frame, Mapping) for frame in frames)
                        for frames in cleanup.values()
                    )
                ):
                    raise ValueError("case retirement and cleanup observations must use the capture envelope")
                if not isinstance(observations, list) or not all(isinstance(frame, Mapping) for frame in observations):
                    raise ValueError("raw case observations must be an array of objects")
                if not isinstance(attempts, list) or not all(isinstance(attempt, Mapping) for attempt in attempts):
                    raise ValueError("recorded attempts must be an array of objects")
                collection_errors = [finding for finding in findings if finding.startswith("INCOMPLETE:")]
                behavior_findings = [finding for finding in findings if not finding.startswith("INCOMPLETE:")]
                result["status"], result["errors"], _ = adjudicate_evidence(
                    result["status"],
                    list(dict.fromkeys([*result["errors"], *collection_errors])),
                    evidence,
                    trusted_observations=result.get("trusted_observations") is True and not collection_errors,
                    observation_findings=behavior_findings,
                    retirement_evidence=retirement,
                    cleanup_evidence=cleanup,
                    observation_evidence=observations,
                    attempts=attempts,
                    validator=suite.validator if suite else None,
                    standard=manifest.get("coverageProfile", "standard") == "standard",
                )
        except (AttributeError, KeyError, TypeError, ValueError, OverflowError) as error:
            if result["status"] == "COLLECTED":
                result["status"] = "INCOMPLETE_EVIDENCE"
            result["errors"].append(f"INCOMPLETE: malformed captured evidence: {error}")
        results.append(result)
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
    input_source = parser.add_mutually_exclusive_group(required=True)
    input_source.add_argument(
        "--collected-evidence", type=Path, help="adjudicate a saved collected-evidence bundle without collecting"
    )
    input_source.add_argument(
        "--adapter", help="local module:factory for the trusted collection adapter that collects the evidence"
    )
    parser.add_argument("--output", type=Path, default=Path("lmi-output"))
    args = parser.parse_args(argv)
    try:
        manifest = load_manifest(args.manifest)
        _make_durable_directory(args.output)
        if any(args.output.iterdir()):
            raise ValueError("output directory must be empty; preserve previous run evidence")
        # Every mode claims the directory with an exclusive manifest write, so
        # simultaneous runs cannot both pass the emptiness check and publish.
        _write_new_manifest(args.output / "manifest.json", manifest)
        results = (
            asyncio.run(collect_and_adjudicate(manifest, args.adapter, audit_dir=args.output))
            if args.adapter is not None
            else adjudicate_collected_evidence(manifest, _read_json(args.collected_evidence))
        )
        report = build_report(manifest, results)
    except (OSError, ValueError, TypeError, ImportError, AttributeError) as exc:
        parser.error(str(exc))
    (args.output / "evidence.json").write_text(_dump_json(results) + "\n")
    (args.output / "coverage.json").write_text(json.dumps(coverage_inventory(manifest), indent=2) + "\n")
    print(render_console(report))
    for report_format in ("json", "junit"):
        write_report(report, report_format, str(args.output / "report"))
    sys.exit(report.exit_code())


if __name__ == "__main__":
    run()
