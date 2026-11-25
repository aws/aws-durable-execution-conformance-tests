# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: Apache-2.0
"""Reusable manifest, strict report execution and run-scoped cleanup."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from aws_durable_execution_conformance_tests.lmi.rules.controls import invoke_rejection_proven

from .artifacts import (
    case_key,
    case_output_directory,
    owner_lock,
    same_owner_lock,
    save,
    saved_case_plans,
    sdk_artifact_directory,
    sdk_plan_with_artifact_directory,
)
from .catalog import COVERAGE_PROFILES
from .cloud import find_execution_by_name, require_execution_name, require_ledger_launch
from .deployment import recover_deployment_update
from .errors import CollectionError
from .quarantine import fence_matches, read_fence, release_fence
from .service import client, release_control, request_execution_stops, retryable_read_error


def _require_conformance_revision(revision: Any) -> None:
    if not isinstance(revision, str) or not revision.strip() or revision.strip().lower() == "unverified":
        raise ValueError("Shared run requires a verified conformanceRevision")


def make_manifest(
    deployment: Mapping[str, Any],
    coverage: str,
    directory: Path,
    *,
    artifact_id: str,
    conformance_revision: str,
) -> dict[str, Any]:
    from aws_durable_execution_conformance_tests.lmi.configuration import configuration_identity
    from aws_durable_execution_conformance_tests.lmi.matrix import required_cases
    from aws_durable_execution_conformance_tests.lmi.runtime import RuntimeDefinition

    if coverage not in COVERAGE_PROFILES:
        raise ValueError("Unknown shared coverage profile")
    _require_conformance_revision(conformance_revision)
    actual = deepcopy(dict(deployment))
    actual["conformanceRevision"] = conformance_revision
    if not artifact_id:
        raise ValueError("An exact SDK artifact identity is required")
    runtime = actual["runtimeModel"]
    recovery_policy = RuntimeDefinition.from_manifest(actual).binding.recovery_policy
    profiles = [
        {
            "profile_id": key,
            "resource_id": actual["functions"][key],
            "native_concurrency": actual["concurrencies"][key],
            "capacity": actual["concurrencies"][key],
            "artifact_id": artifact_id,
            "configuration_id": configuration_identity(actual, key),
            "recovery_policy": recovery_policy,
        }
        for key in ("c1", "c2")
    ]
    scenarios = []
    for required in required_cases(runtime, "lifecycle", coverage):
        concurrency = required.minimum_native_concurrency
        case_id = f"{required.requirement_id}-{required.scenario}"
        scenarios.append(
            {
                "case_id": case_id,
                "requirement_id": required.requirement_id,
                "profile_id": f"c{concurrency}",
                "scenario": required.scenario,
                "roles": list(required.roles),
                "slots": concurrency,
                "exclusive": required.exclusive,
                "behavior_budgets": {
                    "observationGraceMs": 1000,
                    "admissionMs": 5000,
                    "cleanupMs": 5000,
                    "recoveryMs": 15000,
                },
            }
        )
    return {
        "schemaVersion": 1,
        "runId": actual["run"],
        "runtime": runtime,
        "region": actual["region"],
        "coverageProfile": coverage,
        "capabilityProfile": "lifecycle",
        "scenarioConcurrency": 8,
        "profiles": profiles,
        "scenarios": scenarios,
        "budgets": {
            "provisioning": 120,
            "lease": 1800,
            "placement": 120,
            "execution": 240,
            "collection": 180,
            "release": 60,
            "retirement": 180,
        },
        "sdk": {
            "deployment": actual,
            "artifacts_dir": str(directory.resolve() / "source"),
        },
    }


def run(deployment: Mapping[str, Any], directory: Path, *, manifest_factory: Callable) -> int:
    from aws_durable_execution_conformance_tests.lmi.app import (
        _run_collection,
        build_report,
        collect_and_adjudicate,
        coverage_inventory,
        load_manifest,
    )
    from aws_durable_execution_conformance_tests.reporters import (
        render_console,
        write_report,
    )

    directory.mkdir(parents=True, exist_ok=True)
    if any(directory.iterdir()):
        raise ValueError("Shared run output must be empty; preserve prior attempts and failure evidence")
    location = deployment.get("deploymentArtifactDirectory")
    if isinstance(location, str) and location:
        artifacts = Path(location).resolve()
        output = directory.resolve()
        recorded = output.relative_to(artifacts) if output.is_relative_to(artifacts) else output
        save(artifacts / "case-output.json", {"runId": deployment["run"], "directory": str(recorded)})
    exit_code = 0
    for coverage in COVERAGE_PROFILES:
        output = directory / coverage
        output.mkdir()
        plan = manifest_factory(deployment, coverage, output)
        sdk = plan.get("sdk")
        actual = sdk.get("deployment") if isinstance(sdk, Mapping) else None
        _require_conformance_revision(actual.get("conformanceRevision") if isinstance(actual, Mapping) else None)
        plan = sdk_plan_with_artifact_directory(output, plan)
        path = output / "manifest.json"
        save(path, plan)
        validated = load_manifest(path)
        # Bound teardown like the CLI: the coordinator abandons adapter work
        # that outlives its budget, and asyncio.run would wait for it without
        # a bound, so a task that ignores cancellation would block the reports.
        results = _run_collection(collect_and_adjudicate(validated, audit_dir=output))
        report = build_report(validated, results)
        save(output / "evidence.json", results)
        save(output / "coverage.json", coverage_inventory(validated))
        print(render_console(report))
        for format_name in ("json", "junit"):
            write_report(report, format_name, str(output / "report"))
        exit_code = max(exit_code, report.exit_code())
    return exit_code


def _recover_profile_fences(deployment, source, profiles, storage) -> list[str]:
    errors = []
    journals = source / "configuration"
    for resource in {profile["resource_id"] for profile in profiles.values()}:
        path = journals / f"{case_key(resource)}-fence.json"
        if not path.is_file():
            continue
        try:
            with owner_lock(path) as owner:
                if not path.is_file():
                    continue
                record = json.loads(path.read_text())
                state = record.get("state")
                if state not in {"ACQUIRING", "NOT_ADMITTED", "RETIRED"}:
                    continue  # A live acquisition needs separate runtime retirement evidence.
                token = record.get("token")
                if (
                    record.get("resourceId") != resource
                    or record.get("run") != deployment["run"]
                    or record.get("commit") != deployment["commit"]
                    or resource not in deployment["functions"].values()
                    or not isinstance(token, str)
                    or not token
                ):
                    raise CollectionError("Profile fence journal does not match the cleanup run")
                if state == "ACQUIRING" and not same_owner_lock(record, owner):
                    raise CollectionError("Profile journal lacks its original local owner lock")
                current = read_fence(storage, deployment["bucket"], resource)
                owned = fence_matches(current, deployment, resource, token)
                if state == "ACQUIRING":
                    if record.get("singleAttempt") is not True or not owned:
                        raise CollectionError("Profile fence acquisition outcome is unresolved")
                    save(path, {**record, "state": "NOT_ADMITTED"})
                if owned:
                    release_fence(storage, deployment, resource, token)
                path.unlink(missing_ok=True)
        except Exception as error:
            errors.append(f"{resource}: {type(error).__name__}: {error}")
    return errors


def cleanup(
    deployment: Mapping[str, Any], directory: Path | None = None, *, client_factory: Callable | None = None
) -> int:
    """Release this run's controls and request stops without altering its reports.

    GitHub Actions holds the cloud job's concurrency slot through this step.
    Successful requests do not imply that every runtime task has retired.
    """
    client_factory = client_factory or client
    errors: list[str] = []
    location = deployment.get("deploymentArtifactDirectory")
    if isinstance(location, str):
        artifacts = Path(location)
    elif directory is not None:
        artifacts = directory.parent
    else:
        raise CollectionError("Cleanup needs the deployment artifact directory or an explicit case-output directory")
    missing_output = False
    if directory is None:
        try:
            directory = case_output_directory(artifacts, deployment["run"], require_available=True)
        except ValueError as error:
            errors.append(str(error))
            missing_output = True
            directory = artifacts / "cleanup" / case_key(deployment["run"])
    journal = artifacts / "deployment-update.json"
    active = artifacts / "manifest.json"
    pre_archive = False
    if journal.is_file() and active.is_file():
        record = json.loads(journal.read_text())
        pre_archive = all(record.get(key) == deployment.get(key) for key in ("stack", "bucket", "run", "commit")) and (
            json.loads(active.read_text()).get("run") != deployment["run"]
        )
        if pre_archive:
            # An older active manifest can still own the shared case output.
            # Keep this run's pre-case receipt next to its deployment journal.
            directory = artifacts / "cleanup" / case_key(deployment["run"])
    try:
        recover_deployment_update(
            deployment,
            artifacts,
            lambda service: client_factory(service, deployment["region"]),
        )
    except Exception as error:
        errors.append(f"Deployment update reconciliation: {error}")
    try:
        plans = [] if pre_archive or missing_output else saved_case_plans(directory, deployment["run"])
    except ValueError as error:
        raise CollectionError(str(error)) from error
    for _, plan in plans:
        profiles = {profile["profile_id"]: profile for profile in plan["profiles"]}
        for case in plan["scenarios"]:
            target = profiles[case["profile_id"]]["resource_id"]
            if target not in deployment["functions"].values():
                raise CollectionError("Cleanup target is outside this deployment")
    if not plans:
        save(
            directory / "cleanup.json",
            {
                "runId": deployment["run"],
                "status": "FAILED" if errors else "NO_CASES",
                "errors": errors,
                "reason": (
                    "Recorded case-output directory is unavailable"
                    if missing_output
                    else "No shared case manifests were recorded"
                ),
            },
        )
        return int(bool(errors))
    storage, service = (
        client_factory("s3", deployment["region"]),
        client_factory("lambda", deployment["region"]),
    )
    for plan_root, plan in plans:
        profiles = {profile["profile_id"]: profile for profile in plan["profiles"]}
        try:
            source = sdk_artifact_directory(plan_root, plan)
        except ValueError as error:
            source = None
            errors.append(str(error))
        if source is not None:
            errors.extend(_recover_profile_fences(deployment, source, profiles, storage))
        for case in plan["scenarios"]:
            target = profiles[case["profile_id"]]["resource_id"]
            if target not in deployment["functions"].values():
                raise CollectionError("Cleanup target is outside the owned deployment")
            owner = case_key(case["case_id"])
            prefix = f"runs/{deployment['run']}/cases/{owner}/control/"
            case_dir = source / "cases" / owner if source is not None else None
            try:
                try:
                    objects = [
                        item["Key"]
                        for page in storage.get_paginator("list_objects_v2").paginate(
                            Bucket=deployment["bucket"], Prefix=prefix
                        )
                        for item in page.get("Contents", [])
                    ]
                    if any(not key.startswith(prefix) for key in objects):
                        raise CollectionError("Control listing escaped the case-owned prefix")
                except Exception as error:
                    errors.append(f"{case['case_id']}/controls: {type(error).__name__}: {error}")
                    objects = []
                pending_controls = set()
                control_journal = case_dir / "control-records.json" if case_dir is not None else None
                if control_journal is not None and control_journal.is_file():
                    try:
                        records = json.loads(control_journal.read_text())
                        if not isinstance(records, list):
                            raise CollectionError("Control journal must contain a record array")
                        for record in records:
                            if not isinstance(record, dict):
                                raise CollectionError("Control journal contains a malformed record")
                            if record.get("action") != "hold" or record.get("state") not in {"PENDING", "SUPERSEDED"}:
                                continue
                            gate = record.get("gate")
                            if (
                                not isinstance(gate, str)
                                or not gate
                                or not all(char.isascii() and (char.isalnum() or char in "-_") for char in gate)
                            ):
                                raise CollectionError("Control journal contains an invalid gate name")
                            pending_controls.add(prefix + gate)
                    except Exception as error:
                        errors.append(f"{case['case_id']}/control-journal: {error}")
                executions: set[str] = set()
                unresolved: dict[str, tuple[str, dict[str, Any]]] = {}
                invalid_intent = False
                journals = (case_dir / "invocations").glob("*.json") if case_dir is not None else ()
                for path in journals:
                    try:
                        invocation = json.loads(path.read_text())
                        payload = invocation.get("payload", {})
                        if payload.get("run") != deployment["run"] or payload.get("caseId") != case["case_id"]:
                            raise CollectionError("Invocation intent belongs to another case")
                        name = invocation["durableExecutionName"]
                        prepared = invocation.get("dispatchState") == "PREPARED" and not any(
                            key in invocation for key in ("arn", "executionArn", "requestId")
                        )
                        if prepared or invoke_rejection_proven(invocation):
                            if (
                                invocation.get("function") != target
                                or path.name != f"{payload.get('marker')}.json"
                                or not isinstance(name, str)
                                or not name
                            ):
                                raise CollectionError("Undispatched invocation journal has inconsistent identity")
                            continue
                        if invocation.get("dispatchState") == "CONFLICT":
                            raise CollectionError("Invoke name conflicts with another launch")
                        if invocation.get("arn"):
                            require_execution_name(invocation["arn"], target, name)
                            executions.add(invocation["arn"])
                        else:
                            recovered_executions: set[str] = set()
                            marker = payload["marker"]
                            unresolved[marker] = (name, payload)
                            event_prefix = f"runs/{deployment['run']}/events/{marker}/"
                            for page in storage.get_paginator("list_objects_v2").paginate(
                                Bucket=deployment["bucket"], Prefix=event_prefix
                            ):
                                for obj in page.get("Contents", []):
                                    with storage.get_object(Bucket=deployment["bucket"], Key=obj["Key"])[
                                        "Body"
                                    ] as stream:
                                        event = json.loads(stream.read())
                                    require_ledger_launch(event, payload)
                                    if event["phase"] == "WRAPPER_ENTER":
                                        require_execution_name(event["execution"], target, name)
                                        recovered_executions.add(event["execution"])
                            if len(recovered_executions) > 1:
                                raise CollectionError("Multiple executions matched one invocation intent")
                            executions.update(recovered_executions)
                            if recovered_executions:
                                unresolved.pop(marker)
                    except Exception as error:
                        # A transport failure while reading one valid launch's
                        # ledger does not invalidate the case's control prefix.
                        if not isinstance(error, (BotoCoreError, ClientError)):
                            invalid_intent = True
                        errors.append(f"{case['case_id']}/{path.name}: {type(error).__name__}: {error}")
                if invalid_intent and not executions and not unresolved:
                    objects = []
                for key in sorted(set(objects) | pending_controls, key=lambda value: value != prefix + "release-all"):
                    try:
                        release_control(storage, deployment["bucket"], key, allow_absent=key in pending_controls)
                    except Exception as error:
                        errors.append(f"{case['case_id']}/{key}: {type(error).__name__}: {error}")
                for arn, stop_error in request_execution_stops(service, executions).items():
                    errors.append(f"{case['case_id']}/{arn}: {type(stop_error).__name__}: {stop_error}")
                deadline = time.monotonic() + min(30, deployment.get("driverTimeout", 30))
                while unresolved:
                    discovered = set()
                    for marker in list(unresolved):
                        try:
                            name, payload = unresolved[marker]
                            recovered_arn = find_execution_by_name(service, target, name, payload)
                        except Exception as error:
                            if not retryable_read_error(error):
                                errors.append(f"{case['case_id']}/{marker}: {error}")
                                del unresolved[marker]
                        else:
                            if recovered_arn is not None:
                                discovered.add(recovered_arn)
                                del unresolved[marker]
                    for arn, stop_error in request_execution_stops(service, discovered).items():
                        errors.append(f"{case['case_id']}/{arn}: {type(stop_error).__name__}: {stop_error}")
                    remaining = deadline - time.monotonic()
                    if not unresolved or remaining <= 0:
                        break
                    time.sleep(min(0.5, remaining))
                errors.extend(f"Unreconciled Invoke: {case['case_id']}/{marker}" for marker in unresolved)
            except Exception as error:
                errors.append(f"{case['case_id']}: {type(error).__name__}: {error}")
    save(
        directory / "cleanup.json",
        {
            "runId": deployment["run"],
            "errors": errors,
            "status": "FAILED" if errors else "CLEANUP_REQUESTED",
            "reason": "Run-scoped control release and execution-stop requests; case reports retain task retirement findings",
        },
    )
    return int(bool(errors))
