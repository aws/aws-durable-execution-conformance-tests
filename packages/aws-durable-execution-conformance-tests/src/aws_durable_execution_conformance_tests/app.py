# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Main pipeline for the durable execution conformance test framework.

1. Deploy all functions in template.yaml via SAM CLI
2. Parse TestingMetadata.TestDescription from each function to discover test IDs
3. For each test description, invoke the function and validate execution history
4. Print a summary of which test descriptions passed / failed
"""

import argparse
import shutil
import sys
import tempfile
import time
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from aws_durable_execution_conformance_tests.config import (
    BUILD_DIR,
    DEFAULT_REGION,
    OUTPUT_DIR,
    STACK_NAME_PREFIX,
    TESTS_DIR,
)
from aws_durable_execution_conformance_tests.extensions import (
    ExtensionError,
    ExtensionRegistry,
    ValidationContext,
    load_extensions,
)
from aws_durable_execution_conformance_tests.history import load_yaml_file
from aws_durable_execution_conformance_tests.report import (
    Report,
    ReportEntry,
    ReportStatus,
    RunMetadata,
)
from aws_durable_execution_conformance_tests.reporters import render_console, write_report
from aws_durable_execution_conformance_tests.sam import (
    BuildRequiredError,
    Deployer,
    Invoker,
    SamCliError,
    delete_stack,
)
from aws_durable_execution_conformance_tests.validate import (
    DescriptionResult,
    ExpectedFailure,
    parse_expected_failures,
    parse_function_descriptions,
    parse_not_implemented,
    validate_description,
)


def _parse_parameter_override(value: str) -> tuple[str, str]:
    """Parse one ``KEY=VALUE`` SAM parameter override."""
    key, separator, parameter_value = value.partition("=")
    if not separator or not key:
        raise argparse.ArgumentTypeError("parameter overrides must use KEY=VALUE")
    return key, parameter_value


def parse_args(
    argv: list[str] | None = None,
    *,
    extension_registry: ExtensionRegistry | None = None,
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate durable execution SDK test requirements against SDK test cases.",
    )
    try:
        registry = extension_registry or load_extensions(TESTS_DIR)
    except ExtensionError as exc:
        parser.error(f"Failed to load conformance extensions: {exc}")
    parser.add_argument(
        "--template",
        required=True,
        help="Path to the SAM template.yaml file.",
    )
    parser.add_argument(
        "--region",
        default=DEFAULT_REGION,
        help=f"AWS region for deployment and validation. Defaults to {DEFAULT_REGION}.",
    )
    parser.add_argument(
        "--name",
        default=uuid.uuid4().hex[:8],
        help="Test run name used as the CloudFormation stack name suffix. "
        "If not provided, a random string is generated.",
    )
    parser.add_argument(
        "--history-dir",
        default=str(OUTPUT_DIR),
        help="Directory to save execution history JSON files. Defaults to output/.",
    )
    # Discover suites from the requirements tree so accepted values track the
    # folders that actually exist without a hardcoded list. Suites may cover
    # operations, cross-cutting capabilities, or integrations.
    discovered_suites: list[str] = sorted(registry.suites)
    valid_suites: list[str] = [*discovered_suites, "all"]

    parser.add_argument(
        "--suite",
        nargs="+",
        default=["all"],
        choices=valid_suites,
        metavar="SUITE",
        help="Only validate requirements from specific suites. "
        "Accepts one or more values (e.g. --suite step serdes). "
        f"Discovered suites: {', '.join(discovered_suites) or '(none)'}. "
        "Defaults to 'all' (validate all suites).",
    )
    parser.add_argument(
        "--image-uri",
        default=None,
        help="Pre-built container image URI (ECR). When provided, the URI is "
        "passed as an ImageUri parameter override during sam deploy.",
    )
    parser.add_argument(
        "--parameter-overrides",
        action="extend",
        nargs="+",
        default=[],
        type=_parse_parameter_override,
        metavar="KEY=VALUE",
        help="Additional SAM template parameter overrides. Explicit values take "
        "precedence over extension-provided parameters.",
    )
    parser.add_argument(
        "--cleanup",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Delete the CloudFormation stack after the run (best-effort, "
        "fire-and-forget). Enabled by default; pass --no-cleanup to keep the "
        "deployed stack for debugging.",
    )
    parser.add_argument(
        "--language",
        required=True,
        metavar="LANGUAGE",
        help="SDK language/runtime under test (free-form, e.g. python, js, java, "
        "dotnet, go). Recorded in the report and used to scope NOT_IMPLEMENTED "
        "resolution (the validator runs one SDK per run).",
    )
    parser.add_argument(
        "--report",
        nargs="+",
        default=["console"],
        choices=["console", "json", "junit"],
        help="Report format(s) to emit. Repeatable (e.g. --report console json junit). Defaults to 'console'.",
    )
    parser.add_argument(
        "--report-file",
        default=None,
        help="Base path for machine reports (json/junit). The format extension is "
        "appended (.json / .xml). Defaults to <history-dir>/report.",
    )
    parser.add_argument(
        "--fail-on",
        default="failed",
        choices=["failed", "failed+uncovered"],
        help="Exit-code policy: which statuses cause a non-zero exit. "
        "'failed' (default) blocks on FAILED and UNEXPECTED_PASSED; "
        "'failed+uncovered' also blocks on UNCOVERED. EXPECTED_FAILED, "
        "NOT_IMPLEMENTED, and OPTIONAL_FAILED never block.",
    )
    try:
        registry.add_arguments(parser)
    except Exception as exc:
        parser.error(f"Failed to configure conformance extensions: {exc}")
    args = parser.parse_args(argv)
    try:
        registry.validate_configuration(args)
    except (ExtensionError, ValueError) as exc:
        parser.error(str(exc))
    args._extension_registry = registry
    return args


def run(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    template_path = args.template
    stack_name = f"{STACK_NAME_PREFIX}-{args.name}"
    run_start = time.monotonic()
    started_at = datetime.now(UTC).isoformat()

    # 1. Deploy
    print(f"=== Deploying stack '{stack_name}' ===")
    deployer = Deployer(template_path=template_path, build_dir=str(BUILD_DIR / stack_name), region=args.region)
    try:
        deployer.build()
        print("  Build succeeded.")
    except SamCliError as e:
        print(f"  Build failed: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        _deploy_validate_report(args, deployer, stack_name, template_path, run_start, started_at)
    finally:
        _maybe_cleanup(args, stack_name)


def _maybe_cleanup(args: argparse.Namespace, stack_name: str) -> None:
    """Best-effort stack teardown after a run (enabled unless --no-cleanup).

    Fire-and-forget: initiates deletion and returns immediately. Never raises --
    a cleanup failure must not mask the run's own exit code.
    """
    if not args.cleanup:
        print(f"\n=== Cleanup skipped (--no-cleanup); stack '{stack_name}' left deployed ===")
        return
    print(f"\n=== Cleanup: deleting stack '{stack_name}' (best-effort, not waiting) ===")
    if delete_stack(stack_name, args.region):
        print("  Delete request submitted.")
    else:
        print(
            "  Cleanup request could not be issued; delete the stack manually if it exists.",
            file=sys.stderr,
        )


def _deploy_validate_report(
    args: argparse.Namespace,
    deployer: Deployer,
    stack_name: str,
    template_path: str,
    run_start: float,
    started_at: str,
) -> None:
    """Deploy the stack, validate every test description, and emit the report.

    Calls ``sys.exit`` with the report's exit code (or 0/1 for early outcomes).
    """
    try:
        registry: ExtensionRegistry = args._extension_registry
        parameter_overrides: dict[str, str] = registry.deployment_parameters(args)
        parameter_overrides.update(dict(args.parameter_overrides))
        secret_parameter_overrides: dict[str, str] = registry.deployment_secrets(args)
        resolve_image_repos: bool = True
        image_repository: str | None = None
        if args.image_uri:
            parameter_overrides["ImageUri"] = args.image_uri
            resolve_image_repos = False
            # Extract repo URI for SAM's --image-repository requirement.
            # Handles both tag format (repo:tag) and digest format (repo@sha256:...).
            if "@" in args.image_uri:
                image_repository = args.image_uri.split("@", 1)[0]
            else:
                image_repository = args.image_uri.rsplit(":", 1)[0]
        deployer.deploy(
            stack_name=stack_name,
            parameter_overrides=parameter_overrides or None,
            secret_parameter_overrides=secret_parameter_overrides or None,
            resolve_image_repos=resolve_image_repos,
            image_repository=image_repository,
        )
        print("  Deploy succeeded.")
    except (BuildRequiredError, SamCliError) as e:
        print(f"  Deploy failed: {e}", file=sys.stderr)
        sys.exit(1)

    # 2. Discover test descriptions from template
    function_descriptions = parse_function_descriptions(template_path)
    if not function_descriptions:
        print("No functions with TestingMetadata.TestDescription found in template.")
        sys.exit(0)
    try:
        expected_failures = parse_expected_failures(template_path)
    except (OSError, ValueError, yaml.YAMLError) as exc:
        print(f"Invalid expected-failure declaration: {exc}", file=sys.stderr)
        sys.exit(1)

    mapped_description_ids = {description_id for _function_name, description_id in function_descriptions}
    stale_expected_failures = sorted(set(expected_failures) - mapped_description_ids)
    if stale_expected_failures:
        joined = ", ".join(stale_expected_failures)
        print(
            f"Expected failure declarations have no mapped test function: {joined}",
            file=sys.stderr,
        )
        sys.exit(1)

    # 3. Filter by suite if specified
    try:
        requirements = registry.discover_requirements(args.suite)
    except ExtensionError as exc:
        print(f"Could not discover test requirements: {exc}", file=sys.stderr)
        sys.exit(1)

    if "all" not in args.suite:
        function_descriptions = [(fn, did) for fn, did in function_descriptions if did in requirements]
        if not function_descriptions:
            print(f"No test descriptions match suite(s) {args.suite!r} in the template.")
            sys.exit(0)

    print(f"\n=== Found {len(function_descriptions)} test description(s) to validate ===")
    for fn, did in function_descriptions:
        print(f"  {fn} -> {did}")

    # 4 & 5. Invoke and assert each test description
    invoker = Invoker(stack_name=stack_name, region=args.region, output_format="json")
    results: list[DescriptionResult] = []

    tmp_dir = tempfile.mkdtemp(prefix="sdk-test-events-")
    try:
        for function_name, description_id in function_descriptions:
            print(f"\n--- Validating test description {description_id} ({function_name}) ---")
            requirement = requirements.get(description_id)
            if not requirement:
                result = DescriptionResult(
                    description_id=description_id,
                    function_name=function_name,
                    passed=False,
                    errors=[f"Test file not found for description: {description_id}"],
                )
            else:
                result = validate_description(
                    function_name,
                    description_id,
                    str(requirement.path),
                    invoker,
                    tmp_dir,
                    output_dir=args.history_dir,
                    region=args.region,
                )
                if result.passed and requirement.suite.validation_hook is not None:
                    result = _run_extension_validation(
                        result=result,
                        hook=requirement.suite.validation_hook,
                        requirement_path=requirement.path,
                        args=args,
                    )
            results.append(result)

            expected_failure = expected_failures.get(description_id)
            status, classified_errors = _classify_result(result, expected_failure)
            if status == ReportStatus.PASSED:
                print("  ✅ PASSED")
                if result.placeholders:
                    print(f"     Placeholders: {result.placeholders}")
            elif status == ReportStatus.EXPECTED_FAILED:
                assert expected_failure is not None
                print("  ⚠️  EXPECTED FAILURE")
                print(f"     {expected_failure.reason}")
                for err in classified_errors:
                    print(f"     {err}")
            elif status == ReportStatus.UNEXPECTED_PASSED:
                print("  ❌ UNEXPECTED PASS")
                for err in classified_errors:
                    print(f"     {err}")
            elif status == ReportStatus.OPTIONAL_FAILED:
                print("  ⚠️  FAILED (optional)")
                for err in classified_errors:
                    print(f"     {err}")
            else:
                print("  ❌ FAILED")
                for err in classified_errors:
                    print(f"     {err}")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    # 5. Build the report model from results + coverage.
    # Coverage check -- descriptions in tests/ with no example in template.
    all_description_ids = sorted(requirements)
    referenced_description_ids = {did for _, did in function_descriptions}
    uncovered = [did for did in all_description_ids if did not in referenced_description_ids]

    # Declared intentional gaps for this SDK's template.
    not_implemented = parse_not_implemented(template_path)

    # Warn on stale declarations: an id declared NotImplemented that is actually
    # covered by an example (e.g. the SDK gained the feature and an example was
    # added, but the declaration was never removed). Only uncovered ids consult
    # not_implemented, so such a declaration would otherwise be silently ignored.
    report_warnings: list[str] = []
    for did in sorted(set(not_implemented) & referenced_description_ids):
        message = (
            f"'{did}' is declared NotImplemented but is covered by an example in "
            f"the template; the declaration may be stale."
        )
        report_warnings.append(message)
        print(f"  WARNING: {message}", file=sys.stderr)

    def _suite_for(description_id: str) -> str | None:
        requirement = requirements.get(description_id)
        return requirement.suite.name if requirement else None

    _description_cache: dict[str, str | None] = {}

    def _description_for(description_id: str) -> str | None:
        if description_id in _description_cache:
            return _description_cache[description_id]
        description: str | None = None
        requirement = requirements.get(description_id)
        if requirement:
            try:
                data = load_yaml_file(str(requirement.path))
                if isinstance(data, dict):
                    description = data.get("description")
            except (OSError, ValueError, yaml.YAMLError):
                description = None
        _description_cache[description_id] = description
        return description

    report = Report(
        run=RunMetadata(
            name=args.name,
            template=template_path,
            region=args.region,
            language=args.language,
            suites=list(args.suite),
            started_at=started_at,
            finished_at=datetime.now(UTC).isoformat(),
            duration_seconds=time.monotonic() - run_start,
        ),
        fail_on=args.fail_on,
        warnings=report_warnings,
    )

    for r in results:
        expected_failure = expected_failures.get(r.description_id)
        status, classified_errors = _classify_result(r, expected_failure)
        report.add(
            ReportEntry(
                id=r.description_id,
                suite=_suite_for(r.description_id),
                status=status,
                function=r.function_name,
                description=_description_for(r.description_id),
                reason=expected_failure.reason if expected_failure else None,
                errors=classified_errors,
            )
        )

    for did in uncovered:
        if did in not_implemented:
            report.add(
                ReportEntry(
                    id=did,
                    suite=_suite_for(did),
                    status=ReportStatus.NOT_IMPLEMENTED,
                    description=_description_for(did),
                    reason=not_implemented[did],
                )
            )
        else:
            report.add(
                ReportEntry(
                    id=did,
                    suite=_suite_for(did),
                    status=ReportStatus.UNCOVERED,
                    description=_description_for(did),
                )
            )

    # 6. Emit reports.
    if "console" in args.report:
        print("\n" + render_console(report))

    machine_formats = [fmt for fmt in args.report if fmt in ("json", "junit")]
    if machine_formats:
        report_base = args.report_file or str(Path(args.history_dir) / "report")
        for fmt in machine_formats:
            path = write_report(report, fmt, report_base)
            print(f"  Wrote {fmt} report to {path}")

    sys.exit(report.exit_code())


def _classify_result(
    result: DescriptionResult,
    expected_failure: ExpectedFailure | None,
) -> tuple[ReportStatus, list[str]]:
    """Classify one validation result, including strict expected failures."""

    if expected_failure is None:
        if result.passed:
            return ReportStatus.PASSED, []
        if result.optional:
            return ReportStatus.OPTIONAL_FAILED, list(result.errors)
        return ReportStatus.FAILED, list(result.errors)

    if result.passed:
        return (
            ReportStatus.UNEXPECTED_PASSED,
            [f"Expected failure unexpectedly passed: {expected_failure.reason}"],
        )
    if tuple(result.errors) == expected_failure.errors:
        return ReportStatus.EXPECTED_FAILED, list(result.errors)
    return (
        ReportStatus.FAILED,
        [
            *result.errors,
            "Expected failure signature did not match; "
            f"expected {list(expected_failure.errors)!r}, got {list(result.errors)!r}",
        ],
    )


def _run_extension_validation(
    *,
    result: DescriptionResult,
    hook: Any,
    requirement_path: Path,
    args: argparse.Namespace,
) -> DescriptionResult:
    """Run an extension hook and preserve the core result/report model."""

    if (
        result.execution_arn is None
        or result.invocation_started_at_ms is None
        or result.invocation_finished_at_ms is None
    ):
        return replace(
            result,
            passed=False,
            errors=["Extension validation could not run because execution metadata is missing"],
        )

    try:
        requirement = load_yaml_file(str(requirement_path))
        if not isinstance(requirement, dict):
            raise ValueError("requirement YAML must contain a mapping")
        context = ValidationContext(
            description_id=result.description_id,
            function_name=result.function_name,
            execution_arn=result.execution_arn,
            invocation_started_at_ms=result.invocation_started_at_ms,
            invocation_finished_at_ms=result.invocation_finished_at_ms,
            region=args.region,
            language=args.language,
            requirement=requirement,
            execution_history=result.execution_history,
            output_dir=Path(args.history_dir),
            placeholders={
                **result.placeholders,
                "EXECUTION_ARN": result.execution_arn,
            },
            options={key: value for key, value in vars(args).items() if not key.startswith("_")},
        )
        errors = list(hook(context))
    except Exception as exc:
        errors = [f"Extension validation failed to run: {exc}"]

    if not errors:
        return result
    return replace(result, passed=False, errors=errors)


if __name__ == "__main__":
    run()
