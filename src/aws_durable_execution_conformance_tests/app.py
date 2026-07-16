# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Main pipeline for the durable execution conformance test framework.

1. Deploy all functions in template.yaml via SAM CLI
2. Parse TestingMetadata.TestDescription from each function to discover test IDs
3. For each test description, invoke the function and validate execution history
4. Print a summary of which test descriptions passed / failed
"""
# ruff: noqa: T201

import argparse
import shutil
import sys
import tempfile
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import yaml

from aws_durable_execution_conformance_tests.config import (
    BUILD_DIR,
    DEFAULT_REGION,
    OUTPUT_DIR,
    STACK_NAME_PREFIX,
    TESTS_DIR,
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
)
from aws_durable_execution_conformance_tests.validate import (
    DescriptionResult,
    discover_suites,
    discover_test_files,
    parse_function_descriptions,
    parse_not_implemented,
    validate_description,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate durable execution SDK test requirements against SDK test cases.",
    )
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
    discovered_suites: list[str] = discover_suites(TESTS_DIR)
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
        "'failed' (default) blocks only on FAILED; 'failed+uncovered' also blocks "
        "on UNCOVERED. NOT_IMPLEMENTED and OPTIONAL_FAILED never block.",
    )
    return parser.parse_args(argv)


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
        parameter_overrides: dict[str, str] | None = None
        resolve_image_repos: bool = True
        image_repository: str | None = None
        if args.image_uri:
            parameter_overrides = {"ImageUri": args.image_uri}
            resolve_image_repos = False
            # Extract repo URI for SAM's --image-repository requirement.
            # Handles both tag format (repo:tag) and digest format (repo@sha256:...).
            if "@" in args.image_uri:
                image_repository = args.image_uri.split("@", 1)[0]
            else:
                image_repository = args.image_uri.rsplit(":", 1)[0]
        deployer.deploy(
            stack_name=stack_name,
            parameter_overrides=parameter_overrides,
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

    # 3. Filter by suite if specified
    test_files = discover_test_files(TESTS_DIR, suite=args.suite)

    if "all" not in args.suite:
        function_descriptions = [(fn, did) for fn, did in function_descriptions if did in test_files]
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
            test_file = test_files.get(description_id)
            if not test_file:
                results.append(
                    DescriptionResult(
                        description_id=description_id,
                        function_name=function_name,
                        passed=False,
                        errors=[f"Test file not found for description: {description_id}"],
                    )
                )
                print("  ❌ FAILED")
                print(f"     Test file not found for description: {description_id}")
                continue
            result = validate_description(
                function_name,
                description_id,
                test_file,
                invoker,
                tmp_dir,
                output_dir=args.history_dir,
                region=args.region,
            )
            results.append(result)

            if result.passed:
                print("  ✅ PASSED")
                if result.placeholders:
                    print(f"     Placeholders: {result.placeholders}")
            elif result.optional:
                print("  ⚠️  FAILED (optional)")
                for err in result.errors:
                    print(f"     {err}")
            else:
                print("  ❌ FAILED")
                for err in result.errors:
                    print(f"     {err}")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    # 5. Build the report model from results + coverage.
    # Coverage check -- descriptions in tests/ with no example in template.
    all_description_ids = sorted(test_files.keys())
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
        path = test_files.get(description_id)
        return Path(path).parent.name if path else None

    _description_cache: dict[str, str | None] = {}

    def _description_for(description_id: str) -> str | None:
        if description_id in _description_cache:
            return _description_cache[description_id]
        description: str | None = None
        path = test_files.get(description_id)
        if path:
            try:
                data = load_yaml_file(path)
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
        if r.passed:
            status = ReportStatus.PASSED
        elif r.optional:
            status = ReportStatus.OPTIONAL_FAILED
        else:
            status = ReportStatus.FAILED
        report.add(
            ReportEntry(
                id=r.description_id,
                suite=_suite_for(r.description_id),
                status=status,
                function=r.function_name,
                description=_description_for(r.description_id),
                errors=list(r.errors),
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


if __name__ == "__main__":
    run()
