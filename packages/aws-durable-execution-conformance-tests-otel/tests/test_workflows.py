# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contract tests for the language-grouped OpenTelemetry workflows."""

from pathlib import Path

WORKFLOWS_DIR = Path(__file__).resolve().parents[3] / ".github" / "workflows"
LANGUAGES = ("java", "python", "typescript")
DISPLAY_NAMES = {"java": "Java", "python": "Python", "typescript": "TypeScript"}
LANGUAGE_WORKFLOWS = {language: WORKFLOWS_DIR / f"{language}-opentelemetry.yml" for language in LANGUAGES}
SUITE_WORKFLOWS = {language: WORKFLOWS_DIR / f"{language}-opentelemetry-suite.yml" for language in LANGUAGES}
LONG_RUNNING_WORKFLOWS = {
    language: WORKFLOWS_DIR / f"{language}-opentelemetry-long-running.yml" for language in LANGUAGES
}
SUPPORTED_VIEWS = {
    "java": ("invocation",),
    "python": ("invocation", "execution"),
    "typescript": ("invocation", "execution"),
}
VIEW_SUFFIX = "${{ inputs.suite == 'otel-invocation' && 'inv' || 'exec' }}"
COLLECTOR_PATH_FILTER = "packages/aws-durable-execution-conformance-tests-otel/collector/**"


def test_suite_workflows_run_one_parameterized_suite() -> None:
    for path in SUITE_WORKFLOWS.values():
        workflow = path.read_text(encoding="utf-8")

        assert "  workflow_call:" in workflow
        assert "  pull_request:" not in workflow
        assert "  push:" not in workflow
        assert "  workflow_dispatch:" not in workflow
        assert "OTEL_CASE_COUNT: ${{ inputs.case_count }}" in workflow
        assert "OTEL_SUITE: ${{ inputs.suite }}" in workflow
        assert '--suite "$OTEL_SUITE"' in workflow
        report_options = [line.strip() for line in workflow.splitlines() if line.strip().startswith("--report ")]
        assert len(report_options) == workflow.count("hatch run validate")
        assert all("github" in options for options in report_options)
        concurrency_group = next(line for line in workflow.splitlines() if line.startswith("  group:"))
        assert "${{ inputs.suite }}" in concurrency_group
        assert "${{ inputs.aws_region }}" in concurrency_group
        assert "  xray:" in workflow
        assert "    name: ADOT + X-Ray" in workflow
        assert "  s3_collector:" in workflow
        assert "    name: Community layer + S3 collector" in workflow


def test_suite_workflows_use_language_and_view_specific_resources() -> None:
    for language, path in SUITE_WORKFLOWS.items():
        workflow = path.read_text(encoding="utf-8")

        assert f"TEST_NAME: {language}-xray-{VIEW_SUFFIX}" in workflow
        assert f"TEST_STACK_NAME: conformance-tests-{language}-s3-{VIEW_SUFFIX}" in workflow
        assert f"TEST_NAME: {language}-s3-{VIEW_SUFFIX}" in workflow
        assert f"name: {language}-otel-xray-${{{{ inputs.suite }}}}-${{{{ github.run_id }}}}" in workflow
        assert f"name: {language}-otel-s3-${{{{ inputs.suite }}}}-${{{{ github.run_id }}}}" in workflow
        assert f"dex-otel-{language}-${{OTEL_VIEW_SUFFIX}}-" in workflow


def test_shared_view_templates_receive_the_selected_suite() -> None:
    for language in ("python", "typescript"):
        workflow = SUITE_WORKFLOWS[language].read_text(encoding="utf-8")

        assert workflow.count('"OtelSuite=$OTEL_SUITE"') == workflow.count("hatch run validate")


def test_language_workflows_own_all_supported_views() -> None:
    for language, path in LANGUAGE_WORKFLOWS.items():
        workflow = path.read_text(encoding="utf-8")
        views = SUPPORTED_VIEWS[language]

        assert f"name: {DISPLAY_NAMES[language]} OpenTelemetry" in workflow
        assert "  pull_request:" in workflow
        assert "  push:" in workflow
        assert "  schedule:" in workflow
        assert "  workflow_dispatch:" in workflow
        assert COLLECTOR_PATH_FILTER in workflow
        assert f"uses: ./.github/workflows/{SUITE_WORKFLOWS[language].name}" in workflow
        assert f"uses: ./.github/workflows/{LONG_RUNNING_WORKFLOWS[language].name}" in workflow
        assert workflow.count("case_count: 19") == len(views)
        assert workflow.count("delay_seconds: >-") == len(views)
        for view in views:
            assert f"suite: otel-{view}" in workflow
            assert f"view: {view}" in workflow or language == "java"
            assert f"test-requirements/otel-{view}/**" in workflow


def test_long_running_workflows_are_reusable_only() -> None:
    for path in LONG_RUNNING_WORKFLOWS.values():
        workflow = path.read_text(encoding="utf-8")

        assert "  workflow_call:" in workflow
        assert "  pull_request:" not in workflow
        assert "  push:" not in workflow
        assert "  schedule:" not in workflow
        assert "  workflow_dispatch:" not in workflow


def test_view_grouped_workflows_were_removed() -> None:
    assert not (WORKFLOWS_DIR / "opentelemetry-invocation.yml").exists()
    assert not (WORKFLOWS_DIR / "opentelemetry-execution.yml").exists()
