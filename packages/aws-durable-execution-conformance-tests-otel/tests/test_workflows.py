# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contract tests for the split OpenTelemetry view workflows."""

from pathlib import Path

WORKFLOWS_DIR = Path(__file__).resolve().parents[3] / ".github" / "workflows"
LANGUAGE_WORKFLOWS = {
    "java": WORKFLOWS_DIR / "java-opentelemetry.yml",
    "python": WORKFLOWS_DIR / "python-opentelemetry.yml",
    "typescript": WORKFLOWS_DIR / "typescript-opentelemetry.yml",
}


def test_language_workflows_run_one_parameterized_suite() -> None:
    for path in LANGUAGE_WORKFLOWS.values():
        workflow = path.read_text(encoding="utf-8")

        assert "  workflow_call:" in workflow
        assert "  pull_request:" not in workflow
        assert "  workflow_dispatch:" not in workflow
        assert "OTEL_CASE_COUNT: ${{ inputs.case_count }}" in workflow
        assert "OTEL_SUITE: ${{ inputs.suite }}" in workflow
        assert '--suite "$OTEL_SUITE"' in workflow
        assert "--suite otel-invocation otel-execution" not in workflow


def test_invocation_view_workflow_calls_every_language() -> None:
    workflow = (WORKFLOWS_DIR / "opentelemetry-invocation.yml").read_text(encoding="utf-8")

    assert "name: OpenTelemetry Invocation View" in workflow
    assert "  pull_request:" in workflow
    assert "  workflow_dispatch:" in workflow
    assert workflow.count("suite: otel-invocation") == 3
    assert workflow.count("case_count: 19") == 3
    for path in LANGUAGE_WORKFLOWS.values():
        assert f"uses: ./.github/workflows/{path.name}" in workflow
    assert "test-requirements/otel-invocation/**" in workflow
    assert "test-requirements/otel-execution/**" not in workflow


def test_execution_view_workflow_calls_supported_languages() -> None:
    workflow = (WORKFLOWS_DIR / "opentelemetry-execution.yml").read_text(encoding="utf-8")

    assert "name: OpenTelemetry Execution View" in workflow
    assert "  pull_request:" in workflow
    assert "  workflow_dispatch:" in workflow
    assert workflow.count("suite: otel-execution") == 2
    assert workflow.count("case_count: 3") == 2
    for language in ("python", "typescript"):
        assert f"uses: ./.github/workflows/{LANGUAGE_WORKFLOWS[language].name}" in workflow
    assert f"uses: ./.github/workflows/{LANGUAGE_WORKFLOWS['java'].name}" not in workflow
    assert "test-requirements/otel-execution/**" in workflow
    assert "test-requirements/otel-invocation/**" not in workflow
