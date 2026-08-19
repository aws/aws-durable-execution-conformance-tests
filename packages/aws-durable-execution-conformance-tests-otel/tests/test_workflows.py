# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contract tests for the reusable OpenTelemetry workflows."""

import os
import runpy
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS_DIR = ROOT / ".github" / "workflows"
LANGUAGES = ("java", "javascript", "python")
DISPLAY_NAMES = {"java": "Java", "javascript": "JavaScript", "python": "Python"}
LANGUAGE_WORKFLOWS = {language: WORKFLOWS_DIR / f"{language}-opentelemetry.yml" for language in LANGUAGES}
ORCHESTRATOR = WORKFLOWS_DIR / "opentelemetry-orchestrator.yml"
RESOLVER_WORKFLOW = WORKFLOWS_DIR / "opentelemetry-resolve.yml"
SUITE_WORKFLOW = WORKFLOWS_DIR / "opentelemetry-suite.yml"
LONG_RUNNING_WORKFLOW = WORKFLOWS_DIR / "opentelemetry-long-running.yml"
PREPARE_ACTION = ROOT / ".github" / "actions" / "prepare-otel-example" / "action.yml"
DATADOG_RETENTION_SCRIPT = ROOT / "scripts" / "configure-datadog-retention.py"
ALL_OTEL_WORKFLOWS = {
    *LANGUAGE_WORKFLOWS.values(),
    ORCHESTRATOR,
    RESOLVER_WORKFLOW,
    SUITE_WORKFLOW,
    LONG_RUNNING_WORKFLOW,
}
COLLECTOR_PATH_FILTER = "packages/aws-durable-execution-conformance-tests-otel/collector/**"


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _triggers(workflow: dict) -> dict:
    return workflow.get("on") or workflow[True]


def _run_resolver_validation(resource_prefix: str) -> subprocess.CompletedProcess[str]:
    workflow = _load(RESOLVER_WORKFLOW)
    validation = next(
        step for step in workflow["jobs"]["resolve"]["steps"] if step["name"] == "Validate workflow inputs"
    )
    return subprocess.run(
        ["bash", "-eo", "pipefail", "-c", validation["run"]],
        env={
            **os.environ,
            "ADOT_LAYER_ARN": "",
            "ADOT_RELEASE_REPOSITORY": "aws-observability/aws-otel-python-instrumentation",
            "CONFORMANCE_REPOSITORY": "aws/aws-durable-execution-conformance-tests",
            "LANGUAGE": "python",
            "REQUESTED_PHASE": "short",
            "RESOURCE_PREFIX": resource_prefix,
            "SDK_REPOSITORY": "aws/aws-durable-execution-sdk-python",
        },
        capture_output=True,
        text=True,
        check=False,
    )


def test_one_shared_worker_owns_each_otel_lifecycle() -> None:
    assert ORCHESTRATOR.exists()
    assert RESOLVER_WORKFLOW.exists()
    assert SUITE_WORKFLOW.exists()
    assert LONG_RUNNING_WORKFLOW.exists()
    assert PREPARE_ACTION.exists()
    assert not (WORKFLOWS_DIR / "opentelemetry-suite-orchestrator.yml").exists()
    assert not (WORKFLOWS_DIR / "opentelemetry-long-running-orchestrator.yml").exists()

    for language in LANGUAGES:
        assert not (WORKFLOWS_DIR / f"{language}-opentelemetry-suite.yml").exists()
        assert not (WORKFLOWS_DIR / f"{language}-opentelemetry-long-running.yml").exists()


def test_shared_entry_point_accepts_language_owned_setup() -> None:
    workflow = _load(ORCHESTRATOR)
    call = _triggers(workflow)["workflow_call"]
    inputs = call["inputs"]

    assert set(_triggers(workflow)) == {"workflow_call"}
    for name in (
        "language",
        "resource_prefix",
        "sdk_repository",
        "sdk_ref",
        "conformance_repository",
        "conformance_test_ref",
        "setup_command",
        "prepare_command",
        "contract_test_command",
        "adot_release_repository",
        "adot_layer_arn",
        "collector_compatible_runtime",
        "collector_otlp_endpoint",
        "delay_seconds",
    ):
        assert name in inputs
    assert "runtime_language" not in inputs
    assert inputs["setup_command"]["default"] == ""
    assert inputs["prepare_command"]["default"] == ""
    assert inputs["sdk_ref"]["default"] == ""
    assert inputs["conformance_test_ref"]["default"] == ""
    assert inputs["conformance_repository"]["default"] == ("aws/aws-durable-execution-conformance-tests")
    assert "otlp_endpoint" not in inputs
    for secret in ("DASH0_AUTH_TOKEN", "DATADOG_ACCESS_TOKEN", "DATADOG_API_KEY"):
        assert call["secrets"][secret]["required"] is False
    assert call["secrets"]["DATADOG_APPLICATION_KEY"]["required"] is False

    text = ORCHESTRATOR.read_text(encoding="utf-8")
    assert "setup-java" not in text
    assert "setup-node" not in text
    assert "aws-durable-execution-sdk-java" not in text
    assert "aws-durable-execution-sdk-python" not in text
    assert "aws-durable-execution-sdk-js" not in text
    assert "toolchain must be one of" not in text
    assert "job.workflow_repository" not in text
    assert "job.workflow_sha" not in text


def test_resolver_detects_optional_backend_credentials() -> None:
    workflow = _load(RESOLVER_WORKFLOW)
    call = _triggers(workflow)["workflow_call"]
    resolve = workflow["jobs"]["resolve"]
    steps = {step["name"]: step for step in resolve["steps"]}
    detection = steps["Detect configured telemetry backends"]

    for secret in ("DASH0_AUTH_TOKEN", "DATADOG_ACCESS_TOKEN", "DATADOG_API_KEY"):
        assert call["secrets"][secret]["required"] is False
    for backend in ("dash0", "datadog"):
        assert call["outputs"][f"{backend}_enabled"]["value"] == (f"${{{{ jobs.resolve.outputs.{backend}_enabled }}}}")
        assert resolve["outputs"][f"{backend}_enabled"] == (
            f"${{{{ steps.detect-backends.outputs.{backend}_enabled }}}}"
        )
    assert detection["env"] == {
        "DASH0_AUTH_TOKEN": "${{ secrets.DASH0_AUTH_TOKEN }}",
        "DATADOG_ACCESS_TOKEN": "${{ secrets.DATADOG_ACCESS_TOKEN }}",
        "DATADOG_API_KEY": "${{ secrets.DATADOG_API_KEY }}",
    }
    assert 'echo "dash0_enabled=true" >> "$GITHUB_OUTPUT"' in detection["run"]
    assert '[ -n "$DATADOG_ACCESS_TOKEN" ] && [ -n "$DATADOG_API_KEY" ]' in detection["run"]


def test_prepare_action_runs_arbitrary_language_hooks() -> None:
    action = _load(PREPARE_ACTION)
    inputs = action["inputs"]
    commands = "\n".join(step.get("run", "") for step in action["runs"]["steps"])

    assert "setup-command" in inputs
    assert "prepare-command" in inputs
    assert "contract-test-command" in inputs
    assert 'eval "$SETUP_COMMAND"' in commands
    assert 'eval "$PREPARE_COMMAND"' in commands
    assert 'eval "$CONTRACT_TEST_COMMAND"' in commands
    assert "actions/setup-java" not in PREPARE_ACTION.read_text(encoding="utf-8")
    assert "actions/setup-node" not in PREPARE_ACTION.read_text(encoding="utf-8")


def test_language_workflows_are_thin_presets() -> None:
    expected = {
        "java": {
            "resource_prefix": "j",
            "sdk_repository": "aws/aws-durable-execution-sdk-java",
            "adot_release_repository": "aws-observability/aws-otel-java-instrumentation",
            "collector_compatible_runtime": "java21",
            "collector_otlp_endpoint": "http://localhost:4318",
        },
        "python": {
            "resource_prefix": "p",
            "sdk_repository": "aws/aws-durable-execution-sdk-python",
            "adot_release_repository": "aws-observability/aws-otel-python-instrumentation",
            "collector_compatible_runtime": "python3.13",
            "collector_otlp_endpoint": "http://localhost:4318",
        },
        "javascript": {
            "resource_prefix": "js",
            "sdk_repository": "aws/aws-durable-execution-sdk-js",
            "adot_release_repository": "aws-observability/aws-otel-js-instrumentation",
            "collector_compatible_runtime": "nodejs22.x",
            "collector_otlp_endpoint": "http://localhost:4318",
        },
    }

    for language, path in LANGUAGE_WORKFLOWS.items():
        text = path.read_text(encoding="utf-8")
        workflow = _load(path)
        preset = workflow["jobs"]["conformance"]

        assert set(workflow["jobs"]) == {"conformance"}
        assert preset["uses"] == "./.github/workflows/opentelemetry-orchestrator.yml"
        assert "needs" not in preset
        assert preset["with"]["language"] == language
        assert preset["with"]["sdk_repository"] == expected[language]["sdk_repository"]
        for name in ("resource_prefix", "adot_release_repository"):
            value = expected[language][name]
            assert preset["with"][name] == value
        for name in ("collector_compatible_runtime", "collector_otlp_endpoint"):
            assert preset["with"][name] == expected[language][name]
        assert preset["with"]["delay_seconds"] == "${{ inputs.delay_seconds || '82800' }}"
        assert f"name: {DISPLAY_NAMES[language]} OpenTelemetry" in text
        assert "  pull_request:" in text
        assert "  pull_request:\n    branches: [main]" in text
        assert "  push:" in text
        assert "  schedule:" in text
        assert "  workflow_dispatch:" in text
        assert COLLECTOR_PATH_FILTER in text
        assert "hatch run validate" not in text


@pytest.mark.parametrize(
    ("resource_prefix", "expected_return_code"),
    [
        ("abc123-x", 0),
        ("abc123-xy", 1),
    ],
)
def test_resource_prefix_validation_matches_lambda_name_limit(
    resource_prefix: str,
    expected_return_code: int,
) -> None:
    result = _run_resolver_validation(resource_prefix)

    assert result.returncode == expected_return_code
    if expected_return_code:
        assert "resource_prefix must contain 1-8 lowercase resource-safe characters" in result.stdout


def test_python_preset_preserves_its_external_caller_contract() -> None:
    workflow = _load(LANGUAGE_WORKFLOWS["python"])
    call = _triggers(workflow)["workflow_call"]
    preset = workflow["jobs"]["conformance"]["with"]

    assert call["inputs"]["python_sdk_ref"]["required"] is True
    assert "conformance_test_ref" in call["inputs"]
    assert preset["sdk_ref"] == "${{ inputs.python_sdk_ref || '' }}"
    assert preset["conformance_test_ref"] == "${{ inputs.conformance_test_ref || '' }}"
    for secret in (
        "CONFORMANCE_TEST_ROLE_ARN",
        "CONFORMANCE_TEST_ACCOUNT_ID",
        "CONFORMANCE_TEST_LAMBDA_EXECUTION_ROLE_ARN",
    ):
        assert call["secrets"][secret]["required"] is True
    for secret in ("DASH0_AUTH_TOKEN", "DATADOG_ACCESS_TOKEN", "DATADOG_API_KEY"):
        assert call["secrets"][secret]["required"] is False
    assert call["secrets"]["DATADOG_APPLICATION_KEY"]["required"] is False
    assert "otlp_endpoint" not in call["inputs"]
    assert "otlp_endpoint" not in preset


def test_orchestrator_owns_suite_and_long_running_views() -> None:
    jobs = _load(ORCHESTRATOR)["jobs"]

    assert set(jobs) == {
        "resolve",
        "invocation",
        "execution",
        "long-running-invocation",
        "long-running-execution",
    }
    assert jobs["resolve"]["uses"] == "./.github/workflows/opentelemetry-resolve.yml"
    assert jobs["resolve"]["secrets"] == {
        "DASH0_AUTH_TOKEN": "${{ secrets.DASH0_AUTH_TOKEN }}",
        "DATADOG_ACCESS_TOKEN": "${{ secrets.DATADOG_ACCESS_TOKEN }}",
        "DATADOG_API_KEY": "${{ secrets.DATADOG_API_KEY }}",
    }
    assert jobs["invocation"]["uses"] == "./.github/workflows/opentelemetry-suite.yml"
    assert jobs["execution"]["uses"] == "./.github/workflows/opentelemetry-suite.yml"
    assert jobs["invocation"]["with"]["suite"] == "otel-invocation"
    assert jobs["execution"]["with"]["suite"] == "otel-execution"
    for view in ("invocation", "execution"):
        assert jobs[view]["with"]["resource_prefix"] == "${{ inputs.resource_prefix }}"
        assert jobs[view]["with"]["dash0_enabled"] == ("${{ needs.resolve.outputs.dash0_enabled == 'true' }}")
        assert jobs[view]["with"]["datadog_enabled"] == ("${{ needs.resolve.outputs.datadog_enabled == 'true' }}")
    for view in ("invocation", "execution"):
        initial = jobs[f"long-running-{view}"]
        assert initial["uses"] == "./.github/workflows/opentelemetry-long-running.yml"
        assert initial["with"]["view"] == view
    for name, job in jobs.items():
        if name == "resolve":
            continue
        assert job["needs"] == "resolve"
        assert job["with"]["sdk_ref"] == "${{ needs.resolve.outputs.sdk_ref }}"
        assert job["with"]["conformance_test_sha"] == "${{ needs.resolve.outputs.conformance_test_sha }}"


def test_suite_worker_is_parameterized_by_language_and_backend() -> None:
    workflow = _load(SUITE_WORKFLOW)
    text = SUITE_WORKFLOW.read_text(encoding="utf-8")
    call = _triggers(workflow)["workflow_call"]
    backend = workflow["jobs"]["backend"]

    assert set(_triggers(workflow)) == {"workflow_call"}
    assert backend["strategy"]["matrix"]["backend"] == (
        '${{ fromJSON(inputs.dash0_enabled && \'["xray", "dash0"]\' || \'["xray"]\') }}'
    )
    for job in ("backend", "datadog", "s3_collector"):
        name = workflow["jobs"][job]["name"]
        assert "inputs.suite == 'otel-invocation'" in name
        assert "'Invocation' || 'Execution'" in name
    assert "ADOT/X-Ray" in backend["name"]
    assert "Community/Dash0" in backend["name"]
    assert "Community/Datadog" in workflow["jobs"]["datadog"]["name"]
    assert "Community/S3" in workflow["jobs"]["s3_collector"]["name"]
    assert workflow["concurrency"]["group"] == (
        "${{ inputs.language }}-otel-${{ inputs.suite }}-${{ inputs.aws_region }}"
    )
    assert call["inputs"]["language"]["required"] is True
    assert call["inputs"]["resource_prefix"]["required"] is True
    assert call["inputs"]["conformance_repository"]["required"] is True
    assert call["inputs"]["setup_command"]["default"] == ""
    assert call["inputs"]["dash0_enabled"]["default"] is False
    assert call["inputs"]["datadog_enabled"]["default"] is False
    assert "otlp_endpoint" not in call["inputs"]
    assert "examples_dir" in call["inputs"]
    assert (
        "format('packages/aws-durable-execution-conformance-tests-otel/examples/{0}', "
        "inputs.language) }}/template.yaml" in text
    )
    assert "runtime_language" not in call["inputs"]
    assert '--language "$LANGUAGE"' in text
    assert '"OtelSuite=$OTEL_SUITE"' in text
    assert '"OtelServiceName=$OTEL_RESOURCE_SERVICE_NAME"' in text
    assert "--report console json junit github" in text


def test_workers_load_support_from_their_own_workflow_revision() -> None:
    expected_support_checkout = {
        "repository": "${{ job.workflow_repository }}",
        "ref": "${{ job.workflow_sha }}",
        "path": ".build/workflow-support",
    }
    expected_action = "./.build/workflow-support/.github/actions/prepare-otel-example"

    for path, job_names, conformance_ref in (
        (SUITE_WORKFLOW, ("backend", "datadog", "s3_collector"), "${{ inputs.conformance_test_sha }}"),
        (
            LONG_RUNNING_WORKFLOW,
            ("run",),
            "${{ steps.state.outputs.source_revision || inputs.conformance_test_sha }}",
        ),
    ):
        workflow = _load(path)
        for job_name in job_names:
            steps = {step["name"]: step for step in workflow["jobs"][job_name]["steps"]}
            assert steps["Check out conformance tests"]["with"] == {
                "repository": "${{ inputs.conformance_repository }}",
                "ref": conformance_ref,
            }
            assert steps["Check out workflow support"]["with"] == expected_support_checkout
            assert steps["Prepare conformance example"]["uses"] == expected_action

    long_running_steps = {step["name"]: step for step in _load(LONG_RUNNING_WORKFLOW)["jobs"]["run"]["steps"]}
    assert long_running_steps["Check out next conformance tests"]["with"] == {
        "repository": "${{ inputs.conformance_repository }}",
        "ref": "${{ inputs.conformance_test_sha }}",
        "clean": False,
    }
    assert long_running_steps["Prepare next conformance example"]["uses"] == expected_action


def test_optional_dash0_and_s3_resources_remain_stable() -> None:
    workflow = _load(SUITE_WORKFLOW)
    backend = workflow["jobs"]["backend"]
    s3 = workflow["jobs"]["s3_collector"]
    text = SUITE_WORKFLOW.read_text(encoding="utf-8")

    assert backend["env"]["DASH0_API_URL"] == "https://api.us-west-2.aws.dash0.com"
    assert backend["env"]["DASH0_OTLP_ENDPOINT"] == "https://ingress.us-west-2.aws.dash0.com"
    assert backend["env"]["DASH0_AUTH_TOKEN"] == "${{ secrets.DASH0_AUTH_TOKEN }}"
    assert backend["env"]["OTEL_EXPORTER_OTLP_HEADERS"] == ("Authorization=Bearer%20${{ secrets.DASH0_AUTH_TOKEN }}")
    assert backend["if"] == s3["if"]
    assert backend["if"] == (
        "github.event_name != 'pull_request' || "
        "(github.base_ref == 'main' && github.event.pull_request.head.repo.full_name == github.repository)"
    )
    assert s3["env"]["TEST_STACK_NAME"].startswith("conformance-tests-${{ inputs.language }}-s3-")
    assert 'OTEL_S3_BUCKET="dex-otel-${LANGUAGE}-${OTEL_VIEW_SUFFIX}-${TEST_ACCOUNT_ID}-${AWS_REGION}"' in text
    assert "aws s3api head-bucket" in text
    assert 'aws s3 rm "s3://$OTEL_S3_BUCKET/$OTEL_S3_PREFIX" --recursive' in text
    assert "aws s3api delete-bucket" not in text
    assert "aws lambda delete-layer-version" not in text


def test_datadog_runs_beside_dash0_with_separate_credentials() -> None:
    workflow = _load(SUITE_WORKFLOW)
    datadog = workflow["jobs"]["datadog"]
    steps = {step["name"]: step for step in datadog["steps"]}
    commands = "\n".join(step.get("run", "") for step in datadog["steps"])

    assert datadog["if"] == (
        "inputs.datadog_enabled && "
        "(github.event_name != 'pull_request' || "
        "(github.base_ref == 'main' && github.event.pull_request.head.repo.full_name == github.repository))"
    )
    assert datadog["env"]["DATADOG_ACCESS_TOKEN"] == "${{ secrets.DATADOG_ACCESS_TOKEN }}"
    assert datadog["env"]["DATADOG_OTLP_ENDPOINT"] == "https://otlp.datadoghq.com"
    assert datadog["env"]["OTEL_EXPORTER_OTLP_HEADERS"] == "dd-api-key=${{ secrets.DATADOG_API_KEY }}"
    assert datadog["env"]["TEST_NAME"] == (
        "${{ inputs.resource_prefix }}-datadog-${{ inputs.suite == 'otel-invocation' && 'inv' || 'exec' }}"
    )
    assert datadog["env"]["TEST_STACK_NAME"] == f"conformance-tests-{datadog['env']['TEST_NAME']}"
    assert datadog["env"]["LEGACY_DATADOG_STACK_NAME"] == (
        "conformance-tests-${{ inputs.language }}-datadog-${{ inputs.suite == 'otel-invocation' && 'inv' || 'exec' }}"
    )
    assert datadog["concurrency"] == {
        "group": "${{ inputs.language }}-otel-datadog-${{ inputs.aws_region }}",
        "cancel-in-progress": False,
    }
    assert steps["Configure Datadog trace retention"]["run"] == (
        "hatch run python scripts/configure-datadog-retention.py"
    )
    assert steps["Configure Datadog trace retention"]["env"] == {
        "DATADOG_API_KEY": "${{ secrets.DATADOG_API_KEY }}",
        "DATADOG_APPLICATION_KEY": "${{ secrets.DATADOG_APPLICATION_KEY }}",
    }
    assert "--otel-exporter community" in commands
    assert '--otel-endpoint "$DATADOG_OTLP_ENDPOINT"' in commands
    assert "--otel-poll-interval 15" in commands
    assert "--otel-backend datadog" in commands
    assert "--max-workers 2" in commands
    assert "--no-cleanup" in commands
    assert "DD_API_KEY" not in datadog["env"]
    assert "DD_APPLICATION_KEY" not in datadog["env"]
    assert "DATADOG_OTLP_HEADERS" not in datadog["env"]
    assert steps["Delete rolled-back test stacks"]["env"]["LEGACY_STACK_PREFIX"] == (
        "${{ inputs.legacy_stack_prefix }}"
    )
    assert 'stack_names+=("$LEGACY_DATADOG_STACK_NAME")' in steps["Delete rolled-back test stacks"]["run"]
    assert steps["Upload reports and histories"]["with"]["if-no-files-found"] == "ignore"


def test_maximum_resource_prefix_fits_datadog_target_lambda_limit() -> None:
    resource_prefix = "abc123-x"
    test_name = f"{resource_prefix}-datadog-exec"
    target_name = f"conformance-tests-{test_name}-otel-execution-11-target"

    assert len(resource_prefix) == 8
    assert len(target_name) == 64


def test_datadog_retention_setup_is_optional(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("DATADOG_API_KEY", "api-secret")
    monkeypatch.delenv("DATADOG_APPLICATION_KEY", raising=False)
    namespace = runpy.run_path(str(DATADOG_RETENTION_SCRIPT), run_name="datadog_retention_script")

    namespace["main"]()

    assert capsys.readouterr().out == (
        "Skipping automatic Datadog retention setup because DATADOG_APPLICATION_KEY is not configured; "
        "a 100% retention filter for service:durable-execution-conformance must already exist\n"
    )


def test_long_running_worker_is_reusable_and_language_neutral() -> None:
    workflow = _load(LONG_RUNNING_WORKFLOW)
    text = LONG_RUNNING_WORKFLOW.read_text(encoding="utf-8")
    call = _triggers(workflow)["workflow_call"]

    assert set(_triggers(workflow)) == {"workflow_call"}
    for name in (
        "language",
        "resource_prefix",
        "conformance_repository",
        "setup_command",
        "prepare_command",
    ):
        assert name in call["inputs"]
    assert "runtime_language" not in call["inputs"]
    assert workflow["concurrency"]["group"].startswith("${{ inputs.language }}-otel-long-running-")
    assert workflow["env"]["EXAMPLES_DIR"] == (
        "${{ inputs.examples_dir || format('packages/aws-durable-execution-conformance-tests-otel"
        "/examples/{0}', inputs.language) }}"
    )
    assert workflow["env"]["STATE_FILE"] == "/tmp/${{ inputs.language }}-otel-long-running-state.json"
    assert workflow["env"]["TEST_TEMPLATE"] == (
        "${{ inputs.examples_dir || format('packages/aws-durable-execution-conformance-tests-otel"
        "/examples/{0}', inputs.language) }}/template-long-running.yaml"
    )
    assert '--language "$LANGUAGE"' in text
    assert "aws cloudformation delete-stack" not in text
    assert "setup-java" not in text
    assert "setup-node" not in text


def test_otel_workflows_only_delete_rolled_back_stacks() -> None:
    allowed_recovery_steps = {
        "Delete rolled-back test stack",
        "Delete rolled-back test stacks",
    }

    for path in ALL_OTEL_WORKFLOWS:
        workflow = _load(path)
        for job in workflow["jobs"].values():
            for step in job.get("steps", []):
                command = step.get("run", "")
                if "hatch run validate" in command:
                    assert "--no-cleanup" in command, path
                if "aws_durable_execution_conformance_tests_otel.long_running check" in command:
                    assert "--no-cleanup" in command, path
                if "aws cloudformation delete-stack" in command:
                    assert step["name"] in allowed_recovery_steps, (path, step["name"])


def test_otel_stack_names_do_not_depend_on_run_numbers() -> None:
    for path in (SUITE_WORKFLOW, LONG_RUNNING_WORKFLOW):
        workflow = _load(path)
        environments = [workflow.get("env", {})]
        environments.extend(job.get("env", {}) for job in workflow["jobs"].values())
        for environment in environments:
            for variable in ("TEST_NAME", "TEST_STACK_NAME"):
                value = str(environment.get(variable, ""))
                assert "github.run_" not in value.lower(), (path, variable)
                assert "GITHUB_RUN_" not in value, (path, variable)
