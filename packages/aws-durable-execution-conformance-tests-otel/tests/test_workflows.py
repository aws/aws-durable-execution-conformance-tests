# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contract tests for the reusable OpenTelemetry workflows."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS_DIR = ROOT / ".github" / "workflows"
LANGUAGES = ("java", "javascript", "python")
DISPLAY_NAMES = {"java": "Java", "javascript": "JavaScript", "python": "Python"}
LANGUAGE_WORKFLOWS = {language: WORKFLOWS_DIR / f"{language}-opentelemetry.yml" for language in LANGUAGES}
ORCHESTRATOR = WORKFLOWS_DIR / "opentelemetry.yml"
RESOLVER_WORKFLOW = WORKFLOWS_DIR / "opentelemetry-resolve.yml"
SUITE_WORKFLOW = WORKFLOWS_DIR / "opentelemetry-suite.yml"
LONG_RUNNING_ORCHESTRATOR = WORKFLOWS_DIR / "opentelemetry-long-running-orchestrator.yml"
LONG_RUNNING_WORKFLOW = WORKFLOWS_DIR / "opentelemetry-long-running.yml"
PREPARE_ACTION = ROOT / ".github" / "actions" / "prepare-otel-example" / "action.yml"
ALL_OTEL_WORKFLOWS = {
    *LANGUAGE_WORKFLOWS.values(),
    ORCHESTRATOR,
    RESOLVER_WORKFLOW,
    SUITE_WORKFLOW,
    LONG_RUNNING_ORCHESTRATOR,
    LONG_RUNNING_WORKFLOW,
}
COLLECTOR_PATH_FILTER = "packages/aws-durable-execution-conformance-tests-otel/collector/**"


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _triggers(workflow: dict) -> dict:
    return workflow.get("on") or workflow[True]


def test_one_shared_worker_owns_each_otel_lifecycle() -> None:
    assert ORCHESTRATOR.exists()
    assert RESOLVER_WORKFLOW.exists()
    assert SUITE_WORKFLOW.exists()
    assert LONG_RUNNING_ORCHESTRATOR.exists()
    assert LONG_RUNNING_WORKFLOW.exists()
    assert PREPARE_ACTION.exists()

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
        "sdk_repository",
        "sdk_ref",
        "conformance_repository",
        "conformance_test_sha",
        "setup_command",
        "prepare_command",
        "contract_test_command",
        "adot_release_repository",
        "adot_layer_arn",
        "collector_compatible_runtime",
        "collector_otlp_endpoint",
    ):
        assert name in inputs
    assert "runtime_language" not in inputs
    assert inputs["setup_command"]["default"] == ""
    assert inputs["prepare_command"]["default"] == ""
    assert inputs["conformance_repository"]["default"] == ("aws/aws-durable-execution-conformance-tests")

    text = ORCHESTRATOR.read_text(encoding="utf-8")
    assert "setup-java" not in text
    assert "setup-node" not in text
    assert "aws-durable-execution-sdk-java" not in text
    assert "aws-durable-execution-sdk-python" not in text
    assert "aws-durable-execution-sdk-js" not in text
    assert "toolchain must be one of" not in text
    assert "job.workflow_repository" not in text
    assert "job.workflow_sha" not in text


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
            "collector_otlp_endpoint": "http://localhost:4317",
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
        resolver_preset = workflow["jobs"]["resolve"]
        preset = workflow["jobs"]["conformance"]
        long_running_preset = workflow["jobs"]["long-running"]

        assert set(workflow["jobs"]) == {"resolve", "conformance", "long-running"}
        assert resolver_preset["uses"] == "./.github/workflows/opentelemetry-resolve.yml"
        assert preset["uses"] == "./.github/workflows/opentelemetry.yml"
        assert long_running_preset["uses"] == "./.github/workflows/opentelemetry-long-running-orchestrator.yml"
        assert preset["needs"] == "resolve"
        assert long_running_preset["needs"] == "resolve"
        for job in (resolver_preset, preset, long_running_preset):
            assert job["with"]["language"] == language
            assert job["with"]["sdk_repository"] == expected[language]["sdk_repository"]
        for name in ("resource_prefix", "adot_release_repository"):
            value = expected[language][name]
            assert resolver_preset["with"][name] == value
            assert long_running_preset["with"][name] == value
        for name in ("collector_compatible_runtime", "collector_otlp_endpoint"):
            assert preset["with"][name] == expected[language][name]
            assert name not in long_running_preset["with"]
        for job in (preset, long_running_preset):
            assert job["with"]["sdk_ref"] == "${{ needs.resolve.outputs.sdk_ref }}"
            assert job["with"]["conformance_test_sha"] == "${{ needs.resolve.outputs.conformance_test_sha }}"
        for name in ("checkout_sdk", "setup_command", "prepare_command", "contract_test_command"):
            assert long_running_preset["with"].get(name) == preset["with"].get(name)
        assert f"name: {DISPLAY_NAMES[language]} OpenTelemetry" in text
        assert "  pull_request:" in text
        assert "  push:" in text
        assert "  schedule:" in text
        assert "  workflow_dispatch:" in text
        assert COLLECTOR_PATH_FILTER in text
        assert "hatch run validate" not in text


def test_python_preset_preserves_its_external_caller_contract() -> None:
    workflow = _load(LANGUAGE_WORKFLOWS["python"])
    call = _triggers(workflow)["workflow_call"]
    resolver = workflow["jobs"]["resolve"]["with"]
    preset = workflow["jobs"]["conformance"]["with"]
    long_running = workflow["jobs"]["long-running"]["with"]

    assert call["inputs"]["python_sdk_ref"]["required"] is True
    assert "conformance_test_ref" in call["inputs"]
    assert resolver["sdk_ref"] == "${{ inputs.python_sdk_ref || '' }}"
    assert resolver["conformance_test_ref"] == "${{ inputs.conformance_test_ref || '' }}"
    for job in (preset, long_running):
        assert job["sdk_ref"] == "${{ needs.resolve.outputs.sdk_ref }}"
        assert job["conformance_test_sha"] == "${{ needs.resolve.outputs.conformance_test_sha }}"
    for secret in (
        "CONFORMANCE_TEST_ROLE_ARN",
        "CONFORMANCE_TEST_ACCOUNT_ID",
        "CONFORMANCE_TEST_LAMBDA_EXECUTION_ROLE_ARN",
        "DASH0_AUTH_TOKEN",
    ):
        assert call["secrets"][secret]["required"] is True


def test_orchestrators_split_suite_and_long_running_views() -> None:
    suite_jobs = _load(ORCHESTRATOR)["jobs"]
    long_running_jobs = _load(LONG_RUNNING_ORCHESTRATOR)["jobs"]

    assert set(suite_jobs) == {"invocation", "execution"}
    assert set(long_running_jobs) == {"invocation", "execution"}
    assert suite_jobs["invocation"]["uses"] == "./.github/workflows/opentelemetry-suite.yml"
    assert suite_jobs["execution"]["uses"] == "./.github/workflows/opentelemetry-suite.yml"
    assert suite_jobs["invocation"]["with"]["suite"] == "otel-invocation"
    assert suite_jobs["execution"]["with"]["suite"] == "otel-execution"
    for view in ("invocation", "execution"):
        initial = long_running_jobs[view]
        assert initial["uses"] == "./.github/workflows/opentelemetry-long-running.yml"
        assert initial["with"]["view"] == view


def test_suite_worker_is_parameterized_by_language_and_backend() -> None:
    workflow = _load(SUITE_WORKFLOW)
    text = SUITE_WORKFLOW.read_text(encoding="utf-8")
    call = _triggers(workflow)["workflow_call"]
    backend = workflow["jobs"]["backend"]

    assert set(_triggers(workflow)) == {"workflow_call"}
    assert backend["strategy"]["matrix"]["backend"] == ["xray", "dash0"]
    assert backend["name"] == "${{ matrix.backend == 'xray' && 'ADOT + X-Ray' || 'Community layer + Dash0' }}"
    assert workflow["jobs"]["datadog"]["name"] == "Community layer + Datadog"
    assert workflow["jobs"]["s3_collector"]["name"] == "Community layer + S3 collector"
    assert workflow["concurrency"]["group"] == (
        "${{ inputs.language }}-otel-${{ inputs.suite }}-${{ inputs.aws_region }}"
    )
    assert call["inputs"]["language"]["required"] is True
    assert call["inputs"]["conformance_repository"]["required"] is True
    assert call["inputs"]["setup_command"]["default"] == ""
    assert "examples/${{ inputs.language }}/template.yaml" in text
    assert "runtime_language" not in call["inputs"]
    assert '--language "$LANGUAGE"' in text
    assert '"OtelSuite=$OTEL_SUITE"' in text
    assert '"OtelServiceName=$OTEL_RESOURCE_SERVICE_NAME"' in text
    assert "--report console json junit github" in text
    assert "--report console github" in text


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


def test_dash0_and_s3_resources_remain_stable() -> None:
    workflow = _load(SUITE_WORKFLOW)
    backend = workflow["jobs"]["backend"]
    s3 = workflow["jobs"]["s3_collector"]
    text = SUITE_WORKFLOW.read_text(encoding="utf-8")

    assert backend["env"]["DASH0_API_URL"] == "https://api.us-west-2.aws.dash0.com"
    assert backend["env"]["DASH0_OTLP_ENDPOINT"] == "https://ingress.us-west-2.aws.dash0.com"
    assert backend["env"]["DASH0_AUTH_TOKEN"] == "${{ secrets.DASH0_AUTH_TOKEN }}"
    assert backend["env"]["OTEL_EXPORTER_OTLP_HEADERS"] == ("Authorization=Bearer%20${{ secrets.DASH0_AUTH_TOKEN }}")
    assert s3["env"]["TEST_STACK_NAME"].startswith("conformance-tests-${{ inputs.language }}-s3-")
    assert 'OTEL_S3_BUCKET="dex-otel-${LANGUAGE}-${OTEL_VIEW_SUFFIX}-${TEST_ACCOUNT_ID}-${AWS_REGION}"' in text
    assert "aws s3api head-bucket" in text
    assert 'aws s3 rm "s3://$OTEL_S3_BUCKET/$OTEL_S3_PREFIX" --recursive' in text
    assert "aws s3api delete-bucket" not in text
    assert "aws lambda delete-layer-version" not in text


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
        "packages/aws-durable-execution-conformance-tests-otel/examples/${{ inputs.language }}"
    )
    assert workflow["env"]["STATE_FILE"] == "/tmp/${{ inputs.language }}-otel-long-running-state.json"
    assert workflow["env"]["TEST_TEMPLATE"].endswith("examples/${{ inputs.language }}/template-long-running.yaml")
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
