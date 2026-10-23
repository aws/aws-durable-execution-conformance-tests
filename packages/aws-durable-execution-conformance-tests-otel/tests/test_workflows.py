# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contract tests for the reusable OpenTelemetry workflows."""

import json
import os
import runpy
import shlex
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS_DIR = ROOT / ".github" / "workflows"
LANGUAGES = ("java", "javascript", "python")
ORCHESTRATOR = WORKFLOWS_DIR / "opentelemetry-orchestrator.yml"
RESOLVER_WORKFLOW = WORKFLOWS_DIR / "opentelemetry-resolve.yml"
SUITE_WORKFLOW = WORKFLOWS_DIR / "opentelemetry-suite.yml"
LONG_RUNNING_WORKFLOW = WORKFLOWS_DIR / "opentelemetry-long-running.yml"
PREPARE_ACTION = ROOT / ".github" / "actions" / "prepare-otel-example" / "action.yml"
DATADOG_RETENTION_SCRIPT = ROOT / "scripts" / "configure-datadog-retention.py"
ALL_OTEL_WORKFLOWS = {
    ORCHESTRATOR,
    RESOLVER_WORKFLOW,
    SUITE_WORKFLOW,
    LONG_RUNNING_WORKFLOW,
}


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _triggers(workflow: dict) -> dict:
    return workflow.get("on") or workflow[True]


def _run_resolver_validation(resource_prefix: str, *, case_count: str = "27") -> subprocess.CompletedProcess[str]:
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
            "REQUESTED_CASE_COUNT": case_count,
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
        assert not (WORKFLOWS_DIR / f"{language}-opentelemetry.yml").exists()
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
        "examples_dir",
    ):
        assert name in inputs
    assert inputs["examples_dir"]["required"] is True
    assert "default" not in inputs["examples_dir"]
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


def test_resolver_disables_optional_backend_runs() -> None:
    workflow = _load(RESOLVER_WORKFLOW)
    call = _triggers(workflow)["workflow_call"]
    resolve = workflow["jobs"]["resolve"]
    steps = {step["name"]: step for step in resolve["steps"]}

    assert set(call["outputs"]) == {"conformance_test_sha", "dash0_enabled", "datadog_enabled", "sdk_ref"}
    assert set(resolve["outputs"]) == {"conformance_test_sha", "dash0_enabled", "datadog_enabled", "sdk_ref"}
    for secret in ("DASH0_AUTH_TOKEN", "DATADOG_ACCESS_TOKEN", "DATADOG_API_KEY"):
        assert call["secrets"][secret]["required"] is False
    for backend in ("dash0", "datadog"):
        assert call["outputs"][f"{backend}_enabled"]["value"] == (f"${{{{ jobs.resolve.outputs.{backend}_enabled }}}}")
        assert resolve["outputs"][f"{backend}_enabled"] == "${{ false }}"
    assert "Detect configured telemetry backends" not in steps


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


@pytest.mark.parametrize("case_count", ["20", "24", "26", "27"])
def test_resolver_accepts_supported_catalog_counts(case_count: str) -> None:
    result = _run_resolver_validation("py", case_count=case_count)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("case_count", ["", "0", "1", "-20", "20.5", "23", "25", "28", "20; exit 0"])
def test_resolver_rejects_unsupported_catalog_counts(case_count: str) -> None:
    result = _run_resolver_validation("py", case_count=case_count)
    assert result.returncode != 0
    assert "case_count must be one of: 20, 24, 26, 27" in result.stdout


def test_catalog_count_defaults_and_routes_to_both_views_without_changing_test_refs() -> None:
    orchestrator = _load(ORCHESTRATOR)
    resolver = _load(RESOLVER_WORKFLOW)
    for workflow in (orchestrator, resolver):
        definition = _triggers(workflow)["workflow_call"]["inputs"]["case_count"]
        assert definition["type"] == "number"
        assert definition["required"] is False
        assert definition["default"] == 27
    jobs = orchestrator["jobs"]
    for name in ("resolve", "invocation", "execution"):
        assert jobs[name]["with"]["case_count"] == "${{ inputs.case_count }}"
    assert jobs["resolve"]["with"]["conformance_test_ref"] == "${{ inputs.conformance_test_ref }}"
    for view in ("invocation", "execution"):
        assert "case_count" not in jobs[f"long-running-{view}"]["with"]
    self_test = _load(WORKFLOWS_DIR / "opentelemetry-conformance-tests.yml")
    assert "case_count" not in self_test["jobs"]["opentelemetry"]["with"]


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
    assert "secrets" not in jobs["resolve"]
    assert jobs["invocation"]["uses"] == "./.github/workflows/opentelemetry-suite.yml"
    assert jobs["execution"]["uses"] == "./.github/workflows/opentelemetry-suite.yml"
    assert jobs["invocation"]["with"]["suite"] == "otel-invocation"
    assert jobs["execution"]["with"]["suite"] == "otel-execution"
    for view in ("invocation", "execution"):
        assert jobs[view]["with"]["resource_prefix"] == "${{ inputs.resource_prefix }}"
        assert "dash0_enabled" not in jobs[view]["with"]
        assert "datadog_enabled" not in jobs[view]["with"]
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


def test_suite_workers_run_only_adot_and_s3() -> None:
    workflow = _load(SUITE_WORKFLOW)
    text = SUITE_WORKFLOW.read_text(encoding="utf-8")
    call = _triggers(workflow)["workflow_call"]
    adot = workflow["jobs"]["adot"]

    assert set(_triggers(workflow)) == {"workflow_call"}
    assert set(workflow["jobs"]) == {"adot", "s3_collector"}
    for job in ("adot", "s3_collector"):
        name = workflow["jobs"][job]["name"]
        assert "inputs.suite == 'otel-invocation'" in name
        assert "'Invocation' || 'Execution'" in name
    assert "ADOT/X-Ray" in adot["name"]
    assert "Community/S3" in workflow["jobs"]["s3_collector"]["name"]
    assert "DASH0_OTLP_ENDPOINT" not in text
    assert "DATADOG_OTLP_ENDPOINT" not in text
    assert "--otel-backend dash0" not in text
    assert "--otel-backend datadog" not in text
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
    assert call["inputs"]["examples_dir"]["required"] is True
    assert "default" not in call["inputs"]["examples_dir"]
    assert "${{ inputs.examples_dir }}/template.yaml" in text
    assert "aws-durable-execution-conformance-tests-otel/examples" not in text
    assert "runtime_language" not in call["inputs"]
    assert '--language "$LANGUAGE"' in text
    assert '"OtelSuite=$OTEL_SUITE"' in text
    assert '"OtelServiceName=$OTEL_RESOURCE_SERVICE_NAME"' in text
    assert "--report console json junit github" in text
    assert "--otel-exporter adot" in text
    assert "--otel-backend xray" in text
    assert "--otel-backend collector" in text


def test_workers_load_support_from_their_own_workflow_revision() -> None:
    expected_support_checkout = {
        "repository": "${{ job.workflow_repository }}",
        "ref": "${{ job.workflow_sha }}",
        "path": ".build/workflow-support",
    }
    expected_action = "./.build/workflow-support/.github/actions/prepare-otel-example"

    workflow = _load(SUITE_WORKFLOW)
    for job_name in ("adot", "s3_collector"):
        steps = {step["name"]: step for step in workflow["jobs"][job_name]["steps"]}
        assert steps["Check out conformance tests"]["with"] == {
            "repository": "${{ inputs.conformance_repository }}",
            "ref": "${{ inputs.conformance_test_sha }}",
        }
        assert steps["Check out workflow support"]["with"] == expected_support_checkout
        assert steps["Prepare conformance example"]["uses"] == expected_action


def test_setup_go_uses_pinned_action_with_collector_inputs() -> None:
    steps = _load(SUITE_WORKFLOW)["jobs"]["s3_collector"]["steps"]
    setup = next(step for step in steps if step["name"] == "Set up Go")
    assert setup["uses"] == "actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e"
    assert setup["with"] == {
        "go-version-file": "${{ env.OTEL_LAMBDA_CHECKOUT }}/collector/go.mod",
        "cache-dependency-path": "${{ env.OTEL_LAMBDA_CHECKOUT }}/collector/go.sum\n${{ env.OTEL_LAMBDA_CHECKOUT }}/collector/lambdacomponents/go.sum\n",
    }


def test_prepare_tool_overrides_do_not_leak_into_caller_hooks(tmp_path: Path) -> None:
    steps = {step["name"]: step for step in _load(PREPARE_ACTION)["runs"]["steps"]}
    caller_modules = tmp_path / "caller-modules"
    caller_modules.mkdir()
    (caller_modules / "caller_marker.py").write_text('VALUE = "caller module"\n')
    caller_config = tmp_path / "caller-pip.conf"
    caller_config.write_text("[global]\nindex-url = https://packages.example.invalid/simple\n")
    github_env = tmp_path / "github-env"
    github_env.touch()
    install_probe = tmp_path / "install-probe.json"
    uv = tmp_path / "uv"
    uv.write_text(
        "#!/bin/sh\n"
        + shlex.join(
            [
                sys.executable,
                "-I",
                "-c",
                "import json,os,pathlib;pathlib.Path(os.environ['INSTALL_PROBE']).write_text(json.dumps({k:os.environ.get(k) for k in ('PIP_CONFIG_FILE','PYTHONPATH')}))",
            ]
        )
        + "\n"
    )
    uv.chmod(0o755)
    inherited = {
        **os.environ,
        "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}",
        "PIP_CONFIG_FILE": str(caller_config),
        "PYTHONPATH": str(caller_modules),
        "GITHUB_ENV": str(github_env),
        "INSTALL_PROBE": str(install_probe),
    }
    install = steps["Install Hatch"]
    subprocess.run(
        ["bash", "-eo", "pipefail", "-c", install["run"]],
        env={**inherited, **install["env"]},
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(install_probe.read_text()) == {"PIP_CONFIG_FILE": "/dev/null", "PYTHONPATH": ""}
    # Apply the actual step's GITHUB_ENV output as the Actions runner does.
    for line in github_env.read_text().splitlines():
        key, value = line.split("=", 1)
        inherited[key] = value
    probe = tmp_path / "caller-probe.py"
    probe.write_text(
        "import os,sys\nfrom caller_marker import VALUE\n"
        f"assert os.environ['PIP_CONFIG_FILE'] == {str(caller_config)!r}\n"
        f"assert os.environ['PYTHONPATH'] == {str(caller_modules)!r}\n"
        "assert VALUE == 'caller module'\nprint(sys.argv[1])\n"
    )
    command = shlex.join([sys.executable, str(probe)])
    hooks = steps["Run language setup and preparation"]
    hooks_env = {
        **inherited,
        **hooks.get("env", {}),
        "SETUP_COMMAND": command + " setup",
        "CONTRACT_TEST_COMMAND": command + " contract",
        "PREPARE_COMMAND": command + " prepare",
    }
    completed = subprocess.run(
        ["bash", "-eo", "pipefail", "-c", hooks["run"]], env=hooks_env, capture_output=True, text=True, check=False
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.splitlines() == ["setup", "contract", "prepare"]
    later = subprocess.run(
        [sys.executable, str(probe), "later"], env=inherited, capture_output=True, text=True, check=False
    )
    assert later.returncode == 0, later.stderr
    assert later.stdout.strip() == "later"


def test_sam_and_owned_hatch_steps_keep_isolation_local() -> None:
    isolation = {"PIP_CONFIG_FILE": "/dev/null", "PYTHONPATH": ""}
    sam = next(step for step in _load(PREPARE_ACTION)["runs"]["steps"] if step["name"] == "Set up SAM CLI")
    assert sam.get("env") == isolation
    owned_steps = []
    for path in (SUITE_WORKFLOW, LONG_RUNNING_WORKFLOW):
        workflow = _load(path)
        assert not isolation.keys() & workflow.get("env", {}).keys()
        for job in workflow["jobs"].values():
            assert not isolation.keys() & job.get("env", {}).keys()
            for step in job["steps"]:
                if "hatch run " in step.get("run", ""):
                    owned_steps.append((path.name, step["name"]))
                    assert all(step.get("env", {}).get(key) == value for key, value in isolation.items())
    assert len(owned_steps) == 3


def test_adot_and_s3_resources_remain_stable() -> None:
    workflow = _load(SUITE_WORKFLOW)
    adot = workflow["jobs"]["adot"]
    s3 = workflow["jobs"]["s3_collector"]
    text = SUITE_WORKFLOW.read_text(encoding="utf-8")

    assert adot["env"]["TEST_STACK_NAME"].startswith("conformance-tests-${{ inputs.language }}-xray-")
    assert adot["if"] == s3["if"]
    assert adot["if"] == (
        "github.event_name != 'pull_request' || "
        "(github.base_ref == 'main' && github.event.pull_request.head.repo.full_name == github.repository)"
    )
    assert s3["env"]["TEST_STACK_NAME"].startswith("conformance-tests-${{ inputs.language }}-s3-")
    assert 'OTEL_S3_BUCKET="dex-otel-${LANGUAGE}-${OTEL_VIEW_SUFFIX}-${TEST_ACCOUNT_ID}-${AWS_REGION}"' in text
    assert "aws s3api head-bucket" in text
    assert 'aws s3 rm "s3://$OTEL_S3_BUCKET/$OTEL_S3_PREFIX" --recursive' in text
    assert "aws s3api delete-bucket" not in text
    assert "aws lambda delete-layer-version" not in text


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


def test_long_running_worker_preserves_caller_compatibility_as_noop() -> None:
    workflow = _load(LONG_RUNNING_WORKFLOW)
    call = _triggers(workflow)["workflow_call"]

    assert set(_triggers(workflow)) == {"workflow_call"}
    required_inputs = {name for name, spec in call["inputs"].items() if spec.get("required")}
    for view in ("invocation", "execution"):
        caller = _load(ORCHESTRATOR)["jobs"][f"long-running-{view}"]
        assert required_inputs <= caller["with"].keys() <= call["inputs"].keys()

    assert set(call["outputs"]) == {"check_status", "rollover_ready"}
    assert call["outputs"]["check_status"]["value"] == "${{ jobs.run.outputs.check_status }}"
    assert call["outputs"]["rollover_ready"]["value"] == "${{ jobs.run.outputs.rollover_ready }}"
    assert workflow["permissions"] == {}
    assert "concurrency" not in workflow
    assert "env" not in workflow
    assert set(workflow["jobs"]) == {"run"}
    job = workflow["jobs"]["run"]
    assert job["outputs"] == {"check_status": "skipped", "rollover_ready": "false"}
    assert "if" not in job
    assert job["runs-on"] == "${{ inputs.runs_on }}"
    assert len(job["steps"]) == 1
    assert job["steps"][0]["run"] == ":"
    assert "uses" not in job["steps"][0]


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


@pytest.mark.parametrize(
    "requested_ref", ["", "main", "feature/otel-context", "refs/heads/feature/otel-context", "v2.0.1"]
)
def test_sdk_resolver_resolves_named_refs_to_commit_sha(tmp_path: Path, requested_ref: str) -> None:
    expected_sha = "a" * 40
    result, output, arguments = _run_sdk_resolver(tmp_path, requested_ref, expected_sha)

    assert result.returncode == 0, result.stderr
    assert output == f"ref={expected_sha}\n"
    assert arguments == [
        "api",
        "--method",
        "GET",
        "repos/aws/aws-durable-execution-sdk-python/commits",
        "-f",
        f"sha={requested_ref or 'main'}",
        "-F",
        "per_page=1",
        "--jq",
        ".[0].sha",
    ]


def test_sdk_resolver_keeps_pinned_sha_without_network_lookup(tmp_path: Path) -> None:
    pinned_sha = "b" * 40
    result, output, arguments = _run_sdk_resolver(tmp_path, pinned_sha, "", api_status=1)

    assert result.returncode == 0, result.stderr
    assert output == f"ref={pinned_sha}\n"
    assert arguments == []


@pytest.mark.parametrize(("api_output", "api_status"), [("null", 0), ("invalid-sha", 0), ("", 1)])
def test_sdk_resolver_fails_without_a_resolved_commit(
    tmp_path: Path,
    api_output: str,
    api_status: int,
) -> None:
    result, output, _ = _run_sdk_resolver(tmp_path, "missing-branch", api_output, api_status=api_status)

    assert result.returncode != 0
    assert output == ""


def _run_sdk_resolver(
    tmp_path: Path,
    requested_ref: str,
    api_output: str,
    *,
    api_status: int = 0,
) -> tuple[subprocess.CompletedProcess[str], str, list[str]]:
    workflow = _load(RESOLVER_WORKFLOW)
    step = next(step for step in workflow["jobs"]["resolve"]["steps"] if step.get("id") == "resolve-sdk")
    stub = tmp_path / "gh"
    stub.write_text(
        '#!/bin/sh\nprintf "%s\\n" "$@" > "$TEST_GH_ARGUMENTS"\n'
        'printf "%s\\n" "$TEST_GH_OUTPUT"\nexit "$TEST_GH_STATUS"\n',
        encoding="utf-8",
    )
    stub.chmod(0o755)
    output_path = tmp_path / "output"
    args_path = tmp_path / "args"
    result = subprocess.run(
        ["bash", "-eo", "pipefail", "-c", step["run"]],
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "REQUESTED_SDK_REF": requested_ref,
            "SDK_REPOSITORY": "aws/aws-durable-execution-sdk-python",
            "GITHUB_OUTPUT": str(output_path),
            "TEST_GH_ARGUMENTS": str(args_path),
            "TEST_GH_OUTPUT": api_output,
            "TEST_GH_STATUS": str(api_status),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    return (
        result,
        output_path.read_text() if output_path.exists() else "",
        args_path.read_text().splitlines() if args_path.exists() else [],
    )
