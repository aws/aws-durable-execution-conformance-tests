# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contracts and shell behavior of the reusable telemetry worker."""

import os
import shlex
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github/workflows/telemetry-suite.yml"


def _job() -> dict:
    return yaml.safe_load(WORKFLOW.read_text())["jobs"]["telemetry"]


def _step(name: str) -> dict:
    return next(step for step in _job()["steps"] if step["name"] == name)


def _run(name: str, tmp_path: Path, *, prelude: str = "", **env: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-euo", "pipefail", "-c", prelude + "\n" + _step(name)["run"]],
        cwd=tmp_path,
        env={
            **os.environ,
            "LANGUAGE": "js",
            "RESOURCE_PREFIX": "js",
            "SDK_REPOSITORY": "aws/aws-durable-execution-sdk-js",
            "SDK_REF": "a" * 40,
            "TEST_ACCOUNT_ID": "123456789012",
            "EXAMPLES_DIR": ".build/durable-sdk/telemetry handlers",
            "TEST_STACK_NAME": "conformance-tests-js-telemetry",
            "RUNNER_TEMP": str(tmp_path),
            **env,
        },
        capture_output=True,
        text=True,
        check=False,
    )


def test_caller_contract_and_revision_pins() -> None:
    workflow = yaml.safe_load(WORKFLOW.read_text())
    call = (workflow.get("on") or workflow[True])["workflow_call"]
    for name in ("language", "resource_prefix", "sdk_repository", "sdk_ref", "examples_dir"):
        assert call["inputs"][name]["required"] is True
    for name in (
        "CONFORMANCE_TEST_ROLE_ARN",
        "CONFORMANCE_TEST_ACCOUNT_ID",
        "CONFORMANCE_TEST_LAMBDA_EXECUTION_ROLE_ARN",
    ):
        assert call["secrets"][name]["required"] is True
    checkout = _step("Check out conformance tests")["with"]
    assert checkout["repository"] == "${{ job.workflow_repository }}"
    assert checkout["ref"] == "${{ inputs.conformance_test_ref || job.workflow_sha }}"
    assert _step("Check out SDK")["with"]["ref"] == "${{ inputs.sdk_ref }}"
    assert workflow["permissions"] == {"contents": "read", "id-token": "write"}
    assert _job()["runs-on"] == "${{ inputs.runs_on }}"


def test_cloud_trust_boundary_and_step_order() -> None:
    job = _job()
    assert "github.event.pull_request.head.repo.full_name == github.repository" in job["if"]
    assert "github.actor != 'dependabot[bot]'" in job["if"]
    names = [step["name"] for step in job["steps"]]
    ordered = [
        "Validate workflow inputs",
        "Prepare SDK handlers",
        "Check handler template",
        "Configure AWS credentials",
        "Verify AWS test account",
        "Recover rolled-back test stack",
        "Run telemetry conformance",
    ]
    assert [names.index(name) for name in ordered] == sorted(names.index(name) for name in ordered)
    credentials = _step("Configure AWS credentials")["with"]
    assert credentials["allowed-account-ids"] == "${{ secrets.CONFORMANCE_TEST_ACCOUNT_ID }}"
    assert _step("Show CloudFormation diagnostics")["if"] == "failure() && steps.account.outcome == 'success'"


def test_serializes_persistent_stack_and_always_uploads_artifacts() -> None:
    job = _job()
    assert job["concurrency"] == {
        "group": "telemetry-stack-${{ inputs.resource_prefix }}-${{ inputs.aws_region }}",
        "cancel-in-progress": False,
        "queue": "max",
    }
    assert job["env"]["TEST_STACK_NAME"] == "conformance-tests-${{ inputs.resource_prefix }}-telemetry"
    upload = _step("Upload reports and histories")
    assert upload["if"] == "always()"
    for path in ("${{ env.HISTORY_DIR }}/", "${{ env.REPORT_FILE }}.json", "${{ env.REPORT_FILE }}.xml"):
        assert path in upload["with"]["path"]
    assert "${{ github.run_attempt }}" in upload["with"]["name"]


def test_installs_both_packages_in_isolated_python_environment() -> None:
    setup = _step("Set up Python")
    assert setup["with"] == {"python-version": "3.14", "activate-environment": True}
    assert _job()["env"]["PIP_CONFIG_FILE"] == "/dev/null"
    assert _job()["env"]["PYTHONPATH"] == ""
    command = _step("Install conformance runner and telemetry extension")["run"]
    assert shlex.split(command.replace("\\\n", "")) == [
        "uv",
        "pip",
        "install",
        "./packages/aws-durable-execution-conformance-tests",
        "./packages/aws-durable-execution-conformance-tests-telemetry",
    ]


@pytest.mark.parametrize("prefix", ["js", "ct-js", "abc123-x"])
def test_accepts_resource_safe_inputs(tmp_path: Path, prefix: str) -> None:
    assert _run("Validate workflow inputs", tmp_path, RESOURCE_PREFIX=prefix).returncode == 0


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("RESOURCE_PREFIX", "abc123-xy"),
        ("RESOURCE_PREFIX", "js; touch unexpected"),
        ("LANGUAGE", "../js"),
        ("SDK_REPOSITORY", "not-a-repository"),
        ("SDK_REF", "main"),
        ("TEST_ACCOUNT_ID", "123"),
        ("EXAMPLES_DIR", ""),
        ("EXAMPLES_DIR", "/absolute"),
    ],
)
def test_invalid_inputs_stop_before_deployment(tmp_path: Path, key: str, value: str) -> None:
    assert _run("Validate workflow inputs", tmp_path, **{key: value}).returncode != 0
    assert not (tmp_path / "unexpected").exists()


def test_setup_and_preparation_share_shell(tmp_path: Path) -> None:
    result = _run(
        "Prepare SDK handlers",
        tmp_path,
        SETUP_COMMAND="export TOOLCHAIN_READY=selected",
        PREPARE_COMMAND='printf "%s" "$TOOLCHAIN_READY"',
    )
    assert result.returncode == 0
    assert result.stdout == "selected"


def test_failed_setup_stops_preparation(tmp_path: Path) -> None:
    result = _run("Prepare SDK handlers", tmp_path, SETUP_COMMAND="false", PREPARE_COMMAND="touch unexpected")
    assert result.returncode != 0
    assert not (tmp_path / "unexpected").exists()


@pytest.mark.parametrize(("account", "expected"), [("123456789012", 0), ("999999999999", 1)])
def test_account_validation(tmp_path: Path, account: str, expected: int) -> None:
    result = _run("Verify AWS test account", tmp_path, prelude=f"aws() {{ echo {account}; }}")
    assert result.returncode == expected


@pytest.mark.parametrize(
    ("status", "error", "expected", "deleted"),
    [
        ("CREATE_COMPLETE", "", 0, False),
        ("ROLLBACK_COMPLETE", "", 0, True),
        ("", "ValidationError: Stack does not exist", 0, False),
        ("", "AccessDenied: not authorized", 1, False),
    ],
)
def test_stack_recovery(tmp_path: Path, status: str, error: str, expected: int, deleted: bool) -> None:
    result = _run(
        "Recover rolled-back test stack",
        tmp_path,
        prelude="""aws() {
          printf '%s\n' "$*" >> calls
          if [ "$2" = describe-stacks ]; then
            if [ -n "$ERROR" ]; then echo "$ERROR" >&2; return 1; fi
            echo "$STATUS"
          fi
        }""",
        STATUS=status,
        ERROR=error,
    )
    assert result.returncode == expected
    calls = (tmp_path / "calls").read_text()
    assert ("delete-stack --stack-name conformance-tests-js-telemetry" in calls) is deleted
    assert ("wait stack-delete-complete" in calls) is deleted


@pytest.mark.parametrize("exit_code", [0, 1, 2])
def test_runner_arguments_and_failure_propagation(tmp_path: Path, exit_code: int) -> None:
    result = _run(
        "Run telemetry conformance",
        tmp_path,
        prelude='python() { printf "%s\\n" "$@" > arguments; return "$RUNNER_EXIT"; }',
        AWS_REGION="us-west-2",
        TEST_NAME="js-telemetry",
        HISTORY_DIR=str(tmp_path / "history"),
        REPORT_FILE=str(tmp_path / "report"),
        LAMBDA_EXECUTION_ROLE_ARN="arn:aws:iam::123456789012:role/telemetry",
        RUNNER_EXIT=str(exit_code),
    )
    assert result.returncode == exit_code
    args = (tmp_path / "arguments").read_text().splitlines()
    assert args[:2] == ["-m", "aws_durable_execution_conformance_tests.app"]
    assert args[args.index("--template") + 1] == ".build/durable-sdk/telemetry handlers/template.yaml"
    assert args[args.index("--suite") + 1] == "telemetry"
    assert args[args.index("--fail-on") + 1] == "failed+uncovered"
    assert "--no-cleanup" in args
    assert args[-1] == "LambdaExecutionRoleArn=arn:aws:iam::123456789012:role/telemetry"
