"""Reusable workflow keeps the complete cloud lifecycle and its trust boundary."""

import re
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[4]
CODEBUILD = "format('codebuild-github-actions-runner-{0}-{1}', github.run_id, github.run_attempt)"
STAGES = ("setup", "build", "deploy", "run", "collect", "cleanup", "summary")


def workflow():
    return yaml.safe_load((ROOT / ".github/workflows/lmi-lifecycle.yml").read_text())


def stage_step(job, stage):
    return next(step for step in job["steps"] if f'"$LMI_SDK_SCRIPT" {stage}' in step.get("run", ""))


def evaluate(expression, context):
    """Evaluate the comparison/boolean subset of GitHub expressions used by the trust guard."""
    tokens = re.findall(r"\|\||&&|==|!=|!|\(|\)|,|'[^']*'|[A-Za-z_][\w.]*", expression)
    assert "".join(tokens) == re.sub(r"\s+", "", expression), expression
    position = 0

    def take():
        nonlocal position
        position += 1
        return tokens[position - 1]

    def peek():
        return tokens[position] if position < len(tokens) else None

    def atom():
        token = take()
        if token == "!":
            return not atom()
        if token == "(":
            value = either()
            assert take() == ")"
            return value
        if token.startswith("'"):
            return token[1:-1]
        if peek() == "(":
            assert token == "startsWith", token
            take()
            subject = either()
            assert take() == ","
            prefix = either()
            assert take() == ")"
            # GitHub compares case-insensitively.
            return isinstance(subject, str) and subject.lower().startswith(prefix.lower())
        value = context
        for part in token.split("."):
            value = value.get(part) if isinstance(value, dict) else None
        return value

    def comparison():
        left = atom()
        if peek() in ("==", "!="):
            operator, right = take(), atom()
            return (left == right) == (operator == "==")
        return left

    def both():
        value = comparison()
        while peek() == "&&":
            take()
            right = comparison()
            value = value and right
        return value

    def either():
        value = both()
        while peek() == "||":
            take()
            right = both()
            value = value or right
        return value

    value = either()
    assert peek() is None, expression
    return bool(value)


def trust_guard():
    return workflow()["jobs"]["cloud"]["if"]


def test_harness_runner_and_cloud_job_share_one_trust_guard():
    harness = workflow()["jobs"]["harness"]
    assert harness["runs-on"] == f"${{{{ ({trust_guard()}) && {CODEBUILD} || 'ubuntu-latest' }}}}"


SAME = {"head": {"repo": {"full_name": "aws/sdk"}}, "user": {"login": "maintainer"}}
FORK = {"head": {"repo": {"full_name": "someone/sdk"}}, "user": {"login": "someone"}}
DEPENDABOT_PR = {"head": {"repo": {"full_name": "aws/sdk"}}, "user": {"login": "dependabot[bot]"}}


@pytest.mark.parametrize(
    ("event_name", "actor", "pull_request", "trusted"),
    [
        ("push", "maintainer", None, True),
        ("schedule", "maintainer", None, True),
        ("workflow_dispatch", "maintainer", None, True),
        ("pull_request", "maintainer", SAME, True),
        ("pull_request", "someone", FORK, False),
        # A fork PR review event checks out the PR's code too.
        ("pull_request_review", "maintainer", FORK, False),
        ("pull_request_target", "maintainer", SAME, False),
        ("merge_group", "maintainer", None, False),
        ("push", "dependabot[bot]", None, False),
        ("pull_request", "dependabot[bot]", DEPENDABOT_PR, False),
        # A maintainer reopening or synchronizing a Dependabot PR is the actor.
        ("pull_request", "maintainer", DEPENDABOT_PR, False),
    ],
)
def test_only_trusted_events_reach_the_testing_account(event_name, actor, pull_request, trusted):
    event = {} if pull_request is None else {"pull_request": pull_request}
    ref = "refs/pull/1/merge" if pull_request is not None else "refs/heads/main"
    context = {
        "github": {"event_name": event_name, "actor": actor, "repository": "aws/sdk", "event": event, "ref": ref}
    }
    assert evaluate(trust_guard(), context) is trusted


@pytest.mark.parametrize("head_ref", ["dependabot/npm_and_yarn/x", "Dependabot/pip/y"])
def test_maintainer_pull_requests_from_dependabot_branches_stay_untrusted(head_ref):
    # A maintainer can open a same-repository PR from a branch that Dependabot created.
    context = {
        "github": {
            "event_name": "pull_request",
            "actor": "maintainer",
            "repository": "aws/sdk",
            "event": {"pull_request": SAME},
            "ref": "refs/pull/1/merge",
            "head_ref": head_ref,
        }
    }
    assert evaluate(trust_guard(), context) is False


@pytest.mark.parametrize("ref", ["refs/heads/dependabot/npm_and_yarn/x", "refs/heads/Dependabot/pip/y"])
def test_maintainer_pushes_to_dependabot_branches_stay_untrusted(ref):
    # "Update branch" on a Dependabot PR pushes as the maintainer, with no PR payload.
    context = {
        "github": {"event_name": "push", "actor": "maintainer", "repository": "aws/sdk", "event": {}, "ref": ref}
    }
    assert evaluate(trust_guard(), context) is False


def test_cloud_lifecycle_and_credentials_are_owned_by_the_reusable_workflow():
    value = workflow()
    trigger = value.get("on", value.get(True))
    assert trigger["workflow_call"]["inputs"]["conformance-ref"]["required"] is False
    harness, cloud = value["jobs"]["harness"], value["jobs"]["cloud"]
    assert harness["permissions"] == {"contents": "read"}
    assert cloud["permissions"] == {"contents": "read", "id-token": "write"}
    assert cloud["needs"] == "harness"
    assert cloud["concurrency"] == {
        "group": "lmi-e2e-shared-capacity-provider",
        "cancel-in-progress": False,
        "queue": "max",
    }
    assert value["env"]["PYTHONPATH"] == ""
    assert value["env"]["PIP_CONFIG_FILE"] == "/dev/null"
    assert "CONFORMANCE_REVISION" not in value["env"]
    for job in (harness, cloud):
        checkouts = [step for step in job["steps"] if step.get("uses", "").startswith("actions/checkout@")]
        assert len(checkouts) == 2
        assert all(step["with"]["persist-credentials"] is False for step in checkouts)
        # The shared checkout is the reusable workflow's own commit, not a caller-chosen ref.
        assert checkouts[1]["with"]["repository"] == "${{ job.workflow_repository }}"
        assert checkouts[1]["with"]["ref"] == "${{ job.workflow_sha }}"
        resolve = next(step for step in job["steps"] if step.get("name") == "Resolve pinned shared conformance")
        assert job["steps"].index(resolve) < job["steps"].index(checkouts[1])
        assert resolve["env"] == {
            "REQUESTED_REVISION": "${{ inputs.conformance-ref }}",
            "WORKFLOW_REVISION": "${{ job.workflow_sha }}",
        }
        assert 'echo "CONFORMANCE_REVISION=$WORKFLOW_REVISION" >> "$GITHUB_ENV"' in resolve["run"]
    steps = cloud["steps"]
    stages = [stage for step in steps for stage in STAGES if f'"$LMI_SDK_SCRIPT" {stage}' in step.get("run", "")]
    assert stages == list(STAGES)
    credentials = [step for step in steps if step.get("uses", "").startswith("aws-actions/configure-aws-credentials@")]
    assert len(credentials) == 3
    assert all(step["with"]["allowed-account-ids"] == "${{ secrets.TEST_ACCOUNT_ID }}" for step in credentials)
    # A blank role would let the credentials action fall back to the runner's identity.
    required = next(step for step in steps if step.get("name") == "Require test-account configuration")
    assert required["env"] == {"TEST_ROLE_ARN": "${{ secrets.TEST_ROLE_ARN }}"}
    assert steps.index(required) < steps.index(stage_step(cloud, "setup"))
    # The account is verified after the first session and before deploy.
    verify = next(step for step in steps if step.get("name") == "Verify test account")
    assert steps.index(credentials[0]) < steps.index(verify) < steps.index(stage_step(cloud, "deploy"))
    # Collection and cleanup depend on the manifest that deploy must leave behind.
    deploy_script = stage_step(cloud, "deploy")["run"]
    assert deploy_script.index('"$LMI_SDK_SCRIPT" deploy') < deploy_script.index(
        'test -f "$LMI_ARTIFACT_DIRECTORY/manifest.json"'
    )
    # The testing-account session is configured only after package-manager and build scripts.
    assert (
        steps.index(stage_step(cloud, "build")) < steps.index(credentials[0]) < steps.index(stage_step(cloud, "deploy"))
    )


SECRETS = ("TEST_ROLE_ARN", "TEST_ACCOUNT_ID", "CAPACITY_PROVIDER_ARN", "TEST_LAMBDA_EXECUTION_ROLE_ARN")


@pytest.mark.parametrize("blank", ["", " ", "\t \n"])
@pytest.mark.parametrize("name", SECRETS)
def test_blank_test_account_secrets_fail_before_setup(name, blank):
    # The credentials action trims its inputs, so whitespace is as empty as nothing.
    steps = workflow()["jobs"]["cloud"]["steps"]
    script = next(step for step in steps if step.get("name") == "Require test-account configuration")["run"]
    values = {secret: "configured" for secret in SECRETS} | {name: blank}
    result = subprocess.run(["bash", "-eo", "pipefail", "-c", script], env=values, capture_output=True, text=True)
    assert result.returncode != 0
    assert name in result.stdout


def test_configured_test_account_secrets_pass():
    steps = workflow()["jobs"]["cloud"]["steps"]
    script = next(step for step in steps if step.get("name") == "Require test-account configuration")["run"]
    values = {secret: f" arn-or-id-for-{secret} " for secret in SECRETS}
    assert subprocess.run(["bash", "-eo", "pipefail", "-c", script], env=values).returncode == 0


def test_harness_job_runs_setup_then_harness_after_python():
    harness = workflow()["jobs"]["harness"]
    steps = harness["steps"]
    stages = [
        stage for step in steps for stage in (*STAGES, "harness") if f'"$LMI_SDK_SCRIPT" {stage}' in step.get("run", "")
    ]
    assert stages == ["setup", "harness"]
    python = next(step for step in steps if step.get("uses", "").startswith("astral-sh/setup-uv@"))
    assert steps.index(python) < steps.index(stage_step(harness, "setup")) < steps.index(stage_step(harness, "harness"))


def test_evidence_steps_share_the_manifest_guard_and_fail_without_artifacts():
    cloud = workflow()["jobs"]["cloud"]
    steps = cloud["steps"]
    detect = next(step for step in steps if step.get("id") == "deployment")
    assert detect["if"] == "always()"
    assert 'test -f "$LMI_ARTIFACT_DIRECTORY/manifest.json"' in detect["run"]
    guard = "always() && steps.deployment.outputs.present == 'true'"
    refresh = next(step for step in steps if step.get("name") == "Refresh credentials for evidence and cleanup")
    for step in (refresh, stage_step(cloud, "collect"), stage_step(cloud, "cleanup")):
        assert step["if"] == guard
        assert steps.index(detect) < steps.index(step)
    assert stage_step(cloud, "summary")["if"] == "always()"
    upload = next(step for step in steps if step.get("uses", "").startswith("actions/upload-artifact@"))
    assert upload["if"] == "always()"
    assert upload["with"]["if-no-files-found"] == "error"
    # The manifest exists only after deploy; receipts exist only after cleanup.
    deploy, run, cleanup, summary = (stage_step(cloud, stage) for stage in ("deploy", "run", "cleanup", "summary"))
    assert steps.index(deploy) < steps.index(run) < steps.index(detect)
    assert steps.index(cleanup) < steps.index(upload) < steps.index(summary)
    credentials = [step for step in steps if step.get("uses", "").startswith("aws-actions/configure-aws-credentials@")]
    assert steps.index(credentials[1]) == steps.index(run) - 1


def test_step_budgets_cover_deployment_recovery_within_the_job_timeout():
    cloud = workflow()["jobs"]["cloud"]
    # Cleanup can poll an interrupted CloudFormation update for up to 25 minutes
    # (deployment.recover_deployment_update) before releasing its fences.
    assert stage_step(cloud, "cleanup")["timeout-minutes"] >= 30
    budgets = [stage_step(cloud, stage)["timeout-minutes"] for stage in STAGES if stage != "summary"]
    assert sum(budgets) < cloud["timeout-minutes"]
    # The harness job runs the same setup stage plus its own tests.
    harness = workflow()["jobs"]["harness"]
    assert harness["timeout-minutes"] > stage_step(cloud, "setup")["timeout-minutes"]


def test_empty_artifact_directory_uses_the_default_for_stages_and_uploads():
    value = workflow()
    trigger = value.get("on", value.get(True))
    default = trigger["workflow_call"]["inputs"]["artifact-directory"]["default"]
    assert value["env"]["LMI_ARTIFACT_DIRECTORY"] == f"${{{{ inputs.artifact-directory || '{default}' }}}}"
    paths = [
        step["with"]["path"]
        for job in value["jobs"].values()
        for step in job["steps"]
        if step.get("uses", "").startswith("actions/upload-artifact@")
    ]
    # No appended slash: a blank directory must not become the runner's root.
    assert paths == ["${{ env.LMI_ARTIFACT_DIRECTORY }}"] * 2


def test_artifact_names_are_unique_per_workflow_invocation():
    # One caller run can invoke the workflow for several runtimes or regions.
    value = workflow()
    names = [
        step["with"]["name"]
        for job in value["jobs"].values()
        for step in job["steps"]
        if step.get("uses", "").startswith("actions/upload-artifact@")
    ]
    assert len(names) == 2
    for name in names:
        for identity in ("inputs.stack-name", "inputs.lambda-runtime", "inputs.region"):
            assert identity in name, name
