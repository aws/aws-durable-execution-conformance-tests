"""Reusable workflow keeps the complete cloud lifecycle and its trust boundary."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[4]


def workflow():
    return yaml.safe_load((ROOT / ".github/workflows/lmi-lifecycle.yml").read_text())


def test_cloud_lifecycle_and_credentials_are_owned_by_the_reusable_workflow():
    value = workflow()
    assert "workflow_call" in value.get("on", value.get(True))
    harness, cloud = value["jobs"]["harness"], value["jobs"]["cloud"]
    assert harness["permissions"] == {"contents": "read"}
    assert cloud["permissions"] == {"contents": "read", "id-token": "write"}
    assert cloud["needs"] == "harness"
    assert cloud["concurrency"] == {
        "group": "lmi-e2e-shared-capacity-provider",
        "cancel-in-progress": False,
        "queue": "max",
    }
    assert (
        cloud["if"]
        == "github.actor != 'dependabot[bot]' && (github.event_name == 'workflow_dispatch' || (github.event_name == 'pull_request' && github.event.pull_request.head.repo.full_name == github.repository))"
    )
    assert "head.repo.full_name != github.repository" in harness["runs-on"]
    assert "'ubuntu-latest'" in harness["runs-on"]
    assert value["env"]["PYTHONPATH"] == ""
    assert value["env"]["PIP_CONFIG_FILE"] == "/dev/null"
    for job in (harness, cloud):
        checkouts = [step for step in job["steps"] if step.get("uses", "").startswith("actions/checkout@")]
        assert len(checkouts) == 2
        assert all(step["with"]["persist-credentials"] is False for step in checkouts)
        assert checkouts[1]["with"]["ref"] == "${{ env.CONFORMANCE_REVISION }}"
    stages = [
        next(
            stage
            for stage in ("setup", "build", "deploy", "run", "collect", "cleanup", "summary")
            if f'"$LMI_SDK_SCRIPT" {stage}' in step["run"]
        )
        for step in cloud["steps"]
        if '"$LMI_SDK_SCRIPT"' in step.get("run", "")
    ]
    assert stages == ["setup", "build", "deploy", "run", "collect", "cleanup", "summary"]
    for stage in ("collect", "cleanup", "summary"):
        step = next(step for step in cloud["steps"] if f'"$LMI_SDK_SCRIPT" {stage}' in step.get("run", ""))
        assert step["if"] == "always()"
    credentials = [
        step for step in cloud["steps"] if step.get("uses", "").startswith("aws-actions/configure-aws-credentials@")
    ]
    assert len(credentials) == 3
    assert all(step["with"]["allowed-account-ids"] == "${{ secrets.TEST_ACCOUNT_ID }}" for step in credentials)
