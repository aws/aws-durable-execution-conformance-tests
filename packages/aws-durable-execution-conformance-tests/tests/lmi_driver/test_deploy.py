import copy
import json
from pathlib import Path
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from aws_durable_execution_conformance_tests.lmi import artifacts as artifact_module
from aws_durable_execution_conformance_tests.lmi import deployment as deployment_module
from aws_durable_execution_conformance_tests.lmi.artifacts import clear_run_artifacts, save
from aws_durable_execution_conformance_tests.lmi.deployment import DeploymentManager
from aws_durable_execution_conformance_tests.lmi.errors import ProvisioningError
from aws_durable_execution_conformance_tests.lmi.runtime import CONCURRENCIES, QUALIFIER, SCALING

from .support import RUNTIME


@pytest.fixture
def manifest():
    return {
        "run": "unit",
        "stack": "python-lmi-e2e",
        "bucket": "test-bucket",
        "role": "role",
        "provider": "arn:aws:lambda:us-west-2:123456789012:capacity-provider:provider",
        "region": "us-west-2",
        "account": "123456789012",
        "runtime": "python3.14",
        "concurrencies": CONCURRENCIES,
        "functions": {},
        "commit": "sha",
        "codeKey": "code/sha.zip",
        "codeSha256": "hash",
        "owned": True,
        "invocationTimeout": 60,
        "executionTimeout": 240,
    }


def test_one_deployment_shares_two_functions_across_all_scenarios(deploy, manifest):
    runtime = "python3.14"
    template = deploy.template(manifest)
    functions = {
        key: resource for key, resource in template["Resources"].items() if resource["Type"] == "AWS::Lambda::Function"
    }
    assert set(functions) == {"c1Function", "c2Function"}
    assert functions["c1Function"]["Properties"]["Code"] == functions["c2Function"]["Properties"]["Code"]
    for key, concurrency in CONCURRENCIES.items():
        config = template["Resources"][key + "Function"]["Properties"]
        assert config["FunctionScalingConfig"] == SCALING
        assert config["Runtime"] == runtime
        assert (
            config["CapacityProviderConfig"]["LambdaManagedInstancesCapacityProviderConfig"][
                "PerExecutionEnvironmentMaxConcurrency"
            ]
            == concurrency
        )
        assert config["DurableConfig"]["ExecutionTimeout"] == 240
        assert config["Timeout"] == 60
        assert config["Handler"] == "fixture.handler"
        assert template["Outputs"][key]["Value"]["Fn::Join"][1][-1] == ":$LATEST.PUBLISHED"
    assert not any(
        r["Type"] in {"AWS::Lambda::Version", "AWS::Lambda::CapacityProvider"} for r in template["Resources"].values()
    )


def test_verify_rejects_standard_lambda_wrong_artifact_and_unbounded_placement(
    deploy,
    manifest,
):
    config = copy.deepcopy(deploy.template(manifest)["Resources"]["c1Function"]["Properties"])
    config.update(State="Active", Version=QUALIFIER, CodeSha256="hash")
    scaling = {"AppliedFunctionScalingConfig": SCALING}
    deploy.verify(config, scaling, manifest, "c1")
    with pytest.raises(ProvisioningError):
        deploy.verify(config, scaling, manifest, "c2")
    for field in (
        "CapacityProviderConfig",
        "CodeSha256",
        "Runtime",
        "Timeout",
        "DurableConfig",
        "Environment",
        "Role",
        "Handler",
    ):
        bad = copy.deepcopy(config)
        del bad[field]
        with pytest.raises(ProvisioningError):
            deploy.verify(bad, scaling, manifest, "c1")
    with pytest.raises(ProvisioningError):
        deploy.verify(
            config,
            {"AppliedFunctionScalingConfig": {"MaxExecutionEnvironments": 2}},
            manifest,
            "c1",
        )


def test_cleanup_refuses_foreign_owner_and_failed_create(deploy, manifest, monkeypatch):
    cfn = Mock()
    cfn.describe_stacks.return_value = {"Stacks": [{"Tags": [{"Key": "Suite", "Value": "other"}]}]}
    monkeypatch.setattr(deploy, "client", lambda _: cfn)
    with pytest.raises(ProvisioningError, match="Refusing"):
        deploy.cleanup(manifest)
    cfn.delete_stack.assert_not_called()
    deploy.cleanup({**manifest, "owned": False})
    cfn.delete_stack.assert_not_called()


def stack(manifest, status="UPDATE_COMPLETE"):
    return {
        "StackStatus": status,
        "Tags": [
            {"Key": "Suite", "Value": "test-lmi-e2e"},
            {"Key": "Stack", "Value": manifest["stack"]},
            {"Key": "Persistent", "Value": "true"},
        ],
        "Outputs": [
            {
                "OutputKey": key,
                "OutputValue": f"arn:aws:lambda:us-west-2:123456789012:function:{manifest['stack']}-{key}:$LATEST.PUBLISHED",
            }
            for key in CONCURRENCIES
        ],
    }


@pytest.fixture
def apis(deploy, monkeypatch, tmp_path):
    clients = {name: Mock() for name in ("cloudformation", "s3", "lambda")}
    monkeypatch.setattr(deploy, "client", clients.__getitem__)
    monkeypatch.setattr(deploy, "artifacts", tmp_path)
    clients["s3"].get_public_access_block.return_value = {
        "PublicAccessBlockConfiguration": {
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        }
    }
    clients["s3"].get_object.side_effect = AssertionError("Deployment must not read an S3 lock")
    return clients


def test_later_run_updates_fixed_functions_without_recreating_resources(deploy, manifest, apis, monkeypatch):
    cfn, lam = apis["cloudformation"], apis["lambda"]
    monkeypatch.setattr(deploy, "owned_stack", Mock(side_effect=[None, stack(manifest)]))
    monkeypatch.setattr(deploy, "wait_stack", Mock(return_value=stack(manifest)))
    idle = Mock()
    monkeypatch.setattr(deploy, "wait_for_idle_functions", idle)
    verify = Mock()
    monkeypatch.setattr(deploy, "verify", verify)
    deploy.deploy_stack(manifest, b"first-code")
    second = {
        **manifest,
        "run": "second",
        "codeKey": "code/new-digest.zip",
        "functions": {},
    }
    deploy.deploy_stack(second, b"second-code")
    cfn.create_stack.assert_called_once()
    assert cfn.update_stack.call_count == 2
    apis["s3"].get_object.assert_not_called()
    assert all(not call.kwargs["Key"].startswith("ownership/") for call in apis["s3"].put_object.call_args_list)
    templates = [json.loads(call.kwargs["TemplateBody"]) for call in cfn.update_stack.call_args_list]
    assert templates[0]["Outputs"] == templates[1]["Outputs"]
    assert templates[0]["Resources"].keys() == templates[1]["Resources"].keys()
    for key in CONCURRENCIES:
        before = templates[0]["Resources"][key + "Function"]["Properties"]
        after = templates[1]["Resources"][key + "Function"]["Properties"]
        assert before["FunctionName"] == after["FunctionName"]
        assert before["Code"]["S3Key"] != after["Code"]["S3Key"]
        assert after["Environment"]["Variables"]["LMI_RUN_ID"] == "second"
    idle.assert_called_once()
    assert verify.call_count == 4
    assert lam.get_function_configuration.call_count == 4
    cfn.delete_stack.assert_not_called()
    lam.delete_function.assert_not_called()


def test_unchanged_update_still_verifies_functions(deploy, manifest, apis, monkeypatch):
    cfn = apis["cloudformation"]
    cfn.describe_stacks.return_value = {"Stacks": [stack(manifest)]}
    cfn.update_stack.side_effect = ClientError(
        {
            "Error": {
                "Code": "ValidationError",
                "Message": "No updates are to be performed.",
            }
        },
        "UpdateStack",
    )
    monkeypatch.setattr(deploy, "wait_for_idle_functions", Mock())
    wait = Mock()
    monkeypatch.setattr(deploy, "wait_stack", wait)
    verify = Mock()
    monkeypatch.setattr(deploy, "verify", verify)
    deploy.deploy_stack(manifest, b"code")
    wait.assert_not_called()
    assert verify.call_count == 2
    cfn.create_stack.assert_not_called()


@pytest.mark.parametrize("status", ["ROLLBACK_COMPLETE", "UPDATE_IN_PROGRESS", "UPDATE_ROLLBACK_FAILED"])
def test_failed_or_busy_persistent_stack_is_retained(deploy, manifest, apis, status):
    cfn = apis["cloudformation"]
    cfn.describe_stacks.return_value = {"Stacks": [stack(manifest, status)]}
    with pytest.raises(ProvisioningError, match="retained"):
        deploy.deploy_stack(manifest, b"code")
    cfn.update_stack.assert_not_called()
    cfn.delete_stack.assert_not_called()
    apis["s3"].put_object.assert_not_called()


def test_foreign_stack_cannot_be_adopted(deploy, manifest, apis):
    cfn = apis["cloudformation"]
    cfn.describe_stacks.return_value = {"Stacks": [{"Tags": []}]}
    with pytest.raises(ProvisioningError, match="not owned"):
        deploy.deploy_stack(manifest, b"code")
    cfn.update_stack.assert_not_called()
    cfn.create_stack.assert_not_called()


def test_failed_creation_does_not_trigger_deletion(deploy, manifest, apis):
    cfn = apis["cloudformation"]
    cfn.describe_stacks.side_effect = ClientError(
        {"Error": {"Code": "ValidationError", "Message": "Stack does not exist"}},
        "DescribeStacks",
    )
    cfn.create_stack.side_effect = RuntimeError("create failed")
    with pytest.raises(RuntimeError, match="create failed"):
        deploy.deploy_stack(manifest, b"code")
    cfn.delete_stack.assert_not_called()


def test_prior_executions_must_finish_before_code_updates(deploy, manifest, apis, monkeypatch):
    lam = apis["lambda"]
    one_function = {"Outputs": stack(manifest)["Outputs"][:1]}
    lam.get_paginator.return_value.paginate.side_effect = [
        [{"DurableExecutions": [{"DurableExecutionArn": "old"}]}],
        [{"DurableExecutions": []}],
    ]
    sleep = Mock()
    monkeypatch.setattr(deployment_module.time, "sleep", sleep)
    deploy.wait_for_idle_functions(one_function)
    sleep.assert_called_once_with(2)
    lam.stop_durable_execution.assert_not_called()
    lam.get_paginator.return_value.paginate.assert_called_with(
        FunctionName=one_function["Outputs"][0]["OutputValue"].rsplit(":", 1)[0],
        Statuses=["RUNNING"],
    )


@pytest.mark.parametrize(
    "page",
    [
        {"DurableExecutions": [{"DurableExecutionArn": "old"}]},
        {"DurableExecutions": [], "NextMarker": "more"},
    ],
)
def test_busy_or_incompletely_listed_functions_block_updates(deploy, manifest, apis, monkeypatch, page):
    apis["lambda"].get_paginator.return_value.paginate.return_value = [page]
    cfn = apis["cloudformation"]
    cfn.describe_stacks.return_value = {"Stacks": [stack(manifest)]}
    wait = deploy.wait_for_idle_functions
    monkeypatch.setattr(deploy, "wait_for_idle_functions", lambda value: wait(value, seconds=0))
    with pytest.raises(ProvisioningError, match="code was not updated"):
        deploy.deploy_stack(manifest, b"code")
    cfn.update_stack.assert_not_called()
    assert not any(call.kwargs["Key"] == manifest["codeKey"] for call in apis["s3"].put_object.call_args_list)
    apis["lambda"].stop_durable_execution.assert_not_called()


def test_only_run_data_expires_and_code_is_retained(deploy, manifest):
    rules = deploy.template(manifest)["Resources"]["Bucket"]["Properties"]["LifecycleConfiguration"]["Rules"]
    assert all(rule.get("Prefix") == "runs/" for rule in rules)
    assert all(rule["ExpirationInDays"] == 1 for rule in rules)


def test_cleanup_releases_only_current_run_and_keeps_infrastructure(deploy, manifest, apis, monkeypatch, tmp_path):
    from aws_durable_execution_conformance_tests.lmi import cloud

    cfn, s3, lam = apis["cloudformation"], apis["s3"], apis["lambda"]
    cfn.describe_stacks.return_value = {"Stacks": [stack(manifest)]}
    cfn.describe_stack_resources.return_value = {
        "StackResources": [
            {
                "ResourceType": "AWS::S3::Bucket",
                "PhysicalResourceId": manifest["bucket"],
                "ResourceStatus": "CREATE_COMPLETE",
            }
        ]
    }
    driver = Mock()
    driver.run_invocations.return_value = [{"arn": "this-run"}]
    monkeypatch.setattr(cloud, "Cloud", Mock(return_value=driver))
    deploy.cleanup(manifest)
    s3.put_object.assert_called_once_with(
        Bucket=manifest["bucket"], Key="runs/unit/control/release-all", Body=b"release"
    )
    assert driver.invocations == [{"arn": "this-run"}]
    driver.settle_case.assert_called_once()
    cfn.delete_stack.assert_not_called()
    lam.delete_function.assert_not_called()
    s3.delete_objects.assert_not_called()
    assert json.loads((tmp_path / "cleanup.json").read_text())["status"] == "RETAINED"


@pytest.mark.parametrize(
    "failure",
    ["run-id", "stack-name", "environment", "account", "capacity", "build", "digest"],
)
def test_failed_preflight_cannot_reuse_prior_run_evidence(
    deploy, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: str
) -> None:
    """An EXIT trap after any preflight error must not target the previous run."""
    import hashlib
    from types import SimpleNamespace

    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    package = tmp_path / "lmi_tests/build"
    package.mkdir(parents=True)
    data = b"current-build"
    (package / "function.zip").write_bytes(data)
    build = {"commit": "current", "sha256": hashlib.sha256(data).hexdigest()}
    if failure == "digest":
        build["sha256"] = "wrong"
    save(artifacts / "build.json", build)
    build_bytes = (artifacts / "build.json").read_bytes()
    save(artifacts / "manifest.json", {"run": "previous", "owned": True})
    save(artifacts / "cleanup.json", {"run": "previous", "status": "RETAINED"})
    save(artifacts / "configuration/c1.json", {"run": "previous"})
    (artifacts / "cloud.xml").write_text('<testsuite><testcase name="old-case"/></testsuite>')
    clients = {name: Mock() for name in ("sts", "lambda", "s3")}
    clients["sts"].get_caller_identity.return_value = {"Account": "123456789012"}
    clients["lambda"].get_capacity_provider.return_value = {
        "CapacityProvider": {"CapacityProviderScalingConfig": {"MaxVCpuCount": 2}}
    }
    monkeypatch.setattr(deploy, "bundle", package / "function.zip")
    monkeypatch.setattr(deploy, "artifacts", artifacts)
    monkeypatch.setattr(deploy, "client", clients.__getitem__)
    deploy_stack = Mock()
    monkeypatch.setattr(deploy, "deploy_stack", deploy_stack)
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setenv("TEST_ACCOUNT_ID", "123456789012")
    monkeypatch.setenv(
        "CAPACITY_PROVIDER_ARN",
        "arn:aws:lambda:us-west-2:123456789012:capacity-provider/test",
    )
    monkeypatch.setenv("TEST_LAMBDA_EXECUTION_ROLE_ARN", "test-role")
    args = SimpleNamespace(run_id="current", stack_name="python-lmi-e2e", runtime="python3.14")
    if failure == "run-id":
        args.run_id = "INVALID"
    elif failure == "stack-name":
        args.stack_name = "INVALID"
    elif failure == "environment":
        monkeypatch.delenv("CAPACITY_PROVIDER_ARN")
    elif failure == "account":
        clients["sts"].get_caller_identity.return_value = {"Account": "other"}
    elif failure == "capacity":
        clients["lambda"].get_capacity_provider.side_effect = RuntimeError("capacity unavailable")
    elif failure == "build":
        (package / "function.zip").unlink()
    with pytest.raises((ProvisioningError, KeyError, RuntimeError, FileNotFoundError)):
        deploy.deploy(args)
    assert (artifacts / "build.json").read_bytes() == build_bytes
    assert not (artifacts / "manifest.json").exists()
    assert not (artifacts / "cleanup.json").exists()
    assert not (artifacts / "cloud.xml").exists()
    assert not (artifacts / "configuration").exists()
    deploy_stack.assert_not_called()
    clients["s3"].put_object.assert_not_called()
    from aws_durable_execution_conformance_tests.lmi.summary import summarize

    summary = summarize(artifacts)
    assert "old-case" not in summary
    assert "Cloud scenarios did not run" in summary
    assert "Run cleanup not confirmed" in summary


def test_artifact_cleanup_does_not_follow_symlinks(deploy, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "evidence.json").write_text("keep")
    (artifacts / "old-output").symlink_to(outside, target_is_directory=True)
    monkeypatch.setattr(deploy, "artifacts", artifacts)
    clear_run_artifacts(deploy.artifacts)
    assert (outside / "evidence.json").read_text() == "keep"
    assert not (artifacts / "old-output").exists()


def test_manifest_is_invalidated_even_if_later_artifact_cleanup_fails(
    deploy, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    save(tmp_path / "manifest.json", {"run": "previous", "owned": True})
    save(tmp_path / "configuration/c1.json", {"run": "previous"})
    monkeypatch.setattr(deploy, "artifacts", tmp_path)
    monkeypatch.setattr(artifact_module.shutil, "rmtree", Mock(side_effect=PermissionError("blocked")))
    with pytest.raises(PermissionError, match="blocked"):
        clear_run_artifacts(deploy.artifacts)
    assert not (tmp_path / "manifest.json").exists()


@pytest.fixture
def deploy(tmp_path):
    return DeploymentManager(
        runtime=RUNTIME, owner="test-lmi-e2e", artifacts=tmp_path, bundle=tmp_path / "function.zip"
    )
