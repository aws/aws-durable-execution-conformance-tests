# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
import copy
import io
import json
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from aws_durable_execution_conformance_tests.lmi import artifacts as artifacts_module
from aws_durable_execution_conformance_tests.lmi import deployment as deployment_module
from aws_durable_execution_conformance_tests.lmi.artifacts import clear_run_artifacts, save
from aws_durable_execution_conformance_tests.lmi.deployment import DeploymentManager
from aws_durable_execution_conformance_tests.lmi.errors import CollectionError, ProvisioningError
from aws_durable_execution_conformance_tests.lmi.runtime import CONCURRENCIES, QUALIFIER, SCALING

from .support import RUNTIME

# Captured before the autouse fixture below ever patches the name, so the
# per-platform tests can restore the real implementation and exercise it.
_REAL_BOOT_IDENTITY = artifacts_module._boot_identity


@pytest.fixture(autouse=True)
def _stable_boot_identity(monkeypatch):
    """Pin owner_lock's boot identity so these tests do not depend on the real host.

    Individual tests that exercise an unavailable or foreign identity still
    override this with their own, more specific monkeypatch.
    """
    monkeypatch.setattr(artifacts_module, "_boot_identity", lambda: "test-host-boot-id")


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
    wrong_cpu = copy.deepcopy(config)
    wrong_cpu["CapacityProviderConfig"]["LambdaManagedInstancesCapacityProviderConfig"][
        "ExecutionEnvironmentMemoryGiBPerVCpu"
    ] = 4
    with pytest.raises(ProvisioningError):
        deploy.verify(wrong_cpu, scaling, manifest, "c1")
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
    objects: dict[str, bytes] = {}

    def get_object(*, Bucket, Key):
        if Key not in objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(objects[Key]), "ETag": f'"etag-{Key}"'}

    def put_object(*, Bucket, Key, Body, IfNoneMatch=None):
        if IfNoneMatch and Key in objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        objects[Key] = Body
        return {"ResponseMetadata": {"RetryAttempts": 0}}

    def delete_object(*, Bucket, Key, IfMatch):
        assert IfMatch == f'"etag-{Key}"'
        del objects[Key]

    clients["s3"].get_object.side_effect = get_object
    clients["s3"].put_object.side_effect = put_object
    clients["s3"].delete_object.side_effect = delete_object
    clients["lambda"].get_function_configuration.return_value = {}
    return clients


def assert_deployment_fenced(storage, update_count):
    holds = [call for call in storage.put_object.call_args_list if call.kwargs["Key"].startswith("quarantine/")]
    assert len(holds) == update_count * 2
    assert storage.delete_object.call_count == (update_count - 1) * 2


def test_later_run_updates_fixed_functions_without_recreating_resources(deploy, manifest, apis, monkeypatch):
    cfn, lam = apis["cloudformation"], apis["lambda"]
    monkeypatch.setattr(deploy, "owned_stack", Mock(side_effect=[None, *([stack(manifest)] * 3)]))
    monkeypatch.setattr(deploy, "wait_stack", Mock(return_value=stack(manifest)))
    monkeypatch.setattr(deploy, "wait_update", Mock(return_value="UPDATE_COMPLETE"))
    idle = Mock()
    monkeypatch.setattr(deploy, "wait_for_idle_functions", idle)

    def verify_with_fences(*_):
        assert apis["s3"].delete_object.call_count == ((verify.call_count - 1) // 2) * 2

    verify = Mock(side_effect=verify_with_fences)
    monkeypatch.setattr(deploy, "verify", verify)
    cfn.update_stack.side_effect = lambda **_: (assert_deployment_fenced(apis["s3"], cfn.update_stack.call_count))
    lam.get_function_configuration.return_value = {"Environment": {"Variables": {"secret": "redacted"}}}
    deploy.deploy_stack(manifest, b"first-code")
    assert "Environment" not in json.loads((deploy.artifacts / "configuration/c1.json").read_text())["function"]
    second = {
        **manifest,
        "run": "second",
        "codeKey": "code/new-digest.zip",
        "functions": {},
    }
    deploy.deploy_stack(second, b"second-code")
    cfn.create_stack.assert_called_once()
    assert cfn.update_stack.call_count == 2
    assert apis["s3"].get_object.call_count == 12
    assert apis["s3"].delete_object.call_count == 4
    assert all(call.kwargs["Key"].startswith("quarantine/") for call in apis["s3"].get_object.call_args_list)
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
            },
            "ResponseMetadata": {"RetryAttempts": 0},
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
        Statuses=["RUNNING", "PAUSING", "PAUSED"],
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
    driver.run_invocations.return_value = ([{"arn": "this-run"}], [])
    construct = Mock(return_value=driver)
    monkeypatch.setattr(cloud, "Cloud", construct)
    deploy.cleanup(manifest)
    assert construct.call_args.kwargs["client_factory"]("lambda", manifest["region"]) is lam
    s3.put_object.assert_called_once_with(
        Bucket=manifest["bucket"], Key="runs/unit/control/release-all", Body=b"release"
    )
    assert driver.invocations == [{"arn": "this-run"}]
    driver.settle_case.assert_called_once()
    cfn.delete_stack.assert_not_called()
    lam.delete_function.assert_not_called()
    s3.delete_objects.assert_not_called()
    assert json.loads((tmp_path / "cleanup.json").read_text())["status"] == "RETAINED"
    driver.run_invocations.return_value = ([{"arn": "this-run"}], ["Unreconciled Invoke"])
    driver.settle_case.reset_mock()
    with pytest.raises(CollectionError, match="Unreconciled Invoke"):
        deploy.cleanup(manifest)
    driver.settle_case.assert_called_once()
    assert json.loads((tmp_path / "cleanup.json").read_text())["status"] == "FAILED"


@pytest.mark.parametrize(
    "failure",
    ["run-id", "stack-name", "environment", "account", "capacity", "build", "digest"],
)
def test_failed_preflight_keeps_prior_run_evidence(
    deploy, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: str
) -> None:
    """A rejected deployment cannot move evidence without the target fences."""
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
    assert json.loads((artifacts / "manifest.json").read_text())["run"] == "previous"
    assert (artifacts / "configuration/c1.json").is_file()
    assert (artifacts / "cleanup.json").is_file()
    assert (artifacts / "cloud.xml").is_file()
    assert list(tmp_path.glob("artifacts-previous-*")) == []
    deploy_stack.assert_not_called()
    clients["s3"].put_object.assert_not_called()


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


def test_manifest_is_archived_before_later_artifact_moves(deploy, monkeypatch, tmp_path):
    artifacts = tmp_path / "artifacts"
    save(artifacts / "manifest.json", {"run": "previous", "owned": True})
    save(artifacts / "configuration/c1.json", {"run": "previous"})
    replace = Path.replace

    def move(path, target):
        if path.name == "configuration":
            raise PermissionError("blocked")
        return replace(path, target)

    monkeypatch.setattr(Path, "replace", move)
    with pytest.raises(PermissionError, match="blocked"):
        clear_run_artifacts(artifacts)
    assert not (artifacts / "manifest.json").exists()
    (archive,) = tmp_path.glob("artifacts-previous-*")
    assert (archive / "manifest.json").is_file()


@pytest.fixture
def deploy(tmp_path):
    return DeploymentManager(
        runtime=RUNTIME, owner="test-lmi-e2e", artifacts=tmp_path, bundle=tmp_path / "function.zip"
    )


@pytest.mark.parametrize("drift", [{"AWS_RETRY_MODE": "legacy"}, {"AWS_MAX_ATTEMPTS": "10"}])
def test_verify_rejects_environment_drift(deploy, manifest, drift):
    # Retry mode and extra SDK settings change behavior but are absent from
    # the configuration identity, so the whole environment must match.
    config = copy.deepcopy(deploy.template(manifest)["Resources"]["c1Function"]["Properties"])
    config.update(State="Active", Version=QUALIFIER, CodeSha256="hash")
    scaling = {"AppliedFunctionScalingConfig": SCALING}
    deploy.verify(config, scaling, manifest, "c1")
    config["Environment"]["Variables"].update(drift)
    with pytest.raises(ProvisioningError):
        deploy.verify(config, scaling, manifest, "c1")


def test_retained_fence_blocks_deployment_before_upload_or_function_update(deploy, manifest, apis, monkeypatch):
    current_manifest = deploy.artifacts / "manifest.json"
    save(current_manifest, {"run": "active"})
    retained = stack(manifest)
    apis["cloudformation"].describe_stacks.return_value = {"Stacks": [retained]}
    resource = retained["Outputs"][0]["OutputValue"]
    from aws_durable_execution_conformance_tests.lmi.quarantine import fence_key

    def get_object(*, Key, **_):
        if Key == fence_key(resource):
            return {
                "Body": io.BytesIO(json.dumps({"resourceId": resource, "runId": "previous", "commit": "old"}).encode())
            }
        raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

    apis["s3"].get_object.side_effect = get_object
    with pytest.raises(ProvisioningError, match="quarantined by run previous"):
        deploy.deploy_stack(manifest, b"new-code", capacity={})
    assert json.loads(current_manifest.read_text()) == {"run": "active"}
    apis["cloudformation"].update_stack.assert_not_called()
    apis["s3"].put_object.assert_not_called()
    apis["lambda"].get_paginator.assert_not_called()


@pytest.mark.parametrize(
    "code, message",
    [
        ("ValidationError", "Update already in progress"),
        ("AccessDenied", "Access denied after an unknown attempt count"),
        ("ValidationError", "No updates are to be performed."),
    ],
)
def test_ambiguous_submission_holds_fences_until_its_own_rollback(deploy, manifest, apis, monkeypatch, code, message):
    cfn, s3 = apis["cloudformation"], apis["s3"]
    cfn.describe_stacks.return_value = {"Stacks": [stack(manifest)]}
    monkeypatch.setattr(deploy, "wait_for_idle_functions", Mock())
    monkeypatch.setattr(deployment_module.time, "sleep", Mock())
    monkeypatch.setattr(deployment_module, "_verified_recovery_noop", Mock(return_value=False))
    cfn.update_stack.side_effect = ClientError({"Error": {"Code": code, "Message": message}}, "UpdateStack")
    polls = 0

    def stack_events(**_):
        nonlocal polls
        polls += 1
        s3.delete_object.assert_not_called()
        token = cfn.update_stack.call_args.kwargs["ClientRequestToken"]
        return {
            "StackEvents": [
                {
                    "ResourceType": "AWS::CloudFormation::Stack",
                    "LogicalResourceId": manifest["stack"],
                    "ClientRequestToken": "older-update",
                    "ResourceStatus": "UPDATE_COMPLETE",
                },
                {
                    "ResourceType": "AWS::CloudFormation::Stack",
                    "LogicalResourceId": manifest["stack"],
                    "ClientRequestToken": token,
                    "ResourceStatus": ("UPDATE_ROLLBACK_IN_PROGRESS" if polls == 1 else "UPDATE_ROLLBACK_COMPLETE"),
                },
            ]
        }

    cfn.describe_stack_events.side_effect = stack_events
    with pytest.raises(ProvisioningError, match="UPDATE_ROLLBACK_COMPLETE"):
        deploy.deploy_stack(manifest, b"new-code")
    assert polls == 2
    assert s3.delete_object.call_count == 2


def test_failed_atomic_artifact_update_preserves_existing_journal(tmp_path, monkeypatch):
    from aws_durable_execution_conformance_tests.lmi import artifacts

    journal = tmp_path / "journal.json"
    save(journal, {"state": "PREPARED"})

    def broken_dump(value, stream, **kwargs):
        stream.write("{partial")
        raise OSError("full disk")

    monkeypatch.setattr(artifacts.json, "dump", broken_dump)
    with pytest.raises(OSError, match="full disk"):
        save(journal, {"state": "SUBMITTED"})
    assert json.loads(journal.read_text()) == {"state": "PREPARED"}
    assert list(tmp_path.iterdir()) == [journal]


@pytest.mark.parametrize(
    "error",
    [
        ClientError(
            {
                "Error": {"Code": "ValidationError", "Message": "Template format error: invalid"},
                "ResponseMetadata": {"RetryAttempts": 0},
            },
            "UpdateStack",
        ),
        deployment_module.ParamValidationError(report="invalid parameter"),
    ],
)
def test_proven_stack_rejection_releases_fences_without_wait(deploy, manifest, apis, monkeypatch, error):
    cfn, s3 = apis["cloudformation"], apis["s3"]
    cfn.describe_stacks.return_value = {"Stacks": [stack(manifest)]}
    monkeypatch.setattr(deploy, "wait_for_idle_functions", Mock())
    cfn.update_stack.side_effect = error
    with pytest.raises(type(error)):
        deploy.deploy_stack(manifest, b"new-code")
    cfn.describe_stack_events.assert_not_called()
    assert s3.delete_object.call_count == 2


def test_interrupted_update_recovers_exact_operation_using_persisted_token(deploy, manifest, apis, monkeypatch):
    cfn, s3 = apis["cloudformation"], apis["s3"]
    cfn.describe_stacks.return_value = {"Stacks": [stack(manifest)]}
    monkeypatch.setattr(deploy, "wait_for_idle_functions", Mock())
    monkeypatch.setattr(deployment_module.time, "sleep", Mock())
    cfn.describe_stack_events.side_effect = KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        deploy.deploy_stack(manifest, b"new-code")
    s3.delete_object.assert_not_called()
    journal = deploy.artifacts / "deployment-update.json"
    record = json.loads(journal.read_text())
    assert record["requestToken"] == cfn.update_stack.call_args.kwargs["ClientRequestToken"]
    cfn.describe_stack_events.side_effect = [
        ClientError({"Error": {"Code": "Throttling"}}, "DescribeStackEvents"),
        {
            "StackEvents": [
                {
                    "ResourceType": "AWS::CloudFormation::Stack",
                    "LogicalResourceId": manifest["stack"],
                    "ClientRequestToken": record["requestToken"],
                    "ResourceStatus": "UPDATE_COMPLETE",
                }
            ]
        },
    ]
    deploy.recover_update(cfn, manifest)
    assert s3.delete_object.call_count == 2
    assert not journal.exists()


def test_public_cleanup_releases_pre_submission_fences_without_case_manifests(deploy, manifest, apis, tmp_path):
    from aws_durable_execution_conformance_tests.lmi import runner
    from aws_durable_execution_conformance_tests.lmi.quarantine import acquire_fence

    s3 = apis["s3"]
    output = tmp_path / "custom-cases"
    manifest["deploymentArtifactDirectory"] = str(deploy.artifacts)
    target = deploy.function_targets(manifest)["c1"]
    token = acquire_fence(s3, manifest, target)
    journal = deploy.artifacts / "deployment-update.json"
    fences = {arn: token if arn == target else "not-acquired" for arn in deploy.function_targets(manifest).values()}
    with deployment_module.owner_lock(journal) as owner:
        save(
            journal,
            {
                **{key: manifest[key] for key in ("stack", "bucket", "run", "commit")},
                "requestToken": "not-submitted",
                "owner": owner,
                "phase": "PREPARED",
                "fences": fences,
                "fenceStates": {arn: ("ACQUIRED" if arn == target else "PREPARED") for arn in fences},
            },
        )
    assert runner.cleanup(manifest, output, client_factory=lambda service, _: apis[service]) == 0
    assert not journal.exists()
    s3.delete_object.assert_called_once()
    apis["cloudformation"].describe_stack_events.assert_not_called()


def test_cleanup_replays_exact_update_after_interruption_before_send(deploy, manifest, apis, tmp_path):
    from aws_durable_execution_conformance_tests.lmi import runner
    from aws_durable_execution_conformance_tests.lmi.quarantine import acquire_fence

    cfn, s3 = apis["cloudformation"], apis["s3"]
    manifest["deploymentArtifactDirectory"] = str(deploy.artifacts.resolve())
    fences = {target: acquire_fence(s3, manifest, target) for target in deploy.function_targets(manifest).values()}
    request = {
        "StackName": manifest["stack"],
        "ClientRequestToken": "stable-replay-token",
        "TemplateBody": '{"Resources":{}}',
        "Tags": [{"Key": "example", "Value": "value"}],
    }
    journal = deploy.artifacts / "deployment-update.json"
    with deployment_module.owner_lock(journal) as owner:
        save(
            journal,
            {
                **{key: manifest[key] for key in ("stack", "bucket", "run", "commit")},
                "recoveryManifest": manifest,
                "requestToken": request["ClientRequestToken"],
                "owner": owner,
                "phase": "SUBMITTING",
                "request": request,
                "fences": fences,
            },
        )
    cfn.describe_stack_events.side_effect = [
        ClientError({"Error": {"Code": "Throttling"}}, "DescribeStackEvents"),
        {"StackEvents": []},
        {
            "StackEvents": [
                {
                    "ResourceType": "AWS::CloudFormation::Stack",
                    "LogicalResourceId": manifest["stack"],
                    "ClientRequestToken": request["ClientRequestToken"],
                    "ResourceStatus": "UPDATE_COMPLETE",
                }
            ]
        },
    ]
    # Existing CLI readers can still see the previous deployment identity.
    previous_manifest = {**manifest, "run": "previous"}
    save(deploy.artifacts / "manifest.json", previous_manifest)
    old_case_manifest = tmp_path / "shared" / "standard" / "manifest.json"
    old_cleanup = tmp_path / "shared" / "cleanup.json"
    save(old_case_manifest, {"runId": "previous"})
    save(old_cleanup, {"runId": "previous", "status": "CLEANUP_REQUESTED"})
    cleanup_manifest = deployment_module.load_cleanup_manifest(deploy.artifacts, manifest["run"])
    assert cleanup_manifest["run"] == manifest["run"]
    assert runner.cleanup(cleanup_manifest, tmp_path / "shared", client_factory=lambda service, _: apis[service]) == 0
    cfn.update_stack.assert_called_once_with(**request)
    assert s3.delete_object.call_count == 2
    assert not journal.exists()
    assert json.loads(old_case_manifest.read_text())["runId"] == "previous"
    assert json.loads(old_cleanup.read_text())["runId"] == "previous"
    receipt = tmp_path / "cleanup" / runner.case_key(manifest["run"]) / "cleanup.json"
    assert json.loads(receipt.read_text()) == {
        "runId": manifest["run"],
        "status": "NO_CASES",
        "errors": [],
        "reason": "No shared case manifests were recorded",
    }


@pytest.mark.parametrize("drift", [False, True])
def test_recovery_noop_requires_idle_owned_stack_and_matching_configuration(deploy, manifest, apis, drift):
    cfn, lam = apis["cloudformation"], apis["lambda"]
    manifest.update(RUNTIME.to_manifest(), suiteOwner="test-lmi-e2e")
    cfn.describe_stacks.return_value = {"Stacks": [stack(manifest)]}
    lam.get_paginator.return_value.paginate.return_value = [{"DurableExecutions": []}]
    resources = deploy.template(manifest)["Resources"]
    lam.get_function_configuration.side_effect = [
        {
            **resources[f"{key}Function"]["Properties"],
            "State": "Active",
            "Version": QUALIFIER,
            "CodeSha256": "drift" if drift else manifest["codeSha256"],
        }
        for key in CONCURRENCIES
    ]
    lam.get_function_scaling_config.return_value = {"AppliedFunctionScalingConfig": SCALING}
    if drift:
        with pytest.raises(ProvisioningError, match="readback mismatch"):
            deployment_module._verified_recovery_noop(cfn, manifest, deploy.artifacts, apis.__getitem__)
    else:
        assert deployment_module._verified_recovery_noop(cfn, manifest, deploy.artifacts, apis.__getitem__)


@pytest.mark.parametrize("field", ["run", "stack", "bucket", "commit"])
def test_recovery_rejects_journal_for_a_different_selected_deployment(deploy, manifest, field):
    journal = deploy.artifacts / "deployment-update.json"
    manifest["deploymentArtifactDirectory"] = str(deploy.artifacts.resolve())
    other = {**manifest, field: "unrelated"}
    save(
        journal,
        {
            **{key: other[key] for key in ("stack", "bucket", "run", "commit")},
            "recoveryManifest": other,
            "phase": "SUBMITTING",
        },
    )
    clients = Mock()
    with pytest.raises(ProvisioningError, match="another run"):
        deployment_module.recover_deployment_update(manifest, deploy.artifacts, clients)
    clients.assert_not_called()
    assert journal.is_file()


def test_noop_recovery_journals_settlement_before_partial_fence_release(deploy, manifest, apis, monkeypatch):
    from aws_durable_execution_conformance_tests.lmi.quarantine import acquire_fence

    cfn, storage = apis["cloudformation"], apis["s3"]
    manifest["deploymentArtifactDirectory"] = str(deploy.artifacts.resolve())
    fences = {arn: acquire_fence(storage, manifest, arn) for arn in deploy.function_targets(manifest).values()}
    journal = deploy.artifacts / "deployment-update.json"
    request = {"StackName": manifest["stack"], "ClientRequestToken": "noop-token", "TemplateBody": "{}"}
    with deployment_module.owner_lock(journal) as owner:
        save(
            journal,
            {
                **{key: manifest[key] for key in ("stack", "bucket", "run", "commit")},
                "recoveryManifest": manifest,
                "owner": owner,
                "phase": "SUBMITTING",
                "requestToken": "noop-token",
                "request": request,
                "fences": fences,
            },
        )
    cfn.describe_stack_events.return_value = {"StackEvents": []}
    cfn.update_stack.side_effect = ClientError(
        {"Error": {"Code": "ValidationError", "Message": "No updates are to be performed."}}, "UpdateStack"
    )
    monkeypatch.setattr(deployment_module, "_verified_recovery_noop", Mock(return_value=True))
    remove = storage.delete_object.side_effect

    def interrupted(**kwargs):
        assert json.loads(journal.read_text())["phase"] == "SETTLED"
        if storage.delete_object.call_count == 2:
            raise KeyboardInterrupt
        return remove(**kwargs)

    storage.delete_object.side_effect = interrupted
    with pytest.raises(KeyboardInterrupt):
        deployment_module.recover_deployment_update(manifest, deploy.artifacts, apis.__getitem__)
    storage.delete_object.side_effect = remove
    deployment_module.recover_deployment_update(manifest, deploy.artifacts, apis.__getitem__)
    cfn.update_stack.assert_called_once_with(**request)
    assert not journal.exists()


def test_direct_cleanup_recovers_pre_ownership_fences(deploy, manifest, apis):
    from aws_durable_execution_conformance_tests.lmi.quarantine import acquire_fence

    manifest.pop("owned")
    token = "pre-ownership"
    target = deploy.function_targets(manifest)["c1"]
    acquire_fence(apis["s3"], manifest, target, token=token)
    journal = deploy.artifacts / "deployment-update.json"
    with deployment_module.owner_lock(journal) as owner:
        save(
            journal,
            {
                **{key: manifest[key] for key in ("stack", "bucket", "run", "commit")},
                "owner": owner,
                "phase": "PREPARED",
                "fences": {arn: token for arn in deploy.function_targets(manifest).values()},
                "fenceStates": {
                    arn: ("ACQUIRED" if arn == target else "PREPARED")
                    for arn in deploy.function_targets(manifest).values()
                },
            },
        )
    deploy.cleanup(manifest)
    assert not journal.exists()
    apis["s3"].delete_object.assert_called_once()
    apis["cloudformation"].describe_stacks.assert_not_called()


@pytest.mark.parametrize("single_attempt", [False, True])
def test_interrupted_fence_write_retains_its_token_even_if_initially_absent(deploy, manifest, apis, single_attempt):
    storage, cfn = apis["s3"], apis["cloudformation"]
    cfn.describe_stacks.return_value = {"Stacks": [stack(manifest)]}
    put = storage.put_object.side_effect
    journal = deploy.artifacts / "deployment-update.json"

    def interrupted(**_):
        assert "ACQUIRING" in json.loads(journal.read_text())["fenceStates"].values()
        raise KeyboardInterrupt

    storage.put_object.side_effect = interrupted
    with pytest.raises(KeyboardInterrupt):
        deploy.deploy_stack(manifest, b"test")
    saved = json.loads(journal.read_text())
    save(journal, {**saved, "singleAttemptFences": single_attempt})
    original_put = storage.put_object.call_args.kwargs
    storage.put_object.side_effect = put
    with pytest.raises(ProvisioningError, match="outcome is unresolved"):
        deployment_module.recover_deployment_update(manifest, deploy.artifacts, apis.__getitem__)
    assert journal.is_file()
    storage.put_object(**original_put)
    if single_attempt:
        deployment_module.recover_deployment_update(manifest, deploy.artifacts, apis.__getitem__)
        assert not journal.exists()
        storage.delete_object.assert_called_once()
    else:
        with pytest.raises(ProvisioningError, match="outcome is unresolved"):
            deployment_module.recover_deployment_update(manifest, deploy.artifacts, apis.__getitem__)
        assert journal.is_file()
        storage.delete_object.assert_not_called()
    cfn.update_stack.assert_not_called()


@pytest.mark.parametrize("code", ["ExpiredToken", "ConditionalRequestConflict"])
def test_definitive_rejection_does_not_leave_ambiguous_deployment_fence(deploy, manifest, apis, code):
    storage, cfn = apis["s3"], apis["cloudformation"]
    cfn.describe_stacks.return_value = {"Stacks": [stack(manifest)]}
    storage.put_object.side_effect = ClientError(
        {"Error": {"Code": code}, "ResponseMetadata": {"RetryAttempts": 0}}, "PutObject"
    )
    with pytest.raises(ClientError, match=code):
        deploy.deploy_stack(manifest, b"test")
    assert not (deploy.artifacts / "deployment-update.json").exists()
    storage.delete_object.assert_not_called()
    cfn.update_stack.assert_not_called()


def test_default_fence_transport_retry_downgrades_persisted_certainty(deploy, manifest, apis, monkeypatch):
    storage, cfn = apis["s3"], apis["cloudformation"]
    cfn.describe_stacks.return_value = {"Stacks": [stack(manifest)]}
    put = storage.put_object.side_effect
    journal = deploy.artifacts / "deployment-update.json"

    def default_client(service, *args, **kwargs):
        return apis[service]

    def retried(**kwargs):
        put(**kwargs)
        return {"ResponseMetadata": {"RetryAttempts": 1}}

    monkeypatch.setattr(deployment_module, "client", default_client)
    monkeypatch.setattr(deploy, "client", default_client)
    storage.put_object.side_effect = retried
    with pytest.raises(ProvisioningError, match="single-attempt response"):
        deploy.deploy_stack(manifest, b"test")
    saved = json.loads(journal.read_text())
    assert saved["singleAttemptFences"] is False
    assert "ACQUIRING" in saved["fenceStates"].values()
    with pytest.raises(ProvisioningError, match="outcome is unresolved"):
        deployment_module.recover_deployment_update(manifest, deploy.artifacts, apis.__getitem__)
    storage.delete_object.assert_not_called()
    cfn.update_stack.assert_not_called()


def test_retried_noop_update_requires_exact_idle_stack_readback(deploy, manifest, apis, monkeypatch):
    cfn = apis["cloudformation"]
    cfn.describe_stacks.return_value = {"Stacks": [stack(manifest)]}
    cfn.update_stack.side_effect = ClientError(
        {
            "Error": {"Code": "ValidationError", "Message": "No updates are to be performed."},
            "ResponseMetadata": {"RetryAttempts": 1},
        },
        "UpdateStack",
    )
    noop = Mock(return_value=True)
    monkeypatch.setattr(deploy, "wait_for_idle_functions", Mock())
    monkeypatch.setattr(deploy, "verify", Mock())
    monkeypatch.setattr(deployment_module, "_verified_recovery_noop", noop)
    deploy.deploy_stack(manifest, b"test")
    noop.assert_called_once_with(cfn, manifest, deploy.artifacts, deploy.client)
    cfn.describe_stack_events.assert_not_called()
    assert apis["s3"].delete_object.call_count == 2


def test_explicit_noop_retry_uses_positive_readback(deploy, manifest, apis, monkeypatch):
    from botocore.exceptions import ReadTimeoutError

    cfn = apis["cloudformation"]
    cfn.describe_stacks.return_value = {"Stacks": [stack(manifest)]}
    cfn.update_stack.side_effect = [
        ReadTimeoutError(endpoint_url="test"),
        ClientError(
            {"Error": {"Code": "ValidationError", "Message": "No updates are to be performed."}}, "UpdateStack"
        ),
    ]
    verify = Mock(return_value=True)
    monkeypatch.setattr(deploy, "wait_for_idle_functions", Mock())
    monkeypatch.setattr(deploy, "verify", Mock())
    monkeypatch.setattr(deployment_module, "_verified_recovery_noop", verify)
    deploy.deploy_stack(manifest, b"new")
    assert cfn.update_stack.call_args_list[0] == cfn.update_stack.call_args_list[1]
    verify.assert_called_once()
    cfn.describe_stack_events.assert_not_called()
    assert apis["s3"].delete_object.call_count == 2


def test_deployment_journal_race_preserves_winning_writer(deploy, manifest, apis, monkeypatch):
    cfn = apis["cloudformation"]
    cfn.describe_stacks.return_value = {"Stacks": [stack(manifest)]}
    actual_save = deployment_module.save

    def raced(path, value, **kwargs):
        if kwargs.get("exclusive"):
            actual_save(path, {**value, "requestToken": "winner"})
        return actual_save(path, value, **kwargs)

    monkeypatch.setattr(deployment_module, "save", raced)
    with pytest.raises(ProvisioningError, match="Existing deployment update journal"):
        deploy.deploy_stack(manifest, b"new")
    journal = deploy.artifacts / "deployment-update.json"
    assert json.loads(journal.read_text())["requestToken"] == "winner"
    apis["s3"].put_object.assert_not_called()
    cfn.update_stack.assert_not_called()


def test_live_deployment_lock_prevents_recovery_during_fence_acquisition(deploy, manifest, apis, monkeypatch):
    storage, cfn = apis["s3"], apis["cloudformation"]
    cfn.describe_stacks.return_value = {"Stacks": [stack(manifest)]}
    put = storage.put_object.side_effect

    def put_and_attempt_recovery(**kw):
        result = put(**kw)
        if kw.get("IfNoneMatch"):
            with pytest.raises(ProvisioningError, match="Journal ownership is busy"):
                deployment_module.recover_deployment_update(manifest, deploy.artifacts, apis.__getitem__)
            storage.delete_object.assert_not_called()
            raise KeyboardInterrupt
        return result

    storage.put_object.side_effect = put_and_attempt_recovery
    with pytest.raises(KeyboardInterrupt):
        deploy.deploy_stack(manifest, b"test")
    assert (deploy.artifacts / "deployment-update.json").is_file()


# --- Boot identity: owner_lock refuses to operate without one ---


def test_owner_lock_rejects_an_unavailable_boot_identity(tmp_path, monkeypatch):
    monkeypatch.setattr(artifacts_module, "_boot_identity", lambda: None)
    journal = tmp_path / "deployment-update.json"
    with pytest.raises(ProvisioningError, match="stable boot identity"), artifacts_module.owner_lock(journal):
        pass
    # Nothing on disk, not even the lock file itself, when the boot identity
    # cannot be established.
    assert not journal.exists()
    assert not artifacts_module.owner_lock_path(journal).exists()


def test_deploy_stack_requires_boot_identity_before_any_write(deploy, manifest, apis, monkeypatch):
    monkeypatch.setattr(artifacts_module, "_boot_identity", lambda: None)
    cfn, s3 = apis["cloudformation"], apis["s3"]
    with pytest.raises(ProvisioningError, match="stable boot identity"):
        deploy.deploy_stack(manifest, b"code")
    cfn.create_stack.assert_not_called()
    cfn.update_stack.assert_not_called()
    s3.put_object.assert_not_called()
    assert not (deploy.artifacts / "deployment-update.json").exists()


# --- Boot identity: recovery refuses only a record that is still uncertain ---


def test_recover_deployment_update_refuses_an_uncertain_journal_without_boot_identity(
    deploy, manifest, apis, monkeypatch
):
    """A non-SETTLED journal is "uncertain" and still needs same_owner_lock's falsy check."""
    cfn, s3 = apis["cloudformation"], apis["s3"]
    cfn.describe_stacks.return_value = {"Stacks": [stack(manifest)]}
    monkeypatch.setattr(deploy, "wait_for_idle_functions", Mock())
    cfn.describe_stack_events.side_effect = KeyboardInterrupt
    with pytest.raises(KeyboardInterrupt):
        deploy.deploy_stack(manifest, b"new-code")
    journal = deploy.artifacts / "deployment-update.json"
    assert journal.is_file()
    update_calls_before_recovery = cfn.update_stack.call_count
    monkeypatch.setattr(artifacts_module, "_boot_identity", lambda: None)
    with pytest.raises(ProvisioningError, match="lacks its original local owner lock"):
        deployment_module.recover_deployment_update(manifest, deploy.artifacts, apis.__getitem__)
    s3.delete_object.assert_not_called()
    assert cfn.update_stack.call_count == update_calls_before_recovery
    assert journal.is_file()


def test_recover_deployment_update_still_recovers_a_settled_journal_without_boot_identity(
    deploy, manifest, apis, monkeypatch
):
    """A SETTLED journal is not "uncertain": recovery needs only the flock, not an owner."""
    from aws_durable_execution_conformance_tests.lmi.quarantine import acquire_fence

    cfn, s3 = apis["cloudformation"], apis["s3"]
    manifest["deploymentArtifactDirectory"] = str(deploy.artifacts.resolve())
    fences = {target: acquire_fence(s3, manifest, target) for target in deploy.function_targets(manifest).values()}
    journal = deploy.artifacts / "deployment-update.json"
    with deployment_module.owner_lock(journal) as owner:
        save(
            journal,
            {
                **{key: manifest[key] for key in ("stack", "bucket", "run", "commit")},
                "owner": owner,
                "phase": "SETTLED",
                "fences": fences,
            },
        )
    monkeypatch.setattr(artifacts_module, "_boot_identity", lambda: None)
    assert deployment_module.recover_deployment_update(manifest, deploy.artifacts, apis.__getitem__) == manifest["run"]
    assert not journal.exists()
    assert s3.delete_object.call_count == len(fences)
    cfn.update_stack.assert_not_called()


# --- Boot identity: same_owner_lock refuses a foreign owner (defense in depth) ---


@pytest.mark.parametrize(
    "differing_field,replacement",
    [("bootId", "a-different-hosts-boot-id"), ("inode", 999999999)],
)
def test_recover_deployment_update_refuses_a_foreign_owner(deploy, manifest, apis, differing_field, replacement):
    cfn, s3 = apis["cloudformation"], apis["s3"]
    manifest["deploymentArtifactDirectory"] = str(deploy.artifacts.resolve())
    targets = deploy.function_targets(manifest).values()
    journal = deploy.artifacts / "deployment-update.json"
    with deployment_module.owner_lock(journal) as owner:
        foreign_owner = {**owner, differing_field: replacement}
        save(
            journal,
            {
                **{key: manifest[key] for key in ("stack", "bucket", "run", "commit")},
                "recoveryManifest": manifest,
                "requestToken": "replay-token",
                "owner": foreign_owner,
                "phase": "SUBMITTING",
                "fences": {target: "fence-token" for target in targets},
            },
        )
    with pytest.raises(ProvisioningError, match="lacks its original local owner lock"):
        deployment_module.recover_deployment_update(manifest, deploy.artifacts, apis.__getitem__)
    cfn.update_stack.assert_not_called()
    s3.delete_object.assert_not_called()
    assert journal.is_file()


def test_same_owner_lock_refuses_matching_owners_without_a_boot_identity():
    # Recovery on a host without a boot identity yields bootId None. Even an
    # exact owner match cannot prove the writer ran on this kernel, so an
    # uncertain record must still be refused.
    owner = {"bootId": None, "device": 1, "inode": 2, "root": "/artifacts"}
    assert not artifacts_module.same_owner_lock({"owner": owner}, owner)
    booted = {**owner, "bootId": "boot"}
    assert artifacts_module.same_owner_lock({"owner": booted}, booted)


def test_recover_deployment_update_refuses_an_uncertain_journal_whose_owner_also_lacks_a_boot_identity(
    deploy, manifest, apis, monkeypatch
):
    cfn, s3 = apis["cloudformation"], apis["s3"]
    monkeypatch.setattr(artifacts_module, "_boot_identity", lambda: None)
    manifest["deploymentArtifactDirectory"] = str(deploy.artifacts.resolve())
    targets = deploy.function_targets(manifest).values()
    journal = deploy.artifacts / "deployment-update.json"
    # The stored owner matches the recovery owner exactly, bootId None included.
    with deployment_module.owner_lock(journal, require_boot_identity=False) as owner:
        assert owner["bootId"] is None
        save(
            journal,
            {
                **{key: manifest[key] for key in ("stack", "bucket", "run", "commit")},
                "recoveryManifest": manifest,
                "requestToken": "replay-token",
                "owner": owner,
                "phase": "SUBMITTING",
                "fences": {target: "fence-token" for target in targets},
            },
        )
    with pytest.raises(ProvisioningError, match="lacks its original local owner lock"):
        deployment_module.recover_deployment_update(manifest, deploy.artifacts, apis.__getitem__)
    cfn.update_stack.assert_not_called()
    s3.delete_object.assert_not_called()
    assert journal.is_file()


@pytest.mark.skipif(
    not (sys.platform.startswith("linux") or sys.platform == "darwin"),
    reason="needs a Linux or macOS boot identity",
)
def test_boot_identity_reads_a_stable_value_on_this_host():
    first, second = _REAL_BOOT_IDENTITY(), _REAL_BOOT_IDENTITY()
    assert first
    assert first == second


# --- Boot identity: per-platform reading, independent of the real host ---


def test_boot_identity_reads_the_linux_boot_id_file(tmp_path, monkeypatch):
    boot_id_file = tmp_path / "boot_id"
    boot_id_file.write_text("f47ac10b-58cc-4372-a567-0e02b2c3d479\n")
    monkeypatch.setattr(artifacts_module, "_boot_identity", _REAL_BOOT_IDENTITY)
    monkeypatch.setattr(artifacts_module.sys, "platform", "linux")
    monkeypatch.setattr(artifacts_module, "_LINUX_BOOT_ID_PATH", boot_id_file)
    assert artifacts_module._boot_identity() == "f47ac10b-58cc-4372-a567-0e02b2c3d479"


def test_boot_identity_reads_the_linux_boot_id_file_failure_returns_none(tmp_path, monkeypatch):
    monkeypatch.setattr(artifacts_module, "_boot_identity", _REAL_BOOT_IDENTITY)
    monkeypatch.setattr(artifacts_module.sys, "platform", "linux")
    monkeypatch.setattr(artifacts_module, "_LINUX_BOOT_ID_PATH", tmp_path / "missing-boot-id")
    assert artifacts_module._boot_identity() is None


def test_boot_identity_uses_the_macos_sysctl_value(monkeypatch):
    monkeypatch.setattr(artifacts_module, "_boot_identity", _REAL_BOOT_IDENTITY)
    monkeypatch.setattr(artifacts_module.sys, "platform", "darwin")
    monkeypatch.setattr(artifacts_module, "_macos_boot_session_uuid", lambda: "6F3C1B2A-DARWIN-BOOT-SESSION")
    assert artifacts_module._boot_identity() == "6F3C1B2A-DARWIN-BOOT-SESSION"


def test_boot_identity_on_an_unknown_platform_returns_none(monkeypatch):
    monkeypatch.setattr(artifacts_module, "_boot_identity", _REAL_BOOT_IDENTITY)
    monkeypatch.setattr(artifacts_module.sys, "platform", "win32")
    assert artifacts_module._boot_identity() is None


def test_macos_boot_session_uuid_reads_the_sysctl_cli(monkeypatch):
    completed = Mock(stdout="6F3C1B2A-DARWIN-BOOT-SESSION\n")
    run = Mock(return_value=completed)
    monkeypatch.setattr(artifacts_module.subprocess, "run", run)
    assert artifacts_module._macos_boot_session_uuid() == "6F3C1B2A-DARWIN-BOOT-SESSION"
    run.assert_called_once_with(
        ["/usr/sbin/sysctl", "-n", "kern.bootsessionuuid"],
        capture_output=True,
        text=True,
        timeout=5,
        check=True,
    )


def test_macos_boot_session_uuid_returns_none_when_the_sysctl_cli_fails(monkeypatch):
    monkeypatch.setattr(
        artifacts_module.subprocess,
        "run",
        Mock(side_effect=OSError("sysctl: command not found")),
    )
    assert artifacts_module._macos_boot_session_uuid() is None
