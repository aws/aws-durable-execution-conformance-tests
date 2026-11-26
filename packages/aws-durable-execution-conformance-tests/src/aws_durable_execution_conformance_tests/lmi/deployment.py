# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Reusable persistent LMI stack deployment; runtime/build choices are caller-owned."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    ConnectionError,
    HTTPClientError,
    NoCredentialsError,
    ParamValidationError,
    PartialCredentialsError,
)

from .artifacts import clear_run_artifacts, owner_lock, owner_lock_path, same_owner_lock, save
from .errors import CollectionError, ProvisioningError
from .quarantine import FenceRetryUncertain, acquire_fence, fence_matches, read_fence, release_fence
from .runtime import CONCURRENCIES, MEMORY_GIB_PER_VCPU, QUALIFIER, SCALING, RuntimeDefinition
from .service import OUTSTANDING_EXECUTION_STATUSES, client, definitive_s3_write_rejection, retryable_read_error


def verify_profile(runtime: RuntimeDefinition, config, scaling, manifest, key) -> None:
    """Check one deployed function's readback against its manifest and runtime.

    Shared by ``DeploymentManager.verify`` (the deploying SDK) and the default
    ``Cloud`` verifier (derived from the recorded ``RuntimeDefinition``), so both
    paths apply exactly the same profile checks.
    """
    actual = config.get("CapacityProviderConfig", {}).get("LambdaManagedInstancesCapacityProviderConfig", {})
    checks = [
        actual.get("CapacityProviderArn") == manifest["provider"],
        actual.get("PerExecutionEnvironmentMaxConcurrency") == manifest["concurrencies"][key],
        actual.get("ExecutionEnvironmentMemoryGiBPerVCpu") == MEMORY_GIB_PER_VCPU,
        config.get("Runtime") == manifest["runtime"],
        config.get("Role") == manifest["role"],
        config.get("Handler") == runtime.handler,
        not config.get("Layers", []),
        config.get("Environment", {}).get("Variables") == DeploymentManager.environment(manifest),
        config.get("Architectures") == [runtime.architecture],
        config.get("Version") == QUALIFIER,
        config.get("CodeSha256") == manifest["codeSha256"],
        config.get("MemorySize") == runtime.memory_size,
        config.get("State") == "Active",
        config.get("DurableConfig", {}).get("ExecutionTimeout") == manifest["executionTimeout"],
        config.get("Timeout") == manifest["invocationTimeout"],
        scaling.get("AppliedFunctionScalingConfig") == SCALING,
    ]
    if not all(checks):
        raise ProvisioningError(f"{key}: LMI configuration/artifact/scaling readback mismatch")


class DeploymentManager:
    def __init__(
        self,
        *,
        runtime: RuntimeDefinition,
        owner: str,
        artifacts: Path,
        bundle: Path,
        bucket_prefix: str = "lmi-e2e",
        client_factory: Callable[..., Any] | None = None,
    ):
        self.runtime = runtime
        self.owner = owner
        self.artifacts = Path(artifacts)
        self.bundle = Path(bundle)
        self.bucket_prefix = bucket_prefix
        self.client = client_factory or client

    def template(self, manifest, functions=True):
        bucket = manifest["bucket"]
        resources = {
            "Bucket": {
                "Type": "AWS::S3::Bucket",
                "Properties": {
                    "BucketName": bucket,
                    "PublicAccessBlockConfiguration": {
                        "BlockPublicAcls": True,
                        "IgnorePublicAcls": True,
                        "BlockPublicPolicy": True,
                        "RestrictPublicBuckets": True,
                    },
                    "BucketEncryption": {
                        "ServerSideEncryptionConfiguration": [
                            {"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
                        ]
                    },
                    "LifecycleConfiguration": {
                        "Rules": [
                            {
                                "Id": "expire-run-data",
                                "Status": "Enabled",
                                "Prefix": "runs/",
                                "ExpirationInDays": 1,
                                "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1},
                            }
                        ]
                    },
                },
            },
            "BucketPolicy": {
                "Type": "AWS::S3::BucketPolicy",
                "Properties": {
                    "Bucket": {"Ref": "Bucket"},
                    "PolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": {"AWS": manifest["role"]},
                                "Action": ["s3:GetObject", "s3:PutObject"],
                                "Resource": [
                                    f"arn:aws:s3:::{bucket}/runs/*",
                                ],
                            },
                            {
                                "Effect": "Deny",
                                "Principal": "*",
                                "Action": "s3:*",
                                "Resource": [
                                    f"arn:aws:s3:::{bucket}",
                                    f"arn:aws:s3:::{bucket}/*",
                                ],
                                "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                            },
                        ],
                    },
                },
            },
        }
        outputs = {}
        if functions:
            for key, concurrency in manifest["concurrencies"].items():
                name = manifest["stack"] + "-" + key
                resources[key + "Logs"] = {
                    "Type": "AWS::Logs::LogGroup",
                    "Properties": {
                        "LogGroupName": "/aws/lambda/" + name,
                        "RetentionInDays": 1,
                    },
                }
                resources[key + "Function"] = {
                    "Type": "AWS::Lambda::Function",
                    "DependsOn": "BucketPolicy",
                    "Properties": {
                        "FunctionName": name,
                        "Runtime": manifest["runtime"],
                        "Architectures": [self.runtime.architecture],
                        "Handler": self.runtime.handler,
                        "Role": manifest["role"],
                        "MemorySize": self.runtime.memory_size,
                        "Timeout": manifest["invocationTimeout"],
                        "Code": {"S3Bucket": bucket, "S3Key": manifest["codeKey"]},
                        "FunctionScalingConfig": SCALING,
                        "DurableConfig": {
                            "ExecutionTimeout": manifest["executionTimeout"],
                            "RetentionPeriodInDays": 1,
                        },
                        "CapacityProviderConfig": {
                            "LambdaManagedInstancesCapacityProviderConfig": {
                                "CapacityProviderArn": manifest["provider"],
                                "PerExecutionEnvironmentMaxConcurrency": concurrency,
                                "ExecutionEnvironmentMemoryGiBPerVCpu": MEMORY_GIB_PER_VCPU,
                            }
                        },
                        "Environment": {"Variables": self.environment(manifest)},
                        "LoggingConfig": {
                            "LogFormat": "JSON",
                            "ApplicationLogLevel": "INFO",
                            "SystemLogLevel": "INFO",
                            "LogGroup": {"Ref": key + "Logs"},
                        },
                    },
                }
                # LMI publishes this qualified version automatically, including its
                # scaling settings. An extra numbered version would allocate more capacity.
                outputs[key] = {
                    "Value": {
                        "Fn::Join": [
                            "",
                            [{"Fn::GetAtt": [key + "Function", "Arn"]}, ":" + QUALIFIER],
                        ]
                    }
                }
        return {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Resources": resources,
            "Outputs": outputs,
        }

    def wait_stack(self, cfn, name, expected, seconds=1500):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            try:
                result = cfn.describe_stacks(StackName=name)["Stacks"][0]
            except ClientError as error:
                if expected == "DELETE_COMPLETE" and "does not exist" in str(error):
                    return None
                raise
            status = result["StackStatus"]
            if status == expected:
                return result
            if "FAILED" in status or "ROLLBACK" in status:
                save(
                    self.artifacts / "stack-events.json",
                    cfn.describe_stack_events(StackName=name),
                )
                raise ProvisioningError(f"{name}: {status}; no fallback to standard Lambda")
            time.sleep(3)
        raise ProvisioningError(f"{name}: provisioning deadline exceeded; stack retained")

    @staticmethod
    def environment(manifest):
        """The complete function environment; anything else is drift."""
        return {
            "LMI_BUCKET": manifest["bucket"],
            "LMI_COMMIT": manifest["commit"],
            "LMI_RUN_ID": manifest["run"],
            "AWS_RETRY_MODE": "standard",
        }

    def verify(self, config, scaling, manifest, key):
        verify_profile(self.runtime, config, scaling, manifest, key)

    def deploy(self, args):
        if not args.run_id or not re.fullmatch(r"[a-z0-9-]{1,24}", args.run_id):
            raise ProvisioningError("run-id must be 1-24 lowercase letters, digits or hyphens")
        if not re.fullmatch(r"[a-z][a-z0-9-]{0,49}", args.stack_name):
            raise ProvisioningError(
                "stack-name must be 1-50 lowercase letters, digits or hyphens, starting with a letter"
            )
        provider = os.environ["CAPACITY_PROVIDER_ARN"]
        account = self.client("sts").get_caller_identity()["Account"]
        if account != os.environ["TEST_ACCOUNT_ID"] or provider.split(":")[3:5] != [
            os.environ["AWS_REGION"],
            account,
        ]:
            raise ProvisioningError("Authenticated account/region must match the test account and capacity provider")
        capacity = self.client("lambda").get_capacity_provider(
            CapacityProviderName=provider.rsplit(":", 1)[-1].split("/")[-1]
        )
        provider_config = capacity["CapacityProvider"]
        if not 2 <= provider_config.get("CapacityProviderScalingConfig", {}).get("MaxVCpuCount", 0) <= 128:
            raise ProvisioningError("Use a bounded, test-owned capacity provider (2-128 maximum vCPUs)")
        data = self.bundle.read_bytes()
        digest = hashlib.sha256(data).digest()
        built = json.loads((self.artifacts / "build.json").read_text())
        if built["sha256"] != digest.hex():
            raise ProvisioningError("Artifact changed since build")
        name = args.stack_name
        bucket_scope = hashlib.sha256(f"{os.environ['AWS_REGION']}:{name}".encode()).hexdigest()[:12]
        manifest = {
            **self.runtime.to_manifest(),
            "suiteOwner": self.owner,
            "run": args.run_id,
            "stack": name,
            "bucket": f"{self.bucket_prefix}-{account}-{bucket_scope}",
            "persistent": True,
            "account": account,
            "region": os.environ["AWS_REGION"],
            "provider": provider,
            "role": os.environ["TEST_LAMBDA_EXECUTION_ROLE_ARN"],
            "runtime": args.runtime,
            "concurrencies": CONCURRENCIES,
            "commit": built["commit"],
            "wheels": built.get("wheels", {}),
            "dependencies": built.get("dependencies", []),
            "codeKey": "code/" + digest.hex() + ".zip",
            "codeSha256": base64.b64encode(digest).decode(),
            "invocationTimeout": 60,
            "executionTimeout": 240,
            "driverTimeout": 120,
            "cleanupGrace": 5,
            "created": time.time(),
            "functions": {},
        }
        self.deploy_stack(manifest, data, capacity=capacity)

    def deploy_stack(self, manifest, data, *, capacity=None):
        journal = self.artifacts / "deployment-update.json"
        with owner_lock(journal) as owner:
            self._deploy_stack(manifest, data, capacity=capacity, owner=owner)

    def _deploy_stack(self, manifest, data, *, capacity, owner):
        """Bootstrap the bucket once, then update the same two functions in place."""
        cfn = self.client("cloudformation")
        tags = [
            {"Key": "Suite", "Value": self.owner},
            {"Key": "Stack", "Value": manifest["stack"]},
            {"Key": "Persistent", "Value": "true"},
        ]
        stack = self.owned_stack(cfn, manifest)
        if stack is None:
            cfn.create_stack(
                StackName=manifest["stack"],
                TemplateBody=json.dumps(self.template(manifest, False)),
                Tags=tags,
            )
            self.wait_stack(cfn, manifest["stack"], "CREATE_COMPLETE", 180)
        else:
            if stack["StackStatus"] not in {
                "CREATE_COMPLETE",
                "UPDATE_COMPLETE",
                "UPDATE_ROLLBACK_COMPLETE",
            }:
                raise ProvisioningError(
                    f"Persistent stack requires recovery from {stack['StackStatus']}; it was retained"
                )
        storage = self.client("s3")
        storage.put_public_access_block(
            Bucket=manifest["bucket"],
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )
        protection = storage.get_public_access_block(Bucket=manifest["bucket"])["PublicAccessBlockConfiguration"]
        if not all(
            protection.get(name) is True
            for name in (
                "BlockPublicAcls",
                "IgnorePublicAcls",
                "BlockPublicPolicy",
                "RestrictPublicBuckets",
            )
        ):
            raise ProvisioningError("Evidence bucket must be private before uploading the tested artifact")
        targets = self.function_targets(manifest)
        outputs = {item["OutputKey"]: item["OutputValue"] for item in (stack or {}).get("Outputs", [])}
        if outputs and outputs != targets:
            raise ProvisioningError("The existing stack outputs differ from the owned function targets")
        journal = self.artifacts / "deployment-update.json"
        if journal.exists():
            raise ProvisioningError("Existing deployment update journal requires reconciliation")
        tokens = {target: uuid.uuid4().hex for target in targets.values()}
        manifest["deploymentArtifactDirectory"] = str(self.artifacts.resolve())
        submission = {
            "recoveryManifest": dict(manifest),
            "stack": manifest["stack"],
            "bucket": manifest["bucket"],
            "run": manifest["run"],
            "commit": manifest["commit"],
            "requestToken": f"lmi-{uuid.uuid4().hex}",
            "fences": tokens,
            "fenceStates": dict.fromkeys(tokens, "PREPARED"),
            "singleAttemptFences": self.client is client,
            "phase": "PREPARED",
            "owner": owner,
        }
        try:
            save(journal, submission, exclusive=True)
        except FileExistsError as error:
            raise ProvisioningError("Existing deployment update journal requires reconciliation") from error
        active_manifest = self.artifacts / "manifest.json"
        preserve = frozenset({journal.name, active_manifest.name, owner_lock_path(journal).name})
        if not active_manifest.exists():
            save(active_manifest, manifest)
        update_pending = False
        primary: BaseException | None = None
        fence_storage = client("s3", manifest["region"], single_attempt=True) if self.client is client else storage
        try:
            for target, token in tokens.items():

                def before_put(target=target):
                    submission["fenceStates"][target] = "ACQUIRING"
                    save(journal, submission)

                try:
                    acquire_fence(
                        fence_storage,
                        manifest,
                        target,
                        token=token,
                        before_put=before_put,
                        single_attempt=submission["singleAttemptFences"],
                    )
                except FenceRetryUncertain:
                    submission["singleAttemptFences"] = False
                    save(journal, submission)
                    raise
                except ClientError as error:
                    if submission["fenceStates"][target] == "ACQUIRING" and definitive_s3_write_rejection(
                        error, precondition=True
                    ):
                        submission["fenceStates"][target] = "REJECTED"
                        save(journal, submission)
                    raise
                submission["fenceStates"][target] = "ACQUIRED"
                save(journal, submission)
            if capacity is not None:
                archive = clear_run_artifacts(self.artifacts, preserve=preserve)
                if archive is not None:
                    save(archive / active_manifest.name, json.loads(active_manifest.read_text()))
                save(active_manifest, manifest)
                save(self.artifacts / "capacity-provider.json", capacity)
            if stack is not None:
                self.wait_for_idle_functions(stack)

            def update_stack(spec):
                nonlocal update_pending
                request = {
                    "StackName": manifest["stack"],
                    "TemplateBody": json.dumps(spec),
                    "Tags": tags,
                    "ClientRequestToken": submission["requestToken"],
                }
                submission.update(phase="SUBMITTING", submittedAt=time.time(), request=request)
                save(journal, submission)
                update_pending = True
                try:
                    cfn.update_stack(**request)
                except (ParamValidationError, NoCredentialsError, PartialCredentialsError):
                    update_pending = False
                    raise
                except (BotoCoreError, ClientError) as error:
                    if isinstance(error, ClientError):
                        code = error.response["Error"]["Code"]
                        message = error.response["Error"].get("Message", "")
                        if (
                            code == "ValidationError"
                            and "No updates are to be performed" in message
                            and (
                                self.update_rejected(error)
                                or _verified_recovery_noop(cfn, manifest, self.artifacts, self.client)
                            )
                        ):
                            update_pending = False
                            return self.owned_stack(cfn, manifest)
                        if self.update_rejected(error):
                            update_pending = False
                            raise
                    if retryable_read_error(error):
                        # An earlier submission may still have won. Only a
                        # matching operation can settle the fences.
                        try:
                            cfn.update_stack(**request)
                        except (BotoCoreError, ClientError) as retry_error:
                            if (
                                isinstance(retry_error, ClientError)
                                and retry_error.response["Error"]["Code"] == "ValidationError"
                                and "No updates are to be performed" in retry_error.response["Error"].get("Message", "")
                                and _verified_recovery_noop(cfn, manifest, self.artifacts, self.client)
                            ):
                                update_pending = False
                                return self.owned_stack(cfn, manifest)
                status = self.wait_update(cfn, manifest["stack"], submission["requestToken"])
                update_pending = status not in {"UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE"}
                if status != "UPDATE_COMPLETE":
                    raise ProvisioningError(f"{manifest['stack']}: {status}")
                return self.owned_stack(cfn, manifest)

            self.update_functions(manifest, data, storage, targets, update_stack)
        except BaseException as error:
            primary = error
            raise
        finally:
            # If submission or polling failed before the update was known to
            # be terminal, continued ownership prevents early admission.
            if not update_pending:
                try:
                    save(journal, {**submission, "phase": "SETTLED"})
                    self.release_deployment_fences(
                        storage,
                        manifest,
                        tokens,
                        submission["fenceStates"],
                        single_attempt=submission["singleAttemptFences"],
                        persist=lambda: save(journal, {**submission, "phase": "SETTLED"}),
                    )
                    journal.unlink(missing_ok=True)
                except Exception as error:
                    if primary is None:
                        raise
                    primary.add_note(str(error))

    @staticmethod
    def release_deployment_fences(storage, manifest, tokens, states=None, *, single_attempt=False, persist=None):
        errors = []
        for target, token in tokens.items():
            try:
                state = states[target] if states is not None else "ACQUIRED"
                if state in {"PREPARED", "REJECTED"}:
                    continue
                if state == "ACQUIRING" and single_attempt and persist is not None:
                    existing = read_fence(storage, manifest["bucket"], target)
                    if fence_matches(existing, manifest, target, token):
                        # Preserve proof before a conditional delete can commit.
                        states[target] = state = "ACQUIRED"
                        persist()
                if state != "ACQUIRED":
                    raise ProvisioningError("Fence acquisition outcome is unresolved")
                existing = read_fence(storage, manifest["bucket"], target)
                if existing is not None and existing.get("token") == token:
                    release_fence(storage, manifest, target, token)
            except Exception as error:
                errors.append(f"{target}: {error}")
        if errors:
            raise ProvisioningError("Could not release deployment fences: " + "; ".join(errors))

    @staticmethod
    def function_targets(manifest):
        partition = manifest["provider"].split(":")[1]
        return {
            key: f"arn:{partition}:lambda:{manifest['region']}:{manifest['account']}"
            f":function:{manifest['stack']}-{key}:{QUALIFIER}"
            for key in CONCURRENCIES
        }

    @staticmethod
    def update_rejected(error):
        response = error.response
        code = response["Error"]["Code"]
        metadata = response.get("ResponseMetadata", {})
        retries = metadata.get("RetryAttempts")
        if type(retries) is not int or retries != 0:
            return False
        if code == "ValidationError":
            return True
        if code in {
            "AccessDenied",
            "AccessDeniedException",
            "InsufficientCapabilitiesException",
            "ExpiredToken",
            "ExpiredTokenException",
            "InvalidClientTokenId",
            "InvalidSignatureException",
            "SignatureDoesNotMatch",
            "UnrecognizedClientException",
        }:
            return True
        status = metadata.get("HTTPStatusCode")
        return (
            type(status) is int
            and 400 <= status < 500
            and status not in {408, 409, 429}
            and not retryable_read_error(error)
            and code not in {"TokenAlreadyExistsException", "OperationInProgressException"}
        )

    def wait_update(self, cfn, name, token, timeout=1500):
        return self.poll_update(cfn, name, token, self.artifacts, timeout)

    @staticmethod
    def poll_update(cfn, name, token, artifacts, timeout, *, reconcile=None):
        """Wait for this update, never a stale successful stack read."""
        terminal = {"UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE", "UPDATE_ROLLBACK_FAILED"}
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                response = cfn.describe_stack_events(StackName=name)
            except (ConnectionError, HTTPClientError):
                pass  # Retry a transient read without replaying the update.
            except ClientError as error:
                code = error.response["Error"]["Code"]
                status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
                if not (
                    code.startswith("Throttl") or code in {"RequestTimeout", "ServiceUnavailable"} or status >= 500
                ):
                    raise
            else:
                events = response.get("StackEvents", [])
                for event in events:
                    if (
                        event.get("ResourceType") == "AWS::CloudFormation::Stack"
                        and event.get("LogicalResourceId") == name
                        and event.get("ClientRequestToken") == token
                        and event.get("ResourceStatus") in terminal
                    ):
                        status = event["ResourceStatus"]
                        if status != "UPDATE_COMPLETE":
                            save(
                                artifacts / "stack-events.json",
                                [row for row in response["StackEvents"] if row.get("ClientRequestToken") == token],
                            )
                        return status
                if reconcile is not None and not any(event.get("ClientRequestToken") == token for event in events):
                    try:
                        matched = reconcile()
                    except (BotoCoreError, ClientError) as error:
                        if not retryable_read_error(error):
                            raise
                    else:
                        if matched:
                            return "UPDATE_COMPLETE"
            time.sleep(3)
        raise ProvisioningError(f"{name}: submitted update could not be reconciled; deployment fences retained")

    def update_functions(self, manifest, data, storage, targets, update_stack):
        manifest["owned"] = True
        save(self.artifacts / "manifest.json", manifest)
        storage.put_object(Bucket=manifest["bucket"], Key=manifest["codeKey"], Body=data)
        storage.put_object(
            Bucket=manifest["bucket"],
            Key=f"runs/{manifest['run']}/control/release-all",
            Body=b"hold",
        )
        spec = self.template(manifest)
        save(self.artifacts / "template.json", spec)
        stack = update_stack(spec)
        outputs = {item["OutputKey"]: item["OutputValue"] for item in stack.get("Outputs", [])}
        if outputs != targets:
            raise ProvisioningError("The persistent stack must expose both shared LMI functions")
        for output in stack["Outputs"]:
            key, arn = output["OutputKey"], output["OutputValue"]
            manifest["functions"][key] = arn
            save(self.artifacts / "manifest.json", manifest)
            config = self.client("lambda").get_function_configuration(FunctionName=arn)
            scaling = self.client("lambda").get_function_scaling_config(
                FunctionName=arn.rsplit(":", 1)[0], Qualifier=QUALIFIER
            )
            self.verify(config, scaling, manifest, key)
            save(
                self.artifacts / f"configuration/{key}.json",
                {
                    "function": {name: value for name, value in config.items() if name != "Environment"},
                    "scaling": scaling,
                },
            )
        save(
            self.artifacts / "function-name-map.json",
            {key: {self.runtime.handler: arn} for key, arn in manifest["functions"].items()},
        )

    def owned_stack(self, cfn, manifest):
        try:
            stack = cfn.describe_stacks(StackName=manifest["stack"])["Stacks"][0]
        except ClientError as error:
            if error.response["Error"]["Code"] == "ValidationError" and "does not exist" in str(error):
                return None
            raise
        tags = {t["Key"]: t["Value"] for t in stack.get("Tags", [])}
        if (
            tags.get("Suite") != self.owner
            or tags.get("Stack") != manifest["stack"]
            or tags.get("Persistent") != "true"
        ):
            raise ProvisioningError("Refusing to modify a stack not owned by this persistent suite")
        return stack

    def wait_for_idle_functions(self, stack, seconds=270):
        """Wait for prior runs without stopping them or updating active functions."""
        lam = self.client("lambda")
        deadline = time.monotonic() + seconds
        while True:
            running = []
            for output in stack.get("Outputs", []):
                if output["OutputKey"] not in CONCURRENCIES:
                    raise ProvisioningError("Unexpected function in persistent stack outputs")
                for page in lam.get_paginator("list_durable_executions_by_function").paginate(
                    FunctionName=output["OutputValue"].rsplit(":", 1)[0],
                    Statuses=list(OUTSTANDING_EXECUTION_STATUSES),
                ):
                    running.extend(page.get("DurableExecutions", []))
                    if page.get("NextMarker") and time.monotonic() >= deadline:
                        raise ProvisioningError("Could not finish checking previous executions; code was not updated")
            save(self.artifacts / "pre-deploy-executions.json", running)
            if not running:
                return
            if time.monotonic() >= deadline:
                raise ProvisioningError(
                    "Previous durable executions are still running; persistent code was not updated"
                )
            time.sleep(2)

    def recover_update(self, cfn, manifest):
        recover_deployment_update(
            manifest, self.artifacts, lambda service: cfn if service == "cloudformation" else self.client(service)
        )

    def cleanup(self, manifest):
        """Release only this run's work; retain functions, stack, bucket, and code."""
        errors = []
        try:
            recover_deployment_update(manifest, self.artifacts, self.client)
        except Exception as error:
            errors.append(f"Deployment update reconciliation: {error}")
        if not manifest.get("owned"):
            if errors:
                raise CollectionError("; ".join(errors))
            return
        cfn = self.client("cloudformation")
        if self.owned_stack(cfn, manifest) is None:
            if errors:
                raise CollectionError("; ".join(errors))
            return
        resources = cfn.describe_stack_resources(StackName=manifest["stack"])["StackResources"]
        if any(
            r["ResourceType"] == "AWS::S3::Bucket"
            and r.get("PhysicalResourceId") == manifest["bucket"]
            and r["ResourceStatus"] != "DELETE_COMPLETE"
            for r in resources
        ):
            try:
                self.client("s3").put_object(
                    Bucket=manifest["bucket"],
                    Key=f"runs/{manifest['run']}/control/release-all",
                    Body=b"release",
                )
            except Exception as error:
                errors.append(f"Run control release: {error}")
            from .cloud import Cloud

            driver = Cloud(
                manifest,
                artifacts_dir=self.artifacts,
                verify_profile=self.verify,
                client_factory=lambda service, _region: self.client(service),
            )
            driver.invocations, recovery_errors = driver.run_invocations()
            errors.extend(recovery_errors)
            try:
                driver.settle_case()
            except Exception as error:
                errors.append(str(error))
        save(
            self.artifacts / "cleanup.json",
            {
                "stack": manifest["stack"],
                "run": manifest["run"],
                "status": "FAILED" if errors else "RETAINED",
                "errors": errors,
                "message": "Run work released; persistent functions, bucket, and code retained",
            },
        )
        if errors:
            raise CollectionError("; ".join(errors))


def load_cleanup_manifest(artifacts: Path, expected_run: str, *, active_manifest: Path | None = None) -> dict[str, Any]:
    """Resolve the requested deployment from its journal before the active manifest."""
    journal = artifacts / "deployment-update.json"
    if journal.is_file():
        record = json.loads(journal.read_text())
        if record.get("run") == expected_run:
            recovery = record.get("recoveryManifest")
            if (
                not isinstance(recovery, dict)
                or recovery.get("run") != expected_run
                or (
                    recovery.get("deploymentArtifactDirectory") != str(artifacts.resolve())
                    and not _relocatable_settled_update(record)
                )
                or any(recovery.get(key) != record.get(key) for key in ("stack", "bucket", "commit"))
            ):
                raise ProvisioningError("Requested deployment recovery journal has an inconsistent identity")
            return {**recovery, "deploymentArtifactDirectory": str(artifacts.resolve())}
    manifest = json.loads((active_manifest or artifacts / "manifest.json").read_text())
    if manifest.get("run") != expected_run:
        raise ProvisioningError("No cleanup manifest matches the requested run")
    return {**manifest, "deploymentArtifactDirectory": str(artifacts.resolve())}


def _relocatable_settled_update(record):
    states = record.get("fenceStates")
    return (
        record.get("phase") == "SETTLED"
        and isinstance(states, dict)
        and set(states) == set(record.get("fences", {}))
        and all(state in {"PREPARED", "REJECTED", "ACQUIRED"} for state in states.values())
    )


def recover_deployment_update(manifest, artifacts: Path, client_factory) -> str | None:
    """Resolve a durable update journal without guessing whether AWS received it."""
    journal = artifacts / "deployment-update.json"
    if not journal.is_file():
        return None
    # Recovery, not a new write: a SETTLED (or relocatable-settled) journal needs
    # only the flock. same_owner_lock's falsy check refuses an uncertain record
    # when no boot identity could be established here.
    with owner_lock(journal, require_boot_identity=False) as owner:
        if not journal.is_file():
            return None
        return _recover_deployment_update(manifest, artifacts, client_factory, journal, owner)


def _recover_deployment_update(manifest, artifacts: Path, client_factory, journal: Path, owner) -> str:
    record = json.loads(journal.read_text())
    if any(record.get(key) != manifest.get(key) for key in ("stack", "bucket", "run", "commit")):
        raise ProvisioningError("Deployment update journal belongs to another run")
    recovery = record.get("recoveryManifest")
    if (
        isinstance(recovery, dict)
        and recovery.get("deploymentArtifactDirectory") != str(artifacts.resolve())
        and not _relocatable_settled_update(record)
    ):
        raise ProvisioningError("Deployment update journal belongs to another artifact directory")
    fences = record["fences"]
    if set(fences) != set(DeploymentManager.function_targets(manifest).values()):
        raise ProvisioningError("Deployment update journal has unexpected targets")
    states = record.get("fenceStates")
    if states is None and record.get("phase") == "PREPARED":
        raise ProvisioningError("Pre-submission journal does not identify which fence acquisitions settled")
    if states is not None and (not isinstance(states, dict) or set(states) != set(fences)):
        raise ProvisioningError("Deployment update journal has invalid fence acquisition states")
    settled = record.get("phase") == "SETTLED" or record.get("settled") is True
    uncertain = not settled or (isinstance(states, dict) and "ACQUIRING" in states.values())
    if uncertain and not same_owner_lock(record, owner):
        raise ProvisioningError("Deployment journal lacks its original local owner lock; fences retained")
    storage = client_factory("s3")
    if record.get("phase") not in {"PREPARED", "SETTLED"} and record.get("settled") is not True:
        cfn = client_factory("cloudformation")
        token = record["requestToken"]
        request = record.get("request")
        reconcile = None
        if isinstance(request, dict):
            if request.get("StackName") != manifest["stack"] or request.get("ClientRequestToken") != token:
                raise ProvisioningError("Deployment update journal has an inconsistent update request")

            def reconcile():
                for target, owner in fences.items():
                    current = read_fence(storage, manifest["bucket"], target)
                    if current is None or current.get("token") != owner:
                        raise ProvisioningError("Cannot replay a deployment update without its fences")
                # Repeating the exact token is idempotent. A missing event or
                # a rejection alone does not settle an earlier submission.
                try:
                    cfn.update_stack(**request)
                except (BotoCoreError, ClientError) as error:
                    if isinstance(error, ClientError) and (
                        error.response["Error"]["Code"] == "ValidationError"
                        and "No updates are to be performed" in error.response["Error"].get("Message", "")
                    ):
                        return _verified_recovery_noop(cfn, manifest, artifacts, client_factory)
                return False

        submitted = record.get("submittedAt")
        remaining = max(30, 1500 - (time.time() - submitted)) if isinstance(submitted, (int, float)) else 30
        status = DeploymentManager.poll_update(cfn, manifest["stack"], token, artifacts, remaining, reconcile=reconcile)
        if status == "UPDATE_ROLLBACK_FAILED":
            raise ProvisioningError("Deployment rollback failed; deployment fences retained")
    save(journal, {**record, "phase": "SETTLED"})
    DeploymentManager.release_deployment_fences(
        storage,
        manifest,
        fences,
        states,
        single_attempt=record.get("singleAttemptFences") is True,
        persist=lambda: save(journal, {**record, "phase": "SETTLED"}),
    )
    journal.unlink(missing_ok=True)
    return record["run"]


def _verified_recovery_noop(cfn, manifest, artifacts, client_factory):
    manager = DeploymentManager(
        runtime=RuntimeDefinition.from_manifest(manifest),
        owner=manifest["suiteOwner"],
        artifacts=artifacts,
        bundle=artifacts,
        client_factory=client_factory,
    )
    stack = manager.owned_stack(cfn, manifest)
    if stack is None or stack["StackStatus"] not in {"CREATE_COMPLETE", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE"}:
        return False
    targets = manager.function_targets(manifest)
    outputs = {row["OutputKey"]: row["OutputValue"] for row in stack.get("Outputs", [])}
    if outputs != targets:
        raise ProvisioningError("No-op update did not confirm the requested function targets")
    manager.wait_for_idle_functions(stack, seconds=0)
    lam = client_factory("lambda")
    for key, arn in targets.items():
        config = lam.get_function_configuration(FunctionName=arn)
        scaling = lam.get_function_scaling_config(FunctionName=arn.rsplit(":", 1)[0], Qualifier=QUALIFIER)
        manager.verify(config, scaling, manifest, key)
    return True
