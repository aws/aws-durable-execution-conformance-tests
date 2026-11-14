# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: Apache-2.0
"""Reusable persistent LMI stack deployment; runtime/build choices are caller-owned."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

from .artifacts import clear_run_artifacts, save
from .errors import ProvisioningError
from .runtime import CONCURRENCIES, QUALIFIER, SCALING, RuntimeDefinition
from .service import client


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
                                "ExecutionEnvironmentMemoryGiBPerVCpu": 2,
                            }
                        },
                        "Environment": {
                            "Variables": {
                                "LMI_BUCKET": bucket,
                                "LMI_COMMIT": manifest["commit"],
                                "LMI_RUN_ID": manifest["run"],
                                "AWS_RETRY_MODE": "standard",
                            }
                        },
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

    def verify(self, config, scaling, manifest, key):
        actual = config.get("CapacityProviderConfig", {}).get("LambdaManagedInstancesCapacityProviderConfig", {})
        checks = [
            actual.get("CapacityProviderArn") == manifest["provider"],
            actual.get("PerExecutionEnvironmentMaxConcurrency") == manifest["concurrencies"][key],
            config.get("Runtime") == manifest["runtime"],
            config.get("Role") == manifest["role"],
            config.get("Handler") == self.runtime.handler,
            not config.get("Layers", []),
            config.get("Environment", {}).get("Variables", {}).get("LMI_BUCKET") == manifest["bucket"],
            config.get("Architectures") == [self.runtime.architecture],
            config.get("Version") == QUALIFIER,
            config.get("CodeSha256") == manifest["codeSha256"],
            config.get("MemorySize") == self.runtime.memory_size,
            config.get("State") == "Active",
            config.get("DurableConfig", {}).get("ExecutionTimeout") == manifest["executionTimeout"],
            config.get("Timeout") == manifest["invocationTimeout"],
            scaling.get("AppliedFunctionScalingConfig") == SCALING,
            config.get("Environment", {}).get("Variables", {}).get("LMI_COMMIT") == manifest["commit"],
            config.get("Environment", {}).get("Variables", {}).get("LMI_RUN_ID") == manifest["run"],
        ]
        if not all(checks):
            raise ProvisioningError(f"{key}: LMI configuration/artifact/scaling readback mismatch")

    def deploy(self, args):
        clear_run_artifacts(self.artifacts)
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
        save(self.artifacts / "capacity-provider.json", capacity)
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
        save(self.artifacts / "manifest.json", manifest)
        self.deploy_stack(manifest, data)

    def deploy_stack(self, manifest, data):
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
            manifest["owned"] = True
            save(self.artifacts / "manifest.json", manifest)
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
        if stack is not None:
            self.wait_for_idle_functions(stack)
        manifest["owned"] = True
        save(self.artifacts / "manifest.json", manifest)
        self.client("s3").put_object(Bucket=manifest["bucket"], Key=manifest["codeKey"], Body=data)
        self.client("s3").put_object(
            Bucket=manifest["bucket"],
            Key=f"runs/{manifest['run']}/control/release-all",
            Body=b"hold",
        )
        spec = self.template(manifest)
        save(self.artifacts / "template.json", spec)
        try:
            cfn.update_stack(StackName=manifest["stack"], TemplateBody=json.dumps(spec), Tags=tags)
        except ClientError as error:
            if "No updates are to be performed" not in error.response["Error"].get("Message", ""):
                raise
            stack = self.owned_stack(cfn, manifest)
        else:
            stack = self.wait_stack(cfn, manifest["stack"], "UPDATE_COMPLETE")
        if {output["OutputKey"] for output in stack.get("Outputs", [])} != set(CONCURRENCIES):
            raise ProvisioningError("The persistent stack must expose both shared LMI functions")
        for output in stack["Outputs"]:
            key, arn = output["OutputKey"], output["OutputValue"]
            manifest["functions"][key] = arn
            save(self.artifacts / "manifest.json", manifest)
            config = self.client("lambda").get_function_configuration(FunctionName=arn)
            scaling = self.client("lambda").get_function_scaling_config(
                FunctionName=arn.rsplit(":", 1)[0], Qualifier=QUALIFIER
            )
            save(
                self.artifacts / f"configuration/{key}.json",
                {"function": config, "scaling": scaling},
            )
            self.verify(config, scaling, manifest, key)
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
                    Statuses=["RUNNING"],
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

    def cleanup(self, manifest):
        """Release only this run's work; retain functions, stack, bucket, and code."""
        if not manifest.get("owned"):
            return
        cfn = self.client("cloudformation")
        if self.owned_stack(cfn, manifest) is None:
            return
        resources = cfn.describe_stack_resources(StackName=manifest["stack"])["StackResources"]
        if any(
            r["ResourceType"] == "AWS::S3::Bucket"
            and r.get("PhysicalResourceId") == manifest["bucket"]
            and r["ResourceStatus"] != "DELETE_COMPLETE"
            for r in resources
        ):
            self.client("s3").put_object(
                Bucket=manifest["bucket"],
                Key=f"runs/{manifest['run']}/control/release-all",
                Body=b"release",
            )
            from .cloud import Cloud

            driver = Cloud(manifest, artifacts_dir=self.artifacts, verify_profile=self.verify)
            driver.invocations = driver.run_invocations()
            driver.settle_case()
        save(
            self.artifacts / "cleanup.json",
            {
                "stack": manifest["stack"],
                "run": manifest["run"],
                "status": "RETAINED",
                "message": "Run work released; persistent functions, bucket, and code retained",
            },
        )
