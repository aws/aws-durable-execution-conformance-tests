# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""SAM CLI orchestration for build, deploy, and invocation.

Provides Deployer and Invoker classes that wrap SAM CLI commands for building,
deploying, and invoking Lambda functions via CloudFormation stacks.
"""

from __future__ import annotations

import json
import re
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from aws_durable_execution_conformance_tests import config

# region Cleanup


def delete_stack(stack_name: str, region: str | None = None) -> bool:
    """Best-effort, fire-and-forget CloudFormation stack deletion.

    Initiates deletion and returns immediately without waiting for the stack to
    reach ``DELETE_COMPLETE``. Never raises -- cleanup must not fail a run.

    Args:
        stack_name: CloudFormation stack name to delete.
        region: AWS region the stack lives in.

    Returns:
        True if the delete request was accepted, False if it could not be issued.
    """
    try:
        client = boto3.client("cloudformation", region_name=region)
        client.delete_stack(StackName=stack_name)
    except (BotoCoreError, ClientError):
        return False
    return True


# endregion


# region Exceptions


class SamCliError(Exception):
    """Raised when a SAM CLI command exits with a non-zero exit code."""

    def __init__(self, command: str, exit_code: int, stderr: str):
        super().__init__(f"SAM CLI command failed (exit code {exit_code}): {stderr}")
        self.command = command
        self.exit_code = exit_code
        self.stderr = stderr


class BuildRequiredError(Exception):
    """Raised when deploy is called without a prior successful build."""

    def __init__(self):
        super().__init__("A successful build is required before deployment")


class EventFileError(Exception):
    """Raised when an event file cannot be parsed."""

    def __init__(self, file_path: str, reason: str):
        super().__init__(f"Failed to parse event file '{file_path}': {reason}")
        self.file_path = file_path
        self.reason = reason


class InvokeError(Exception):
    """Raised when a direct Lambda invocation fails."""

    def __init__(self, function_name: str, reason: str):
        super().__init__(f"Failed to invoke function '{function_name}': {reason}")
        self.function_name = function_name
        self.reason = reason


# endregion


# region Models


@dataclass(frozen=True)
class BuildResult:
    """Result of a sam build operation."""

    success: bool
    command: str
    output: str

    def to_dict(self) -> dict:
        """Serialize to dictionary."""
        return {
            "success": self.success,
            "command": self.command,
            "output": self.output,
        }

    @classmethod
    def from_dict(cls, data: dict) -> BuildResult:
        """Deserialize from dictionary."""
        return cls(
            success=data["success"],
            command=data["command"],
            output=data["output"],
        )


@dataclass(frozen=True)
class DeployResult:
    """Result of a sam deploy operation."""

    success: bool
    command: str
    output: str
    stack_name: str

    def to_dict(self) -> dict:
        """Serialize to dictionary."""
        return {
            "success": self.success,
            "command": self.command,
            "output": self.output,
            "stack_name": self.stack_name,
        }

    @classmethod
    def from_dict(cls, data: dict) -> DeployResult:
        """Deserialize from dictionary."""
        return cls(
            success=data["success"],
            command=data["command"],
            output=data["output"],
            stack_name=data["stack_name"],
        )


@dataclass(frozen=True)
class InvocationResult:
    """Result of a sam invoke operation (remote or local)."""

    success: bool
    command: str
    output: str
    stderr: str
    function_name: str
    execution_id: str | None = None

    def to_dict(self) -> dict:
        """Serialize to dictionary."""
        return {
            "success": self.success,
            "command": self.command,
            "output": self.output,
            "stderr": self.stderr,
            "function_name": self.function_name,
            "execution_id": self.execution_id,
        }

    @classmethod
    def from_dict(cls, data: dict) -> InvocationResult:
        """Deserialize from dictionary."""
        return cls(
            success=data["success"],
            command=data["command"],
            output=data["output"],
            stderr=data["stderr"],
            function_name=data["function_name"],
            execution_id=data.get("execution_id"),
        )


@dataclass(frozen=True)
class LocalInvokeOutput:
    """Parsed fields from sam local invoke console output."""

    execution_id: str | None
    status: str | None
    result: str | None
    duration: str | None
    request_id: str | None


# endregion


# region Output parser

# Patterns based on the Execution Summary block printed by sam local invoke.
_EXECUTION_ID_RE = re.compile(r"ARN:\s+(\S+)")
_STATUS_RE = re.compile(r"Status:\s+(\S+)")
_RESULT_RE = re.compile(r"Result:\s+(.*)")
_DURATION_RE = re.compile(r"Duration:\s+(\S+)")
_REQUEST_ID_RE = re.compile(r"RequestId:\s+(\S+)")


def parse_local_invoke_output(output: str) -> LocalInvokeOutput:
    """Extract execution metadata from sam local invoke stdout/stderr.

    Args:
        output: Combined stdout + stderr from sam local invoke.

    Returns:
        LocalInvokeOutput with parsed fields (None if not found).
    """
    execution_id = _find(_EXECUTION_ID_RE, output)
    status = _find(_STATUS_RE, output)
    result = _find(_RESULT_RE, output)
    duration = _find(_DURATION_RE, output)
    request_id = _find(_REQUEST_ID_RE, output)

    return LocalInvokeOutput(
        execution_id=execution_id,
        status=status,
        result=result.strip() if result else None,
        duration=duration,
        request_id=request_id,
    )


def _find(pattern: re.Pattern, text: str) -> str | None:
    """Return the first capture group match or None."""
    m = pattern.search(text)
    return m.group(1) if m else None


# endregion


# region Event file reader


class EventFileReader:
    """Stateless utility for reading JSON event files and extracting payloads."""

    @staticmethod
    def read(file_path: str) -> dict | list:
        """Read and parse a JSON event file.

        Args:
            file_path: Path to the JSON event file.

        Returns:
            Parsed JSON content as a dict or list.

        Raises:
            FileNotFoundError: If file_path does not exist.
            EventFileError: If file contains invalid JSON.
        """
        try:
            with open(file_path) as f:
                return json.load(f)
        except FileNotFoundError:
            msg = f"Event file not found: {file_path}"
            raise FileNotFoundError(msg) from None
        except json.JSONDecodeError as e:
            raise EventFileError(file_path, str(e)) from e

    @staticmethod
    def extract_payload(event_data: dict | list) -> Any:
        """Extract the event payload from parsed event data.

        If event_data is a dict with an "Input" key, returns event_data["Input"].
        Otherwise returns the entire event_data.

        Args:
            event_data: Parsed JSON event data.

        Returns:
            The extracted event payload.
        """
        if isinstance(event_data, dict) and "Input" in event_data:
            return event_data["Input"]
        return event_data


# endregion


# region Command builder


class CommandBuilder:
    """Constructs SAM CLI command argument lists. Pure static methods, no state."""

    @staticmethod
    def build_command(template_path: str, build_dir: str | None = None) -> list[str]:
        """Construct the argument list for sam build.

        Args:
            template_path: Path to the SAM template file.
            build_dir: Optional custom build output directory.

        Returns:
            List of command arguments suitable for subprocess invocation.
        """
        cmd = ["sam", "build", "--template-file", template_path]
        if build_dir is not None:
            cmd.extend(["--build-dir", build_dir])
        return cmd

    @staticmethod
    def deploy_command(
        template_path: str,
        stack_name: str,
        region: str | None = None,
        capabilities: list[str] | None = None,
        parameter_overrides: dict[str, str] | None = None,
        resolve_image_repos: bool = True,
        image_repository: str | None = None,
    ) -> list[str]:
        """Construct the argument list for sam deploy.

        Args:
            template_path: Path to the SAM template file.
            stack_name: CloudFormation stack name.
            region: Optional AWS region.
            capabilities: Optional list of IAM capabilities.
            parameter_overrides: Optional key-value pairs for template parameters.
            resolve_image_repos: Whether to include --resolve-image-repos.
                Set to False when using a pre-built ImageUri.
            image_repository: ECR repository URI to use with --image-repository.
                Used when resolve_image_repos is False but SAM still requires
                an image repo option for PackageType: Image functions.

        Returns:
            List of command arguments suitable for subprocess invocation.
        """
        cmd = [
            "sam",
            "deploy",
            "--template-file",
            template_path,
            "--stack-name",
            stack_name,
            "--resolve-s3",
            "--no-confirm-changeset",
        ]
        if resolve_image_repos:
            cmd.append("--resolve-image-repos")
        elif image_repository:
            cmd.extend(["--image-repository", image_repository])
        caps = capabilities if capabilities is not None else ["CAPABILITY_IAM"]
        cmd.extend(["--capabilities", *caps])
        if region is not None:
            cmd.extend(["--region", region])
        if parameter_overrides is not None:
            serialized = CommandBuilder.serialize_parameter_overrides(parameter_overrides)
            if serialized:
                cmd.extend(["--parameter-overrides", serialized])
        return cmd

    @staticmethod
    def local_invoke_command(
        function_name: str,
        event_file: str | None = None,
        template_file: str | None = None,
        env_vars: str | None = None,
        docker_network: str | None = None,
        region: str | None = None,
    ) -> list[str]:
        """Construct the argument list for sam local invoke.

        Args:
            function_name: Logical resource ID of the Lambda function.
            event_file: Optional path to the event JSON file.
            template_file: Optional path to the SAM template file.
            env_vars: Optional path to a JSON file with environment variable overrides.
            docker_network: Optional Docker network to connect the Lambda container to.
            region: Optional AWS region.

        Returns:
            List of command arguments suitable for subprocess invocation.
        """
        cmd = ["sam", "local", "invoke", function_name]
        if event_file is not None:
            cmd.extend(["--event", event_file])
        if template_file is not None:
            cmd.extend(["--template", template_file])
        if env_vars is not None:
            cmd.extend(["--env-vars", env_vars])
        if docker_network is not None:
            cmd.extend(["--docker-network", docker_network])
        if region is not None:
            cmd.extend(["--region", region])
        return cmd

    @staticmethod
    def serialize_parameter_overrides(overrides: dict[str, str]) -> str:
        """Convert a dict to 'Key1=Value1 Key2=Value2' format.

        Args:
            overrides: Dictionary of parameter key-value pairs.

        Returns:
            Serialized string in SAM CLI parameter overrides format.
        """
        return " ".join(f"{k}={v}" for k, v in overrides.items())


# endregion


# region Executor


class SamExecutor:
    """Executes SAM CLI commands and captures output."""

    @staticmethod
    def run(command: list[str]) -> subprocess.CompletedProcess:
        """Execute a SAM CLI command.

        Args:
            command: List of command arguments.

        Returns:
            CompletedProcess with stdout and stderr captured as strings.
        """
        return subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )


# endregion


# region Deployer


class Deployer:
    """Orchestrates SAM CLI commands for building and deploying Lambda functions.

    Args:
        template_path: Path to an existing SAM template.yaml file.
        build_dir: Optional custom build output directory.
        region: Optional AWS region for deployment.

    Raises:
        FileNotFoundError: If template_path does not exist.
    """

    def __init__(
        self,
        template_path: str,
        build_dir: str | None = None,
        region: str | None = None,
    ):
        template: Path = Path(template_path)
        if not template.is_file():
            msg = f"SAM template not found: {template_path}"
            raise FileNotFoundError(msg)
        self._template_path = template_path
        self._build_dir = build_dir if build_dir is not None else str(config.BUILD_DIR)
        self._region = region
        self._built = False

    def build(self) -> BuildResult:
        """Run sam build against the configured template.

        Returns:
            BuildResult with success status and command output.

        Raises:
            SamCliError: If sam build exits with a non-zero exit code.
        """
        cmd = CommandBuilder.build_command(self._template_path, self._build_dir)
        result = SamExecutor.run(cmd)
        command_str = " ".join(cmd)

        if result.returncode != 0:
            raise SamCliError(
                command=command_str,
                exit_code=result.returncode,
                stderr=result.stderr,
            )

        self._built = True

        # SAM places the built template at the root of the build directory.
        built_template: Path = Path(self._build_dir) / "template.yaml"
        if built_template.is_file():
            self._deploy_template_path = str(built_template)
        else:
            self._deploy_template_path = self._template_path

        return BuildResult(success=True, command=command_str, output=result.stdout)

    def deploy(
        self,
        stack_name: str,
        capabilities: list[str] | None = None,
        parameter_overrides: dict[str, str] | None = None,
        secret_parameter_overrides: dict[str, str] | None = None,
        resolve_image_repos: bool = True,
        image_repository: str | None = None,
    ) -> DeployResult:
        """Run sam deploy for the given stack name.

        Args:
            stack_name: CloudFormation stack name.
            capabilities: Optional list of IAM capabilities.
            parameter_overrides: Optional key-value pairs for template parameters.
            secret_parameter_overrides: Optional parameters passed to SAM but
                redacted from returned commands, output, and errors.
            resolve_image_repos: Whether to include --resolve-image-repos.
                Set to False when using a pre-built ImageUri.
            image_repository: ECR repository URI to satisfy SAM's image repo
                requirement when resolve_image_repos is False.

        Returns:
            DeployResult with success status and command output.

        Raises:
            BuildRequiredError: If build() has not been called successfully.
            SamCliError: If sam deploy exits with a non-zero exit code.
        """
        if not self._built:
            raise BuildRequiredError

        all_parameter_overrides = {
            **(parameter_overrides or {}),
            **(secret_parameter_overrides or {}),
        }
        cmd = CommandBuilder.deploy_command(
            template_path=self._deploy_template_path,
            stack_name=stack_name,
            region=self._region,
            capabilities=capabilities,
            parameter_overrides=all_parameter_overrides or None,
            resolve_image_repos=resolve_image_repos,
            image_repository=image_repository,
        )
        result = SamExecutor.run(cmd)
        secret_values = tuple((secret_parameter_overrides or {}).values())
        command_str = _redact_text(" ".join(cmd), secret_values)
        stdout = _redact_text(result.stdout, secret_values)
        stderr = _redact_text(result.stderr, secret_values)

        if result.returncode != 0:
            # "No changes to deploy" is not a real failure — the stack is
            # already up to date, so treat it as a successful no-op.
            combined = f"{stdout}\n{stderr}"
            if "No changes to deploy" in combined:
                return DeployResult(
                    success=True,
                    command=command_str,
                    output=stdout or stderr,
                    stack_name=stack_name,
                )
            raise SamCliError(
                command=command_str,
                exit_code=result.returncode,
                stderr=stderr,
            )

        return DeployResult(
            success=True,
            command=command_str,
            output=stdout,
            stack_name=stack_name,
        )


def _redact_text(value: str, secrets: tuple[str, ...]) -> str:
    """Remove secret parameter values from SAM diagnostics."""

    result = value
    for secret in secrets:
        if secret:
            result = result.replace(secret, "[REDACTED]")
    return result


# endregion


# region Invoker


class Invoker:
    """Orchestrates reading event files, building commands, and executing invocations.

    Args:
        stack_name: CloudFormation stack name containing the Lambda function (for remote invoke).
        region: Optional AWS region.
        template_file: Optional path to SAM template file (for local invoke).
    """

    def __init__(
        self,
        stack_name: str,
        region: str | None = None,
        template_file: str | None = None,
        lambda_client: Any | None = None,
        cfn_client: Any | None = None,
    ):
        self._stack_name = stack_name
        self._region = region
        self._template_file = template_file
        boto_config = Config(retries={"mode": "adaptive", "max_attempts": 10})
        self._lambda_client = lambda_client or boto3.client("lambda", region_name=region, config=boto_config)
        self._cfn_client = cfn_client or boto3.client("cloudformation", region_name=region, config=boto_config)
        self._function_map: dict[str, str] | None = None

    @property
    def stack_name(self) -> str:
        """CloudFormation stack name containing the Lambda function."""
        return self._stack_name

    def _resolve_function(self, logical_id: str) -> str:
        """Resolve a logical resource ID to its physical Lambda function name.

        The whole stack is resolved with a single (paginated) CloudFormation
        call on first use and cached for the lifetime of the Invoker, avoiding
        a per-invocation control-plane lookup.

        Raises:
            InvokeError: If the stack cannot be listed or the logical ID is
                not a Lambda function in the stack.
        """
        if self._function_map is None:
            mapping: dict[str, str] = {}
            try:
                paginator = self._cfn_client.get_paginator("list_stack_resources")
                for page in paginator.paginate(StackName=self._stack_name):
                    for res in page["StackResourceSummaries"]:
                        if res["ResourceType"] == "AWS::Lambda::Function":
                            mapping[res["LogicalResourceId"]] = res["PhysicalResourceId"]
            except (BotoCoreError, ClientError) as e:
                raise InvokeError(logical_id, f"could not list stack resources: {e}") from e
            self._function_map = mapping
        physical = self._function_map.get(logical_id)
        if not physical:
            raise InvokeError(
                logical_id,
                f"no Lambda function with this logical ID in stack '{self._stack_name}'",
            )
        return physical

    def _invoke_boto3(
        self,
        function_name: str,
        event_file_path: str | None,
        invocation_type: str,
    ) -> InvocationResult:
        """Invoke a durable Lambda function directly via the Lambda API.

        Durable functions must be invoked with a qualified ARN, so the
        qualifier is always set explicitly. The output mirrors the JSON that
        ``sam remote invoke --output json`` prints: the full Invoke response
        with the payload read into a string, including the top-level
        ``DurableExecutionArn`` field consumed by the validator.
        """
        payload_bytes = b"{}"
        if event_file_path is not None:
            event_data = EventFileReader.read(event_file_path)
            payload = EventFileReader.extract_payload(event_data)
            payload_bytes = json.dumps(payload).encode()

        physical_name = self._resolve_function(function_name)
        try:
            response = self._lambda_client.invoke(
                FunctionName=physical_name,
                InvocationType=invocation_type,
                Qualifier="$LATEST",
                Payload=payload_bytes,
            )
            # Reading the StreamingBody is a network operation too: it can raise
            # streaming/timeout errors after a successful 200 response, and the
            # bytes may fail to decode. Keep it inside the wrapping so a single
            # bad invocation records one failed requirement instead of aborting
            # the whole suite run.
            payload_str = response["Payload"].read().decode() if "Payload" in response else ""
        except (BotoCoreError, ClientError, UnicodeDecodeError) as e:
            raise InvokeError(function_name, str(e)) from e

        execution_arn = response.get("DurableExecutionArn") or response.get("ResponseMetadata", {}).get(
            "HTTPHeaders", {}
        ).get("x-amz-durable-execution-arn")

        output: dict[str, Any] = {
            "StatusCode": response.get("StatusCode"),
            "Payload": payload_str,
        }
        if execution_arn:
            output["DurableExecutionArn"] = execution_arn
        if response.get("ExecutedVersion"):
            output["ExecutedVersion"] = response["ExecutedVersion"]
        if response.get("FunctionError"):
            output["FunctionError"] = response["FunctionError"]

        return InvocationResult(
            success=True,
            command=f"lambda.invoke {physical_name} (InvocationType={invocation_type})",
            output=json.dumps(output),
            stderr="",
            function_name=function_name,
        )

    def invoke(
        self,
        function_name: str,
        event_file_path: str | None = None,
        parameters: list[str] | None = None,
    ) -> InvocationResult:
        """Invoke a Lambda function with an optional event file.

        Resolves the logical resource ID to the physical function name via
        CloudFormation (cached per stack) and calls the Lambda Invoke API
        directly.

        Args:
            function_name: Logical resource ID of the Lambda function.
            event_file_path: Optional path to a JSON event file.
            parameters: Optional list of "Key=Value" overrides; only
                "InvocationType=<type>" is honored.

        Returns:
            InvocationResult with success status and the Invoke response as
            JSON output (including DurableExecutionArn).

        Raises:
            FileNotFoundError: If event file does not exist.
            EventFileError: If event file contains invalid JSON.
            InvokeError: If resolution or the Invoke API call fails.
        """
        invocation_type = "RequestResponse"
        for param in parameters or []:
            key, _, value = param.partition("=")
            if key == "InvocationType" and value:
                invocation_type = value
            else:
                raise InvokeError(
                    function_name,
                    f"unsupported invoke parameter '{param}'; only 'InvocationType=<type>' is supported",
                )
        return self._invoke_boto3(function_name, event_file_path, invocation_type)

    def invoke_async(
        self,
        function_name: str,
        event_file_path: str | None = None,
    ) -> InvocationResult:
        """Invoke a Lambda function asynchronously (InvocationType=Event).

        The function is triggered without waiting for completion. Durable
        functions still return the DurableExecutionArn for Event invocations,
        surfaced in the JSON output.

        Args:
            function_name: Logical resource ID of the Lambda function.
            event_file_path: Optional path to a JSON event file.

        Returns:
            InvocationResult with success status. The payload is empty for
            Event invocations; DurableExecutionArn is present in the output.

        Raises:
            FileNotFoundError: If event file does not exist.
            EventFileError: If event file contains invalid JSON.
            InvokeError: If resolution or the Invoke API call fails.
        """
        return self._invoke_boto3(function_name, event_file_path, "Event")

    def local_invoke(
        self,
        function_name: str,
        event_file_path: str | None = None,
        env_vars: str | None = None,
        docker_network: str | None = None,
    ) -> InvocationResult:
        """Invoke a Lambda function locally using sam local invoke.

        Reads the event file, extracts the payload, writes it to a temp file,
        builds the local invoke command, executes it, and returns the result.

        Args:
            function_name: Logical resource ID of the Lambda function.
            event_file_path: Optional path to a JSON event file.
            env_vars: Optional path to a JSON file with environment variable overrides.
            docker_network: Optional Docker network to connect the Lambda container to.

        Returns:
            InvocationResult with success status and command output.

        Raises:
            FileNotFoundError: If event file does not exist.
            EventFileError: If event file contains invalid JSON.
            SamCliError: If sam local invoke exits with non-zero code.
        """
        temp_file_path = None
        try:
            event_file_for_command = None

            if event_file_path is not None:
                event_data = EventFileReader.read(event_file_path)
                payload = EventFileReader.extract_payload(event_data)

                fd, temp_file_path = tempfile.mkstemp(suffix=".json")
                with open(fd, "w", closefd=True) as f:
                    json.dump(payload, f)

                event_file_for_command = temp_file_path

            cmd = CommandBuilder.local_invoke_command(
                function_name=function_name,
                event_file=event_file_for_command,
                template_file=self._template_file,
                env_vars=env_vars,
                docker_network=docker_network,
                region=self._region,
            )

            result = SamExecutor.run(cmd)
            command_str = " ".join(cmd)

            if result.returncode != 0:
                raise SamCliError(
                    command=command_str,
                    exit_code=result.returncode,
                    stderr=result.stderr,
                )

            combined_output = result.stdout + result.stderr
            parsed = parse_local_invoke_output(combined_output)

            return InvocationResult(
                success=True,
                command=command_str,
                output=result.stdout,
                stderr=result.stderr,
                function_name=function_name,
                execution_id=parsed.execution_id,
            )
        finally:
            if temp_file_path is not None:
                Path(temp_file_path).unlink(missing_ok=True)


# endregion
