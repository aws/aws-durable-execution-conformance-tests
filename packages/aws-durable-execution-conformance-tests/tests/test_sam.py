# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the SAM cleanup helper."""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

from aws_durable_execution_conformance_tests import sam
from aws_durable_execution_conformance_tests.sam import Deployer, delete_stack
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_delete_stack_returns_true_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, object] = {}

    class _FakeClient:
        def delete_stack(self, **kwargs: object) -> None:
            calls.update(kwargs)

    def _fake_boto3_client(service: str, region_name: str | None = None) -> _FakeClient:
        calls["service"] = service
        calls["region_name"] = region_name
        return _FakeClient()

    monkeypatch.setattr(sam.boto3, "client", _fake_boto3_client)

    assert delete_stack("my-stack", "us-west-2") is True
    assert calls["service"] == "cloudformation"
    assert calls["region_name"] == "us-west-2"
    assert calls["StackName"] == "my-stack"


def test_delete_stack_is_best_effort_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeClient:
        def delete_stack(self, **_kwargs: object) -> None:
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "no"}}, "DeleteStack")

    monkeypatch.setattr(sam.boto3, "client", lambda *_a, **_k: _FakeClient())

    # Never raises; returns False so the caller can note the best-effort miss.
    assert delete_stack("my-stack", "us-west-2") is False


def test_deploy_passes_but_redacts_secret_parameters(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    template = tmp_path / "template.yaml"
    template.write_text("Resources: {}\n", encoding="utf-8")
    commands: list[list[str]] = []

    def _run(command: list[str]) -> subprocess.CompletedProcess:
        commands.append(command)
        output = "deployed header-secret" if "deploy" in command else "built"
        return subprocess.CompletedProcess(command, 0, stdout=output, stderr="")

    monkeypatch.setattr(sam.SamExecutor, "run", _run)
    deployer = Deployer(str(template), build_dir=str(tmp_path / "build"))
    deployer.build()

    result = deployer.deploy(
        "stack",
        secret_parameter_overrides={"OtelExporterHeaders": "header-secret"},
    )

    assert "header-secret" in " ".join(commands[-1])
    assert "header-secret" not in result.command
    assert "header-secret" not in result.output
    assert "[REDACTED]" in result.command
    assert "[REDACTED]" in result.output
