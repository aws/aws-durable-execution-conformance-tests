# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Deployment and manifest APIs accept every supported runtime model."""

from copy import deepcopy

import pytest

from aws_durable_execution_conformance_tests.lmi.configuration import configuration_identity
from aws_durable_execution_conformance_tests.lmi.deployment import DeploymentManager
from aws_durable_execution_conformance_tests.lmi.errors import ProvisioningError
from aws_durable_execution_conformance_tests.lmi.runner import make_manifest
from aws_durable_execution_conformance_tests.lmi.runtime import FixtureBinding, RuntimeDefinition

from .test_shared_adapter import deployment


@pytest.mark.parametrize(
    "name,handler,native",
    [
        ("python", "fixture.handler", "python3.14"),
        ("java", "example.Fixture::handleRequest", "java25"),
        ("javascript", "index.handler", "nodejs24.x"),
    ],
)
def test_all_runtimes_supply_their_own_artifact_and_configuration(tmp_path, name, handler, native):
    binding = FixtureBinding(
        fixtures={
            "native-overlap": "barrier",
            "normal-return": "success",
            "deadline-during-step": "deadline",
            "peer-recovery": "deadline",
            "suspension-retry": "replay",
            "warm-reuse": "warm-owner",
            "normal-failure": "failure",
        },
        failure_error_type="ValueError",
        worker_identity="process",
        recovery_policy="replace-worker",
    )
    runtime = RuntimeDefinition(name, handler, architecture="x86_64", memory_size=3072, binding=binding)
    manifest = {
        **deployment(),
        **runtime.to_manifest(),
        "runtime": native,
        "stack": "test-stack",
        "codeKey": "code/digest.zip",
    }
    manifest["functions"]["c2"] = manifest["functions"]["c1"].replace("function:lmi:", "function:lmi-c2:")
    manifest["concurrencies"]["c2"] = 2
    manager = DeploymentManager(runtime=runtime, owner="sdk-test", artifacts=tmp_path, bundle=tmp_path / "bundle.zip")
    config = manager.template(manifest)["Resources"]["c1Function"]["Properties"]
    assert config["Handler"] == handler
    assert config["Architectures"] == ["x86_64"]
    assert config["MemorySize"] == 3072
    config.update(State="Active", Version="$LATEST.PUBLISHED", CodeSha256=manifest["codeSha256"])
    scaling = {"AppliedFunctionScalingConfig": config["FunctionScalingConfig"]}
    manager.verify(config, scaling, manifest, "c1")
    altered = deepcopy(config)
    altered["Handler"] = "foreign.handler"
    with pytest.raises(ProvisioningError):
        manager.verify(altered, scaling, manifest, "c1")
    plan = make_manifest(
        manifest,
        "standard",
        tmp_path,
        artifact_id="sha256:tested-artifact",
        conformance_revision="revision",
    )
    assert plan["runtime"] == name
    assert {profile["recovery_policy"] for profile in plan["profiles"]} == {"replace-worker"}
    assert {case["requirement_id"] for case in plan["scenarios"]} == {f"lmi-lifecycle-{i}" for i in range(1, 8)}
    assert all(profile["artifact_id"] == "sha256:tested-artifact" for profile in plan["profiles"])
    assert configuration_identity(manifest, "c1") != configuration_identity(
        {**manifest, "handler": "other.handler"}, "c1"
    )
