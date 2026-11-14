"""Deployment, placement and manifest APIs accept every supported runtime model."""

from copy import deepcopy
from functools import partial
from typing import cast

import pytest

from aws_durable_execution_conformance_tests.lmi.cloud import Cloud
from aws_durable_execution_conformance_tests.lmi.configuration import configuration_identity
from aws_durable_execution_conformance_tests.lmi.deployment import DeploymentManager
from aws_durable_execution_conformance_tests.lmi.errors import PlacementError, ProvisioningError
from aws_durable_execution_conformance_tests.lmi.observations import overlap
from aws_durable_execution_conformance_tests.lmi.runner import make_manifest
from aws_durable_execution_conformance_tests.lmi.runtime import RuntimeDefinition

from .test_evidence import alive, event
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
    runtime = RuntimeDefinition(name, handler, architecture="x86_64", memory_size=3072)
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
        runtime=name,
        artifact_id="sha256:tested-artifact",
        conformance_revision="revision",
        configuration_identity=partial(configuration_identity, runtime=runtime),
    )
    assert plan["runtime"] == name
    assert {case["requirement_id"] for case in plan["scenarios"]} == {f"lmi-lifecycle-{i}" for i in range(1, 8)}
    assert all(profile["artifact_id"] == "sha256:tested-artifact" for profile in plan["profiles"])
    assert configuration_identity(manifest, "c1", runtime) != configuration_identity(
        manifest, "c1", RuntimeDefinition(name, "other.handler")
    )


@pytest.mark.parametrize("runtime", ["java", "javascript"])
def test_same_process_runtime_overlap_requires_distinct_requests_and_shared_worker(runtime):
    first = event("BLOCKED", worker="worker")
    second = event("BLOCKED", 2, marker="b", request="r2", execution="exec2", worker="worker")
    assert overlap(alive([first, second]), {"a", "b"}, 2, runtime=runtime) == "env1"
    with pytest.raises(PlacementError):
        overlap(alive([first, {**second, "process": "other"}]), {"a", "b"}, 2, runtime=runtime)
    if runtime == "javascript":
        with pytest.raises(PlacementError):
            overlap(alive([first, {**second, "worker": "other"}]), {"a", "b"}, 2, runtime=runtime)


def test_fixture_routes_are_sdk_defined():
    from aws_durable_execution_conformance_tests.lmi.scenarios import ScenarioDriver

    from .test_shared_adapter import ScriptCloud, context

    cloud = ScriptCloud(2)
    driver = ScenarioDriver(
        context(scenario="native-overlap", roles=("victim", "peer"), native=2),
        cast(Cloud, cloud),
        runtime="java",
        fixtures={"native-overlap": "nativeBarrier", "peer": "recoveryPeer"},
    )
    driver.start()
    assert [action[1] for action in cloud.actions if action[0] == "start"] == ["nativeBarrier", "nativeBarrier"]


def test_fault_identity_does_not_depend_on_a_python_fixture_name(tmp_path):
    from types import SimpleNamespace
    from unittest.mock import patch

    from .support import build_evidence
    from .test_shared_partial_evidence import context

    cloud = SimpleNamespace(
        manifest={"invocationTimeout": 60},
        retired_at=None,
        observation_ended_at=2,
        source_uris=set(),
        control_records=[{"action": "hold", "gate": "fault"}, {"action": "hold", "gate": "peer"}],
        invocations=[{"role": "victim", "payload": {"scenario": "javaTimeoutFixture", "gate": "fault"}}],
        _save=lambda *args: None,
        artifacts_dir=tmp_path,
    )
    sources = SimpleNamespace(
        observations=lambda: ([], []), histories=lambda _: ([], {}, []), attempts=lambda _: ([], {}, [])
    )
    with patch("aws_durable_execution_conformance_tests.lmi.source_evidence.CaseSources.capture", return_value=sources):
        evidence = build_evidence(context(), cloud)
    assert evidence["faultsCreated"] == [{"id": "fault", "ownerCaseId": "case"}]
