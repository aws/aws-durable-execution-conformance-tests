# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Data-only fixture binding: parsing, round-tripping, fail-closed validation and identity coverage."""

import hashlib
import json
from copy import deepcopy
from typing import Any
from unittest.mock import Mock

import pytest

from aws_durable_execution_conformance_tests.lmi.cloud import Cloud
from aws_durable_execution_conformance_tests.lmi.configuration import MEMORY_GIB_PER_VCPU, configuration_identity
from aws_durable_execution_conformance_tests.lmi.deployment import DeploymentManager
from aws_durable_execution_conformance_tests.lmi.errors import ProvisioningError
from aws_durable_execution_conformance_tests.lmi.runtime import (
    QUALIFIER,
    FixtureBinding,
    ReplayEffect,
    RuntimeDefinition,
)

from .support import RUNTIME
from .test_shared_adapter import deployment

FIXTURES = {
    "native-overlap": "barrier",
    "normal-return": "success",
    "deadline-during-step": "deadline",
    "peer-recovery": "deadline",
    "suspension-retry": "replay",
    "warm-reuse": "warm-owner",
    "normal-failure": "failure",
    "peer": "barrier",
    "recovery": "barrier",
    "warm": "warm-probe",
}


def binding(**overrides: Any) -> FixtureBinding:
    defaults: dict[str, Any] = {"fixtures": FIXTURES, "failure_error_type": "ValueError", "worker_identity": "process"}
    defaults.update(overrides)
    return FixtureBinding(**defaults)


# ---------------------------------------------------------------------------
# Parsing, round-tripping, and fail-closed validation.
# ---------------------------------------------------------------------------


def test_default_binding_round_trips_through_manifest():
    value = binding()
    manifest = value.to_manifest()
    assert manifest == {
        "bindingVersion": 1,
        "fixtures": FIXTURES,
        "failureErrorType": "ValueError",
        "workerIdentity": "process",
        "suspension": "callback",
        "callbackPayload": "marker",
        "recoveryPolicy": "reuse-worker",
        "replayEffects": [
            {"operation": "success", "count": 1},
            {"operation": "failure", "count": 1},
            {"operation": "retry", "count": 2},
        ],
    }
    assert FixtureBinding.from_manifest(manifest) == value


def test_non_default_fields_round_trip():
    value = binding(
        suspension="wait",
        callback_payload="json-marker",
        recovery_policy="replace-worker",
        replay_effects=(
            ReplayEffect(operation="success", count=1),
            ReplayEffect(operation="failure", count=1),
            ReplayEffect(operation="retry", count=2),
            ReplayEffect(operation="item", count=1, values=(1, 2, 3)),
            ReplayEffect(operation="left", count=1),
            ReplayEffect(operation="right", count=1),
        ),
    )
    manifest = value.to_manifest()
    assert manifest["replayEffects"][3] == {"operation": "item", "count": 1, "values": [1, 2, 3]}
    assert FixtureBinding.from_manifest(manifest) == value


def test_load_reads_a_checked_in_json_file(tmp_path):
    path = tmp_path / "binding.json"
    path.write_text(json.dumps(binding().to_manifest()))
    assert FixtureBinding.load(path) == binding()


@pytest.mark.parametrize(
    "mutate",
    [
        lambda m: m.update(unknownField="x"),
        lambda m: m.update(bindingVersion=2),
        lambda m: m.update(bindingVersion=True),
        lambda m: m.update(bindingVersion="1"),
        lambda m: m["fixtures"].update({"unknown-scenario": "x"}),
        lambda m: m["fixtures"].update({"peer": ""}),
        lambda m: m["fixtures"].update({"peer": 1}),
        lambda m: m.update(failureErrorType=""),
        lambda m: m.update(failureErrorType=1),
        lambda m: m.update(workerIdentity="thread"),
        lambda m: m.update(suspension="poll"),
        lambda m: m.update(callbackPayload="binary"),
        lambda m: m.update(recoveryPolicy="restart-worker"),
        lambda m: m.update(replayEffects=[]),
        lambda m: m.update(replayEffects="success"),
        lambda m: m["replayEffects"].append({"operation": "", "count": 1}),
        lambda m: m["replayEffects"].append({"operation": "extra", "count": 0}),
        lambda m: m["replayEffects"].append({"operation": "extra", "count": -1}),
        lambda m: m["replayEffects"].append({"operation": "extra", "count": True}),
        lambda m: m["replayEffects"].append({"operation": "extra", "count": 1, "values": [1, 1]}),
        lambda m: m["replayEffects"].append({"operation": "extra", "count": 1, "values": []}),
        lambda m: m["replayEffects"].append({"operation": "extra", "count": 1, "values": [True]}),
        lambda m: m["replayEffects"].append({"operation": "extra", "count": 1, "extraField": True}),
    ],
)
def test_invalid_bindings_are_rejected(mutate):
    manifest = binding().to_manifest()
    mutate(manifest)
    with pytest.raises(ValueError):
        FixtureBinding.from_manifest(manifest)


@pytest.mark.parametrize("missing", ["bindingVersion", "fixtures", "failureErrorType", "workerIdentity"])
def test_missing_required_fields_are_rejected(missing):
    manifest = binding().to_manifest()
    del manifest[missing]
    with pytest.raises(ValueError):
        FixtureBinding.from_manifest(manifest)


def test_an_empty_fixtures_map_is_allowed_but_a_non_mapping_is_rejected():
    # A missing key still yields NOT_IMPLEMENTED at run time as today; an SDK
    # may legitimately map no fixtures yet.
    FixtureBinding(fixtures={}, failure_error_type="E", worker_identity="process")
    with pytest.raises(ValueError):
        FixtureBinding(fixtures="not-a-mapping", failure_error_type="E", worker_identity="process")  # type: ignore[arg-type]


def test_runtime_definition_requires_a_valid_fixture_binding():
    with pytest.raises(TypeError):
        RuntimeDefinition("python", "fixture.handler")  # type: ignore[call-arg] # binding is required
    with pytest.raises(ValueError):
        RuntimeDefinition("python", "fixture.handler", binding="not-a-binding")  # type: ignore[arg-type]


def test_runtime_definition_round_trips_through_manifest():
    value = RuntimeDefinition("python", "fixture.handler", binding=binding())
    manifest = value.to_manifest()
    assert manifest["fixtureBinding"] == binding().to_manifest()
    assert RuntimeDefinition.from_manifest(manifest) == value


# ---------------------------------------------------------------------------
# Identity covers the binding.
# ---------------------------------------------------------------------------


def test_configuration_identity_changes_with_each_binding_field():
    base = deployment()
    base_identity = configuration_identity(base, "c1")

    def with_binding_change(**overrides):
        value = deepcopy(base)
        value["fixtureBinding"] = {**value["fixtureBinding"], **overrides}
        return value

    for overrides in (
        {"failureErrorType": "OtherError"},
        {"workerIdentity": "worker"},
        {"suspension": "wait"},
        {"callbackPayload": "json-marker"},
        {"recoveryPolicy": "replace-worker"},
        {"replayEffects": [{"operation": "success", "count": 5}]},
        {"fixtures": {**base["fixtureBinding"]["fixtures"], "peer": "other-fixture"}},
    ):
        assert configuration_identity(with_binding_change(**overrides), "c1") != base_identity


def test_derived_identity_equals_the_same_hash_built_from_an_explicit_runtime_and_binding():
    manifest = deployment()
    runtime = RuntimeDefinition.from_manifest(manifest)
    fields = (
        "runtime",
        "role",
        "bucket",
        "commit",
        "run",
        "provider",
        "codeSha256",
        "invocationTimeout",
        "executionTimeout",
    )
    expected_payload = {name: manifest[name] for name in fields}
    expected_payload.update(
        function=manifest["functions"]["c1"],
        nativeConcurrency=manifest["concurrencies"]["c1"],
        architecture=runtime.architecture,
        memorySize=runtime.memory_size,
        memoryGiBPerVCpu=MEMORY_GIB_PER_VCPU,
        handler=runtime.handler,
        scaling={"MinExecutionEnvironments": 1, "MaxExecutionEnvironments": 1},
        fixtureBinding=runtime.binding.to_manifest(),
    )
    expected = "sha256:" + hashlib.sha256(json.dumps(expected_payload, sort_keys=True).encode()).hexdigest()
    assert configuration_identity(manifest, "c1") == expected


# ---------------------------------------------------------------------------
# The profile verifier is derived from the plan.
# ---------------------------------------------------------------------------


def test_cloud_default_verify_profile_uses_the_shared_verifier(tmp_path):
    manager = DeploymentManager(runtime=RUNTIME, owner="owner", artifacts=tmp_path, bundle=tmp_path / "bundle.zip")
    manifest = {**deployment(), "stack": "stack", "codeKey": "code/digest.zip"}
    config = manager.template(manifest)["Resources"]["c1Function"]["Properties"]
    config.update(State="Active", Version=QUALIFIER, CodeSha256=manifest["codeSha256"])
    scaling = {"AppliedFunctionScalingConfig": config["FunctionScalingConfig"]}
    cloud = Cloud(manifest, "c1", artifacts_dir=tmp_path, client_factory=lambda *_: Mock())
    # No explicit verify_profile: the default must match DeploymentManager's.
    cloud.verify_profile(config, scaling, manifest, "c1")
    manager.verify(config, scaling, manifest, "c1")
    bad = {**config, "Handler": "wrong.handler"}
    with pytest.raises(ProvisioningError):
        cloud.verify_profile(bad, scaling, manifest, "c1")


@pytest.mark.parametrize(
    "field,value",
    [("workerIdentity", ["process"]), ("suspension", {"mode": "callback"}), ("callbackPayload", ["marker"])],
)
def test_binding_rejects_unhashable_enum_values_as_value_errors(field: str, value: Any) -> None:
    raw = {**binding().to_manifest(), field: value}
    with pytest.raises(ValueError):
        FixtureBinding.from_manifest(raw)


def test_binding_rejects_a_repeated_replay_effect_operation() -> None:
    raw = {
        **binding().to_manifest(),
        "replayEffects": [{"operation": "success", "count": 1}, {"operation": "success", "count": 2}],
    }
    with pytest.raises(ValueError, match="each operation once"):
        FixtureBinding.from_manifest(raw)
