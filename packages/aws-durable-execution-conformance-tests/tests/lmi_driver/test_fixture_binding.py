# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Data-only fixture binding: parsing, identity coverage, and derived rules.

Golden equivalence against the Python SDK's #931 shared binding is checked by
re-implementing its ``expected_outcome`` and ``replay_effects`` as fixed
reference copies here (never importing the downloaded reference files), and
comparing their output against the conformance-owned, binding-derived rules.
"""

import hashlib
import json
from copy import deepcopy
from types import SimpleNamespace
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
from aws_durable_execution_conformance_tests.lmi.source_evidence import (
    _binding_expected_outcome,
    _binding_replay_effect_policy,
    binding_evidence_builder,
    replay_evidence,
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
# Verifier and evidence are derived from the plan.
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


@pytest.mark.parametrize("worker_identity_field,expected_value", [("process", "p-1"), ("worker", "w-1")])
def test_binding_evidence_builder_wires_expected_outcome_worker_identity_and_replay_effects(
    monkeypatch, worker_identity_field, expected_value
):
    from aws_durable_execution_conformance_tests.lmi import source_evidence

    captured = {}

    def fake_build_evidence(context, cloud, **kwargs):
        captured.update(kwargs)
        return {}

    monkeypatch.setattr(source_evidence, "build_evidence", fake_build_evidence)
    builder = binding_evidence_builder(binding(worker_identity=worker_identity_field), "python")
    builder(object(), object(), phase="final")
    assert captured["runtime"] == "python"
    assert captured["phase"] == "final"
    assert captured["worker_identity_key"] == worker_identity_field
    assert captured["expected_outcome"]({"role": "victim", "marker": "m"}, "normal-failure") == {
        "status": "FAILED",
        "error": {"type": "ValueError", "message": "expected:m"},
    }
    assert captured["worker_identity"]({"process": "p-1", "worker": "w-1"}) == expected_value
    assert captured["expected_replay_effects"] == {"success": 1, "failure": 1, "retry": 2}


def test_build_evidence_passes_the_worker_identity_key_to_case_sources():
    from unittest.mock import patch

    from aws_durable_execution_conformance_tests.lmi.source_evidence import build_evidence

    from .test_shared_partial_evidence import context

    cloud: Any = SimpleNamespace(
        manifest={"invocationTimeout": 60, "bucket": "private"},
        observation_ended_at=2,
        events={},
        raw_histories={},
        control_records=[],
        invocations=[],
        source_uris=set(),
        _save=lambda *args: None,
    )
    captured = {}

    def fake_capture(ctx, cld, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            observations=lambda **_: ([], []),
            histories=lambda **_: ([], {}, []),
            attempts=lambda _, **kwargs: ([], {}, []),
        )

    with patch(
        "aws_durable_execution_conformance_tests.lmi.source_evidence.CaseSources.capture", side_effect=fake_capture
    ):
        build_evidence(
            context(),
            cloud,
            runtime="python",
            expected_outcome=lambda item, scenario: {"status": "SUCCEEDED"},
            worker_identity=lambda raw: raw["worker"],
            worker_identity_key="worker",
        )
    assert captured["worker_identity_key"] == "worker"


# ---------------------------------------------------------------------------
# Golden equivalence with Python SDK PR #931's hard-coded expected_outcome and
# replay_effects (re-implemented here as fixed reference copies; never
# imported from the downloaded reference files).
# ---------------------------------------------------------------------------

_PR931_FIXTURES = {
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
_PR931_BINDING = FixtureBinding(
    fixtures=_PR931_FIXTURES,
    failure_error_type="ValueError",
    worker_identity="process",
    replay_effects=(
        ReplayEffect(operation="success", count=1),
        ReplayEffect(operation="failure", count=1),
        ReplayEffect(operation="retry", count=2),
        ReplayEffect(operation="item", count=1, values=(1, 2, 3)),
        ReplayEffect(operation="left", count=1),
        ReplayEffect(operation="right", count=1),
    ),
)


def _reference_expected_outcome(item, scenario):
    """Fixed copy of Python SDK #931's ``shared_evidence.expected_outcome``."""
    fixture = item.get("payload", {}).get("scenario")
    if fixture == "failure":
        return {
            "status": "FAILED",
            "error": {
                "type": "ValueError",
                "message": "expected:" + item["marker"],
            },
        }
    elif fixture == "deadline":
        return {
            "allowedStatuses": [
                "RUNNING",
                "SUCCEEDED",
                "FAILED",
                "TIMED_OUT",
                "STOPPED",
            ]
        }
    else:
        return {"status": "SUCCEEDED", "result": item["marker"]}


def _reference_replay_effects(bodies: list[dict[str, Any]]) -> tuple[dict[str, int], list[str]]:
    """Fixed copy of Python SDK #931's ``shared_evidence.replay_effects``."""
    owners: dict[tuple[str, int | None], str] = {}
    allowances: dict[str, int] = {}
    expected = {
        (name, item): count
        for name, count in {"success": 1, "failure": 1, "retry": 2, "item": 1, "left": 1, "right": 1}.items()
        for item in ((1, 2, 3) if name == "item" else (None,))
    }
    for body in bodies:
        name, identity = body["operation"], body["operationId"]
        item = body.get("value") if name == "item" else None
        key = (name, item)
        if (name == "item" and type(item) is not int) or key not in expected:
            allowances.setdefault(identity, 0)
            continue
        owner = owners.setdefault(key, identity)
        allowances.setdefault(identity, expected[key] if identity == owner else 0)
    missing = [f"item:{item}" if name == "item" else name for name, item in expected if (name, item) not in owners]
    return allowances, missing


# Every catalog scenario/role launch with #931's fixtures map: (catalog
# scenario, role, fixture name passed to Cloud.start for that role).
_PR931_LAUNCHES = [
    ("native-overlap", "victim", _PR931_FIXTURES["native-overlap"]),
    ("native-overlap", "peer", _PR931_FIXTURES["native-overlap"]),
    ("normal-return", "victim", _PR931_FIXTURES["normal-return"]),
    ("deadline-during-step", "victim", _PR931_FIXTURES["deadline-during-step"]),
    ("peer-recovery", "victim", _PR931_FIXTURES["peer-recovery"]),
    ("peer-recovery", "peer", _PR931_FIXTURES["peer"]),
    ("peer-recovery", "recovery", _PR931_FIXTURES["recovery"]),
    ("suspension-retry", "victim", _PR931_FIXTURES["suspension-retry"]),
    ("warm-reuse", "victim", _PR931_FIXTURES["warm-reuse"]),
    ("warm-reuse", "warm", _PR931_FIXTURES["warm"]),
    ("normal-failure", "victim", _PR931_FIXTURES["normal-failure"]),
]


@pytest.mark.parametrize("scenario,role,fixture_name", _PR931_LAUNCHES)
def test_derived_expected_outcome_matches_pr931_for_every_catalog_launch(scenario, role, fixture_name):
    item = {"role": role, "marker": "marker-value", "payload": {"scenario": fixture_name}}
    derived = _binding_expected_outcome(_PR931_BINDING, item, scenario)
    reference = _reference_expected_outcome(item, scenario)
    assert derived == reference


def test_derived_replay_policy_matches_pr931_on_owner_non_owner_and_missing_bodies():
    bodies: list[dict[str, Any]] = [
        {"operation": "success", "operationId": "s1"},
        {"operation": "success", "operationId": "s2"},  # non-owner: same key, later identity
        {"operation": "failure", "operationId": "f1"},
        {"operation": "unexpected", "operationId": "u1"},  # unexpected operation
        {"operation": "item", "operationId": "i1", "value": 1},
        {"operation": "item", "operationId": "i1b", "value": 1},  # non-owner for value 1
        {"operation": "item", "operationId": "i2", "value": 2},
        {"operation": "item", "operationId": "ibad", "value": "not-an-int"},  # non-int item value
        # "retry", "left", "right" and item value 3 are intentionally absent -> missing
    ]
    reference_allowances, reference_missing = _reference_replay_effects(bodies)
    derived_allowances, derived_missing = _binding_replay_effect_policy(_PR931_BINDING)(bodies)
    assert derived_allowances == reference_allowances
    assert derived_missing == reference_missing
    assert reference_missing == ["retry", "item:3", "left", "right"]


def _conflicting_values_binding() -> FixtureBinding:
    """One operation, one value key, expecting count 2 for that key."""
    return binding(replay_effects=(ReplayEffect(operation="item", count=2, values=(1,)),))


@pytest.mark.parametrize("second", [2, True, 1.5])
def test_binding_replay_effect_policy_zeroes_an_id_that_reports_a_different_value(second: Any):
    # The same operation ID cannot own two different (or unexpected) keys: a
    # later body with a different value must not be mistaken for a repeat of
    # the first, so its allowance drops to 0 instead of silently keeping the
    # first owned count. True compares equal to 1 in Python but is not the
    # integer the binding declares, and a fractional value is not an integer
    # at all; neither normalizes to the key the first body owns.
    bodies = [
        {"operation": "item", "operationId": "x", "value": 1},
        {"operation": "item", "operationId": "x", "value": second},
    ]
    allowances, missing = _binding_replay_effect_policy(_conflicting_values_binding())(bodies)
    assert allowances == {"x": 0}
    assert missing == []


def test_binding_replay_effect_policy_keeps_an_id_that_repeats_its_value():
    # The same key observed twice for the same ID is the valid case the fix
    # must not regress: the ID keeps owning its expected count.
    bodies = [
        {"operation": "item", "operationId": "x", "value": 1},
        {"operation": "item", "operationId": "x", "value": 1},
    ]
    allowances, missing = _binding_replay_effect_policy(_conflicting_values_binding())(bodies)
    assert allowances == {"x": 2}
    assert missing == []


@pytest.mark.parametrize("first", [1.0, 1e0])
def test_binding_replay_effect_policy_accepts_an_equivalent_integral_float_value(first: Any):
    # An integral JSON float (1.0, 1e0) is the same wire value as the int 1,
    # independently of which spelling a handler's JSON serializer emits
    # (Jackson doubles, for example, emit 1.0). It must own the same key as
    # a body reporting the int, not zero the allowance or report it missing.
    bodies = [
        {"operation": "item", "operationId": "x", "value": first},
        {"operation": "item", "operationId": "x", "value": first},
    ]
    allowances, missing = _binding_replay_effect_policy(_conflicting_values_binding())(bodies)
    assert allowances == {"x": 2}
    assert missing == []


def test_binding_replay_effect_policy_treats_an_integral_float_and_its_int_as_the_same_owner():
    # A mix of spellings for the same logical value must still count as one
    # repeated key for the same ID, not a conflicting later key.
    bodies = [
        {"operation": "item", "operationId": "x", "value": 1},
        {"operation": "item", "operationId": "x", "value": 1.0},
    ]
    allowances, missing = _binding_replay_effect_policy(_conflicting_values_binding())(bodies)
    assert allowances == {"x": 2}
    assert missing == []


def test_conflicting_value_bodies_make_replay_evidence_report_a_side_effect_count_mismatch():
    """End-to-end: one ID observed with two different values for one key is a
    latent false pass without the fix (indistinguishable from a valid repeat).
    The fix makes ``replay_evidence`` emit a real count/expectedCount mismatch,
    which ``rules/replay.py`` reports as "side-effect count differs".
    """
    policy = _binding_replay_effect_policy(_conflicting_values_binding())
    events: list[dict[str, Any]] = [
        {"kind": "invocation_start", "requestId": "first", "executionArn": "execution"},
        {"kind": "body", "requestId": "first", "operationId": "x", "operation": "item", "attempt": 1, "value": 1},
        {"kind": "body", "requestId": "first", "operationId": "x", "operation": "item", "attempt": 2, "value": 2},
    ]
    histories = [{"executionArn": "execution", "operations": [{"id": "x", "name": "item"}]}]
    replay = replay_evidence(
        events,
        histories,
        {"victim": "first"},
        expected_effects={"item": 2},
        effect_policy=policy,
    )
    row = next(item for item in replay["sideEffects"] if item["operationId"] == "x")
    assert (row["count"], row["expectedCount"]) == (2, 0)
    # This is exactly the condition rules/replay.py flags as an ASSERTION
    # finding ("side-effect count differs from the fixture's retry policy").
    assert row["count"] != row["expectedCount"]


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
