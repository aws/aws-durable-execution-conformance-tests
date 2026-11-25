# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: Apache-2.0
"""Explicit runtime and data-only fixture binding for an SDK-owned LMI fixture.

``FixtureBinding`` is the SDK's declaration of how its deployed fixture maps onto
the shared catalog; it is pure data and carries no SDK code. Parsing fails closed:
unknown fields or values are rejected rather than ignored, so a drifted binding
cannot silently fall back to a default.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any

from .catalog import RUNTIMES

CONCURRENCIES = {"c1": 1, "c2": 2}
SCALING = {"MinExecutionEnvironments": 1, "MaxExecutionEnvironments": 1}
MEMORY_GIB_PER_VCPU = 2
QUALIFIER = "$LATEST.PUBLISHED"

# The seven catalog lifecycle scenarios, plus the three supporting fixture roles
# (a healthy peer, a recovery replacement, and a warm-reuse probe) that a binding
# may also map to an SDK fixture entry point.
CATALOG_SCENARIOS = (
    "native-overlap",
    "normal-return",
    "deadline-during-step",
    "peer-recovery",
    "suspension-retry",
    "warm-reuse",
    "normal-failure",
)
_FIXTURE_KEYS = frozenset({*CATALOG_SCENARIOS, "peer", "recovery", "warm"})
_WORKER_IDENTITIES = frozenset({"process", "worker"})
_SUSPENSION_MODES = frozenset({"callback", "wait"})
_CALLBACK_PAYLOADS = frozenset({"marker", "json-marker"})
_RECOVERY_POLICIES = frozenset({"reuse-worker", "replace-worker"})
_REPLAY_EFFECT_FIELDS = frozenset({"operation", "count", "values"})
_BINDING_FIELDS = frozenset(
    {
        "bindingVersion",
        "fixtures",
        "failureErrorType",
        "workerIdentity",
        "suspension",
        "callbackPayload",
        "recoveryPolicy",
        "replayEffects",
    }
)


def _non_empty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value)


def _one_of(value: Any, allowed: frozenset[str]) -> bool:
    # Check the type first: an unhashable JSON value must be a ValueError too.
    return isinstance(value, str) and value in allowed


def _default_replay_effects() -> tuple[ReplayEffect, ...]:
    return (
        ReplayEffect(operation="success", count=1),
        ReplayEffect(operation="failure", count=1),
        ReplayEffect(operation="retry", count=2),
    )


@dataclass(frozen=True)
class ReplayEffect:
    """One expected replay side-effect count for a durable operation name."""

    operation: str
    count: int
    values: tuple[int, ...] | None = None

    def __post_init__(self) -> None:
        if not _non_empty_str(self.operation):
            raise ValueError("fixtureBinding.replayEffects.operation must be a non-empty string")
        if type(self.count) is not int or self.count <= 0:
            raise ValueError("fixtureBinding.replayEffects.count must be a positive integer")
        if self.values is not None:
            values = tuple(self.values)
            if not values or any(type(value) is not int for value in values) or len(set(values)) != len(values):
                raise ValueError("fixtureBinding.replayEffects.values must be distinct integers")
            object.__setattr__(self, "values", values)

    def to_manifest(self) -> dict[str, Any]:
        result: dict[str, Any] = {"operation": self.operation, "count": self.count}
        if self.values is not None:
            result["values"] = list(self.values)
        return result

    @classmethod
    def from_manifest(cls, raw: Mapping[str, Any]) -> ReplayEffect:
        if not isinstance(raw, Mapping) or set(raw) - _REPLAY_EFFECT_FIELDS:
            raise ValueError("fixtureBinding.replayEffects entry has unexpected fields")
        if "operation" not in raw or "count" not in raw:
            raise ValueError("fixtureBinding.replayEffects entry is missing operation or count")
        values = raw.get("values")
        if values is not None and not isinstance(values, Sequence):
            raise ValueError("fixtureBinding.replayEffects.values must be an array")
        return cls(
            operation=raw["operation"],
            count=raw["count"],
            values=tuple(values) if values is not None else None,
        )


@dataclass(frozen=True)
class FixtureBinding:
    """The SDK's data-only description of its deployed shared fixture."""

    fixtures: Mapping[str, str]
    failure_error_type: str
    worker_identity: str
    suspension: str = "callback"
    callback_payload: str = "marker"
    recovery_policy: str = "reuse-worker"
    replay_effects: tuple[ReplayEffect, ...] = field(default_factory=_default_replay_effects)
    binding_version: int = 1

    def __post_init__(self) -> None:
        if type(self.binding_version) is not int or self.binding_version != 1:
            raise ValueError("fixtureBinding.bindingVersion must be 1")
        if not isinstance(self.fixtures, Mapping) or any(
            key not in _FIXTURE_KEYS or not _non_empty_str(value) for key, value in self.fixtures.items()
        ):
            raise ValueError("fixtureBinding.fixtures has an unknown key or a non-empty-string value is required")
        object.__setattr__(self, "fixtures", MappingProxyType(dict(self.fixtures)))
        if not _non_empty_str(self.failure_error_type):
            raise ValueError("fixtureBinding.failureErrorType must be a non-empty string")
        if not _one_of(self.worker_identity, _WORKER_IDENTITIES):
            raise ValueError("fixtureBinding.workerIdentity must be process or worker")
        if not _one_of(self.suspension, _SUSPENSION_MODES):
            raise ValueError("fixtureBinding.suspension must be callback or wait")
        if not _one_of(self.callback_payload, _CALLBACK_PAYLOADS):
            raise ValueError("fixtureBinding.callbackPayload must be marker or json-marker")
        if not _one_of(self.recovery_policy, _RECOVERY_POLICIES):
            raise ValueError("fixtureBinding.recoveryPolicy must be reuse-worker or replace-worker")
        effects = tuple(self.replay_effects)
        if not effects or any(not isinstance(effect, ReplayEffect) for effect in effects):
            raise ValueError("fixtureBinding.replayEffects must be a non-empty list of ReplayEffect")
        operations = [effect.operation for effect in effects]
        if len(set(operations)) != len(operations):
            # One count per operation: a repeated name would make the expected
            # allowance ambiguous instead of failing closed.
            raise ValueError("fixtureBinding.replayEffects must name each operation once")
        object.__setattr__(self, "replay_effects", effects)

    def to_manifest(self) -> dict[str, Any]:
        return {
            "bindingVersion": self.binding_version,
            "fixtures": dict(self.fixtures),
            "failureErrorType": self.failure_error_type,
            "workerIdentity": self.worker_identity,
            "suspension": self.suspension,
            "callbackPayload": self.callback_payload,
            "recoveryPolicy": self.recovery_policy,
            "replayEffects": [effect.to_manifest() for effect in self.replay_effects],
        }

    @classmethod
    def from_manifest(cls, raw: Mapping[str, Any]) -> FixtureBinding:
        if not isinstance(raw, Mapping):
            raise ValueError("fixtureBinding must be an object")
        if set(raw) - _BINDING_FIELDS:
            raise ValueError("fixtureBinding has unknown fields")
        for required in ("bindingVersion", "fixtures", "failureErrorType", "workerIdentity"):
            if required not in raw:
                raise ValueError(f"fixtureBinding is missing required field: {required}")
        kwargs: dict[str, Any] = {
            "binding_version": raw["bindingVersion"],
            "fixtures": raw["fixtures"],
            "failure_error_type": raw["failureErrorType"],
            "worker_identity": raw["workerIdentity"],
        }
        if "suspension" in raw:
            kwargs["suspension"] = raw["suspension"]
        if "callbackPayload" in raw:
            kwargs["callback_payload"] = raw["callbackPayload"]
        if "recoveryPolicy" in raw:
            kwargs["recovery_policy"] = raw["recoveryPolicy"]
        if "replayEffects" in raw:
            effects = raw["replayEffects"]
            if not isinstance(effects, Sequence) or isinstance(effects, (str, bytes)) or not effects:
                raise ValueError("fixtureBinding.replayEffects must be a non-empty array")
            kwargs["replay_effects"] = tuple(ReplayEffect.from_manifest(item) for item in effects)
        return cls(**kwargs)

    @classmethod
    def load(cls, path: str | Path) -> FixtureBinding:
        """Load a checked-in JSON fixture binding file."""
        return cls.from_manifest(json.loads(Path(path).read_text()))


@dataclass(frozen=True)
class RuntimeDefinition:
    name: str
    handler: str
    architecture: str = "arm64"
    memory_size: int = 2048
    binding: FixtureBinding = field(kw_only=True)

    def __post_init__(self):
        if self.name not in RUNTIMES or not self.handler:
            raise ValueError("An LMI fixture needs a supported runtime model and handler")
        if self.architecture not in {"arm64", "x86_64"} or self.memory_size < 128:
            raise ValueError("Invalid LMI fixture architecture or memory size")
        if not isinstance(self.binding, FixtureBinding):
            raise ValueError("An LMI fixture needs a FixtureBinding")

    def to_manifest(self) -> dict[str, Any]:
        return {
            "runtimeModel": self.name,
            "handler": self.handler,
            "architecture": self.architecture,
            "memorySize": self.memory_size,
            "fixtureBinding": self.binding.to_manifest(),
        }

    @classmethod
    def from_manifest(cls, manifest: Mapping[str, Any]):
        return cls(
            manifest["runtimeModel"],
            manifest["handler"],
            manifest["architecture"],
            manifest["memorySize"],
            binding=FixtureBinding.from_manifest(manifest["fixtureBinding"]),
        )
