# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: Apache-2.0
"""Explicit runtime and artifact configuration for an SDK-owned LMI fixture."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .catalog import RUNTIMES

CONCURRENCIES = {"c1": 1, "c2": 2}
SCALING = {"MinExecutionEnvironments": 1, "MaxExecutionEnvironments": 1}
QUALIFIER = "$LATEST.PUBLISHED"


@dataclass(frozen=True)
class RuntimeDefinition:
    name: str
    handler: str
    architecture: str = "arm64"
    memory_size: int = 2048

    def __post_init__(self):
        if self.name not in RUNTIMES or not self.handler:
            raise ValueError("An LMI fixture needs a supported runtime model and handler")
        if self.architecture not in {"arm64", "x86_64"} or self.memory_size < 128:
            raise ValueError("Invalid LMI fixture architecture or memory size")

    def to_manifest(self) -> dict[str, Any]:
        return {
            "runtimeModel": self.name,
            "handler": self.handler,
            "architecture": self.architecture,
            "memorySize": self.memory_size,
        }

    @classmethod
    def from_manifest(cls, manifest: Mapping[str, Any]):
        return cls(manifest["runtimeModel"], manifest["handler"], manifest["architecture"], manifest["memorySize"])
