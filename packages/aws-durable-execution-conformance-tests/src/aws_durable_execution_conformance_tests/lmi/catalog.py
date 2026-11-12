# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""LMI applicability is explicit and independent of ordinary suite discovery."""

from importlib.resources import files
from typing import Any

import yaml

RUNTIMES = ("javascript", "java", "python")
CAPABILITIES = ("lifecycle", "lifecycle-otel")


def requirements(capability: str) -> dict[str, dict[str, Any]]:
    if capability not in CAPABILITIES:
        raise ValueError(f"Unknown capability profile: {capability}")
    root = files(__package__).joinpath("requirements/lmi-lifecycle")
    result = {}
    for resource in sorted(root.iterdir(), key=lambda path: path.name):
        if resource.name.endswith(".yaml"):
            requirement = yaml.safe_load(resource.read_text())
            if capability == "lifecycle-otel" or requirement["capability"] == "lifecycle":
                result[requirement["id"]] = requirement
    return result
