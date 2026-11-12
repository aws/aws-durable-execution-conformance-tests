# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""LMI applicability is explicit and independent of ordinary suite discovery."""

from importlib.resources import files
from typing import Any

import yaml

RUNTIMES = ("javascript", "java", "python")
CAPABILITIES = ("lifecycle", "lifecycle-otel")
COVERAGE_PROFILES = ("standard", "python-lifecycle")


def requirements(
    capability: str, *, runtime: str | None = None, coverage_profile: str = "standard"
) -> dict[str, dict[str, Any]]:
    """Read only the contracts applicable to the selected coverage profile.

    Default callers keep the original six/eight requirement catalog. The Python
    regression profile explicitly replaces generic scenarios with the mapped SDK
    variants, while keeping the same stable IDs for contracts already present.
    """
    if capability not in CAPABILITIES:
        raise ValueError(f"Unknown capability profile: {capability}")
    if runtime is not None and runtime not in RUNTIMES:
        raise ValueError(f"Unknown runtime: {runtime}")
    if coverage_profile not in COVERAGE_PROFILES:
        raise ValueError(f"Unknown coverage profile: {coverage_profile}")
    if coverage_profile == "python-lifecycle" and runtime != "python":
        raise ValueError("python-lifecycle coverage requires runtime python")
    root = files(__package__).joinpath("requirements/lmi-lifecycle")
    result = {}
    for resource in sorted(root.iterdir(), key=lambda path: path.name):
        if not resource.name.endswith(".yaml"):
            continue
        requirement = yaml.safe_load(resource.read_text())
        if coverage_profile not in requirement.get("coverageProfiles", ("standard",)):
            continue
        if runtime is not None and runtime not in requirement["runtimes"]:
            continue
        if capability != "lifecycle-otel" and requirement["capability"] != "lifecycle":
            continue
        if coverage_profile == "python-lifecycle":
            regression = requirement["pythonLifecycle"]
            for field in ("description", "contract"):
                if field in regression:
                    requirement[field] = regression[field]
            definitions = list(regression["scenarios"])
            if capability == "lifecycle-otel":
                definitions.extend(requirement["pythonLifecycle"]["otelScenarios"])
            requirement["requiredScenarios"] = list(dict.fromkeys(item["scenario"] for item in definitions))
            # Matrix entries supply the exact roles and capacity for c1/c2. The
            # catalog describes the intersection, not an invented c2-only rule.
            requirement["minimumNativeConcurrency"] = 1
            requirement["roles"] = ["victim"]
        result[requirement["id"]] = requirement
    return result
