# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""LMI applicability is explicit and independent of ordinary suite discovery."""

from importlib.resources import files
from typing import Any

import yaml

RUNTIMES = ("javascript", "java", "python")
CAPABILITIES = ("lifecycle",)
COVERAGE_PROFILES = ("standard",)


def requirements(
    capability: str, *, runtime: str | None = None, coverage_profile: str = "standard"
) -> dict[str, dict[str, Any]]:
    """Validate the run selection and load every shared lifecycle contract.

    All bundled requirements are mandatory for every supported runtime. These
    suite-wide rules do not need per-requirement applicability metadata.
    SDK-specific regression suites belong to their SDK repositories.
    """
    if capability not in CAPABILITIES:
        raise ValueError(f"Unknown capability profile: {capability}")
    if runtime is not None and runtime not in RUNTIMES:
        raise ValueError(f"Unknown runtime: {runtime}")
    if coverage_profile not in COVERAGE_PROFILES:
        raise ValueError(f"Unknown coverage profile: {coverage_profile}")
    root = files(__package__).joinpath("requirements/lmi-lifecycle")
    result = {}
    for resource in sorted(root.iterdir(), key=lambda path: path.name):
        if not resource.name.endswith(".yaml"):
            continue
        requirement = yaml.safe_load(resource.read_text())
        if (
            not isinstance(requirement, dict)
            or "requiredScenarios" in requirement
            or not isinstance(requirement.get("scenario"), str)
            or not requirement["scenario"].strip()
        ):
            raise ValueError(f"{resource.name} must declare exactly one non-empty scenario")
        result[requirement["id"]] = requirement
    return result
