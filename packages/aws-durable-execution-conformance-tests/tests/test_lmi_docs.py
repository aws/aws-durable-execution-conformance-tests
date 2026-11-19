# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The LMI guide documents manifests that the loader accepts."""

import json
import re
from importlib.resources import files

from aws_durable_execution_conformance_tests.lmi.catalog import requirements


def test_documented_behavior_budgets_cover_each_requirement():
    guide = files("aws_durable_execution_conformance_tests.lmi").joinpath("README.md").read_text()
    for requirement in requirements("lifecycle").values():
        required = set(requirement.get("behaviorBudgets", ()))
        if not required:
            continue
        documented = re.search(
            rf"`{re.escape(requirement['scenario'])}` cases must set `\"behavior_budgets\": (\{{[^`]*\}})`", guide
        )
        assert documented, requirement["scenario"]
        assert set(json.loads(documented.group(1))) == required, requirement["scenario"]
