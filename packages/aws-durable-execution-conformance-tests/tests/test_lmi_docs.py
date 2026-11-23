# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The LMI guide documents manifests that the loader accepts."""

import json
import re
from importlib.resources import files

import pytest

from aws_durable_execution_conformance_tests.lmi.app import load_manifest, run
from aws_durable_execution_conformance_tests.lmi.catalog import requirements


def test_documented_behavior_budgets_cover_each_requirement():
    guide = files("aws_durable_execution_conformance_tests.lmi").joinpath("README.md").read_text()
    for requirement in requirements("lifecycle").values():
        required = set(requirement.get("behaviorBudgets", ()))
        if not required:
            continue
        documented = re.search(
            rf"`{re.escape(requirement['scenario'])}` cases must set every budget in, for example,\s+"
            rf"`\"behavior_budgets\": (\{{[^`]*\}})`",
            guide,
        )
        assert documented, requirement["scenario"]
        assert set(json.loads(documented.group(1))) == required, requirement["scenario"]


def test_documented_lmi_commands_use_supported_flags(capsys):
    with pytest.raises(SystemExit):
        run(["--help"])
    supported = set(re.findall(r"--[a-z][a-z-]*", capsys.readouterr().out))
    guide = files("aws_durable_execution_conformance_tests.lmi").joinpath("README.md").read_text()
    blocks = [block for block in re.findall(r"```bash\n(.*?)```", guide, re.DOTALL) if "lmi-lifecycle" in block]
    documented = {flag for block in blocks for flag in re.findall(r"--[a-z][a-z-]*", block)}
    assert {"--adapter", "--collected-evidence", "--manifest", "--output"} <= documented
    assert documented <= supported


def test_documented_manifests_pass_the_loader(tmp_path):
    guide = files("aws_durable_execution_conformance_tests.lmi").joinpath("README.md").read_text()
    manifests = [block for block in re.findall(r"```json\n(.*?)```", guide, re.DOTALL) if '"schemaVersion"' in block]
    assert manifests
    for index, block in enumerate(manifests):
        path = tmp_path / f"manifest-{index}.json"
        path.write_text(block)
        assert load_manifest(path)["runId"]
