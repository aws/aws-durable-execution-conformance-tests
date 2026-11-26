# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The LMI guide documents manifests that the loader accepts."""

import json
import re
import shlex
from importlib.resources import files

import pytest

from aws_durable_execution_conformance_tests.lmi import app
from aws_durable_execution_conformance_tests.lmi.app import _require_shared_catalog_run, load_manifest, run
from aws_durable_execution_conformance_tests.lmi.catalog import requirements


class _Accepted(Exception):
    """Raised in place of loading a manifest, once argparse accepted the command."""


def _guide() -> str:
    return files("aws_durable_execution_conformance_tests.lmi").joinpath("README.md").read_text()


def _documented_commands() -> list[list[str]]:
    commands = []
    for block in re.findall(r"```bash\n(.*?)```", _guide(), re.DOTALL):
        if "lmi-lifecycle" in block:
            argv = shlex.split(block.replace("\\\n", " "))
            # The dispatcher requires the subcommand right after the executable.
            assert argv[:2] == ["durable-execution-conformance", "lmi-lifecycle"], argv
            commands.append(argv[2:])
    return commands


def test_documented_behavior_budgets_cover_each_requirement():
    guide = _guide()
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
    guide = _guide()
    blocks = [block for block in re.findall(r"```bash\n(.*?)```", guide, re.DOTALL) if "lmi-lifecycle" in block]
    documented = {flag for block in blocks for flag in re.findall(r"--[a-z][a-z-]*", block)}
    assert documented == {"--collect", "--collected-evidence", "--manifest", "--output"}
    assert documented <= supported


def test_each_documented_lmi_command_parses(monkeypatch):
    commands = _documented_commands()
    assert {"--collect", "--collected-evidence"} <= {flag for argv in commands for flag in argv}

    def accepted(path):
        raise _Accepted(path)

    monkeypatch.setattr(app, "load_manifest", accepted)
    for argv in commands:
        # A usage error exits with status 2 instead of reaching the manifest load.
        with pytest.raises(_Accepted):
            run(argv)


def test_documented_manifests_pass_the_loader(tmp_path):
    guide = _guide()
    manifests = [block for block in re.findall(r"```json\n(.*?)```", guide, re.DOTALL) if '"schemaVersion"' in block]
    assert manifests
    for index, block in enumerate(manifests):
        path = tmp_path / f"manifest-{index}.json"
        path.write_text(block)
        assert load_manifest(path)["runId"]
        # The guide says --collect also needs the sdk block from runner.make_manifest.
        with pytest.raises(ValueError, match=r"sdk\.deployment"):
            _require_shared_catalog_run(load_manifest(path))
