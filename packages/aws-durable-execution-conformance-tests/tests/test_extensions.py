# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for the optional conformance extension contract."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pytest
from aws_durable_execution_conformance_tests.extensions import (
    ExtensionCompatibilityError,
    ExtensionLoadError,
    RequirementSuite,
    SuiteCollisionError,
    load_extensions,
)


class _Point:
    name = "fake"
    value = "tests:extension"

    def __init__(self, value: Any) -> None:
        self._value = value

    def load(self) -> Any:
        if isinstance(self._value, Exception):
            raise self._value
        return self._value


class _Extension:
    name = "fake"
    requires_core = ">=0.2,<0.3"

    def __init__(
        self,
        suites: tuple[RequirementSuite, ...] = (),
        validation_services: tuple[str, ...] = (),
    ) -> None:
        self._suites = suites
        self._validation_services = validation_services

    def requirement_suites(self) -> tuple[RequirementSuite, ...]:
        return self._suites

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--fake", default="ok")

    def validate_configuration(self, args: argparse.Namespace) -> None:
        if args.fake == "bad":
            raise ValueError("bad fake configuration")

    def deployment_parameters(self, args: argparse.Namespace) -> dict[str, str]:
        return {"FakeParameter": args.fake}

    def validation_client_services(self, args: argparse.Namespace) -> tuple[str, ...]:
        del args
        return self._validation_services


def _requirement(root: Path, suite: str, description_id: str) -> Path:
    path = root / suite / f"{description_id}.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("description: test\n", encoding="utf-8")
    return path


def test_loads_extension_suite_and_discovers_resources(tmp_path: Path) -> None:
    core = tmp_path / "core"
    extension = tmp_path / "extension"
    _requirement(core, "step", "1-1")
    expected = _requirement(extension, "otel", "otel-1")

    registry = load_extensions(
        core,
        entry_points=[
            _Point(
                _Extension(
                    (
                        RequirementSuite(
                            name="otel",
                            root=extension / "otel",
                        ),
                    )
                )
            )
        ],
    )

    assert sorted(registry.suites) == ["otel", "step"]
    assert registry.discover_requirements(["otel"])["otel-1"].path == expected


def test_rejects_suite_name_collision(tmp_path: Path) -> None:
    _requirement(tmp_path, "step", "1-1")
    with pytest.raises(SuiteCollisionError, match="Suite 'step'"):
        load_extensions(
            tmp_path,
            entry_points=[_Point(_Extension((RequirementSuite(name="step", root=tmp_path / "step"),)))],
        )


def test_rejects_duplicate_requirement_ids(tmp_path: Path) -> None:
    core = tmp_path / "core"
    extension = tmp_path / "extension"
    _requirement(core, "step", "same")
    _requirement(extension, "otel", "same")
    registry = load_extensions(
        core,
        entry_points=[_Point(_Extension((RequirementSuite(name="otel", root=extension / "otel"),)))],
    )

    with pytest.raises(SuiteCollisionError, match="Requirement id 'same'"):
        registry.discover_requirements("all")


@pytest.mark.parametrize(
    ("requires_core", "message"),
    [
        (">=9", "requires core"),
        ("not a range", "invalid core range"),
        ("", "does not declare"),
    ],
)
def test_rejects_incompatible_core(
    tmp_path: Path,
    requires_core: str,
    message: str,
) -> None:
    extension = _Extension()
    extension.requires_core = requires_core
    with pytest.raises(ExtensionCompatibilityError, match=message):
        load_extensions(tmp_path, entry_points=[_Point(extension)])


def test_reports_import_failure(tmp_path: Path) -> None:
    with pytest.raises(ExtensionLoadError, match="Could not load extension"):
        load_extensions(
            tmp_path,
            entry_points=[_Point(ImportError("missing dependency"))],
        )


def test_rejects_missing_requirement_resource(tmp_path: Path) -> None:
    extension = _Extension((RequirementSuite(name="otel", root=tmp_path / "missing"),))
    with pytest.raises(ExtensionLoadError, match="has no requirement resource"):
        load_extensions(tmp_path, entry_points=[_Point(extension)])


def test_extension_arguments_and_deployment_parameters(tmp_path: Path) -> None:
    registry = load_extensions(tmp_path, entry_points=[_Point(_Extension())])
    parser = argparse.ArgumentParser()
    registry.add_arguments(parser)
    args = parser.parse_args(["--fake", "value"])
    args.suite = ["all"]

    registry.validate_configuration(args)
    assert registry.deployment_parameters(args) == {"FakeParameter": "value"}


def test_collects_clients_only_for_active_extension_suites(tmp_path: Path) -> None:
    extension_root = tmp_path / "extension"
    _requirement(extension_root, "otel", "otel-1")
    registry = load_extensions(
        tmp_path / "core",
        entry_points=[
            _Point(
                _Extension(
                    (RequirementSuite(name="otel", root=extension_root / "otel"),),
                    validation_services=("xray",),
                )
            )
        ],
    )
    args = argparse.Namespace()

    assert registry.validation_client_services(args, {"otel"}) == ("xray",)
    assert registry.validation_client_services(args, {"step"}) == ()
