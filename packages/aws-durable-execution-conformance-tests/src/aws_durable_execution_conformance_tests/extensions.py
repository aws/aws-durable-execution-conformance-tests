# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Public extension contract for optional conformance suites."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from importlib import metadata
from pathlib import Path
from typing import Any, Protocol

from packaging.specifiers import InvalidSpecifier, SpecifierSet

from aws_durable_execution_conformance_tests.__about__ import __version__
from aws_durable_execution_conformance_tests.history import load_yaml_file
from aws_durable_execution_conformance_tests.validate import discover_suites

EXTENSION_ENTRY_POINT_GROUP = "aws_durable_execution_conformance_tests.extensions"


class ExtensionError(RuntimeError):
    """Base class for actionable extension discovery failures."""


class ExtensionLoadError(ExtensionError):
    """Raised when an installed extension cannot be imported."""


class ExtensionCompatibilityError(ExtensionError):
    """Raised when an extension does not support this core version."""


class SuiteCollisionError(ExtensionError):
    """Raised when two providers register the same suite name."""


@dataclass(frozen=True)
class ValidationContext:
    """Provider-neutral context supplied to post-execution validators."""

    description_id: str
    function_name: str
    execution_arn: str
    invocation_started_at_ms: int
    invocation_finished_at_ms: int
    region: str
    language: str
    requirement: Mapping[str, Any]
    execution_history: Mapping[str, Any]
    output_dir: Path
    placeholders: Mapping[str, Any] = field(default_factory=dict)
    options: Mapping[str, Any] = field(default_factory=dict)
    aws_clients: Mapping[str, Any] = field(default_factory=dict)


ValidationHook = Callable[[ValidationContext], Sequence[str]]


@dataclass(frozen=True)
class RequirementSuite:
    """A named requirement resource root and its optional validation hook."""

    name: str
    root: Path
    validation_hook: ValidationHook | None = None
    provider: str = "core"


@dataclass(frozen=True)
class RequirementCase:
    """A discovered requirement file with its owning suite."""

    description_id: str
    path: Path
    suite: RequirementSuite


class ConformanceExtension(Protocol):
    """Contract loaded through :data:`EXTENSION_ENTRY_POINT_GROUP`."""

    name: str
    requires_core: str

    def requirement_suites(self) -> Sequence[RequirementSuite]:
        """Return requirement suites contributed by this extension."""

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Register extension-owned CLI options."""

    def validate_configuration(self, args: argparse.Namespace) -> None:
        """Reject invalid configuration before deployment."""

    def deployment_parameters(self, args: argparse.Namespace) -> Mapping[str, str]:
        """Return non-secret SAM parameter overrides."""

    def deployment_secrets(self, args: argparse.Namespace) -> Mapping[str, str]:
        """Return secret SAM parameters that must be redacted from diagnostics."""


class ValidationClientProvider(Protocol):
    """Optional extension capability for pre-created AWS validation clients."""

    def validation_client_services(self, args: argparse.Namespace) -> Sequence[str]:
        """Return AWS services whose clients must be created before validation."""


@dataclass
class ExtensionRegistry:
    """Loaded extensions and a collision-free suite catalog."""

    extensions: tuple[ConformanceExtension, ...]
    suites: dict[str, RequirementSuite]

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        for extension in self.extensions:
            extension.add_arguments(parser)

    def validate_configuration(self, args: argparse.Namespace) -> None:
        selected = set(args.suite)
        for extension in self.extensions:
            owned_suites = {suite.name for suite in extension.requirement_suites()}
            if "all" in selected or selected & owned_suites:
                extension.validate_configuration(args)

    def deployment_parameters(self, args: argparse.Namespace) -> dict[str, str]:
        selected = set(args.suite)
        parameters: dict[str, str] = {}
        for extension in self.extensions:
            owned_suites = {suite.name for suite in extension.requirement_suites()}
            if "all" not in selected and not selected & owned_suites:
                continue
            for key, value in extension.deployment_parameters(args).items():
                if key in parameters:
                    raise ExtensionError(f"Deployment parameter {key!r} is provided by more than one extension")
                parameters[key] = value
        return parameters

    def deployment_secrets(self, args: argparse.Namespace) -> dict[str, str]:
        selected = set(args.suite)
        secrets: dict[str, str] = {}
        for extension in self.extensions:
            owned_suites = {suite.name for suite in extension.requirement_suites()}
            if "all" not in selected and not selected & owned_suites:
                continue
            provider = getattr(extension, "deployment_secrets", None)
            if provider is None:
                continue
            for key, value in provider(args).items():
                if key in secrets:
                    raise ExtensionError(f"Secret deployment parameter {key!r} is provided by more than one extension")
                secrets[key] = value
        return secrets

    def validation_client_services(
        self,
        args: argparse.Namespace,
        active_suites: Iterable[str],
    ) -> tuple[str, ...]:
        """Collect AWS client services needed by active extension suites."""
        active = set(active_suites)
        services: set[str] = set()
        for extension in self.extensions:
            owned_suites = {suite.name for suite in extension.requirement_suites()}
            if not active.intersection(owned_suites):
                continue
            provider = getattr(extension, "validation_client_services", None)
            if provider is None:
                continue
            for service_name in provider(args):
                if not isinstance(service_name, str) or not service_name:
                    raise ExtensionError(f"Extension {extension.name!r} returned an invalid AWS service name")
                services.add(service_name)
        return tuple(sorted(services))

    def discover_requirements(
        self,
        suites: str | Sequence[str] | None = None,
    ) -> dict[str, RequirementCase]:
        if isinstance(suites, str):
            selected = {suites}
        elif suites is None:
            selected = {"all"}
        else:
            selected = set(suites)

        cases: dict[str, RequirementCase] = {}
        for name, suite in self.suites.items():
            if "all" not in selected and name not in selected:
                continue
            for path in sorted(suite.root.rglob("*.yaml")):
                description_id = path.stem
                if description_id in cases:
                    previous = cases[description_id]
                    raise SuiteCollisionError(
                        f"Requirement id {description_id!r} is provided by both {previous.path} and {path}"
                    )
                cases[description_id] = RequirementCase(description_id, path, suite)
        return cases


def _entry_points(
    entry_points: Iterable[Any] | None,
) -> Iterable[Any]:
    if entry_points is not None:
        return entry_points
    return metadata.entry_points(group=EXTENSION_ENTRY_POINT_GROUP)


def load_extensions(
    core_requirements_root: str | Path,
    *,
    entry_points: Iterable[Any] | None = None,
    core_version: str = __version__,
) -> ExtensionRegistry:
    """Load installed extensions and combine their suites with the core suites."""

    suites: dict[str, RequirementSuite] = {}
    core_root = Path(core_requirements_root)
    for name in discover_suites(core_root):
        suites[name] = RequirementSuite(name=name, root=core_root / name)

    loaded_extensions: list[ConformanceExtension] = []
    extension_names: set[str] = set()
    for entry_point in _entry_points(entry_points):
        try:
            candidate = entry_point.load()
            extension = candidate() if isinstance(candidate, type) else candidate
        except Exception as exc:
            raise ExtensionLoadError(
                f"Could not load extension {entry_point.name!r} from {entry_point.value!r}: {exc}"
            ) from exc

        name = getattr(extension, "name", entry_point.name)
        if not isinstance(name, str) or not name:
            raise ExtensionLoadError(f"Extension entry point {entry_point.name!r} does not define a non-empty name")
        if name in extension_names:
            raise ExtensionLoadError(f"Duplicate extension name {name!r}")

        requires_core = getattr(extension, "requires_core", None)
        if not isinstance(requires_core, str) or not requires_core:
            raise ExtensionCompatibilityError(f"Extension {name!r} does not declare requires_core")
        try:
            compatible = core_version in SpecifierSet(requires_core)
        except InvalidSpecifier as exc:
            raise ExtensionCompatibilityError(
                f"Extension {name!r} declares invalid core range {requires_core!r}"
            ) from exc
        if not compatible:
            raise ExtensionCompatibilityError(
                f"Extension {name!r} requires core {requires_core}, but {core_version} is installed"
            )

        try:
            extension_suites = tuple(extension.requirement_suites())
        except Exception as exc:
            raise ExtensionLoadError(f"Extension {name!r} failed while registering requirement suites: {exc}") from exc
        for suite in extension_suites:
            if suite.name in suites:
                previous = suites[suite.name]
                raise SuiteCollisionError(
                    f"Suite {suite.name!r} from extension {name!r} conflicts with "
                    f"suite provided by {previous.provider!r}"
                )
            if not suite.root.is_dir():
                raise ExtensionLoadError(
                    f"Suite {suite.name!r} from extension {name!r} has no "
                    f"requirement resource directory at {suite.root}"
                )
            suites[suite.name] = RequirementSuite(
                name=suite.name,
                root=suite.root,
                validation_hook=suite.validation_hook,
                provider=name,
            )

        extension_names.add(name)
        loaded_extensions.append(extension)

    return ExtensionRegistry(tuple(loaded_extensions), suites)


def requirement_description(case: RequirementCase) -> str | None:
    """Load a requirement's human-readable description."""

    try:
        data = load_yaml_file(str(case.path))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    description = data.get("description")
    return str(description) if description is not None else None
