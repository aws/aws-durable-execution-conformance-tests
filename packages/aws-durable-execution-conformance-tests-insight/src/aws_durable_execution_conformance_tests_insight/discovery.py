# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Nested entry-point discovery for Workflow Insight sink factories."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from importlib import metadata
from typing import Any

SINK_ENTRY_POINT_GROUP = "aws_durable_execution_conformance_tests_insight.sinks"


class PluginDiscoveryError(RuntimeError):
    """Raised for duplicate, missing, or failed sink plugins."""


def discover_plugins(
    group: str,
    builtins: Mapping[str, type[Any]],
    *,
    entry_points: Iterable[Any] | None = None,
) -> dict[str, Any]:
    """Load a named plugin catalog, rejecting ambiguous registrations."""

    plugins: dict[str, Any] = {name: plugin() for name, plugin in builtins.items()}
    points = entry_points if entry_points is not None else metadata.entry_points(group=group)
    for entry_point in points:
        try:
            loaded = entry_point.load()
            plugin = loaded() if isinstance(loaded, type) else loaded
        except Exception as exc:
            raise PluginDiscoveryError(
                f"Could not load {group} plugin {entry_point.name!r} from {entry_point.value!r}: {exc}"
            ) from exc
        name = getattr(plugin, "name", entry_point.name)
        if name in plugins:
            expected = builtins.get(name)
            if expected is not None and type(plugin) is expected:
                continue
            raise PluginDiscoveryError(f"Duplicate {group} plugin name {name!r}; rename one registration")
        plugins[name] = plugin
    return plugins
