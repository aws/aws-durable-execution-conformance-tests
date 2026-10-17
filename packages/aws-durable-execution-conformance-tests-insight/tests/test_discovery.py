# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Sink discovery tests."""

from __future__ import annotations

from typing import Any

import pytest

from aws_durable_execution_conformance_tests_insight.discovery import (
    PluginDiscoveryError,
    discover_plugins,
)
from aws_durable_execution_conformance_tests_insight.sinks import BUILTIN_SINKS


class _Builtin:
    name = "builtin"


class _External:
    name = "external"


class _Point:
    value = "tests:plugin"

    def __init__(self, name: str, plugin: Any) -> None:
        self.name = name
        self._plugin = plugin

    def load(self) -> Any:
        if isinstance(self._plugin, Exception):
            raise self._plugin
        return self._plugin


def test_builtin_sinks_registered() -> None:
    assert sorted(BUILTIN_SINKS) == ["cloudwatch", "s3"]


def test_discovers_external_sink_alongside_builtins() -> None:
    plugins = discover_plugins(
        "example",
        {"builtin": _Builtin},
        entry_points=[_Point("external", _External)],
    )
    assert sorted(plugins) == ["builtin", "external"]


def test_rejects_duplicate_sink_name() -> None:
    class _Duplicate:
        name = "builtin"

    with pytest.raises(PluginDiscoveryError, match="Duplicate"):
        discover_plugins(
            "example",
            {"builtin": _Builtin},
            entry_points=[_Point("builtin", _Duplicate)],
        )


def test_reports_sink_load_failure() -> None:
    with pytest.raises(PluginDiscoveryError, match="Could not load"):
        discover_plugins(
            "example",
            {},
            entry_points=[_Point("broken", ImportError("missing"))],
        )
