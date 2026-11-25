# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Fixtures shared by every test module."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _no_hard_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    """An in-process ``lmi-lifecycle`` run must never end the test process."""
    from aws_durable_execution_conformance_tests.lmi import app

    def hard_exit(code: int) -> None:
        raise AssertionError(f"lmi-lifecycle would have ended the test process with exit code {code}")

    monkeypatch.setattr(app, "_hard_exit", hard_exit)
