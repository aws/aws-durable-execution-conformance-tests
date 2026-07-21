# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Shared input validation for the Python OTel conformance examples."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def require_scenario(event: Mapping[str, Any], expected: str) -> None:
    """Reject an event that was routed to the wrong conformance handler."""

    actual = event.get("scenario")
    if actual != expected:
        raise ValueError(f"Expected scenario {expected!r}, received {actual!r}")
