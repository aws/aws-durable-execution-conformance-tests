# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Identify requirement diagnostics without depending on YAML assertion order."""

from collections.abc import Mapping, Sequence
from typing import Any


def _contains(actual: Any, identity: Any) -> bool:
    if isinstance(identity, Mapping):
        return isinstance(actual, Mapping) and all(
            key in actual and _contains(actual[key], value) for key, value in identity.items()
        )
    return actual == identity


def span_assertion_path(assertions: Mapping[str, Any], selector: Mapping[str, Any]) -> str:
    """Resolve a unique assertion by literal fields in its select mapping.

    This identifies YAML rules, not observed spans: matcher expressions remain
    literal, and omitted fields permit a concise partial selector identity.
    """
    matches = [
        index
        for index, assertion in enumerate(assertions["span_assertions"])
        if _contains(assertion["select"], selector)
    ]
    assert len(matches) == 1, f"Expected one assertion for {selector!r}, found {matches}"
    return f"span_assertions[{matches[0]}]"


def assert_span_error(
    errors: Sequence[str], assertions: Mapping[str, Any], selector: Mapping[str, Any], diagnostic: str
) -> None:
    """Require the intended rule's diagnostic, even when other rules also fail.

    Include the rule path and message, e.g. 'select matched no spans' or
    'expect.status: expected', rather than an unscoped error fragment.
    """
    expected = f"{span_assertion_path(assertions, selector)}.{diagnostic}"
    assert any(error.startswith(expected) for error in errors), (
        f"Missing {expected!r}; validation errors: {list(errors)!r}"
    )
