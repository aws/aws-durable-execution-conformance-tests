# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Chained execution ancestry requires real parent edges, not trace correlation."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from aws_durable_execution_conformance_tests_otel.model import Span, SpanLink, TelemetryQuery, Trace
from aws_durable_execution_conformance_tests_otel.polling import BackendFeatureDisparity
from aws_durable_execution_conformance_tests_otel.validators import validate_trace


def _chain() -> tuple[Span, Span, Span]:
    now = datetime(2026, 10, 14, tzinfo=UTC)
    invoke = Span(
        trace_id="1" * 32,
        span_id="2" * 16,
        name="otel-invoke",
        start_time=now,
        end_time=now + timedelta(seconds=1),
        attributes={"durable.execution.arn": "arn:caller", "durable.operation.status": "STARTED"},
    )
    infrastructure = replace(
        invoke,
        span_id="3" * 16,
        parent_span_id=invoke.span_id,
        name="target Lambda",
        attributes={},
    )
    target = replace(
        invoke,
        span_id="4" * 16,
        parent_span_id=infrastructure.span_id,
        name="Workflow",
        # Parent identity is independent of clock ordering across executions.
        start_time=now + timedelta(seconds=2),
        end_time=now + timedelta(seconds=3),
        attributes={"durable.execution.arn": "arn:target"},
    )
    return invoke, infrastructure, target


def _validate(spans: tuple[Span, ...], **selector: object) -> list[str]:
    now = datetime(2026, 10, 14, tzinfo=UTC)
    return validate_trace(
        Trace(trace_id="1" * 32, spans=spans),
        {
            "allowed_execution_arns": ["arn:caller", "arn:target"],
            "span_assertions": {
                "select": {"name": "Workflow"},
                "expect": {
                    "descendant_of": {
                        "name": "otel-invoke",
                        "attributes": {"durable.operation.status": "STARTED"},
                        **selector,
                    }
                },
            },
        },
        TelemetryQuery("arn:caller", "service", now, now),
        feature_disparities={BackendFeatureDisparity.SPAN_LINKS},
    )


def test_descendant_of_accepts_actual_infrastructure_path_without_clock_containment() -> None:
    assert _validate(_chain()) == []


def test_descendant_of_accepts_direct_parent() -> None:
    invoke, _, target = _chain()
    assert _validate((invoke, replace(target, parent_span_id=invoke.span_id))) == []


def test_descendant_of_accepts_repeated_consistent_intermediate_exports() -> None:
    invoke, infrastructure, target = _chain()
    assert _validate((invoke, infrastructure, infrastructure, target)) == []


@pytest.mark.parametrize("wrong_path", ["same-root", "link-only", "missing-parent", "cycle", "later-segment"])
def test_descendant_of_rejects_paths_not_reaching_first_invoke(wrong_path: str) -> None:
    invoke, infrastructure, target = _chain()
    other = replace(invoke, span_id="5" * 16, name="other", attributes={})
    if wrong_path == "same-root":
        infrastructure = replace(infrastructure, parent_span_id=other.span_id)
    elif wrong_path == "link-only":
        infrastructure = replace(infrastructure, parent_span_id=None)
        target = replace(target, links=(SpanLink(invoke.trace_id, invoke.span_id),))
    elif wrong_path == "missing-parent":
        infrastructure = replace(infrastructure, parent_span_id="f" * 16)
    elif wrong_path == "cycle":
        infrastructure = replace(infrastructure, parent_span_id=target.span_id)
    else:
        other = replace(other, name="otel-invoke", attributes={"durable.operation.status": "SUCCEEDED"})
        infrastructure = replace(infrastructure, parent_span_id=other.span_id)
    errors = _validate((invoke, infrastructure, target, other))
    assert any("descendant_of:" in error for error in errors), errors


def test_descendant_of_rejects_cross_trace_id_collision() -> None:
    invoke, infrastructure, target = _chain()
    errors = _validate((invoke, replace(infrastructure, trace_id="9" * 32), target))
    assert any("unresolved parent" in error for error in errors), errors


def test_descendant_of_rejects_selected_ancestor_on_another_trace() -> None:
    invoke, infrastructure, target = _chain()
    errors = _validate((replace(invoke, trace_id="9" * 32), infrastructure, target))
    assert any("different traces" in error for error in errors), errors


def test_descendant_of_rejects_cycle_through_required_ancestor() -> None:
    invoke, infrastructure, target = _chain()
    errors = _validate((replace(invoke, parent_span_id=target.span_id), infrastructure, target))
    assert any("parent cycle" in error for error in errors), errors


def test_descendant_of_rejects_same_id_alias_as_ancestor() -> None:
    invoke, _, target = _chain()
    errors = _validate((invoke, replace(target, span_id=invoke.span_id, parent_span_id=None)))
    assert any("own ancestor" in error for error in errors), errors


def test_descendant_of_does_not_require_ancestors_above_selected_target() -> None:
    invoke, infrastructure, target = _chain()
    assert _validate((replace(invoke, parent_span_id="f" * 16), infrastructure, target)) == []


def test_descendant_of_rejects_conflicting_intermediate_parent_exports() -> None:
    invoke, infrastructure, target = _chain()
    conflict = replace(infrastructure, parent_span_id="f" * 16)
    errors = _validate((invoke, infrastructure, conflict, target))
    assert any("conflicting parents" in error for error in errors), errors


@pytest.mark.parametrize("control", ["$linked", "$millisecond_precision", "$allow_unresolved"])
def test_descendant_of_cannot_skip_path_validation(control: str) -> None:
    errors = _validate(_chain(), **{control: True})
    assert any(control in error for error in errors), errors


def test_descendant_of_requires_unique_first_invoke_target() -> None:
    invoke, infrastructure, target = _chain()
    errors = _validate((invoke, replace(invoke, span_id="5" * 16), infrastructure, target))
    assert any("must select exactly one" in error for error in errors), errors
