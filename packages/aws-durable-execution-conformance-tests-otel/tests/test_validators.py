# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Provider-neutral telemetry validator and redaction tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

from aws_durable_execution_conformance_tests_otel.model import (
    Span,
    SpanLink,
    TelemetryQuery,
    Trace,
)
from aws_durable_execution_conformance_tests_otel.polling import (
    BackendFeatureDisparity,
)
from aws_durable_execution_conformance_tests_otel.redaction import REDACTED, redact
from aws_durable_execution_conformance_tests_otel.validators import validate_trace


def _trace(execution_arn: str = "arn:test") -> Trace:
    now = datetime.now(UTC)
    root = Span(
        trace_id="1" * 32,
        span_id="2" * 16,
        name="root",
        start_time=now,
        end_time=now + timedelta(seconds=10),
        kind="SERVER",
        status="OK",
        service_name="service",
        attributes={
            "durable.execution.arn": execution_arn,
            "faas.invocation_id": "invocation-1",
            "durable.operation.outcome": "retry",
        },
    )
    child = Span(
        trace_id="1" * 32,
        span_id="3" * 16,
        parent_span_id=root.span_id,
        name="child",
        start_time=now + timedelta(seconds=2),
        end_time=now + timedelta(seconds=3),
        kind="INTERNAL",
        status="OK",
        service_name="service",
        attributes={
            "durable.execution.arn": execution_arn,
            "faas.invocation_id": "invocation-2",
            "custom.metadata": {
                "attempt": 2,
                "labels": ["durable", "resumed"],
            },
        },
        links=(SpanLink(trace_id="1" * 32, span_id=root.span_id),),
    )
    return Trace(trace_id="1" * 32, spans=(root, child))


def _query() -> TelemetryQuery:
    now = datetime.now(UTC)
    return TelemetryQuery("arn:test", "service", now, now)


def test_validates_stable_cross_invocation_invariants() -> None:
    errors = validate_trace(
        _trace(),
        {
            "minimum_spans": 2,
            "require_execution_correlation": True,
        },
        _query(),
    )
    assert errors == []


def test_counts_case_insensitive_invocations_without_status_attributes() -> None:
    trace = _trace()
    root, child = trace.spans
    invocation_attributes = {
        "durable.execution.arn": "arn:test",
        "durable.invocation.first": True,
    }
    first = replace(root, name="invocation", attributes=invocation_attributes)
    resumed = replace(
        child,
        name="Invocation",
        attributes={
            **invocation_attributes,
            "durable.invocation.first": False,
        },
    )

    assert (
        validate_trace(
            replace(trace, spans=(first, resumed)),
            {"minimum_invocations": 2},
            _query(),
        )
        == []
    )


def test_requires_at_most_one_distinct_root_per_trace() -> None:
    trace = _trace()
    root, child = trace.spans
    ambient_root = replace(
        root,
        trace_id="9" * 32,
        span_id="4" * 16,
        name="ambient",
    )
    invocation = replace(
        child,
        trace_id=ambient_root.trace_id,
        span_id="5" * 16,
        parent_span_id=ambient_root.span_id,
        name="Invocation",
    )

    assert (
        validate_trace(
            replace(trace, spans=(root, child, ambient_root, invocation)),
            {"require_unique_root_per_trace": True},
            _query(),
        )
        == []
    )
    assert (
        validate_trace(
            replace(
                trace,
                spans=(
                    replace(
                        root,
                        parent_span_id="f" * 16,
                    ),
                ),
            ),
            {"require_unique_root_per_trace": True},
            _query(),
        )
        == []
    )


def test_reports_multiple_distinct_roots_and_ignores_duplicate_exports() -> None:
    trace = _trace()
    root, child = trace.spans
    second_root = replace(
        root,
        span_id="4" * 16,
        name="second-root",
    )

    assert (
        validate_trace(
            replace(trace, spans=(root, root, child)),
            {"require_unique_root_per_trace": True},
            _query(),
        )
        == []
    )
    assert validate_trace(
        replace(trace, spans=(root, child, second_root)),
        {"require_unique_root_per_trace": True},
        _query(),
    ) == [
        f"Trace {root.trace_id} contains 2 distinct root spans: "
        f"'root' ({root.span_id}), 'second-root' ({second_root.span_id})"
    ]


def test_requires_one_trace_per_execution_but_allows_distinct_chained_executions() -> None:
    trace = _trace()
    root, child = trace.spans
    split_child = replace(
        child,
        trace_id="9" * 32,
    )

    assert validate_trace(
        replace(trace, spans=(root, split_child)),
        {"require_single_trace_per_execution": True},
        _query(),
    ) == [f"Durable execution 'arn:test' appears in 2 trace IDs: {root.trace_id}, {split_child.trace_id}"]

    target_child = replace(
        split_child,
        attributes={
            **split_child.attributes,
            "durable.execution.arn": "arn:target",
        },
    )
    assert (
        validate_trace(
            replace(trace, spans=(root, target_child)),
            {
                "require_single_trace_per_execution": True,
                "allowed_execution_arns": ["arn:test", "arn:target"],
            },
            _query(),
        )
        == []
    )


def test_requires_selected_spans_to_have_parent_ids_without_resolving_the_parent() -> None:
    trace = _trace()
    root, child = trace.spans
    workflow = replace(
        root,
        name="Workflow",
        parent_span_id="f" * 16,
    )
    invocation = replace(
        child,
        name="Invocation",
        parent_span_id=workflow.span_id,
    )
    assertions = {
        "require_parented_spans": [
            {"name": "Workflow"},
            {"name": "Invocation"},
        ]
    }

    assert validate_trace(replace(trace, spans=(workflow, invocation)), assertions, _query()) == []
    parentless_workflow = replace(workflow, parent_span_id=None)
    assert validate_trace(replace(trace, spans=(parentless_workflow, invocation)), assertions, _query()) == [
        f"require_parented_spans[0]: span 'Workflow' ({workflow.span_id}) has no parent"
    ]
    for invalid_parent_id in ("0" * 16, "g" * 16):
        invalid_workflow = replace(workflow, parent_span_id=invalid_parent_id)
        assert validate_trace(replace(trace, spans=(invalid_workflow, invocation)), assertions, _query()) == [
            f"require_parented_spans[0]: span 'Workflow' ({workflow.span_id}) "
            f"has invalid parent span ID {invalid_parent_id!r}"
        ]


def test_rejects_spans_that_end_before_they_start() -> None:
    trace = _trace()
    root, child = trace.spans
    invalid_child = replace(
        child,
        start_time=child.end_time + timedelta(seconds=1),
    )

    errors = validate_trace(
        replace(trace, spans=(root, invalid_child)),
        {},
        _query(),
    )

    assert errors == [
        f"Span 'child' ({child.span_id}) starts at {invalid_child.start_time.isoformat()}, "
        f"after it ends at {invalid_child.end_time.isoformat()}"
    ]


def test_asserts_before_after_non_parent_inside_and_parent_containment() -> None:
    trace = _trace()
    root, child = trace.spans
    child = replace(
        child,
        start_time=root.start_time,
        end_time=root.end_time,
    )
    later = replace(
        child,
        span_id="4" * 16,
        name="later",
        start_time=child.end_time,
        end_time=child.end_time + timedelta(seconds=1),
    )
    container = replace(
        root,
        span_id="5" * 16,
        name="container",
        start_time=child.start_time - timedelta(seconds=1),
        end_time=child.end_time + timedelta(seconds=1),
    )

    errors = validate_trace(
        replace(trace, spans=(root, child, later, container)),
        {
            "span_assertions": [
                {
                    "select": {"name": "child"},
                    "expect": {
                        "before": {"name": "later"},
                        "inside": {"name": "container"},
                        "parent": {"name": "root"},
                    },
                },
                {
                    "select": {"name": "later"},
                    "expect": {"after": {"name": "child"}},
                },
            ]
        },
        _query(),
    )

    assert errors == []


def test_millisecond_timestamp_disparity_tolerates_backend_rounding() -> None:
    trace = _trace()
    root, child = trace.spans
    rounded_child = replace(
        child,
        start_time=root.start_time - timedelta(milliseconds=1),
        end_time=root.end_time + timedelta(milliseconds=1),
    )
    assertions = {
        "span_assertions": {
            "select": {"name": "child"},
            "expect": {
                "parent": {"name": "root"},
            },
        }
    }

    assert validate_trace(replace(trace, spans=(root, rounded_child)), assertions, _query()) == [
        (
            f"span_assertions[0].expect.parent: child span 'child' ({child.span_id}) starts at "
            f"{rounded_child.start_time.isoformat()}, before parent span 'root' ({root.span_id}) starts at "
            f"{root.start_time.isoformat()}"
        ),
        (
            f"span_assertions[0].expect.parent: child span 'child' ({child.span_id}) ends at "
            f"{rounded_child.end_time.isoformat()}, after parent span 'root' ({root.span_id}) ends at "
            f"{root.end_time.isoformat()}"
        ),
    ]
    assert (
        validate_trace(
            replace(trace, spans=(root, rounded_child)),
            assertions,
            _query(),
            feature_disparities=frozenset({BackendFeatureDisparity.MILLISECOND_TIMESTAMPS}),
        )
        == []
    )


def test_inside_can_select_a_linked_span_among_duplicate_matches() -> None:
    trace = _trace()
    root, child = trace.spans
    first_invocation = replace(
        root,
        name="invocation",
        span_id="4" * 16,
    )
    linked_invocation = replace(
        root,
        name="invocation",
        span_id="5" * 16,
    )
    linked_child = replace(
        child,
        links=(
            SpanLink(
                trace_id=linked_invocation.trace_id,
                span_id=linked_invocation.span_id,
            ),
        ),
    )
    linked_trace = replace(
        trace,
        spans=(first_invocation, linked_invocation, linked_child),
    )

    errors = validate_trace(
        linked_trace,
        {
            "span_assertions": {
                "select": {"name": "child"},
                "expect": {
                    "inside": {
                        "$linked": True,
                        "name": "invocation",
                    }
                },
            }
        },
        _query(),
    )
    ambiguous_errors = validate_trace(
        linked_trace,
        {
            "span_assertions": {
                "select": {"name": "child"},
                "expect": {"inside": {"name": "invocation"}},
            }
        },
        _query(),
    )

    assert errors == []
    assert ambiguous_errors == ["span_assertions[0].expect.inside matched 2 spans; it must select exactly one"]


def test_validates_parentage_and_links_across_correlated_traces() -> None:
    trace = _trace()
    root, child = trace.spans
    workflow = replace(root, name="Workflow")
    invocation = replace(
        root,
        trace_id="9" * 32,
        span_id="8" * 16,
        name="Invocation",
        attributes={
            "durable.execution.arn": "arn:test",
            "durable.invocation.first": True,
        },
    )
    operation = replace(
        child,
        parent_span_id=workflow.span_id,
        links=(SpanLink(trace_id=invocation.trace_id, span_id=invocation.span_id),),
    )
    correlated_trace = replace(trace, spans=(workflow, operation, invocation))

    errors = validate_trace(
        correlated_trace,
        {
            "span_assertions": [
                {
                    "select": {"name": "Invocation"},
                    "expect": {
                        "attributes": {
                            "durable.execution.arn": "arn:test",
                            "durable.invocation.first": True,
                        }
                    },
                },
                {
                    "select": {"name": "child"},
                    "expect": {
                        "parent": {"name": "Workflow"},
                        "links": [{"name": "Invocation"}],
                    },
                },
            ]
        },
        _query(),
    )

    assert errors == []


def test_link_occurrence_identifies_the_chronological_invocation() -> None:
    trace = _trace()
    root, child = trace.spans
    first_invocation = replace(
        root,
        trace_id="8" * 32,
        span_id="6" * 16,
        name="Invocation",
        attributes={
            "durable.execution.arn": "arn:test",
            "durable.invocation.first": True,
        },
    )
    second_invocation = replace(
        first_invocation,
        trace_id="9" * 32,
        span_id="7" * 16,
        start_time=first_invocation.start_time + timedelta(seconds=20),
        end_time=first_invocation.end_time + timedelta(seconds=20),
        attributes={
            "durable.execution.arn": "arn:test",
            "durable.invocation.first": False,
        },
    )
    target_invocation = replace(
        first_invocation,
        trace_id="a" * 32,
        span_id="8" * 16,
        start_time=first_invocation.start_time + timedelta(seconds=10),
        end_time=first_invocation.end_time + timedelta(seconds=10),
        attributes={
            "durable.execution.arn": "arn:target",
            "durable.invocation.first": True,
        },
    )
    operation = replace(
        child,
        start_time=second_invocation.start_time + timedelta(seconds=2),
        end_time=second_invocation.start_time + timedelta(seconds=3),
        links=(SpanLink(trace_id=second_invocation.trace_id, span_id=second_invocation.span_id),),
    )
    correlated_trace = replace(
        trace,
        spans=(operation, second_invocation, target_invocation, first_invocation),
    )
    link_expectation = {
        "name": "Invocation",
        "attributes": {"durable.execution.arn": "arn:test"},
    }

    assert (
        validate_trace(
            correlated_trace,
            {
                "span_assertions": {
                    "select": {"name": "child"},
                    "expect": {
                        "inside": {
                            "$linked": True,
                            "name": "Invocation",
                        },
                        "links": [
                            {
                                **link_expectation,
                                "$occurrence": 2,
                            }
                        ],
                    },
                },
                "allowed_execution_arns": ["arn:test", "arn:target"],
            },
            _query(),
        )
        == []
    )
    assert validate_trace(
        correlated_trace,
        {
            "span_assertions": {
                "select": {"name": "child"},
                "expect": {
                    "inside": {
                        "$linked": True,
                        "name": "Invocation",
                    },
                    "links": [
                        {
                            **link_expectation,
                            "$occurrence": 1,
                        }
                    ],
                },
            },
            "allowed_execution_arns": ["arn:test", "arn:target"],
        },
        _query(),
    ) == ["span_assertions[0].expect.links[0].$occurrence: linked span is occurrence 2, expected 1"]


def test_link_occurrence_rejects_a_replayed_operation_self_link_by_occurrence() -> None:
    trace = _trace()
    root, child = trace.spans
    operation_id = "operation-1"
    initial_operation = replace(
        root,
        name="operation",
        attributes={
            **root.attributes,
            "durable.operation.id": operation_id,
            "durable.operation.status": "STARTED",
        },
    )
    replayed_operation = replace(
        child,
        name="operation",
        attributes={
            **child.attributes,
            "durable.operation.id": operation_id,
            "durable.operation.status": "SUCCEEDED",
        },
        links=(SpanLink(trace_id=child.trace_id, span_id=child.span_id),),
    )

    assert validate_trace(
        replace(trace, spans=(initial_operation, replayed_operation)),
        {
            "span_assertions": {
                "select": {
                    "name": "operation",
                    "attributes": {"durable.operation.id": operation_id},
                },
                "count": 2,
                "expect": {},
                "expect_by_occurrence": [
                    {"links": []},
                    {
                        "links": [
                            {
                                "$occurrence": 1,
                                "name": "operation",
                                "attributes": {"durable.operation.id": operation_id},
                            }
                        ]
                    },
                ],
            }
        },
        _query(),
    ) == ["span_assertions[0].expect_by_occurrence[1].links[0].$occurrence: linked span is occurrence 2, expected 1"]


def test_repeated_span_assertions_apply_expectations_by_chronological_occurrence() -> None:
    trace = _trace()
    root, child = trace.spans
    replay = replace(
        child,
        span_id="4" * 16,
        start_time=child.start_time + timedelta(seconds=2),
        end_time=child.end_time + timedelta(seconds=2),
        links=(
            SpanLink(trace_id=child.trace_id, span_id=child.span_id),
            SpanLink(trace_id=root.trace_id, span_id=root.span_id),
        ),
    )
    assertions = {
        "span_assertions": {
            "select": {"name": "child"},
            "count": 2,
            "expect": {"status": "OK"},
            "expect_by_occurrence": [
                {"links": [{"name": "root"}]},
                {
                    "links": [
                        {"span_id": child.span_id},
                        {"name": "root"},
                    ]
                },
            ],
        }
    }

    unordered_trace = replace(trace, spans=(replay, root, child))
    assert validate_trace(unordered_trace, assertions, _query()) == []

    replay_without_initial_link = replace(
        replay,
        links=(SpanLink(trace_id=root.trace_id, span_id=root.span_id),),
    )
    assert validate_trace(
        replace(trace, spans=(replay_without_initial_link, root, child)),
        assertions,
        _query(),
    ) == ["span_assertions[0].expect_by_occurrence[1].links: expected 2 item(s), found 1"]


def test_span_link_disparity_skips_linked_temporal_relations() -> None:
    trace = _trace()
    root, child = trace.spans
    trace_without_links = replace(trace, spans=(root, replace(child, links=())))
    assertions = {
        "span_assertions": {
            "select": {"name": "child"},
            "expect": {
                "inside": {
                    "$linked": True,
                    "name": "root",
                }
            },
        }
    }

    assert validate_trace(trace_without_links, assertions, _query()) == [
        "span_assertions[0].expect.inside matched no spans"
    ]
    assert (
        validate_trace(
            trace_without_links,
            assertions,
            _query(),
            feature_disparities=frozenset({BackendFeatureDisparity.SPAN_LINKS}),
        )
        == []
    )


def test_reports_timestamp_and_parent_containment_violations() -> None:
    trace = _trace()
    root, child = trace.spans
    overlapping = replace(
        child,
        span_id="4" * 16,
        name="overlapping",
        start_time=child.start_time - timedelta(seconds=1),
        end_time=child.start_time + timedelta(milliseconds=500),
    )
    narrow_parent = replace(
        root,
        span_id="5" * 16,
        name="narrow-parent",
        start_time=child.start_time + timedelta(milliseconds=250),
        end_time=child.end_time - timedelta(milliseconds=250),
    )
    narrow_container = replace(
        narrow_parent,
        span_id="6" * 16,
        name="narrow-container",
    )
    child_outside_parent = replace(
        child,
        parent_span_id=narrow_parent.span_id,
    )

    errors = validate_trace(
        replace(trace, spans=(root, child_outside_parent, overlapping, narrow_parent, narrow_container)),
        {
            "span_assertions": {
                "select": {"name": "child"},
                "expect": {
                    "before": {"name": "overlapping"},
                    "after": {"name": "overlapping"},
                    "inside": {"name": "narrow-container"},
                    "parent": {"name": "narrow-parent"},
                },
            }
        },
        _query(),
    )

    assert len(errors) == 6
    assert any("expect.before: span 'child'" in error and "after span 'overlapping'" in error for error in errors)
    assert any("expect.after: span 'child'" in error and "before span 'overlapping'" in error for error in errors)
    assert any(
        "expect.inside: span 'child'" in error and "before containing span 'narrow-container'" in error
        for error in errors
    )
    assert any(
        "expect.inside: span 'child'" in error and "after containing span 'narrow-container'" in error
        for error in errors
    )
    assert any(
        "expect.parent: child span 'child'" in error and "before parent span 'narrow-parent'" in error
        for error in errors
    )
    assert any(
        "expect.parent: child span 'child'" in error and "after parent span 'narrow-parent'" in error
        for error in errors
    )


def test_reports_invalid_timestamp_relationship_selectors() -> None:
    trace = _trace()
    root, child = trace.spans
    sibling = replace(
        child,
        span_id="4" * 16,
        name="sibling",
    )
    invalid_after = replace(
        child,
        span_id="5" * 16,
        name="invalid-after",
    )
    invalid_before = replace(
        child,
        span_id="6" * 16,
        name="invalid-before",
    )
    invalid_link = replace(
        child,
        span_id="7" * 16,
        name="invalid-link",
    )
    errors = validate_trace(
        replace(trace, spans=(root, child, sibling, invalid_after, invalid_before, invalid_link)),
        {
            "span_assertions": [
                {
                    "select": {"name": "child"},
                    "expect": {"inside": "root"},
                },
                {
                    "select": {"name": "invalid-after"},
                    "expect": {"after": {"name": "missing"}},
                },
                {
                    "select": {"name": "invalid-before"},
                    "expect": {"before": {"name": "${/^(?:root|sibling)$/}"}},
                },
                {
                    "select": {"name": "invalid-link"},
                    "expect": {"inside": {"$linked": False, "name": "root"}},
                },
            ]
        },
        _query(),
    )

    assert errors == [
        "span_assertions[0].expect.inside must be a mapping",
        "span_assertions[1].expect.after matched no spans",
        "span_assertions[2].expect.before matched 2 spans; it must select exactly one",
        "span_assertions[3].expect.inside.$linked must be true",
    ]


def test_counts_every_canonical_invocation_span_occurrence() -> None:
    trace = _trace()
    invocation = replace(
        trace.spans[0],
        name="invocation",
        attributes={
            "durable.execution.arn": "arn:test",
            "durable.invocation.first": True,
            "durable.invocation.status": "SUCCEEDED",
        },
    )

    assert (
        validate_trace(
            replace(trace, spans=(invocation, invocation)),
            {"minimum_invocations": 2},
            _query(),
        )
        == []
    )


def test_asserts_duplicate_trace_and_span_ids_individually() -> None:
    trace = _trace()
    root, child = trace.spans
    replayed_child = replace(
        child,
        status="ERROR",
        attributes={
            **child.attributes,
            "faas.invocation_id": "invocation-3",
        },
    )

    errors = validate_trace(
        replace(trace, spans=(root, child, replayed_child)),
        {
            "span_assertions": [
                {
                    "select": {
                        "name": "child",
                        "attributes": {"faas.invocation_id": "invocation-2"},
                    },
                    "expect": {
                        "span_id": child.span_id,
                        "status": "OK",
                    },
                },
                {
                    "select": {
                        "name": "child",
                        "attributes": {"faas.invocation_id": "invocation-3"},
                    },
                    "expect": {
                        "span_id": replayed_child.span_id,
                        "status": "ERROR",
                    },
                },
            ]
        },
        _query(),
    )

    assert errors == []


def test_does_not_count_noncanonical_spans_named_invocation() -> None:
    trace = _trace()
    root, child = trace.spans
    durable_invocation = replace(
        root,
        name="invocation",
        attributes={
            "durable.execution.arn": "arn:test",
            "durable.invocation.first": True,
            "durable.invocation.status": "SUCCEEDED",
        },
    )
    application_span = replace(
        child,
        name="invocation",
        attributes={"durable.execution.arn": "arn:test"},
    )

    assert validate_trace(
        replace(trace, spans=(durable_invocation, application_span)),
        {"minimum_invocations": 2},
        _query(),
    ) == ["Expected at least 2 canonical durable invocation spans, found 1"]


def test_reports_correlation_mismatches() -> None:
    errors = validate_trace(
        _trace("arn:wrong"),
        {"require_execution_correlation": True},
        _query(),
    )
    assert any("durable execution ARN" in error for error in errors)


def test_allows_declared_correlations_from_nested_durable_executions() -> None:
    trace = _trace()
    root, child = trace.spans
    target = replace(
        child,
        span_id="4" * 16,
        name="target",
        attributes={
            "durable.execution.arn": "arn:target",
            "faas.invocation_id": "invocation-3",
        },
    )
    distributed_trace = replace(trace, spans=(root, child, target))

    assert validate_trace(
        distributed_trace,
        {"require_execution_correlation": True},
        _query(),
    ) == ["Spans contain durable execution correlation values outside allowed_execution_arns"]
    assert (
        validate_trace(
            distributed_trace,
            {
                "require_execution_correlation": True,
                "allowed_execution_arns": ["arn:test", "arn:target"],
            },
            _query(),
        )
        == []
    )


def test_asserts_any_property_and_nested_metadata_on_one_span() -> None:
    errors = validate_trace(
        _trace(),
        {
            "span_assertions": {
                "select": {
                    "name": "child",
                    "attributes": {"faas.invocation_id": "invocation-2"},
                },
                "expect": {
                    "trace_id": "1" * 32,
                    "span_id": "3" * 16,
                    "parent_span_id": "2" * 16,
                    "parent": {
                        "name": "root",
                        "kind": "SERVER",
                        "status": "OK",
                        "parent_span_id": None,
                        "attributes": {
                            "faas.invocation_id": "invocation-1",
                            "durable.operation.outcome": "retry",
                        },
                    },
                    "name": "child",
                    "kind": "INTERNAL",
                    "start_time": "*",
                    "end_time": "*",
                    "status": "OK",
                    "service_name": "service",
                    "attributes": {
                        "durable.execution.arn": "arn:test",
                        "faas.invocation_id": "invocation-2",
                        "custom.metadata": {
                            "attempt": 2,
                            "labels": ["durable", "resumed"],
                        },
                    },
                    "links": [
                        {
                            "name": "root",
                            "kind": "SERVER",
                            "status": "OK",
                            "attributes": {
                                "durable.execution.arn": "arn:test",
                            },
                        }
                    ],
                },
            }
        },
        _query(),
    )

    assert errors == []


def test_span_names_accept_regex_matchers() -> None:
    errors = validate_trace(
        _trace(),
        {
            "span_assertions": {
                "select": {"name": "${/^chi(?:ld|rp)$/}"},
                "expect": {
                    "name": "${/^child$/}",
                    "parent": {"name": "${/^root$/}"},
                },
            }
        },
        _query(),
    )

    assert errors == []


def test_span_links_accept_any_of_matchers() -> None:
    errors = validate_trace(
        _trace(),
        {
            "span_assertions": {
                "select": {"name": "child"},
                "expect": {
                    "links": {
                        "$any_of": [
                            [],
                            [
                                {
                                    "name": "root",
                                    "attributes": {
                                        "durable.execution.arn": "arn:test",
                                    },
                                }
                            ],
                        ]
                    }
                },
            }
        },
        _query(),
    )

    assert errors == []


def test_linked_span_attributes_accept_any_of_matchers() -> None:
    errors = validate_trace(
        _trace(),
        {
            "span_assertions": {
                "select": {"name": "child"},
                "expect": {
                    "links": [
                        {
                            "attributes": {
                                "$any_of": [
                                    {"faas.invocation_id": "*"},
                                    {"aws.lambda.invocation_id": "*"},
                                ]
                            }
                        }
                    ]
                },
            }
        },
        _query(),
    )

    assert errors == []


def test_link_assertion_counts_matching_replayed_spans_with_duplicate_id() -> None:
    trace = _trace()
    root, child = trace.spans
    replayed_root = replace(
        root,
        name="replayed-root",
        status="ERROR",
        attributes={
            **root.attributes,
            "durable.operation.outcome": "pending",
        },
    )
    trace_with_duplicates = replace(trace, spans=(replayed_root, root, child))

    def validate_link(expected_link: dict[str, object]) -> list[str]:
        return validate_trace(
            trace_with_duplicates,
            {
                "span_assertions": {
                    "select": {"name": "child"},
                    "expect": {"links": [expected_link]},
                }
            },
            _query(),
        )

    matching_root: dict[str, object] = {
        "name": "root",
        "status": "OK",
        "attributes": {"durable.operation.outcome": "retry"},
    }
    both_roots: dict[str, object] = {"attributes": {"durable.execution.arn": "arn:test"}}

    assert validate_link(matching_root) == []
    assert validate_link(both_roots) == [
        "span_assertions[0].expect.links[0]: linked span expectation matched 2 spans; expected 1"
    ]
    assert validate_link({"count": 2, **both_roots}) == []
    assert validate_link({"count": 2, **matching_root}) == [
        "span_assertions[0].expect.links[0]: linked span expectation matched 1 spans; expected 2"
    ]


def test_reports_missing_and_mismatched_linked_span_assertions() -> None:
    trace = _trace()
    root, child = trace.spans
    missing_link = replace(
        child,
        links=(SpanLink(trace_id=child.trace_id, span_id="9" * 16),),
    )

    errors = validate_trace(
        trace,
        {
            "span_assertions": {
                "select": {"name": "child"},
                "expect": {
                    "links": [
                        {
                            "name": "not-root",
                            "attributes": {"missing.key": "value"},
                        }
                    ]
                },
            }
        },
        _query(),
    )
    missing_errors = validate_trace(
        replace(trace, spans=(root, missing_link)),
        {
            "span_assertions": {
                "select": {"name": "child"},
                "expect": {"links": [{"name": "root"}]},
            }
        },
        _query(),
    )

    assert "span_assertions[0].expect.links[0].name: expected 'not-root'" in errors
    assert "span_assertions[0].expect.links[0].attributes.missing.key: property is missing" in errors
    assert missing_errors == ["span_assertions[0].expect.links[0]: linked span is not present in the trace"]


def test_asserts_repeated_spans_and_complete_plugin_contract() -> None:
    trace = _trace()
    root, child = trace.spans
    repeated_root = replace(
        root,
        span_id="4" * 16,
        name="repeated",
    )
    repeated_child = replace(
        child,
        span_id="5" * 16,
        name="repeated",
    )
    errors = validate_trace(
        replace(trace, spans=(root, child, repeated_root, repeated_child)),
        {
            "require_all_spans": True,
            "exact_attribute_prefixes": ["durable."],
            "span_assertions": [
                {
                    "select": {"name": "repeated"},
                    "count": 2,
                    "expect": {
                        "service_name": "service",
                    },
                },
                {
                    "select": {"name": "root"},
                    "expect": {
                        "attributes": {
                            "durable.execution.arn": "arn:test",
                            "durable.operation.outcome": "retry",
                        },
                    },
                },
                {
                    "select": {"name": "child"},
                    "expect": {
                        "attributes": {
                            "durable.execution.arn": "arn:test",
                        },
                        "parent": {"name": "root"},
                    },
                },
            ],
        },
        _query(),
    )

    assert errors == []


def test_asserts_one_of_multiple_allowed_span_counts() -> None:
    trace = _trace()
    root, child = trace.spans
    repeated = replace(child, span_id="4" * 16)
    assertions = {
        "span_assertions": {
            "select": {"name": "child"},
            "count": {"$any_of": [1, 2]},
            "expect": {"status": "OK"},
        }
    }

    assert validate_trace(trace, assertions, _query()) == []
    assert validate_trace(replace(trace, spans=(root, child, repeated)), assertions, _query()) == []


def test_reports_uncovered_spans_and_unasserted_plugin_attributes() -> None:
    trace = _trace()
    root, child = trace.spans
    infrastructure = replace(
        child,
        span_id="4" * 16,
        name="infrastructure",
        attributes={"cloud.provider": "aws"},
    )
    errors = validate_trace(
        replace(trace, spans=(root, child, infrastructure)),
        {
            "require_all_spans": True,
            "span_assertion_scope": {
                "attributes": {"durable.execution.arn": "*"},
            },
            "exact_attribute_prefixes": "durable.",
            "span_assertions": {
                "select": {"name": "root"},
                "expect": {
                    "attributes": {
                        "durable.execution.arn": "arn:test",
                    },
                },
            },
        },
        _query(),
    )

    assert any("durable.operation.outcome" in error for error in errors)
    assert any("Span assertions did not cover: child" in error for error in errors)
    assert all("infrastructure" not in error for error in errors)


def test_scopes_complete_span_coverage_to_multiple_executions() -> None:
    trace = _trace()
    root, child = trace.spans
    target = replace(
        child,
        span_id="4" * 16,
        name="target",
        attributes={"durable.execution.arn": "arn:target"},
    )
    infrastructure = replace(
        child,
        span_id="5" * 16,
        name="infrastructure",
        attributes={"cloud.provider": "aws"},
    )

    errors = validate_trace(
        replace(trace, spans=(root, child, target, infrastructure)),
        {
            "require_execution_correlation": False,
            "require_all_spans": True,
            "span_assertion_scope": [
                {"attributes": {"durable.execution.arn": "arn:test"}},
                {"attributes": {"durable.execution.arn": "arn:target"}},
            ],
            "span_assertions": [
                {
                    "select": {"name": "root"},
                    "expect": {},
                },
                {
                    "select": {"name": "child"},
                    "expect": {},
                },
            ],
        },
        _query(),
    )

    assert any("Span assertions did not cover: target" in error for error in errors)
    assert all("infrastructure" not in error for error in errors)


def test_reports_missing_external_and_mismatched_parent_assertions() -> None:
    trace = _trace()
    root, child = trace.spans
    invalid_parent_child = replace(
        child,
        span_id="4" * 16,
        name="invalid-parent-child",
    )
    external_child = Span(
        trace_id=child.trace_id,
        span_id=child.span_id,
        parent_span_id="9" * 16,
        name=child.name,
        start_time=child.start_time,
        end_time=child.end_time,
        kind=child.kind,
        status=child.status,
        service_name=child.service_name,
        attributes=child.attributes,
        links=child.links,
    )

    errors = validate_trace(
        replace(trace, spans=(root, child, invalid_parent_child)),
        {
            "span_assertions": [
                {
                    "select": {"name": "root"},
                    "expect": {"parent": {"name": "root"}},
                },
                {
                    "select": {"name": "child"},
                    "expect": {
                        "parent": {
                            "name": "not-root",
                            "attributes": {"missing.key": "value"},
                        }
                    },
                },
                {
                    "select": {"name": "invalid-parent-child"},
                    "expect": {"parent": "root"},
                },
            ]
        },
        _query(),
    )
    external_errors = validate_trace(
        Trace(trace_id=trace.trace_id, spans=(root, external_child)),
        {
            "span_assertions": {
                "select": {"name": "child"},
                "expect": {"parent": {"name": "root"}},
            }
        },
        _query(),
    )

    assert "span_assertions[0].expect.parent: selected span has no parent" in errors
    assert "span_assertions[1].expect.parent.name: expected 'not-root'" in errors
    assert "span_assertions[1].expect.parent.attributes.missing.key: property is missing" in errors
    assert "span_assertions[2].expect.parent must be a mapping" in errors
    assert external_errors == ["span_assertions[0].expect.parent: parent span is not present in the trace"]


def test_parent_assertion_accepts_matching_replayed_span_with_duplicate_id() -> None:
    trace = _trace()
    root, child = trace.spans
    replayed_root = replace(
        root,
        name="replayed-root",
        attributes={
            **root.attributes,
            "durable.operation.outcome": "pending",
        },
    )

    assert (
        validate_trace(
            replace(trace, spans=(replayed_root, root, child)),
            {
                "span_assertions": {
                    "select": {"name": "child"},
                    "expect": {
                        "parent": {
                            "name": "root",
                            "attributes": {
                                "durable.operation.outcome": "retry",
                            },
                        }
                    },
                }
            },
            _query(),
        )
        == []
    )


def test_parent_assertion_can_allow_replay_backdated_child() -> None:
    trace = _trace()
    root, child = trace.spans
    backdated_child = replace(
        child,
        start_time=root.start_time - timedelta(seconds=1),
    )
    assertions = {
        "span_assertions": {
            "select": {"name": "child"},
            "expect": {
                "parent": {
                    "$allow_outside": True,
                    "name": "root",
                }
            },
        }
    }

    assert validate_trace(replace(trace, spans=(root, backdated_child)), assertions, _query()) == []


def test_parent_assertion_can_allow_an_unresolved_external_parent() -> None:
    trace = _trace()
    root, child = trace.spans
    external_child = replace(child, parent_span_id="9" * 16)
    assertions = {
        "span_assertions": {
            "select": {"name": "child"},
            "expect": {
                "parent": {
                    "$allow_unresolved": True,
                    "name": "Durable Execution Attempt #1",
                }
            },
        }
    }

    assert validate_trace(replace(trace, spans=(root, external_child)), assertions, _query()) == []
    assert validate_trace(trace, assertions, _query()) == [
        "span_assertions[0].expect.parent.name: expected 'Durable Execution Attempt #1'"
    ]
    invalid_external_child = replace(child, parent_span_id="0" * 16)
    assert validate_trace(
        replace(trace, spans=(root, invalid_external_child)),
        assertions,
        _query(),
    ) == [
        "span_assertions[0].expect.parent: selected span has invalid parent span ID "
        f"{invalid_external_child.parent_span_id!r}"
    ]


def test_parent_assertion_allows_unresolved_parent_with_similar_parent_in_trace() -> None:
    trace = _trace()
    root, child = trace.spans
    backend_parent = replace(
        root,
        name="Durable Execution Attempt #1",
        attributes={},
    )
    other_workflow = replace(
        child,
        span_id="4" * 16,
        parent_span_id=backend_parent.span_id,
        name="other-workflow",
        attributes={
            **child.attributes,
            "durable.execution.arn": "arn:other",
        },
    )
    target_workflow = replace(
        child,
        span_id="5" * 16,
        parent_span_id="9" * 16,
        name="target-workflow",
    )
    assertions = {
        "allowed_execution_arns": ["arn:test", "arn:other"],
        "span_assertions": {
            "select": {"name": "target-workflow"},
            "expect": {
                "parent": {
                    "$allow_unresolved": True,
                    "name": backend_parent.name,
                }
            },
        },
    }

    assert (
        validate_trace(
            replace(trace, spans=(backend_parent, other_workflow, target_workflow)),
            assertions,
            _query(),
        )
        == []
    )


def test_parent_assertion_rejects_invalid_directives() -> None:
    outside_errors = validate_trace(
        _trace(),
        {
            "span_assertions": {
                "select": {"name": "child"},
                "expect": {
                    "parent": {
                        "$allow_outside": False,
                        "name": "root",
                    }
                },
            }
        },
        _query(),
    )
    unresolved_errors = validate_trace(
        _trace(),
        {
            "span_assertions": {
                "select": {"name": "child"},
                "expect": {
                    "parent": {
                        "$allow_unresolved": False,
                        "name": "root",
                    }
                },
            }
        },
        _query(),
    )

    assert outside_errors == [
        "span_assertions[0].expect.parent.$allow_outside must be true",
    ]
    assert unresolved_errors == [
        "span_assertions[0].expect.parent.$allow_unresolved must be true",
    ]


def test_reports_missing_ambiguous_and_mismatched_span_assertions() -> None:
    errors = validate_trace(
        _trace(),
        {
            "span_assertions": [
                {
                    "select": {"name": "missing"},
                    "expect": {"status": "OK"},
                },
                {
                    "select": {"status": "OK"},
                    "expect": {"service_name": "service"},
                },
                {
                    "select": {"name": "child"},
                    "expect": {
                        "name": "not-child",
                        "kind": "SERVER",
                        "status": "ERROR",
                        "service_name": "not-service",
                        "attributes": {
                            "missing.key": "value",
                        },
                        "links": [],
                    },
                },
            ]
        },
        _query(),
    )

    assert "span_assertions[0].select matched no spans" in errors
    assert "span_assertions[1].select matched 2 spans; it must select exactly one" in errors
    assert "span_assertions[2].expect.name: expected 'not-child'" in errors
    assert "span_assertions[2].expect.kind: expected 'SERVER'" in errors
    assert "span_assertions[2].expect.status: expected 'ERROR'" in errors
    assert "span_assertions[2].expect.service_name: expected 'not-service'" in errors
    assert "span_assertions[2].expect.attributes.missing.key: property is missing" in errors
    assert "span_assertions[2].expect.links: expected 0 item(s), found 1" in errors


def test_unset_status_disparity_applies_to_span_and_parent_expectations() -> None:
    assertions = {
        "span_assertions": {
            "select": {"name": "child"},
            "expect": {
                "status": "UNSET",
                "parent": {"status": "UNSET"},
            },
        }
    }

    assert validate_trace(_trace(), assertions, _query()) == [
        "span_assertions[0].expect.status: expected 'UNSET'",
        "span_assertions[0].expect.parent.status: expected 'UNSET'",
    ]
    assert (
        validate_trace(
            _trace(),
            assertions,
            _query(),
            feature_disparities=frozenset({BackendFeatureDisparity.UNSET_STATUS}),
        )
        == []
    )


def test_unset_status_disparity_applies_to_span_selectors() -> None:
    assertions = {
        "span_assertions": {
            "select": {"name": "root", "status": "UNSET"},
            "expect": {},
        }
    }

    assert validate_trace(_trace(), assertions, _query()) == ["span_assertions[0].select matched no spans"]
    assert (
        validate_trace(
            _trace(),
            assertions,
            _query(),
            feature_disparities=frozenset({BackendFeatureDisparity.UNSET_STATUS}),
        )
        == []
    )


def test_unset_status_disparity_prefers_earliest_unset_and_latest_ok_spans() -> None:
    trace = _trace()
    root, child = trace.spans
    native_trace = replace(
        trace,
        spans=(
            replace(root, name="callback", status="UNSET"),
            replace(child, name="callback"),
        ),
    )
    normalized_trace = replace(
        trace,
        spans=(
            replace(child, name="callback"),
            replace(root, name="callback"),
        ),
    )
    assertions = {
        "span_assertions": [
            {
                "select": {"name": "callback", "status": "UNSET"},
                "expect": {"span_id": root.span_id, "status": "UNSET"},
            },
            {
                "select": {"name": "callback", "status": "OK"},
                "expect": {"span_id": child.span_id, "status": "OK"},
            },
        ]
    }
    ok_first_assertions = {
        "span_assertions": list(reversed(assertions["span_assertions"])),
    }
    disparities = frozenset({BackendFeatureDisparity.UNSET_STATUS})

    assert validate_trace(native_trace, assertions, _query(), feature_disparities=disparities) == []
    assert validate_trace(normalized_trace, assertions, _query()) == [
        "span_assertions[0].select matched no spans",
        "span_assertions[1].select matched 2 spans; it must select exactly one",
    ]
    assert validate_trace(normalized_trace, assertions, _query(), feature_disparities=disparities) == []
    assert validate_trace(normalized_trace, ok_first_assertions, _query(), feature_disparities=disparities) == []


def test_span_selectors_do_not_reuse_consumed_spans() -> None:
    assertions = {
        "span_assertions": [
            {
                "select": {"name": "root"},
                "expect": {},
            },
            {
                "select": {"name": "root"},
                "expect": {},
            },
        ]
    }

    assert validate_trace(_trace(), assertions, _query()) == [
        "span_assertions[1].select matched no spans",
    ]


def test_unset_status_disparity_applies_to_status_matchers() -> None:
    assertions = {
        "span_assertions": {
            "select": {"name": "child"},
            "expect": {"status": "${/^(?:ERROR|UNSET)$/}"},
        }
    }

    assert validate_trace(_trace(), assertions, _query()) == [
        "span_assertions[0].expect.status: value 'OK' does not match regex pattern '^(?:ERROR|UNSET)$'",
    ]
    assert (
        validate_trace(
            _trace(),
            assertions,
            _query(),
            feature_disparities=frozenset({BackendFeatureDisparity.UNSET_STATUS}),
        )
        == []
    )


def test_span_link_disparity_skips_link_expectations() -> None:
    assertions = {
        "span_assertions": {
            "select": {"name": "child"},
            "expect": {"links": []},
        }
    }

    assert validate_trace(_trace(), assertions, _query()) == [
        "span_assertions[0].expect.links: expected 0 item(s), found 1",
    ]
    assert (
        validate_trace(
            _trace(),
            assertions,
            _query(),
            feature_disparities=frozenset({BackendFeatureDisparity.SPAN_LINKS}),
        )
        == []
    )


def test_reports_invalid_span_assertion_schema() -> None:
    assert validate_trace(
        _trace(),
        {"span_assertions": "not-a-mapping-or-sequence"},
        _query(),
    ) == ["span_assertions must be a mapping or sequence of mappings"]

    errors = validate_trace(
        _trace(),
        {
            "span_assertions": [
                "not-a-mapping",
                {"select": "root", "expect": {}},
                {"select": {"name": "root"}},
                {"select": {}, "expect": {}, "unknown": True},
            ]
        },
        _query(),
    )

    assert errors == [
        "span_assertions[0] must be a mapping",
        "span_assertions[1].select must be a mapping",
        "span_assertions[2].expect must be a mapping",
        "span_assertions[3] has unknown field(s): unknown",
    ]

    count_errors = validate_trace(
        _trace(),
        {
            "allowed_execution_arns": 1,
            "exact_attribute_prefixes": 1,
            "require_parented_spans": ["Workflow"],
            "require_single_trace_per_execution": "yes",
            "require_unique_root_per_trace": "yes",
            "span_assertion_scope": ["plugin"],
            "span_assertions": {
                "select": {"name": "root"},
                "count": 0,
                "expect": {},
            },
        },
        _query(),
    )
    assert count_errors == [
        "require_unique_root_per_trace must be a boolean",
        "require_single_trace_per_execution must be a boolean",
        "require_parented_spans[0] must be a mapping",
        "allowed_execution_arns must be a string or sequence of strings",
        "exact_attribute_prefixes must be a string or sequence of strings",
        "span_assertion_scope must be a mapping or sequence of mappings",
        "span_assertions[0].count must be a positive integer or $any_of positive integers",
    ]

    occurrence_errors = validate_trace(
        _trace(),
        {
            "span_assertions": {
                "select": {"name": "child"},
                "expect": {},
                "expect_by_occurrence": ["not-a-mapping"],
            }
        },
        _query(),
    )
    assert occurrence_errors == ["span_assertions[0].expect_by_occurrence must be a sequence of mappings"]


def test_redacts_secret_keys_and_values() -> None:
    payload = {
        "headers": "x-api-key=secret-value",
        "message": "request used secret-value",
        "nested": {"token": "secret-value"},
    }
    safe = redact(payload, secrets=["secret-value"])

    assert safe["headers"] == REDACTED
    assert safe["nested"]["token"] == REDACTED
    assert safe["message"] == f"request used {REDACTED}"
