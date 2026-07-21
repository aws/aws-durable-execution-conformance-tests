# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Stable, provider-neutral OpenTelemetry conformance assertions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from aws_durable_execution_conformance_tests_otel.model import (
    TelemetryQuery,
    Trace,
    normalize_id,
    span_to_dict,
)

_EXECUTION_ATTRIBUTE_KEYS = (
    "aws.lambda.durable_execution.arn",
    "durable.execution.arn",
    "durable_execution_arn",
)
_INVOCATION_ATTRIBUTE_KEYS = (
    "faas.invocation_id",
    "aws.lambda.invocation_id",
)
_OUTCOME_ATTRIBUTE_KEYS = (
    "durable.operation.outcome",
    "durable.execution.outcome",
    "durable.execution.status",
)
_ATTEMPT_NUMBER_ATTRIBUTE_KEYS = (
    "durable.attempt.number",
    "durable.operation.attempt",
)


def _attribute_values(
    trace: Trace,
    keys: tuple[str, ...],
) -> list[str]:
    return [str(span.attributes[key]).lower() for span in trace.spans for key in keys if key in span.attributes]


def _has_retry_attempt(trace: Trace) -> bool:
    for span in trace.spans:
        for key in _ATTEMPT_NUMBER_ATTRIBUTE_KEYS:
            value = span.attributes.get(key)
            try:
                if value is not None and float(value) > 1:
                    return True
            except (TypeError, ValueError):
                continue
    return False


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _matches(expected: Any, actual: Any) -> bool:
    if expected == "*":
        return True
    if isinstance(expected, Mapping):
        return isinstance(actual, Mapping) and all(
            key in actual and _matches(value, actual[key]) for key, value in expected.items()
        )
    if _is_sequence(expected):
        return (
            _is_sequence(actual)
            and len(expected) == len(actual)
            and all(
                _matches(expected_item, actual_item)
                for expected_item, actual_item in zip(expected, actual, strict=True)
            )
        )
    return expected == actual


def _expectation_errors(
    expected: Any,
    actual: Any,
    *,
    path: str,
) -> list[str]:
    if expected == "*":
        return []
    if isinstance(expected, Mapping):
        if not isinstance(actual, Mapping):
            return [f"{path}: expected a mapping"]
        errors: list[str] = []
        for key, value in expected.items():
            child_path = f"{path}.{key}"
            if key not in actual:
                errors.append(f"{child_path}: property is missing")
                continue
            errors.extend(_expectation_errors(value, actual[key], path=child_path))
        return errors
    if _is_sequence(expected):
        if not _is_sequence(actual):
            return [f"{path}: expected a sequence"]
        if len(expected) != len(actual):
            return [f"{path}: expected {len(expected)} item(s), found {len(actual)}"]
        errors = []
        for index, (expected_item, actual_item) in enumerate(zip(expected, actual, strict=True)):
            errors.extend(
                _expectation_errors(
                    expected_item,
                    actual_item,
                    path=f"{path}[{index}]",
                )
            )
        return errors
    if expected != actual:
        return [f"{path}: expected {expected!r}"]
    return []


def _parent_expectation_errors(
    expected: Any,
    span: Mapping[str, Any],
    spans_by_id: Mapping[str, list[Mapping[str, Any]]],
    *,
    path: str,
) -> list[str]:
    if not isinstance(expected, Mapping):
        return [f"{path} must be a mapping"]

    parent_span_id = span["parent_span_id"]
    if parent_span_id is None:
        return [f"{path}: selected span has no parent"]

    parents = spans_by_id.get(parent_span_id, [])
    if not parents:
        return [f"{path}: parent span is not present in the trace"]
    if len(parents) > 1:
        return [f"{path}: parent span id matched {len(parents)} spans; it must identify exactly one"]

    return _expectation_errors(expected, parents[0], path=path)


def _span_assertion_errors(trace: Trace, raw_assertions: Any) -> list[str]:
    if raw_assertions is None:
        return []
    if isinstance(raw_assertions, Mapping):
        span_assertions = [raw_assertions]
    elif _is_sequence(raw_assertions):
        span_assertions = list(raw_assertions)
    else:
        return ["span_assertions must be a mapping or sequence of mappings"]

    spans = [span_to_dict(span) for span in trace.spans]
    spans_by_id: dict[str, list[Mapping[str, Any]]] = {}
    for span in spans:
        spans_by_id.setdefault(span["span_id"], []).append(span)

    errors: list[str] = []
    for index, assertion in enumerate(span_assertions):
        path = f"span_assertions[{index}]"
        if not isinstance(assertion, Mapping):
            errors.append(f"{path} must be a mapping")
            continue

        unknown = sorted(set(assertion) - {"select", "expect"}, key=str)
        if unknown:
            errors.append(f"{path} has unknown field(s): {', '.join(str(key) for key in unknown)}")
            continue

        selector = assertion.get("select", {})
        expected = assertion.get("expect")
        if not isinstance(selector, Mapping):
            errors.append(f"{path}.select must be a mapping")
            continue
        if not isinstance(expected, Mapping):
            errors.append(f"{path}.expect must be a mapping")
            continue

        matches = [span for span in spans if _matches(selector, span)]
        if not matches:
            errors.append(f"{path}.select matched no spans")
            continue
        if len(matches) > 1:
            errors.append(f"{path}.select matched {len(matches)} spans; it must select exactly one")
            continue
        expected_properties = {key: value for key, value in expected.items() if key != "parent"}
        errors.extend(
            _expectation_errors(
                expected_properties,
                matches[0],
                path=f"{path}.expect",
            )
        )
        if "parent" in expected:
            errors.extend(
                _parent_expectation_errors(
                    expected["parent"],
                    matches[0],
                    spans_by_id,
                    path=f"{path}.expect.parent",
                )
            )
    return errors


def validate_trace(
    trace: Trace,
    assertions: Mapping[str, Any],
    query: TelemetryQuery,
) -> list[str]:
    """Validate stable integration invariants without prescribing span schemas."""

    errors: list[str] = []
    minimum_spans = int(assertions.get("minimum_spans", 1))
    if len(trace.spans) < minimum_spans:
        errors.append(f"Expected at least {minimum_spans} span(s), found {len(trace.spans)}")

    inconsistent = [span.span_id for span in trace.spans if span.trace_id != trace.trace_id]
    if inconsistent:
        errors.append("Canonical trace contains spans with a different trace id: " + ", ".join(inconsistent))

    if assertions.get("require_execution_correlation", True):
        execution_values = _attribute_values(trace, _EXECUTION_ATTRIBUTE_KEYS)
        if query.execution_arn.lower() not in execution_values:
            errors.append(
                "No span carries the durable execution ARN in a supported "
                f"correlation attribute ({', '.join(_EXECUTION_ATTRIBUTE_KEYS)})"
            )
        wrong = {value for value in execution_values if value != query.execution_arn.lower()}
        if wrong:
            errors.append("Spans contain conflicting durable execution correlation values")

    minimum_invocations = int(assertions.get("minimum_invocations", 1))
    if minimum_invocations > 1:
        invocations = set(_attribute_values(trace, _INVOCATION_ATTRIBUTE_KEYS))
        if len(invocations) < minimum_invocations:
            errors.append(
                f"Expected telemetry from at least {minimum_invocations} Lambda invocations, found {len(invocations)}"
            )

    required_outcomes = {str(value).lower() for value in assertions.get("required_outcomes", [])}
    if required_outcomes:
        actual_outcomes = set(_attribute_values(trace, _OUTCOME_ATTRIBUTE_KEYS))
        actual_outcomes.update(
            "success" if span.status == "OK" else "failure" for span in trace.spans if span.status in {"OK", "ERROR"}
        )
        if _has_retry_attempt(trace):
            actual_outcomes.add("retry")
        missing = required_outcomes - actual_outcomes
        if missing:
            errors.append("Missing operation outcome(s): " + ", ".join(sorted(missing)))

    if assertions.get("require_continuation", False):
        span_ids = {span.span_id for span in trace.spans}
        linked = any(
            span.parent_span_id in span_ids
            or any(
                normalize_id(link.trace_id, 32) == trace.trace_id and link.span_id in span_ids for link in span.links
            )
            for span in trace.spans
        )
        if not linked:
            errors.append("No parent or span-link relationship connects the durable continuation")

    if assertions.get("require_log_trace_correlation", False):
        if not trace.log_trace_ids:
            errors.append("No log trace identifiers were returned by the backend")
        mismatched = {
            normalize_id(trace_id, 32)
            for trace_id in trace.log_trace_ids
            if normalize_id(trace_id, 32) != trace.trace_id
        }
        if mismatched:
            errors.append("Log trace identifiers do not match the active trace")

    errors.extend(_span_assertion_errors(trace, assertions.get("span_assertions")))

    return errors
