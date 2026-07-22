# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Stable, provider-neutral OpenTelemetry conformance assertions."""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from typing import Any

from aws_durable_execution_conformance_tests_otel.model import (
    TelemetryQuery,
    Trace,
    span_to_dict,
)
from aws_durable_execution_conformance_tests_otel.polling import (
    BackendFeatureDisparity,
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


def _attribute_values(
    trace: Trace,
    keys: tuple[str, ...],
) -> list[str]:
    return [str(span.attributes[key]).lower() for span in trace.spans for key in keys if key in span.attributes]


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


def _matches_span_status(
    expected: Any,
    actual: Any,
    feature_disparities: Collection[BackendFeatureDisparity],
) -> bool:
    return _matches(expected, actual) or (
        BackendFeatureDisparity.UNSET_STATUS in feature_disparities and expected == "UNSET" and actual == "OK"
    )


def _matches_span(
    expected: Mapping[str, Any],
    actual: Mapping[str, Any],
    feature_disparities: Collection[BackendFeatureDisparity],
) -> bool:
    return all(
        key in actual
        and (
            _matches_span_status(value, actual[key], feature_disparities)
            if key == "status"
            else _matches(value, actual[key])
        )
        for key, value in expected.items()
    )


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


def _span_expectation_errors(
    expected: Mapping[str, Any],
    actual: Mapping[str, Any],
    *,
    path: str,
    feature_disparities: Collection[BackendFeatureDisparity],
) -> list[str]:
    errors: list[str] = []
    for key, value in expected.items():
        child_path = f"{path}.{key}"
        if key not in actual:
            errors.append(f"{child_path}: property is missing")
            continue
        if key == "status" and _matches_span_status(value, actual[key], feature_disparities):
            continue
        errors.extend(_expectation_errors(value, actual[key], path=child_path))
    return errors


def _parent_expectation_errors(
    expected: Any,
    span: Mapping[str, Any],
    spans_by_id: Mapping[str, list[Mapping[str, Any]]],
    *,
    path: str,
    feature_disparities: Collection[BackendFeatureDisparity],
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

    return _span_expectation_errors(
        expected,
        parents[0],
        path=path,
        feature_disparities=feature_disparities,
    )


def _span_assertion_errors(
    trace: Trace,
    raw_assertions: Any,
    *,
    require_all_spans: bool = False,
    assertion_scopes: Sequence[Mapping[str, Any]] = (),
    exact_attribute_prefixes: Sequence[str] = (),
    feature_disparities: Collection[BackendFeatureDisparity] = (),
) -> list[str]:
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
    covered_span_indexes: set[int] = set()
    for index, assertion in enumerate(span_assertions):
        path = f"span_assertions[{index}]"
        if not isinstance(assertion, Mapping):
            errors.append(f"{path} must be a mapping")
            continue

        unknown = sorted(set(assertion) - {"select", "expect", "count"}, key=str)
        if unknown:
            errors.append(f"{path} has unknown field(s): {', '.join(str(key) for key in unknown)}")
            continue

        selector = assertion.get("select", {})
        expected = assertion.get("expect")
        expected_count = assertion.get("count", 1)
        if not isinstance(selector, Mapping):
            errors.append(f"{path}.select must be a mapping")
            continue
        if not isinstance(expected, Mapping):
            errors.append(f"{path}.expect must be a mapping")
            continue
        if isinstance(expected_count, bool) or not isinstance(expected_count, int) or expected_count < 1:
            errors.append(f"{path}.count must be a positive integer")
            continue

        matches = [
            (span_index, span)
            for span_index, span in enumerate(spans)
            if _matches_span(selector, span, feature_disparities)
        ]
        covered_span_indexes.update(span_index for span_index, _span in matches)
        if not matches and expected_count == 1:
            errors.append(f"{path}.select matched no spans")
            continue
        if len(matches) > 1 and expected_count == 1:
            errors.append(f"{path}.select matched {len(matches)} spans; it must select exactly one")
            continue
        if len(matches) != expected_count:
            errors.append(f"{path}.select matched {len(matches)} spans; expected {expected_count}")
            continue

        expected_properties = {key: value for key, value in expected.items() if key != "parent"}
        expected_attributes = expected.get("attributes")
        for match_index, (_span_index, matched_span) in enumerate(matches):
            expectation_path = f"{path}.expect"
            if expected_count > 1:
                expectation_path = f"{expectation_path}[{match_index}]"
            errors.extend(
                _span_expectation_errors(
                    expected_properties,
                    matched_span,
                    path=expectation_path,
                    feature_disparities=feature_disparities,
                )
            )
            if "parent" in expected:
                errors.extend(
                    _parent_expectation_errors(
                        expected["parent"],
                        matched_span,
                        spans_by_id,
                        path=f"{expectation_path}.parent",
                        feature_disparities=feature_disparities,
                    )
                )

            if exact_attribute_prefixes and isinstance(expected_attributes, Mapping):
                actual_attributes = matched_span["attributes"]
                for prefix in exact_attribute_prefixes:
                    expected_keys = sorted(str(key) for key in expected_attributes if str(key).startswith(prefix))
                    actual_keys = sorted(str(key) for key in actual_attributes if str(key).startswith(prefix))
                    if expected_keys != actual_keys:
                        errors.append(
                            f"{expectation_path}.attributes: expected exact {prefix!r} "
                            f"attribute keys {expected_keys!r}, found {actual_keys!r}"
                        )

    if require_all_spans:
        uncovered = [
            f"{span['name']} ({span['span_id']})"
            for span_index, span in enumerate(spans)
            if span_index not in covered_span_indexes
            and any(_matches_span(scope, span, feature_disparities) for scope in (assertion_scopes or ({},)))
        ]
        if uncovered:
            errors.append("Span assertions did not cover: " + ", ".join(uncovered))
    return errors


def validate_trace(
    trace: Trace,
    assertions: Mapping[str, Any],
    query: TelemetryQuery,
    *,
    feature_disparities: Collection[BackendFeatureDisparity] = (),
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

        raw_allowed_execution_arns = assertions.get(
            "allowed_execution_arns",
            (query.execution_arn,),
        )
        if isinstance(raw_allowed_execution_arns, str):
            allowed_execution_arns = {raw_allowed_execution_arns.lower()}
        elif _is_sequence(raw_allowed_execution_arns) and all(
            isinstance(value, str) for value in raw_allowed_execution_arns
        ):
            allowed_execution_arns = {value.lower() for value in raw_allowed_execution_arns}
        else:
            errors.append("allowed_execution_arns must be a string or sequence of strings")
            allowed_execution_arns = {query.execution_arn.lower()}

        if set(execution_values) - allowed_execution_arns:
            errors.append("Spans contain durable execution correlation values outside allowed_execution_arns")

    minimum_invocations = int(assertions.get("minimum_invocations", 1))
    if minimum_invocations > 1:
        invocations = set(_attribute_values(trace, _INVOCATION_ATTRIBUTE_KEYS))
        if len(invocations) < minimum_invocations:
            errors.append(
                f"Expected telemetry from at least {minimum_invocations} Lambda invocations, found {len(invocations)}"
            )

    raw_prefixes = assertions.get("exact_attribute_prefixes", ())
    exact_attribute_prefixes: tuple[str, ...]
    if isinstance(raw_prefixes, str):
        exact_attribute_prefixes = (raw_prefixes,)
    elif _is_sequence(raw_prefixes) and all(isinstance(prefix, str) for prefix in raw_prefixes):
        exact_attribute_prefixes = tuple(raw_prefixes)
    else:
        errors.append("exact_attribute_prefixes must be a string or sequence of strings")
        exact_attribute_prefixes = ()

    raw_assertion_scope = assertions.get("span_assertion_scope", {})
    assertion_scopes: tuple[Mapping[str, Any], ...]
    if isinstance(raw_assertion_scope, Mapping):
        assertion_scopes = (raw_assertion_scope,)
    elif _is_sequence(raw_assertion_scope) and all(isinstance(scope, Mapping) for scope in raw_assertion_scope):
        assertion_scopes = tuple(raw_assertion_scope)
    else:
        errors.append("span_assertion_scope must be a mapping or sequence of mappings")
        assertion_scopes = ({},)

    errors.extend(
        _span_assertion_errors(
            trace,
            assertions.get("span_assertions"),
            require_all_spans=bool(assertions.get("require_all_spans", False)),
            assertion_scopes=assertion_scopes,
            exact_attribute_prefixes=exact_attribute_prefixes,
            feature_disparities=feature_disparities,
        )
    )

    return errors
