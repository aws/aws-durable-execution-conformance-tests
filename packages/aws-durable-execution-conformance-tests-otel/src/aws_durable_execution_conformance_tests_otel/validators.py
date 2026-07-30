# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Stable, provider-neutral OpenTelemetry conformance assertions."""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from typing import Any

from aws_durable_execution_conformance_tests.history import get_regex_pattern
from aws_durable_execution_conformance_tests_otel.model import (
    Span,
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
_DURABLE_INVOCATION_ATTRIBUTE_KEYS = ("durable.invocation.first",)
_TEMPORAL_RELATION_KEYS = ("before", "after", "inside")


def _attribute_values(
    trace: Trace,
    keys: tuple[str, ...],
) -> list[str]:
    return [str(span.attributes[key]).lower() for span in trace.spans for key in keys if key in span.attributes]


def _invocation_count(trace: Trace) -> int:
    return sum(
        1
        for span in trace.spans
        if span.name.lower() == "invocation"
        and all(key in span.attributes for key in _DURABLE_INVOCATION_ATTRIBUTE_KEYS)
    )


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _matches(expected: Any, actual: Any) -> bool:
    if expected == "*":
        return True
    if pattern := get_regex_pattern(expected):
        return bool(pattern.search(str(actual)))
    if isinstance(expected, Mapping) and set(expected) == {"$any_of"}:
        alternatives = expected["$any_of"]
        return _is_sequence(alternatives) and any(_matches(alternative, actual) for alternative in alternatives)
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
        BackendFeatureDisparity.UNSET_STATUS in feature_disparities and actual == "OK" and _matches(expected, "UNSET")
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


def _select_span_matches(
    selector: Mapping[str, Any],
    spans: Sequence[Mapping[str, Any]],
    used_span_indexes: Collection[int],
    expected_count: int,
    feature_disparities: Collection[BackendFeatureDisparity],
) -> list[tuple[int, Mapping[str, Any]]]:
    def allocate_status_matches(
        matches: list[tuple[int, Mapping[str, Any]]],
    ) -> list[tuple[int, Mapping[str, Any]]]:
        if (
            BackendFeatureDisparity.UNSET_STATUS not in feature_disparities
            or len(matches) <= expected_count
            or selector.get("status") not in {"UNSET", "OK"}
        ):
            return matches
        ordered = sorted(
            matches,
            key=lambda match: (
                match[1]["start_time"],
                match[1]["end_time"],
                match[1]["span_id"],
            ),
            reverse=selector["status"] == "OK",
        )
        return ordered[:expected_count]

    exact_matches = [
        (span_index, span)
        for span_index, span in enumerate(spans)
        if span_index not in used_span_indexes and _matches_span(selector, span, ())
    ]
    if exact_matches or BackendFeatureDisparity.UNSET_STATUS not in feature_disparities:
        return allocate_status_matches(exact_matches)

    # Status normalization can collapse distinct UNSET and OK selectors. Allocate
    # the earliest span to UNSET and leave later spans for OK selectors.
    fallback_matches = [
        (span_index, span)
        for span_index, span in enumerate(spans)
        if span_index not in used_span_indexes and _matches_span(selector, span, feature_disparities)
    ]
    return allocate_status_matches(fallback_matches)


def _expectation_errors(
    expected: Any,
    actual: Any,
    *,
    path: str,
) -> list[str]:
    if expected == "*":
        return []
    if pattern := get_regex_pattern(expected):
        if pattern.search(str(actual)):
            return []
        return [f"{path}: value {actual!r} does not match regex pattern {pattern.pattern!r}"]
    if isinstance(expected, Mapping) and set(expected) == {"$any_of"}:
        alternatives = expected["$any_of"]
        if not _is_sequence(alternatives) or not alternatives:
            return [f"{path}.$any_of: expected a non-empty sequence"]
        if any(_matches(alternative, actual) for alternative in alternatives):
            return []
        return [f"{path}: value {actual!r} does not match any allowed value"]
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
    span: Span,
    spans_by_id: Mapping[str, list[tuple[Span, Mapping[str, Any]]]],
    *,
    path: str,
    feature_disparities: Collection[BackendFeatureDisparity],
) -> list[str]:
    if not isinstance(expected, Mapping):
        return [f"{path} must be a mapping"]

    allow_outside = expected.get("$allow_outside", False)
    if "$allow_outside" in expected and allow_outside is not True:
        return [f"{path}.$allow_outside must be true"]
    expected_properties = {key: value for key, value in expected.items() if key != "$allow_outside"}

    parent_span_id = span.parent_span_id
    if parent_span_id is None:
        return [f"{path}: selected span has no parent"]

    parents = [parent for parent in spans_by_id.get(parent_span_id, []) if parent[0].trace_id == span.trace_id]
    if not parents:
        return [f"{path}: parent span is not present in the trace"]

    expectation_errors = [
        _span_expectation_errors(
            expected_properties,
            serialized_parent,
            path=path,
            feature_disparities=feature_disparities,
        )
        for _parent, serialized_parent in parents
    ]
    matching_parents = [
        parent for (parent, _serialized_parent), errors in zip(parents, expectation_errors, strict=True) if not errors
    ]
    if not matching_parents:
        if len(parents) > 1:
            return [f"{path}: parent span id matched {len(parents)} spans; none matched the expected parent"]
        return expectation_errors[0]

    if allow_outside:
        return []

    candidate_errors = []
    for parent in matching_parents:
        errors: list[str] = []
        if span.start_time < parent.start_time:
            errors.append(
                f"{path}: child span {span.name!r} ({span.span_id}) starts at {span.start_time.isoformat()}, "
                f"before parent span {parent.name!r} ({parent.span_id}) starts at {parent.start_time.isoformat()}"
            )
        if span.end_time > parent.end_time:
            errors.append(
                f"{path}: child span {span.name!r} ({span.span_id}) ends at {span.end_time.isoformat()}, "
                f"after parent span {parent.name!r} ({parent.span_id}) ends at {parent.end_time.isoformat()}"
            )
        candidate_errors.append(errors)
    if any(not errors for errors in candidate_errors):
        return []
    if len(matching_parents) > 1:
        return [
            f"{path}: parent span id matched {len(matching_parents)} expected spans; none contained the child timespan"
        ]
    return candidate_errors[0]


def _link_expectation_errors(
    expected: Any,
    span: Mapping[str, Any],
    spans: Sequence[Mapping[str, Any]],
    spans_by_id: Mapping[str, list[Mapping[str, Any]]],
    *,
    path: str,
    feature_disparities: Collection[BackendFeatureDisparity],
) -> list[str]:
    if BackendFeatureDisparity.SPAN_LINKS in feature_disparities:
        return []

    if isinstance(expected, Mapping) and set(expected) == {"$any_of"}:
        alternatives = expected["$any_of"]
        if not _is_sequence(alternatives) or not alternatives:
            return [f"{path}.$any_of: expected a non-empty sequence"]
        if any(
            not _link_expectation_errors(
                alternative,
                span,
                spans,
                spans_by_id,
                path=path,
                feature_disparities=feature_disparities,
            )
            for alternative in alternatives
        ):
            return []
        return [f"{path}: linked spans do not match any allowed value"]

    if not _is_sequence(expected):
        return [f"{path} must be a sequence"]

    links = span["links"]
    if len(expected) != len(links):
        return [f"{path}: expected {len(expected)} item(s), found {len(links)}"]

    errors: list[str] = []
    for index, (expected_span, link) in enumerate(zip(expected, links, strict=True)):
        link_path = f"{path}[{index}]"
        if not isinstance(expected_span, Mapping):
            errors.append(f"{link_path} must be a mapping")
            continue

        expected_count = expected_span.get("count", 1)
        if isinstance(expected_count, bool) or not isinstance(expected_count, int) or expected_count < 1:
            errors.append(f"{link_path}.count must be a positive integer")
            continue
        expected_occurrence = expected_span.get("$occurrence")
        if expected_occurrence is not None and (
            isinstance(expected_occurrence, bool) or not isinstance(expected_occurrence, int) or expected_occurrence < 1
        ):
            errors.append(f"{link_path}.$occurrence must be a positive integer")
            continue
        expected_properties = {
            key: value for key, value in expected_span.items() if key not in {"$occurrence", "count"}
        }
        linked_spans = [
            candidate for candidate in spans_by_id.get(link["span_id"], []) if candidate["trace_id"] == link["trace_id"]
        ]
        if not linked_spans:
            errors.append(f"{link_path}: linked span is not present in the trace")
            continue

        expectation_errors = [
            _span_expectation_errors(
                expected_properties,
                linked_span,
                path=link_path,
                feature_disparities=feature_disparities,
            )
            for linked_span in linked_spans
        ]
        matching_count = sum(not candidate_errors for candidate_errors in expectation_errors)
        if matching_count == expected_count:
            if expected_occurrence is not None:
                candidates = {
                    (candidate["trace_id"], candidate["span_id"]): candidate
                    for candidate in spans
                    if _matches_span(expected_properties, candidate, feature_disparities)
                }
                ordered_candidates = sorted(
                    candidates,
                    key=lambda key: (
                        candidates[key]["start_time"],
                        candidates[key]["end_time"],
                        key[0],
                        key[1],
                    ),
                )
                linked_key = (link["trace_id"], link["span_id"])
                actual_occurrence = ordered_candidates.index(linked_key) + 1
                if actual_occurrence != expected_occurrence:
                    errors.append(
                        f"{link_path}.$occurrence: linked span is occurrence "
                        f"{actual_occurrence}, expected {expected_occurrence}"
                    )
            continue
        if len(linked_spans) == 1 and expected_count == 1:
            errors.extend(expectation_errors[0])
            continue
        errors.append(f"{link_path}: linked span expectation matched {matching_count} spans; expected {expected_count}")
    return errors


def _temporal_relation_errors(
    relation: str,
    expected: Any,
    selected_span: Span,
    selected_span_index: int,
    trace: Trace,
    spans: Sequence[Mapping[str, Any]],
    *,
    path: str,
    feature_disparities: Collection[BackendFeatureDisparity],
) -> list[str]:
    if not isinstance(expected, Mapping):
        return [f"{path} must be a mapping"]

    linked_only = expected.get("$linked", False)
    if "$linked" in expected and linked_only is not True:
        return [f"{path}.$linked must be true"]
    if linked_only and BackendFeatureDisparity.SPAN_LINKS in feature_disparities:
        return []
    selector = {key: value for key, value in expected.items() if key != "$linked"}
    linked_span_keys = {(link.trace_id, link.span_id) for link in selected_span.links}
    matches = [
        span
        for span_index, (span, serialized_span) in enumerate(zip(trace.spans, spans, strict=True))
        if span_index != selected_span_index
        and (not linked_only or (span.trace_id, span.span_id) in linked_span_keys)
        and _matches_span(selector, serialized_span, feature_disparities)
    ]
    if not matches:
        return [f"{path} matched no spans"]
    if len(matches) > 1:
        return [f"{path} matched {len(matches)} spans; it must select exactly one"]

    related_span = matches[0]
    selected_description = f"{selected_span.name!r} ({selected_span.span_id})"
    related_description = f"{related_span.name!r} ({related_span.span_id})"
    if relation == "before" and selected_span.end_time > related_span.start_time:
        return [
            f"{path}: span {selected_description} ends at {selected_span.end_time.isoformat()}, "
            f"after span {related_description} starts at {related_span.start_time.isoformat()}"
        ]
    if relation == "after" and selected_span.start_time < related_span.end_time:
        return [
            f"{path}: span {selected_description} starts at {selected_span.start_time.isoformat()}, "
            f"before span {related_description} ends at {related_span.end_time.isoformat()}"
        ]
    if relation == "inside":
        errors = []
        if selected_span.start_time < related_span.start_time:
            errors.append(
                f"{path}: span {selected_description} starts at {selected_span.start_time.isoformat()}, "
                f"before containing span {related_description} starts at {related_span.start_time.isoformat()}"
            )
        if selected_span.end_time > related_span.end_time:
            errors.append(
                f"{path}: span {selected_description} ends at {selected_span.end_time.isoformat()}, "
                f"after containing span {related_description} ends at {related_span.end_time.isoformat()}"
            )
        return errors
    return []


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
    span_models_by_id: dict[str, list[tuple[Span, Mapping[str, Any]]]] = {}
    for span, serialized_span in zip(trace.spans, spans, strict=True):
        spans_by_id.setdefault(span.span_id, []).append(serialized_span)
        span_models_by_id.setdefault(span.span_id, []).append((span, serialized_span))

    errors: list[str] = []
    covered_span_indexes: set[int] = set()
    used_span_indexes: set[int] = set()
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
        raw_expected_count = assertion.get("count", 1)
        if not isinstance(selector, Mapping):
            errors.append(f"{path}.select must be a mapping")
            continue
        if not isinstance(expected, Mapping):
            errors.append(f"{path}.expect must be a mapping")
            continue
        if isinstance(raw_expected_count, int) and not isinstance(raw_expected_count, bool) and raw_expected_count > 0:
            expected_counts = (raw_expected_count,)
        elif (
            isinstance(raw_expected_count, Mapping)
            and set(raw_expected_count) == {"$any_of"}
            and _is_sequence(raw_expected_count["$any_of"])
            and raw_expected_count["$any_of"]
            and all(
                isinstance(count, int) and not isinstance(count, bool) and count > 0
                for count in raw_expected_count["$any_of"]
            )
        ):
            expected_counts = tuple(dict.fromkeys(raw_expected_count["$any_of"]))
        else:
            errors.append(f"{path}.count must be a positive integer or $any_of positive integers")
            continue

        matches = _select_span_matches(
            selector,
            spans,
            used_span_indexes,
            max(expected_counts),
            feature_disparities,
        )
        if not matches and expected_counts == (1,):
            errors.append(f"{path}.select matched no spans")
            continue
        if len(matches) > 1 and expected_counts == (1,):
            errors.append(f"{path}.select matched {len(matches)} spans; it must select exactly one")
            continue
        if len(matches) not in expected_counts:
            allowed_counts = " or ".join(str(count) for count in expected_counts)
            errors.append(f"{path}.select matched {len(matches)} spans; expected {allowed_counts}")
            continue
        matched_span_indexes = {span_index for span_index, _span in matches}
        covered_span_indexes.update(matched_span_indexes)
        used_span_indexes.update(matched_span_indexes)

        expected_properties = {
            key: value
            for key, value in expected.items()
            if key not in {"links", "parent"} and key not in _TEMPORAL_RELATION_KEYS
        }
        expected_attributes = expected.get("attributes")
        for match_index, (span_index, matched_span) in enumerate(matches):
            expectation_path = f"{path}.expect"
            if len(matches) > 1:
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
                        trace.spans[span_index],
                        span_models_by_id,
                        path=f"{expectation_path}.parent",
                        feature_disparities=feature_disparities,
                    )
                )
            if "links" in expected:
                errors.extend(
                    _link_expectation_errors(
                        expected["links"],
                        matched_span,
                        spans,
                        spans_by_id,
                        path=f"{expectation_path}.links",
                        feature_disparities=feature_disparities,
                    )
                )
            for relation in _TEMPORAL_RELATION_KEYS:
                if relation in expected:
                    errors.extend(
                        _temporal_relation_errors(
                            relation,
                            expected[relation],
                            trace.spans[span_index],
                            span_index,
                            trace,
                            spans,
                            path=f"{expectation_path}.{relation}",
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

    for span in trace.spans:
        if span.start_time > span.end_time:
            errors.append(
                f"Span {span.name!r} ({span.span_id}) starts at {span.start_time.isoformat()}, "
                f"after it ends at {span.end_time.isoformat()}"
            )

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
        invocation_count = _invocation_count(trace)
        if invocation_count < minimum_invocations:
            errors.append(
                f"Expected at least {minimum_invocations} canonical durable invocation spans, found {invocation_count}"
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
