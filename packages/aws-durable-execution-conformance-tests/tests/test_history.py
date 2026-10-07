# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Exact event-field absence and compatibility with existing history matching."""

import pytest

from aws_durable_execution_conformance_tests.history import EventHistoryMatcher


def _expected_event() -> dict:
    return {
        "EventId": 7,
        "EventType": "CallbackFailed",
        "$absent": [["CallbackFailedDetails", "Error"]],
    }


@pytest.mark.parametrize("details", [{}, {"CallbackFailedDetails": {}}])
def test_absent_error_accepts_missing_parent_or_key(details: dict) -> None:
    actual = {"EventId": 7, "EventType": "CallbackFailed", **details}
    assert EventHistoryMatcher().match([_expected_event()], [actual]).success


@pytest.mark.parametrize("error", [None, {}, {"Payload": {"ErrorType": "RejectedError"}}])
def test_absent_error_rejects_every_present_value(error: object) -> None:
    actual = {"EventId": 7, "EventType": "CallbackFailed", "CallbackFailedDetails": {"Error": error}}
    result = EventHistoryMatcher().match([_expected_event()], [actual])
    assert not result.success
    assert result.errors == ["Event[EventId=7].CallbackFailedDetails.Error: expected field to be absent"]


@pytest.mark.parametrize("details", [None, [], "invalid"])
def test_absent_error_rejects_non_object_intermediate(details: object) -> None:
    actual = {"EventId": 7, "EventType": "CallbackFailed", "CallbackFailedDetails": details}
    result = EventHistoryMatcher().match([_expected_event()], [actual])
    assert not result.success
    assert "expected an object" in result.errors[0]


def test_absence_still_requires_the_expected_event() -> None:
    result = EventHistoryMatcher().match([_expected_event()], [])
    assert not result.success
    assert result.errors == ["No actual event found for EventId=7"]


@pytest.mark.parametrize("paths", [None, True, [], "Error", ["Error"], [[]], [[""]], [[1]], [[True]]])
def test_invalid_absence_control_fails_even_when_event_is_missing(paths: object) -> None:
    expected = {**_expected_event(), "$absent": paths}
    result = EventHistoryMatcher().match([expected], [])
    assert not result.success
    assert "$absent must be" in result.errors[0]


def test_absence_does_not_skip_positive_assertions_or_other_paths() -> None:
    expected = {**_expected_event(), "$absent": [["CallbackFailedDetails", "Error"], ["Unexpected"]]}
    result = EventHistoryMatcher().match(
        [expected], [{"EventId": 7, "EventType": "CallbackSucceeded", "Unexpected": 1}]
    )
    assert not result.success
    assert len(result.errors) == 2
    assert any("Unexpected: expected field to be absent" in error for error in result.errors)
    assert any("EventType: expected 'CallbackFailed'" in error for error in result.errors)


def test_null_and_empty_dictionary_semantics_are_unchanged() -> None:
    matcher = EventHistoryMatcher()
    assert matcher.match_value({"Error": None}, {"Error": None}).success
    assert not matcher.match_value({"Error": None}, {}).success
    assert not matcher.match_value({"Error": None}, {"Error": {}}).success
    assert matcher.match_value({"Error": {}}, {"Error": {"Payload": "ignored"}}).success
    assert matcher.match_value({"Error": {}}, {"Error": None}).success
    assert not matcher.match_value({"Error": {}}, {}).success


def test_absence_control_is_literal_in_payloads_and_match_value() -> None:
    literal = {"$absent": [["Error"]]}
    matcher = EventHistoryMatcher()
    assert matcher.match_value(literal, literal).success
    assert not matcher.match_value(literal, {}).success
    expected = {"EventId": 7, "Payload": literal}
    assert matcher.match([expected], [expected]).success
    assert not matcher.match([expected], [{"EventId": 7, "Payload": {}}]).success
