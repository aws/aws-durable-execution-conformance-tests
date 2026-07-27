# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the event-history matcher.

Focus is the two directives added so the converged, customer-facing DAG
container envelope can be pinned without encoding language-specific divergence:

* ``${JSON}`` -- JSON-decode an opaque payload string, then match its fields.
* ``${ABSENT}`` -- assert a mapping key is NOT present (e.g. the offloaded
  envelope drops ``tasks``; the convergence dropped ``completedCount`` etc.).

A few pre-existing rules (``*``, literal equality, extra-key tolerance) are
re-checked so the extension cannot silently regress them.
"""

from __future__ import annotations

import json
from typing import Any

from aws_durable_execution_conformance_tests.history import (
    EventHistoryMatcher,
    get_json_decode_spec,
    is_absent_directive,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _match(expected_details: Any, actual_details: Any) -> list[str]:
    """Match a single ContextSucceeded event's details, return errors.

    Wraps both sides in a minimal EventId-keyed event so the public
    ``EventHistoryMatcher.match`` path (not a private method) is exercised.
    """
    expected = [
        {"EventType": "ContextSucceeded", "EventId": 19, "ContextSucceededDetails": expected_details}
    ]
    actual = [
        {"EventType": "ContextSucceeded", "EventId": 19, "ContextSucceededDetails": actual_details}
    ]
    return EventHistoryMatcher().match(expected, actual).errors


def _payload(expected_spec: Any) -> dict:
    """Expected details pinning Result.Payload via the ${JSON} directive."""
    return {"Result": {"Payload": {"${JSON}": expected_spec}}}


def _actual_payload(envelope: Any) -> dict:
    """Actual details carrying the envelope as an opaque JSON string."""
    return {"Result": {"Payload": json.dumps(envelope)}}


def _inline_envelope(*, failed_result_kind: Any, skipped_result_kind: Any) -> dict:
    """A converged inline envelope shaped like 10-2 (success + failure + skip)."""
    return {
        "type": "DagResult",
        "totalCount": 4,
        "successCount": 2,
        "failureCount": 1,
        "skippedCount": 1,
        "completionReason": "COMPLETED_WITH_FAILURES",
        "startedTaskNames": [],
        "failedTaskNames": ["charge"],
        "tasks": [
            {
                "name": "charge",
                "status": "FAILED",
                "skipReason": None,
                "resultKind": failed_result_kind,
                "result": None,
                "error": {"ErrorType": "StepError", "ErrorMessage": "payment declined", "StackTrace": None},
                "startedAt": "2026-07-26T03:19:02.010Z",
                "completedAt": "2026-07-26T03:19:02.140Z",
            },
            {
                "name": "fulfill",
                "status": "SKIPPED",
                "skipReason": "TRIGGER_RULE",
                "resultKind": skipped_result_kind,
                "result": None,
                "error": None,
                "startedAt": None,
                "completedAt": None,
            },
            {
                "name": "refund",
                "status": "SUCCEEDED",
                "skipReason": None,
                "resultKind": "plain",
                "result": "refunded",
                "error": None,
                "startedAt": "2026-07-26T03:19:02.200Z",
                "completedAt": "2026-07-26T03:19:02.300Z",
            },
            {
                "name": "audit",
                "status": "SUCCEEDED",
                "skipReason": None,
                "resultKind": "plain",
                "result": "logged",
                "error": None,
                "startedAt": "2026-07-26T03:19:02.400Z",
                "completedAt": "2026-07-26T03:19:02.500Z",
            },
        ],
    }


# The 10-2 expected spec: field names / statuses / counts pinned; timestamps,
# resultKind-on-non-succeeded and the language-specific error values wildcarded.
_EXPECTED_10_2_ENVELOPE = {
    "type": "DagResult",
    "totalCount": 4,
    "successCount": 2,
    "failureCount": 1,
    "skippedCount": 1,
    "completionReason": "COMPLETED_WITH_FAILURES",
    "startedTaskNames": [],
    "failedTaskNames": ["charge"],
    "completedCount": "${ABSENT}",
    "terminalTaskNames": "${ABSENT}",
    "summary": "${ABSENT}",
    "tasks": [
        {
            "name": "charge",
            "status": "FAILED",
            "skipReason": None,
            "resultKind": "*",
            "result": None,
            "error": {"ErrorType": "*", "ErrorMessage": "*", "StackTrace": "*"},
            "startedAt": "*",
            "completedAt": "*",
        },
        {
            "name": "fulfill",
            "status": "SKIPPED",
            "skipReason": "TRIGGER_RULE",
            "resultKind": "*",
            "result": None,
            "error": None,
            "startedAt": None,
            "completedAt": None,
        },
        {
            "name": "refund",
            "status": "SUCCEEDED",
            "skipReason": None,
            "resultKind": "plain",
            "result": "refunded",
            "error": None,
            "startedAt": "*",
            "completedAt": "*",
        },
        {
            "name": "audit",
            "status": "SUCCEEDED",
            "skipReason": None,
            "resultKind": "plain",
            "result": "logged",
            "error": None,
            "startedAt": "*",
            "completedAt": "*",
        },
    ],
}


# ---------------------------------------------------------------------------
# Directive helpers (pure)
# ---------------------------------------------------------------------------


def test_get_json_decode_spec_recognises_directive() -> None:
    assert get_json_decode_spec({"${JSON}": {"a": 1}}) == {"a": 1}


def test_get_json_decode_spec_allows_falsy_wrapped_spec() -> None:
    # A wrapped spec that is itself falsy ({}, None) must still be detected as a
    # directive, not confused with "no directive".
    assert get_json_decode_spec({"${JSON}": {}}) == {}
    assert get_json_decode_spec({"${JSON}": None}) is None


def test_get_json_decode_spec_rejects_non_directive() -> None:
    from aws_durable_execution_conformance_tests.history import _NO_JSON_SPEC

    assert get_json_decode_spec({"a": 1}) is _NO_JSON_SPEC
    assert get_json_decode_spec({"${JSON}": 1, "extra": 2}) is _NO_JSON_SPEC
    assert get_json_decode_spec("plain") is _NO_JSON_SPEC


def test_is_absent_directive() -> None:
    assert is_absent_directive("${ABSENT}") is True
    assert is_absent_directive("*") is False
    assert is_absent_directive(None) is False
    assert is_absent_directive({"${ABSENT}": 1}) is False


# ---------------------------------------------------------------------------
# ${JSON} decode directive
# ---------------------------------------------------------------------------


def test_json_directive_decodes_and_matches() -> None:
    errors = _match(
        _payload({"type": "DagResult", "totalCount": 8}),
        _actual_payload({"type": "DagResult", "totalCount": 8, "extra": "ignored"}),
    )
    assert errors == []


def test_json_directive_reports_nested_mismatch() -> None:
    errors = _match(
        _payload({"totalCount": 8}),
        _actual_payload({"totalCount": 7}),
    )
    assert len(errors) == 1
    assert "totalCount" in errors[0]


def test_json_directive_requires_string_actual() -> None:
    # Actual Payload already decoded to a dict is a contract error: the wire form
    # is a string, so a non-string signals the event shape changed.
    errors = _match(
        _payload({"totalCount": 8}),
        {"Result": {"Payload": {"totalCount": 8}}},
    )
    assert len(errors) == 1
    assert "JSON string" in errors[0]


def test_json_directive_reports_invalid_json() -> None:
    errors = _match(
        _payload({"totalCount": 8}),
        {"Result": {"Payload": "{not valid json"}},
    )
    assert len(errors) == 1
    assert "not valid JSON" in errors[0]


# ---------------------------------------------------------------------------
# ${ABSENT} directive
# ---------------------------------------------------------------------------


def test_absent_directive_passes_when_key_missing() -> None:
    errors = _match(
        _payload({"type": "DagResult", "tasks": "${ABSENT}"}),
        _actual_payload({"type": "DagResult"}),
    )
    assert errors == []


def test_absent_directive_fails_when_key_present() -> None:
    errors = _match(
        _payload({"type": "DagResult", "tasks": "${ABSENT}"}),
        _actual_payload({"type": "DagResult", "tasks": []}),
    )
    assert len(errors) == 1
    assert "ABSENT" in errors[0]
    assert "tasks" in errors[0]


def test_absent_directive_works_outside_json_directive() -> None:
    # ${ABSENT} is a generic mapping-value directive, usable on any object.
    expected = {"ContextSucceededDetails": {"Result": {"Truncated": "${ABSENT}"}}}
    ok = EventHistoryMatcher().match(
        [{"EventId": 1, **expected}],
        [{"EventId": 1, "ContextSucceededDetails": {"Result": {"Payload": "x"}}}],
    )
    assert ok.errors == []


# ---------------------------------------------------------------------------
# 10-2 inline envelope: the recorded resultKind divergence must NOT fail
# ---------------------------------------------------------------------------


def test_10_2_envelope_matches_when_non_succeeded_result_kind_is_null() -> None:
    # JS emits resultKind null for FAILED/SKIPPED tasks.
    errors = _match(
        _payload(_EXPECTED_10_2_ENVELOPE),
        _actual_payload(_inline_envelope(failed_result_kind=None, skipped_result_kind=None)),
    )
    assert errors == []


def test_10_2_envelope_matches_when_non_succeeded_result_kind_is_plain() -> None:
    # Python/Java/Go emit resultKind "plain" for FAILED/SKIPPED tasks. Both must
    # pass because resultKind on non-succeeded tasks is wildcarded.
    errors = _match(
        _payload(_EXPECTED_10_2_ENVELOPE),
        _actual_payload(_inline_envelope(failed_result_kind="plain", skipped_result_kind="plain")),
    )
    assert errors == []


def test_10_2_envelope_matches_java_error_type() -> None:
    # ErrorType is wildcarded, so Java's thrown-class ErrorType also passes.
    envelope = _inline_envelope(failed_result_kind="plain", skipped_result_kind="plain")
    envelope["tasks"][0]["error"] = {
        "ErrorType": "java.lang.RuntimeException",
        "ErrorMessage": "payment declined",
        "StackTrace": ["com.example.Orders.charge(Orders.java:42)"],
    }
    errors = _match(_payload(_EXPECTED_10_2_ENVELOPE), _actual_payload(envelope))
    assert errors == []


def test_10_2_envelope_fails_on_wrong_status() -> None:
    envelope = _inline_envelope(failed_result_kind="plain", skipped_result_kind="plain")
    envelope["tasks"][0]["status"] = "SUCCEEDED"  # charge did NOT succeed
    errors = _match(_payload(_EXPECTED_10_2_ENVELOPE), _actual_payload(envelope))
    assert len(errors) == 1
    assert "status" in errors[0]


def test_10_2_envelope_fails_on_wrong_count() -> None:
    envelope = _inline_envelope(failed_result_kind="plain", skipped_result_kind="plain")
    envelope["successCount"] = 3
    errors = _match(_payload(_EXPECTED_10_2_ENVELOPE), _actual_payload(envelope))
    assert any("successCount" in e for e in errors)


def test_10_2_envelope_fails_if_dropped_field_reappears() -> None:
    # A regression that re-adds a convergence-deleted field is caught by ${ABSENT}.
    envelope = _inline_envelope(failed_result_kind="plain", skipped_result_kind="plain")
    envelope["summary"] = "2/4 succeeded"
    errors = _match(_payload(_EXPECTED_10_2_ENVELOPE), _actual_payload(envelope))
    assert any("summary" in e and "ABSENT" in e for e in errors)


def test_10_2_envelope_fails_on_missing_error_key() -> None:
    # camelCase regression (errorType instead of ErrorType) => ErrorType missing.
    envelope = _inline_envelope(failed_result_kind="plain", skipped_result_kind="plain")
    envelope["tasks"][0]["error"] = {
        "errorType": "StepError",
        "errorMessage": "payment declined",
        "stackTrace": None,
    }
    errors = _match(_payload(_EXPECTED_10_2_ENVELOPE), _actual_payload(envelope))
    assert any("ErrorType" in e and "missing" in e for e in errors)


# ---------------------------------------------------------------------------
# 10-15 offloaded envelope
# ---------------------------------------------------------------------------

_EXPECTED_10_15_OFFLOADED = {
    "type": "DagResult",
    "totalCount": 8,
    "successCount": 8,
    "failureCount": 0,
    "skippedCount": 0,
    "completionReason": "ALL_COMPLETED",
    "startedTaskNames": [],
    "failedTaskNames": [],
    "tasks": "${ABSENT}",
    "completedCount": "${ABSENT}",
    "terminalTaskNames": "${ABSENT}",
    "summary": "${ABSENT}",
}


def test_10_15_offloaded_envelope_matches_without_tasks() -> None:
    actual = {
        "type": "DagResult",
        "totalCount": 8,
        "successCount": 8,
        "failureCount": 0,
        "skippedCount": 0,
        "completionReason": "ALL_COMPLETED",
        "startedTaskNames": [],
        "failedTaskNames": [],
    }
    errors = _match(_payload(_EXPECTED_10_15_OFFLOADED), _actual_payload(actual))
    assert errors == []


def test_10_15_offloaded_envelope_fails_if_tasks_present() -> None:
    # If tasks is present the payload is the INLINE case, not the offloaded one.
    actual = {
        "type": "DagResult",
        "totalCount": 8,
        "successCount": 8,
        "failureCount": 0,
        "skippedCount": 0,
        "completionReason": "ALL_COMPLETED",
        "startedTaskNames": [],
        "failedTaskNames": [],
        "tasks": [{"name": "p1", "status": "SUCCEEDED"}],
    }
    errors = _match(_payload(_EXPECTED_10_15_OFFLOADED), _actual_payload(actual))
    assert any("tasks" in e and "ABSENT" in e for e in errors)


# ---------------------------------------------------------------------------
# Pre-existing rules still hold
# ---------------------------------------------------------------------------


def test_wildcard_and_literal_still_work() -> None:
    ok = EventHistoryMatcher().match(
        [{"EventId": 2, "Name": "*", "SubType": "Dag"}],
        [{"EventId": 2, "Name": "anything", "SubType": "Dag"}],
    )
    assert ok.errors == []

    bad = EventHistoryMatcher().match(
        [{"EventId": 2, "SubType": "Dag"}],
        [{"EventId": 2, "SubType": "Step"}],
    )
    assert bad.errors != []


def test_extra_actual_keys_still_ignored() -> None:
    ok = EventHistoryMatcher().match(
        [{"EventId": 2, "SubType": "Dag"}],
        [{"EventId": 2, "SubType": "Dag", "ParentId": "abc", "Extra": 1}],
    )
    assert ok.errors == []
