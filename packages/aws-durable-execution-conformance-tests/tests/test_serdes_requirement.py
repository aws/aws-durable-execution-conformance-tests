# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Regression tests for the filesystem SerDes conformance requirement."""

from __future__ import annotations

import json
from typing import Any

import yaml

from aws_durable_execution_conformance_tests.config import TESTS_DIR
from aws_durable_execution_conformance_tests.history import EventHistoryMatcher

_EXPECTED_CHECKSUM = "3ae2b07fb25ce0c7057d0b08c9b89e96aacf499b1a666c11e5dfe4d9b29544de"


def _payload_expectation(event_id: int) -> tuple[dict[str, Any], str]:
    requirement = yaml.safe_load((TESTS_DIR / "serdes" / "11-1.yaml").read_text())
    event = next(item for item in requirement["ExpectedExecutionHistory"] if item["EventId"] == event_id)
    details_key = "ExecutionSucceededDetails" if event_id == 10 else "StepSucceededDetails"
    return {
        "EventId": event_id,
        details_key: {"Result": {"Payload": event[details_key]["Result"]["Payload"]}},
    }, details_key


def _matches(event_id: int, payload: Any) -> bool:
    expected, details_key = _payload_expectation(event_id)
    actual = {"EventId": event_id, details_key: {"Result": {"Payload": payload}}}
    return EventHistoryMatcher().match([expected], [actual]).success


def _envelope(preview: dict[str, Any], *, file: Any = "/mnt/efs/payload.json") -> dict[str, Any]:
    return {
        "preview": preview,
        "file": file,
        "payloadDigest": "a" * 64,
        "payloadType": "UTF8",
        "ownerEntityId": "entity-1",
        "ownerDurableExecutionArn": "arn:aws:lambda:us-west-2:123:function:fn:1/durable-execution/run/id",
        "__durable_execution_filesystem_serdes": 1,
    }


def test_filesystem_serdes_payload_matchers_accept_valid_envelopes() -> None:
    previews = {
        3: {
            "operationName": "store-payload",
            "length": 41,
            "payloadKind": "RESULT",
            "id": "payload-1",
        },
        8: {
            "checksum": _EXPECTED_CHECKSUM,
            "id": "payload-1",
            "payloadKind": "RESULT",
            "length": 41,
            "operationName": "verify-payload",
        },
        10: {
            "id": "payload-1",
            "checksum": _EXPECTED_CHECKSUM,
            "length": 41,
            "payloadKind": "OUTPUT",
        },
    }

    for event_id, preview in previews.items():
        assert _matches(event_id, json.dumps(_envelope(preview)))


def test_filesystem_serdes_payload_matchers_reject_invalid_envelopes() -> None:
    required_preview = {
        "payloadKind": "RESULT",
        "operationName": "store-payload",
        "id": "payload-1",
        "length": 41,
    }

    assert not _matches(3, json.dumps(_envelope(required_preview, file="")))
    assert not _matches(3, json.dumps(_envelope(required_preview, file=None)))
    assert not _matches(3, '{"file": "/mnt/efs/payload.json", "preview":')
    assert not _matches(
        3,
        json.dumps(
            {
                **_envelope({}),
                **required_preview,
            }
        ),
    )


def test_filesystem_serdes_verification_requires_original_payload_checksum() -> None:
    wrong_preview = {
        "payloadKind": "RESULT",
        "operationName": "verify-payload",
        "id": "payload-1",
        "length": 41,
        "checksum": "0" * 64,
    }

    assert not _matches(8, json.dumps(_envelope(wrong_preview)))
    wrong_preview["payloadKind"] = "OUTPUT"
    wrong_preview.pop("operationName")
    assert not _matches(10, json.dumps(_envelope(wrong_preview)))
