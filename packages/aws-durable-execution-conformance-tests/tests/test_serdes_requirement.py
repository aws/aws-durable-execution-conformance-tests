# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Regression tests for the filesystem SerDes conformance requirement."""

from __future__ import annotations

import json
from typing import Any

import yaml

from aws_durable_execution_conformance_tests.config import TESTS_DIR
from aws_durable_execution_conformance_tests.history import get_regex_pattern

_EXPECTED_CHECKSUM = "3ae2b07fb25ce0c7057d0b08c9b89e96aacf499b1a666c11e5dfe4d9b29544de"


def _payload_pattern(event_id: int):
    requirement = yaml.safe_load((TESTS_DIR / "serdes" / "11-1.yaml").read_text())
    event = next(item for item in requirement["ExpectedExecutionHistory"] if item["EventId"] == event_id)
    details_key = "ExecutionSucceededDetails" if event_id == 10 else "StepSucceededDetails"
    pattern = get_regex_pattern(event[details_key]["Result"]["Payload"])
    assert pattern is not None
    return pattern


def _envelope(preview: dict[str, Any], *, file: Any = "/mnt/efs/payload.json") -> str:
    return json.dumps(
        {
            "__durable_execution_filesystem_serdes": 1,
            "ownerDurableExecutionArn": "arn:aws:lambda:us-west-2:123:function:fn:1/durable-execution/run/id",
            "ownerEntityId": "entity-1",
            "payloadType": "UTF8",
            "payloadDigest": "a" * 64,
            "file": file,
            "preview": preview,
        }
    )


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
        assert _payload_pattern(event_id).search(_envelope(preview))


def test_filesystem_serdes_payload_matchers_reject_invalid_envelopes() -> None:
    required_preview = {
        "payloadKind": "RESULT",
        "operationName": "store-payload",
        "id": "payload-1",
        "length": 41,
    }
    pattern = _payload_pattern(3)

    assert not pattern.search(_envelope(required_preview, file=""))
    assert not pattern.search(_envelope(required_preview, file=None))
    assert not pattern.search(
        json.dumps(
            {
                "__durable_execution_filesystem_serdes": 1,
                "ownerDurableExecutionArn": "arn",
                "ownerEntityId": "entity-1",
                "payloadType": "UTF8",
                "payloadDigest": "a" * 64,
                "file": "/mnt/efs/payload.json",
                "preview": {},
                **required_preview,
            }
        )
    )


def test_filesystem_serdes_verification_requires_original_payload_checksum() -> None:
    wrong_preview = {
        "payloadKind": "RESULT",
        "operationName": "verify-payload",
        "id": "payload-1",
        "length": 41,
        "checksum": "0" * 64,
    }

    assert not _payload_pattern(8).search(_envelope(wrong_preview))
