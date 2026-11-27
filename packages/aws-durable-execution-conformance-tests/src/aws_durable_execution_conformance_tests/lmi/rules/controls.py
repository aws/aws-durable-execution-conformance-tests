# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Definite Invoke rejections and the case key digest shared across the AWS transport."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any

_DEFINITE_INVOKE_REJECTIONS = frozenset(
    {
        "AccessDeniedException",
        "ExpiredTokenException",
        "TooManyRequestsException",
        "DurableExecutionAlreadyStartedException",
        "InvalidParameterValueException",
        "ResourceNotFoundException",
        "RequestTooLargeException",
        "InvalidRequestContentException",
        "UnsupportedMediaTypeException",
        "InvalidRuntimeException",
        "SerializedRequestEntityTooLargeException",
    }
)


def invoke_rejection_proven(record: Mapping[str, Any]) -> bool:
    code = record.get("rejectionCode")
    return (
        record.get("dispatchState") == "REJECTED"
        and type(record.get("retryAttempts")) is int
        and record["retryAttempts"] == 0
        and isinstance(code, str)
        and code in _DEFINITE_INVOKE_REJECTIONS
        and not any(field in record for field in ("arn", "executionArn", "requestId"))
    )


def case_key(case_id: str) -> str:
    return hashlib.sha256(case_id.encode()).hexdigest()[:20]
