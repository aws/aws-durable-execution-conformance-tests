# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Controls-only retirement and definite Invoke rejections, shared by the rules and the driver."""

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


def controls_only_retirement(evidence: Mapping[str, Any]) -> bool:
    retirement = evidence.get("retirement")
    return isinstance(retirement, Mapping) and retirement.get("mode") == "controls-only"
