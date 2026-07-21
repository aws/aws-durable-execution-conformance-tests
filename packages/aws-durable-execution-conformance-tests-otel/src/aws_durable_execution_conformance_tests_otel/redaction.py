# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Secret-safe diagnostic helpers."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping, Sequence
from typing import Any

REDACTED = "[REDACTED]"
_SECRET_KEY = re.compile(
    r"(authorization|api[-_]?key|application[-_]?key|token|password|secret|headers)",
    re.IGNORECASE,
)
SECRET_ENV_NAMES = frozenset(
    {
        "DD_API_KEY",
        "DD_APPLICATION_KEY",
        "DASH0_AUTH_TOKEN",
        "OTEL_EXPORTER_OTLP_HEADERS",
    }
)


def environment_secrets(environ: Mapping[str, str] | None = None) -> tuple[str, ...]:
    source = environ or os.environ
    return tuple(source[name] for name in SECRET_ENV_NAMES if source.get(name))


def redact(value: Any, *, secrets: Sequence[str] | None = None) -> Any:
    """Recursively redact secret-looking keys and known secret values."""

    known = tuple(secret for secret in (secrets or environment_secrets()) if secret)
    if isinstance(value, Mapping):
        return {
            str(key): REDACTED if _SECRET_KEY.search(str(key)) else redact(item, secrets=known)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact(item, secrets=known) for item in value]
    if isinstance(value, tuple):
        return tuple(redact(item, secrets=known) for item in value)
    if isinstance(value, str):
        result = value
        for secret in known:
            result = result.replace(secret, REDACTED)
        return result
    return value
