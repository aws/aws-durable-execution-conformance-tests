# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Bounded polling contract shared by telemetry backends."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass

from aws_durable_execution_conformance_tests_otel.model import TelemetryQuery, Trace


class BackendError(RuntimeError):
    """Provider-neutral telemetry backend failure."""


class TelemetryTimeout(BackendError):
    """Raised when no matching trace arrives before the polling limit."""


@dataclass(frozen=True)
class PollingPolicy:
    timeout_seconds: float = 60.0
    interval_seconds: float = 2.0
    max_attempts: int = 30

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("poll timeout must be greater than zero")
        if self.interval_seconds < 0:
            raise ValueError("poll interval cannot be negative")
        if self.max_attempts <= 0:
            raise ValueError("poll max attempts must be greater than zero")


class PollingBackend(ABC):
    """Backend base class implementing ingestion-latency retries."""

    name: str

    def __init__(
        self,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._monotonic = monotonic
        self._sleep = sleep

    def find_trace(self, query: TelemetryQuery, policy: PollingPolicy) -> Trace:
        started = self._monotonic()
        attempts = 0
        while attempts < policy.max_attempts:
            attempts += 1
            trace = self._lookup(query)
            if trace is not None:
                return trace
            elapsed = self._monotonic() - started
            if elapsed >= policy.timeout_seconds:
                break
            self._sleep(min(policy.interval_seconds, policy.timeout_seconds - elapsed))

        raise TelemetryTimeout(
            f"No correlated trace was found in backend {self.name!r} after "
            f"{attempts} attempt(s) within {policy.timeout_seconds:g}s; "
            f"execution={query.execution_arn!r}, service={query.service_name!r}"
        )

    @abstractmethod
    def _lookup(self, query: TelemetryQuery) -> Trace | None:
        """Return a matching trace or ``None`` while ingestion is pending."""
