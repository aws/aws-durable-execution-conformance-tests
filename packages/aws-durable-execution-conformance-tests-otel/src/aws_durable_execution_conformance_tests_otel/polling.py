# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Bounded polling contract shared by telemetry backends."""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from aws_durable_execution_conformance_tests_otel.model import TelemetryQuery, Trace


class BackendError(RuntimeError):
    """Provider-neutral telemetry backend failure."""


class RetryableBackendError(BackendError):
    """Transient backend failure that may include a server-requested delay."""

    def __init__(
        self,
        message: str,
        *,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class TelemetryTimeout(BackendError):
    """Raised when no matching trace arrives before the polling limit."""


class BackendFeatureDisparity(StrEnum):
    """Known fidelity gaps in a backend's normalized telemetry."""

    SPAN_LINKS = "span-links"
    UNSET_STATUS = "unset-status"


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
    feature_disparities: frozenset[BackendFeatureDisparity] = frozenset()

    def __init__(
        self,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._monotonic = monotonic
        self._sleep = sleep

    def find_trace(
        self,
        query: TelemetryQuery,
        policy: PollingPolicy,
        *,
        accept: Callable[[Trace], bool] | None = None,
    ) -> Trace:
        started = self._monotonic()
        attempts = 0
        latest_trace: Trace | None = None
        latest_retryable_error: RetryableBackendError | None = None
        while attempts < policy.max_attempts:
            attempts += 1
            delay_seconds = policy.interval_seconds
            try:
                trace = self._lookup(query)
            except RetryableBackendError as exc:
                latest_retryable_error = exc
                if exc.retry_after_seconds is not None:
                    delay_seconds = exc.retry_after_seconds
            else:
                latest_retryable_error = None
                if trace is not None:
                    latest_trace = trace
                    if accept is None or accept(trace):
                        return trace
            if attempts >= policy.max_attempts:
                break
            elapsed = self._monotonic() - started
            if elapsed >= policy.timeout_seconds:
                break
            self._sleep(min(delay_seconds, policy.timeout_seconds - elapsed))

        if latest_trace is not None:
            return latest_trace
        if latest_retryable_error is not None:
            raise latest_retryable_error

        raise TelemetryTimeout(
            f"No correlated trace was found in backend {self.name!r} after "
            f"{attempts} attempt(s) within {policy.timeout_seconds:g}s; "
            f"execution={query.execution_arn!r}, service={query.service_name!r}"
        )

    @abstractmethod
    def _lookup(self, query: TelemetryQuery) -> Trace | None:
        """Return a matching trace or ``None`` while ingestion is pending."""
