# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Bounded polling contract shared by Workflow Insight sinks.

Records reach a sink asynchronously (S3 PutObject / CloudWatch Logs ingestion),
so a sink retries until either the fetched records *satisfy* the requirement's
assertions (via the ``accept`` callback) or the polling budget is exhausted --
mirroring the otel add-on's ingestion-lag loop.

Because a valid requirement can assert **absence** (``record_count: 0`` for
sampling / on-failure-success), :meth:`PollingSink.find_records` never raises on
"nothing found": ``_lookup`` returns ``None`` while no record has surfaced, the
loop waits out the budget, and an empty list is returned so an absence assertion
can pass only after the ingestion window elapses.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from aws_durable_execution_conformance_tests_insight.model import InsightRecord, RecordQuery


class SinkError(RuntimeError):
    """A Workflow Insight sink failure (listing, fetching, or decoding)."""


class SinkCapability(StrEnum):
    """What operation shape a sink's records can express."""

    OPERATIONS_ARRAY = "OPERATIONS_ARRAY"
    OPERATIONS_BY_NAME = "OPERATIONS_BY_NAME"


@dataclass(frozen=True)
class PollingPolicy:
    """Ingestion-latency retry budget."""

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


class PollingSink(ABC):
    """Sink base class implementing the shared retry / accept loop."""

    name: str
    capability: SinkCapability

    def __init__(
        self,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._monotonic = monotonic
        self._sleep = sleep

    def find_records(
        self,
        query: RecordQuery,
        policy: PollingPolicy,
        *,
        accept: Callable[[list[InsightRecord]], bool] | None = None,
    ) -> list[InsightRecord]:
        """Poll until the records satisfy ``accept`` or the budget elapses.

        Returns the most recently fetched records (an empty list when none ever
        surfaced). ``accept`` decides whether a non-empty fetch already meets the
        assertions so polling can stop early; when it never does, the latest
        fetch is returned for the caller to diff.
        """

        started = self._monotonic()
        attempts = 0
        latest: list[InsightRecord] = []
        while attempts < policy.max_attempts:
            attempts += 1
            found = self._lookup(query)
            if found is not None:
                latest = found
                if accept is None or accept(found):
                    return found
            elapsed = self._monotonic() - started
            if elapsed >= policy.timeout_seconds:
                break
            self._sleep(min(policy.interval_seconds, policy.timeout_seconds - elapsed))
        return latest

    @abstractmethod
    def _lookup(self, query: RecordQuery) -> list[InsightRecord] | None:
        """Return execution-scoped records, or ``None`` while none have surfaced."""
