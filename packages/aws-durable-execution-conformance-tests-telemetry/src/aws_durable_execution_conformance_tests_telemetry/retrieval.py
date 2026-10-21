# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Retrieve Durable Execution Telemetry records for one execution from CloudWatch Logs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from aws_durable_execution_conformance_tests.cloudwatch import (
    CloudWatchLogError,
    CloudWatchLogRetriever,
)
from aws_durable_execution_conformance_tests_telemetry.model import (
    TelemetryRecord,
    parse_record,
)


class TelemetryAccessError(RuntimeError):
    """CloudWatch Logs could not be read (wraps :class:`CloudWatchLogError`)."""


@dataclass(frozen=True)
class FetchedRecords:
    """Raw events plus their classification into records and malformed events."""

    events: tuple[Mapping[str, Any], ...]
    records: tuple[TelemetryRecord, ...]
    malformed: tuple[Mapping[str, Any], ...]


def fetch_records(
    retriever: CloudWatchLogRetriever,
    log_group: str,
    execution_arn: str,
    *,
    start_time_ms: int,
    wait_seconds: int,
) -> FetchedRecords:
    """Fetch and classify the CloudWatch events for one execution.

    The core retriever already filters on ``$.executionArn``. Records whose
    ``executionArn`` differs from ``execution_arn`` are dropped anyway, so a
    filter change in the core package cannot admit a foreign record here.

    Raises:
        TelemetryAccessError: When the core retriever raises
            :class:`CloudWatchLogError`.
    """
    try:
        events = retriever.get_execution_log_events(
            log_group_name=log_group,
            execution_arn=execution_arn,
            start_time_ms=start_time_ms,
            wait_seconds=wait_seconds,
        )
    except CloudWatchLogError as exc:
        raise TelemetryAccessError(str(exc)) from exc

    records: list[TelemetryRecord] = []
    malformed: list[Mapping[str, Any]] = []
    for event in events:
        record = parse_record(event)
        if record is None:
            malformed.append(event)
            continue
        if record.data.get("executionArn") != execution_arn:
            continue
        records.append(record)
    return FetchedRecords(
        events=tuple(events),
        records=tuple(records),
        malformed=tuple(malformed),
    )
