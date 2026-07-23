# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""CloudWatch log retrieval and validation.

Provides utilities for fetching CloudWatch log events for Lambda functions
and validating them against expected log patterns from YAML specs.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from botocore.exceptions import BotoCoreError, ClientError

if TYPE_CHECKING:
    from aws_durable_execution_conformance_tests.variables import PlaceholderContext

# region Exceptions


class CloudWatchLogError(Exception):
    """Raised when CloudWatch log retrieval fails."""

    def __init__(self, log_group: str, reason: str) -> None:
        super().__init__(f"CloudWatch log error for {log_group}: {reason}")
        self.log_group = log_group
        self.reason = reason


# endregion


# region Models


@dataclass(frozen=True)
class LogExpectation:
    """A single expected log entry from the YAML spec.

    Entries in ``ExpectedLogs`` are matched **in list order by default**
    (see CloudWatchLogValidator for the sequential-scan semantics).

    Attributes:
        pattern: The string or regex pattern to search for.
        match: Matching mode — "contains" (default), "exact", or "regex".
        count: If set, the exact number of matching log lines expected.
        min_count: If set, the minimum number of matches required.
        max_count: If set, the maximum number of matches allowed.
        unordered: If True, this entry is counted over the whole log stream
            and neither anchors nor advances the ordered scan position.
    """

    pattern: str
    match: str = "contains"
    count: int | None = None
    min_count: int | None = None
    max_count: int | None = None
    unordered: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LogExpectation:
        """Build a LogExpectation from a YAML dict entry."""
        return cls(
            pattern=data["pattern"],
            match=data.get("match", "contains"),
            count=data.get("count"),
            min_count=data.get("min_count"),
            max_count=data.get("max_count"),
            unordered=bool(data.get("unordered", False)),
        )

    @property
    def is_absence(self) -> bool:
        """True when the entry asserts the pattern does NOT appear.

        Absence entries (``count: 0`` or ``max_count: 0``) are checked over
        the whole log stream and are exempt from ordering — a pattern that
        never appears cannot anchor a position.
        """
        return self.count == 0 or self.max_count == 0

    @property
    def is_ordered(self) -> bool:
        """True when the entry participates in the ordered sequential scan."""
        return not (self.unordered or self.is_absence)


@dataclass(frozen=True)
class LogMatchResult:
    """Result of validating CloudWatch logs against expectations."""

    success: bool
    errors: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return self.success


# endregion


# region Retriever


class CloudWatchLogRetriever:
    """Fetches CloudWatch log events for a given Lambda function."""

    # Default wait before querying logs to allow propagation
    DEFAULT_WAIT_SECONDS = 5
    QUERY_POLL_INTERVAL_SECONDS = 0.5
    QUERY_TIMEOUT_SECONDS = 30.0
    QUERY_RESULT_LIMIT = 10_000
    _QUERY_FAILURE_STATUSES = frozenset({"Failed", "Cancelled", "Timeout", "Unknown"})

    def __init__(self, cloudformation_client: Any, logs_client: Any) -> None:
        self._cfn_client = cloudformation_client
        self._logs_client = logs_client

    @staticmethod
    def log_group_for_function(function_name: str) -> str:
        """Derive the CloudWatch log group name from a Lambda function name."""
        return f"/aws/lambda/{function_name}"

    def get_log_group_name(self, stack_name: str, logical_function_name: str) -> str:
        """Resolve the physical function name via CloudFormation.

        Uses the CloudFormation SDK to describe the stack resource and
        derive the log group from the physical resource ID.

        Args:
            stack_name: The CloudFormation stack name.
            logical_function_name: The logical resource ID in the template.

        Returns:
            The CloudWatch log group name
            (e.g. /aws/lambda/my-stack-StepBasic-abc123).

        Raises:
            CloudWatchLogError: If the SDK call fails or returns an empty
                physical resource ID.
        """
        try:
            response: dict[str, Any] = self._cfn_client.describe_stack_resource(
                StackName=stack_name,
                LogicalResourceId=logical_function_name,
            )
        except (ClientError, BotoCoreError) as e:
            raise CloudWatchLogError(
                log_group=logical_function_name,
                reason=(f"Failed to resolve physical function name: {e}"),
            ) from e

        physical_name: str = response.get("StackResourceDetail", {}).get("PhysicalResourceId", "")
        if not physical_name:
            raise CloudWatchLogError(
                log_group=logical_function_name,
                reason=("Empty physical resource ID returned from CloudFormation"),
            )
        return self.log_group_for_function(physical_name)

    def get_log_events(
        self,
        log_group_name: str,
        start_time_ms: int,
        end_time_ms: int | None = None,
        filter_pattern: str | None = None,
        wait_seconds: int | None = None,
    ) -> list[dict]:
        """Fetch log events from a CloudWatch log group.

        Args:
            log_group_name: The full log group name.
            start_time_ms: Start of the time range in epoch milliseconds.
            end_time_ms: End of the time range in epoch milliseconds.
                         Defaults to current time if not provided.
            filter_pattern: Optional CloudWatch Logs filter pattern.
            wait_seconds: Seconds to wait before querying (log propagation
                          delay).

        Returns:
            A list of log event dicts, each with at least a "message" key.

        Raises:
            CloudWatchLogError: If the SDK call fails.
        """
        if wait_seconds is None:
            wait_seconds = self.DEFAULT_WAIT_SECONDS
        if wait_seconds > 0:
            time.sleep(wait_seconds)

        if end_time_ms is None:
            end_time_ms = int(time.time() * 1000)

        kwargs: dict[str, Any] = {
            "logGroupName": log_group_name,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }
        if filter_pattern:
            kwargs["filterPattern"] = filter_pattern

        all_events: list[dict] = []

        try:
            while True:
                response: dict[str, Any] = self._logs_client.filter_log_events(**kwargs)
                all_events.extend(response.get("events", []))

                next_token: str | None = response.get("nextToken")
                if not next_token:
                    break
                kwargs["nextToken"] = next_token
        except (ClientError, BotoCoreError) as e:
            raise CloudWatchLogError(
                log_group=log_group_name,
                reason=f"filter-log-events failed: {e}",
            ) from e

        return all_events

    def get_execution_log_events(
        self,
        log_group_name: str,
        execution_arn: str,
        start_time_ms: int,
        end_time_ms: int | None = None,
        wait_seconds: int | None = None,
    ) -> list[dict]:
        """Fetch log events associated with one durable execution.

        CloudWatch Logs Insights discovers ``durableExecutionArn`` or
        ``executionArn`` on structured Lambda log records. Filtering on both
        field names keeps concurrent executions of the same function isolated.

        Args:
            log_group_name: The full log group name.
            execution_arn: Durable execution ARN to filter on.
            start_time_ms: Start of the time range in epoch milliseconds.
            end_time_ms: End of the time range in epoch milliseconds.
                         Defaults to current time if not provided.
            wait_seconds: Seconds to wait before querying for log propagation.

        Returns:
            A list of log event dicts, each with at least a "message" key.

        Raises:
            CloudWatchLogError: If the query cannot be started or completed.
        """
        if wait_seconds is None:
            wait_seconds = self.DEFAULT_WAIT_SECONDS
        if wait_seconds > 0:
            time.sleep(wait_seconds)

        if end_time_ms is None:
            end_time_ms = int(time.time() * 1000)

        start_time_seconds = start_time_ms // 1000
        end_time_seconds = max(start_time_seconds + 1, (end_time_ms + 999) // 1000)
        escaped_execution_arn = execution_arn.replace("\\", "\\\\").replace('"', '\\"')
        query_string = (
            "fields @timestamp, @message\n"
            f'| filter coalesce(durableExecutionArn, executionArn) like "{escaped_execution_arn}"\n'
            "| sort @timestamp asc"
        )

        try:
            response: dict[str, Any] = self._logs_client.start_query(
                queryLanguage="CWLI",
                logGroupName=log_group_name,
                startTime=start_time_seconds,
                endTime=end_time_seconds,
                queryString=query_string,
                limit=self.QUERY_RESULT_LIMIT,
            )
            query_id: str | None = response.get("queryId")
            if not query_id:
                raise CloudWatchLogError(
                    log_group=log_group_name,
                    reason="start-query returned no query ID",
                )

            deadline = time.monotonic() + self.QUERY_TIMEOUT_SECONDS
            while True:
                response = self._logs_client.get_query_results(queryId=query_id)
                status: str = response.get("status", "Unknown")
                if status == "Complete":
                    return self._query_results_to_events(response.get("results", []))
                if status in self._QUERY_FAILURE_STATUSES:
                    raise CloudWatchLogError(
                        log_group=log_group_name,
                        reason=f"logs-insights query ended with status {status}",
                    )
                if time.monotonic() >= deadline:
                    raise CloudWatchLogError(
                        log_group=log_group_name,
                        reason=f"logs-insights query did not complete within {self.QUERY_TIMEOUT_SECONDS:g} seconds",
                    )
                time.sleep(self.QUERY_POLL_INTERVAL_SECONDS)
        except (ClientError, BotoCoreError) as e:
            raise CloudWatchLogError(
                log_group=log_group_name,
                reason=f"logs-insights query failed: {e}",
            ) from e

    @staticmethod
    def _query_results_to_events(results: list[list[dict[str, str]]]) -> list[dict]:
        """Normalize Logs Insights rows for ``CloudWatchLogValidator``."""
        events: list[dict] = []
        for row in results:
            fields = {entry.get("field", ""): entry.get("value", "") for entry in row}
            event = {"message": fields.get("@message", fields.get("message", ""))}
            timestamp = fields.get("@timestamp", fields.get("timestamp"))
            if timestamp is not None:
                event["timestamp"] = timestamp
            events.append(event)
        return events


# endregion


# region Validator


class CloudWatchLogValidator:
    """Matches expected log patterns against actual CloudWatch log events.

    Entries in ``ExpectedLogs`` are matched **in list order by default**
    (sequential scan):

    - Log events are sorted by ``(timestamp, ingestionTime)`` before matching.
    - Each *ordered* entry starts scanning after the previous ordered entry's
      last consumed match; its count constraints are evaluated over the
      remainder of the stream from that position onward. After a successful
      match, the scan position advances past the match that satisfied the
      entry's minimum requirement (``count``, else ``min_count``, else 1).
    - *Absence* entries (``count: 0`` / ``max_count: 0``) and entries marked
      ``unordered: true`` are counted over the whole stream and do not
      affect the scan position.

    With a single positive entry this degenerates to a global count, so
    specs written against the previous (unordered) semantics keep working.
    """

    def validate(
        self,
        expected_logs: list[dict[str, Any]],
        actual_events: list[dict],
        context: PlaceholderContext | None = None,
    ) -> LogMatchResult:
        """Validate actual log events against expected log patterns.

        Args:
            expected_logs: Raw dicts from the YAML ExpectedLogs section.
            actual_events: Log event dicts from CloudWatch (each has a "message" key).
            context: Optional PlaceholderContext for substituting placeholders
                in log patterns before matching.

        Returns:
            LogMatchResult with success flag and any errors.
        """
        errors: list[str] = []
        sorted_events = sorted(actual_events, key=self._event_sort_key)
        messages = [evt.get("message", "") for evt in sorted_events]
        scan_pos = 0

        for i, raw in enumerate(expected_logs):
            # Substitute placeholders in the pattern before building expectation
            resolved_raw: dict[str, Any] = raw
            if context is not None:
                resolved_raw = context.substitute(raw)

            try:
                expectation = LogExpectation.from_dict(resolved_raw)
            except (KeyError, TypeError) as e:
                errors.append(f"ExpectedLogs[{i}]: invalid entry — {e}")
                continue

            if expectation.is_ordered:
                match_indices = self._match_indices(expectation, messages, start=scan_pos)
                entry_errors = self._check_count(expectation, len(match_indices), index=i, scan_pos=scan_pos)
                errors.extend(entry_errors)
                if match_indices:
                    required = expectation.count or expectation.min_count or 1
                    consumed = min(required, len(match_indices))
                    scan_pos = match_indices[consumed - 1] + 1
            else:
                match_indices = self._match_indices(expectation, messages, start=0)
                entry_errors = self._check_count(expectation, len(match_indices), index=i)
                errors.extend(entry_errors)

        return LogMatchResult(
            success=len(errors) == 0,
            errors=errors,
        )

    @staticmethod
    def _event_sort_key(evt: dict) -> tuple:
        """Type-stable ordering key for log events.

        Events from ``filter_log_events`` carry an epoch-ms int ``timestamp``
        plus ``ingestionTime``; events from the Logs Insights path carry a
        string ``@timestamp`` (already ISO-ish sortable) and no ingestion
        time. Any one retrieval yields a homogeneous shape — the typed tuple
        merely prevents TypeError if shapes are ever mixed.
        """
        ts = evt.get("timestamp", 0)
        if isinstance(ts, (int, float)):
            ts_key: tuple = (0, float(ts), "")
        else:
            ts_key = (1, 0.0, str(ts))
        return (ts_key, evt.get("ingestionTime", 0))

    @staticmethod
    def _match_indices(expectation: LogExpectation, messages: list[str], start: int) -> list[int]:
        """Return indices (>= start) of messages matching the expectation's pattern."""
        return [
            idx
            for idx in range(start, len(messages))
            if _matches(expectation.pattern, messages[idx], expectation.match)
        ]

    @staticmethod
    def _check_count(
        expectation: LogExpectation,
        actual_count: int,
        index: int,
        scan_pos: int | None = None,
    ) -> list[str]:
        """Check whether actual_count satisfies the expectation's count constraints."""
        errors: list[str] = []
        label = f"ExpectedLogs[{index}] pattern={expectation.pattern!r}"
        where = f" at/after ordered position {scan_pos}" if scan_pos else ""

        has_constraint = (
            expectation.count is not None or expectation.min_count is not None or expectation.max_count is not None
        )

        if expectation.count is not None:
            if actual_count != expectation.count:
                errors.append(f"{label}: expected exactly {expectation.count} match(es){where}, got {actual_count}")
        else:
            min_c = expectation.min_count
            max_c = expectation.max_count

            # Default: if no constraint at all, require at least 1
            if not has_constraint:
                min_c = 1

            if min_c is not None and actual_count < min_c:
                errors.append(f"{label}: expected at least {min_c} match(es){where}, got {actual_count}")
            if max_c is not None and actual_count > max_c:
                errors.append(f"{label}: expected at most {max_c} match(es){where}, got {actual_count}")

        return errors


def _matches(pattern: str, message: str, mode: str) -> bool:
    """Check if a message matches a pattern using the given mode."""
    match mode:
        case "exact":
            return message.strip() == pattern
        case "regex":
            return re.search(pattern, message) is not None
        case _:
            return pattern in message


# endregion
