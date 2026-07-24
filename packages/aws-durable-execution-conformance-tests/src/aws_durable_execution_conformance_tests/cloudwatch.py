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

    Cardinality (``count``/``min_count``/``max_count``) is always evaluated
    over the whole log stream. Ordering is asserted only where an entry
    declares ``before``/``after`` — each contains the ``pattern`` text of
    another entry in the list (placeholders substituted).

    Attributes:
        pattern: The string or regex pattern to search for.
        match: Matching mode — "contains" (default), "exact", or "regex".
        count: If set, the exact number of matching log lines expected.
        min_count: If set, the minimum number of matches required.
        max_count: If set, the maximum number of matches allowed.
        before: Pattern of another entry; all of this entry's matches must
            occur at-or-before all matches of the referenced entry.
        after: Pattern of another entry; all of this entry's matches must
            occur at-or-after all matches of the referenced entry.
    """

    pattern: str
    match: str = "contains"
    count: int | None = None
    min_count: int | None = None
    max_count: int | None = None
    before: str | None = None
    after: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LogExpectation:
        """Build a LogExpectation from a YAML dict entry."""
        return cls(
            pattern=data["pattern"],
            match=data.get("match", "contains"),
            count=data.get("count"),
            min_count=data.get("min_count"),
            max_count=data.get("max_count"),
            before=data.get("before"),
            after=data.get("after"),
        )


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
    EVENT_POLL_INTERVAL_SECONDS = 1.0
    EVENT_POLL_TIMEOUT_SECONDS = 10.0

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

        CloudWatch's JSON filter matches ``durableExecutionArn`` or
        ``executionArn`` on structured Lambda log records. Filtering on both
        field names keeps concurrent executions of the same function isolated
        without relying on Logs Insights indexing. The method polls through a
        bounded ingestion window because ``FilterLogEvents`` has no signal that
        all matching records are available.

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
            CloudWatchLogError: If fetching log events fails.
        """
        escaped_execution_arn = execution_arn.replace("\\", "\\\\").replace('"', '\\"')
        filter_pattern = (
            f'{{ ($.durableExecutionArn = "{escaped_execution_arn}") || ($.executionArn = "{escaped_execution_arn}") }}'
        )
        if wait_seconds is None:
            wait_seconds = self.DEFAULT_WAIT_SECONDS
        if wait_seconds > 0:
            time.sleep(wait_seconds)

        deadline = time.monotonic() + self.EVENT_POLL_TIMEOUT_SECONDS
        while True:
            events = self.get_log_events(
                log_group_name=log_group_name,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
                filter_pattern=filter_pattern,
                wait_seconds=0,
            )
            if time.monotonic() >= deadline:
                return events
            time.sleep(self.EVENT_POLL_INTERVAL_SECONDS)


# endregion


# region Validator


class CloudWatchLogValidator:
    """Matches expected log patterns against actual CloudWatch log events.

    Cardinality and ordering are independent:

    - **Cardinality**: every entry's ``count``/``min_count``/``max_count``
      constraints are evaluated over the WHOLE stream. There is no implicit
      list-order chain — an entry without ``before``/``after`` asserts
      counts only.
    - **Ordering**: an entry with ``after: <pattern>`` (or ``before``)
      requires all of its matches to occur at-or-after (at-or-before) all
      matches of the entry whose ``pattern`` equals the referenced text.
      Events are compared by ``(timestamp, ingestionTime)``; events sharing
      an identical key are treated as CONCURRENT and satisfy either
      direction — CloudWatch provides no sub-millisecond order, so ties are
      never treated as violations.
    - References must name the ``pattern`` of another entry in the same
      list (after placeholder substitution); dangling or self references
      are validation errors.
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
        keys = [self._event_sort_key(evt) for evt in sorted_events]

        # Parse all entries first so before/after can reference any entry.
        expectations: list[LogExpectation | None] = []
        for i, raw in enumerate(expected_logs):
            resolved_raw: dict[str, Any] = raw
            if context is not None:
                resolved_raw = context.substitute(raw)
            try:
                expectations.append(LogExpectation.from_dict(resolved_raw))
            except (KeyError, TypeError) as e:
                errors.append(f"ExpectedLogs[{i}]: invalid entry — {e}")
                expectations.append(None)

        by_pattern: dict[str, LogExpectation] = {}
        for exp in expectations:
            if exp is not None and exp.pattern not in by_pattern:
                by_pattern[exp.pattern] = exp

        match_cache: dict[tuple[str, str], list[int]] = {}

        def matches_of(exp: LogExpectation) -> list[int]:
            cache_key = (exp.pattern, exp.match)
            if cache_key not in match_cache:
                match_cache[cache_key] = self._match_indices(exp, messages, start=0)
            return match_cache[cache_key]

        for i, exp in enumerate(expectations):
            if exp is None:
                continue

            own_matches = matches_of(exp)
            errors.extend(self._check_count(exp, len(own_matches), index=i))

            for field_name, ref in (("after", exp.after), ("before", exp.before)):
                if ref is None:
                    continue
                label = f"ExpectedLogs[{i}] pattern={exp.pattern!r}"
                if ref == exp.pattern:
                    errors.append(f"{label}: {field_name} must not reference the entry's own pattern")
                    continue
                ref_exp = by_pattern.get(ref)
                if ref_exp is None:
                    errors.append(f"{label}: {field_name}={ref!r} does not match the pattern of any other entry")
                    continue

                ref_matches = matches_of(ref_exp)
                if not own_matches or not ref_matches:
                    # Missing matches are reported by each entry's own
                    # cardinality checks; the ordering constraint is vacuous.
                    continue

                if field_name == "after":
                    # All own matches at-or-after all referenced matches
                    # (identical sort keys = concurrent = satisfied).
                    if keys[own_matches[0]] < keys[ref_matches[-1]]:
                        errors.append(f"{label}: expected all matches after {ref!r} (log lines out of order)")
                elif keys[own_matches[-1]] > keys[ref_matches[0]]:
                    errors.append(f"{label}: expected all matches before {ref!r} (log lines out of order)")

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
    ) -> list[str]:
        """Check whether actual_count satisfies the expectation's count constraints."""
        errors: list[str] = []
        label = f"ExpectedLogs[{index}] pattern={expectation.pattern!r}"

        has_constraint = (
            expectation.count is not None or expectation.min_count is not None or expectation.max_count is not None
        )

        if expectation.count is not None:
            if actual_count != expectation.count:
                errors.append(f"{label}: expected exactly {expectation.count} match(es), got {actual_count}")
        else:
            min_c = expectation.min_count
            max_c = expectation.max_count

            # Default: if no constraint at all, require at least 1
            if not has_constraint:
                min_c = 1

            if min_c is not None and actual_count < min_c:
                errors.append(f"{label}: expected at least {min_c} match(es), got {actual_count}")
            if max_c is not None and actual_count > max_c:
                errors.append(f"{label}: expected at most {max_c} match(es), got {actual_count}")

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
