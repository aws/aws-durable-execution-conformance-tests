# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""CloudWatch log retrieval and validation.

Provides utilities for fetching CloudWatch log events for Lambda functions
and validating them against expected log patterns from YAML specs.
"""

from __future__ import annotations

import json
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
    """A single expected log entry from the YAML spec (v3 schema).

    Cardinality (``count``/``min_count``/``max_count``) is always evaluated
    over the whole log stream. Ordering is asserted only via ``before``/
    ``after`` anchor matchers.

    Attributes:
        match: Field-name -> expected-value map. Values are matched EXACTLY
            (whitespace-stripped for strings) after placeholder substitution;
            a ``/…/``-delimited string value is a regex search instead.
            Multiple fields are ANDed. Fields come from the log record's
            JSON object (Lambda envelope + SDK enrichment); a non-JSON line
            exposes only ``message`` = the raw line.
        count: If set, the exact number of matching log records expected.
        min_count: If set, the minimum number of matches required.
        max_count: If set, the maximum number of matches allowed.
        before: Anchor matcher(s); all of this entry's matches must occur
            at-or-before all records matching every anchor.
        after: Anchor matcher(s); all of this entry's matches must occur
            at-or-after all records matching every anchor.
    """

    match: dict[str, Any]
    count: int | None = None
    min_count: int | None = None
    max_count: int | None = None
    before: tuple[dict[str, Any], ...] = ()
    after: tuple[dict[str, Any], ...] = ()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LogExpectation:
        """Build a LogExpectation from a YAML dict entry."""
        match = data["match"]
        if not isinstance(match, dict) or not match:
            msg = "'match' must be a non-empty field->value mapping"
            raise TypeError(msg)
        return cls(
            match=match,
            count=data.get("count"),
            min_count=data.get("min_count"),
            max_count=data.get("max_count"),
            before=_anchor_tuple(data.get("before")),
            after=_anchor_tuple(data.get("after")),
        )

    @property
    def label(self) -> str:
        """Human-readable identity for error messages."""
        return ", ".join(f"{k}={v!r}" for k, v in self.match.items())


def _anchor_tuple(raw: Any) -> tuple[dict[str, Any], ...]:
    """Normalize a before/after value into a tuple of matcher dicts."""
    if raw is None:
        return ()
    if isinstance(raw, dict):
        return (raw,)
    if isinstance(raw, list) and all(isinstance(item, dict) for item in raw):
        return tuple(raw)
    msg = "'before'/'after' must be a matcher mapping or a list of matcher mappings"
    raise TypeError(msg)


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

        Events are retrieved via ``filter_log_events`` (near-real-time,
        unlike the Logs Insights query engine whose indexing of fresh log
        groups can lag by tens of minutes or more) and attributed
        client-side: structured records carrying ``durableExecutionArn`` or
        ``executionArn`` are kept only when they match ``execution_arn``;
        records with no ARN field (plain stdout lines) cannot be attributed
        and are kept — they are already scoped by the per-function log
        group and the invocation time window.

        Args:
            log_group_name: The full log group name.
            execution_arn: Durable execution ARN to filter attributed records on.
            start_time_ms: Start of the time range in epoch milliseconds.
            end_time_ms: End of the time range in epoch milliseconds.
                         Defaults to current time if not provided.
            wait_seconds: Seconds to wait before querying for log propagation.

        Returns:
            A list of log event dicts, each with at least a "message" key.

        Raises:
            CloudWatchLogError: If retrieval fails.
        """
        if wait_seconds is None:
            wait_seconds = self.DEFAULT_WAIT_SECONDS
        if wait_seconds > 0:
            time.sleep(wait_seconds)

        # FilterLogEvents has no signal that all matching records have been
        # ingested, so poll through a bounded ingestion window (upstream #35)
        # and attribute client-side: unlike a server-side JSON filter
        # pattern, this keeps plain stdout lines (which carry no ARN field
        # and are already scoped by log group + time window).
        deadline = time.monotonic() + self.EVENT_POLL_TIMEOUT_SECONDS
        while True:
            all_events: list[dict] = self.get_log_events(
                log_group_name=log_group_name,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
                wait_seconds=0,
            )
            if time.monotonic() >= deadline:
                return [evt for evt in all_events if _attributed_to_execution(evt.get("message", ""), execution_arn)]
            time.sleep(self.EVENT_POLL_INTERVAL_SECONDS)


def _attributed_to_execution(raw_line: str, execution_arn: str) -> bool:
    """True unless the record is attributed to a DIFFERENT execution."""
    record = _parse_record(raw_line)
    record_arn = record.get("durableExecutionArn", record.get("executionArn"))
    if record_arn is None:
        return True
    return execution_arn in str(record_arn)


# endregion


# region Validator


class CloudWatchLogValidator:
    """Matches expected log records against actual CloudWatch log events.

    v3 semantics:

    - Each event's raw line is JSON-parsed into a field map (Lambda
      envelope + SDK enrichment); non-JSON lines expose only ``message``.
    - ``match`` asserts field values EXACTLY (strings whitespace-stripped,
      ``/…/`` values as regex search, multiple fields ANDed).
    - Cardinality is always global over the whole (timestamp-sorted) stream.
    - ``before``/``after`` anchors are independent matchers: all of the
      entry's matches must occur at-or-before / at-or-after ALL records
      matching every anchor. Identical ``(timestamp, ingestionTime)`` sort
      keys are CONCURRENT and satisfy either direction. An anchor matching
      zero records is a validation error (typo guard).
    """

    def validate(
        self,
        expected_logs: list[dict[str, Any]],
        actual_events: list[dict],
        context: PlaceholderContext | None = None,
    ) -> LogMatchResult:
        """Validate actual log events against expected log entries.

        Args:
            expected_logs: Raw dicts from the YAML ExpectedLogs section.
            actual_events: Log event dicts from CloudWatch (each has a "message" key).
            context: Optional PlaceholderContext for substituting placeholders
                in matcher values before matching.

        Returns:
            LogMatchResult with success flag and any errors.
        """
        errors: list[str] = []
        sorted_events = sorted(actual_events, key=self._event_sort_key)
        keys = [self._event_sort_key(evt) for evt in sorted_events]
        records = [_parse_record(evt.get("message", "")) for evt in sorted_events]

        for i, raw in enumerate(expected_logs):
            resolved_raw: dict[str, Any] = raw
            if context is not None:
                resolved_raw = context.substitute(raw)

            try:
                expectation = LogExpectation.from_dict(resolved_raw)
            except (KeyError, TypeError) as e:
                errors.append(f"ExpectedLogs[{i}]: invalid entry — {e}")
                continue

            label = f"ExpectedLogs[{i}] match({expectation.label})"

            own_matches = _matching_indices(expectation.match, records)
            errors.extend(self._check_count(expectation, len(own_matches), index=i))

            for direction, anchors in (("after", expectation.after), ("before", expectation.before)):
                for anchor in anchors:
                    anchor_matches = _matching_indices(anchor, records)
                    if not anchor_matches:
                        errors.append(f"{label}: {direction} anchor {anchor!r} matched no log records")
                        continue
                    if not own_matches:
                        # Missing own matches are reported by the cardinality
                        # check; the ordering constraint is vacuous.
                        continue
                    if direction == "after":
                        # All own matches at-or-after all anchor matches
                        # (identical sort keys = concurrent = satisfied).
                        if keys[own_matches[0]] < keys[anchor_matches[-1]]:
                            errors.append(f"{label}: expected all matches after {anchor!r} (log records out of order)")
                    elif keys[own_matches[-1]] > keys[anchor_matches[0]]:
                        errors.append(f"{label}: expected all matches before {anchor!r} (log records out of order)")

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
    def _check_count(
        expectation: LogExpectation,
        actual_count: int,
        index: int,
    ) -> list[str]:
        """Check whether actual_count satisfies the expectation's count constraints."""
        errors: list[str] = []
        label = f"ExpectedLogs[{index}] match({expectation.label})"

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


def _parse_record(raw_line: str) -> dict[str, Any]:
    """Parse a raw log line into an assertable field map.

    JSON object lines (the Lambda structured envelope, optionally enriched
    by the SDK logger) expose their top-level keys as fields. When the
    envelope's ``message`` value is itself a JSON object string (a plugin
    printing JSON through a runtime that wraps stdout, e.g. the Node.js
    JSON log format), the inner object's fields are merged in (inner keys
    win) so plugin-emitted fields are assertable uniformly across runtimes.
    Non-JSON lines (plain stdout/println) expose only ``message`` = the
    raw line.
    """
    record = _parse_json_object(raw_line)
    if record is None:
        return {"message": raw_line.strip()}
    inner = record.get("message")
    if isinstance(inner, str):
        inner_record = _parse_json_object(inner)
        if inner_record is not None:
            record = {**record, **inner_record}
    return record


def _parse_json_object(text: str) -> dict[str, Any] | None:
    """Parse text as a JSON object, returning None for anything else."""
    stripped = text.strip()
    if not stripped.startswith("{"):
        return None
    try:
        parsed = json.loads(stripped)
    except ValueError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _matching_indices(matcher: dict[str, Any], records: list[dict[str, Any]]) -> list[int]:
    """Indices of records satisfying every field constraint in the matcher."""
    return [idx for idx, record in enumerate(records) if _record_matches(matcher, record)]


def _record_matches(matcher: dict[str, Any], record: dict[str, Any]) -> bool:
    return all(field in record and _value_matches(expected, record[field]) for field, expected in matcher.items())


def _value_matches(expected: Any, actual: Any) -> bool:
    """Match one field value: exact by default, ``/…/`` string = regex search."""
    if isinstance(expected, str):
        actual_str = str(actual).strip()
        if len(expected) >= 2 and expected.startswith("/") and expected.endswith("/"):
            return re.search(expected[1:-1], actual_str) is not None
        return actual_str == expected.strip()
    if isinstance(expected, bool) or isinstance(actual, bool):
        return expected is actual
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        return float(expected) == float(actual)
    return str(expected) == str(actual)


# endregion
