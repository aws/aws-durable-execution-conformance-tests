# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Cohort evidence checks that reuse the ordinary OTel canonical model.

SDK adapters collect the *whole cohort* through an existing backend adapter and
serialize its spans with ``span_to_dict``. Origin request IDs come from fixture
observations, independently of the attributes emitted by the SDK under test.
Runtime carriers are observed before SDK dispatch, never injected via input.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from typing import Any

from aws_durable_execution_conformance_tests_otel.chained_invoke import execution_trace_context
from aws_durable_execution_conformance_tests_otel.model import (
    Span,
    SpanLink,
    TelemetryQuery,
    Trace,
    parse_timestamp,
)
from aws_durable_execution_conformance_tests_otel.validators import validate_trace

_SOURCES = {
    "javascript": "invocation-context",
    "java": "runtime-caller-context",
    "python": "process-environment",
}
_EXECUTION_KEYS = ("durable.execution.arn", "aws.lambda.durable_execution.arn", "durable_execution_arn")
_REQUEST_KEYS = ("faas.invocation_id", "aws.request_id", "aws.lambda.request_id")


def _records(value: Any, name: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise ValueError(f"{name} must be a list of objects")
    return value


def _identifier(value: Any, width: int) -> str:
    if not isinstance(value, str) or not re.fullmatch(rf"[0-9a-f]{{{width}}}", value) or int(value, 16) == 0:
        raise ValueError(f"canonical ID must be {width} nonzero hexadecimal digits")
    return value


def _number(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError("timestamps and budgets must be finite numbers")
    return float(value)


def _span(raw: Any) -> Span:
    if not isinstance(raw, Mapping) or not isinstance(raw.get("attributes"), Mapping):
        raise ValueError("span must contain canonical properties and an attributes object")
    if not isinstance(raw.get("name"), str) or not raw["name"]:
        raise ValueError("canonical span name must be a nonempty string")
    parent = raw.get("parent_span_id")
    return Span(
        trace_id=_identifier(raw["trace_id"], 32),
        span_id=_identifier(raw["span_id"], 16),
        name=raw["name"],
        start_time=parse_timestamp(raw["start_time"]),
        end_time=parse_timestamp(raw["end_time"]),
        parent_span_id=_identifier(parent, 16) if parent is not None else None,
        kind=raw.get("kind", "UNSPECIFIED"),
        status=raw.get("status", "UNSET"),
        attributes=raw["attributes"],
        service_name=raw.get("service_name"),
        exception_event_count=raw.get("exception_event_count"),
        links=tuple(
            SpanLink(_identifier(link["trace_id"], 32), _identifier(link["span_id"], 16))
            for link in _records(raw.get("links", []), "links")
        ),
    )


def _service_execution_starts(histories: Any) -> dict[str, float]:
    """Preserve optional starts observed from each execution's service history."""
    starts: dict[str, float] = {}
    for history in _records(histories, "histories"):
        if "startedAtMs" not in history:
            continue
        arn = history.get("executionArn")
        if not isinstance(arn, str) or not arn or arn in starts:
            raise ValueError("service execution starts require unique execution ARNs")
        start = _number(history["startedAtMs"])
        if start < 0:
            raise ValueError("service execution start must be nonnegative")
        starts[arn] = start
    return starts


def _matches_operation_history_window(
    span: Span,
    execution_arn: str,
    histories: Sequence[Mapping[str, Any]],
) -> bool:
    """Only a matching completed service operation can explain an early span.

    Execution-view logical operations can span suspension or retry. Native
    invocation, user and attempt spans do not acquire that longer lifetime.
    """
    attributes = span.attributes
    operation_id = attributes.get("durable.operation.id")
    operation_type = attributes.get("durable.operation.type")
    status = attributes.get("durable.operation.status")
    if (
        not isinstance(operation_id, str)
        or not operation_id
        or not isinstance(operation_type, str)
        or not operation_type
        or not isinstance(status, str)
        or not status
    ):
        return False
    if "durable.attempt.outcome" in attributes:
        return False
    if operation_type.upper() not in {
        "STEP",
        "WAIT",
        "CALLBACK",
        "CONTEXT",
        "CHAINED_INVOKE",
    } or status.upper() not in {"SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED", "CANCELLED"}:
        return False
    matches = [
        operation
        for history in histories
        if history.get("executionArn") == execution_arn
        for operation in _records(history.get("operations", []), "history.operations")
        if operation.get("id") == operation_id
    ]
    if len(matches) != 1:
        return False
    operation = matches[0]
    recorded_type, recorded_status = operation.get("type"), operation.get("status")
    if (
        not isinstance(recorded_type, str)
        or recorded_type.upper() != operation_type.upper()
        or not isinstance(recorded_status, str)
        or recorded_status.upper() != status.upper()
        or "name" not in operation
        or span.name != (operation["name"] if operation["name"] is not None else operation_id)
        or attributes.get("durable.operation.name", operation.get("name")) != operation.get("name")
        or "startedAtMs" not in operation
        or "endedAtMs" not in operation
    ):
        return False
    start = _number(operation["startedAtMs"])
    end = _number(operation["endedAtMs"])
    return 0 <= start <= span.start_time.timestamp() * 1000 <= span.end_time.timestamp() * 1000 <= end


def _view_errors(
    name: str,
    view: Mapping[str, Any],
    attempts: Mapping[str, Mapping[str, Any]],
    runtime: str,
    observation_end: float,
    *,
    execution_starts: Mapping[str, float] | None = None,
    operation_histories: Sequence[Mapping[str, Any]] = (),
) -> list[str]:
    errors: list[str] = []
    native_starts: dict[str, float] = {}
    for attempt in attempts.values():
        arn = attempt["executionArn"]
        start = _number(attempt["startedAtMs"])
        native_starts[arn] = min(start, native_starts.get(arn, start))
    service_starts: dict[str, float] = {}
    for arn, raw_start in (execution_starts or {}).items():
        if arn not in native_starts:
            raise ValueError("service execution start belongs to an unobserved execution")
        start = _number(raw_start)
        if start < 0 or start > native_starts[arn]:
            raise ValueError("service execution start must not follow its first native admission")
        service_starts[arn] = start
    collection = view.get("collection", {})
    if not isinstance(collection, Mapping) or collection.get("status") != "complete":
        return [f"INCOMPLETE: {name} collection did not complete (absence is not unsampled evidence)"]
    request_ids = collection.get("requestIds")
    if (
        collection.get("scope") != "cohort"
        or not isinstance(request_ids, list)
        or any(not isinstance(item, str) for item in request_ids)
        or len(request_ids) != len(set(request_ids))
        or set(request_ids) != set(attempts)
    ):
        return [f"INCOMPLETE: {name} collection must cover every launched request without execution filtering"]
    carriers = {}
    for raw_carrier in _records(view.get("carriers"), "carriers"):
        request_id = raw_carrier.get("requestId")
        if not isinstance(request_id, str) or request_id not in attempts or request_id in carriers:
            errors.append(f"INCOMPLETE: {name} ambiguous or unknown runtime carrier request {request_id!r}")
            continue
        attempt = attempts[request_id]
        if raw_carrier.get("source") != _SOURCES[runtime]:
            errors.append(f"ASSERTION: {name}/{request_id} requires the genuine {runtime} runtime carrier")
        for key in ("environmentId", "workerId", "processId"):
            if raw_carrier.get(key) != attempt[key]:
                errors.append(f"ASSERTION: {name}/{request_id} carrier has the wrong {key}")
        if type(raw_carrier.get("expectedSampled")) is not bool:
            errors.append(f"INCOMPLETE: {name}/{request_id} needs an explicit expected sampling decision")
            continue
        context = execution_trace_context({"TraceHeader": {"XAmznTraceId": raw_carrier.get("header")}})
        if context.sampled != raw_carrier["expectedSampled"]:
            errors.append(f"ASSERTION: {name}/{request_id} runtime sampling differs from the configured decision")
        carriers[request_id] = context
    if set(carriers) != set(attempts):
        errors.append(f"INCOMPLETE: {name} is missing original or retry runtime carriers")

    spans_by_request: dict[str, list[Span]] = {request_id: [] for request_id in attempts}
    owners: dict[tuple[str, str], str] = {}
    all_spans: list[tuple[str, Span]] = []
    infrastructure = [_span(raw) for raw in _records(view.get("infrastructureSpans", []), "infrastructureSpans")]
    if any(
        span.name in {"Invocation", "Workflow"}
        or any(key.startswith("durable.") for key in span.attributes)
        or any(key in span.attributes for key in _EXECUTION_KEYS)
        for span in infrastructure
    ):
        errors.append(f"ASSERTION: {name} SDK spans cannot be hidden as infrastructure")
    for record in _records(view.get("spans"), "spans"):
        request_id = record.get("requestId")
        if not isinstance(request_id, str) or request_id not in attempts:
            errors.append(f"INCOMPLETE: {name} span has no independent cohort origin request {request_id!r}")
            continue
        span = _span(record.get("span"))
        earliest_start = _number(attempts[request_id]["startedAtMs"])
        if span.name == "Workflow":
            # The service execution can begin before Lambda admission (for
            # example during cold start), and its Workflow can be exported on
            # a later resume. Only that execution's observed service start
            # relaxes the old first-native floor; missing proof stays strict.
            arn = attempts[request_id]["executionArn"]
            earliest_start = service_starts.get(arn, native_starts[arn])
        early = span.start_time.timestamp() * 1000 < earliest_start
        if early and name == "execution" and span.name not in {"Invocation", "Workflow"}:
            early = not _matches_operation_history_window(
                span, attempts[request_id]["executionArn"], operation_histories
            )
        if early or span.end_time.timestamp() * 1000 > observation_end:
            errors.append(f"INCOMPLETE: {name}/{request_id} span lies outside its lifecycle observation window")
        previous = owners.setdefault((span.trace_id, span.span_id), request_id)
        if previous != request_id:
            errors.append(f"ASSERTION: {name} one span identity is attributed to multiple requests")
        expected_arn = attempts[request_id]["executionArn"]
        observed_arns = {span.attributes[key] for key in _EXECUTION_KEYS if key in span.attributes}
        if observed_arns and observed_arns != {expected_arn}:
            errors.append(f"ASSERTION: {name}/{request_id} span execution attribution does not match its origin")
        if any(span.attributes[key] != request_id for key in _REQUEST_KEYS if key in span.attributes):
            errors.append(f"ASSERTION: {name}/{request_id} span carries another invocation's request ID")
        carrier = carriers.get(request_id)
        if carrier is not None:
            if not carrier.sampled:
                errors.append(f"ASSERTION: {name}/{request_id} exported spans despite the unsampled runtime carrier")
            if span.trace_id != carrier.trace_id:
                errors.append(f"ASSERTION: {name}/{request_id} span does not preserve the runtime trace")
        spans_by_request[request_id].append(span)
        all_spans.append((request_id, span))
    for request_id, span in all_spans:
        parent_owner = owners.get((span.trace_id, span.parent_span_id or ""))
        if parent_owner and attempts[parent_owner]["executionArn"] != attempts[request_id]["executionArn"]:
            errors.append(f"ASSERTION: {name}/{request_id} span has a parent belonging to a peer execution")
        if not any(key in span.attributes for key in _EXECUTION_KEYS):
            # Ordinary user spans need not copy durable attributes. Their
            # observed ancestry must still establish the originating execution.
            ancestor = span
            visited: set[str] = set()
            while ancestor.parent_span_id and ancestor.span_id not in visited:
                visited.add(ancestor.span_id)
                parents = [
                    candidate
                    for _, candidate in all_spans
                    if candidate.trace_id == ancestor.trace_id and candidate.span_id == ancestor.parent_span_id
                ]
                if not parents:
                    break
                ancestor = parents[0]
                if any(ancestor.attributes.get(key) == attempts[request_id]["executionArn"] for key in _EXECUTION_KEYS):
                    break
            else:
                ancestor = span
            if not any(ancestor.attributes.get(key) == attempts[request_id]["executionArn"] for key in _EXECUTION_KEYS):
                errors.append(f"ASSERTION: {name}/{request_id} user span lacks observed execution ancestry")

    outcomes = {}
    for outcome in _records(view.get("exportOutcomes"), "exportOutcomes"):
        request_id = outcome.get("requestId")
        if request_id not in attempts or request_id in outcomes:
            errors.append(f"INCOMPLETE: {name} ambiguous export outcome request {request_id!r}")
            continue
        outcomes[request_id] = outcome
    if set(outcomes) != set(attempts):
        errors.append(f"INCOMPLETE: {name} export outcomes must cover all requests")

    assertions = view.get("assertions")
    if not isinstance(assertions, Mapping):
        errors.append(f"INCOMPLETE: {name} needs ordinary TelemetryAssertions keyed by execution ARN")
        assertions = {}
    for request_id in attempts:
        carrier = carriers.get(request_id)
        outcome = outcomes.get(request_id, {})
        status = outcome.get("status")
        if status == "unsampled":
            if carrier is None or carrier.sampled or spans_by_request[request_id]:
                errors.append(f"ASSERTION: {name}/{request_id} unsampled result lacks matching runtime evidence")
            continue
        if status in {"export-error", "timeout-loss"}:
            # Explicit failures are retained, and cannot masquerade as an empty
            # successful collection. Requirement 8 checks the matching fixture.
            expected_fixture = "bounded-export-error" if status == "export-error" else "bounded-flush-timeout"
            if not outcome.get("error") or outcome.get("fixture") != expected_fixture:
                errors.append(f"INCOMPLETE: {name}/{request_id} {status} requires an observed error and fixture")
            continue
        if status != "delivered":
            errors.append(f"INCOMPLETE: {name}/{request_id} missing or unknown export outcome {status!r}")
            continue
        if carrier is None or not carrier.sampled or not spans_by_request[request_id]:
            errors.append(f"INCOMPLETE: {name}/{request_id} sampled delivery has no collected spans")
            continue
        roots = [span for span in spans_by_request[request_id] if span.name in {"Invocation", "Workflow"}]
        if not roots:
            errors.append(f"INCOMPLETE: {name}/{request_id} has no SDK root to correlate to the runtime parent")
        for root in roots:
            root_carrier = carrier
            if root.name == "Workflow":
                first_request = min(
                    (
                        owner
                        for owner in attempts
                        if attempts[owner]["executionArn"] == attempts[request_id]["executionArn"]
                    ),
                    key=lambda owner: attempts[owner]["startedAtMs"],
                )
                root_carrier = carriers.get(first_request, carrier)
            # A runtime auto-instrumentation parent is accepted only with its
            # observed ancestry. Omitted intermediates cannot prove correlation.
            seen: set[str] = set()
            parent = root.parent_span_id
            while parent and parent != root_carrier.parent_span_id and parent not in seen:
                seen.add(parent)
                ancestors = [
                    span
                    for span in [*(item[1] for item in all_spans), *infrastructure]
                    if span.trace_id == root.trace_id and span.span_id == parent
                ]
                parent = ancestors[0].parent_span_id if ancestors else None
            if parent != root_carrier.parent_span_id:
                errors.append(f"ASSERTION: {name}/{request_id} SDK root does not reach the runtime parent")

    # Scope only after checking the complete cohort's independent attribution.
    for execution_arn in {attempt["executionArn"] for attempt in attempts.values()}:
        selected = [span for request_id, span in all_spans if attempts[request_id]["executionArn"] == execution_arn]
        delivered = any(
            outcomes.get(request_id, {}).get("status") == "delivered"
            for request_id, attempt in attempts.items()
            if attempt["executionArn"] == execution_arn
        )
        if not delivered or not selected:
            continue
        expected = assertions.get(execution_arn)
        if not isinstance(expected, Mapping) or not expected.get("span_assertions"):
            errors.append(f"INCOMPLETE: {name}/{execution_arn} is missing ordinary span assertions")
            continue
        query = TelemetryQuery(
            execution_arn=execution_arn,
            service_name=selected[0].service_name or "",
            started_at=min(span.start_time for span in selected),
            ended_at=max(span.end_time for span in selected),
        )
        # Correlation is mandatory even if a fixture supplies weaker assertions.
        mandatory = {**expected, "require_execution_correlation": True, "allowed_execution_arns": [execution_arn]}
        errors.extend(
            f"ASSERTION: {name}/{execution_arn}: {error}"
            for error in validate_trace(Trace(selected[0].trace_id, tuple(selected)), mandatory, query)
        )
    return errors


def _late_activity_errors(evidence: Mapping[str, Any], name: str, view: Mapping[str, Any]) -> list[str]:
    """Bind lifecycle late probes to actual public-tracer span observations."""
    errors = []
    for event in _records(evidence.get("events", []), "events"):
        if event.get("kind") != "late_activity":
            continue
        correlations = event.get("telemetry", {})
        marker = correlations.get(name) if isinstance(correlations, Mapping) else None
        if not isinstance(marker, Mapping):
            errors.append(f"INCOMPLETE: {name} late_activity needs the actual trace/span IDs")
            continue
        trace_id = _identifier(marker.get("traceId"), 32)
        span_id = _identifier(marker.get("spanId"), 16)
        matches = [
            record
            for record in _records(view.get("spans"), "spans")
            if isinstance(record.get("span"), Mapping)
            and record["span"].get("trace_id") == trace_id
            and record["span"].get("span_id") == span_id
        ]
        # Duplicate exports of the same observed span are permitted. They must
        # all retain the original invocation attribution and probe interval.
        if not matches:
            errors.append(f"INCOMPLETE: {name} late_activity trace/span was not collected")
        for record in matches:
            if record.get("requestId") != event.get("requestId"):
                errors.append(f"ASSERTION: {name} late_activity span belongs to a later or peer invocation")
            span = _span(record["span"])
            if not span.start_time.timestamp() * 1000 <= _number(event.get("atMs")) <= span.end_time.timestamp() * 1000:
                errors.append(f"ASSERTION: {name} late_activity happened outside its observed span")
    return errors


def _provider_errors(
    telemetry: Mapping[str, Any], attempts: Mapping[str, Mapping[str, Any]], observation_end: float
) -> list[str]:
    errors: list[str] = []
    if not any(
        first_id != second_id
        and all(first[key] == second[key] for key in ("environmentId", "workerId", "processId"))
        and _number(second["startedAtMs"]) > _number(first["endedAtMs"])
        for first_id, first in attempts.items()
        for second_id, second in attempts.items()
    ):
        errors.append("INCOMPLETE: provider lifetime needs identified sequential worker/process reuse")
    providers = {}
    for provider_record in _records(telemetry.get("providers"), "providers"):
        key = provider_record.get("providerId")
        if not isinstance(key, str) or not key or key in providers or not provider_record.get("exporterId"):
            errors.append("INCOMPLETE: provider/exporter identities must be nonempty and unambiguous")
            continue
        providers[key] = provider_record
    bindings = {}
    for binding in _records(telemetry.get("providerBindings"), "providerBindings"):
        request_id = binding.get("requestId")
        provider_id = binding.get("providerId")
        provider = providers.get(provider_id) if isinstance(provider_id, str) else None
        if not isinstance(request_id, str) or request_id not in attempts or request_id in bindings or provider is None:
            errors.append("INCOMPLETE: every request must bind to one observed provider")
            continue
        bindings[request_id] = provider
        attempt = attempts[request_id]
        for key in ("environmentId", "processId"):
            if provider.get(key) != attempt[key]:
                errors.append(f"ASSERTION: {request_id} provider belongs to another {key}")
        if _number(provider.get("createdAtMs")) > _number(attempt["startedAtMs"]):
            errors.append(f"ASSERTION: {request_id} provider was created during the invocation")
        if provider.get("shutdownAtMs") is not None and _number(provider["shutdownAtMs"]) < _number(
            attempt["endedAtMs"]
        ):
            errors.append(f"ASSERTION: {request_id} provider shut down while its invocation was active")
    if set(bindings) != set(attempts):
        errors.append("INCOMPLETE: provider bindings omit launched requests")
    for first_id, first in attempts.items():
        for second_id, second in attempts.items():
            if (
                (first["environmentId"], first["processId"]) == (second["environmentId"], second["processId"])
                and first_id in bindings
                and second_id in bindings
                and bindings[first_id] != bindings[second_id]
            ):
                errors.append("ASSERTION: requests in one process replaced their shared provider/exporter")
    flushed = set()
    for flush in _records(telemetry.get("flushes"), "flushes"):
        request_id = flush.get("requestId")
        if not isinstance(request_id, str) or request_id not in attempts or request_id in flushed:
            errors.append("INCOMPLETE: each flush must identify one launched request")
            continue
        flushed.add(request_id)
        duration = _number(flush.get("finishedAtMs")) - _number(flush.get("startedAtMs"))
        budget = _number(flush.get("budgetMs"))
        if (
            _number(flush["startedAtMs"]) < _number(attempts[request_id]["startedAtMs"])
            or _number(flush["finishedAtMs"]) > observation_end
        ):
            errors.append(f"INCOMPLETE: {request_id} flush lies outside its lifecycle observation window")
        if budget <= 0 or not 0 <= duration <= budget:
            errors.append(f"ASSERTION: {request_id} flush exceeded its declared budget")
        if flush.get("outcome") not in {"completed", "error", "timeout"}:
            errors.append(f"INCOMPLETE: {request_id} flush has an unknown outcome")
        if flush.get("outcome") in {"error", "timeout"} and not flush.get("error"):
            errors.append(f"INCOMPLETE: {request_id} failed flush has no observed error")
    if flushed != set(attempts):
        errors.append("INCOMPLETE: bounded flush observations omit launched requests")
    for name, view in telemetry["views"].items():
        outcomes = _records(view.get("exportOutcomes"), "exportOutcomes")
        if not any(outcome.get("status") == "delivered" for outcome in outcomes):
            errors.append(f"INCOMPLETE: {name} provider checks need delivered healthy-peer telemetry")
        for outcome in outcomes:
            if outcome.get("status") in {"export-error", "timeout-loss"}:
                expected = "error" if outcome["status"] == "export-error" else "timeout"
                matching = [
                    flush
                    for flush in telemetry["flushes"]
                    if flush.get("requestId") == outcome.get("requestId") and flush.get("outcome") == expected
                ]
                if not matching:
                    errors.append(f"INCOMPLETE: {name} export loss has no matching bounded flush observation")
    return errors


def validate_lmi_telemetry(evidence: Mapping[str, Any], *, phase: str = "final") -> list[str]:
    """Validate LMI telemetry using independently attributed cohort evidence.

    Collection, intentional unsampling and expected fixture export failures are
    distinct states. Compatibility fixtures cannot satisfy genuine carriers.
    This function is called *in addition to* the core lifecycle validator.
    """
    if evidence.get("coverageProfile") == "python-lifecycle":
        from aws_durable_execution_conformance_tests_otel.lmi_regressions import validate_lifecycle_telemetry

        return validate_lifecycle_telemetry(evidence, phase=phase)
    try:
        if phase not in {"observations", "final"}:
            return ["INCOMPLETE: unknown telemetry validation phase"]
        artifact = evidence.get("artifact", {})
        if not isinstance(artifact, Mapping) or not isinstance(artifact.get("plugin"), str) or not artifact["plugin"]:
            return ["INCOMPLETE: LMI telemetry requires the exact plugin artifact identity"]
        runtime = evidence.get("runtime")
        if runtime not in _SOURCES:
            return ["INCOMPLETE: unknown telemetry runtime profile"]
        attempts = {}
        for attempt in _records(evidence.get("attempts"), "attempts"):
            request_id = attempt.get("requestId")
            if not isinstance(request_id, str) or not request_id or request_id in attempts:
                return ["INCOMPLETE: telemetry needs unique original and retry request identities"]
            attempts[request_id] = attempt
        if not attempts:
            return ["INCOMPLETE: telemetry has no launched requests"]
        telemetry = evidence.get("telemetry")
        if not isinstance(telemetry, Mapping) or not isinstance(telemetry.get("views"), Mapping):
            return ["INCOMPLETE: both telemetry views must be collected"]
        if any(not isinstance(view, Mapping) for view in telemetry["views"].values()):
            return ["INCOMPLETE: telemetry views must be objects"]
        observation_end = _number(evidence.get("observationEndedAtMs"))
        execution_starts = _service_execution_starts(evidence.get("histories", []))
        errors = []
        for name in ("invocation", "execution"):
            view = telemetry["views"].get(name)
            if not isinstance(view, Mapping):
                errors.append(f"INCOMPLETE: missing {name} view")
                continue
            errors.extend(
                _view_errors(
                    name,
                    view,
                    attempts,
                    runtime,
                    observation_end,
                    execution_starts=execution_starts,
                    operation_histories=_records(evidence.get("histories", []), "histories"),
                )
            )
            if evidence.get("requirementId") == "lmi-lifecycle-7":
                errors.extend(_late_activity_errors(evidence, name, view))
                if any(
                    item.get("status") in {"export-error", "timeout-loss"}
                    for item in _records(view.get("exportOutcomes"), "exportOutcomes")
                ):
                    errors.append(f"INCOMPLETE: {name} export loss cannot establish attribution coverage")
                selected = evidence.get("selectedRequests", {})
                delivered = {
                    item.get("requestId") for item in view["exportOutcomes"] if item.get("status") == "delivered"
                }
                if not isinstance(selected, Mapping) or len(selected) < 2 or not set(selected.values()) <= delivered:
                    errors.append(
                        f"INCOMPLETE: {name} attribution requires sampled delivery for the original victim and peer"
                    )
        if evidence.get("requirementId") == "lmi-lifecycle-8":
            errors.extend(_provider_errors(telemetry, attempts, observation_end))
        return errors
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        return [f"INCOMPLETE: invalid LMI telemetry evidence: {exc}"]
