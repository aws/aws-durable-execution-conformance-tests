# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Language-neutral synchronous and asynchronous export lifecycle contracts.

The export mode describes processor behavior, not a programming language.
Returning from a bounded flush wait and settling its underlying work are separate
observations. Call the core common ownership/retirement validator as well.
"""

from __future__ import annotations

from collections.abc import Mapping
from itertools import pairwise
from typing import Any

from aws_durable_execution_conformance_tests_otel.lmi import (
    _number,
    _records,
    _service_execution_starts,
    _span,
    _view_errors,
)
from aws_durable_execution_conformance_tests_otel.model import TelemetryQuery, Trace
from aws_durable_execution_conformance_tests_otel.validators import validate_trace

EXPORT_MODES = ("sync", "async-bounded-flush")
EXPORT_SCENARIOS = ("export-success", "export-failure-recovery", "held-export")


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonempty string")
    return value


def _compare_events(left: Mapping[str, Any], right: Mapping[str, Any]) -> int:
    """Compare actual boundaries; local sequences do not order other requests."""
    left_time, right_time = _number(left.get("atMs")), _number(right.get("atMs"))
    if left_time != right_time:
        return -1 if left_time < right_time else 1
    if left.get("requestId") != right.get("requestId"):
        raise ValueError("causal order between different requests at the same timestamp is not established")
    left_sequence, right_sequence = left.get("sequence"), right.get("sequence")
    if type(left_sequence) is not int or type(right_sequence) is not int or min(left_sequence, right_sequence) < 0:
        raise ValueError("causal boundaries require valid per-request sequence numbers")
    return (left_sequence > right_sequence) - (left_sequence < right_sequence)


class _Exports:
    def __init__(self, evidence: Mapping[str, Any]) -> None:
        self.value = evidence
        self.errors: list[str] = []
        self.mode = evidence.get("exportMode")
        self.scenario = evidence.get("scenario")
        self.view_name = _text(evidence.get("otelView"), "otelView")
        self.attempts: dict[str, Mapping[str, Any]] = {}
        self.events: list[Mapping[str, Any]] = []
        self.starts: dict[str, Mapping[str, Any]] = {}
        self.ends: dict[str, Mapping[str, Any]] = {}
        self.flushes: dict[str, Mapping[str, Any]] = {}
        self.flush_ends: dict[str, Mapping[str, Any]] = {}
        self.wrappers: dict[str, Mapping[str, Any]] = {}
        self.bindings: dict[str, Mapping[str, Any]] = {}
        self.budgets: Mapping[str, Any] = {}
        self.observed_until = _number(evidence.get("observationEndedAtMs"))

    def fail(self, message: str, category: str = "ASSERTION") -> None:
        self.errors.append(f"{category}: {message}")

    def budget(self, name: str) -> float:
        value = _number(self.budgets.get(name))
        if value <= 0:
            raise ValueError(f"exportLifecycle.budgets.{name} must be positive")
        return value

    def selected(self, role: str) -> str:
        selected = self.value.get("selectedRequests", {})
        request_id = selected.get(role) if isinstance(selected, Mapping) else None
        if request_id not in self.attempts:
            raise ValueError(f"missing selected original {role} request")
        original = self.attempts[request_id]
        if original.get("attempt") != 1 or original.get("role") != role:
            self.fail(f"selected {role} is a retry or a substituted request", "PLACEMENT")
        return str(request_id)

    def index(self, kind: str, key: str) -> dict[str, Mapping[str, Any]]:
        result = {}
        for event in self.events:
            if event.get("kind") != kind:
                continue
            identity = _text(event.get(key), f"{kind}.{key}")
            if identity in result:
                raise ValueError(f"duplicate {kind} identity {identity}")
            result[identity] = event
        return result

    def setup(self) -> None:
        if (
            self.value.get("coverageProfile") != "export-lifecycle"
            or self.value.get("requirementId") != "lmi-lifecycle-16"
        ):
            raise ValueError("export lifecycle requires its explicit coverage profile and requirement 16")
        if self.value.get("runtime") not in ("javascript", "java", "python"):
            raise ValueError("unsupported runtime")
        if (
            self.mode not in EXPORT_MODES
            or self.scenario not in EXPORT_SCENARIOS
            or self.view_name not in ("invocation", "execution")
        ):
            raise ValueError("unknown export mode, scenario or telemetry view")
        _text(self.value.get("artifact", {}).get("plugin"), "artifact.plugin")
        self.budgets = self.value.get("exportLifecycle", {}).get("budgets", {})
        for key in ("exportCompletionMs", "observationGraceMs"):
            self.budget(key)
        if self.mode == "async-bounded-flush":
            self.budget("flushWaitMs")
        for attempt in _records(self.value.get("attempts"), "attempts"):
            request = _text(attempt.get("requestId"), "attempt.requestId")
            if request in self.attempts:
                raise ValueError("duplicate native request identity")
            self.attempts[request] = attempt
            if attempt.get("outcome") != "SUCCEEDED":
                self.fail("export fixtures must preserve the original successful handler outcome")
        if not self.attempts:
            raise ValueError("no native invocations were observed")
        self.events = _records(self.value.get("events"), "events")
        sequences: set[tuple[str, int]] = set()
        for event in self.events:
            request = _text(event.get("requestId"), "event.requestId")
            if not isinstance(request, str) or request not in self.attempts:
                raise ValueError("event references an unknown original request")
            if (
                event.get("ownerRequestId") != request
                or event.get("executionArn") != self.attempts[request]["executionArn"]
            ):
                self.fail("event was attributed to another invocation or execution")
            sequence = event.get("sequence")
            if type(sequence) is not int or sequence < 0 or (request, sequence) in sequences:
                raise ValueError("events require unique nonnegative per-request sequence numbers")
            sequences.add((request, sequence))
            if not _number(self.attempts[request]["startedAtMs"]) <= _number(event.get("atMs")) <= self.observed_until:
                raise ValueError("event lies outside its original observation window")
        for request in self.attempts:
            ordered = sorted(
                (event for event in self.events if event["requestId"] == request), key=lambda event: event["sequence"]
            )
            if any(_number(first["atMs"]) > _number(second["atMs"]) for first, second in pairwise(ordered)):
                raise ValueError("per-request event sequence contradicts the normalized boundary timestamps")
        self.wrappers = self.index("wrapper_return", "requestId")
        if set(self.wrappers) != set(self.attempts):
            raise ValueError("every original invocation needs actual wrapper return evidence")
        for request, wrapper in self.wrappers.items():
            if wrapper.get("outcome") != "SUCCEEDED":
                self.fail(f"{request} did not preserve its successful wrapper outcome")
            if self.observed_until < _number(wrapper["atMs"]) + self.budget("observationGraceMs"):
                raise ValueError("post-wrapper collection is shorter than observationGraceMs")
        watermark = self.value.get("collection", {}).get("watermarks", {}).get("exportLifecycle")
        if _number(watermark) < self.observed_until:
            raise ValueError("export lifecycle collection does not cover the complete observation window")
        self.starts, self.ends = self.index("export_started", "exportId"), self.index("export_completed", "exportId")
        if not self.starts or set(self.starts) != set(self.ends):
            raise ValueError("every admitted export needs actual completion evidence")
        if {event["requestId"] for event in self.starts.values()} != set(self.attempts):
            raise ValueError("every original invocation must own observed export work")
        self.selected("victim")

    def providers(self) -> None:
        telemetry = self.value.get("telemetry", {})
        providers = {}
        for provider_record in _records(telemetry.get("providers"), "providers"):
            identity = _text(provider_record.get("providerId"), "providerId")
            if identity in providers:
                raise ValueError("duplicate provider identity")
            _text(provider_record.get("exporterId"), "exporterId")
            _number(provider_record.get("createdAtMs"))
            if (
                provider_record.get("shutdownAtMs") is not None
                and _number(provider_record["shutdownAtMs"]) <= self.observed_until
            ):
                self.fail("an invocation shut down a process-owned provider during observation")
            providers[identity] = provider_record
        process_owners: dict[tuple[str, str], tuple[str, str]] = {}
        for binding in _records(telemetry.get("providerBindings"), "providerBindings"):
            request = binding.get("requestId")
            provider = providers.get(binding.get("providerId", ""))
            if request not in self.attempts or request in self.bindings or provider is None:
                raise ValueError("provider bindings must identify each original request exactly once")
            attempt = self.attempts[request]
            if any(provider.get(key) != attempt[key] for key in ("environmentId", "processId")):
                self.fail("provider belongs to another environment or process")
            process = (attempt["environmentId"], attempt["processId"])
            provider_identity = (provider["providerId"], provider["exporterId"])
            if process_owners.setdefault(process, provider_identity) != provider_identity:
                self.fail("invocations in a warm process replaced their shared provider/exporter")
            self.bindings[request] = provider
        if set(self.bindings) != set(self.attempts):
            raise ValueError("provider bindings omit an invocation")

    def identity(self, started: Mapping[str, Any], ended: Mapping[str, Any]) -> bool:
        if any(started.get(key) != ended.get(key) for key in ("requestId", "executionArn", "providerId", "exporterId")):
            self.fail("completion substituted the original export/flush owner")
            return False
        provider = self.bindings[started["requestId"]]
        if any(started.get(key) != provider[key] for key in ("providerId", "exporterId")):
            self.fail("export/flush used another request's provider")
        if _number(provider["createdAtMs"]) > _number(started["atMs"]):
            self.fail("export/flush predates the observed provider")
        return True

    def exports(self) -> None:
        failed_request = self.selected("victim") if self.scenario == "export-failure-recovery" else None
        for identity, started in self.starts.items():
            completed = self.ends[identity]
            self.identity(started, completed)
            duration = _number(completed["atMs"]) - _number(started["atMs"])
            if not 0 <= duration <= self.budget("exportCompletionMs") or _compare_events(started, completed) > 0:
                self.fail(f"export {identity} exceeded exportCompletionMs or reversed time")
            expected = "failed" if started["requestId"] == failed_request else "succeeded"
            if completed.get("outcome") != expected:
                self.fail(f"export {identity} must have observed outcome {expected}")
            if expected == "failed" and not completed.get("error"):
                raise ValueError("failed export lacks an observed error")
            if self.mode == "sync" and _compare_events(completed, self.wrappers[started["requestId"]]) > 0:
                self.fail(
                    "synchronous export did not settle before actual wrapper return; a flush timeout cannot replace completion"
                )
        if self.scenario == "export-failure-recovery":
            victim, warm = self.selected("victim"), self.selected("warm")
            if self.attempts[victim]["executionArn"] == self.attempts[warm]["executionArn"]:
                self.fail("recovery used a retry of the original execution", "PLACEMENT")
            if (
                any(self.attempts[victim][key] != self.attempts[warm][key] for key in ("environmentId", "processId"))
                or self.bindings[victim] != self.bindings[warm]
            ):
                self.fail("healthy recovery did not reuse the original process-owned provider", "PLACEMENT")
            failed_end = max(_number(event["atMs"]) for event in self.ends.values() if event["requestId"] == victim)
            if _number(self.attempts[warm]["startedAtMs"]) <= max(failed_end, _number(self.wrappers[victim]["atMs"])):
                self.fail("healthy recovery did not follow the original export failure and wrapper return")

    def pending(self, event: Mapping[str, Any]) -> set[str]:
        """Known case-owned work, without assuming the provider is globally idle."""
        expected = {
            identity
            for identity, start in self.starts.items()
            if start["providerId"] == event["providerId"]
            and start["exporterId"] == event["exporterId"]
            and _compare_events(start, event) <= 0
            and _compare_events(event, self.ends[identity]) < 0
        }
        observed = event.get("pendingExportIds")
        if (
            not isinstance(observed, list)
            or any(not isinstance(item, str) for item in observed)
            or len(observed) != len(set(observed))
            or set(observed) != expected
        ):
            raise ValueError("flush pendingExportIds must match observed case-owned export work at that boundary")
        return expected

    def flush(self) -> None:
        if self.mode == "sync":
            return  # A later flush is not evidence that the earlier export returned.
        self.flushes = self.index("flush_started", "flushId")
        waits = self.index("flush_wait_completed", "flushId")
        self.flush_ends = self.index("flush_completed", "flushId")
        if not self.flushes or set(self.flushes) != set(waits) or set(self.flushes) != set(self.flush_ends):
            raise ValueError("async mode requires separate flush start, wait return and actual work completion")
        requests = [event["requestId"] for event in self.flushes.values()]
        if len(requests) != len(set(requests)) or set(requests) != set(self.attempts):
            raise ValueError("every original request needs its own observed bounded flush")
        for identity, started in self.flushes.items():
            waited, completed = waits[identity], self.flush_ends[identity]
            self.identity(started, waited)
            self.identity(started, completed)
            work = self.pending(started)
            pending = self.pending(waited)
            self.pending(completed)
            if (
                not 0 <= _number(waited["atMs"]) - _number(started["atMs"]) <= self.budget("flushWaitMs")
                or _compare_events(started, waited) > 0
            ):
                self.fail("SDK flush wait exceeded the immutable flushWaitMs budget")
            if _compare_events(waited, self.wrappers[started["requestId"]]) > 0:
                self.fail("wrapper returned before its bounded flush wait returned")
            owned = {key for key, event in self.starts.items() if event["requestId"] == started["requestId"]}
            if any(_compare_events(self.starts[key], started) > 0 for key in owned):
                self.fail("invocation admitted more export work after starting final flush")
            if any(
                _compare_events(prerequisite, completed) > 0
                for prerequisite in [started, *(self.ends[key] for key in work)]
            ):
                self.fail("underlying flush work was declared settled before its observed exports")
            expected = "failed" if any(self.ends[key]["outcome"] == "failed" for key in owned | work) else "succeeded"
            if completed.get("outcome") != expected:
                self.fail("actual flush completion does not match its export outcomes")
            wait_outcome = waited.get("outcome")
            if wait_outcome == "timeout":
                if _compare_events(completed, waited) <= 0:
                    self.fail("timed-out wait was confused with already completed flush work")
            elif wait_outcome in ("completed", "error"):
                if pending or _compare_events(completed, waited) > 0:
                    self.fail("flush wait reported completion while export work was still pending")
                if (wait_outcome == "completed") != (expected == "succeeded"):
                    self.fail("flush wait outcome did not preserve the underlying failure")
                if wait_outcome == "error" and not waited.get("error"):
                    raise ValueError("failed flush wait needs observed error evidence")
            else:
                raise ValueError("flush wait outcome must be completed, timeout or error")

    def held(self) -> None:
        if self.scenario != "held-export":
            return
        victim = self.selected("victim")
        held = self.index("export_held", "exportId")
        released = self.index("export_released", "exportId")
        owned = {key for key, event in self.starts.items() if event["requestId"] == victim}
        if not held or not set(held) <= owned or set(released) != set(held):
            raise ValueError("a nonempty victim-owned held subset must retain its exact gate and release observations")
        created = _records(self.value.get("faultsCreated"), "faultsCreated")
        wrapper = self.wrappers[victim]
        wait = None
        pending: set[str] = set()
        if self.mode == "async-bounded-flush":
            waits = [
                event
                for event in self.events
                if event.get("kind") == "flush_wait_completed" and event["requestId"] == victim
            ]
            if len(waits) != 1 or waits[0].get("outcome") != "timeout":
                self.fail("async held export needs an actual timed-out wait with original export work still pending")
            else:
                wait = waits[0]
                pending = self.pending(wait)
        held_across_return = False
        for identity, start in held.items():
            release = released[identity]
            self.identity(self.starts[identity], start)
            self.identity(self.starts[identity], release)
            gates = [
                gate
                for gate in created
                if gate.get("id") == start.get("gateId") and gate.get("ownerCaseId") == self.value.get("caseId")
            ]
            if (
                len(gates) != 1
                or not start.get("gateId")
                or start.get("gateId") != release.get("gateId")
                or release.get("mode") != "normal"
            ):
                raise ValueError("held export needs a case-owned normal measurement release")
            gate = gates[0]
            requested = None
            if "releaseRequestedAtMs" in gate or "releaseAcknowledgedAtMs" in gate:
                # The driver observes PUT intent/acknowledgement; each worker
                # independently observes GET receipt. A receipt can be visible
                # before the PUT acknowledgement, or arrive substantially later.
                requested = _number(gate.get("releaseRequestedAtMs"))
                acknowledged = _number(gate.get("releaseAcknowledgedAtMs"))
                if requested > acknowledged or _number(release["atMs"]) < requested:
                    raise ValueError("gate release interval is reversed or its receipt precedes the release request")
            elif _number(gate.get("releasedAtMs")) != _number(release["atMs"]):
                raise ValueError("legacy held export needs its exact normal measurement release timestamp")
            if not (
                _compare_events(self.starts[identity], start) <= 0
                and _compare_events(start, release) < 0
                and _compare_events(release, self.ends[identity]) <= 0
            ):
                self.fail("export hold and release were not inside the original export work")
            if self.mode == "sync":
                if _compare_events(self.ends[identity], wrapper) > 0:
                    self.fail("sync wrapper returned while its export was held")
            else:
                if requested is not None and requested <= _number(wrapper["atMs"]):
                    self.fail("async gate release request did not follow actual wrapper return")
                if (
                    wait is not None
                    and identity in pending
                    and _compare_events(start, wait) <= 0
                    and _compare_events(start, wrapper) < 0
                    and _compare_events(wrapper, release) < 0
                ):
                    held_across_return = True
        if self.mode == "async-bounded-flush" and not held_across_return:
            self.fail(
                "async bounded flush did not return while an original export remained held across its wait and wrapper"
            )

    def spans(self) -> None:
        views = self.value.get("telemetry", {}).get("views", {})
        if (
            not isinstance(views, Mapping)
            or set(views) != {self.view_name}
            or not isinstance(views[self.view_name], Mapping)
        ):
            raise ValueError("collect exactly the declared otelView for this matrix cell")
        view = views[self.view_name]
        records = _records(view.get("spans"), "spans")
        seen = set()
        for record in records:
            export_id = record.get("exportId")
            if export_id not in self.starts or record.get("requestId") != self.starts[export_id]["requestId"]:
                self.fail("canonical span substituted its independently recorded export owner")
            else:
                seen.add(export_id)
                span = _span(record.get("span"))
                if span.end_time.timestamp() * 1000 > _number(self.ends[export_id]["atMs"]):
                    self.fail("canonical span ended after its export was declared completed")
        if seen != set(self.starts):
            raise ValueError("canonical spans omit admitted export batches")
        logical: dict[tuple[str, str], Mapping[str, Any]] = {}
        for record in records:
            span = _span(record.get("span"))
            key = (span.trace_id, span.span_id)
            previous = logical.setdefault(key, record)
            if previous.get("requestId") != record.get("requestId") or previous.get("span") != record.get("span"):
                self.fail("re-export changed a logical span's original ownership or data")
        records = list(logical.values())
        assertions = {}
        histories = {history["executionArn"]: history for history in _records(self.value.get("histories"), "histories")}
        for request_id, attempt in self.attempts.items():
            if histories.get(attempt["executionArn"], {}).get("status") != "SUCCEEDED":
                self.fail("export fixture changed the successful durable execution outcome")
            assertions[attempt["executionArn"]] = {
                "minimum_spans": 2,
                "span_assertions": [
                    {"select": {"name": "Invocation"}, "expect": {}},
                    {"select": {"name": "Workflow"}, "expect": {"status": "OK"}},
                ],
            }
            selected = [_span(record["span"]) for record in records if record.get("requestId") == request_id]
            if selected:
                query = TelemetryQuery(
                    attempt["executionArn"],
                    selected[0].service_name or "",
                    min(span.start_time for span in selected),
                    max(span.end_time for span in selected),
                )
                self.errors.extend(
                    f"ASSERTION: {error}"
                    for error in validate_trace(
                        Trace(selected[0].trace_id, tuple(selected)), assertions[attempt["executionArn"]], query
                    )
                )
        outcomes = []
        for request in self.attempts:
            failed = any(
                event.get("outcome") == "failed" for event in self.ends.values() if event["requestId"] == request
            )
            outcomes.append(
                {
                    "requestId": request,
                    "status": "export-error" if failed else "delivered",
                    **({"fixture": "bounded-export-error", "error": "observed export failure"} if failed else {}),
                }
            )
        for carrier in _records(view.get("carriers"), "carriers"):
            if carrier.get("expectedSampled") is not True:
                self.fail("export lifecycle fixtures require explicit sampled telemetry")
        self.errors.extend(
            _view_errors(
                self.view_name,
                {**view, "spans": records, "assertions": assertions, "exportOutcomes": outcomes},
                self.attempts,
                str(self.value["runtime"]),
                self.observed_until,
                execution_starts=_service_execution_starts(self.value.get("histories", [])),
            )
        )


def _retirement_errors(evidence: Mapping[str, Any]) -> list[str]:
    """Audit all admitted or inventoried work even when observations are invalid."""
    errors = []
    try:
        created = _records(evidence.get("resourcesCreated"), "resourcesCreated")
        retirement = evidence.get("retirement", {})
        settled = _records(retirement.get("resources"), "retirement.resources")
        retired = _number(retirement.get("retiredAtMs"))
        events = _records(evidence.get("events"), "events")
        for kind in ("export", "flush"):
            observed_kinds = {f"{kind}_started", f"{kind}_completed", f"{kind}_wait_completed"}
            if kind == "export":
                observed_kinds.update(("export_held", "export_released"))
            identities = {item.get("id") for item in created if item.get("kind") == kind}
            identities.update(event.get(f"{kind}Id") for event in events if event.get("kind") in observed_kinds)
            for identity in identities:
                owned = [item for item in created if item.get("kind") == kind and item.get("id") == identity]
                finished = [item for item in settled if item.get("kind") == kind and item.get("id") == identity]
                started = [
                    event
                    for event in events
                    if event.get("kind") == f"{kind}_started" and event.get(f"{kind}Id") == identity
                ]
                completed = [
                    event
                    for event in events
                    if event.get("kind") == f"{kind}_completed" and event.get(f"{kind}Id") == identity
                ]
                if len(owned) != 1 or len(finished) != 1 or len(started) != 1 or len(completed) != 1:
                    errors.append(
                        f"INCOMPLETE: {kind} retirement {identity!r} lacks its owned start, actual completion or settlement"
                    )
                    continue
                record, end = finished[0], completed[0]
                if any(item.get("ownerCaseId") != evidence.get("caseId") for item in owned + finished):
                    errors.append(f"ASSERTION: {kind} retirement changed another case's resource")
                if any(
                    started[0].get(key) != end.get(key)
                    for key in ("requestId", "executionArn", "providerId", "exporterId")
                ):
                    errors.append(f"ASSERTION: {kind} retirement substituted the original completion owner")
                _text(record.get("evidenceUri"), "retirement.evidenceUri")
                if (
                    end.get("outcome") not in ("succeeded", "failed")
                    or record.get("outcome") != end.get("outcome")
                    or _compare_events(started[0], end) > 0
                    or not _number(started[0].get("atMs"))
                    <= _number(end.get("atMs"))
                    <= _number(record.get("settledAtMs"))
                    <= retired
                ):
                    errors.append(f"ASSERTION: {kind} retirement preceded actual completion or replaced its outcome")
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError) as error:
        errors.append(f"INCOMPLETE: export retirement evidence is malformed: {error}")
    return errors


def validate_export_lifecycle(evidence: Mapping[str, Any], *, phase: str = "final") -> list[str]:
    """Check mode-specific observations without attributing behavior to language."""
    if phase not in ("observations", "final"):
        return ["INCOMPLETE: unknown export validation phase"]
    validator: _Exports | None = None
    try:
        validator = _Exports(evidence)
        validator.setup()
        validator.providers()
        validator.exports()
        validator.flush()
        validator.held()
        validator.spans()
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError) as error:
        errors = [*(validator.errors if validator else []), f"INCOMPLETE: malformed export lifecycle evidence: {error}"]
    else:
        errors = validator.errors
    if phase == "final":
        errors.extend(_retirement_errors(evidence))
    return errors
