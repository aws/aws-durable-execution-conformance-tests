# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Adjudication: the LMI lifecycle verdict, derived from collected evidence alone."""

from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from graphlib import CycleError, TopologicalSorter
from itertools import pairwise
from typing import Any

from aws_durable_execution_conformance_tests.lmi.evidence import (
    _WORK_INTERVALS,
    EvidenceValidator,
    is_json_integer,
    normalize_retirement_events,
    validate_retirement_evidence,
)
from aws_durable_execution_conformance_tests.lmi.rules import common as common_rules
from aws_durable_execution_conformance_tests.lmi.rules import deadline as deadline_rules
from aws_durable_execution_conformance_tests.lmi.rules import disposal as disposal_rules
from aws_durable_execution_conformance_tests.lmi.rules import overlap as overlap_rules
from aws_durable_execution_conformance_tests.lmi.rules import recovery as recovery_rules
from aws_durable_execution_conformance_tests.lmi.rules import replay as replay_rules
from aws_durable_execution_conformance_tests.lmi.rules import retirement as retirement_rules
from aws_durable_execution_conformance_tests.lmi.rules import warm_reuse as warm_reuse_rules
from aws_durable_execution_conformance_tests.lmi.rules.cutoff import cutoff_obligations
from aws_durable_execution_conformance_tests.lmi.rules.engine import audit_cutoff, establish, retire
from aws_durable_execution_conformance_tests.lmi.rules.reconcile import ReconcileView
from aws_durable_execution_conformance_tests.lmi.rules.replay import (
    _side_effect_observation_errors,
    cleanup_replay_projection_errors,
)
from aws_durable_execution_conformance_tests.lmi.rules.view import CaseView


def _retry_attempt_order(evidence: Mapping[str, Any]) -> tuple[str, ...] | None:
    replay = evidence.get("replay")
    retry = replay.get("retry") if isinstance(replay, Mapping) else None
    attempts = retry.get("attempts") if isinstance(retry, Mapping) else None
    if not isinstance(attempts, list):
        return None
    identifiers = []
    for attempt in attempts:
        identifier = attempt.get("attemptId") if isinstance(attempt, Mapping) else None
        if not isinstance(identifier, str) or not identifier.strip():
            return None
        identifiers.append(identifier)
    # Singleton fragments establish no ordering constraint. The caller diagnoses
    # duplicate identities before using an observed sequence as an ordering.
    if len(identifiers) < 2:
        return None
    return tuple(identifiers)


def _side_effect_progress_errors(snapshots: Sequence[Mapping[str, Any]]) -> list[str]:
    counts: dict[str, int | float] = {}
    expectations: dict[str, int | float] = {}
    errors: list[str] = []
    for snapshot in snapshots:
        replay = snapshot.get("replay")
        records = replay.get("sideEffects") if isinstance(replay, Mapping) else None
        for record in records if isinstance(records, list) else ():
            if not isinstance(record, Mapping) or not isinstance(operation := record.get("operationId"), str):
                continue
            count, expected = record.get("count"), record.get("expectedCount")
            if is_json_integer(count) and count >= 0:
                if operation in counts and count < counts[operation]:
                    errors.append(
                        f"ASSERTION: observed side-effect count differs from earlier observations for {operation}"
                    )
                counts[operation] = max(count, counts.get(operation, count))
            if is_json_integer(expected) and expected >= 0:
                first = expectations.setdefault(operation, expected)
                if expected != first:
                    errors.append(
                        f"ASSERTION: declared side-effect expectation for {operation} changed across observations"
                    )
    return errors


def _project_retirement_completions(
    events: Sequence[Mapping[str, Any]],
    completions: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    work_intervals = {start: (end, identity) for start, end, identity in _WORK_INTERVALS}
    starts: set[tuple[str, str, str]] = set()
    for event in normalize_retirement_events([event for event in events if isinstance(event, Mapping)], []):
        kind = event.get("kind")
        interval = work_intervals.get(kind) if isinstance(kind, str) else None
        if interval:
            end_kind, identity = interval
            request, work_id = event.get("requestId"), event.get(identity)
            if isinstance(request, str) and isinstance(work_id, str):
                starts.add((end_kind, request, work_id))

    identity_by_end = {end: identity for _, end, identity in _WORK_INTERVALS}
    projected = []
    valid = [completion for completion in completions if isinstance(completion, Mapping)]
    for original, normalized in zip(valid, normalize_retirement_events(valid, []), strict=True):
        kind = normalized.get("kind")
        if not isinstance(kind, str) or kind not in identity_by_end:
            continue
        request, work_id = normalized.get("requestId"), normalized.get(identity_by_end[kind])
        if isinstance(request, str) and isinstance(work_id, str) and (kind, request, work_id) in starts:
            projected.append(original)
    return projected


def _cleanup_behavior_errors(
    original: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    *,
    standard: bool,
    known_snapshots: Sequence[Mapping[str, Any]] = (),
    observation_snapshots: Sequence[Mapping[str, Any]] = (),
) -> list[str]:
    """Detect specific refutations without replacing frozen business evidence."""
    view = ReconcileView(
        original,
        snapshot,
        standard=standard,
        known_snapshots=known_snapshots,
        observation_snapshots=observation_snapshots,
    )
    common_rules.reconcile_records(view)
    common_rules.reconcile_history_expectations(view)
    common_rules.reconcile_sdk_calls(view)
    releases = retirement_rules.collect_fault_releases(view)
    common_rules.reconcile_admission_closures(view)
    if not standard or not view.has_selection:
        return view.findings
    requirement = view.requirement
    if requirement in ("lmi-lifecycle-2", "lmi-lifecycle-7"):
        disposal_rules.reconcile_disposal(view)
    marker_ids: dict[str, set[str]] = {}
    corrected = common_rules.reconcile_admission_order(view)
    if requirement == "lmi-lifecycle-1":
        marker_ids = overlap_rules.reconcile_overlap_identity(view, corrected)
    if requirement in ("lmi-lifecycle-1", "lmi-lifecycle-4"):
        overlap_rules.reconcile_barrier(view, marker_ids)
    common_rules.reconcile_unique_events(view)
    if requirement == "lmi-lifecycle-4":
        recovery_rules.reconcile_recovery_event(view)
    if requirement in ("lmi-lifecycle-3", "lmi-lifecycle-4"):
        timeout_events = deadline_rules.reconcile_deadline(view)
        if requirement == "lmi-lifecycle-4":
            recovery_rules.reconcile_recovery(view, timeout_events)
        deadline_rules.reconcile_fault_release(view, releases)
    if requirement == "lmi-lifecycle-6":
        warm_reuse_rules.reconcile_warm_reuse(view)
    if requirement == "lmi-lifecycle-4":
        recovery_rules.reconcile_python_recovery(view)
    if requirement == "lmi-lifecycle-5":
        replay_rules.reconcile_replay(view)
    elif requirement == "lmi-lifecycle-6":
        warm_reuse_rules.reconcile_late_activity(view)
    return view.findings


def _evaluate_capture(
    evidence: Mapping[str, Any],
    *,
    retirement_evidence: Mapping[str, Any],
    cleanup_evidence: Mapping[str, Sequence[Mapping[str, Any]]],
    standard: bool,
    validator: EvidenceValidator | None = None,
    observation_evidence: Sequence[Mapping[str, Any]] = (),
    attempts: Sequence[Mapping[str, Any]] = (),
) -> tuple[list[str], list[str], dict[str, Any]]:
    """Evaluate captured evidence without querying a target or mutating the capture.

    ``validator=None`` selects the shared standard rules, including the
    lifecycle 2/7 allowance for retries proven during retirement. A supplied
    validator is a custom suite policy and is applied as given.
    """
    if not evidence:
        return [], ["INCOMPLETE: no captured observations"], {}

    established: CaseView | None = None
    try:
        # The shared rules check the frozen observations once here and audit the
        # projected retirement once below; a custom validator is called twice.
        if validator is None:
            established = establish(copy.deepcopy(dict(evidence)))
            findings = list(established.errors)
        else:
            findings = list(validator(copy.deepcopy(dict(evidence)), phase="observations"))
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError, RecursionError):
        return [], ["INCOMPLETE: malformed captured observations"], dict(evidence)
    # Each snapshot contributes only its own adjacent ordering edges. The
    # snapshots contradict one another exactly when those edges form a cycle,
    # including across fragments that share no more than one attempt.
    retry_successors: dict[str, set[str]] = {}
    for snapshot in observation_evidence:
        findings.extend(_side_effect_observation_errors(snapshot))
        order = _retry_attempt_order(snapshot)
        if order is None:
            continue
        if len(set(order)) != len(order):
            findings.append("INCOMPLETE: duplicate retry attemptId in observations")
            continue
        for before, after in pairwise(order):
            retry_successors.setdefault(before, set()).add(after)
    try:
        TopologicalSorter(retry_successors).prepare()
    except CycleError:
        findings.append("ASSERTION: observed retry attempt orders contradict each other across observations")

    findings.extend(_side_effect_progress_errors(observation_evidence))
    observation_findings = list(findings)
    final_evidence = dict(evidence)
    try:
        final_evidence = copy.deepcopy(final_evidence)
        frames = [frame for snapshots in cleanup_evidence.values() for frame in snapshots]
        retirement_snapshot = retirement_evidence or evidence
        findings.extend(
            _cleanup_behavior_errors(
                evidence,
                retirement_snapshot,
                standard=standard,
                known_snapshots=frames,
                observation_snapshots=observation_evidence,
            )
        )
        for frame in [*frames, retirement_snapshot]:
            if isinstance(replay := frame.get("replay"), Mapping):
                findings.extend(cleanup_replay_projection_errors(replay))

        raw_retirement = retirement_evidence.get("retirement")
        if isinstance(raw_retirement, Mapping):
            retirement = copy.deepcopy(dict(raw_retirement))
            for field, inventory in (("resources", "resourcesCreated"), ("faults", "faultsCreated")):
                original = evidence.get(inventory)
                original_ids = {
                    item["id"]
                    for item in (original if isinstance(original, list) else [])
                    if isinstance(item, Mapping) and isinstance(item.get("id"), str)
                }
                settled = retirement.get(field)
                retirement[field] = [
                    item
                    for item in (settled if isinstance(settled, list) else [])
                    if isinstance(item, Mapping) and isinstance(item.get("id"), str) and item["id"] in original_ids
                ]
            window = evidence.get("observationEndedAtMs")
            existing_completions = retirement.get("completions")
            candidates = []
            if "completions" in retirement:
                if not isinstance(existing_completions, list):
                    findings.append("INCOMPLETE: malformed captured retirement completions")
                else:
                    identities = {kind: identity for _, kind, identity in _WORK_INTERVALS}
                    records = [item for item in existing_completions if isinstance(item, Mapping)]
                    normalized = normalize_retirement_events(records, findings)
                    for item, shape in zip(records, normalized, strict=True):
                        kind = shape.get("kind")
                        identity = identities.get(kind) if isinstance(kind, str) else None
                        if (
                            identity is not None
                            and isinstance(shape.get("requestId"), str)
                            and isinstance(shape.get(identity), str)
                        ):
                            candidates.append(item)
                    if len(candidates) != len(existing_completions):
                        findings.append("INCOMPLETE: malformed captured retirement completions")
            final_events = retirement_evidence.get("events")
            if isinstance(window, (int, float)) and not isinstance(window, bool) and isinstance(final_events, list):
                candidates.extend(
                    event
                    for event in final_events
                    if isinstance(event, Mapping)
                    and isinstance(at := event.get("atMs"), (int, float))
                    and not isinstance(at, bool)
                    and at > window
                )
            if candidates or "completions" in retirement:
                original_events = evidence.get("events")
                completions = _project_retirement_completions(
                    original_events if isinstance(original_events, list) else [], candidates
                )
                if completions or "completions" in retirement:
                    retirement["completions"] = completions
            final_evidence["retirement"] = retirement
        _, deferred = cutoff_obligations(evidence)
        observed_attempts = evidence.get("attempts")
        observed_requests = {
            attempt["requestId"]
            for attempt in (observed_attempts if isinstance(observed_attempts, list) else [])
            if isinstance(attempt, Mapping) and isinstance(attempt.get("requestId"), str)
        }
        if any(
            "launchedAtMs" in attempt
            and isinstance(attempt.get("requestId"), str)
            and attempt["requestId"] not in observed_requests
            and attempt["requestId"] not in deferred
            for attempt in attempts
        ):
            findings.append("INCOMPLETE: frozen observations do not cover all client-journaled launches")
        if established is not None:
            if standard and evidence.get("requirementId") in ("lmi-lifecycle-2", "lmi-lifecycle-7"):
                retired_requests: dict[str, set[str]] = {}
                if retirement_evidence and not validate_retirement_evidence(retirement_evidence):
                    for attempt in retirement_evidence["attempts"]:
                        retired_requests.setdefault(attempt["executionArn"], set()).add(attempt["requestId"])
                findings.extend(retire(established, final_evidence, retired_requests=retired_requests))
            else:
                findings.extend(retire(established, copy.deepcopy(final_evidence)))
        elif validator is not None:
            findings.extend(validator(copy.deepcopy(final_evidence), phase="final"))
        # Collection records retries active at the cutoff and requests deferred
        # past it; only the independent retirement evidence can prove them.
        findings.extend(audit_cutoff(evidence, retirement_evidence))
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError, RecursionError):
        findings.append("INCOMPLETE: malformed captured evidence")
    return observation_findings, list(dict.fromkeys(findings)), final_evidence


def evaluate_evidence(
    evidence: Mapping[str, Any],
    *,
    retirement_evidence: Mapping[str, Any],
    cleanup_evidence: Mapping[str, Sequence[Mapping[str, Any]]],
    standard: bool,
    validator: EvidenceValidator | None = None,
    observation_evidence: Sequence[Mapping[str, Any]] = (),
    attempts: Sequence[Mapping[str, Any]] = (),
) -> list[str]:
    """Evaluate captured evidence without querying or mutating the capture.

    ``attempts`` is the collection's launch journal: every client launch it
    records (``launchedAtMs``) must appear in the frozen observations.
    ``validator=None`` selects the shared standard rules. A supplied validator
    is a custom suite policy and is applied as given, without the shared
    lifecycle 2/7 retirement allowance, even if it is ``validate_evidence``.
    """
    _, findings, _ = _evaluate_capture(
        evidence,
        retirement_evidence=retirement_evidence,
        cleanup_evidence=cleanup_evidence,
        observation_evidence=observation_evidence,
        attempts=attempts,
        validator=validator,
        standard=standard,
    )
    return findings


def adjudicate_evidence(
    status: str,
    errors: list[str],
    evidence: Mapping[str, Any],
    *,
    retirement_evidence: Mapping[str, Any],
    cleanup_evidence: Mapping[str, Sequence[Mapping[str, Any]]],
    standard: bool,
    validator: EvidenceValidator | None = None,
    observation_evidence: Sequence[Mapping[str, Any]] = (),
    attempts: Sequence[Mapping[str, Any]] = (),
    trusted_observations: bool = False,
    observation_findings: Sequence[str] = (),
) -> tuple[str, list[str], dict[str, Any]]:
    """Resolve a capture status with the adjudication rules.

    ``status`` and ``errors`` are the collection status and collection errors;
    any collection error stops a pass. ``observation_findings`` are the caller's
    findings about the frozen observations and count like rule findings. With
    ``trusted_observations``, a finding about the frozen observations (from the
    observation rules or ``observation_findings``) that is not ``INCOMPLETE:``
    turns an ``INFRASTRUCTURE_ERROR`` into ``FAILED``; cleanup and retirement
    findings do not. ``attempts`` is the launch journal, as for
    ``evaluate_evidence``.

    ``validator=None`` selects the shared standard rules. A supplied validator
    is a custom suite policy and is applied as given, without the shared
    lifecycle 2/7 retirement allowance, even if it is ``validate_evidence``.
    """
    observed, findings, projected = _evaluate_capture(
        evidence,
        retirement_evidence=retirement_evidence,
        cleanup_evidence=cleanup_evidence,
        observation_evidence=observation_evidence,
        attempts=attempts,
        validator=validator,
        standard=standard,
    )
    observed.extend(observation_findings)
    findings.extend(observation_findings)
    combined = list(dict.fromkeys([*errors, *findings]))
    if (
        status == "INFRASTRUCTURE_ERROR"
        and trusted_observations
        and any(not item.startswith("INCOMPLETE:") for item in observed)
    ):
        status = "FAILED"
    elif status in ("COLLECTED", "PASSED"):
        if errors:
            status = "INCOMPLETE_EVIDENCE"
        elif any(not item.startswith("INCOMPLETE:") for item in findings):
            status = "FAILED"
        elif findings:
            status = "INCOMPLETE_EVIDENCE"
        else:
            status = "PASSED"
    return status, combined, projected
