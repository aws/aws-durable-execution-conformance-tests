# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Adjudication of collected captures: cross-observation rules, retirement and cleanup re-checks."""

from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from runpy import run_path
from typing import Any, Literal

import pytest

from aws_durable_execution_conformance_tests.lmi.adjudication import (
    _cleanup_behavior_errors,
    adjudicate_evidence,
    evaluate_evidence,
)
from aws_durable_execution_conformance_tests.lmi.evidence import validate_evidence, validate_retirement_evidence
from aws_durable_execution_conformance_tests.lmi.rules.replay import replay_checkpoint_timing

_fixtures = run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))
complete_retirement_events = _fixtures["complete_retirement_events"]
deadline_evidence = _fixtures["deadline_evidence"]
event = _fixtures["event"]
evidence = _fixtures["evidence"]
recovery_evidence = _fixtures["recovery_evidence"]
replay_evidence = _fixtures["replay_evidence"]
resequence = _fixtures["resequence"]
standard_retirement_snapshot = _fixtures["standard_retirement_snapshot"]
lifecycle_7_retirement_retry = _fixtures["lifecycle_7_retirement_retry"]


@pytest.mark.parametrize("retained_at", [50, 201])
def test_adjudication_checks_retained_terminal_settlement_against_observation_window(retained_at: int) -> None:
    frozen = deadline_evidence()
    arn = frozen["histories"][0]["executionArn"]
    frozen["histories"][0]["status"] = "RUNNING"
    frozen["expectedHistories"][arn] = {"allowedStatuses": ["RUNNING", "TIMED_OUT"]}
    independent = deepcopy(frozen)
    resource = next(row for row in independent["retirement"]["resources"] if row["id"] == arn)
    resource.update(outcome="SUCCEEDED", settledAtMs=210)
    retained = deepcopy(independent)
    next(row for row in retained["retirement"]["resources"] if row["id"] == arn)["settledAtMs"] = retained_at
    frozen_copy, retained_copy, independent_copy = map(deepcopy, (frozen, retained, independent))

    findings = evaluate_evidence(
        frozen,
        retirement_evidence=independent,
        cleanup_evidence={"release": [retained]},
        standard=True,
    )

    assert any("outside the declared allowedStatuses" in finding for finding in findings) == (retained_at == 50)
    assert (frozen, retained, independent) == (frozen_copy, retained_copy, independent_copy)


def test_adjudication_deadline_budget_targets_the_controlled_task() -> None:
    frozen = deadline_evidence()
    victim = frozen["attempts"][0]
    independent = deepcopy(frozen)
    independent["events"].extend(
        [event(victim, "task_start", 13, taskId="child"), event(victim, "task_exit", 15, taskId="child")]
    )
    findings = evaluate_evidence(
        frozen,
        retirement_evidence=independent,
        cleanup_evidence={},
        standard=True,
    )
    assert not any(
        "cleanupMs measurement" in finding or "exited before the observed" in finding for finding in findings
    )


@pytest.mark.parametrize(("factory", "kind"), [(deadline_evidence, "platform_timeout"), (replay_evidence, "pending")])
def test_adjudication_rejects_duplicate_boundaries_in_one_snapshot(factory, kind: str) -> None:
    frozen = factory()
    independent = deepcopy(frozen)
    duplicate = deepcopy(next(row for row in independent["events"] if row["kind"] == kind))
    duplicate["atMs"] += 0.5
    independent["events"].append(duplicate)
    findings = evaluate_evidence(
        frozen,
        retirement_evidence=independent,
        cleanup_evidence={},
        standard=True,
    )
    assert any(f"needs exactly one {kind} event" in finding for finding in findings)


@pytest.mark.parametrize(("start", "fails"), [(199, True), (201, False)])
def test_adjudication_overlap_excludes_retries_in_frozen_window(start: int, fails: bool) -> None:
    frozen = evidence()
    independent = deepcopy(frozen)
    victim = frozen["attempts"][0]
    independent["attempts"].append({**victim, "requestId": "extra", "attempt": 2, "startedAtMs": start})
    findings = evaluate_evidence(frozen, retirement_evidence=independent, cleanup_evidence={}, standard=True)
    assert any("original successful execution without retry" in item for item in findings) is fails


@pytest.mark.parametrize("requirement", [3, 4])
def test_adjudication_lifecycle_grace_uses_frozen_observation_window(requirement: int) -> None:
    frozen = deadline_evidence() if requirement == 3 else recovery_evidence()
    boundary_kind, fixed_at = ("platform_timeout", 30.5) if requirement == 3 else ("worker_recovered", 45.5)
    frozen["budgets"]["observationGraceMs"] = 170 if requirement == 3 else 155
    independent = deepcopy(frozen)
    independent["observationEndedAtMs"] = 201
    next(row for row in independent["events"] if row["kind"] == boundary_kind)["atMs"] = fixed_at
    findings = evaluate_evidence(frozen, retirement_evidence=independent, cleanup_evidence={}, standard=True)
    assert any("cleanup observation window lacks the declared grace after" in item for item in findings)


def test_adjudication_rejects_multiple_disposal_root_completions_in_one_frame() -> None:
    frozen = evidence(2)
    independent = deepcopy(frozen)
    root = next(row for row in independent["events"] if row["kind"] == "root_completed")
    independent["events"].append({**root, "atMs": 36})
    findings = evaluate_evidence(frozen, retirement_evidence=independent, cleanup_evidence={}, standard=True)
    assert any("needs exactly one root_completed event" in item for item in findings)


@pytest.mark.parametrize(
    "invalid",
    [None, 1, {"kind": "task_exit", "requestId": [], "taskId": []}],
)
def test_adjudication_invalid_retirement_completion_is_incomplete(invalid: Any) -> None:
    frozen = evidence(2)
    retirement = deepcopy(frozen)
    retirement["retirement"]["completions"] = [invalid]
    status, findings, _ = adjudicate_evidence(
        "COLLECTED",
        [],
        frozen,
        retirement_evidence=retirement,
        cleanup_evidence={},
        standard=True,
    )
    assert status == "INCOMPLETE_EVIDENCE", findings
    assert any("malformed captured retirement completions" in item for item in findings)


@pytest.mark.parametrize("trusted", [False, True])
def test_adjudication_retains_trustworthy_behavior_failure_after_cleanup_error(trusted: bool) -> None:
    frozen = evidence(2)
    frozen["expectedHistories"][frozen["histories"][0]["executionArn"]]["result"] = "different"
    status, findings, _ = adjudicate_evidence(
        "INFRASTRUCTURE_ERROR",
        ["cleanup failed"],
        frozen,
        retirement_evidence={},
        cleanup_evidence={},
        standard=True,
        trusted_observations=trusted,
    )
    assert status == ("FAILED" if trusted else "INFRASTRUCTURE_ERROR")
    assert "cleanup failed" in findings
    assert any(item.startswith("ASSERTION:") for item in findings)


def test_adjudication_shared_rules_check_scenario_observations_once(monkeypatch: pytest.MonkeyPatch) -> None:
    from aws_durable_execution_conformance_tests.lmi.rules import engine

    checked: list[Any] = []
    scenario_rules = engine._validate_observations

    def counted(view: Any, value: Any) -> None:
        checked.append(value.get("retirement"))
        scenario_rules(view, value)

    monkeypatch.setattr(engine, "_validate_observations", counted)
    frozen = evidence(2)
    retired = deepcopy(frozen)
    retired["retirement"]["retiredAtMs"] = 221
    status, findings, projected = adjudicate_evidence(
        "COLLECTED", [], frozen, retirement_evidence=retired, cleanup_evidence={}, standard=True
    )
    assert status == "PASSED", findings
    # Scenario rules see the frozen observations once; only retirement sees the projection.
    assert checked == [frozen["retirement"]]
    assert projected["retirement"]["retiredAtMs"] == 221


def test_adjudication_custom_validator_checks_observations_then_final() -> None:
    phases: list[str] = []

    def validator(evidence: Any, *, phase: Literal["observations", "final"] = "final") -> list[str]:
        phases.append(phase)
        return []

    frozen = evidence(2)
    findings = evaluate_evidence(
        frozen, retirement_evidence=deepcopy(frozen), cleanup_evidence={}, standard=True, validator=validator
    )
    assert findings == []
    assert phases == ["observations", "final"]


@pytest.mark.parametrize(("journal_key", "covered"), [("launchedAtMs", False), ("discoveredAtMs", True)])
def test_adjudication_requires_frozen_observations_to_cover_client_launches(journal_key: str, covered: bool) -> None:
    frozen = evidence()
    journal = [{"requestId": row["requestId"], "launchedAtMs": row["startedAtMs"]} for row in frozen["attempts"]]
    journal.append({"requestId": "request-unseen", journal_key: 1})
    findings = evaluate_evidence(
        frozen, retirement_evidence=deepcopy(frozen), cleanup_evidence={}, standard=True, attempts=journal
    )
    if covered:
        assert findings == []
    else:
        assert "INCOMPLETE: frozen observations do not cover all client-journaled launches" in findings


@pytest.mark.parametrize(
    ("counts", "finding"),
    [
        (((2, 2), (2, 2)), None),
        (((2, 2), (1, 2)), "ASSERTION: observed side-effect count differs from earlier observations for retry"),
        (((2, 2), (2, 3)), "ASSERTION: declared side-effect expectation for retry changed across observations"),
    ],
)
def test_adjudication_rejects_side_effect_regressions_across_observations(
    counts: tuple[tuple[int, int], ...], finding: str | None
) -> None:
    frozen = replay_evidence()
    snapshots = []
    for count, expected in counts:
        snapshot = deepcopy(frozen)
        record = next(row for row in snapshot["replay"]["sideEffects"] if row["operationId"] == "retry")
        record.update(count=count, expectedCount=expected)
        snapshots.append(snapshot)
    findings = evaluate_evidence(
        frozen, retirement_evidence=deepcopy(frozen), cleanup_evidence={}, standard=True, observation_evidence=snapshots
    )
    if finding is None:
        assert findings == []
    else:
        assert finding in findings


@pytest.mark.parametrize(
    ("orders", "finding"),
    [
        ((("retry-1", "retry-2"), ("retry-1", "retry-2")), None),
        (
            (("retry-1", "retry-2"), ("retry-2", "retry-1")),
            "ASSERTION: observed retry attempt orders contradict each other across observations",
        ),
        (
            (("retry-1", "retry-2"), ("retry-2", "retry-3"), ("retry-3", "retry-1")),
            "ASSERTION: observed retry attempt orders contradict each other across observations",
        ),
        ((("retry-1", "retry-2"), ("retry-2", "retry-3")), None),
        ((("retry-1", "retry-1"),), "INCOMPLETE: duplicate retry attemptId in observations"),
    ],
)
def test_adjudication_checks_retry_order_across_observations(
    orders: tuple[tuple[str, ...], ...], finding: str | None
) -> None:
    frozen = replay_evidence()
    snapshots = []
    for order in orders:
        snapshot = deepcopy(frozen)
        attempts = {row["attemptId"]: row for row in snapshot["replay"]["retry"]["attempts"]}
        snapshot["replay"]["retry"]["attempts"] = [
            deepcopy(attempts.get(identifier, {**attempts["retry-2"], "attemptId": identifier})) for identifier in order
        ]
        snapshots.append(snapshot)
    findings = evaluate_evidence(
        frozen, retirement_evidence=deepcopy(frozen), cleanup_evidence={}, standard=True, observation_evidence=snapshots
    )
    if finding is None:
        assert findings == []
    else:
        assert finding in findings


@pytest.mark.parametrize(
    ("status", "trusted", "resolved"),
    [
        ("INFRASTRUCTURE_ERROR", True, "FAILED"),
        ("INFRASTRUCTURE_ERROR", False, "INFRASTRUCTURE_ERROR"),
        ("COLLECTED", False, "FAILED"),
    ],
)
def test_adjudication_applies_collection_observation_findings(status: str, trusted: bool, resolved: str) -> None:
    frozen = evidence()
    complete_retirement_events(frozen)
    finding = "ASSERTION: adapter observed a duplicate dispatch"

    result, errors, _ = adjudicate_evidence(
        status,
        [],
        frozen,
        retirement_evidence=deepcopy(frozen),
        cleanup_evidence={},
        standard=True,
        trusted_observations=trusted,
        observation_findings=[finding],
    )

    assert (result, errors) == (resolved, [finding])


@pytest.mark.parametrize("dispatch_sequence", [9, 11])
def test_cleanup_dispatch_uses_semantic_sequence_at_closure(dispatch_sequence):
    original: dict[str, Any] = {
        "attempts": [{"requestId": "request"}],
        "events": [{"requestId": "request", "kind": "wrapper_return", "atMs": 40, "sequence": 10}],
    }
    snapshot = {
        "events": [{**original["events"][0], "sequence": 100}],
        "sdkRequests": [
            {
                "requestId": "request",
                "method": "CheckpointDurableExecution",
                "outcome": "DISPATCHED",
                "atMs": 40,
                "sequence": dispatch_sequence + 90,
            }
        ],
    }
    errors = _cleanup_behavior_errors(original, snapshot, standard=False)
    assert any("dispatched SDK work after admission closed" in error for error in errors) == (dispatch_sequence == 11)


@pytest.mark.parametrize("timeout_sequence", [9, 11])
def test_cleanup_deadline_uses_semantic_sequence_for_inflight_work(timeout_sequence):
    original: dict[str, Any] = {
        "requirementId": "lmi-lifecycle-3",
        "attempts": [{"requestId": "request", "startedAtMs": 10, "endedAtMs": 40}],
        "selectedRequests": {"victim": "request"},
        "profile": {"timeoutMs": 20},
        "events": [{"requestId": "request", "kind": "work_in_flight", "atMs": 30, "sequence": 10}],
    }
    snapshot = {
        "events": [
            {**original["events"][0], "sequence": 100},
            {
                "requestId": "request",
                "kind": "platform_timeout",
                "source": "platform",
                "atMs": 30,
                "sequence": timeout_sequence + 90,
            },
        ]
    }
    errors = _cleanup_behavior_errors(original, snapshot, standard=True)
    assert any("contradicts the frozen in-flight" in error for error in errors) == (timeout_sequence == 9)


def test_cleanup_body_event_cannot_replay_completed_user_code():
    original = replay_evidence()
    resumed = next(row for row in original["attempts"] if row["role"] == "resume")
    operation_id = original["replay"]["before"][0]["id"]
    snapshot = {
        "events": [
            {
                "kind": "body",
                "requestId": resumed["requestId"],
                "executionArn": resumed["executionArn"],
                "operationId": operation_id,
                "atMs": resumed["startedAtMs"] + 1,
            }
        ]
    }
    errors = _cleanup_behavior_errors(original, snapshot, standard=True)
    assert f"ASSERTION: cleanup evidence reports repeated completed user code for {operation_id}" in errors


def test_retirement_completions_project_only_to_matching_frozen_starts():
    from aws_durable_execution_conformance_tests.lmi.adjudication import _project_retirement_completions

    retained = {"kind": "io_exit", "requestId": "request", "taskId": "present"}
    completions = [
        retained,
        {"kind": "io_exit", "requestId": "other", "taskId": "present"},
        {"kind": "io_exit", "requestId": "request", "taskId": "discovered-later"},
        {"kind": "task_exit", "requestId": "request", "taskId": "present"},
    ]
    frozen = [{"kind": "io_start", "requestId": "request", "taskId": "present"}]
    assert _project_retirement_completions(frozen, completions) == [retained]
    assert len(completions) == 4


def test_cleanup_reconciles_call_with_later_request():
    call = {"kind": "checkpoint_call", "requestId": "request", "callId": "call", "atMs": 20}
    request = {**call, "method": "CheckpointDurableExecution", "outcome": "DISPATCHED"}
    assert any(
        "lacks a unique matching SDK request" in error
        for error in _cleanup_behavior_errors({}, {"events": [call]}, standard=False)
    )
    assert not any(
        "lacks a unique matching SDK request" in error
        for error in _cleanup_behavior_errors(
            {}, {"sdkRequests": [request]}, known_snapshots=[{"events": [call]}], standard=False
        )
    )


def test_cleanup_dispatch_tie_without_a_shared_snapshot_cannot_establish_order():
    original = {
        "attempts": [{"requestId": "request"}],
        "events": [{"requestId": "request", "kind": "wrapper_return", "atMs": 40, "sequence": 10}],
    }
    snapshot = {
        "sdkRequests": [
            {
                "requestId": "request",
                "method": "CheckpointDurableExecution",
                "outcome": "DISPATCHED",
                "atMs": 40,
                "sequence": 11,
            }
        ]
    }
    errors = _cleanup_behavior_errors(original, snapshot, standard=False)
    assert any("same-time observations need sequences in one snapshot" in error for error in errors)
    assert not any("dispatched SDK work after admission closed" in error for error in errors)


def test_later_duplicate_sdk_request_is_rejected_despite_earlier_unique_match():
    call = {"kind": "checkpoint_call", "requestId": "request", "callId": "call", "atMs": 20, "outcome": "REJECTED"}
    request = {**call, "method": "CheckpointDurableExecution"}
    errors = _cleanup_behavior_errors(
        {"events": [call], "sdkRequests": [request]},
        {"sdkRequests": [request, request]},
        standard=False,
    )
    assert "ASSERTION: cleanup observed SDK call lacks a unique matching SDK request" in errors


def test_cleanup_uses_platform_end_to_reject_timeout_after_completion():
    before = {
        "requirementId": "lmi-lifecycle-3",
        "selectedRequests": {"victim": "request"},
        "profile": {"timeoutMs": 20},
        "attempts": [{"requestId": "request", "startedAtMs": 10, "endedAtMs": 40, "endedAtMsSource": "fixture"}],
        "events": [{"kind": "platform_timeout", "source": "platform", "requestId": "request", "atMs": 30}],
    }
    final = {"attempts": [{"requestId": "request", "endedAtMs": 29, "endedAtMsSource": "platform"}]}
    assert "ASSERTION: cleanup timeout falls outside the configured native deadline" in _cleanup_behavior_errors(
        before, final, standard=True
    )


def test_source_aware_native_interval_ignores_superseded_fixture_times():
    from aws_durable_execution_conformance_tests.lmi.rules.retention import _native_intervals

    first = {"attempts": [{"requestId": "request", "startedAtMs": 50, "endedAtMs": 90}]}
    later = {
        "attempts": [
            {
                "requestId": "request",
                "startedAtMs": 55,
                "startedAtMsSource": "platform",
                "endedAtMs": 80,
                "endedAtMsSource": "platform",
            }
        ]
    }
    assert _native_intervals([first, later]) == {"request": {"startedAtMs": (55, True), "endedAtMs": (80, True)}}


def test_partial_same_time_dispatch_does_not_override_complete_order():
    request = {"requestId": "request", "method": "CheckpointDurableExecution", "outcome": "DISPATCHED", "atMs": 40}
    closure = {"kind": "wrapper_return", "requestId": "request", "atMs": 40, "sequence": 10}
    original = {
        "attempts": [{"requestId": "request"}],
        "events": [closure],
        "sdkRequests": [{**request, "sequence": 9}],
    }
    partial = {"events": [closure], "sdkRequests": [request]}
    errors = _cleanup_behavior_errors(original, original, standard=False, known_snapshots=[partial])
    assert not any("dispatched SDK work after admission closed" in error or "same-time" in error for error in errors)


def test_cleanup_replay_binding_can_arrive_in_later_retirement_evidence() -> None:
    value = standard_retirement_snapshot("replay_evidence")
    entry = {"requestId": "discovered-native", "operationId": "not-checkpointed"}
    discovered = {"replay": {"operationExecutions": [entry]}}
    binding = {
        "attempts": [{"requestId": "discovered-native", "executionArn": "arn:execution:victim"}],
        "retirement": deepcopy(value["retirement"]),
    }
    binding["retirement"]["resources"].append(
        {
            "id": "discovered-native",
            "kind": "invocation",
            "ownerCaseId": value["caseId"],
            "settledAtMs": 209,
            "outcome": "QUIESCENT",
            "evidenceUri": "memory://retirement",
        }
    )
    assert _cleanup_behavior_errors(value, binding, standard=True, known_snapshots=[discovered]) == []
    unresolved = _cleanup_behavior_errors(value, {}, standard=True, known_snapshots=[discovered])
    assert any(error.startswith("INCOMPLETE:") and "native request" in error for error in unresolved), unresolved
    entry["operationId"] = "done"
    completed = _cleanup_behavior_errors(value, binding, standard=True, known_snapshots=[discovered])
    assert any("completed user code" in error for error in completed), completed
    contradictory = {"attempts": [{"requestId": "discovered-native", "executionArn": "arn:execution:peer"}]}
    conflicts = _cleanup_behavior_errors(value, contradictory, standard=True, known_snapshots=[discovered, binding])
    assert any(error.startswith("INCOMPLETE:") and "conflicting execution binding" in error for error in conflicts)


@pytest.mark.parametrize(
    ("operation_id", "end", "diagnostic"),
    [
        ("done", 20, "completed replay body occurred after recorded completion"),
        ("done", 40, "replay.before contains an operation that completed after suspension"),
        ("retry", 48, "retry owner was admitted after recorded operation completion"),
    ],
)
def test_cleanup_reconciles_service_checkpoint_completion_times(operation_id: str, end: int, diagnostic: str) -> None:
    value = standard_retirement_snapshot("replay_evidence")
    snapshot = deepcopy(value)
    inventories = [snapshot["replay"]["before"], snapshot["replay"]["after"]]
    inventories.extend(history["operations"] for history in snapshot["histories"])
    for operations in inventories:
        for operation in operations:
            if operation["id"] == operation_id:
                operation["endedAtMs"] = end
    assert any(diagnostic in error for error in _cleanup_behavior_errors(value, snapshot, standard=True))


def test_cleanup_overlap_rejects_additional_record_marker() -> None:
    value = standard_retirement_snapshot("evidence", requirement=1)
    snapshot = deepcopy(value)
    history = next(row for row in snapshot["histories"] if row["executionArn"] == "arn:execution:victim")
    marker = next(row for row in history["operations"] if row["name"] == "record-marker")
    history["operations"].append({**marker, "id": "second-marker"})
    assert any(
        "more than one record-marker" in error for error in _cleanup_behavior_errors(value, snapshot, standard=True)
    )


def test_cleanup_retry_endpoints_are_checked_across_partial_orders() -> None:
    value = standard_retirement_snapshot("replay_evidence")
    snapshot = deepcopy(value)
    retry = snapshot["replay"]["retry"]
    last = {"attemptId": "additional", "requestId": "request-resume", "outcome": "FAILED"}
    retry["attempts"] = [retry["attempts"][-1], last]
    snapshot["replay"]["operationExecutions"].append({**last, "operationId": "retry"})
    assert any(
        "do not start with failure and end with success" in error
        for error in _cleanup_behavior_errors(value, snapshot, standard=True)
    )


@pytest.mark.parametrize("changed_time", [False, True])
def test_cleanup_retry_uses_canonical_numeric_body_identity(changed_time: bool) -> None:
    value = standard_retirement_snapshot("replay_evidence")
    resume = next(row for row in value["attempts"] if row["role"] == "resume")
    bodies = []
    for number, declared in enumerate(value["replay"]["retry"]["attempts"], 1):
        prior_id = declared["attemptId"]
        declared.update(attemptId=f"{resume['requestId']}:{number}", requestId=resume["requestId"])
        next(row for row in value["replay"]["operationExecutions"] if row.get("attemptId") == prior_id).update(declared)
        bodies.append(
            {
                "kind": "body",
                "requestId": resume["requestId"],
                "ownerRequestId": resume["requestId"],
                "executionArn": resume["executionArn"],
                "atMs": 55,
                "operationId": "retry",
                "attempt": number,
            }
        )
    value["events"].extend(bodies)
    resequence(value)
    snapshot = deepcopy(value)
    if changed_time:
        next(row for row in snapshot["events"] if row.get("attempt") == 2)["atMs"] = 60
    resequence(snapshot)
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("retry body changed its observed timestamp" in error for error in findings) is changed_time
    if not changed_time:
        assert findings == []


def test_cleanup_deadline_rejects_earlier_admission_closure() -> None:
    value = standard_retirement_snapshot("deadline_evidence")
    snapshot = deepcopy(value)
    next(row for row in snapshot["events"] if row["kind"] == "admission_closed")["atMs"] = 29
    assert any(
        "admission closed before the observed platform timeout" in error
        for error in _cleanup_behavior_errors(value, snapshot, standard=True)
    )


def test_cleanup_reconciles_original_invocation_and_ordinal_order() -> None:
    value = standard_retirement_snapshot("evidence")
    victim = next(row for row in value["attempts"] if row["role"] == "victim")
    value["attempts"].append(
        {**victim, "requestId": "request-victim-retry", "attempt": 2, "startedAtMs": 60, "endedAtMs": 80}
    )
    snapshot = deepcopy(value)
    snapshot["attempts"][-1]["startedAtMs"] = 9
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("ordinals contradict admission order" in error for error in findings)
    assert any("selected victim is not the original invocation" in error for error in findings)


@pytest.mark.parametrize("role_name", ["recovery", "victim"])
def test_cleanup_rechecks_tied_original_admissions(role_name: str) -> None:
    value = standard_retirement_snapshot("recovery_evidence" if role_name == "recovery" else "replay_evidence")
    selected = next(row for row in value["attempts"] if row["role"] == role_name)
    value["attempts"].append(
        {
            **selected,
            "requestId": "later-native",
            "attempt": 2,
            "startedAtMs": 60,
            "startedAtMsSource": "fixture",
            "endedAtMs": 90,
        }
    )
    snapshot = deepcopy(value)
    snapshot["attempts"][-1].update(startedAtMs=selected["startedAtMs"], startedAtMsSource="platform")
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any(f"tied native admissions cannot establish the original {role_name}" in error for error in findings)


def test_cleanup_rechecks_peer_progress_against_corrected_recovery() -> None:
    value = standard_retirement_snapshot("recovery_evidence")
    snapshot = deepcopy(value)
    next(row for row in snapshot["events"] if row["kind"] == "worker_recovered")["atMs"] = 43
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("healthy peer lacks progress across the timeout and recovery" in error for error in findings)


def test_cleanup_retry_checks_transitive_body_order_across_fragments() -> None:
    value = standard_retirement_snapshot("replay_evidence")
    resume = next(row for row in value["attempts"] if row["role"] == "resume")
    retry = value["replay"]["retry"]
    first, middle = retry["attempts"]
    first["requestId"] = resume["requestId"]
    next(row for row in value["replay"]["operationExecutions"] if row.get("attemptId") == first["attemptId"]).update(
        first
    )
    # The middle attempt deliberately has no timed body observation.
    owner = {
        "requestId": resume["requestId"],
        "ownerRequestId": resume["requestId"],
        "executionArn": resume["executionArn"],
    }
    value["events"].append(
        {**owner, "kind": "body", "operationId": "retry", "attemptId": first["attemptId"], "atMs": 60}
    )
    resequence(value)
    snapshot = deepcopy(value)
    last = {"attemptId": "last", "requestId": resume["requestId"], "outcome": "SUCCEEDED"}
    snapshot["replay"]["retry"]["attempts"] = [middle, last]
    snapshot["replay"]["operationExecutions"].append({**last, "operationId": "retry"})
    snapshot["events"].append({**owner, "kind": "body", "operationId": "retry", "attemptId": "last", "atMs": 55})
    resequence(snapshot)
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("retry attempt order contradicts timed user-code entries" in error for error in findings)


def test_cleanup_retry_cannot_certify_ambiguous_endpoints() -> None:
    value = standard_retirement_snapshot("replay_evidence")
    snapshot = deepcopy(value)
    first = snapshot["replay"]["retry"]["attempts"][0]
    unplaced = {"attemptId": "unplaced", "requestId": "request-resume", "outcome": "FAILED"}
    snapshot["replay"]["retry"]["attempts"] = [first, unplaced]
    snapshot["replay"]["operationExecutions"].append({**unplaced, "operationId": "retry"})
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("retry order does not establish unique first and last attempts" in error for error in findings)


def test_cleanup_rejects_completed_replay_bodies_after_observed_pending() -> None:
    value = standard_retirement_snapshot("replay_evidence")
    snapshot = deepcopy(value)
    next(event for event in snapshot["events"] if event["kind"] == "pending")["atMs"] = 20
    resequence(snapshot)
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("reports repeated completed user code for done" in error for error in findings)
    assert any("reports repeated completed user code for failed" in error for error in findings)


@pytest.mark.parametrize("source", ["retirement", "cleanup"])
@pytest.mark.parametrize("view", ["before", "after"])
@pytest.mark.parametrize("operation_id", [["done"], {"id": "done"}, 7])
def test_cleanup_replay_non_string_checkpoint_id_keeps_behavior_findings(
    source: str, view: str, operation_id: Any
) -> None:
    """A non-string checkpoint ID is reported and does not hide the other cleanup findings."""
    value = standard_retirement_snapshot("replay_evidence")
    snapshot = deepcopy(value)
    snapshot["replay"]["operationExecutions"].append({"requestId": "request-resume", "operationId": "done"})
    snapshot["replay"][view][0]["id"] = operation_id
    assert validate_retirement_evidence(snapshot) == []
    findings = evaluate_evidence(
        value,
        retirement_evidence=snapshot if source == "retirement" else deepcopy(value),
        cleanup_evidence={"release": [snapshot]} if source == "cleanup" else {},
        standard=True,
    )
    assert f"ASSERTION: cleanup replay.{view} checkpoint lacks an operation ID" in findings
    assert "ASSERTION: cleanup evidence reports repeated completed user code for done" in findings
    assert "INCOMPLETE: malformed captured evidence" not in findings


@pytest.mark.parametrize("operation_id", [["x"], {"id": "x"}, 7])
def test_replay_checkpoint_timing_skips_non_string_ids_only_for_omission(operation_id: Any) -> None:
    """A non-string ID is never an omitted checkpoint, but its end time still counts as late."""
    before = [{"id": operation_id, "type": "STEP", "status": "SUCCEEDED", "endedAtMs": 40}]
    after = [{"id": operation_id, "status": "SUCCEEDED", "endedAtMs": 20}]
    assert replay_checkpoint_timing(before, after, 35) == (False, True)
    assert replay_checkpoint_timing([], [{**after[0], "id": "x"}], 35) == (True, False)


def test_cleanup_recovery_rechecks_declared_observation_grace() -> None:
    value = standard_retirement_snapshot("recovery_evidence")
    value["observationEndedAtMs"] = 100
    value["budgets"]["observationGraceMs"] = 55
    snapshot = deepcopy(value)
    next(row for row in snapshot["events"] if row["kind"] == "worker_recovered")["atMs"] = 46
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("observation window lacks the declared grace after recovery" in error for error in findings)


def test_cleanup_rejects_malformed_interior_retry_outcome() -> None:
    value = standard_retirement_snapshot("replay_evidence")
    snapshot = deepcopy(value)
    malformed = {"attemptId": "malformed", "requestId": "request-resume", "outcome": False}
    snapshot["replay"]["retry"]["attempts"].insert(1, malformed)
    snapshot["replay"]["operationExecutions"].append({**malformed, "operationId": "retry"})
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("attempt malformed outcome must be a non-empty string" in error for error in findings)


def test_cleanup_rechecks_marker_dispatch_at_corrected_overlap_boundary() -> None:
    value = standard_retirement_snapshot("evidence", requirement=1)
    snapshot = deepcopy(value)
    victim = next(row for row in snapshot["attempts"] if row["role"] == "victim")
    victim.update(endedAtMs=26, endedAtMsSource="platform")
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("overlap peer lacks a marker checkpoint dispatch inside native overlap" in error for error in findings)
    assert not any("barrier lies outside" in error for error in findings)


def test_cleanup_deadline_rechecks_grace_after_corrected_timeout() -> None:
    value = standard_retirement_snapshot("deadline_evidence")
    value["budgets"]["observationGraceMs"] = 170
    snapshot = deepcopy(value)
    next(row for row in snapshot["events"] if row["kind"] == "platform_timeout")["atMs"] = 30.5
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("observation window lacks the declared grace after timeout" in error for error in findings)


def test_cleanup_recovery_admission_is_checked_against_corrected_timeout() -> None:
    value = standard_retirement_snapshot("recovery_evidence")
    next(row for row in value["attempts"] if row["role"] == "recovery")["startedAtMs"] = 30.25
    snapshot = deepcopy(value)
    next(row for row in snapshot["events"] if row["kind"] == "platform_timeout")["atMs"] = 30.5
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("places recovery before the victim timeout" in error for error in findings)


def test_cleanup_orders_same_time_poll_by_later_semantic_sequence() -> None:
    value = standard_retirement_snapshot("warm_evidence")
    returned = next(event for event in value["events"] if event["kind"] == "wrapper_return")
    provisional = deepcopy(value)
    provisional["sdkRequests"].append(
        {
            **{key: returned[key] for key in ("requestId", "executionArn", "atMs")},
            "method": "GetDurableExecutionState",
            "outcome": "DISPATCHED",
            "callId": "same-time-poll",
        }
    )
    final = deepcopy(provisional)
    closed = next(event for event in final["events"] if event["kind"] == "wrapper_return")
    poll = final["sdkRequests"][-1]
    poll["sequence"] = 0
    closed["sequence"] = 1
    diagnostic = "dispatched SDK work after admission closed"
    assert not any(
        diagnostic in error
        for error in _cleanup_behavior_errors(value, final, standard=True, known_snapshots=[provisional])
    )
    poll["sequence"] = 2
    assert any(
        diagnostic in error
        for error in _cleanup_behavior_errors(value, final, standard=True, known_snapshots=[provisional])
    )


def test_cleanup_recovery_rejects_two_recoveries_in_one_snapshot() -> None:
    original = standard_retirement_snapshot("recovery_evidence")
    snapshot = deepcopy(original)
    recovery = next(row for row in snapshot["events"] if row["kind"] == "worker_recovered")
    snapshot["events"].append({**recovery, "atMs": recovery["atMs"] + 0.5})
    assert any(
        "needs exactly one worker_recovered" in failure
        for failure in _cleanup_behavior_errors(original, snapshot, standard=True)
    )


def test_adjudication_disposal_uses_verified_retirement_attempt_count() -> None:
    original, snapshot = lifecycle_7_retirement_retry()
    assert validate_retirement_evidence(snapshot) == []
    errors = evaluate_evidence(
        original,
        retirement_evidence=snapshot,
        cleanup_evidence={},
        standard=True,
    )
    assert errors == []
    # Retry-order observations must not change which rules apply.
    frames = [
        {"replay": {"retry": {"operationId": "retry", "attempts": [{"attemptId": a}, {"attemptId": b}]}}}
        for a, b in (("first-1", "first-2"), ("second-1", "second-2"))
    ]
    assert (
        evaluate_evidence(
            original, retirement_evidence=snapshot, cleanup_evidence={}, standard=True, observation_evidence=frames
        )
        == []
    )

    marker = "ASSERTION: custom suite policy applied"

    def custom(evidence: Mapping[str, Any], *, phase: str = "final") -> list[str]:
        findings = validate_evidence(evidence, phase="observations" if phase == "observations" else "final")
        return [*findings, marker] if phase == "final" else findings

    class FalseyCustom:
        def __bool__(self) -> bool:
            return False

        def __call__(self, evidence: Mapping[str, Any], *, phase: str = "final") -> list[str]:
            return custom(evidence, phase=phase)

    # The retirement allowance is part of the shared rules, selected explicitly
    # with validator=None. A custom suite policy is applied exactly as supplied,
    # even when the policy object is falsey.
    disagreement = "ASSERTION: terminal retirement outcome disagrees with the single disposal invocation"
    for policy in (custom, FalseyCustom()):
        findings = evaluate_evidence(
            original, retirement_evidence=snapshot, cleanup_evidence={}, standard=True, validator=policy
        )
        assert disagreement in findings
        assert marker in findings


@pytest.mark.parametrize("cleanup_history", ["status-only", "contradicting-result"])
def test_partial_cleanup_history_only_fails_on_supplied_contradictions(cleanup_history: str) -> None:
    original = standard_retirement_snapshot("evidence")
    # Executions that settle inside the observation window are rechecked
    # against their fixture expectations in every cleanup snapshot.
    for row in original["retirement"]["resources"]:
        if row["kind"] == "execution":
            row["settledAtMs"] = original["observationEndedAtMs"] - 10
    assert validate_evidence(original) == []
    frame = deepcopy(original)
    frame["histories"] = [
        {"executionArn": history["executionArn"], "status": history["status"]} for history in frame["histories"]
    ]
    if cleanup_history == "contradicting-result":
        frame["histories"][0]["result"] = {"marker": "unexpected"}
    errors = evaluate_evidence(
        original,
        retirement_evidence={},
        cleanup_evidence={"retire": [frame]},
        standard=True,
    )
    if cleanup_history == "status-only":
        assert errors == []
    else:
        arn = frame["histories"][0]["executionArn"]
        assert f"ASSERTION: service history {arn}.result differs from the fixture expectation" in errors


@pytest.mark.parametrize("frozen_status", ["SUCCEEDED", "RUNNING"])
@pytest.mark.parametrize("cleanup_result", ["omitted", "no-row", "expected", "unexpected"])
def test_in_window_terminal_history_must_supply_declared_fields(frozen_status: str, cleanup_result: str) -> None:
    arn = "arn:execution:victim"
    declared = {"marker": "victim"}
    original = {
        "expectedHistories": {arn: {"allowedStatuses": ["RUNNING", "SUCCEEDED"], "result": declared}},
        "observationEndedAtMs": 200,
        # A nonterminal frozen view can match the declared result without
        # proving the result of the terminal execution.
        "histories": [{"executionArn": arn, "status": frozen_status, "result": declared}],
    }
    row: dict[str, Any] = {"executionArn": arn, "status": "SUCCEEDED"}
    if cleanup_result != "omitted":
        row["result"] = declared if cleanup_result == "expected" else {"marker": "unexpected"}
    snapshot = {
        # "no-row": settlement alone, with no history row for the execution.
        "histories": [] if cleanup_result == "no-row" else [row],
        "retirement": {"resources": [{"kind": "execution", "id": arn, "outcome": "SUCCEEDED", "settledAtMs": 190}]},
    }
    findings = _cleanup_behavior_errors(original, snapshot, standard=True)
    contradiction = f"ASSERTION: service history {arn}.result differs from the fixture expectation"
    missing = f"INCOMPLETE: terminal service history {arn} lacks declared result"
    assert (contradiction in findings) is (cleanup_result == "unexpected")
    assert (missing in findings) is (cleanup_result in ("omitted", "no-row") and frozen_status == "RUNNING")


@pytest.mark.parametrize("final_result", ["expected", "omitted", "unexpected"])
def test_terminal_history_may_arrive_without_its_settlement(final_result: str) -> None:
    arn = "arn:execution:victim"
    declared = {"marker": "victim"}
    original = {
        "expectedHistories": {arn: {"allowedStatuses": ["RUNNING", "SUCCEEDED"], "result": declared}},
        "observationEndedAtMs": 200,
        "histories": [{"executionArn": arn, "status": "RUNNING", "result": declared}],
    }
    # A retained snapshot records the in-window settlement with a status-only row.
    retained = {
        "histories": [{"executionArn": arn, "status": "SUCCEEDED"}],
        "retirement": {"resources": [{"kind": "execution", "id": arn, "outcome": "SUCCEEDED", "settledAtMs": 190}]},
    }
    # The final snapshot supplies the terminal result but records a later settlement.
    row: dict[str, Any] = {"executionArn": arn, "status": "SUCCEEDED"}
    if final_result != "omitted":
        row["result"] = declared if final_result == "expected" else {"marker": "unexpected"}
    final = {
        "histories": [row],
        "retirement": {"resources": [{"kind": "execution", "id": arn, "outcome": "SUCCEEDED", "settledAtMs": 210}]},
    }
    findings = _cleanup_behavior_errors(original, final, standard=True, known_snapshots=[retained])
    missing = f"INCOMPLETE: terminal service history {arn} lacks declared result"
    contradiction = f"ASSERTION: service history {arn}.result differs from the fixture expectation"
    assert (missing in findings) is (final_result == "omitted")
    # Any row that supplies a declared field is compared with the expectation.
    assert (contradiction in findings) is (final_result == "unexpected")


def test_every_terminal_history_row_that_supplies_fields_is_compared() -> None:
    arn = "arn:execution:victim"
    declared = {"marker": "victim"}
    original = {
        "expectedHistories": {arn: {"allowedStatuses": ["RUNNING", "SUCCEEDED"], "result": declared}},
        "observationEndedAtMs": 200,
        "histories": [{"executionArn": arn, "status": "RUNNING", "result": declared}],
    }
    snapshot = {
        # The earlier duplicate row carries a wrong result; the later one has none.
        "histories": [
            {"executionArn": arn, "status": "SUCCEEDED", "result": {"marker": "unexpected"}},
            {"executionArn": arn, "status": "SUCCEEDED"},
        ],
        "retirement": {"resources": [{"kind": "execution", "id": arn, "outcome": "SUCCEEDED", "settledAtMs": 190}]},
    }
    findings = _cleanup_behavior_errors(original, snapshot, standard=True)
    assert f"ASSERTION: service history {arn}.result differs from the fixture expectation" in findings
