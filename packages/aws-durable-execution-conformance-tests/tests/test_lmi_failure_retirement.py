# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Failed observations stay failed; only independently proven cleanup permits reuse."""

from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from runpy import run_path
from typing import Any

import pytest

from aws_durable_execution_conformance_tests.lmi.adjudication import _cleanup_behavior_errors
from aws_durable_execution_conformance_tests.lmi.evidence import validate_evidence, validate_retirement_evidence


def _resequence(value: dict[str, Any]) -> None:
    for request in value["attempts"]:
        local = sorted(
            (event for event in value["events"] if event["requestId"] == request["requestId"]),
            key=lambda event: event["atMs"],
        )
        for sequence, event in enumerate(local):
            event["sequence"] = sequence


def _complete_fixture_sdk_calls(value: dict[str, Any]) -> None:
    value["events"] = [
        event for event in value["events"] if not str(event.get("callId", "")).startswith("fixture-dispatch-")
    ]
    for index, request in enumerate(value["sdkRequests"]):
        if request["outcome"] != "DISPATCHED":
            continue
        request["callId"] = f"fixture-dispatch-{index}"
        kind = "poll" if request["method"] == "GetDurableExecutionState" else "checkpoint"
        value["events"].extend([{**request, "kind": f"{kind}_call"}, {**request, "kind": f"{kind}_exit"}])
    _resequence(value)


def _standard_retirement_snapshot(factory: str, runtime: str = "javascript", *, requirement: int = 2) -> dict[str, Any]:
    fixtures = run_path(str(Path(__file__).with_name("test_lmi_evidence.py")))
    value = (
        fixtures[factory](requirement, runtime)
        if factory == "evidence"
        else fixtures[factory]()
        if factory in ("replay_evidence", "failure_evidence")
        else fixtures[factory](runtime=runtime)
    )
    for attempt in value["attempts"]:
        owner = {key: attempt[key] for key in ("requestId", "executionArn")}
        owner["ownerRequestId"] = attempt["requestId"]
        local = [event for event in value["events"] if event["requestId"] == attempt["requestId"]]
        value["events"].extend(
            [
                {**owner, "kind": "wrapper_enter", "atMs": attempt["startedAtMs"]},
                {**owner, "kind": "task_start", "taskId": "root", "atMs": attempt["startedAtMs"] + 0.1},
            ]
        )
        exits = [event for event in local if event["kind"] == "task_exit"]
        if exits:
            for event in exits:
                event["taskId"] = "root"
        else:
            value["events"].append({**owner, "kind": "task_exit", "taskId": "root", "atMs": attempt["endedAtMs"] - 0.1})
        if not any(event["kind"] == "wrapper_return" for event in local):
            value["events"].append({**owner, "kind": "wrapper_return", "atMs": attempt["endedAtMs"]})
    _complete_fixture_sdk_calls(value)
    assert validate_evidence(value) == []
    assert validate_retirement_evidence(value) == []
    return value


def test_cleanup_replay_binding_can_arrive_in_later_retirement_evidence() -> None:
    value = _standard_retirement_snapshot("replay_evidence")
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
    value = _standard_retirement_snapshot("replay_evidence")
    snapshot = deepcopy(value)
    inventories = [snapshot["replay"]["before"], snapshot["replay"]["after"]]
    inventories.extend(history["operations"] for history in snapshot["histories"])
    for operations in inventories:
        for operation in operations:
            if operation["id"] == operation_id:
                operation["endedAtMs"] = end
    assert any(diagnostic in error for error in _cleanup_behavior_errors(value, snapshot, standard=True))


def test_cleanup_overlap_rejects_additional_record_marker() -> None:
    value = _standard_retirement_snapshot("evidence", requirement=1)
    snapshot = deepcopy(value)
    history = next(row for row in snapshot["histories"] if row["executionArn"] == "arn:execution:victim")
    marker = next(row for row in history["operations"] if row["name"] == "record-marker")
    history["operations"].append({**marker, "id": "second-marker"})
    assert any(
        "more than one record-marker" in error for error in _cleanup_behavior_errors(value, snapshot, standard=True)
    )


def test_cleanup_retry_endpoints_are_checked_across_partial_orders() -> None:
    value = _standard_retirement_snapshot("replay_evidence")
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
    value = _standard_retirement_snapshot("replay_evidence")
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
    _resequence(value)
    snapshot = deepcopy(value)
    if changed_time:
        next(row for row in snapshot["events"] if row.get("attempt") == 2)["atMs"] = 60
    _resequence(snapshot)
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("retry body changed its observed timestamp" in error for error in findings) is changed_time
    if not changed_time:
        assert findings == []


def test_cleanup_deadline_rejects_earlier_admission_closure() -> None:
    value = _standard_retirement_snapshot("deadline_evidence")
    snapshot = deepcopy(value)
    next(row for row in snapshot["events"] if row["kind"] == "admission_closed")["atMs"] = 29
    assert any(
        "admission closed before the observed platform timeout" in error
        for error in _cleanup_behavior_errors(value, snapshot, standard=True)
    )


def test_cleanup_reconciles_original_invocation_and_ordinal_order() -> None:
    value = _standard_retirement_snapshot("evidence")
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
    value = _standard_retirement_snapshot("recovery_evidence" if role_name == "recovery" else "replay_evidence")
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
    value = _standard_retirement_snapshot("recovery_evidence")
    snapshot = deepcopy(value)
    next(row for row in snapshot["events"] if row["kind"] == "worker_recovered")["atMs"] = 43
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("healthy peer lacks progress across the timeout and recovery" in error for error in findings)


def test_cleanup_retry_checks_transitive_body_order_across_fragments() -> None:
    value = _standard_retirement_snapshot("replay_evidence")
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
    _resequence(value)
    snapshot = deepcopy(value)
    last = {"attemptId": "last", "requestId": resume["requestId"], "outcome": "SUCCEEDED"}
    snapshot["replay"]["retry"]["attempts"] = [middle, last]
    snapshot["replay"]["operationExecutions"].append({**last, "operationId": "retry"})
    snapshot["events"].append({**owner, "kind": "body", "operationId": "retry", "attemptId": "last", "atMs": 55})
    _resequence(snapshot)
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("retry attempt order contradicts timed user-code entries" in error for error in findings)


def test_cleanup_retry_cannot_certify_ambiguous_endpoints() -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    snapshot = deepcopy(value)
    first = snapshot["replay"]["retry"]["attempts"][0]
    unplaced = {"attemptId": "unplaced", "requestId": "request-resume", "outcome": "FAILED"}
    snapshot["replay"]["retry"]["attempts"] = [first, unplaced]
    snapshot["replay"]["operationExecutions"].append({**unplaced, "operationId": "retry"})
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("retry order does not establish unique first and last attempts" in error for error in findings)


def test_cleanup_rejects_completed_replay_bodies_after_observed_pending() -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    snapshot = deepcopy(value)
    next(event for event in snapshot["events"] if event["kind"] == "pending")["atMs"] = 20
    _resequence(snapshot)
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("reports repeated completed user code for done" in error for error in findings)
    assert any("reports repeated completed user code for failed" in error for error in findings)


def test_cleanup_recovery_rechecks_declared_observation_grace() -> None:
    value = _standard_retirement_snapshot("recovery_evidence")
    value["observationEndedAtMs"] = 100
    value["budgets"]["observationGraceMs"] = 55
    snapshot = deepcopy(value)
    next(row for row in snapshot["events"] if row["kind"] == "worker_recovered")["atMs"] = 46
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("observation window lacks the declared grace after recovery" in error for error in findings)


def test_cleanup_rejects_malformed_interior_retry_outcome() -> None:
    value = _standard_retirement_snapshot("replay_evidence")
    snapshot = deepcopy(value)
    malformed = {"attemptId": "malformed", "requestId": "request-resume", "outcome": False}
    snapshot["replay"]["retry"]["attempts"].insert(1, malformed)
    snapshot["replay"]["operationExecutions"].append({**malformed, "operationId": "retry"})
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("attempt malformed outcome must be a non-empty string" in error for error in findings)


def test_cleanup_rechecks_marker_dispatch_at_corrected_overlap_boundary() -> None:
    value = _standard_retirement_snapshot("evidence", requirement=1)
    snapshot = deepcopy(value)
    victim = next(row for row in snapshot["attempts"] if row["role"] == "victim")
    victim.update(endedAtMs=26, endedAtMsSource="platform")
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("overlap peer lacks a marker checkpoint dispatch inside native overlap" in error for error in findings)
    assert not any("barrier lies outside" in error for error in findings)


def test_cleanup_deadline_rechecks_grace_after_corrected_timeout() -> None:
    value = _standard_retirement_snapshot("deadline_evidence")
    value["budgets"]["observationGraceMs"] = 170
    snapshot = deepcopy(value)
    next(row for row in snapshot["events"] if row["kind"] == "platform_timeout")["atMs"] = 30.5
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("observation window lacks the declared grace after timeout" in error for error in findings)


def test_cleanup_recovery_admission_is_checked_against_corrected_timeout() -> None:
    value = _standard_retirement_snapshot("recovery_evidence")
    next(row for row in value["attempts"] if row["role"] == "recovery")["startedAtMs"] = 30.25
    snapshot = deepcopy(value)
    next(row for row in snapshot["events"] if row["kind"] == "platform_timeout")["atMs"] = 30.5
    findings = _cleanup_behavior_errors(value, snapshot, standard=True)
    assert any("places recovery before the victim timeout" in error for error in findings)


def test_cleanup_orders_same_time_poll_by_later_semantic_sequence() -> None:
    value = _standard_retirement_snapshot("warm_evidence")
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
    original = _standard_retirement_snapshot("recovery_evidence")
    snapshot = deepcopy(original)
    recovery = next(row for row in snapshot["events"] if row["kind"] == "worker_recovered")
    snapshot["events"].append({**recovery, "atMs": recovery["atMs"] + 0.5})
    assert any(
        "needs exactly one worker_recovered" in failure
        for failure in _cleanup_behavior_errors(original, snapshot, standard=True)
    )


def _lifecycle_7_retirement_retry() -> tuple[dict[str, Any], dict[str, Any]]:
    """A lifecycle 7 capture whose service retry is proven only by the retirement snapshot."""
    original = _standard_retirement_snapshot("failure_evidence")
    first = original["attempts"][0]
    arn = first["executionArn"]
    original["histories"][0]["status"] = "RUNNING"
    original["expectedHistories"][arn] = {"allowedStatuses": ["RUNNING", "FAILED"]}
    snapshot = deepcopy(original)
    snapshot["histories"][0]["status"] = "SUCCEEDED"
    snapshot["observationEndedAtMs"] = 206
    snapshot["collection"]["watermarks"] = {key: 206 for key in snapshot["collection"]["watermarks"]}
    retry_id = "service-retry"
    snapshot["attempts"].append(
        {**first, "requestId": retry_id, "attempt": 2, "startedAtMs": 201, "endedAtMs": 205, "outcome": "SUCCEEDED"}
    )
    for event in ("wrapper_enter", "task_start", "task_exit", "wrapper_return"):
        template = next(row for row in original["events"] if row["kind"] == event)
        snapshot["events"].append(
            {
                **template,
                "requestId": retry_id,
                "ownerRequestId": retry_id,
                "atMs": 201 if event in ("wrapper_enter", "task_start") else 205,
            }
        )
    invocation = {"kind": "invocation", "id": retry_id, "ownerCaseId": original["caseId"]}
    snapshot["resourcesCreated"].append(invocation)
    snapshot["retirement"]["resources"].append(
        {**invocation, "settledAtMs": 208, "outcome": "QUIESCENT", "evidenceUri": "memory://service"}
    )
    next(row for row in snapshot["retirement"]["resources"] if row["id"] == arn).update(outcome="SUCCEEDED")
    _resequence(snapshot)
    return original, snapshot


def test_adjudication_disposal_uses_verified_retirement_attempt_count() -> None:
    from aws_durable_execution_conformance_tests.lmi.adjudication import evaluate_evidence
    from aws_durable_execution_conformance_tests.lmi.evidence import validate_retirement_evidence

    original, snapshot = _lifecycle_7_retirement_retry()
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
    from aws_durable_execution_conformance_tests.lmi.adjudication import evaluate_evidence

    original = _standard_retirement_snapshot("evidence")
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
