# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Positive controls and deliberate corruption of independent lifecycle evidence."""

from copy import deepcopy
from decimal import Decimal
from typing import Any, Literal

import pytest

from aws_durable_execution_conformance_tests.lmi.evidence import (
    EvidenceValidator,
    _Evidence,
    same_json,
    validate_common_evidence,
    validate_evidence,
    validate_observed_retirement,
    validate_retirement_evidence,
)


def attempt(role: str, *, start: int = 10, end: int = 40, process: str = "process-1") -> dict[str, Any]:
    return {
        "requestId": f"request-{role}",
        "executionArn": f"arn:execution:{role}",
        "environmentId": "environment-1",
        "workerId": process,
        "processId": process,
        "role": role,
        "attempt": 1,
        "startedAtMs": start,
        "endedAtMs": end,
        "outcome": "SUCCEEDED",
    }


def event(item: dict[str, Any], kind: str, time: int, **extra: Any) -> dict[str, Any]:
    return {
        "requestId": item["requestId"],
        "ownerRequestId": item["requestId"],
        "executionArn": item["executionArn"],
        "kind": kind,
        "atMs": time,
        **extra,
    }


def evidence(requirement: int = 1, runtime: str = "javascript") -> dict[str, Any]:
    victim = attempt("victim")
    peer = attempt("peer", start=12, end=42, process="process-2" if runtime == "python" else "process-1")
    result: dict[str, Any] = {
        "schemaVersion": 1,
        "runtime": runtime,
        "requirementId": f"lmi-lifecycle-{requirement}",
        "runId": "run-1",
        "caseId": "case-1",
        "artifact": {"sdk": "sdk@commit-123", "conformance": "conformance@commit-456"},
        "profile": {
            "functionArn": "arn:function:profile-2",
            "configurationDigest": "sha256:config",
            "nativeConcurrency": 2,
            "timeoutMs": 20,
            "recoveryPolicy": "reuse-worker",
        },
        "observationEndedAtMs": 200,
        "attempts": [victim, peer],
        "selectedRequests": {"victim": victim["requestId"], "peer": peer["requestId"]},
        "events": [
            event(victim, "barrier_arrived", 20),
            event(peer, "barrier_arrived", 21),
            event(victim, "root_completed", 35, outcome="SUCCEEDED"),
            event(victim, "wrapper_return", 40),
            event(peer, "wrapper_return", 42),
            event(victim, "task_exit", 41),
            event(victim, "admission_probe", 45, probe="operation", outcome="REJECTED"),
            event(victim, "admission_probe", 46, probe="checkpoint", outcome="REJECTED"),
        ],
        "sdkRequests": [event(victim, "request", 25, method="CheckpointDurableExecution", outcome="DISPATCHED")],
        "collection": {
            "watermarks": {"lifecycle": 200, "sdkRequests": 200, "history": 200},
            "evidenceUris": ["s3://evidence/run-1/case-1/raw.json"],
        },
        "resourcesCreated": [],
        "faultsCreated": [],
        "retirement": {"retiredAtMs": 220, "resources": [], "faults": []},
    }
    if requirement == 1:
        result["sdkRequests"].append(
            event(peer, "request", 26, method="CheckpointDurableExecution", outcome="DISPATCHED")
        )
    if requirement == 1:
        for index, request in enumerate(result["sdkRequests"]):
            request["updates"] = [{"id": f"step-{index}", "type": "STEP", "action": "SUCCEED"}]
    refresh_histories(result)
    record_owned_resources(result)
    return result


def failure_evidence() -> dict[str, Any]:
    value = evidence(7)
    victim = value["attempts"][0]
    victim["outcome"] = "FAILED"
    next(item for item in value["events"] if item["kind"] == "root_completed")["outcome"] = "FAILED"
    expected = {"status": "FAILED", "error": {"type": "FixtureError", "message": "expected handler failure"}}
    value["expectedHistories"][victim["executionArn"]] = expected
    history = next(item for item in value["histories"] if item["executionArn"] == victim["executionArn"])
    history.pop("result")
    history.update(expected)
    retired = next(item for item in value["retirement"]["resources"] if item["id"] == victim["executionArn"])
    retired["outcome"] = "FAILED"
    return value


def refresh_histories(value: dict[str, Any]) -> None:
    expectations = {
        item["executionArn"]: {"status": "SUCCEEDED", "result": {"marker": item["role"]}} for item in value["attempts"]
    }
    value["expectedHistories"] = expectations
    value["histories"] = [
        {
            "executionArn": arn,
            "ownerCaseId": value["caseId"],
            "operations": (
                [
                    {
                        "id": f"step-{index}",
                        "type": "STEP",
                        "name": "record-marker",
                        "parentId": None,
                        "status": "SUCCEEDED",
                        "result": deepcopy(expected["result"]),
                    }
                ]
                if value["requirementId"] == "lmi-lifecycle-1"
                else []
            ),
            **expected,
        }
        for index, (arn, expected) in enumerate(expectations.items())
    ]


def record_owned_resources(value: dict[str, Any]) -> None:
    resources = [
        {"id": item["requestId"], "kind": "invocation", "ownerCaseId": value["caseId"]} for item in value["attempts"]
    ]
    resources += [
        {"id": arn, "kind": "execution", "ownerCaseId": value["caseId"]} for arn in value["expectedHistories"]
    ]
    value["resourcesCreated"] = resources
    value["retirement"]["resources"] = [
        {
            **item,
            "settledAtMs": 210,
            "outcome": "QUIESCENT" if item["kind"] == "invocation" else "SUCCEEDED",
            "evidenceUri": "s3://evidence/run-1/case-1/retired.json",
        }
        for item in resources
    ]


def deadline_evidence(requirement: int = 3, runtime: str = "javascript") -> dict[str, Any]:
    value = evidence(requirement, runtime)
    victim = value["attempts"][0]
    victim["outcome"] = "TIMED_OUT"
    value["budgets"] = {"admissionMs": 5, "cleanupMs": 10, "recoveryMs": 15, "observationGraceMs": 1}
    value["events"] = [
        item for item in value["events"] if item["kind"] not in ("root_completed", "task_exit", "admission_probe")
    ]
    value["events"].extend(
        [
            event(victim, "work_in_flight", 25, taskId="root"),
            event(victim, "platform_timeout", 30, source="platform"),
            event(victim, "admission_closed", 31),
            event(victim, "task_exit", 35, taskId="root"),
            event(victim, "admission_probe", 32, probe="operation", outcome="REJECTED"),
            event(victim, "admission_probe", 33, probe="checkpoint", outcome="REJECTED"),
        ]
    )
    value["faultsCreated"] = [{"id": "fault-1", "ownerCaseId": "case-1"}]
    value["retirement"]["faults"] = [{"id": "fault-1", "ownerCaseId": "case-1", "releasedAtMs": 210, "mode": "normal"}]
    return value


def recovery_evidence(runtime: str = "javascript", *, native_concurrency: int = 2) -> dict[str, Any]:
    value = deadline_evidence(4, runtime)
    value["profile"]["nativeConcurrency"] = native_concurrency
    peer = value["attempts"][1]
    peer["endedAtMs"] = 100
    recovery = attempt("recovery", start=40, end=60)
    value["attempts"][0]["endedAtMsSource"] = "platform"
    recovery["startedAtMsSource"] = "platform"
    value["attempts"].append(recovery)
    value["selectedRequests"]["recovery"] = recovery["requestId"]
    value["events"] = [
        item for item in value["events"] if item["requestId"] != peer["requestId"] or item["kind"] != "wrapper_return"
    ]
    value["events"].extend(
        [
            event(peer, "peer_progress", 25),
            event(peer, "peer_progress", 44),
            event(recovery, "worker_recovered", 45),
            event(peer, "wrapper_return", 100),
        ]
    )
    for index in range(2, native_concurrency):
        peer = attempt(
            f"peer{index}", start=12, end=100, process=f"process-{index + 1}" if runtime == "python" else "process-1"
        )
        value["attempts"].append(peer)
        value["selectedRequests"][peer["role"]] = peer["requestId"]
        value["events"].extend(
            [event(peer, "peer_progress", 25), event(peer, "peer_progress", 44), event(peer, "wrapper_return", 100)]
        )
    refresh_histories(value)
    record_owned_resources(value)
    return value


def replay_evidence() -> dict[str, Any]:
    value = evidence(5)
    victim = value["attempts"][0]
    victim["outcome"] = "PENDING"
    resume = attempt("resume", start=50, end=80)
    resume["executionArn"] = victim["executionArn"]
    value["attempts"].append(resume)
    value["selectedRequests"]["resume"] = resume["requestId"]
    value["events"].extend(
        [
            event(victim, "body", 21, operationId="done"),
            event(victim, "body", 22, operationId="failed"),
            event(victim, "pending", 35),
        ]
    )
    success = {
        "id": "done",
        "name": "completed-success",
        "type": "STEP",
        "parentId": None,
        "status": "SUCCEEDED",
        "result": {"value": 1},
    }
    failure = {
        "id": "failed",
        "name": "completed-failure",
        "type": "STEP",
        "parentId": None,
        "status": "FAILED",
        "error": {"type": "PermanentError"},
    }
    retried = {
        "id": "retry",
        "name": "retry-transient-step",
        "type": "STEP",
        "parentId": None,
        "status": "SUCCEEDED",
        "result": "recovered",
    }
    wait = {"id": "pause", "name": "pause", "type": "WAIT", "parentId": None, "status": "SUCCEEDED", "endedAtMs": 45}
    value["sdkRequests"][0]["updates"] = [{"id": "pause", "type": "WAIT", "action": "START"}]
    value["replay"] = {
        "before": [success, failure],
        "after": [deepcopy(success), deepcopy(failure), retried, wait],
        "operationExecutions": [
            {"requestId": victim["requestId"], "operationId": "done"},
            {"requestId": victim["requestId"], "operationId": "failed"},
        ],
        "sideEffects": [
            {"operationId": "done", "count": 1, "expectedCount": 1},
            {"operationId": "failed", "count": 1, "expectedCount": 1},
        ],
    }
    value["replay"]["retry"] = {
        "operationId": "retry",
        "attempts": [
            {"attemptId": "retry-1", "requestId": victim["requestId"], "outcome": "FAILED"},
            {"attemptId": "retry-2", "requestId": resume["requestId"], "outcome": "SUCCEEDED"},
        ],
    }
    value["replay"]["operationExecutions"].extend(
        [{**item, "operationId": "retry"} for item in value["replay"]["retry"]["attempts"]]
    )
    value["replay"]["sideEffects"].append({"operationId": "retry", "count": 2, "expectedCount": 2})
    refresh_histories(value)
    next(item for item in value["histories"] if item["executionArn"] == victim["executionArn"])["operations"] = (
        deepcopy(value["replay"]["after"])
    )
    record_owned_resources(value)
    return value


def unnamed_context_replay_evidence() -> dict[str, Any]:
    """A public unnamed child context with a completed, named step inside it."""
    value = replay_evidence()
    context = {
        "id": "context",
        "name": None,
        "type": "CONTEXT",
        "parentId": None,
        "status": "SUCCEEDED",
        "result": {"value": 1},
    }
    for operations in (value["replay"]["before"], value["replay"]["after"]):
        operations[0]["parentId"] = context["id"]
        operations.insert(0, deepcopy(context))
    victim = value["attempts"][0]
    next(history for history in value["histories"] if history["executionArn"] == victim["executionArn"])[
        "operations"
    ] = deepcopy(value["replay"]["after"])
    value["replay"]["operationExecutions"].append({"requestId": victim["requestId"], "operationId": context["id"]})
    value["replay"]["sideEffects"].append({"operationId": context["id"], "count": 0, "expectedCount": 0})
    return value


def warm_evidence(runtime: str = "python") -> dict[str, Any]:
    value = evidence(6, runtime)
    victim = value["attempts"][0]
    next(item for item in value["events"] if item["kind"] == "task_exit")["atMs"] = 65
    victim["endedAtMsSource"] = "platform"
    warm = {**attempt("warm", start=50, end=80), "startedAtMsSource": "platform"}
    value["attempts"].append(warm)
    value["selectedRequests"]["warm"] = warm["requestId"]
    value["events"].append(event(victim, "late_activity", 60, observedDuringRequestId=warm["requestId"]))
    refresh_histories(value)
    record_owned_resources(value)
    return value


@pytest.mark.parametrize("runtime", ["javascript", "java", "python"])
def test_native_runtime_overlap_positive_controls(runtime: str) -> None:
    assert validate_evidence(evidence(runtime=runtime)) == []


@pytest.mark.parametrize(
    "factory",
    [lambda: evidence(2), deadline_evidence, recovery_evidence, replay_evidence, warm_evidence, failure_evidence],
)
def test_lifecycle_positive_controls(factory: Any) -> None:
    assert validate_evidence(factory()) == []


@pytest.mark.parametrize("requirement_id", [2, 7])
def test_normal_return_and_failure_require_their_own_outcome(requirement_id: int) -> None:
    value = evidence(2) if requirement_id == 2 else failure_evidence()
    assert validate_evidence(value) == []
    value["requirementId"] = "lmi-lifecycle-7" if requirement_id == 2 else "lmi-lifecycle-2"
    expected = "FAILED" if requirement_id == 2 else "SUCCEEDED"
    assert any(f"requires the selected victim outcome {expected}" in error for error in validate_evidence(value))


@pytest.mark.parametrize("requirement_id", [2, 7])
def test_disposal_binds_root_outcome_and_wrapper_order(requirement_id: int) -> None:
    value = evidence(2) if requirement_id == 2 else failure_evidence()
    root = next(item for item in value["events"] if item["kind"] == "root_completed")
    outcome = root["outcome"]
    root["outcome"] = "FAILED" if outcome == "SUCCEEDED" else "SUCCEEDED"
    assert "ASSERTION: root completion and actual invocation outcome disagree" in validate_evidence(value)

    root["outcome"] = outcome
    wrapper = next(item for item in value["events"] if item["kind"] == "wrapper_return")
    wrapper["atMs"] = root["atMs"] - 1
    assert "ASSERTION: wrapper return precedes root completion" in validate_evidence(value)


@pytest.mark.parametrize(
    "field",
    [
        "artifact",
        "profile",
        "attempts",
        "selectedRequests",
        "events",
        "sdkRequests",
        "histories",
        "expectedHistories",
        "collection",
        "retirement",
    ],
)
def test_missing_evidence_never_passes(field: str) -> None:
    value = evidence()
    del value[field]
    assert validate_evidence(value)


@pytest.mark.parametrize(
    "runtime,field,replacement",
    [
        ("javascript", "workerId", "other-worker"),
        ("java", "processId", "other-jvm"),
        ("python", "processId", "process-1"),
        ("python", "environmentId", "replacement-environment"),
    ],
)
def test_runtime_placement_rejects_wrong_sharing_boundary(runtime: str, field: str, replacement: str) -> None:
    value = evidence(runtime=runtime)
    value["attempts"][1][field] = replacement
    assert any(error.startswith("PLACEMENT:") for error in validate_evidence(value))


def test_selected_retry_cannot_replace_original_request() -> None:
    value = evidence()
    retry = deepcopy(value["attempts"][0])
    retry.update(requestId="replacement-retry", attempt=2)
    value["attempts"].append(retry)
    value["selectedRequests"]["victim"] = retry["requestId"]
    assert any("retry" in error for error in validate_evidence(value))


@pytest.mark.parametrize("runtime", ["javascript", "java", "python"])
@pytest.mark.parametrize("phase", ["observations", "final"])
def test_duplicate_original_ordinal_cannot_hide_failed_placement(
    runtime: str, phase: Literal["observations", "final"]
) -> None:
    value = evidence(runtime=runtime)
    original = attempt("victim", start=1, end=9)
    original.update(
        requestId="request-original-victim",
        executionArn="arn:execution:original-victim",
        environmentId="failed-placement-environment",
    )
    # The selected, later request has valid placement but also claims victim/1.
    # Retain the real original and its history instead of discarding the miss.
    value["attempts"].insert(0, original)
    value["events"].append(event(original, "wrapper_return", 9))
    refresh_histories(value)
    record_owned_resources(value)
    for validate in (validate_evidence, validate_common_evidence):
        errors = validate(value, phase=phase)
        assert any("duplicate attempt ordinal 1 for role victim" in error for error in errors), errors


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("same_execution", [False, True])
def test_duplicate_retry_ordinals_are_rejected_across_executions(
    phase: Literal["observations", "final"], same_execution: bool
) -> None:
    value = evidence()
    first = attempt("victim", start=50, end=60)
    first.update(requestId="request-victim-retry-2", attempt=2)
    second = {**first, "requestId": "request-duplicate-retry-2", "startedAtMs": 70, "endedAtMs": 80}
    if not same_execution:
        second["executionArn"] = "arn:execution:another-victim"
    value["attempts"].extend([first, second])
    refresh_histories(value)
    record_owned_resources(value)
    for validate in (validate_evidence, validate_common_evidence):
        errors = validate(value, phase=phase)
        assert any("duplicate attempt ordinal 2 for role victim" in error for error in errors), errors


@pytest.mark.parametrize("phase", ["observations", "final"])
def test_attempt_ordinals_are_unique_within_each_role(phase: Literal["observations", "final"]) -> None:
    value = replay_evidence()
    # victim/1 and resume/1 share an execution; a subsequent resume/2 is valid.
    resume = value["attempts"][-1]
    value["attempts"].append(
        {**resume, "requestId": "request-resume-2", "attempt": 2, "startedAtMs": 90, "endedAtMs": 100}
    )
    record_owned_resources(value)
    for validate in (validate_evidence, validate_common_evidence):
        assert validate(value, phase=phase) == []


def test_barriers_and_intervals_must_both_prove_overlap() -> None:
    value = evidence()
    value["events"][1]["atMs"] = 41
    assert any("barrier" in error for error in validate_evidence(value))


def test_overlap_requires_distinct_executions_and_native_capacity() -> None:
    value = evidence()
    value["profile"]["nativeConcurrency"] = 1
    assert "PLACEMENT: native overlap requires a deployed concurrency of at least two" in validate_evidence(value)

    value = evidence()
    victim, peer = value["attempts"]
    peer["executionArn"] = victim["executionArn"]
    peer["startedAtMs"] = victim["startedAtMs"]
    for record in (*value["events"], *value["sdkRequests"]):
        if record["requestId"] == peer["requestId"]:
            record["executionArn"] = victim["executionArn"]
    refresh_histories(value)
    record_owned_resources(value)
    assert "PLACEMENT: overlap needs distinct durable executions" in validate_evidence(value)


@pytest.mark.parametrize("stream", ["events", "sdkRequests"])
def test_cross_invocation_attribution_is_rejected(stream: str) -> None:
    value = warm_evidence()
    value[stream][0]["ownerRequestId"] = "request-warm"
    assert any("different invocation" in error for error in validate_evidence(value))


def test_dispatch_after_wrapper_return_is_rejected() -> None:
    value = warm_evidence()
    value["sdkRequests"][0]["atMs"] = 60
    assert any("after admission closed" in error for error in validate_evidence(value))


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("validate", [validate_evidence, validate_common_evidence])
@pytest.mark.parametrize(
    ("keep_peer_return", "at", "outcome", "expected"),
    [
        (True, 100, "DISPATCHED", "ASSERTION: request request-peer dispatched SDK work after admission closed"),
        (
            False,
            100,
            "DISPATCHED",
            "INCOMPLETE: request request-peer dispatched SDK work after its recorded end without closure evidence",
        ),
        (False, 41, "DISPATCHED", None),
        (False, 42, "DISPATCHED", None),
        (False, 100, "REJECTED", None),
    ],
)
def test_late_peer_dispatch(
    phase: Literal["observations", "final"],
    validate: EvidenceValidator,
    keep_peer_return: bool,
    at: int,
    outcome: str,
    expected: str | None,
) -> None:
    value = evidence()
    peer = value["attempts"][1]
    if not keep_peer_return:
        value["events"].remove(
            next(e for e in value["events"] if e["requestId"] == peer["requestId"] and e["kind"] == "wrapper_return")
        )
    value["sdkRequests"].append(event(peer, "request", at, method="CheckpointDurableExecution", outcome=outcome))

    assert validate(value, phase=phase) == ([expected] if expected is not None else [])


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("validate", [validate_evidence, validate_common_evidence])
def test_dispatch_after_recorded_end_before_explicit_closure_is_allowed(
    phase: Literal["observations", "final"], validate: EvidenceValidator
) -> None:
    value = evidence()
    peer = value["attempts"][1]
    closure = next(e for e in value["events"] if e["requestId"] == peer["requestId"] and e["kind"] == "wrapper_return")
    closure.update(kind="admission_closed", atMs=50)
    value["sdkRequests"].append(event(peer, "request", 45, method="CheckpointDurableExecution", outcome="DISPATCHED"))

    assert validate(value, phase=phase) == []


@pytest.mark.parametrize("ended", [False, -1])
@pytest.mark.parametrize("validate", [validate_evidence, validate_common_evidence])
def test_malformed_recorded_end_does_not_order_dispatch(validate: EvidenceValidator, ended: Any) -> None:
    value = evidence()
    peer = value["attempts"][1]
    value["events"].remove(
        next(e for e in value["events"] if e["requestId"] == peer["requestId"] and e["kind"] == "wrapper_return")
    )
    peer["endedAtMs"] = ended

    errors = validate(value)
    assert "INCOMPLETE: attempt request-peer.endedAtMs must be a finite non-negative number" in errors, errors
    assert not any("after its recorded end" in error for error in errors), errors


def test_malformed_sdk_requests_keep_disposal_wrapper_diagnostics() -> None:
    value = evidence(2)
    value["events"].append(event(value["attempts"][0], "wrapper_raise", 41))
    value["sdkRequests"] = None

    errors = validate_evidence(value)
    assert "INCOMPLETE: sdkRequests must be an array" in errors, errors
    assert "INCOMPLETE: disposal needs exactly one wrapper exit for the selected victim" in errors, errors


@pytest.mark.parametrize("validate", [validate_evidence, validate_common_evidence])
def test_malformed_dispatch_time_does_not_order_dispatch(validate: EvidenceValidator) -> None:
    value = evidence()
    peer = value["attempts"][1]
    value["events"].remove(
        next(e for e in value["events"] if e["requestId"] == peer["requestId"] and e["kind"] == "wrapper_return")
    )
    peer.update(startedAtMs=0, endedAtMs=0.5)
    next(request for request in value["sdkRequests"] if request["requestId"] == peer["requestId"])["atMs"] = True

    errors = validate(value)
    assert "INCOMPLETE: sdkRequest.atMs must be a finite non-negative number" in errors, errors
    assert not any("after its recorded end" in error for error in errors), errors


@pytest.mark.parametrize("validate", [validate_evidence, validate_common_evidence])
def test_unvalidated_recorded_end_keeps_the_closure_assertion(validate: EvidenceValidator) -> None:
    value = evidence()
    victim = value["attempts"][0]
    # common() rejects an active attempt with an end without validating that end.
    victim.update(activeAtObservationEnd=True, endedAtMs=10**400)
    value["sdkRequests"].append(event(victim, "request", 41, method="CheckpointDurableExecution", outcome="DISPATCHED"))

    errors = validate(value)
    assert "INCOMPLETE: attempt request-victim cannot represent an active retry at observation end" in errors, errors
    assert "ASSERTION: request request-victim dispatched SDK work after admission closed" in errors, errors


@pytest.mark.parametrize(("sequence", "late"), [(None, True), (9, False), (11, True)])
def test_dispatch_at_closure_timestamp_uses_semantic_sequence(sequence: int | None, late: bool) -> None:
    value = evidence(2)
    wrapper = next(item for item in value["events"] if item["kind"] == "wrapper_return")
    wrapper["sequence"] = 10
    request = value["sdkRequests"][0]
    request["atMs"] = wrapper["atMs"]
    if sequence is not None:
        request["sequence"] = sequence
    errors = validate_evidence(value)
    assert any("dispatched SDK work after admission closed" in error for error in errors) is late, errors


def test_missing_dispatch_collection_cannot_prove_no_dispatch() -> None:
    value = evidence(2)
    value["sdkRequests"] = []
    value["collection"]["watermarks"]["sdkRequests"] = 39
    assert any("does not cover" in error for error in validate_evidence(value))


@pytest.mark.parametrize("requirement", [2, 7])
@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("probe", ["operation", "checkpoint"])
@pytest.mark.parametrize(
    ("at", "outcome", "rejected"),
    [
        (35, "ACCEPTED", False),
        (36, "ACCEPTED", True),
        (37, "ACCEPTED", True),
        (45, "ACCEPTED", True),
        (37, "REJECTED", False),
    ],
)
def test_disposal_checks_probe_outcomes_from_the_first_closure(
    requirement: int, phase: Literal["observations", "final"], probe: str, at: int, outcome: str, rejected: bool
) -> None:
    value = evidence(2) if requirement == 2 else failure_evidence()
    victim = value["attempts"][0]
    # Keep the rejected probes after wrapper return at 40. They must not hide
    # an accepted probe at or after the earlier admission closure at 36.
    value["events"].extend(
        [event(victim, "admission_closed", 36), event(victim, "admission_probe", at, probe=probe, outcome=outcome)]
    )
    errors = validate_evidence(value, phase=phase)
    if rejected:
        assert any(f"closed invocation admitted a {probe} probe" in error for error in errors), errors
    else:
        assert errors == [], errors


@pytest.mark.parametrize("requirement", [3, 4])
@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("probe", ["operation", "checkpoint"])
def test_deadline_checks_probes_from_an_earlier_wrapper_return(
    requirement: int, phase: Literal["observations", "final"], probe: str
) -> None:
    value = deadline_evidence() if requirement == 3 else recovery_evidence()
    victim = value["attempts"][0]
    wrapper = next(
        item
        for item in value["events"]
        if item["requestId"] == victim["requestId"] and item["kind"] == "wrapper_return"
    )
    wrapper["atMs"] = 30
    assert validate_evidence(value, phase=phase) == []
    # Admission closure at 31 cannot hide acceptance after wrapper return.
    value["events"].append(event(victim, "admission_probe", 30, probe=probe, outcome="ACCEPTED"))
    assert any(
        f"closed invocation admitted a {probe} probe" in error for error in validate_evidence(value, phase=phase)
    )


@pytest.mark.parametrize("factory", [lambda: evidence(2), failure_evidence])
@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("outcome", [None, "ACCEPTED", "REJECTED", "UNKNOWN"])
def test_disposal_operation_probe_is_optional_diagnostic(factory: Any, phase: Any, outcome: str | None) -> None:
    value = factory()
    value["events"] = [item for item in value["events"] if item.get("probe") != "operation"]
    if outcome is not None:
        value["events"].append(event(value["attempts"][0], "admission_probe", 40, probe="operation", outcome=outcome))
    errors = validate_evidence(value, phase=phase)
    if outcome == "UNKNOWN":
        assert any("operation probe outcome" in error for error in errors), errors
    else:
        assert errors == [], errors


@pytest.mark.parametrize("factory", [lambda: evidence(2), failure_evidence])
@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize(
    ("change", "diagnostic"),
    [
        ("early-checkpoint", "missing post-closure checkpoint admission probe"),
        ("accepted-checkpoint", "closed invocation admitted a checkpoint probe"),
        ("dispatch", "dispatched SDK work after admission closed"),
        ("ownership", "different invocation"),
        ("missing-wrapper", "wrapper_return"),
        ("peer-operation", "closed invocation admitted a operation probe"),
        ("raised-wrapper", "closed invocation admitted a operation probe"),
    ],
)
def test_disposal_diagnostic_does_not_replace_closure_evidence(
    factory: Any, phase: Any, change: str, diagnostic: str
) -> None:
    value = factory()
    victim, peer = value["attempts"]
    operation = next(item for item in value["events"] if item.get("probe") == "operation")
    checkpoint = next(item for item in value["events"] if item.get("probe") == "checkpoint")
    operation["outcome"] = "ACCEPTED"
    if change == "early-checkpoint":
        operation["outcome"] = "REJECTED"
        value["events"].append(event(victim, "admission_closed", 36))
        checkpoint["atMs"] = 37
    elif change == "accepted-checkpoint":
        checkpoint["outcome"] = "ACCEPTED"
    elif change == "dispatch":
        value["sdkRequests"][0]["atMs"] = 45
    elif change == "ownership":
        operation["ownerRequestId"] = peer["requestId"]
    elif change == "peer-operation":
        value["events"].append(event(peer, "admission_probe", 45, probe="operation", outcome="ACCEPTED"))
    elif change == "raised-wrapper":
        wrapper = next(
            e for e in value["events"] if e["requestId"] == victim["requestId"] and e["kind"] == "wrapper_return"
        )
        wrapper["kind"] = "wrapper_raise"
    else:
        value["events"] = [
            item
            for item in value["events"]
            if item["kind"] != "wrapper_return" or item["requestId"] != victim["requestId"]
        ]
    errors = validate_evidence(value, phase=phase)
    assert any(diagnostic in error for error in errors), errors


@pytest.mark.parametrize("factory", [lambda: evidence(2), failure_evidence])
@pytest.mark.parametrize("phase", ["observations", "final"])
def test_disposal_rejects_mixed_wrapper_exits_without_operation_probe(factory: Any, phase: Any) -> None:
    value = factory()
    victim = value["attempts"][0]
    value["events"] = [item for item in value["events"] if item.get("probe") != "operation"]
    value["events"].append(event(victim, "wrapper_raise", 39))

    errors = validate_evidence(value, phase=phase)
    assert any("wrapper exit" in error for error in errors), errors


@pytest.mark.parametrize("factory", [lambda: evidence(2), failure_evidence])
@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("probe", ["operation", "checkpoint"])
@pytest.mark.parametrize(
    ("wrapper_sequence", "probe_sequence", "position"),
    [
        (10, 9, "before"),
        (10, 11, "after"),
        (None, None, "after"),
        (10, None, "after"),
        (None, 9, "after"),
        (10, 10, "ambiguous"),
        (10, "9", "ambiguous"),
        (10, -1, "ambiguous"),
    ],
)
def test_disposal_orders_probes_sharing_the_wrapper_timestamp(
    factory: Any, phase: Any, probe: str, wrapper_sequence: Any, probe_sequence: Any, position: str
) -> None:
    value = factory()
    victim = value["attempts"][0]
    wrapper = next(
        item
        for item in value["events"]
        if item["requestId"] == victim["requestId"] and item["kind"] == "wrapper_return"
    )
    sample = next(item for item in value["events"] if item.get("probe") == probe)
    sample.update(atMs=40, outcome="ACCEPTED" if probe == "operation" else "REJECTED")
    if position == "before":
        value["events"].append(event(victim, "admission_closed", 36))
    if wrapper_sequence is not None:
        wrapper["sequence"] = wrapper_sequence
    if probe_sequence is not None:
        sample["sequence"] = probe_sequence

    errors = validate_evidence(value, phase=phase)
    if position == "ambiguous":
        assert any("order" in error or "sequence" in error for error in errors), errors
    elif position == "after":
        assert errors == [], errors
    else:
        diagnostic = (
            "closed invocation admitted a operation probe"
            if probe == "operation"
            else "missing post-closure checkpoint admission probe"
        )
        assert any(diagnostic in error for error in errors), errors


@pytest.mark.parametrize("closure_first", [True, False])
@pytest.mark.parametrize("probe_kind", ["checkpoint", "operation"])
def test_disposal_validates_tied_closure_even_after_return(closure_first: bool, probe_kind: str) -> None:
    value = evidence(2)
    probe = next(item for item in value["events"] if item.get("probe") == probe_kind)
    probe["sequence"] = -1
    closure = event(value["attempts"][0], "admission_closed", probe["atMs"], sequence=12)
    value["events"].insert(0 if closure_first else len(value["events"]), closure)
    errors = validate_evidence(value)
    assert any("sequence" in error for error in errors), errors


@pytest.mark.parametrize("factory", [deadline_evidence, recovery_evidence])
@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("probe", ["operation", "checkpoint"])
def test_deadline_and_recovery_require_both_probes(factory: Any, phase: Any, probe: str) -> None:
    value = factory()
    value["events"] = [item for item in value["events"] if item.get("probe") != probe]
    errors = validate_evidence(value, phase=phase)
    assert any(f"missing post-closure {probe} admission probe" in error for error in errors), errors


@pytest.mark.parametrize("phase", ["observations", "final"])
def test_probe_outcomes_ignore_another_invocations_closure(phase: Literal["observations", "final"]) -> None:
    value = evidence(2)
    victim, peer = value["attempts"]
    value["events"].extend(
        [
            event(peer, "admission_closed", 36),
            event(victim, "admission_probe", 37, probe="operation", outcome="ACCEPTED"),
        ]
    )
    assert validate_evidence(value, phase=phase) == []


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("probe", ["operation", "checkpoint"])
@pytest.mark.parametrize(
    ("role", "at_ms", "outcome", "rejected"),
    [
        ("victim", 39, "ACCEPTED", False),
        ("victim", 40, "ACCEPTED", True),
        ("victim", 60, "ACCEPTED", True),
        ("victim", 60, "REJECTED", False),
        ("peer", 60, "ACCEPTED", True),
        ("warm", 60, "ACCEPTED", False),
    ],
)
def test_warm_reuse_checks_all_recorded_post_closure_probes(
    phase: Any, probe: str, role: str, at_ms: int, outcome: str, rejected: bool
) -> None:
    value = warm_evidence()
    owner = next(item for item in value["attempts"] if item["role"] == role)
    value["events"].append(event(owner, "admission_probe", at_ms, probe=probe, outcome=outcome))
    errors = validate_evidence(value, phase=phase)
    if rejected:
        assert any(f"closed invocation admitted a {probe} probe" in error for error in errors), errors
    else:
        assert errors == [], errors


def complete_sdk_dispatches(value: dict[str, Any]) -> None:
    for index, request in enumerate(value["sdkRequests"]):
        if request["outcome"] != "DISPATCHED":
            continue
        call_id = f"dispatch-{index}"
        request["callId"] = call_id
        call_kind = "poll" if request["method"] == "GetDurableExecutionState" else "checkpoint"
        value["events"].extend(
            [
                {**request, "kind": f"{call_kind}_call"},
                {**request, "kind": f"{call_kind}_exit", "atMs": request["atMs"] + 0.001},
            ]
        )


def complete_retirement_events(value: dict[str, Any]) -> None:
    complete_sdk_dispatches(value)
    for owner in value["attempts"]:
        value["events"].extend(
            [
                event(owner, "wrapper_enter", owner["startedAtMs"]),
                event(owner, "task_start", owner["startedAtMs"], taskId="root"),
            ]
        )
        owned = [item for item in value["events"] if item["requestId"] == owner["requestId"]]
        exits = [item for item in owned if item["kind"] == "task_exit"]
        if exits:
            exits[0]["taskId"] = "root"
        else:
            value["events"].append(event(owner, "task_exit", owner["endedAtMs"], taskId="root"))
        if not any(item["kind"] in {"wrapper_return", "wrapper_raise"} for item in owned):
            value["events"].append(event(owner, "wrapper_return", owner["endedAtMs"]))
        ordered = sorted(
            (item for item in value["events"] if item["requestId"] == owner["requestId"]),
            key=lambda item: item["atMs"],
        )
        for sequence, item in enumerate(ordered):
            item["sequence"] = sequence


def resequence(value: dict[str, Any]) -> None:
    for request in value["attempts"]:
        local = sorted(
            (event for event in value["events"] if event["requestId"] == request["requestId"]),
            key=lambda event: event["atMs"],
        )
        for sequence, event in enumerate(local):
            event["sequence"] = sequence


def complete_fixture_sdk_calls(value: dict[str, Any]) -> None:
    value["events"] = [
        event for event in value["events"] if not str(event.get("callId", "")).startswith("fixture-dispatch-")
    ]
    for index, request in enumerate(value["sdkRequests"]):
        if request["outcome"] != "DISPATCHED":
            continue
        request["callId"] = f"fixture-dispatch-{index}"
        kind = "poll" if request["method"] == "GetDurableExecutionState" else "checkpoint"
        value["events"].extend([{**request, "kind": f"{kind}_call"}, {**request, "kind": f"{kind}_exit"}])
    resequence(value)


def standard_retirement_snapshot(factory: str, runtime: str = "javascript", *, requirement: int = 2) -> dict[str, Any]:
    make = globals()[factory]
    value = (
        make(requirement, runtime)
        if factory == "evidence"
        else make()
        if factory in ("replay_evidence", "failure_evidence")
        else make(runtime=runtime)
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
    complete_fixture_sdk_calls(value)
    # Callers load this module with run_path, which bypasses pytest's assertion
    # rewriting, so the messages carry the findings.
    errors = validate_evidence(value)
    assert errors == [], errors
    errors = validate_retirement_evidence(value)
    assert errors == [], errors
    return value


def lifecycle_7_retirement_retry() -> tuple[dict[str, Any], dict[str, Any]]:
    """A lifecycle 7 capture whose service retry is proven only by the retirement snapshot."""
    original = standard_retirement_snapshot("failure_evidence")
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
    resequence(snapshot)
    return original, snapshot


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ("none", None),
        ("missing-pair", "cannot correlate dispatched SDK request"),
        ("wrong-method", "cannot correlate dispatched SDK request"),
        ("unknown-method", "no completion contract for dispatched SDK method"),
        ("missing-id", "dispatched SDK request.callId"),
        ("wrong-time", "cannot correlate dispatched SDK request"),
        ("duplicate", "cannot correlate dispatched SDK request"),
        ("rejected", None),
    ],
)
def test_independent_retirement_correlates_dispatch_with_completion(mutation: str, reason: str | None) -> None:
    value = evidence()
    request = value["sdkRequests"][0]
    request["method"] = "GetDurableExecutionState"
    if mutation == "rejected":
        request["outcome"] = "REJECTED"
    complete_retirement_events(value)
    if mutation == "missing-pair":
        value["events"] = [item for item in value["events"] if item.get("callId") != "dispatch-0"]
    elif mutation == "wrong-method":
        request["method"] = "CheckpointDurableExecution"
    elif mutation == "unknown-method":
        request["method"] = "UnknownMethod"
    elif mutation == "missing-id":
        del request["callId"]
    elif mutation == "wrong-time":
        request["atMs"] += 1
    elif mutation == "duplicate":
        value["sdkRequests"].append(request.copy())
    errors = validate_retirement_evidence(value)
    if reason is None:
        assert errors == [], errors
    else:
        assert any(reason in error for error in errors), errors


@pytest.mark.parametrize("missing", ["dispatch", "operation", "foreign-result"])
def test_overlap_requires_activity_attributed_to_each_invocation(missing: str) -> None:
    value = evidence()
    if missing == "dispatch":
        value["sdkRequests"][1]["outcome"] = "REJECTED"
        reason = "overlap peer lacks an attributed checkpoint dispatch"
    elif missing == "operation":
        value["histories"][1]["operations"] = []
        reason = "overlap peer needs exactly one record-marker step"
    else:
        value["histories"][1]["operations"][0]["result"] = value["histories"][0]["operations"][0]["result"]
        reason = "overlap peer record-marker step does not match the fixture expectation"
    for phase in ("observations", "final"):
        assert any(reason in error for error in validate_evidence(value, phase=phase))


def test_overlap_requires_distinct_fixture_results() -> None:
    value = evidence()
    victim, peer = value["histories"]
    peer["result"] = victim["result"]
    peer["operations"][0]["result"] = victim["result"]
    value["expectedHistories"][peer["executionArn"]]["result"] = victim["result"]
    assert "INCOMPLETE: overlap fixture must declare distinct expected results for the selected executions" in (
        validate_evidence(value)
    )


@pytest.mark.parametrize(
    ("completion_at", "reason"),
    [
        (None, "retirement lacks a unique completed io_start/io_exit interval"),
        (205, None),
        (199, "completion must follow the behavior observation window"),
        (211, "interval lies outside settlement"),
    ],
)
def test_final_retirement_checks_observed_work_and_cleanup_completions(
    completion_at: int | None, reason: str | None
) -> None:
    value = evidence()
    owner = value["attempts"][0]
    value["events"].append(event(owner, "io_start", 30, taskId="held"))
    if completion_at is not None:
        value["retirement"]["completions"] = [event(owner, "io_exit", completion_at, taskId="held")]
    assert validate_evidence(value, phase="observations") == []
    errors = validate_evidence(value, phase="final")
    if reason is None:
        assert errors == [], errors
    else:
        assert any(reason in error for error in errors), errors


def test_unlisted_sdk_call_is_incomplete_behavior_but_does_not_block_retirement() -> None:
    value = evidence()
    complete_retirement_events(value)
    dispatch = next(item for item in value["events"] if item.get("kind") == "checkpoint_call")
    del dispatch["outcome"]
    assert validate_evidence(value) == []
    del value["sdkRequests"][0]
    for phase in ("observations", "final"):
        assert any(
            "observed SDK call lacks a unique matching SDK request" in error
            for error in validate_evidence(value, phase=phase)
        )
    assert validate_retirement_evidence(value) == []


@pytest.mark.parametrize("source", [None, "fixture"])
def test_warm_reuse_requires_platform_native_end(source: str | None) -> None:
    value = warm_evidence()
    victim = value["attempts"][0]
    if source is None:
        del victim["endedAtMsSource"]
    else:
        victim["endedAtMsSource"] = source
    for phase in ("observations", "final"):
        assert "INCOMPLETE: warm reuse requires the victim's platform native completion time" in validate_evidence(
            value, phase=phase
        )


def test_independent_retirement_audits_quiescence_without_rechecking_probe_behavior() -> None:
    value = evidence()
    value["events"].append(event(value["attempts"][0], "admission_probe", 60, probe="operation", outcome="ACCEPTED"))
    complete_retirement_events(value)

    assert any("closed invocation admitted" in error for error in validate_common_evidence(value))
    assert validate_retirement_evidence(value) == []


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ("early", "platform timeout did not cross the configured invocation deadline"),
        ("simulated", "deadline requires an observed platform timeout"),
        ("no-inflight", "needs exactly one work_in_flight event"),
        ("late-inflight", "controlled work was not in flight across the real deadline"),
        ("accepted", "closed invocation admitted a"),
        ("admission-budget", "deadline admissionMs exceeds the declared fixture budget"),
        ("cleanup-budget", "deadline cleanupMs exceeds the declared fixture budget"),
    ],
)
def test_deadline_rejects_insufficient_platform_and_admission_evidence(change: str, reason: str) -> None:
    value = deadline_evidence()
    timeout = next(item for item in value["events"] if item["kind"] == "platform_timeout")
    if change == "early":
        timeout["atMs"] = 29
    elif change == "simulated":
        timeout["source"] = "fixture"
    elif change == "no-inflight":
        value["events"] = [item for item in value["events"] if item["kind"] != "work_in_flight"]
    elif change == "late-inflight":
        next(item for item in value["events"] if item["kind"] == "work_in_flight")["atMs"] = timeout["atMs"] + 1
    elif change == "accepted":
        next(item for item in value["events"] if item["kind"] == "admission_probe")["outcome"] = "ACCEPTED"
    elif change == "admission-budget":
        value["budgets"]["admissionMs"] = 0.5
    else:
        value["budgets"]["cleanupMs"] = 1
    errors = validate_evidence(value)
    assert any(reason in error for error in errors), errors


@pytest.mark.parametrize("runtime", ["javascript", "java", "python"])
@pytest.mark.parametrize("requirement", [3, 4])
@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("native_end", [29, 30, 40])
def test_native_interval_contains_the_asserted_platform_timeout(
    runtime: str, requirement: int, phase: Literal["observations", "final"], native_end: int
) -> None:
    value = deadline_evidence(runtime=runtime) if requirement == 3 else recovery_evidence(runtime)
    # Admission at 10 plus timeoutMs=20 establishes the timeout at 30.
    # Closure, probes and task exit may still follow native completion.
    value["attempts"][0]["endedAtMs"] = native_end
    errors = validate_evidence(value, phase=phase)
    if native_end < 30:
        assert any("platform timeout follows native invocation completion" in error for error in errors), errors
    else:
        assert errors == [], errors


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ("failed", "ASSERTION: recovered capacity did not complete the selected recovery request"),
        ("early", "ASSERTION: recovery preceded the selected victim's timeout"),
        ("missing-policy", "INCOMPLETE: profile.recoveryPolicy must explicitly select reuse-worker or replace-worker"),
    ],
)
def test_recovery_requires_success_after_timeout_under_declared_policy(change: str, reason: str) -> None:
    value = recovery_evidence()
    recovery = value["attempts"][-1]
    if change == "failed":
        recovery["outcome"] = "FAILED"
    elif change == "early":
        recovery["startedAtMs"] = 29
    else:
        del value["profile"]["recoveryPolicy"]
    assert reason in validate_evidence(value)


@pytest.mark.parametrize("field", ["environmentId", "workerId", "processId"])
def test_recovery_cannot_use_unrelated_capacity(field: str) -> None:
    value = recovery_evidence()
    value["attempts"][-1][field] = "replacement"
    assert any(error.startswith("PLACEMENT:") for error in validate_evidence(value))


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("peer_role", ["peer", "peer2", "peer3"])
def test_python_recovery_cannot_replace_victim_with_an_occupied_peer_process(
    phase: Literal["observations", "final"], peer_role: str
) -> None:
    value = recovery_evidence("python", native_concurrency=4)
    value["profile"]["recoveryPolicy"] = "replace-worker"
    selected = {item["role"]: item for item in value["attempts"]}
    recovery = selected["recovery"]
    recovery.update(workerId="replacement-worker", processId="replacement-process", replacesWorkerId="process-1")
    assert validate_evidence(value, phase=phase) == []

    peer = selected[peer_role]
    recovery.update(workerId=peer["workerId"], processId=peer["processId"])
    assert any("occupied Python peer process" in error for error in validate_evidence(value, phase=phase))


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("runtime", ["python", "javascript", "java"])
@pytest.mark.parametrize("policy", ["reuse-worker", "replace-worker"])
@pytest.mark.parametrize("victim_end", [39, 40, 50])
def test_python_recovery_waits_for_same_process_native_completion(
    phase: Literal["observations", "final"], runtime: str, policy: str, victim_end: int
) -> None:
    value = recovery_evidence(runtime)
    victim, recovery = value["attempts"][0], value["attempts"][-1]
    victim["endedAtMs"] = victim_end
    next(item for item in value["events"] if item["kind"] == "wrapper_return")["atMs"] = min(40, victim_end)
    value["profile"]["recoveryPolicy"] = policy
    if policy == "replace-worker":
        # A different worker label cannot make an occupied Python process free.
        recovery.update(workerId="replacement-worker", replacesWorkerId=victim["workerId"])
    errors = validate_evidence(value, phase=phase)
    if runtime == "python" and victim_end > recovery["startedAtMs"]:
        assert any("native invocation completion" in error for error in errors), errors
    else:
        assert errors == [], errors


@pytest.mark.parametrize("phase", ["observations", "final"])
def test_python_recovery_can_replace_a_process_with_an_unfinished_native_invocation(
    phase: Literal["observations", "final"],
) -> None:
    value = recovery_evidence("python")
    victim, recovery = value["attempts"][0], value["attempts"][-1]
    victim["endedAtMs"] = 50
    value["profile"]["recoveryPolicy"] = "replace-worker"
    recovery.update(workerId="replacement-worker", processId="replacement-process", replacesWorkerId=victim["workerId"])
    assert validate_evidence(value, phase=phase) == []


@pytest.mark.parametrize("phase", ["observations", "final"])
def test_python_recovery_uses_native_completion_separately_from_background_task_exit(
    phase: Literal["observations", "final"],
) -> None:
    value = recovery_evidence("python")
    value["budgets"]["cleanupMs"] = 15
    next(item for item in value["events"] if item["kind"] == "task_exit")["atMs"] = 45
    # Native completion at 40 releases the process for recovery at 40, even
    # though a victim-owned background task exits later within its budget.
    assert validate_evidence(value, phase=phase) == []


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("runtime", ["javascript", "java", "python"])
@pytest.mark.parametrize("existing_role", ["victim", "peer", "peer2", "peer3"])
def test_recovery_execution_is_independent_of_every_selected_peer(
    phase: Literal["observations", "final"], runtime: str, existing_role: str
) -> None:
    value = recovery_evidence(runtime, native_concurrency=4)
    # Reusing the victim process remains valid, including shared JavaScript/JVM
    # processes, provided the recovery has its own durable execution identity.
    assert validate_evidence(value, phase=phase) == []
    selected = {item["role"]: item for item in value["attempts"]}
    recovery = selected["recovery"]
    recovery["executionArn"] = selected[existing_role]["executionArn"]
    for item in value["events"]:
        if item["requestId"] == recovery["requestId"]:
            item["executionArn"] = recovery["executionArn"]
    refresh_histories(value)
    record_owned_resources(value)
    assert any("independent request" in error for error in validate_evidence(value, phase=phase))


def test_recovery_requires_healthy_peer_progress_and_occupied_capacity() -> None:
    value = recovery_evidence()
    value["attempts"][1]["endedAtMs"] = 43
    assert any("capacity" in error for error in validate_evidence(value))
    value = recovery_evidence()
    value["events"] = [item for item in value["events"] if item["kind"] != "peer_progress"]
    assert any("healthy peer" in error for error in validate_evidence(value))


def test_recovery_must_fit_the_declared_recovery_budget() -> None:
    # Ordering alone accepted any duration: the observed 15 ms recovery passed
    # even when the declared and retained recoveryMs were tighter.
    value = recovery_evidence()
    assert not validate_evidence(value)
    value["budgets"]["recoveryMs"] = 14
    assert any("recoveryMs" in error for error in validate_evidence(value))


def test_extra_native_slots_cannot_substitute_for_failed_worker() -> None:
    value = recovery_evidence()
    value["profile"]["nativeConcurrency"] = 8
    assert any("every other native" in error for error in validate_evidence(value))


@pytest.mark.parametrize("mode", ["normal", "emergency"])
def test_cleanup_release_cannot_supply_the_recovery_under_test(mode: str) -> None:
    value = recovery_evidence()
    value["retirement"]["faults"][0].update(releasedAtMs=34, mode=mode)
    assert any("retroactively" in error for error in validate_evidence(value))


def test_early_release_is_detected_before_retirement_too() -> None:
    value = recovery_evidence()
    value["faultsCreated"][0]["releasedAtMs"] = 34
    assert any("fault release" in error for error in validate_evidence(value, phase="observations"))


def test_observation_phase_does_not_require_release_to_prove_cleanup() -> None:
    value = deadline_evidence()
    del value["retirement"]
    assert validate_evidence(value, phase="observations") == []
    assert validate_evidence(value)


def test_replay_checks_stored_failures_and_completed_side_effects() -> None:
    value = replay_evidence()
    value["replay"]["after"][1]["error"]["type"] = "DifferentError"
    assert any("failure" in error for error in validate_evidence(value))
    value = replay_evidence()
    value["replay"]["operationExecutions"].append({"requestId": "request-resume", "operationId": "done"})
    assert any("repeated completed" in error for error in validate_evidence(value))
    value = replay_evidence()
    value["replay"]["sideEffects"][0]["count"] = 2
    assert any("side-effect count" in error for error in validate_evidence(value))


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("field", ["requestId", "operationId"])
@pytest.mark.parametrize("invalid", [None, False, 1, "", " ", [], {}])
def test_replay_execution_identities_must_be_nonempty_strings(
    phase: Literal["observations", "final"], field: str, invalid: Any
) -> None:
    value = replay_evidence()
    execution = {"requestId": "request-resume", "operationId": "unfinished"}
    execution[field] = invalid
    value["replay"]["operationExecutions"].append(execution)
    errors = validate_evidence(value, phase=phase)
    assert any(f"replay.operationExecutions.{field} must be a non-empty string" in error for error in errors), errors


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("field", ["requestId", "operationId"])
def test_replay_execution_cannot_omit_either_identity(phase: Literal["observations", "final"], field: str) -> None:
    value = replay_evidence()
    execution = {"requestId": "request-resume", "operationId": "unfinished"}
    del execution[field]
    value["replay"]["operationExecutions"].append(execution)
    errors = validate_evidence(value, phase=phase)
    assert any(f"replay.operationExecutions.{field} must be a non-empty string" in error for error in errors), errors


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("operation_id", ["done", "failed", "unfinished"])
def test_replay_execution_distinguishes_completed_from_uncheckpointed_operations(
    phase: Literal["observations", "final"], operation_id: str
) -> None:
    value = replay_evidence()
    value["replay"]["operationExecutions"].append({"requestId": "request-resume", "operationId": operation_id})
    errors = validate_evidence(value, phase=phase)
    if operation_id == "unfinished":
        # A real body entry can precede its checkpoint, so its nonempty ID need
        # not be present in the completed snapshots or final service history.
        assert errors == [], errors
    else:
        assert any("repeated completed user code" in error for error in errors), errors


def test_no_later_invocation_may_repeat_completed_user_code() -> None:
    # A side-effect count cannot catch this when the body has no external
    # effect, so every request after the victim on the same execution is
    # checked, not only the selected first resume.
    value = replay_evidence()
    victim = value["attempts"][0]
    second = attempt("resume", start=90, end=100)
    second.update(requestId="request-resume-2", executionArn=victim["executionArn"], attempt=2)
    value["attempts"].append(second)
    record_owned_resources(value)
    assert not validate_evidence(value)
    value["replay"]["operationExecutions"].append({"requestId": second["requestId"], "operationId": "done"})
    assert any("repeated completed" in error for error in validate_evidence(value))


def test_resume_must_have_original_execution_and_real_pending() -> None:
    value = replay_evidence()
    value["attempts"][-1]["executionArn"] = "replacement-execution"
    refresh_histories(value)
    assert any("original suspended" in error for error in validate_evidence(value))
    value = replay_evidence()
    value["events"] = [item for item in value["events"] if item["kind"] != "pending"]
    assert any("pending" in error for error in validate_evidence(value))


@pytest.mark.parametrize("change", ["wrong-request", "outside-warm-run", "after-task-exit"])
def test_warm_reuse_orders_late_activity_within_the_selected_invocation(change: str) -> None:
    value = warm_evidence()
    victim, peer, _ = value["attempts"]
    late_activity = next(item for item in value["events"] if item["kind"] == "late_activity")
    reason = "ASSERTION: late activity was not observed during the selected warm invocation"
    if change == "wrong-request":
        peer["endedAtMs"] = 70
        late_activity["observedDuringRequestId"] = peer["requestId"]
    elif change == "outside-warm-run":
        late_activity["atMs"] = 49
        reason = "ASSERTION: event lies outside its observedDuringRequestId interval"
    else:
        task_exit = next(
            item for item in value["events"] if item["kind"] == "task_exit" and item["requestId"] == victim["requestId"]
        )
        task_exit["atMs"] = 59
        reason = "ASSERTION: late activity continued after the recorded task exit"
    assert reason in validate_evidence(value)


def test_warm_reuse_requires_the_original_process() -> None:
    value = warm_evidence()
    value["attempts"][-1]["processId"] = "new-process"
    assert any("original process" in error for error in validate_evidence(value))


@pytest.mark.parametrize("runtime", ["javascript", "java", "python"])
@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("native_end", [49, 50, 70])
def test_warm_reuse_requires_native_completion_before_warm_admission(
    runtime: str, phase: Literal["observations", "final"], native_end: int
) -> None:
    value = warm_evidence(runtime)
    # Wrapper return at 40 precedes warm admission at 50 in every case.
    # Late activity at 60 and task exit at 65 may outlive native completion.
    value["attempts"][0]["endedAtMs"] = native_end
    errors = validate_evidence(value, phase=phase)
    if native_end > 50:
        assert any("warm invocation was not sequential reuse" in error for error in errors), errors
    else:
        assert errors == [], errors


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize(("exit_sequence", "late_sequence"), [(10, 11), (11.0, 10)])
def test_warm_reuse_orders_late_activity_and_task_exit_at_the_same_timestamp(
    phase: Literal["observations", "final"], exit_sequence: int | float, late_sequence: int
) -> None:
    value = warm_evidence()
    exited = next(item for item in value["events"] if item["kind"] == "task_exit")
    late = next(item for item in value["events"] if item["kind"] == "late_activity")
    exited.update(atMs=60, sequence=exit_sequence)
    late["sequence"] = late_sequence

    errors = validate_evidence(value, phase=phase)

    if exit_sequence == 10:
        assert errors == ["ASSERTION: late activity continued after the recorded task exit"]
    else:
        assert errors == [], errors


@pytest.mark.parametrize(
    ("kind", "fields", "error"),
    [
        ("late_activity", {}, "same-timestamp late_activity and task_exit need distinct sequences"),
        ("task_exit", {}, "same-timestamp late_activity and task_exit need distinct sequences"),
        ("task_exit", {"sequence": 10.0}, "same-timestamp late_activity and task_exit need distinct sequences"),
        ("late_activity", {"sequence": True}, "event.sequence must be a non-negative integer"),
        ("task_exit", {"sequence": None}, "event.sequence must be a non-negative integer"),
    ],
)
def test_warm_reuse_requires_unambiguous_sequences_for_same_timestamp_events(
    kind: str, fields: dict[str, Any], error: str
) -> None:
    value = warm_evidence()
    for event in value["events"]:
        if event["kind"] in ("late_activity", "task_exit"):
            event.update(atMs=60, **(fields if event["kind"] == kind else {"sequence": 10}))

    assert validate_evidence(value) == [f"INCOMPLETE: {error}"]


@pytest.mark.parametrize("exit_at", [59, 61])
@pytest.mark.parametrize("with_sequences", [False, True], ids=["no-sequences", "conflicting-sequences"])
def test_warm_reuse_prefers_distinct_timestamps(exit_at: int, with_sequences: bool) -> None:
    value = warm_evidence()
    exited = next(item for item in value["events"] if item["kind"] == "task_exit")
    late = next(item for item in value["events"] if item["kind"] == "late_activity")
    exited["atMs"] = exit_at
    if with_sequences:
        exited["sequence"], late["sequence"] = (11, 10) if exit_at == 59 else (10, 11)

    errors = validate_evidence(value)

    if exit_at == 59:
        assert errors == ["ASSERTION: late activity continued after the recorded task exit"]
    else:
        assert errors == []


@pytest.mark.parametrize("kind", ["late_activity", "task_exit"])
def test_warm_reuse_reports_missing_events_without_a_tie_error(kind: str) -> None:
    value = warm_evidence()
    value["events"] = [item for item in value["events"] if item["kind"] != kind]

    assert validate_evidence(value) == [f"INCOMPLETE: request request-victim needs exactly one {kind} event"]


def test_case_owned_cleanup_and_retirement_completeness() -> None:
    value = evidence()
    value["resourcesCreated"] = [{"id": "callback-1", "ownerCaseId": "case-1"}]
    assert any("every owned" in error for error in validate_evidence(value))
    value["retirement"]["resources"] = [{"id": "callback-1", "ownerCaseId": "other-case", "settledAtMs": 210}]
    assert any("another case" in error for error in validate_evidence(value))


@pytest.mark.parametrize("invalid", [None, [], 1, "1", True, float("nan"), float("inf"), {}])
def test_malformed_schema_values_produce_findings_not_exceptions(invalid: Any) -> None:
    for key in ("schemaVersion", "runtime", "artifact", "attempts", "events", "collection"):
        if key == "schemaVersion" and type(invalid) is int and invalid == 1:
            continue
        value = evidence()
        value[key] = invalid
        assert validate_evidence(value)


@pytest.mark.parametrize(
    "change",
    [
        "duplicate-role",
        "wrong-execution",
        "outside-window",
        "duplicate-request",
        "duplicate-history",
        "wrong-history-owner",
    ],
)
def test_shared_evidence_rejects_conflicting_identities_and_observation_windows(change: str) -> None:
    value = evidence()
    victim, peer = value["attempts"]
    if change == "duplicate-role":
        value["selectedRequests"]["peer"] = victim["requestId"]
        reason = "PLACEMENT: distinct selected roles must bind distinct original requests"
    elif change == "wrong-execution":
        value["events"][0]["executionArn"] = peer["executionArn"]
        reason = "ASSERTION: event is attributed to a different execution"
    elif change == "outside-window":
        value["events"][0]["atMs"] = value["observationEndedAtMs"] + 1
        reason = "INCOMPLETE: event lies outside its observation window"
    elif change == "duplicate-request":
        value["attempts"].append(deepcopy(victim))
        reason = f"INCOMPLETE: duplicate requestId {victim['requestId']}"
    elif change == "duplicate-history":
        value["histories"].append(deepcopy(value["histories"][0]))
        reason = f"INCOMPLETE: duplicate service history {victim['executionArn']}"
    else:
        value["histories"][0]["ownerCaseId"] = "another-case"
        reason = f"ASSERTION: history {victim['executionArn']} belongs to a different case"
    assert reason in validate_evidence(value)


def test_misattributed_service_history_is_not_a_successful_output() -> None:
    value = evidence()
    value["histories"][0]["result"] = {"marker": "peer"}
    assert any("fixture expectation" in error for error in validate_evidence(value))


@pytest.mark.parametrize("missing", ["retry", "operationExecutions", "sideEffects"])
def test_replay_cannot_omit_retry_or_execution_observations(missing: str) -> None:
    value = replay_evidence()
    del value["replay"][missing]
    assert validate_evidence(value)


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ("missing-failure", "replay must preserve both a completed success and a completed failure"),
        ("one-attempt", "retry requires at least two observed attempts"),
        ("duplicate-attempt", "retry requires distinct observed attempts"),
        ("wrong-outcome-order", "retry must demonstrate a failed attempt followed by success"),
        ("missing-body", "retry attempt lacks observed user-code execution"),
        ("already-completed", "retry must not repeat an already completed operation"),
        ("missing-count", "side-effect counts must cover stored successes, failures, and retries"),
    ],
)
def test_replay_requires_completed_outcomes_and_independent_retry_evidence(change: str, reason: str) -> None:
    value = replay_evidence()
    replay = value["replay"]
    retries = replay["retry"]["attempts"]
    if change == "missing-failure":
        replay["before"].pop()
    elif change == "one-attempt":
        retries.pop()
    elif change == "duplicate-attempt":
        retries[1]["attemptId"] = retries[0]["attemptId"]
    elif change == "wrong-outcome-order":
        retries[0]["outcome"], retries[1]["outcome"] = retries[1]["outcome"], retries[0]["outcome"]
    elif change == "missing-body":
        replay["operationExecutions"] = [
            item for item in replay["operationExecutions"] if item.get("attemptId") != retries[0]["attemptId"]
        ]
    elif change == "already-completed":
        retried = next(item for item in replay["after"] if item["id"] == replay["retry"]["operationId"])
        replay["before"].append(deepcopy(retried))
    else:
        replay["sideEffects"].pop()
    assert any(error.endswith(reason) for error in validate_evidence(value))


def test_replay_snapshot_is_bound_to_service_history() -> None:
    value = replay_evidence()
    value["replay"]["after"][-1]["result"] = "invented success"
    assert any("collected service history" in error for error in validate_evidence(value))


def test_replay_cannot_retype_a_stored_number_as_a_boolean() -> None:
    # The stored result is 1 and Python container equality accepts True for it,
    # so this corruption used to survive the completed-operation comparison.
    value = replay_evidence()
    assert not validate_evidence(value)
    value["replay"]["before"][0]["result"]["value"] = True
    assert any("operation identity" in error for error in validate_evidence(value))


@pytest.mark.parametrize(
    ("left", "right"),
    [
        (1, True),
        (True, 1),
        (0, False),
        (False, 0),
        ({"value": [1]}, {"value": [True]}),
        ({"value": [True]}, {"value": [1]}),
        ([{"nested": False}], [{"nested": 0}]),
    ],
)
def test_json_comparison_keeps_booleans_distinct_from_numbers(left: Any, right: Any) -> None:
    assert left == right, "the fixture must rely on Python's bool/number equality"
    assert not same_json(left, right)


@pytest.mark.parametrize("value", [1, 1.5, "text", None, True, False, {"a": [1, {"b": None}, "c"]}, []])
def test_json_comparison_accepts_equal_values(value: Any) -> None:
    assert same_json(value, deepcopy(value))


def test_retirement_cannot_omit_suspended_executions_or_invocations() -> None:
    value = evidence()
    value["resourcesCreated"] = []
    value["retirement"]["resources"] = []
    assert any("every launched" in error for error in validate_evidence(value))
    value = evidence()
    execution = next(item for item in value["retirement"]["resources"] if item["kind"] == "execution")
    execution["outcome"] = "PENDING"
    assert any("still resume" in error for error in validate_evidence(value))


@pytest.mark.parametrize("validator", [validate_evidence, validate_common_evidence, validate_retirement_evidence])
@pytest.mark.parametrize("kind", ["invocation", "execution"])
@pytest.mark.parametrize("mismatch", ["extra-inventory", "missing-inventory"])
def test_retirement_inventory_matches_observations_in_both_directions(validator: Any, kind: str, mismatch: str) -> None:
    value = evidence()
    complete_retirement_events(value)
    assert validator(value) == []
    if mismatch == "extra-inventory":
        resource = {"id": f"unobserved-{kind}", "kind": kind, "ownerCaseId": value["caseId"]}
        value["resourcesCreated"].append(resource)
        value["retirement"]["resources"].append(
            {
                **resource,
                "settledAtMs": 210,
                "evidenceUri": "s3://evidence/unobserved-resource.json",
                "outcome": "QUIESCENT" if kind == "invocation" else "SUCCEEDED",
            }
        )
    else:
        resource = next(item for item in value["resourcesCreated"] if item["kind"] == kind)
        for records in (value["resourcesCreated"], value["retirement"]["resources"]):
            records[:] = [item for item in records if item["id"] != resource["id"]]

    assert validate_evidence(value, phase="observations") == []
    assert validate_common_evidence(value, phase="observations") == []
    assert any("resourcesCreated" in error for error in validator(value))


@pytest.mark.parametrize("validator", [validate_evidence, validate_common_evidence, validate_retirement_evidence])
@pytest.mark.parametrize(
    ("kind", "outcome", "unfinished", "reason"),
    [
        ("callback", "SUCCEEDED", "PENDING", "callback completion is not established"),
        ("control", "DELETED", "DELETE_REQUESTED", "control object deletion is not established"),
    ],
)
def test_retirement_inventory_keeps_other_owned_resource_kinds(
    validator: Any, kind: str, outcome: str, unfinished: str, reason: str
) -> None:
    value = evidence()
    complete_retirement_events(value)
    resource = {"id": f"owned-{kind}", "kind": kind, "ownerCaseId": value["caseId"]}
    value["resourcesCreated"].append(resource)
    value["retirement"]["resources"].append(
        {**resource, "settledAtMs": 210, "evidenceUri": "s3://evidence/owned-resource.json", "outcome": outcome}
    )
    assert validator(value) == []
    value["retirement"]["resources"][-1]["outcome"] = unfinished
    assert f"INCOMPLETE: {reason}" in validator(value)


@pytest.mark.parametrize("validator", [validate_evidence, validate_common_evidence, validate_retirement_evidence])
@pytest.mark.parametrize("shared_execution", [False, True])
def test_retirement_inventory_accepts_every_observed_request_and_execution(
    validator: Any, shared_execution: bool
) -> None:
    value = evidence(2)
    extra = attempt("extra", start=60, end=80)
    if shared_execution:
        extra["executionArn"] = value["attempts"][0]["executionArn"]
    value["attempts"].append(extra)
    refresh_histories(value)
    record_owned_resources(value)
    complete_retirement_events(value)
    assert validator(value) == []


@pytest.mark.parametrize(
    "diagnostics",
    [
        {"handlerOwnerId": "another-handler", "threadId": "same-thread"},
        {"handlerOwnerId": "same-handler", "threadId": "different-thread"},
        {"handlerOwnerId": None, "threadId": {"runtime": "opaque-diagnostic"}},
    ],
)
def test_java_optional_owner_diagnostics_do_not_determine_shared_conformance(diagnostics: dict[str, Any]) -> None:
    value = evidence(runtime="java")
    value["attempts"][0].update(handlerOwnerId="same-handler", threadId="same-thread")
    value["attempts"][1].update(diagnostics)
    assert validate_evidence(value) == []


def test_explicit_worker_replacement_policy_stays_bound_to_original_capacity() -> None:
    value = recovery_evidence("python")
    value["profile"]["recoveryPolicy"] = "replace-worker"
    recovery = value["attempts"][-1]
    recovery.update(workerId="replacement-worker", processId="replacement-process", replacesWorkerId="process-1")
    assert validate_evidence(value) == []
    recovery["replacesWorkerId"] = "unrelated-worker"
    assert any("original failed worker" in error for error in validate_evidence(value))


@pytest.mark.parametrize("identity", ["environmentId", "processId", "workerId"])
def test_java_still_requires_observed_runtime_placement(identity: str) -> None:
    value = evidence(runtime="java")
    del value["attempts"][1][identity]
    assert any(identity in error for error in validate_evidence(value))


def test_java_shared_conformance_does_not_require_owner_or_thread_diagnostics() -> None:
    value = evidence(runtime="java")
    assert all("handlerOwnerId" not in item and "threadId" not in item for item in value["attempts"])
    assert validate_evidence(value) == []
    value["attempts"][1]["environmentId"] = "replacement-environment"
    assert any(error.startswith("PLACEMENT:") for error in validate_evidence(value))


def test_disposal_task_exit_cannot_precede_the_root_boundary() -> None:
    value = evidence(2)
    next(item for item in value["events"] if item["kind"] == "task_exit")["atMs"] = 34
    assert any("preceded root completion" in error for error in validate_evidence(value))


def test_final_retirement_is_checked_even_when_observation_evidence_is_incomplete() -> None:
    value = evidence()
    del value["collection"]["watermarks"]["sdkRequests"]
    observations = validate_evidence(value, phase="observations")
    assert observations
    assert validate_evidence(value) == observations
    del value["retirement"]
    final = validate_evidence(value)
    assert all(error in final for error in observations)
    assert any("retirement" in error and error not in observations for error in final)


def test_final_retirement_rejects_live_execution_despite_prior_observation_error() -> None:
    value = evidence()
    del value["collection"]["watermarks"]["sdkRequests"]
    execution = next(item for item in value["retirement"]["resources"] if item["kind"] == "execution")
    execution["outcome"] = "PENDING"
    assert any("still resume" in error for error in validate_evidence(value))


@pytest.mark.parametrize("status", [False, True, None, {}, [], 1, "", "INVENTED"])
def test_malformed_or_unknown_history_status_cannot_match_its_way_to_a_pass(status: Any) -> None:
    value = evidence()
    for history in value["histories"]:
        history["status"] = status
        value["expectedHistories"][history["executionArn"]]["status"] = status
    assert any("recognized execution status" in error for error in validate_evidence(value))


def test_unnamed_context_preserves_actual_identity_without_inventing_a_name() -> None:
    value = evidence()
    operation = {"id": "actual-context", "type": "CONTEXT", "name": None, "parentId": None, "status": "SUCCEEDED"}
    value["histories"][0]["operations"].append(operation)
    assert validate_evidence(value) == []
    assert operation == {
        "id": "actual-context",
        "type": "CONTEXT",
        "name": None,
        "parentId": None,
        "status": "SUCCEEDED",
    }


@pytest.mark.parametrize("name", ["", False, {}, []])
def test_unnamed_operation_support_does_not_accept_invalid_names(name: Any) -> None:
    value = evidence()
    value["histories"][0]["operations"] = [
        {"id": "context", "type": "CONTEXT", "name": name, "parentId": None, "status": "SUCCEEDED"}
    ]
    assert any("operation.name" in error for error in validate_evidence(value))


def test_operation_name_and_parent_still_require_explicit_fields() -> None:
    for missing in ("name", "parentId"):
        value = evidence()
        operation = {"id": "context", "type": "CONTEXT", "name": None, "parentId": None, "status": "SUCCEEDED"}
        del operation[missing]
        value["histories"][0]["operations"] = [operation]
        assert any(missing in error for error in validate_evidence(value))


@pytest.mark.parametrize("status", ["RUNNING", "FAILED", "TIMED_OUT", "STOPPED"])
def test_held_execution_has_an_independent_explicit_status_allowance(status: str) -> None:
    value = evidence(2)
    history = value["histories"][0]
    history["status"] = status
    history.pop("result")
    value["expectedHistories"][history["executionArn"]] = {
        "allowedStatuses": ["RUNNING", "FAILED", "TIMED_OUT", "STOPPED"]
    }
    if status != "RUNNING":
        history["error"] = {"type": "expected fixture termination"}
        # Retirement of an already terminal execution keeps its outcome.
        retired = next(item for item in value["retirement"]["resources"] if item["id"] == history["executionArn"])
        retired["outcome"] = status
    assert validate_common_evidence(value) == []


@pytest.mark.parametrize("allowed", [None, "RUNNING", [], ["RUNNING", "RUNNING"], ["UNKNOWN"], [False], [{}]])
def test_history_status_allowance_rejects_malformed_or_unknown_choices(allowed: Any) -> None:
    value = evidence()
    arn = value["histories"][0]["executionArn"]
    value["expectedHistories"][arn] = {"allowedStatuses": allowed}
    assert any("allowedStatuses" in error for error in validate_evidence(value))


@pytest.mark.parametrize(
    "expectation",
    [{}, {"status": "SUCCEEDED", "allowedStatuses": ["RUNNING", "SUCCEEDED"], "result": {"marker": "victim"}}],
)
def test_history_expectations_require_one_unambiguous_status_contract(expectation: dict[str, Any]) -> None:
    value = evidence()
    arn = value["histories"][0]["executionArn"]
    value["expectedHistories"][arn] = expectation
    assert any("exactly one" in error for error in validate_evidence(value))


@pytest.mark.parametrize("expectation", [{"status": "SUCCEEDED"}, {"allowedStatuses": ["FAILED", "STOPPED"]}])
def test_fixed_terminal_expectations_still_require_result_or_error(expectation: dict[str, Any]) -> None:
    value = evidence()
    arn = value["histories"][0]["executionArn"]
    value["expectedHistories"][arn] = expectation
    assert any("result/error" in error for error in validate_evidence(value))


def test_allowed_statuses_do_not_accept_success_or_ignore_declared_output() -> None:
    value = evidence(2)
    history = value["histories"][0]
    value["expectedHistories"][history["executionArn"]] = {"allowedStatuses": ["RUNNING", "FAILED"], "result": None}
    assert any("outside the declared" in error for error in validate_evidence(value))
    history["status"] = "RUNNING"
    assert any("fixture expectation" in error for error in validate_evidence(value))
    history["result"] = None
    assert validate_evidence(value) == []


def test_running_history_allowance_does_not_establish_execution_retirement() -> None:
    value = evidence()
    history = value["histories"][0]
    history["status"] = "RUNNING"
    value["expectedHistories"][history["executionArn"]] = {"allowedStatuses": ["RUNNING"]}
    retired = next(item for item in value["retirement"]["resources"] if item["kind"] == "execution")
    retired["outcome"] = "RUNNING"
    assert any("still resume" in error for error in validate_evidence(value))


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("failure", ["collectionErrors", "observation_error", "control_error"])
def test_complete_watermarks_do_not_override_source_failures(phase: Any, failure: str) -> None:
    value = evidence(2)
    if failure == "collectionErrors":
        value[failure] = ["SDK request source collection failed"]
    else:
        value["events"].append(event(value["attempts"][0], failure, 38))
    for validate in (validate_evidence, validate_common_evidence):
        errors = validate(value, phase=phase)
        assert any(failure in error for error in errors), errors


@pytest.mark.parametrize("invalid", [None, False, 0, "", {}, [""]])
def test_malformed_collection_errors_cannot_hide_source_failures(invalid: Any) -> None:
    value = evidence(2)
    value["collectionErrors"] = invalid
    assert any("collectionErrors" in error for error in validate_evidence(value))


def test_empty_optional_collection_errors_preserves_complete_evidence() -> None:
    value = evidence(2)
    value["collectionErrors"] = []
    assert validate_evidence(value) == []


@pytest.mark.parametrize("phase", ["observations", "final"])
def test_replay_preserves_unnamed_context_and_its_completed_child(phase: Any) -> None:
    value = unnamed_context_replay_evidence()
    assert validate_evidence(value, phase=phase) == []
    before = value["replay"]["before"]
    assert before[0]["name"] is None
    assert before[1]["parentId"] == before[0]["id"]


@pytest.mark.parametrize("invalid", ["", " ", False, {}, []])
def test_replay_unnamed_context_does_not_accept_invalid_names(invalid: Any) -> None:
    value = unnamed_context_replay_evidence()
    value["replay"]["before"][0]["name"] = invalid
    assert any("replay.before.name" in error for error in validate_evidence(value))


def test_replay_unnamed_context_requires_explicit_null_name() -> None:
    value = unnamed_context_replay_evidence()
    del value["replay"]["before"][0]["name"]
    assert any("replay.before.name" in error for error in validate_evidence(value))


@pytest.mark.parametrize("operation_id", ["context", "done"])
def test_unnamed_context_replay_still_rejects_completed_user_code(operation_id: str) -> None:
    value = unnamed_context_replay_evidence()
    value["replay"]["operationExecutions"].append({"requestId": "request-resume", "operationId": operation_id})
    assert any("repeated completed" in error for error in validate_evidence(value))


@pytest.mark.parametrize("parent", [False, 1, ["parent"], {"id": "parent"}])
def test_history_operation_parent_must_be_a_string_or_null(parent: Any) -> None:
    # The wire schema allows a string or null; presence alone accepted a
    # malformed parent that the replay comparison then matched consistently.
    value = evidence()
    value["histories"][0]["operations"] = [
        {"id": "op-1", "type": "STEP", "status": "SUCCEEDED", "name": "step", "parentId": parent}
    ]
    assert any("parentId" in error for error in validate_evidence(value))
    value["histories"][0]["operations"][0]["parentId"] = "parent-op"
    assert not any("parentId" in error for error in validate_evidence(value))


def test_retry_must_run_on_the_suspended_execution() -> None:
    value = replay_evidence()
    peer = next(item for item in value["attempts"] if item["role"] == "peer")
    assert peer["executionArn"] != value["attempts"][0]["executionArn"]
    for item in (*value["replay"]["retry"]["attempts"], *value["replay"]["operationExecutions"]):
        if item.get("operationId", "retry") == "retry":
            item["requestId"] = peer["requestId"]
    assert any("suspended execution" in error for error in validate_evidence(value))


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("native_end", [40, 70])
def test_retry_body_order_is_not_inferred_from_native_invocation_intervals(phase: Any, native_end: int) -> None:
    # User-code observations retain their owner after native completion. A
    # later platform observation can widen the interval without changing them.
    value = replay_evidence()
    victim, resume = value["attempts"][0], value["attempts"][-1]
    victim["endedAtMs"] = native_end
    retries = value["replay"]["retry"]["attempts"]
    retries[0]["requestId"], retries[1]["requestId"] = resume["requestId"], victim["requestId"]
    for retry, owner, at_ms in zip(retries, (resume, victim), (55, 65), strict=True):
        entry = next(
            item for item in value["replay"]["operationExecutions"] if item.get("attemptId") == retry["attemptId"]
        )
        entry["requestId"] = owner["requestId"]
        value["events"].append(event(owner, "body", at_ms, operationId="retry", attemptId=retry["attemptId"]))
    value["events"][-1]["observedDuringRequestId"] = resume["requestId"]

    assert validate_evidence(value, phase=phase) == []


def test_retried_operation_must_succeed_in_the_collected_history() -> None:
    value = replay_evidence()
    victim = value["attempts"][0]
    value["replay"]["after"] = [item for item in value["replay"]["after"] if item["id"] != "retry"]
    history = next(item for item in value["histories"] if item["executionArn"] == victim["executionArn"])
    history["operations"] = deepcopy(value["replay"]["after"])
    assert any("retried operation" in error for error in validate_evidence(value))


@pytest.mark.parametrize(("settled", "rejected"), [(0, True), (45, True), (46, False)])
def test_invocation_cannot_settle_before_its_recorded_activity(settled: int, rejected: bool) -> None:
    # The victim's last activity is its admission probe at 46.
    value = evidence()
    victim = value["attempts"][0]
    invocation = next(item for item in value["retirement"]["resources"] if item["id"] == victim["requestId"])
    invocation["settledAtMs"] = settled
    errors = validate_evidence(value)
    assert any("before its recorded activity" in error for error in errors) is rejected, errors


@pytest.mark.parametrize("validator", [validate_evidence, validate_common_evidence])
@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("settled", [0, 5, 6])
def test_execution_settlement_respects_its_known_service_start(validator: Any, phase: Any, settled: int) -> None:
    value = evidence()
    victim = value["attempts"][0]
    value["histories"][0]["startedAtMs"] = 5
    execution = next(item for item in value["retirement"]["resources"] if item["id"] == victim["executionArn"])
    execution["settledAtMs"] = settled

    errors = validator(value, phase=phase)

    if phase == "final" and settled < 5:
        assert any("execution settled before its service start" in error for error in errors), errors
    else:
        assert errors == [], errors


def test_independent_retirement_audit_rejects_settlement_before_service_start() -> None:
    value = evidence()
    value["histories"][0]["startedAtMs"] = 5
    execution = next(
        item for item in value["retirement"]["resources"] if item["id"] == value["histories"][0]["executionArn"]
    )
    execution["settledAtMs"] = 0
    assert any("execution settled before its service start" in error for error in validate_retirement_evidence(value))


def test_missing_service_start_is_not_inferred_from_a_later_native_admission() -> None:
    # A failed-case audit can include delayed native requests for an execution
    # that already became terminal. Native admission is not service birth.
    value = failure_evidence()
    execution = next(
        item for item in value["retirement"]["resources"] if item["id"] == value["histories"][0]["executionArn"]
    )
    execution["settledAtMs"] = 0
    assert "startedAtMs" not in value["histories"][0]
    assert validate_common_evidence(value) == []


@pytest.mark.parametrize(("released", "rejected"), [(50, False), (40, True)])
def test_fault_release_is_ordered_against_the_selected_cleanup_and_recovery(released: int, rejected: bool) -> None:
    # Victim exit at 35 and recovery at 45 are the asserted boundaries. A
    # healthy peer that stays occupied until 100 must not move them.
    value = recovery_evidence()
    peer = next(item for item in value["attempts"] if item["role"] == "peer")
    value["events"].append(event(peer, "task_exit", 100))
    value["faultsCreated"][0]["releasedAtMs"] = released
    value["retirement"]["faults"][0]["releasedAtMs"] = released
    errors = validate_evidence(value)
    assert any("retroactively" in error or "fault release cannot satisfy" in error for error in errors) is rejected, (
        errors
    )


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("requirement", [3, 4])
@pytest.mark.parametrize("released", [None, False, True, "34", -1, float("inf"), float("nan"), [], {}])
def test_observed_fault_release_requires_a_finite_nonnegative_timestamp(
    phase: Any, requirement: int, released: Any
) -> None:
    value = deadline_evidence() if requirement == 3 else recovery_evidence()
    value["faultsCreated"][0]["releasedAtMs"] = released
    # A later valid retirement release cannot repair malformed observations.
    assert value["retirement"]["faults"][0]["releasedAtMs"] == 210
    for validate in (validate_evidence, validate_common_evidence):
        errors = validate(value, phase=phase)
        assert any("faultsCreated.releasedAtMs must be a finite non-negative number" in error for error in errors), (
            errors
        )


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("requirement", [3, 4])
@pytest.mark.parametrize("released", [50, 50.5])
def test_observed_fault_release_accepts_valid_timestamps(phase: Any, requirement: int, released: int | float) -> None:
    value = deadline_evidence() if requirement == 3 else recovery_evidence()
    value["faultsCreated"][0]["releasedAtMs"] = released
    value["retirement"]["faults"][0]["releasedAtMs"] = released
    for validate in (validate_evidence, validate_common_evidence):
        assert validate(value, phase=phase) == []


def test_expected_null_does_not_match_a_missing_history_field() -> None:
    # dict.get() turns an absent field into None, so an expected null result
    # matched a history that never recorded any output.
    value = evidence()
    victim = value["attempts"][0]
    value["expectedHistories"][victim["executionArn"]]["result"] = None
    history = next(item for item in value["histories"] if item["executionArn"] == victim["executionArn"])
    history.pop("result")
    assert any(
        "result" in error and "differs from the fixture expectation" in error for error in validate_evidence(value)
    )
    history["result"] = None
    assert not any("result" in error and "fixture expectation" in error for error in validate_evidence(value))


@pytest.mark.parametrize(
    ("history_status", "contradicts"), [("FAILED", True), ("SUCCEEDED", False), ("RUNNING", False)]
)
def test_retirement_outcome_matches_an_already_terminal_history(history_status: str, contradicts: bool) -> None:
    # An earlier RUNNING snapshot may become terminal, but a terminal history
    # cannot retire with a different outcome.
    value = evidence()
    victim = value["attempts"][0]
    history = next(item for item in value["histories"] if item["executionArn"] == victim["executionArn"])
    history["status"] = history_status
    errors = validate_evidence(value)
    assert any("terminal service history" in error for error in errors) is contradicts, errors


def test_replay_compares_completed_operations_by_identity_not_position() -> None:
    # A parent context still open at the first resume is absent from
    # replay.before but comes first in the final history, ahead of its
    # completed children. Their checkpoints are unchanged.
    value = replay_evidence()
    victim = value["attempts"][0]
    context = {
        "id": "ctx",
        "name": "nested",
        "type": "CONTEXT",
        "parentId": None,
        "status": "SUCCEEDED",
        "result": None,
    }
    for operation in (*value["replay"]["before"], *value["replay"]["after"]):
        if operation["id"] in {"done", "failed"}:
            operation["parentId"] = "ctx"
    value["replay"]["after"].insert(0, context)
    history = next(item for item in value["histories"] if item["executionArn"] == victim["executionArn"])
    history["operations"] = deepcopy(value["replay"]["after"])
    assert not any("operation identity" in error for error in validate_evidence(value))
    # A completed operation missing from the final history is still rejected.
    value["replay"]["after"] = [item for item in value["replay"]["after"] if item["id"] != "done"]
    history["operations"] = deepcopy(value["replay"]["after"])
    assert any("operation identity" in error for error in validate_evidence(value))


@pytest.mark.parametrize("outcome", [None, "", 7])
def test_every_retired_resource_needs_an_outcome(outcome: Any) -> None:
    value = evidence()
    settled = value["retirement"]["resources"][0]
    if outcome is None:
        del settled["outcome"]
    else:
        settled["outcome"] = outcome
    errors = validate_evidence(value)
    assert any("retirement.resources.outcome" in error for error in errors), errors


def test_final_evidence_rejects_unknown_resource_kind() -> None:
    value = evidence()
    resource = {"id": "timer-1", "kind": "timer", "ownerCaseId": value["caseId"]}
    value["resourcesCreated"].append(resource)
    value["retirement"]["resources"].append(
        {**resource, "settledAtMs": 210, "evidenceUri": "s3://evidence/timer.json", "outcome": "STILL_RUNNING"}
    )
    errors = validate_evidence(value, phase="final")
    assert any("retirement has no completion contract for resource kind timer" in error for error in errors), errors


@pytest.mark.parametrize(
    ("started", "reason"),
    [
        (False, "finite non-negative"),
        (-1, "finite non-negative"),
        (float("inf"), "finite non-negative"),
        (11, "first native admission"),
        (10, None),
        (5, None),
    ],
)
def test_supplied_execution_start_is_valid_and_precedes_first_admission(started: Any, reason: str | None) -> None:
    # The victim's first native admission is at 10.
    value = evidence()
    victim = value["attempts"][0]
    history = next(item for item in value["histories"] if item["executionArn"] == victim["executionArn"])
    history["startedAtMs"] = started
    errors = [
        error for error in validate_evidence(value) if "startedAtMs" in error or "first native admission" in error
    ]
    if reason is None:
        assert errors == [], errors
    else:
        assert any(reason in error for error in errors), errors


@pytest.mark.parametrize("peer_process", ["process-1", "process-2", "process-3"])
def test_python_recovery_occupies_distinct_processes_for_every_peer(peer_process: str) -> None:
    value = recovery_evidence("python")
    value["profile"]["nativeConcurrency"] = 3
    peer = attempt("peer2", start=12, end=100, process=peer_process)
    value["attempts"].append(peer)
    value["selectedRequests"]["peer2"] = peer["requestId"]
    value["events"].extend(
        [event(peer, "peer_progress", 25), event(peer, "peer_progress", 44), event(peer, "wrapper_return", 100)]
    )
    refresh_histories(value)
    record_owned_resources(value)
    errors = validate_evidence(value)
    if peer_process == "process-3":
        assert errors == [], errors
    else:
        assert any("distinct Python processes" in error for error in errors), errors


@pytest.mark.parametrize("records", ["events", "sdkRequests"])
@pytest.mark.parametrize(("observed_at", "rejected"), [(49, True), (50, False), (60, False), (80, False), (81, True)])
def test_observed_during_request_contains_the_recorded_activity(records: str, observed_at: int, rejected: bool) -> None:
    value = warm_evidence()
    victim, warm = value["attempts"][0], value["attempts"][-1]
    value[records].append(
        event(
            victim,
            "background_observation",
            observed_at,
            observedDuringRequestId=warm["requestId"],
            method="CheckpointDurableExecution",
            outcome="REJECTED",
        )
    )
    errors = validate_evidence(value)
    if rejected:
        assert any("observedDuringRequestId interval" in error for error in errors), errors
    else:
        assert errors == [], errors


@pytest.mark.parametrize("field", ["startedAtMs", "endedAtMs"])
@pytest.mark.parametrize("timestamp", [False, "10", -1, float("inf"), float("nan"), 0, 25])
def test_supplied_operation_timestamps_are_finite_nonnegative_numbers(field: str, timestamp: Any) -> None:
    value = evidence()
    value["histories"][0]["operations"].append(
        {
            "id": "step-1",
            "name": "completed-step",
            "type": "STEP",
            "parentId": None,
            "status": "SUCCEEDED",
            "result": None,
            field: timestamp,
        }
    )
    errors = validate_evidence(value)
    if type(timestamp) is int and timestamp >= 0:
        assert errors == [], errors
    else:
        assert any(f"history operation.{field}" in error for error in errors), errors


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("replay", [False, True])
@pytest.mark.parametrize(
    ("started", "ended", "rejected"),
    [
        (25, 10, True),
        (10, 10, False),
        (10, 25, False),
        (0, 0, False),
        (None, 10, False),
        (25, None, False),
        (None, None, False),
    ],
)
def test_operation_interval_preserves_timestamp_order_and_optional_metadata(
    phase: Any, replay: bool, started: int | None, ended: int | None, rejected: bool
) -> None:
    value = replay_evidence() if replay else evidence(2)
    operation: dict[str, Any] = {
        "id": "done",
        "name": "completed-step",
        "type": "STEP",
        "parentId": None,
        "status": "SUCCEEDED",
    }
    if replay:
        operation = deepcopy(value["replay"]["before"][0])
    if started is not None:
        operation["startedAtMs"] = started
    if ended is not None:
        operation["endedAtMs"] = ended
    if replay:
        if not rejected:
            body = next(item for item in value["events"] if item.get("operationId") == operation["id"])
            body_time = started if started is not None else min(body["atMs"], ended if ended is not None else 35)
            body["atMs"] = body_time
            victim = value["attempts"][0]
            victim["startedAtMs"] = min(victim["startedAtMs"], body_time)
        value["replay"]["before"][0] = deepcopy(operation)
        value["replay"]["after"][0] = deepcopy(operation)
        value["histories"][0]["operations"] = deepcopy(value["replay"]["after"])
    else:
        value["histories"][0]["operations"] = [operation]

    errors = validate_evidence(value, phase=phase)

    if rejected:
        assert any("operation endedAtMs precedes startedAtMs" in error for error in errors), errors
    else:
        assert errors == [], errors


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("requirement", [3, 4])
@pytest.mark.parametrize("observation_end", [199, 200, 201])
def test_observation_grace_extends_past_the_scenario_boundary(
    phase: Any, requirement: int, observation_end: int
) -> None:
    value = deadline_evidence() if requirement == 3 else recovery_evidence()
    # Timeout is at 30; recovered capacity is observed at 45. The configured
    # grace makes the minimum observation end exactly 200 for both scenarios.
    value["budgets"]["observationGraceMs"] = 170 if requirement == 3 else 155
    value["observationEndedAtMs"] = observation_end
    value["collection"]["watermarks"] = dict.fromkeys(("lifecycle", "sdkRequests", "history"), observation_end)
    errors = validate_evidence(value, phase=phase)
    if observation_end < 200:
        assert any(error.startswith("INCOMPLETE:") and "observationGraceMs" in error for error in errors), errors
    else:
        assert errors == [], errors


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("requirement", [3, 4])
@pytest.mark.parametrize("missing", [False, True])
def test_observation_grace_cannot_be_omitted_or_exceed_the_retained_tail(
    phase: Any, requirement: int, missing: bool
) -> None:
    value = deadline_evidence() if requirement == 3 else recovery_evidence()
    if missing:
        del value["budgets"]["observationGraceMs"]
    else:
        value["budgets"]["observationGraceMs"] = 1000
    errors = validate_evidence(value, phase=phase)
    assert any(error.startswith("INCOMPLETE:") and "observationGraceMs" in error for error in errors), errors


@pytest.mark.parametrize("requirement", [3, 4])
def test_independent_retirement_does_not_require_a_successful_observation_grace(requirement: int) -> None:
    value = deadline_evidence() if requirement == 3 else recovery_evidence()
    value["budgets"]["observationGraceMs"] = 1000
    complete_retirement_events(value)
    assert any("observationGraceMs" in error for error in validate_evidence(value))
    assert validate_retirement_evidence(value) == []


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("activity", ["dispatch", "operation", "checkpoint"])
@pytest.mark.parametrize("later_closure", [False, True])
def test_raised_wrapper_closes_admission_at_its_first_exit(phase: Any, activity: str, later_closure: bool) -> None:
    value = evidence(2)
    peer = value["attempts"][1]
    peer["outcome"] = "FAILED"
    wrapper = next(
        item for item in value["events"] if item["requestId"] == peer["requestId"] and item["kind"] == "wrapper_return"
    )
    wrapper["kind"] = "wrapper_raise"
    history = next(item for item in value["histories"] if item["executionArn"] == peer["executionArn"])
    history.pop("result")
    history.update(status="FAILED", error={"type": "PeerFailure"})
    value["expectedHistories"][peer["executionArn"]] = {"status": "FAILED", "error": {"type": "PeerFailure"}}
    next(item for item in value["retirement"]["resources"] if item["id"] == peer["executionArn"])["outcome"] = "FAILED"
    if later_closure:
        value["events"].append(event(peer, "admission_closed", 50))
    assert validate_evidence(value, phase=phase) == []
    if activity == "dispatch":
        value["sdkRequests"].append(
            event(peer, "request", 43, method="CheckpointDurableExecution", outcome="DISPATCHED")
        )
        expected = "dispatched SDK work after admission closed"
    else:
        value["events"].append(event(peer, "admission_probe", 43, probe=activity, outcome="ACCEPTED"))
        expected = f"closed invocation admitted a {activity} probe"
    assert any(expected in error for error in validate_evidence(value, phase=phase))


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("has_result", [False, True])
def test_successful_retry_requires_its_stored_result_including_null(phase: Any, has_result: bool) -> None:
    value = replay_evidence()
    victim = value["attempts"][0]
    history = next(item for item in value["histories"] if item["executionArn"] == victim["executionArn"])
    for operations in (value["replay"]["after"], history["operations"]):
        retried = next(item for item in operations if item["id"] == value["replay"]["retry"]["operationId"])
        if has_result:
            retried["result"] = None
        else:
            retried.pop("result")
        # An unrelated pending operation must not require a stored result.
        operations.append(
            {"id": "pending", "name": "pending-step", "type": "STEP", "parentId": None, "status": "PENDING"}
        )
    errors = validate_evidence(value, phase=phase)
    if has_result:
        assert errors == []
    else:
        assert any("retried operation" in error and "stored result" in error for error in errors), errors


@pytest.mark.parametrize("validator", [validate_evidence, validate_common_evidence, validate_retirement_evidence])
@pytest.mark.parametrize("factory", [deadline_evidence, recovery_evidence])
@pytest.mark.parametrize("mode", ["normal", "emergency"])
def test_observed_fault_release_cannot_outlive_retirement(validator: Any, factory: Any, mode: str) -> None:
    value = factory()
    complete_retirement_events(value)
    value["retirement"]["faults"][0]["mode"] = mode
    assert validator(value) == []
    value["faultsCreated"][0]["releasedAtMs"] = 230
    errors = validator(value)
    assert any("observed fault release" in error and "retirement" in error for error in errors), errors


@pytest.mark.parametrize("validator", [validate_evidence, validate_common_evidence, validate_retirement_evidence])
@pytest.mark.parametrize(
    ("observed", "acknowledged", "rejected"),
    [(None, 210, False), (50, 210, False), (210, 210.0, False), (220, 220, False), (215, 210, False), (210, 230, True)],
)
def test_fault_receipt_and_acknowledgment_independently_fit_retirement(
    validator: Any, observed: Any, acknowledged: int | float, rejected: bool
) -> None:
    value = deadline_evidence()
    complete_retirement_events(value)
    if observed is not None:
        value["faultsCreated"][0]["releasedAtMs"] = observed
    value["retirement"]["faults"][0]["releasedAtMs"] = acknowledged
    errors = validator(value)
    if rejected:
        assert any("retirement ended before owned work settled" in error for error in errors), errors
    else:
        # The fixture can poll the release after its provider ACK returned:
        # request205 -> ACK210 -> receipt215 is a valid completed release.
        assert errors == []
        assert validate_evidence(value, phase="observations") == []


@pytest.mark.parametrize("kind", ["wrapper_enter", "wrapper_return", "task_exit"])
@pytest.mark.parametrize("mutation", ["missing", "duplicate"])
def test_physical_retirement_requires_unique_request_completion_boundaries(kind: str, mutation: str) -> None:
    value = evidence()
    complete_retirement_events(value)
    assert validate_retirement_evidence(value) == []
    peer = value["attempts"][1]
    boundary = next(item for item in value["events"] if item["requestId"] == peer["requestId"] and item["kind"] == kind)
    if mutation == "missing":
        value["events"].remove(boundary)
    else:
        value["events"].append({**boundary, "sequence": 100})
    assert any(
        "actual entry, wrapper exit and root-task exit" in error for error in validate_retirement_evidence(value)
    )


@pytest.mark.parametrize("exit_kind", ["wrapper_return", "wrapper_raise"])
def test_physical_retirement_accepts_each_actual_wrapper_exit(exit_kind: str) -> None:
    value = evidence()
    complete_retirement_events(value)
    peer = value["attempts"][1]
    next(
        item for item in value["events"] if item["requestId"] == peer["requestId"] and item["kind"] == "wrapper_return"
    )["kind"] = exit_kind
    assert validate_retirement_evidence(value) == []


@pytest.mark.parametrize(
    ("start_kind", "end_kind", "identity"),
    [
        ("task_start", "task_exit", "taskId"),
        ("io_start", "io_exit", "taskId"),
        ("checkpoint_call", "checkpoint_exit", "callId"),
        ("poll_call", "poll_exit", "callId"),
    ],
)
@pytest.mark.parametrize(
    "mutation", ["missing-start", "missing-end", "duplicate-start", "duplicate-end", "missing-id", "reversed"]
)
def test_physical_retirement_requires_complete_work_intervals(
    start_kind: str, end_kind: str, identity: str, mutation: str
) -> None:
    value = evidence()
    owner = value["attempts"][1]
    start = event(owner, start_kind, 23, **{identity: "work-1"})
    end = event(owner, end_kind, 24, **{identity: "work-1"})
    complete_retirement_events(value)
    value["events"].extend([start, end])
    local = sorted(
        (item for item in value["events"] if item["requestId"] == owner["requestId"]), key=lambda item: item["atMs"]
    )
    for sequence, item in enumerate(local):
        item["sequence"] = sequence
    assert validate_retirement_evidence(value) == []
    if mutation == "missing-start":
        value["events"].remove(start)
    elif mutation == "missing-end":
        value["events"].remove(end)
    elif mutation == "duplicate-start":
        value["events"].append({**start, "sequence": 100})
    elif mutation == "duplicate-end":
        value["events"].append({**end, "sequence": 100})
    elif mutation == "missing-id":
        del start[identity]
    else:
        start["atMs"], end["atMs"] = end["atMs"], start["atMs"]
        start["sequence"], end["sequence"] = end["sequence"], start["sequence"]
    errors = validate_retirement_evidence(value)
    assert any(
        "completion pair" in error or "non-empty string" in error or "reverses an observed work interval" in error
        for error in errors
    ), errors


@pytest.mark.parametrize(
    "mutation",
    [
        "entry-after-exit",
        "wrong-root",
        "duplicate-sequence",
        "timestamp-order",
        "unknown-kind",
        "running-history",
        "pending-history",
    ],
)
def test_physical_retirement_rejects_uncertain_completion(mutation: str) -> None:
    value = evidence()
    complete_retirement_events(value)
    assert validate_retirement_evidence(value) == []
    peer = value["attempts"][1]
    local = sorted(
        (item for item in value["events"] if item["requestId"] == peer["requestId"]), key=lambda item: item["sequence"]
    )
    if mutation == "entry-after-exit":
        entry = next(item for item in local if item["kind"] == "wrapper_enter")
        entry.update(atMs=43, sequence=100)
        expected = "completion precedes actual invocation entry"
    elif mutation == "wrong-root":
        next(item for item in local if item["kind"] == "task_exit")["taskId"] = "other"
        expected = "actual entry, wrapper exit and root-task exit"
    elif mutation == "duplicate-sequence":
        local[-1]["sequence"] = local[0]["sequence"]
        expected = "unique per-request semantic sequence numbers"
    elif mutation == "timestamp-order":
        local[-1]["atMs"] = 13
        expected = "sequence contradicts its normalized timestamps"
    elif mutation == "unknown-kind":
        value["resourcesCreated"].append({"id": "other-1", "ownerCaseId": value["caseId"], "kind": "other"})
        value["retirement"]["resources"].append(
            {
                "id": "other-1",
                "ownerCaseId": value["caseId"],
                "kind": "other",
                "settledAtMs": 210,
                "outcome": "DONE",
                "evidenceUri": "s3://evidence/other",
            }
        )
        expected = "no completion contract for resource kind"
    else:
        history = next(item for item in value["histories"] if item["executionArn"] == peer["executionArn"])
        history["status"] = "RUNNING" if mutation == "running-history" else "PENDING"
        expected = "service history still permits execution resume"
    errors = validate_retirement_evidence(value)
    assert any(expected in error for error in errors), errors


@pytest.mark.parametrize(
    "fragment", [{}, {"status": "SUCCEEDED", "result": None}, {"startedAtMs": 10, "endedAtMs": 20}]
)
def test_partial_cleanup_operation_preserves_provisional_omissions(fragment: dict[str, Any]) -> None:
    # Cleanup can collect a checkpoint fragment before its complete identity or
    # outcome is available. This cannot certify the same fragment as evidence.
    partial = _Evidence({})
    partial.operation_shape(fragment, "cleanup.operation", completed=True, check_interval=True, partial=True)
    assert partial.errors == []
    complete = _Evidence({})
    complete.operation_shape(fragment, "history.operation", completed=True, check_interval=True)
    assert complete.errors


@pytest.mark.parametrize(
    ("fragment", "diagnostic"),
    [
        ({"id": ""}, "id must be a non-empty string"),
        ({"type": False}, "type must be a non-empty string"),
        ({"status": False}, "status must be a non-empty string"),
        ({"name": False}, "name must be a non-empty string"),
        ({"parentId": False}, "parentId must be a non-empty string"),
        ({"startedAtMs": False}, "startedAtMs must be a finite non-negative number"),
        ({"endedAtMs": float("inf")}, "endedAtMs must be a finite non-negative number"),
        ({"startedAtMs": 20, "endedAtMs": 10}, "endedAtMs precedes startedAtMs"),
    ],
)
def test_partial_cleanup_cannot_hide_supplied_checkpoint_diagnostics(fragment: dict[str, Any], diagnostic: str) -> None:
    partial = _Evidence({})
    partial.operation_shape(fragment, "cleanup.operation", completed=True, check_interval=True, partial=True)
    assert any(diagnostic in error for error in partial.errors), partial.errors


@pytest.mark.parametrize(
    "case",
    [
        "paired",
        "mixed",
        "root",
        "unmatched-start",
        "unmatched-exit",
        "missing-source",
        "wrong-start-source",
        "wrong-exit-source",
        "unexpected-source",
        "unknown-alias",
    ],
)
def test_physical_retirement_audits_preserved_task_aliases_without_mutating_evidence(case: str) -> None:
    value = evidence()
    complete_retirement_events(value)
    owner = value["attempts"][1]
    assert validate_retirement_evidence(value) == []
    if case == "root":
        for item in value["events"]:
            if item["requestId"] == owner["requestId"] and item["kind"] in {"task_start", "task_exit"}:
                item["sourceKind"] = item["kind"]
                item["kind"] = "observed_" + item["kind"]
    else:
        start = event(owner, "observed_task_start", 23, taskId="child", sourceKind="task_start")
        exit_event = event(owner, "observed_task_exit", 24, taskId="child", sourceKind="task_exit")
        if case == "mixed":
            start["kind"] = "task_start"
        elif case == "missing-source":
            del start["sourceKind"]
        elif case == "wrong-start-source":
            start["sourceKind"] = "task_exit"
        elif case == "wrong-exit-source":
            exit_event["sourceKind"] = "task_start"
        elif case == "unexpected-source":
            start["sourceKind"] = "io_start"
        elif case == "unknown-alias":
            start["kind"] = "observed_task_pause"
        if case != "unmatched-exit":
            value["events"].append(start)
        if case != "unmatched-start":
            value["events"].append(exit_event)
    local = sorted(
        (item for item in value["events"] if item["requestId"] == owner["requestId"]), key=lambda item: item["atMs"]
    )
    for sequence, item in enumerate(local):
        item["sequence"] = sequence
    original = deepcopy(value)
    errors = validate_retirement_evidence(value)
    if case in {"paired", "mixed", "root"}:
        assert errors == []
    elif case in {"unmatched-start", "unmatched-exit"}:
        assert errors == ["INCOMPLETE: retirement lacks the unique task_start/task_exit completion pair"]
    else:
        assert any("retirement task alias" in error for error in errors), errors
    assert value == original


@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("requirement", [3, 4])
@pytest.mark.parametrize("source", ["receipt", "acknowledgment"])
@pytest.mark.parametrize("after_boundary", [False, True])
def test_fault_causality_each_observer_has_an_independent_scenario_boundary(
    phase: Any, requirement: int, source: str, after_boundary: bool
) -> None:
    value = deadline_evidence() if requirement == 3 else recovery_evidence()
    boundary = 35 if requirement == 3 else 45
    release = boundary + int(after_boundary)
    # Keep the other observer safely late so its independent rule cannot mask
    # deletion or inversion of the condition under test.
    value["faultsCreated"][0]["releasedAtMs"] = release if source == "receipt" else 210
    value["retirement"]["faults"][0]["releasedAtMs"] = release if source == "acknowledgment" else 210
    errors = validate_evidence(value, phase=phase)
    if after_boundary or (source == "acknowledgment" and phase == "observations"):
        assert errors == []
    else:
        diagnostic = (
            "fault release cannot retroactively establish cleanup or recovery"
            if source == "acknowledgment"
            else "fault release supplied the cleanup being asserted"
            if requirement == 3
            else "fault release cannot satisfy worker recovery"
        )
        assert errors == [f"ASSERTION: {diagnostic}"]


def test_retirement_rejects_dispatched_request_matched_to_rejected_call() -> None:
    value = evidence()
    complete_retirement_events(value)
    for item in value["events"]:
        if item["kind"] == "checkpoint_call" and item.get("callId") == "dispatch-0":
            item["outcome"] = "REJECTED"
    value["events"] = [
        item
        for item in value["events"]
        if not (item["kind"] == "checkpoint_exit" and item.get("callId") == "dispatch-0")
    ]
    assert any("cannot correlate dispatched SDK request" in error for error in validate_retirement_evidence(value))
    assert any(
        "lacks a unique completed checkpoint_call/checkpoint_exit" in error
        for error in validate_observed_retirement(value)
    )


@pytest.mark.parametrize("requests", [[None], ["bad"], {"not": "an array"}])
def test_sdk_request_rejection_check_handles_malformed_records(requests: Any) -> None:
    value = evidence()
    complete_retirement_events(value)
    call = next(row for row in value["events"] if row["kind"] == "checkpoint_call")
    call["outcome"] = "REJECTED"
    value["sdkRequests"] = requests
    assert any("sdkRequests" in error for error in validate_evidence(value))


def test_replay_requires_observed_durable_suspension_checkpoint() -> None:
    value = replay_evidence()
    del value["sdkRequests"][0]["updates"]
    assert any("needs an observed wait or callback checkpoint" in error for error in validate_evidence(value))


def test_replay_rejects_an_extra_unaccounted_retry_body() -> None:
    value = replay_evidence()
    value["replay"]["operationExecutions"].append(
        {"requestId": value["selectedRequests"]["resume"], "operationId": "retry", "attemptId": "extra"}
    )
    assert any("retry body executions must match" in error for error in validate_evidence(value))


def test_warm_reuse_rejects_a_fixture_sourced_start() -> None:
    value = warm_evidence()
    value["attempts"][-1]["startedAtMsSource"] = "fixture"
    assert any("warm request's platform native start time" in error for error in validate_evidence(value))


def test_physical_retirement_audits_completed_launches_after_failed_selection() -> None:
    value = evidence()
    complete_retirement_events(value)
    value["selectedRequests"] = {}
    assert any("selectedRequests must not be empty" in error for error in validate_evidence(value))
    assert validate_retirement_evidence(value) == []


def test_execution_settlement_cannot_precede_recorded_service_operation() -> None:
    value = evidence()
    complete_retirement_events(value)
    execution = value["histories"][0]
    execution["operations"][0]["endedAtMs"] = 190
    settled = next(item for item in value["retirement"]["resources"] if item["id"] == execution["executionArn"])
    settled["settledAtMs"] = 180
    for validator in (validate_evidence, validate_retirement_evidence):
        assert "INCOMPLETE: execution settled before its recorded service operation" in validator(value)


def test_deadline_probe_before_tied_close_does_not_count() -> None:
    value = deadline_evidence()
    closure = next(item for item in value["events"] if item["kind"] == "admission_closed")
    closure["sequence"] = 10
    probe = next(item for item in value["events"] if item.get("probe") == "checkpoint")
    probe.update(atMs=closure["atMs"], sequence=9)
    assert "INCOMPLETE: missing post-closure checkpoint admission probe" in validate_evidence(value)


def test_overlap_cannot_use_retry_to_attribute_original_marker() -> None:
    value = evidence()
    victim = value["attempts"][0]
    victim["outcome"] = "FAILED"
    value["attempts"].append({**victim, "requestId": "retry-victim", "attempt": 2, "outcome": "SUCCEEDED"})
    record_owned_resources(value)
    assert "ASSERTION: overlap victim needs an original successful execution without retry" in validate_evidence(value)


@pytest.mark.parametrize(
    ("field", "replacement"), [("id", "unrelated-step"), ("type", "WAIT"), ("action", "START"), ("atMs", 41)]
)
def test_overlap_requires_marker_completion_in_the_overlap_checkpoint(field: str, replacement: str | int) -> None:
    value = evidence()
    request = value["sdkRequests"][1]
    if field == "atMs":
        request[field] = replacement
    else:
        request["updates"][0][field] = replacement
    assert any("overlap peer lacks an attributed checkpoint dispatch" in error for error in validate_evidence(value))


@pytest.mark.parametrize(("field", "replacement"), [("endedAtMs", 30), ("endedAtMs", 60), ("status", "STARTED")])
def test_replay_requires_completion_between_suspension_and_resume(field: str, replacement: str | int) -> None:
    value = replay_evidence()
    for operations in (value["replay"]["after"], value["histories"][0]["operations"]):
        next(item for item in operations if item["type"] == "WAIT")[field] = replacement
    assert any("needs an observed wait or callback checkpoint" in error for error in validate_evidence(value))


def test_replay_requires_positive_evidence_of_original_completed_step_bodies() -> None:
    value = replay_evidence()
    del value["replay"]["operationExecutions"][0]
    assert any(
        "completed replay step lacks observed user-code execution" in error for error in validate_evidence(value)
    )


@pytest.mark.parametrize("requirement", [2, 7])
def test_disposal_rejects_consistent_but_contradictory_history_and_retirement(requirement: int) -> None:
    value = evidence(2) if requirement == 2 else failure_evidence()
    arn = value["attempts"][0]["executionArn"]
    expectation = (
        {"status": "FAILED", "error": {"type": "example"}}
        if requirement == 2
        else {"status": "SUCCEEDED", "result": "example"}
    )
    history = value["histories"][0]
    history.pop("result", None)
    history.pop("error", None)
    history.update(expectation)
    value["expectedHistories"][arn] = expectation
    retirement = next(item for item in value["retirement"]["resources"] if item["id"] == arn)
    retirement["outcome"] = expectation["status"]
    assert "ASSERTION: terminal service history disagrees with the single disposal invocation" in validate_evidence(
        value
    )


def test_disposal_final_retirement_does_not_reverse_observed_success() -> None:
    value = evidence(2)
    arn = value["attempts"][0]["executionArn"]
    value["histories"][0]["status"] = "RUNNING"
    value["expectedHistories"][arn] = {"allowedStatuses": ["RUNNING", "SUCCEEDED"]}
    retirement = next(item for item in value["retirement"]["resources"] if item["id"] == arn)
    retirement["outcome"] = "FAILED"
    assert validate_evidence(value, phase="observations") == []
    assert "ASSERTION: terminal retirement outcome disagrees with the single disposal invocation" in validate_evidence(
        value
    )


@pytest.mark.parametrize("updates", [None, [None], [{}], [{"id": "", "type": "STEP", "action": "SUCCEED"}]])
def test_supplied_checkpoint_updates_are_structurally_validated(updates: Any) -> None:
    value = evidence(2)
    value["sdkRequests"][0]["updates"] = updates
    for validator in (validate_evidence, validate_common_evidence):
        assert any("sdkRequest.updates" in error for error in validator(value))


@pytest.mark.parametrize("exit_sequence", [9, 11])
def test_deadline_orders_timeout_and_task_exit_at_same_millisecond(exit_sequence: int) -> None:
    value = deadline_evidence()
    timeout = next(item for item in value["events"] if item["kind"] == "platform_timeout")
    exited = next(item for item in value["events"] if item["kind"] == "task_exit")
    timeout["sequence"] = 10
    exited.update(atMs=timeout["atMs"], sequence=exit_sequence)
    errors = validate_evidence(value)
    assert any("controlled work was not in flight" in error for error in errors) == (exit_sequence == 9)


def test_replay_late_original_body_does_not_prove_execution_before_suspension() -> None:
    value = replay_evidence()
    body = next(item for item in value["events"] if item.get("operationId") == "done")
    body.update(atMs=60, observedDuringRequestId=value["selectedRequests"]["resume"])
    assert any("original request before suspension" in error for error in validate_evidence(value))


def test_replay_rejects_an_earlier_invocation_on_the_selected_execution() -> None:
    value = replay_evidence()
    victim = value["attempts"][0]
    previous = attempt("earlier", start=1, end=8)
    previous["executionArn"] = victim["executionArn"]
    value["attempts"].append(previous)
    record_owned_resources(value)
    assert "PLACEMENT: replay victim is not the original invocation of the selected execution" in validate_evidence(
        value
    )


@pytest.mark.parametrize("history_type", ["CALLBACK", "WAIT"])
def test_replay_suspension_checkpoint_type_matches_service_operation(history_type: str) -> None:
    value = replay_evidence()
    value["sdkRequests"][0]["updates"][0]["type"] = "CALLBACK"
    for operations in (value["replay"]["after"], value["histories"][0]["operations"]):
        next(item for item in operations if item["id"] == "pause")["type"] = history_type
    errors = validate_evidence(value)
    if history_type == "CALLBACK":
        assert errors == [], errors
    else:
        assert any("needs an observed wait or callback checkpoint" in error for error in errors)


def test_replay_needs_completed_user_code_not_only_durable_control_operations() -> None:
    value = replay_evidence()
    for operations in (value["replay"]["before"], value["replay"]["after"], value["histories"][0]["operations"]):
        for operation in operations:
            if operation["id"] in ("done", "failed"):
                operation["type"] = "CALLBACK"
    value["replay"]["operationExecutions"] = [
        item for item in value["replay"]["operationExecutions"] if item["operationId"] not in ("done", "failed")
    ]
    assert "INCOMPLETE: replay requires a completed user-code step before suspension" in validate_evidence(value)


def test_replay_rejects_later_body_event_even_when_missing_from_execution_journal() -> None:
    value = replay_evidence()
    resume = value["attempts"][-1]
    value["events"].append(event(resume, "body", 55, operationId="done"))
    assert "ASSERTION: resume repeated completed user code" in validate_evidence(value)


def test_replay_rejects_a_completed_snapshot_from_after_suspension() -> None:
    value = replay_evidence()
    for operations in (value["replay"]["before"], value["replay"]["after"], value["histories"][0]["operations"]):
        next(item for item in operations if item["id"] == "done")["endedAtMs"] = 60
    assert "ASSERTION: replay.before contains an operation that completed after suspension" in validate_evidence(value)


def test_replay_rejects_a_step_the_service_completed_after_suspension_even_without_snapshot_timing() -> None:
    value = replay_evidence()
    for operations in (value["replay"]["after"], value["histories"][0]["operations"]):
        next(item for item in operations if item["id"] == "done")["endedAtMs"] = 60
    assert "ASSERTION: replay changed completed operation identity, result, or failure" in validate_evidence(value)


def test_deadline_rejects_admission_closed_before_a_tied_timeout() -> None:
    value = deadline_evidence()
    timeout = next(item for item in value["events"] if item["kind"] == "platform_timeout")
    closed = next(item for item in value["events"] if item["kind"] == "admission_closed")
    timeout["sequence"] = 10
    closed.update(atMs=timeout["atMs"], sequence=9)
    assert any("admissionMs" in error and "precedes the timeout" in error for error in validate_evidence(value))


def test_role_attempt_ordinals_must_follow_admission_order() -> None:
    value = evidence()
    earlier = attempt("victim", start=1, end=5)
    earlier.update(requestId="earlier-victim", executionArn="arn:earlier-victim", attempt=2)
    value["attempts"].append(earlier)
    refresh_histories(value)
    record_owned_resources(value)
    assert "PLACEMENT: attempt ordinals contradict the admission order for victim" in validate_evidence(value)


def test_deadline_victim_must_be_its_executions_first_admission() -> None:
    value = deadline_evidence()
    victim = value["attempts"][0]
    earlier = attempt("earlier", start=1, end=5)
    earlier["executionArn"] = victim["executionArn"]
    value["attempts"].append(earlier)
    record_owned_resources(value)
    assert "PLACEMENT: selectedRequests.victim is not the first invocation of its execution" in validate_evidence(value)


def test_replay_rejects_late_body_that_retains_victim_ownership() -> None:
    value = replay_evidence()
    victim = value["attempts"][0]
    value["events"].append(event(victim, "body", 60, operationId="done"))
    assert "ASSERTION: resume repeated completed user code" in validate_evidence(value)


def test_replay_rejects_body_after_recorded_operation_completion() -> None:
    value = replay_evidence()
    for operations in (value["replay"]["before"], value["replay"]["after"], value["histories"][0]["operations"]):
        next(item for item in operations if item["id"] == "done")["endedAtMs"] = 20
    assert any("lacks observed user-code execution" in error for error in validate_evidence(value))


def test_replay_rejects_retry_order_contradicted_by_body_events() -> None:
    value = replay_evidence()
    by_request = {item["requestId"]: item for item in value["attempts"]}
    retries = value["replay"]["retry"]["attempts"]
    for retry, at in zip(retries, (65, 55), strict=True):
        value["events"].append(
            event(by_request[retry["requestId"]], "body", at, operationId="retry", attemptId=retry["attemptId"])
        )
    assert "ASSERTION: retry attempt order contradicts timed user-code entries" in validate_evidence(value)


@pytest.mark.parametrize("sequence", [None, 9, 11])
def test_deadline_inflight_sequence_when_timestamp_ties(sequence: int | None) -> None:
    value = deadline_evidence()
    timeout = next(item for item in value["events"] if item["kind"] == "platform_timeout")
    inflight = next(item for item in value["events"] if item["kind"] == "work_in_flight")
    timeout["sequence"] = 10
    inflight["atMs"] = timeout["atMs"]
    if sequence is not None:
        inflight["sequence"] = sequence
    errors = validate_evidence(value)
    if sequence is None:
        assert errors == ["INCOMPLETE: same-time observations need sequence numbers to establish their order"]
    elif sequence < timeout["sequence"]:
        assert errors == [], errors
    else:
        assert "ASSERTION: controlled work was not in flight across the real deadline" in errors


def test_retirement_only_task_completion_accepts_a_normalized_alias() -> None:
    value = evidence()
    victim = value["attempts"][0]
    value["events"].append(event(victim, "observed_task_start", 30, taskId="child", sourceKind="task_start"))
    value["retirement"]["completions"] = [
        event(victim, "observed_task_exit", 205, taskId="child", sourceKind="task_exit")
    ]
    assert validate_evidence(value) == []


def test_replay_before_rejects_a_nonterminal_step_even_with_a_stored_error() -> None:
    value = replay_evidence()
    for operations in (value["replay"]["before"], value["replay"]["after"], value["histories"][0]["operations"]):
        operation = next(item for item in operations if item["id"] == "done")
        operation.update(status="PENDING", error={"type": "not-terminal"})
    assert any("replay.before must have a terminal operation status" in error for error in validate_evidence(value))


@pytest.mark.parametrize("completion_at", [205, 215])
def test_independent_retirement_audits_cleanup_only_completion(completion_at: int) -> None:
    value = evidence()
    victim = value["attempts"][0]
    value["events"].append(event(victim, "io_start", 30, taskId="held"))
    complete_retirement_events(value)
    value["retirement"]["completions"] = [event(victim, "io_exit", completion_at, taskId="held", sequence=100)]
    errors = validate_retirement_evidence(value)
    if completion_at == 205:
        assert errors == [], errors
    else:
        assert any("interval lies outside settlement" in error for error in errors)


def test_replay_rejects_snapshot_omitting_known_completed_step() -> None:
    value = replay_evidence()
    step = {
        "id": "omitted",
        "name": "omitted",
        "type": "STEP",
        "status": "SUCCEEDED",
        "parentId": None,
        "endedAtMs": 30,
        "result": None,
    }
    value["replay"]["after"].append(step)
    value["histories"][0]["operations"].append(step)
    assert (
        "INCOMPLETE: replay.before omits an operation known to have completed before suspension"
        in validate_evidence(value)
    )


def test_replay_ignores_service_start_timestamp_when_body_precedes_async_checkpoint() -> None:
    value = replay_evidence()
    for operations in (value["replay"]["before"], value["replay"]["after"], value["histories"][0]["operations"]):
        next(item for item in operations if item["id"] == "done").update(startedAtMs=25, endedAtMs=30)
    assert validate_evidence(value) == []


def test_replay_allows_completed_suspension_callback_in_resume_snapshot() -> None:
    value = replay_evidence()
    value["sdkRequests"][0]["updates"][0]["type"] = "CALLBACK"
    callback = next(item for item in value["replay"]["after"] if item["id"] == "pause")
    callback.update(type="CALLBACK", result=None)
    observed = next(item for item in value["histories"][0]["operations"] if item["id"] == "pause")
    observed.update(callback)
    value["replay"]["before"].append(deepcopy(callback))
    value["replay"]["sideEffects"].append({"operationId": "pause", "count": 0, "expectedCount": 0})
    assert validate_evidence(value) == []


def test_replay_resume_snapshot_without_service_timing_matches_completed_wait() -> None:
    value = replay_evidence()
    wait = next(item for item in value["replay"]["after"] if item["id"] == "pause")
    snapshot = {key: item for key, item in wait.items() if key != "endedAtMs"}
    value["replay"]["before"].append({**snapshot, "result": None})
    wait["result"] = None
    next(item for item in value["histories"][0]["operations"] if item["id"] == "pause")["result"] = None
    value["replay"]["sideEffects"].append({"operationId": "pause", "count": 0, "expectedCount": 0})
    assert validate_evidence(value) == []


def test_replay_after_must_keep_the_service_completion_time() -> None:
    value = replay_evidence()
    next(item for item in value["replay"]["after"] if item["id"] == "pause")["endedAtMs"] = 46
    assert any("replay.after differs from the collected service history" in error for error in validate_evidence(value))


def test_replay_unordered_body_tie_is_incomplete_without_asserting_sdk_failure() -> None:
    value = replay_evidence()
    next(item for item in value["events"] if item.get("operationId") == "done")["atMs"] = 35
    errors = validate_evidence(value)
    assert errors and all(error.startswith("INCOMPLETE:") for error in errors), errors


def test_replay_rejects_unaccounted_timed_retry_body() -> None:
    value = replay_evidence()
    resume = value["attempts"][-1]
    value["events"].append(event(resume, "body", 55, operationId="retry", attemptId="unlisted"))
    assert "INCOMPLETE: observed retry body does not match a declared attempt" in validate_evidence(value)


def test_replay_rejects_tied_native_admissions_when_selecting_original() -> None:
    value = replay_evidence()
    victim = value["attempts"][0]
    another = attempt("earlier", start=victim["startedAtMs"], end=25)
    another["executionArn"] = victim["executionArn"]
    value["attempts"].append(another)
    record_owned_resources(value)
    assert "INCOMPLETE: tied native admissions cannot establish the original replay invocation" in validate_evidence(
        value
    )


def test_replay_requires_one_side_effect_count_per_operation() -> None:
    value = replay_evidence()
    value["replay"]["sideEffects"].append({"operationId": "done", "count": 2, "expectedCount": 2})
    assert "INCOMPLETE: side-effect counts must be unique per operation" in validate_evidence(value)


def test_retirement_accepts_an_observed_exit_for_a_rejected_call() -> None:
    value = evidence()
    victim = value["attempts"][0]
    record = event(victim, "checkpoint_call", 28, callId="rejected", outcome="REJECTED")
    value["events"].extend([record, event(victim, "checkpoint_exit", 29, callId="rejected")])
    value["sdkRequests"].append({**record, "method": "CheckpointDurableExecution"})
    complete_retirement_events(value)
    assert validate_evidence(value) == []
    assert validate_retirement_evidence(value) == []


def test_final_retirement_requires_sequence_for_tied_io_interval() -> None:
    value = evidence()
    victim = value["attempts"][0]
    value["events"].extend(
        [
            event(victim, "io_start", 30, taskId="tied"),
            event(victim, "io_exit", 30, taskId="tied"),
        ]
    )
    errors = validate_evidence(value, phase="final")
    assert errors == ["INCOMPLETE: same-time observations need sequence numbers to establish their order"]


def test_physical_audit_requires_sdk_request_to_corroborate_rejection() -> None:
    value = evidence()
    victim = value["attempts"][0]
    value["events"].append(event(victim, "checkpoint_call", 28, callId="uncorroborated", outcome="REJECTED"))
    complete_retirement_events(value)
    assert "INCOMPLETE: retirement lacks the unique checkpoint_call/checkpoint_exit completion pair" in (
        validate_retirement_evidence(value)
    )
    value["sdkRequests"].append(
        event(victim, "request", 28, method="CheckpointDurableExecution", callId="uncorroborated", outcome="REJECTED")
    )
    assert validate_retirement_evidence(value) == []


def test_deadline_requires_cleanup_of_the_controlled_task() -> None:
    value = deadline_evidence()
    exited = next(item for item in value["events"] if item["kind"] == "task_exit")
    exited["taskId"] = "unrelated"
    assert "ASSERTION: deadline task exit is not the controlled in-flight task" in validate_evidence(value)


def test_single_timed_retry_body_cannot_follow_recorded_success() -> None:
    value = replay_evidence()
    for operations in (value["replay"]["after"], value["histories"][0]["operations"]):
        next(item for item in operations if item["id"] == "retry")["endedAtMs"] = 55
    victim = value["attempts"][0]
    value["events"].append(event(victim, "body", 60, operationId="retry", attemptId="retry-1"))
    assert "ASSERTION: timed retry body occurred after the recorded operation completion" in validate_evidence(value)


@pytest.mark.parametrize("kind", ["task_start", "observed_task_start"])
def test_deadline_rejects_controlled_task_start_after_inflight_observation(kind: str) -> None:
    value = deadline_evidence()
    victim = value["attempts"][0]
    value["events"].append(event(victim, kind, 33, taskId="root", sourceKind="task_start"))
    assert "ASSERTION: controlled task started after it was observed in flight" in validate_evidence(value)


def test_untimed_retry_cannot_complete_before_its_owner_is_admitted() -> None:
    value = replay_evidence()
    for operations in (value["replay"]["after"], value["histories"][0]["operations"]):
        next(item for item in operations if item["id"] == "retry")["endedAtMs"] = 45
    assert "ASSERTION: retry owner was admitted after the recorded operation completion" in validate_evidence(value)


@pytest.mark.parametrize("start_sequence", [None, 9, 11])
def test_deadline_orders_task_start_tied_with_inflight_observation(start_sequence: int | None) -> None:
    value = deadline_evidence()
    victim = value["attempts"][0]
    inflight = next(item for item in value["events"] if item["kind"] == "work_in_flight")
    inflight["sequence"] = 10
    start = event(victim, "task_start", inflight["atMs"], taskId="root")
    if start_sequence is not None:
        start["sequence"] = start_sequence
    value["events"].append(start)
    errors = validate_evidence(value)
    if start_sequence is None:
        assert errors == ["INCOMPLETE: same-time observations need sequence numbers to establish their order"]
    elif start_sequence < 10:
        assert not errors
    else:
        assert "ASSERTION: controlled task started after it was observed in flight" in errors


def test_timed_retry_cannot_precede_an_untimed_predecessors_admission() -> None:
    value = replay_evidence()
    victim = value["attempts"][0]
    resume = value["attempts"][-1]
    late = attempt("extra", start=90, end=100)
    late["executionArn"] = victim["executionArn"]
    value["attempts"].append(late)
    record_owned_resources(value)
    value["replay"]["retry"]["attempts"][0]["requestId"] = late["requestId"]
    next(item for item in value["replay"]["operationExecutions"] if item.get("attemptId") == "retry-1")["requestId"] = (
        late["requestId"]
    )
    value["events"].append(event(resume, "body", 60, operationId="retry", attemptId="retry-2"))
    assert "ASSERTION: retry body precedes a prior attempt's admission" in validate_evidence(value)


def test_same_process_recovery_requires_platform_interval() -> None:
    value = recovery_evidence("python")
    del value["attempts"][0]["endedAtMsSource"]
    assert (
        "INCOMPLETE: same-process Python recovery requires platform native completion and admission"
        in validate_evidence(value)
    )


def test_integral_float_retry_body_attempt_uses_normalized_identity() -> None:
    value = replay_evidence()
    resume = value["attempts"][-1]
    identity = f"{resume['requestId']}:2"
    value["replay"]["retry"]["attempts"][-1]["attemptId"] = identity
    value["replay"]["operationExecutions"][-1]["attemptId"] = identity
    value["events"].append(event(resume, "body", 60, operationId="retry", attempt=2.0))
    assert validate_evidence(value) == []


def test_recovery_requires_unambiguous_original_admission() -> None:
    value = recovery_evidence()
    recovery = value["attempts"][-1]
    extra = attempt("extra", start=recovery["startedAtMs"], end=recovery["endedAtMs"])
    extra["executionArn"] = recovery["executionArn"]
    value["attempts"].append(extra)
    record_owned_resources(value)
    assert "INCOMPLETE: tied native admissions cannot establish the original recovery invocation" in validate_evidence(
        value
    )


def test_retry_body_cannot_match_a_synthetic_none_identifier() -> None:
    value = replay_evidence()
    resume = value["attempts"][-1]
    identity = f"{resume['requestId']}:None"
    value["replay"]["retry"]["attempts"][-1]["attemptId"] = identity
    value["replay"]["operationExecutions"][-1]["attemptId"] = identity
    value["events"].append(event(resume, "body", 60, operationId="retry"))
    assert "INCOMPLETE: observed retry body does not match a declared attempt" in validate_evidence(value)


def test_deadline_rejects_an_extra_aliased_exit_for_controlled_task() -> None:
    value = deadline_evidence()
    victim = value["attempts"][0]
    value["events"].append(event(victim, "observed_task_exit", 28, taskId="root", sourceKind="task_exit"))
    for phase in ("observations", "final"):
        assert "INCOMPLETE: deadline controlled task must have exactly one recorded exit" in validate_evidence(
            value, phase=phase
        )


def test_same_json_compares_exact_numbers_across_decoders() -> None:
    assert same_json(Decimal("0.500000000000000000000"), 0.5)
    assert not same_json(Decimal("0.500000000000000000001"), 0.5)
    assert not same_json(Decimal(1), True)


@pytest.mark.parametrize("role", ["victim", "peer", "peer2"])
def test_recovery_cohort_requires_unambiguous_original_admission(role: str) -> None:
    value = recovery_evidence(native_concurrency=3)
    original = next(item for item in value["attempts"] if item["role"] == role)
    extra = attempt("extra", start=original["startedAtMs"], end=original["endedAtMs"])
    extra["executionArn"] = original["executionArn"]
    value["attempts"].append(extra)
    record_owned_resources(value)
    assert f"INCOMPLETE: tied native admissions cannot establish the original {role} invocation" in validate_evidence(
        value
    )


@pytest.mark.parametrize("last_sequence", [8, None, 12])
def test_retry_sequences_order_nonadjacent_bodies_on_the_same_request(last_sequence: int | None) -> None:
    value = replay_evidence()
    victim, resume = value["attempts"][0], value["attempts"][-1]
    retries = value["replay"]["retry"]["attempts"]
    retries.insert(0, {"requestId": resume["requestId"], "attemptId": "resume-first", "outcome": "FAILED"})
    value["replay"]["operationExecutions"].append({**retries[0], "operationId": "retry"})
    for owner, retry, sequence in zip((resume, victim, resume), retries, (10, 11, last_sequence), strict=True):
        observation = event(owner, "body", 60, operationId="retry", attemptId=retry["attemptId"])
        if sequence is not None:
            observation["sequence"] = sequence
        value["events"].append(observation)
    errors = validate_evidence(value)
    if last_sequence is None:
        assert "INCOMPLETE: same-time observations need sequence numbers to establish their order" in errors
    elif last_sequence < 10:
        assert "ASSERTION: retry attempt order contradicts timed user-code entries" in errors
    else:
        assert not errors


@pytest.mark.parametrize("allowed", [[], ["UNKNOWN"], ["FAILED", "FAILED"], [[]]])
def test_adjudication_invalid_status_allowance_is_incomplete(allowed: list[Any]) -> None:
    frozen = evidence(2)
    arn = frozen["histories"][0]["executionArn"]
    frozen["expectedHistories"][arn].pop("status")
    frozen["expectedHistories"][arn]["allowedStatuses"] = allowed
    findings = validate_evidence(frozen, phase="observations")
    assert any(item.startswith("INCOMPLETE:") and "allowedStatuses" in item for item in findings)
    assert not any(item.startswith("ASSERTION:") and "allowedStatuses" in item for item in findings)


@pytest.mark.parametrize("dispatch", ["complete", "uncorrelated"])
def test_observed_retirement_correlates_dispatched_sdk_requests(dispatch: str) -> None:
    value = evidence()
    complete_retirement_events(value)
    if dispatch == "uncorrelated":
        call_id = next(row["callId"] for row in value["sdkRequests"] if row["outcome"] == "DISPATCHED")
        value["events"] = [row for row in value["events"] if row.get("callId") != call_id]

    errors = validate_observed_retirement(value)

    if dispatch == "complete":
        assert errors == []
    else:
        assert errors == [
            "INCOMPLETE: retirement cannot correlate dispatched SDK request with its unique checkpoint_call"
        ]
