# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Positive controls and deliberate corruption of independent lifecycle evidence."""

from copy import deepcopy
from typing import Any, Literal

import pytest

from aws_durable_execution_conformance_tests.lmi.evidence import (
    same_json,
    validate_common_evidence,
    validate_evidence,
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
        {"executionArn": arn, "ownerCaseId": value["caseId"], "operations": [], **expected}
        for arn, expected in expectations.items()
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
            event(victim, "work_in_flight", 25),
            event(victim, "platform_timeout", 30, source="platform"),
            event(victim, "admission_closed", 31),
            event(victim, "task_exit", 35),
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
    value["events"].append(event(victim, "pending", 35))
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
    value["replay"] = {
        "before": [success, failure],
        "after": [deepcopy(success), deepcopy(failure), retried],
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
    warm = attempt("warm", start=50, end=80)
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


@pytest.mark.parametrize("stream", ["events", "sdkRequests"])
def test_cross_invocation_attribution_is_rejected(stream: str) -> None:
    value = warm_evidence()
    value[stream][0]["ownerRequestId"] = "request-warm"
    assert any("different invocation" in error for error in validate_evidence(value))


def test_dispatch_after_wrapper_return_is_rejected() -> None:
    value = warm_evidence()
    value["sdkRequests"][0]["atMs"] = 60
    assert any("after admission closed" in error for error in validate_evidence(value))


def test_missing_dispatch_collection_cannot_prove_no_dispatch() -> None:
    value = evidence(2)
    value["sdkRequests"] = []
    value["collection"]["watermarks"]["sdkRequests"] = 39
    assert any("does not cover" in error for error in validate_evidence(value))


def test_admission_rejection_requires_actual_probes() -> None:
    value = evidence(2)
    value["events"] = [item for item in value["events"] if item["kind"] != "admission_probe"]
    assert any("admission probe" in error for error in validate_evidence(value))


@pytest.mark.parametrize("requirement", [2, 7])
@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("probe", ["operation", "checkpoint"])
@pytest.mark.parametrize(
    ("at", "outcome", "rejected"),
    [(35, "ACCEPTED", False), (36, "ACCEPTED", True), (37, "ACCEPTED", True), (37, "REJECTED", False)],
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


@pytest.mark.parametrize("requirement", [2, 7])
@pytest.mark.parametrize("phase", ["observations", "final"])
@pytest.mark.parametrize("probe", ["operation", "checkpoint"])
def test_disposal_still_requires_probes_after_wrapper_return(
    requirement: int, phase: Literal["observations", "final"], probe: str
) -> None:
    value = evidence(2) if requirement == 2 else failure_evidence()
    victim = value["attempts"][0]
    value["events"] = [item for item in value["events"] if item.get("probe") != probe]
    value["events"].extend(
        [event(victim, "admission_closed", 36), event(victim, "admission_probe", 37, probe=probe, outcome="REJECTED")]
    )
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


def complete_retirement_events(value: dict[str, Any]) -> None:
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


def test_independent_retirement_audits_quiescence_without_rechecking_probe_behavior() -> None:
    value = evidence()
    value["events"].append(event(value["attempts"][0], "admission_probe", 60, probe="operation", outcome="ACCEPTED"))
    complete_retirement_events(value)

    assert any("closed invocation admitted" in error for error in validate_common_evidence(value))
    assert validate_retirement_evidence(value) == []


@pytest.mark.parametrize("change", ["early", "simulated", "no-inflight", "accepted", "budget"])
def test_deadline_rejects_insufficient_platform_and_admission_evidence(change: str) -> None:
    value = deadline_evidence()
    timeout = next(item for item in value["events"] if item["kind"] == "platform_timeout")
    if change == "early":
        timeout["atMs"] = 29
    elif change == "simulated":
        timeout["source"] = "fixture"
    elif change == "no-inflight":
        value["events"] = [item for item in value["events"] if item["kind"] != "work_in_flight"]
    elif change == "accepted":
        next(item for item in value["events"] if item["kind"] == "admission_probe")["outcome"] = "ACCEPTED"
    else:
        value["budgets"]["cleanupMs"] = 1
    assert validate_evidence(value)


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


def test_misattributed_service_history_is_not_a_successful_output() -> None:
    value = evidence()
    value["histories"][0]["result"] = {"marker": "peer"}
    assert any("fixture expectation" in error for error in validate_evidence(value))


@pytest.mark.parametrize("missing", ["retry", "operationExecutions", "sideEffects"])
def test_replay_cannot_omit_retry_or_execution_observations(missing: str) -> None:
    value = replay_evidence()
    del value["replay"][missing]
    assert validate_evidence(value)


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
        ({"value": [1]}, {"value": [True]}),
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
    value = evidence()
    history = value["histories"][0]
    history["status"] = status
    history.pop("result")
    value["expectedHistories"][history["executionArn"]] = {
        "allowedStatuses": ["RUNNING", "FAILED", "TIMED_OUT", "STOPPED"]
    }
    if status != "RUNNING":
        # Retirement of an already terminal execution keeps its outcome.
        retired = next(item for item in value["retirement"]["resources"] if item["id"] == history["executionArn"])
        retired["outcome"] = status
    assert validate_evidence(value) == []


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
    value = evidence()
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


@pytest.mark.parametrize(("outcome", "rejected"), [(None, True), ("", True), (7, True), ("FIRED", False)])
def test_every_retired_resource_needs_an_outcome(outcome: Any, rejected: bool) -> None:
    # The schema requires a non-empty outcome for every retired resource, not
    # only for the four kinds with their own completion rules.
    value = evidence()
    resource = {"id": "timer-1", "kind": "timer", "ownerCaseId": value["caseId"]}
    value["resourcesCreated"].append(resource)
    settled = {**resource, "settledAtMs": 210, "evidenceUri": "s3://evidence/run-1/case-1/timer.json"}
    if outcome is not None:
        settled["outcome"] = outcome
    value["retirement"]["resources"].append(settled)
    errors = validate_evidence(value)
    assert any("retirement.resources.outcome" in error for error in errors) is rejected, errors


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
    value = replay_evidence() if replay else evidence()
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
