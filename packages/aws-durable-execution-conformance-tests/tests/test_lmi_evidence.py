# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Positive controls and deliberate corruption of independent lifecycle evidence."""

from copy import deepcopy
from typing import Any

import pytest

from aws_durable_execution_conformance_tests.lmi.evidence import validate_evidence


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
        "cohortId": "cohort-1",
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
            "evidenceUris": ["s3://evidence/run-1/case-1/cohort-1/raw.json"],
        },
        "resourcesCreated": [],
        "faultsCreated": [],
        "retirement": {"retiredAtMs": 220, "resources": [], "faults": []},
    }
    refresh_histories(result)
    record_owned_resources(result)
    return result


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
    value["budgets"] = {"admissionMs": 5, "cleanupMs": 10}
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


def recovery_evidence(runtime: str = "javascript") -> dict[str, Any]:
    value = deadline_evidence(4, runtime)
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
    "factory", [lambda: evidence(2), deadline_evidence, recovery_evidence, replay_evidence, warm_evidence]
)
def test_lifecycle_positive_controls(factory: Any) -> None:
    assert validate_evidence(factory()) == []


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


@pytest.mark.parametrize("field", ["environmentId", "workerId", "processId"])
def test_recovery_cannot_use_unrelated_capacity(field: str) -> None:
    value = recovery_evidence()
    value["attempts"][-1][field] = "replacement"
    assert any(error.startswith("PLACEMENT:") for error in validate_evidence(value))


def test_recovery_requires_healthy_peer_progress_and_occupied_capacity() -> None:
    value = recovery_evidence()
    value["attempts"][1]["endedAtMs"] = 43
    assert any("capacity" in error for error in validate_evidence(value))
    value = recovery_evidence()
    value["events"] = [item for item in value["events"] if item["kind"] != "peer_progress"]
    assert any("healthy peer" in error for error in validate_evidence(value))


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


@pytest.mark.parametrize("runtime", ["javascript", "java", "python"])
def test_telemetry_cohort_requires_overlap_and_identified_warm_reuse(runtime: str) -> None:
    value = warm_evidence(runtime)
    value["requirementId"] = "lmi-lifecycle-7"
    assert validate_evidence(value) == []
    del value["selectedRequests"]["warm"]
    assert any("warm" in error for error in validate_evidence(value))


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
