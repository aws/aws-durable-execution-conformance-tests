import copy

import pytest

from aws_durable_execution_conformance_tests.lmi import observations as evidence


def event(phase, time=1, **fields):
    return {
        "phase": phase,
        "time": time,
        "sequence": int(time * 10),
        "marker": "a",
        "request": "r1",
        "execution": "exec1",
        "environment": "env1",
        "process": "p1",
        "pid": 10,
        "deadline": 10,
        "gate": "gate",
        **fields,
    }


def alive(events):
    return events + [
        {**e, "phase": "ALIVE", "time": max(v["time"] for v in events) + 1} for e in events if e["phase"] == "BLOCKED"
    ]


def test_overlap_requires_distinct_processes_requests_and_executions():
    events = [
        event("BLOCKED"),
        event(
            "BLOCKED",
            2,
            marker="b",
            process="p2",
            pid=11,
            request="r2",
            execution="exec2",
        ),
    ]
    assert evidence.overlap(alive(events), {"a", "b"}, 2, runtime="python") == "env1"
    with pytest.raises(evidence.PlacementError):
        evidence.overlap(events, {"a", "b"}, 2, runtime="python")  # starts alone do not prove overlap
    for key in ("process", "pid", "request", "execution"):
        invalid = copy.deepcopy(events)
        invalid[1][key] = invalid[0][key]
        with pytest.raises(evidence.PlacementError):
            evidence.overlap(alive(invalid), {"a", "b"}, 2, runtime="python")


def test_different_environment_and_sequential_invocations_cannot_pass():
    a = event("BLOCKED")
    b = event("BLOCKED", 3, marker="b", process="p2", pid=11, request="r2", execution="exec2")
    for events in ([a, {**b, "environment": "env2"}], [a, event("IO_EXIT", 2), b]):
        with pytest.raises(evidence.PlacementError):
            evidence.overlap(alive(events), {"a", "b"}, 2, runtime="python")
    with pytest.raises(evidence.PlacementError):
        evidence.overlap(alive([a, b]), {"a", "b"}, 2, runtime="python", environment="original-env")


def sources(raw, **cloud_fields):
    from types import SimpleNamespace
    from typing import Any, cast

    from aws_durable_execution_conformance_tests.lmi.source_evidence import CaseSources

    from .test_shared_partial_evidence import context

    ctx = context()
    # Only the manifest commit and the fields each case needs are read here.
    cloud = SimpleNamespace(manifest={"commit": "commit"}, **cloud_fields)
    return ctx, CaseSources(
        ctx,
        cast("Any", cloud),
        tuple(raw),
        expected_outcome=lambda item, scenario: {"status": "SUCCEEDED"},
        worker_identity=lambda item: item["process"],
    )


def raw_record(phase, sequence, **fields):
    return {
        "run": "run",
        "commit": "commit",
        "caseId": "case",
        "request": "r1",
        "sequence": sequence,
        "phase": phase,
        "execution": "exec",
        "marker": "m",
        "environment": "env",
        "process": "p",
        "time": 1,
        **fields,
    }


def test_raw_control_failures_become_collection_errors():
    """Collection reads the ledger with control validation disabled.

    Only CONFORMANCE records become observations, so a control fault first seen
    during collection used to vanish and leave full watermarks behind.
    """
    _, case_sources = sources([raw_record("CONTROL_ERROR", 1, gate="fault", error="AccessDenied")])
    events, errors = case_sources.observations()
    assert events == []
    assert any("fault" in error and "AccessDenied" in error for error in errors)


def test_replacement_worker_correlation_is_preserved():
    """A replace-worker runtime reports the worker capacity it took over."""
    ctx, case_sources = sources(
        [raw_record("WRAPPER_ENTER", 1)],
        invocations=[{"marker": "m", "payload": {"scenario": "barrier"}}],
        request_roles={"r1": "victim"},
        raw_histories={},
    )
    ctx.record_attempt("r1", role="victim", requestId="r1")
    events = [
        {"kind": "worker_recovered", "requestId": "r1", "atMs": 10, "replacesWorkerId": "retired-worker"},
        {"kind": "wrapper_return", "requestId": "r1", "atMs": 20, "outcome": "SUCCEEDED"},
    ]
    attempts, _, errors = case_sources.attempts(events)
    assert errors == []
    assert attempts[0]["replacesWorkerId"] == "retired-worker"
    assert attempts[0]["workerId"] == "p"


def retry_body(request, attempt, at_ms):
    return {
        "kind": "body",
        "requestId": request,
        "operation": "retry",
        "operationId": "retry-op",
        "attempt": attempt,
        "atMs": at_ms,
        "sequence": 1,
    }


def retry_exit(request, attempt, outcome, at_ms):
    return {
        "kind": "task_exit",
        "requestId": request,
        "operationId": "retry-op",
        "attempt": attempt,
        "outcome": outcome,
        "atMs": at_ms,
        "sequence": 2,
    }


def test_retry_attempts_follow_causal_time_not_collection_order():
    """Raw collection order follows the S3 request keys, not execution order.

    A batch first read together can list the later successful attempt before its
    earlier failure, which previously rejected a conforming SDK.
    """
    from aws_durable_execution_conformance_tests.lmi.source_evidence import replay_evidence

    events = [
        retry_body("aa-later-request", 2, 200),
        retry_exit("aa-later-request", 2, "SUCCEEDED", 210),
        retry_body("zz-earlier-request", 1, 100),
        retry_exit("zz-earlier-request", 1, "FAILED", 110),
    ]
    replay = replay_evidence(events, [], {"victim": "zz-earlier-request", "resume": "aa-later-request"})
    assert [item["outcome"] for item in replay["retry"]["attempts"]] == ["FAILED", "SUCCEEDED"]
    assert [item["requestId"] for item in replay["operationExecutions"]] == [
        "zz-earlier-request",
        "aa-later-request",
    ]
