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
