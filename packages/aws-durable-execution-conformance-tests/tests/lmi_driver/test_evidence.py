import copy
from dataclasses import replace
from typing import Any

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
    from typing import cast

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


def test_service_recorded_request_without_a_ledger_stream_is_incomplete():
    # The original was rejected before running, so only its retry published a
    # lifecycle stream. The retry must not stand in for the original silently.
    ctx, case_sources = sources(
        [raw_record("WRAPPER_ENTER", 1, request="r2")],
        invocations=[{"marker": "m", "arn": "exec", "payload": {"scenario": "barrier"}}],
        request_roles={"r2": "victim"},
        raw_histories={
            "exec": [
                {
                    "EventType": "InvocationCompleted",
                    "InvocationCompletedDetails": {
                        "RequestId": "r1",
                        "StartTimestamp": 0.0001,
                        "EndTimestamp": 0.0002,
                        "Error": {"Payload": {"ErrorType": "TooManyRequestsException"}},
                    },
                },
                {
                    "EventType": "InvocationCompleted",
                    "InvocationCompletedDetails": {"RequestId": "r2", "StartTimestamp": 0.5, "EndTimestamp": 0.9},
                },
            ]
        },
    )
    ctx.record_attempt("r2", role="victim", requestId="r2")
    events = [{"kind": "wrapper_return", "requestId": "r2", "atMs": 900, "outcome": "SUCCEEDED"}]
    _, _, errors = case_sources.attempts(events)
    missing = [error for error in errors if "no lifecycle ledger stream" in error]
    assert len(missing) == 1 and "r1" in missing[0] and "TooManyRequestsException" in missing[0], errors


@pytest.mark.parametrize(
    ("details", "ended_at", "outcome", "expected_error"),
    [
        ({"EndTimestamp": 0.065}, 65, "SUCCEEDED", None),
        ({"EndTimestamp": 0.059}, 59, "SUCCEEDED", None),
        (None, 60, "SUCCEEDED", "missing a matching service completion"),
        ({"EndTimestamp": None}, 60, "SUCCEEDED", "EndTimestamp"),
        ({"EndTimestamp": 0.065, "Error": {"ErrorType": "Runtime.ExitError"}}, 65, "FAILED", None),
    ],
)
def test_selected_recovery_uses_matching_native_completion(details, ended_at, outcome, expected_error):
    history: list[dict[str, Any]] = [
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {
                "RequestId": "different-request",
                "StartTimestamp": 0.002,
                "EndTimestamp": 0.100,
            },
        }
    ]
    if details is not None:
        history.append(
            {
                "EventType": "InvocationCompleted",
                "EventTimestamp": 0.080,
                "InvocationCompletedDetails": {"RequestId": "r1", "StartTimestamp": 0.040, **details},
            }
        )
    ctx, case_sources = sources(
        [raw_record("WRAPPER_ENTER", 1, time=0.050)],
        invocations=[{"marker": "m", "arn": "exec", "payload": {"scenario": "recovery"}}],
        request_roles={"r1": "recovery"},
        raw_histories={"exec": history},
    )
    ctx.scenario = replace(
        ctx.scenario, requirement_id="lmi-lifecycle-4", scenario="peer-recovery", roles=("victim", "peer", "recovery")
    )
    ctx.record_attempt("r1", role="recovery", requestId="r1")
    attempts, selected, errors = case_sources.attempts(
        [{"kind": "wrapper_return", "requestId": "r1", "atMs": 60, "outcome": "SUCCEEDED"}]
    )
    assert selected == {"recovery": "r1"}
    assert attempts[0]["endedAtMs"] == ended_at
    assert attempts[0]["outcome"] == outcome
    recovery_errors = [error for error in errors if "request r1" in error]
    if expected_error:
        assert len(recovery_errors) == 1 and expected_error in recovery_errors[0]
    else:
        assert recovery_errors == []


@pytest.mark.parametrize("native_end", [None, 0.055])
def test_selected_peer_uses_native_completion_when_available(native_end):
    details = {"RequestId": "r1", "StartTimestamp": 0.040, "EndTimestamp": native_end}
    history = [{"EventType": "InvocationCompleted", "InvocationCompletedDetails": details}] if native_end else []
    ctx, case_sources = sources(
        [raw_record("WRAPPER_ENTER", 1, time=0.050)],
        invocations=[{"marker": "m", "arn": "exec", "payload": {"scenario": "barrier"}}],
        request_roles={"r1": "peer"},
        raw_histories={"exec": history},
    )
    ctx.scenario = replace(
        ctx.scenario, requirement_id="lmi-lifecycle-4", scenario="peer-recovery", roles=("victim", "peer", "recovery")
    )
    ctx.record_attempt("r1", role="peer", requestId="r1")
    attempts, selected, errors = case_sources.attempts(
        [{"kind": "wrapper_return", "requestId": "r1", "atMs": 70, "outcome": "SUCCEEDED"}]
    )
    assert selected == {"peer": "r1"}
    assert attempts[0]["endedAtMs"] == (55 if native_end else 70)
    assert errors == []


@pytest.mark.parametrize("release_gate", ["victim-gate", "release-all"])
@pytest.mark.parametrize("state", ["PENDING", "ACKNOWLEDGED", "missing-intent"])
def test_recovery_fault_retains_first_release_intent_after_retry(tmp_path, release_gate, state):
    from types import SimpleNamespace
    from unittest.mock import patch

    from .support import build_evidence
    from .test_shared_partial_evidence import context

    ctx = context()
    ctx.scenario = replace(
        ctx.scenario, requirement_id="lmi-lifecycle-4", scenario="peer-recovery", roles=("victim", "peer", "recovery")
    )
    records: list[dict[str, Any]] = [{"action": "hold", "gate": "victim-gate"}]
    if state == "missing-intent":
        records.append({"action": "release", "gate": release_gate, "time": 0.060, "state": "ACKNOWLEDGED"})
    else:
        records.append({"action": "release", "gate": release_gate, "started": 0.050, "state": "PENDING"})
        if state == "ACKNOWLEDGED":
            records.append({"action": "release", "gate": release_gate, "started": 0.070, "time": 0.080, "state": state})
    cloud = SimpleNamespace(
        manifest={"invocationTimeout": 60, "bucket": "private"},
        control_prefix="runs/run/cases/case/control/",
        observation_ended_at=1,
        source_uris=set(),
        control_records=records,
        invocations=[{"role": "victim", "payload": {"gate": "victim-gate"}}],
        _save=lambda *args: None,
        artifacts_dir=tmp_path,
    )
    sources = SimpleNamespace(
        observations=lambda: ([], []), histories=lambda: ([], {}, []), attempts=lambda _: ([], {}, [])
    )
    with patch("aws_durable_execution_conformance_tests.lmi.source_evidence.CaseSources.capture", return_value=sources):
        evidence = build_evidence(ctx, cloud)
    fault = evidence["faultsCreated"][0]
    if state == "missing-intent":
        assert "releaseRequestedAtMs" not in fault
        assert "Fault victim-gate release is missing a request timestamp" in evidence["collectionErrors"]
        assert fault["releasedAtMs"] == fault["releaseAcknowledgedAtMs"] == 60
    else:
        assert fault["releaseRequestedAtMs"] == 50
        if state == "ACKNOWLEDGED":
            assert fault["releasedAtMs"] == fault["releaseAcknowledgedAtMs"] == 80
        else:
            assert "releasedAtMs" not in fault
            assert "releaseAcknowledgedAtMs" not in fault
