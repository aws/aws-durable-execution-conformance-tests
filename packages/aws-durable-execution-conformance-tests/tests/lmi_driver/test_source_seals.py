# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Source coverage is a positive witness, separate from visible work pairing."""

from types import SimpleNamespace
from typing import Any, cast

import pytest

from aws_durable_execution_conformance_tests.lmi.evidence import validate_retirement_evidence
from aws_durable_execution_conformance_tests.lmi.source_evidence import ledger_seal_status, requests_after_cutoff

from .support import ScenarioDriver, build_evidence
from .test_shared_adapter import context

LAUNCHES = [{"arn": "execution", "marker": "marker", "native_requests": ["native"]}]


def raw_stream(*observations):
    scope = {
        "request": "native",
        "execution": "execution",
        "marker": "marker",
        "environment": "environment",
        "process": "process",
        "run": "run",
        "caseId": "case",
        "commit": "commit",
    }
    records = [
        {**scope, "phase": "WRAPPER_ENTER", "sequence": 1, "time": 0.1},
        *(
            {
                **scope,
                "phase": "CONFORMANCE",
                "sequence": capture + 1,
                "time": 2,
                "observation": {**event, "captureSequence": capture},
            }
            for capture, event in enumerate(observations, 1)
        ),
        {
            **scope,
            "phase": "SEALED",
            "sequence": len(observations) + 2,
            "time": 2,
            "sourceProtocol": 2,
            "throughCapture": len(observations),
        },
    ]
    return records


def cloud_source(tmp_path, *extra):
    ctx = context(scenario="normal-return", roles=("victim",))
    ctx.record_attempt("native", role="victim", requestId="native")
    records = raw_stream(
        {"kind": "wrapper_enter", "atMs": 100},
        {"kind": "task_start", "taskId": "root", "atMs": 110},
        {"kind": "task_exit", "taskId": "root", "atMs": 300},
        {"kind": "wrapper_return", "outcome": "SUCCEEDED", "atMs": 400},
        *extra,
    )
    cloud = SimpleNamespace(
        manifest={"bucket": "private", "commit": "commit", "invocationTimeout": 60},
        observation_ended_at=1,
        retired_at=3,
        request_roles={"native": "victim"},
        events={r["sequence"]: r for r in records},
        raw_histories={
            "execution": [
                {"EventId": 1, "EventType": "ExecutionStarted", "EventTimestamp": 0.05},
                {"EventId": 2, "EventType": "ExecutionSucceeded", "EventTimestamp": 0.45},
                {
                    "EventId": 3,
                    "EventType": "InvocationCompleted",
                    "InvocationCompletedDetails": {
                        "RequestId": "native",
                        "StartTimestamp": 0.1,
                        "EndTimestamp": 0.5,
                    },
                },
            ]
        },
        terminal_histories={"execution"},
        execution_results={"execution": {"Status": "SUCCEEDED", "Result": '"marker"', "EndTimestamp": 0.45}},
        invocations=[{"arn": "execution", "marker": "marker", "native_requests": ["native"]}],
        source_uris={"memory://ledger"},
        control_records=[],
        artifacts_dir=tmp_path,
        _save=lambda *args: None,
    )
    return ctx, cast("Any", cloud)


def test_seal_is_required_even_when_service_reports_native_completion(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    cloud.events.pop(max(cloud.events))
    evidence = build_evidence(ctx, cloud)
    assert evidence["collection"]["watermarks"] == {"history": 1000}
    assert evidence["collectionErrors"] == []
    assert any("Missing ledger seals" in pending for pending in evidence["collection"]["pending"])

    final = build_evidence(ctx, cloud, phase="final")
    assert final["collection"]["watermarks"] == {}
    assert any("Missing ledger seals" in error for error in final["collectionErrors"])
    assert validate_retirement_evidence(final)


@pytest.mark.parametrize(
    "problem",
    [
        "wrong-version",
        "trailing-record",
        "duplicate-seal",
        "capture-gap",
        "publication-gap",
        "wrong-arn",
        "wrong-marker",
    ],
)
def test_malformed_seals_fail_closed(problem):
    records = raw_stream({"kind": "task_exit", "atMs": 500})
    seal = records[-1]
    if problem == "wrong-version":
        seal["sourceProtocol"] = 1
    elif problem == "trailing-record":
        records.append({**seal, "phase": "RAW", "sequence": seal["sequence"] + 1})
    elif problem == "duplicate-seal":
        records.append({**seal, "sequence": seal["sequence"] + 1})
    elif problem == "capture-gap":
        seal["throughCapture"] = 2
    elif problem == "wrong-arn":
        seal["execution"] = "wrong"
    elif problem == "wrong-marker":
        seal["marker"] = "wrong"
    else:
        seal["sequence"] += 1
    _, missing, errors = ledger_seal_status(records, {}, LAUNCHES, set())
    assert not missing and errors


def test_service_discovered_retry_needs_its_own_seal():
    records = raw_stream({"kind": "task_exit", "atMs": 500})
    service = {
        "execution": [
            {"InvocationCompletedDetails": {"RequestId": "native"}},
            {"InvocationCompletedDetails": {"RequestId": "unpublished-retry"}},
        ]
    }
    assert ledger_seal_status(records, service, LAUNCHES, set()) == (
        {"native", "unpublished-retry"},
        {"unpublished-retry"},
        [],
    )


@pytest.mark.parametrize("service_state", ["running", "stale-terminal", "history-gap", "no-terminal-event"])
def test_matching_ledger_seals_do_not_certify_unproven_service_history(tmp_path, service_state):
    ctx, cloud = cloud_source(tmp_path)
    if service_state == "running":
        cloud.execution_results["execution"]["Status"] = "RUNNING"
    elif service_state == "stale-terminal":
        cloud.terminal_histories.clear()
    elif service_state == "history-gap":
        cloud.raw_histories["execution"][-1]["EventId"] = 10
    else:
        cloud.raw_histories["execution"][1]["EventType"] = "StepSucceeded"
    evidence = build_evidence(ctx, cloud)
    assert evidence["collection"]["watermarks"] == {"lifecycle": 1000, "sdkRequests": 1000}
    assert evidence["collectionErrors"] == []
    assert any("Terminal service history" in pending for pending in evidence["collection"]["pending"])


def test_late_publication_and_late_activity_use_occurrence_time(tmp_path):
    ctx, cloud = cloud_source(
        tmp_path,
        {"kind": "checkpoint_call", "callId": "late", "atMs": 1100},
        {"kind": "checkpoint_exit", "callId": "late", "atMs": 1200},
    )
    before = build_evidence(ctx, cloud)
    assert before["collectionErrors"] == []
    assert before["collection"]["watermarks"] == {"lifecycle": 1000, "sdkRequests": 1000, "history": 1000}
    assert before["sdkRequests"] == []
    assert {event["kind"] for event in before["events"]} == {
        "wrapper_enter",
        "wrapper_return",
        "task_start",
        "task_exit",
    }
    assert len(cloud.events) == 8

    cloud.observation_ended_at = 2
    final = build_evidence(ctx, cloud, phase="final")
    assert final["collectionErrors"] == []
    assert [row["callId"] for row in final["sdkRequests"]] == ["late"]


def test_verified_launch_inventory_survives_missing_lifecycle_attempt(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    cloud.events.clear()
    cloud.invocations[0]["native_requests"].clear()
    cloud.raw_histories["execution"].append(
        {"EventType": "CallbackStarted", "Id": "callback-operation", "EventTimestamp": 0.2}
    )
    cloud.control_records = [{"gate": "never-created", "action": "hold", "state": "REJECTED"}]

    evidence = build_evidence(ctx, cloud, phase="partial")

    assert not evidence["attempts"]
    assert evidence["resourcesCreated"] == [
        {"kind": "invocation", "id": "native", "ownerCaseId": "case"},
        {"kind": "execution", "id": "execution", "ownerCaseId": "case"},
        {
            "kind": "callback",
            "id": "execution/callback/callback-operation",
            "ownerCaseId": "case",
            "executionArn": "execution",
            "operationId": "callback-operation",
        },
    ]
    assert any("has no lifecycle ledger stream" in pending for pending in evidence["collection"]["pending"])


def test_integral_json_ordinals_are_accepted_in_a_ledger_seal():
    records = raw_stream({"kind": "task_exit", "atMs": 500})
    for record in records:
        record["sequence"] = float(record["sequence"])
    records[1]["observation"]["captureSequence"] = 1e0
    records[-1].update(throughCapture=1e0, sourceProtocol=2e0)
    assert ledger_seal_status(records, {}, LAUNCHES, set()) == ({"native"}, set(), [])


@pytest.mark.parametrize("details", [None, {}, {"RequestId": []}])
def test_all_raw_and_service_native_requests_are_audited(tmp_path, details):
    ctx, cloud = cloud_source(tmp_path)
    cloud.events["orphan"] = {**cloud.events[1], "phase": "BLOCKED", "request": "unentered-request"}
    cloud.raw_histories["execution"].append({"EventType": "InvocationCompleted", "InvocationCompletedDetails": details})
    snapshot = build_evidence(ctx, cloud, phase="final")
    assert snapshot["collection"]["watermarks"] == {}
    assert any("unentered-request" in message for message in snapshot["collectionErrors"])
    assert any("has no request ID" in message for message in snapshot["collectionErrors"])


@pytest.mark.parametrize("request_fields", [{}, {"request": []}])
def test_malformed_wrapper_and_truncated_payload_do_not_erase_owned_inventory(tmp_path, request_fields):
    ctx, cloud = cloud_source(tmp_path)
    malformed = {key: value for key, value in cloud.events[1].items() if key != "request"}
    cloud.events["malformed"] = {**malformed, **request_fields}
    cloud.raw_histories["execution"].extend(
        [
            {
                "EventType": "StepSucceeded",
                "Id": "truncated-step",
                "EventTimestamp": 0.2,
                "StepSucceededDetails": {"Result": {"Payload": "prefix", "Truncated": True}},
            },
            {"EventType": "CallbackStarted", "Id": "callback-op", "EventTimestamp": 0.2},
        ]
    )
    partial = build_evidence(ctx, cloud, phase="partial")
    assert any("Invalid WRAPPER_ENTER identity" in error for error in partial["collectionErrors"])
    assert any("Truncated service data" in error for error in partial["collectionErrors"])
    operation = partial["histories"][0]["operations"][0]
    assert operation["id"] == "truncated-step" and operation["status"] == "SUCCEEDED"
    assert "result" not in operation
    assert {row["kind"] for row in partial["resourcesCreated"]} == {"invocation", "execution", "callback"}


def test_ledger_only_request_is_retained_only_for_a_verified_launch(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    cloud.events["owned"] = {**cloud.events[1], "phase": "BLOCKED", "request": "seen-without-wrapper"}
    cloud.events["foreign"] = {
        **cloud.events[1],
        "phase": "BLOCKED",
        "request": "another-launch",
        "marker": "other-marker",
    }
    final = build_evidence(ctx, cloud, phase="final")
    inventory = {row["id"]: row for row in final["retirement"]["resources"]}
    assert inventory["seen-without-wrapper"]["outcome"] == "UNKNOWN"
    assert "another-launch" not in inventory
    assert final["collection"]["watermarks"] == {}


def test_observation_scope_does_not_need_a_post_cutoff_retry_seal(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    cloud.events["future"] = {**cloud.events[1], "request": "future-retry", "time": 2}
    cloud.invocations[0]["native_requests"].append("future-retry")
    cloud.raw_histories["execution"].append(
        {
            "EventType": "InvocationCompleted",
            "EventId": 4,
            "InvocationCompletedDetails": {"RequestId": "future-retry", "StartTimestamp": 1.9, "EndTimestamp": 2.5},
        }
    )
    cloud.execution_results["execution"]["Status"] = "RUNNING"
    assert requests_after_cutoff(ctx.attempts, 1000) == set()
    ctx.record_attempt(
        "retry-attempt",
        role="victim",
        requestId="future-retry",
        executionArn="execution",
        source="service-observed",
        nativeStartedAtMs=1900,
    )
    excluded = requests_after_cutoff(ctx.attempts, 1000)
    assert excluded == {"future-retry"}
    assert ledger_seal_status(tuple(cloud.events.values()), cloud.raw_histories, cloud.invocations, excluded) == (
        {"native"},
        set(),
        [],
    )
    observation = build_evidence(ctx, cloud)
    assert observation["collectionErrors"] == []
    assert observation["collection"]["watermarks"] == {"lifecycle": 1000, "sdkRequests": 1000}
    assert "future" in cloud.events

    final = build_evidence(ctx, cloud, phase="final")
    assert any("future-retry" in message for message in final["collectionErrors"])
    assert final["collection"]["watermarks"] == {}


def test_malformed_task_id_does_not_hide_verified_resources(tmp_path):
    ctx, cloud = cloud_source(tmp_path, {"kind": "task_start", "taskId": [], "atMs": 410})
    cloud.raw_histories["execution"].append(
        {"EventType": "CallbackStarted", "Id": "callback-operation", "EventTimestamp": 0.2}
    )
    cloud.refresh = lambda **_: list(cloud.events.values())
    assert not ScenarioDriver(ctx, cloud).quiescent()
    evidence = build_evidence(ctx, cloud, phase="partial")
    assert any("native at sequence 6 has invalid taskId" in message for message in evidence["collectionErrors"])
    assert {row["kind"] for row in evidence["resourcesCreated"]} == {"invocation", "execution", "callback"}


@pytest.mark.parametrize("missing_end", [False, True])
@pytest.mark.parametrize(
    ("status", "request_at", "pause_at", "expected_status"),
    [
        ("RUNNING", None, None, "RUNNING"),
        ("PAUSED", 0.8, 0.9, "PAUSED"),
        ("PAUSED", 1.01, 1.02, "RUNNING"),
        ("PAUSED", 0.99, 1.01, None),
        ("PAUSING", None, None, None),
    ],
)
def test_postcutoff_terminal_proof_does_not_leak_stop_into_frozen_observations(
    tmp_path, missing_end, status, request_at, pause_at, expected_status
):
    ctx, cloud = cloud_source(tmp_path)
    ctx.scenario = ctx.scenario.__class__(
        "case",
        "lmi-lifecycle-3",
        ctx.profile.profile_id,
        "deadline-during-step",
        roles=("victim",),
        coverage_profile="standard",
    )
    cloud.invocations[0]["role"] = "victim"
    witnessed_at = max(1001, pause_at * 1000 + 1) if status == "PAUSED" and pause_at is not None else 1001
    cloud.cutoff_results = {"execution": {"Status": status, "observedAtMs": witnessed_at}}
    cloud.execution_results["execution"] = {
        "Status": "STOPPED",
        "Error": {"ErrorType": "ControlStop"},
        **({} if missing_end else {"EndTimestamp": 1.5}),
    }
    cloud.raw_histories["execution"][1].update(EventType="ExecutionStopped", EventTimestamp=1.5)
    cloud.raw_histories["execution"].append(
        {"EventType": "StepSucceeded", "EventId": 4, "Id": "later", "EventTimestamp": 1.3}
    )
    history = cloud.raw_histories["execution"]
    if pause_at is not None:
        history.insert(
            1,
            {
                "EventType": "ExecutionPaused",
                "EventTimestamp": pause_at,
                "ExecutionPausedDetails": {"PauseRequestTimestamp": request_at},
            },
        )
    for index, event in enumerate(history, 1):
        event["EventId"] = index
    observation = build_evidence(ctx, cloud)
    if expected_status is None:
        assert "history" not in observation["collection"]["watermarks"]
        assert any("does not establish the pause state" in reason for reason in observation["collection"]["pending"])
    else:
        assert observation["collection"]["watermarks"]["history"] == 1000
    assert observation["histories"][0]["status"] == expected_status
    assert observation["histories"][0]["operations"] == []
    assert "error" not in observation["histories"][0]

    cloud.observation_ended_at = 2
    final = build_evidence(ctx, cloud, phase="final")
    assert final["collection"]["watermarks"]["history"] == 2000
    assert final["histories"][0]["status"] == "STOPPED"


def test_cutoff_running_witness_cannot_certify_a_contradictory_terminal_end(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    ctx.scenario = ctx.scenario.__class__(
        "case",
        "lmi-lifecycle-3",
        ctx.profile.profile_id,
        "deadline-during-step",
        roles=("victim",),
        coverage_profile="standard",
    )
    cloud.invocations[0]["role"] = "victim"
    cloud.cutoff_results = {"execution": {"Status": "RUNNING", "observedAtMs": 1001}}
    observation = build_evidence(ctx, cloud)
    assert "history" not in observation["collection"]["watermarks"]
    assert observation["histories"][0]["status"] == "RUNNING"


def test_deadline_history_proof_does_not_substitute_for_a_missing_native_seal(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    ctx.scenario = ctx.scenario.__class__(
        "case",
        "lmi-lifecycle-3",
        ctx.profile.profile_id,
        "deadline-during-step",
        roles=("victim",),
        coverage_profile="standard",
    )
    cloud.invocations[0]["role"] = "victim"
    cloud.events.pop(max(cloud.events))
    cloud.cutoff_results = {"execution": {"Status": "RUNNING", "observedAtMs": 1001}}
    cloud.execution_results["execution"].update(Status="STOPPED", EndTimestamp=1.5)
    cloud.raw_histories["execution"][1].update(EventType="ExecutionStopped", EventTimestamp=1.5)
    observation = build_evidence(ctx, cloud)
    assert observation["collection"]["watermarks"] == {"history": 1000}
    assert any("Missing ledger seals" in pending for pending in observation["collection"]["pending"])


def test_observation_collection_rereads_stale_normal_return_history_without_stopping(tmp_path, monkeypatch):
    ctx, cloud = cloud_source(tmp_path)
    monkeypatch.setattr("aws_durable_execution_conformance_tests.lmi.scenarios.time", SimpleNamespace(time=lambda: 1))
    complete = cloud.raw_histories["execution"]
    calls = []
    cutoff = []

    def collect(*, full_run):
        assert not full_run
        calls.append("read")
        cutoff.append(cloud.observation_ended_at)
        cloud.raw_histories["execution"] = complete[:-1] if len(calls) == 1 else complete

    def poll(predicate, **options):
        assert not predicate()
        assert predicate()

    cloud.wait = lambda _: None
    cloud.collect = collect
    cloud.poll = poll
    cloud.stop_victim_after_cutoff = pytest.fail
    driver = ScenarioDriver(ctx, cloud)
    driver.collect()
    assert calls == ["read", "read"]
    assert cutoff == [1, 1]
    assert build_evidence(ctx, cloud)["collection"]["watermarks"]["history"] == 1000


def test_first_attempt_for_each_role_is_preserved_even_with_late_service_start():
    starts = [
        {
            "requestId": "first-resume",
            "attempt": 1,
            "source": "service-observed",
            "executionArn": "exec",
            "nativeStartedAtMs": 1100,
        },
        {
            "requestId": "wrapper-only",
            "attempt": 2,
            "source": "service-observed",
            "executionArn": "exec",
            "launchedAtMs": 1100,
        },
        {
            "requestId": "script-launch",
            "attempt": 2,
            "source": "driver-invoke",
            "executionArn": "exec",
            "nativeStartedAtMs": 1100,
        },
    ]
    assert requests_after_cutoff(starts, 1000) == set()


def test_partial_observation_at_the_cutoff_does_not_include_driver_stop(tmp_path):
    ctx, cloud = cloud_source(tmp_path, {"kind": "task_exit", "taskId": "late", "atMs": 1100})
    ctx.scenario = ctx.scenario.__class__(
        "case",
        "lmi-lifecycle-3",
        ctx.profile.profile_id,
        "deadline-during-step",
        roles=("victim",),
        coverage_profile="standard",
    )
    cloud.invocations[0]["role"] = "victim"
    cloud.cutoff_results = {"execution": {"Status": "RUNNING", "observedAtMs": 1001}}
    cloud.execution_results["execution"].update(Status="STOPPED", EndTimestamp=1.5)
    cloud.raw_histories["execution"][1].update(EventType="ExecutionStopped", EventTimestamp=1.5)
    before_release = build_evidence(ctx, cloud, phase="partial")
    assert before_release["observationEndedAtMs"] == 1000
    assert before_release["histories"][0]["status"] == "RUNNING"
    assert not any(event.get("taskId") == "late" for event in before_release["events"])
    assert before_release["collection"]["watermarks"] == {}

    physical = build_evidence(ctx, cloud, phase="retirement-partial")
    assert physical["histories"][0]["status"] == "STOPPED"
    assert any(event.get("taskId") == "late" for event in physical["events"])
    assert physical["collection"]["watermarks"] == {}


@pytest.mark.parametrize(
    "operations",
    [
        None,
        [None],
        [{"operationType": "STEP", "status": "SUCCEEDED"}],
        [{"operationId": "id", "operationType": "STEP", "status": "SUCCEEDED", "name": []}],
    ],
)
def test_malformed_replay_checkpoint_does_not_hide_owned_resources(tmp_path, operations):
    ctx, cloud = cloud_source(tmp_path, {"kind": "invocation_start", "atMs": 110, "operations": operations})
    ctx.scenario = ctx.scenario.__class__(
        "case",
        "lmi-lifecycle-5",
        ctx.profile.profile_id,
        "suspension-retry",
        roles=("victim", "resume"),
        coverage_profile="standard",
    )
    ctx.attempts[0]["role"] = "resume"
    cloud.request_roles["native"] = "resume"
    cloud.raw_histories["execution"].append(
        {"EventType": "CallbackStarted", "Id": "callback-operation", "EventTimestamp": 0.2}
    )
    snapshot = build_evidence(ctx, cloud, phase="partial")
    assert not snapshot["collectionErrors"]
    assert any("Replay request native has a malformed" in error for error in snapshot["replay"]["projectionErrors"])
    assert {row["kind"] for row in snapshot["resourcesCreated"]} == {"invocation", "execution", "callback"}


def test_unpublished_post_cutoff_service_retry_is_included_only_in_retirement(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    ctx.attempts[0]["executionArn"] = "execution"
    cloud.invocations[0].update(role="victim", resume_roles=())
    cloud.raw_histories["execution"].append(
        {
            "EventType": "InvocationCompleted",
            "EventId": 4,
            "InvocationCompletedDetails": {"RequestId": "future", "StartTimestamp": 1.1, "EndTimestamp": 1.3},
        }
    )
    cloud.execution_results["execution"]["Status"] = "RUNNING"
    observed = build_evidence(ctx, cloud)
    assert observed["collection"]["watermarks"] == {"lifecycle": 1000, "sdkRequests": 1000}
    assert "future" not in {item["id"] for item in observed["resourcesCreated"]}
    final = build_evidence(ctx, cloud, phase="final")
    assert final["collection"]["watermarks"] == {}
    assert any("future" in error for error in final["collectionErrors"])

    # An as-yet unbound first resume is not an ignorable retry.
    cloud.invocations[0]["resume_roles"] = ("resume",)
    observed = build_evidence(ctx, cloud)
    assert observed["collection"]["watermarks"] == {}


def test_undated_operation_is_not_placed_in_the_frozen_window(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    cloud.raw_histories["execution"].append(
        {
            "EventType": "CallbackStarted",
            "Id": "undated",
            "EventId": 4,
        }
    )
    frozen = build_evidence(ctx, cloud)
    assert "history" not in frozen["collection"]["watermarks"]
    assert not frozen["histories"][0]["operations"]
    assert "execution/callback/undated" not in {item["id"] for item in frozen["resourcesCreated"]}
    retired = build_evidence(ctx, cloud, phase="final")
    assert retired["histories"][0]["operations"][0]["id"] == "undated"
    assert "execution/callback/undated" in {item["id"] for item in retired["resourcesCreated"]}


@pytest.mark.parametrize(
    ("mode", "suspended", "keeps_checkpoint", "repeats_completed"),
    [
        ("wait", False, True, False),
        ("wait", True, True, False),
        ("wait", True, False, False),
        ("wait", True, True, True),
        ("callback", True, True, False),
        ("callback", True, False, False),
        ("callback", False, True, False),
    ],
)
def test_builder_checks_the_public_suspension_checkpoint_and_service_wakeup_tie(
    tmp_path, mode, suspended, keeps_checkpoint, repeats_completed
):
    ctx, cloud = cloud_source(tmp_path)
    ctx.scenario = ctx.scenario.__class__(
        "case",
        "lmi-lifecycle-5",
        ctx.profile.profile_id,
        "suspension-retry",
        roles=("victim", "resume"),
        coverage_profile="standard",
    )
    cloud.suspension_mode = mode
    cloud.observation_ended_at = 2
    cloud.invocations[0]["native_requests"].append("resume")
    cloud.request_roles["resume"] = "resume"
    ctx.record_attempt("resume", role="resume", requestId="resume", executionArn="execution")
    victim = raw_stream(
        {"kind": "wrapper_enter", "atMs": 100},
        {"kind": "task_start", "taskId": "root", "atMs": 110},
        {"kind": "invocation_end", "outcome": "PENDING", "atMs": 300},
        {"kind": "task_exit", "taskId": "root", "atMs": 350},
        {"kind": "wrapper_return", "outcome": "PENDING", "atMs": 400},
    )
    victim.insert(-1, {**victim[0], "phase": "WRAPPER_RETURN", "status": "PENDING", "time": 0.4})
    for seq, row in enumerate(victim, 1):
        row["sequence"] = seq
    prefix = "Wait" if mode == "wait" else "Callback"
    checkpoint = {"operationId": "operation", "operationType": prefix.upper(), "status": "SUCCEEDED"}
    if mode == "callback":
        checkpoint.update(name="completion", serializedResult="marker")
    checkpoints = [checkpoint] if suspended and keeps_checkpoint else []
    resume = raw_stream(
        {"kind": "wrapper_enter", "atMs": 600},
        {"kind": "invocation_start", "operations": checkpoints, "atMs": 610},
        {"kind": "task_start", "taskId": "root", "atMs": 620},
        *(
            [{"kind": "body", "operation": "already completed", "operationId": "operation", "attempt": 1, "atMs": 630}]
            if repeats_completed
            else []
        ),
        {"kind": "task_exit", "taskId": "root", "atMs": 700},
        {"kind": "wrapper_return", "outcome": "SUCCEEDED", "atMs": 710},
    )
    for row in resume:
        row["request"] = "resume"
    resume[0]["time"] = 0.6
    cloud.events = dict(enumerate([*victim, *resume]))
    history: list[dict[str, Any]] = [
        {"EventType": "ExecutionStarted", "EventTimestamp": 0.05},
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {
                "RequestId": "native",
                "StartTimestamp": 0.1,
                "EndTimestamp": 0.5,
            },
        },
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {
                "RequestId": "resume",
                "StartTimestamp": 0.6,
                "EndTimestamp": 0.75,
            },
        },
        {"EventType": "ExecutionSucceeded", "EventTimestamp": 0.74},
    ]
    if suspended or mode == "callback":
        start = {"EventType": prefix + "Started", "Id": "operation", "EventTimestamp": 0.2 if suspended else 0.61}
        end: dict[str, Any] = {
            "EventType": prefix + "Succeeded",
            "Id": "operation",
            "EventTimestamp": 0.6 if suspended else 0.62,
        }
        if repeats_completed:
            start["Name"] = end["Name"] = checkpoint["name"] = "already completed"
        if mode == "callback":
            start["Name"] = end["Name"] = "completion"
            end["CallbackSucceededDetails"] = {"Result": {"Payload": "marker"}}
        history[2:2] = [start, end]
    for index, row in enumerate(history, 1):
        row["EventId"] = index
    cloud.raw_histories["execution"] = history
    cloud.execution_results["execution"]["EndTimestamp"] = 0.74
    evidence = build_evidence(ctx, cloud)
    assert evidence["collectionErrors"] == []
    if mode == "wait":
        assert evidence["replay"]["durableWaitObserved"] is suspended
    assert evidence["replay"]["suspensionCheckpointMatches"] is (
        suspended and keeps_checkpoint and not repeats_completed
    )


@pytest.mark.parametrize("old_callback", [False, True, "stale-history"])
def test_terminal_callback_without_resume_preserves_physical_retirement(tmp_path, monkeypatch, old_callback):
    from unittest.mock import Mock

    ctx, cloud = cloud_source(tmp_path)
    ctx.scenario = ctx.scenario.__class__(
        "case",
        "lmi-lifecycle-5",
        ctx.profile.profile_id,
        "suspension-retry",
        roles=("victim", "resume"),
        coverage_profile="standard",
    )
    actor = cloud.invocations[0]
    actor.update(role="victim", runner=Mock())
    cloud.history = lambda _: cloud.raw_histories["execution"]
    cloud.for_item = lambda _: list(cloud.events.values())
    cloud.collect = lambda **_: None
    cloud.poll = lambda predicate, **_: predicate()
    candidate = Mock(return_value="expired")
    if old_callback:
        monkeypatch.setattr("aws_durable_execution_conformance_tests.lmi.scenarios.pending_callback", candidate)
    if old_callback == "stale-history":
        original = cloud.raw_histories["execution"]
        stale = [row for row in original if row.get("EventType") != "ExecutionSucceeded"]
        snapshots = iter([original, stale])
        cloud.history = lambda _: next(snapshots)

        def poll(predicate, **_):
            assert not predicate()
            assert predicate()

        cloud.poll = poll
        from aws_durable_execution_conformance_tests.lmi import scenarios

        actual_seals = scenarios.ledger_seal_status
        seals = iter(
            [
                (set(), {"missing"}, []),
                actual_seals(tuple(cloud.events.values()), cloud.raw_histories, cloud.invocations, set()),
            ]
        )
        with monkeypatch.context() as scoped:
            scoped.setattr(scenarios, "ledger_seal_status", lambda *args: next(seals))
            ScenarioDriver(ctx, cloud)._callback(actor)
    else:
        ScenarioDriver(ctx, cloud)._callback(actor)
    candidate.assert_not_called()
    actor["runner"].send_callback_success.assert_not_called()
    final = build_evidence(ctx, cloud, phase="final")
    assert final["collectionErrors"] == []
    assert validate_retirement_evidence(final) == []


def test_failed_native_pending_does_not_authorize_callback():
    from aws_durable_execution_conformance_tests.lmi.observations import pending_callback

    ledger: list[dict[str, Any]] = [
        {"phase": "WRAPPER_ENTER", "request": "request"},
        {"phase": "WRAPPER_RETURN", "request": "request", "status": "PENDING", "time": 1},
    ]
    completion = {
        "RequestId": "request",
        "StartTimestamp": 0,
        "EndTimestamp": 1.1,
        "Error": {"Payload": {"ErrorType": "Runtime.ExitError"}},
    }
    history: list[dict[str, Any]] = [
        {
            "EventType": "CallbackStarted",
            "Name": "completion",
            "Id": "callback",
            "EventTimestamp": 0.9,
            "CallbackStartedDetails": {"CallbackId": "token"},
        },
        {"EventType": "InvocationCompleted", "InvocationCompletedDetails": completion},
    ]
    assert pending_callback(ledger, history, "completion") is None
    del completion["Error"]
    assert pending_callback(ledger, history, "completion") == "token"


def test_service_proven_late_retry_with_a_scoped_nonwrapper_row_is_deferred(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    ctx.attempts[0]["executionArn"] = "execution"
    cloud.invocations[0].update(role="victim", resume_roles=())
    cloud.raw_histories["execution"].append(
        {
            "EventType": "InvocationCompleted",
            "EventId": 4,
            "InvocationCompletedDetails": {"RequestId": "future", "StartTimestamp": 1.1, "EndTimestamp": 1.3},
        }
    )
    cloud.events["future"] = {**cloud.events[1], "phase": "BLOCKED", "request": "future", "time": 1.2}
    observed = build_evidence(ctx, cloud)
    assert "lifecycle" in observed["collection"]["watermarks"]
    assert any("future" in error for error in build_evidence(ctx, cloud, phase="final")["collectionErrors"])

    cloud.events["future"]["marker"] = "wrong-launch"
    observed = build_evidence(ctx, cloud)
    assert "lifecycle" not in observed["collection"]["watermarks"]


def test_old_pause_cycle_cannot_identify_a_new_pausing_witness(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    ctx.scenario = ctx.scenario.__class__(
        "case",
        "lmi-lifecycle-3",
        ctx.profile.profile_id,
        "deadline-during-step",
        roles=("victim",),
        coverage_profile="standard",
    )
    cloud.invocations[0]["role"] = "victim"
    cloud.cutoff_results = {"execution": {"Status": "PAUSING", "observedAtMs": 1001}}
    cloud.execution_results["execution"].update(Status="STOPPED", EndTimestamp=1.5)
    history = cloud.raw_histories["execution"]
    history[1].update(EventType="ExecutionStopped", EventTimestamp=1.5)
    history[1:1] = [
        {
            "EventType": "ExecutionPaused",
            "EventTimestamp": 0.7,
            "ExecutionPausedDetails": {"PauseRequestTimestamp": 0.6},
        },
        {"EventType": "ExecutionResumed", "EventTimestamp": 0.8},
    ]
    for index, event in enumerate(history, 1):
        event["EventId"] = index
    observed = build_evidence(ctx, cloud)
    assert observed["histories"][0]["status"] is None
    assert "history" not in observed["collection"]["watermarks"]


def test_shared_run_rejects_missing_revision_before_admission(tmp_path, monkeypatch):
    from unittest.mock import Mock

    from aws_durable_execution_conformance_tests.lmi import app
    from aws_durable_execution_conformance_tests.lmi.runner import run

    factory = Mock(return_value={"sdk": {"deployment": {}}})
    admission = Mock()
    monkeypatch.setattr(app, "live_results", admission)
    output = tmp_path / "output"
    with pytest.raises(ValueError, match="verified conformanceRevision"):
        run({}, output, manifest_factory=factory, adapter_name="unused:adapter")
    factory.assert_called_once()
    admission.assert_not_called()


def test_sealed_stream_without_wrapper_is_an_observation_error(tmp_path):
    ctx, cloud = cloud_source(tmp_path)
    wrapper = next(key for key, row in cloud.events.items() if row["phase"] == "WRAPPER_ENTER")
    del cloud.events[wrapper]

    evidence = build_evidence(ctx, cloud)

    assert any("ledger seal but no wrapper entry" in error for error in evidence["collectionErrors"])
    assert not evidence["collection"]["watermarks"]
