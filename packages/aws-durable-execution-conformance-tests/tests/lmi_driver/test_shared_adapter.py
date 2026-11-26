# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Case ownership and native-request journaling of the AWS transport, and suspension resumption."""

import io
import json
from dataclasses import replace
from typing import Any
from unittest.mock import Mock

import pytest

from aws_durable_execution_conformance_tests.lmi import cloud as cloud_module
from aws_durable_execution_conformance_tests.lmi.coordinator import (
    Profile,
    Scenario,
    ScenarioContext,
)
from aws_durable_execution_conformance_tests.lmi.errors import (
    CollectionError,
    ProvisioningError,
)

from .support import RUNTIME

FUNCTION = "arn:aws:lambda:us-west-2:123456789012:function:lmi:$LATEST.PUBLISHED"
EXECUTION = FUNCTION + "/durable-execution/name/id"


def context(
    case_id="case",
    scenario="suspension-retry",
    roles=("victim", "resume"),
    *,
    native=1,
):
    profile = Profile(f"native-{native}", FUNCTION, native, native, "sdk-sha", "configuration-sha")
    return ScenarioContext(
        "run",
        Scenario(
            case_id,
            "lmi-lifecycle-5",
            profile.profile_id,
            scenario,
            roles=roles,
            coverage_profile="standard",
        ),
        profile,
    )


def deployment():
    return {
        **RUNTIME.to_manifest(),
        "functions": {"c1": FUNCTION, "c2": FUNCTION + "-c2"},
        "concurrencies": {"c1": 1, "c2": 2},
        "region": "us-west-2",
        "run": "run",
        "bucket": "private-evidence",
        "commit": "sdk-sha",
        "runtime": "python3.14",
        "role": "role",
        "provider": "provider",
        "codeSha256": "code",
        "invocationTimeout": 60,
        "executionTimeout": 240,
        "driverTimeout": 1,
    }


@pytest.fixture
def cloud(monkeypatch, tmp_path):
    clients = {name: Mock() for name in ("lambda", "s3", "logs")}
    clients["lambda"].invoke.side_effect = lambda **kwargs: {
        "StatusCode": 202,
        "DurableExecutionArn": f"{kwargs['FunctionName']}/durable-execution/{kwargs['DurableExecutionName']}/id",
    }
    clients["s3"].put_object.return_value = {"ResponseMetadata": {"RetryAttempts": 0}}
    clients["s3"].get_paginator.return_value.paginate.return_value = []
    return cloud_module.Cloud(
        deployment(),
        "c1",
        artifacts_dir=tmp_path / "case",
        case_context=context(),
        verify_profile=Mock(),
        client_factory=lambda service, region: clients[service],
    )


def enter(item, request, at):
    return {
        "phase": "WRAPPER_ENTER",
        "marker": item["marker"],
        "request": request,
        "execution": item.get("arn") or EXECUTION,
        "time": at,
        "sequence": 0,
        "run": "run",
        "commit": "sdk-sha",
        "runtime": "python3.14",
        "role": "role",
        "provider": "provider",
        "codeSha256": "code",
        "invocationTimeout": 60,
        "executionTimeout": 240,
    }


def refresh_entries(cloud, *events):
    cloud.events.update({f"events/{event['request']}": event for event in events})
    return cloud.refresh()


def test_direct_launch_is_journaled_before_public_runner_transport(cloud):
    observed = []

    def invoke(**kwargs):
        observed.append(kwargs)
        assert len(cloud.case_context.attempts) == 1
        assert "requestId" not in cloud.case_context.attempts[0]
        return {
            "StatusCode": 202,
            "DurableExecutionArn": f"{kwargs['FunctionName']}/durable-execution/{kwargs['DurableExecutionName']}/id",
        }

    cloud.lam.invoke.side_effect = invoke
    item = cloud.start(
        "replay",
        role="victim",
        resume_roles=("resume",),
        dispatcher_payload=cloud.case_context.dispatcher("victim"),
    )
    payload = json.loads(observed[0]["Payload"])
    assert observed[0]["FunctionName"] == FUNCTION
    assert payload["scenario"] == "replay"
    assert payload["sharedScenario"] == "suspension-retry"
    assert payload["caseId"] == "case"
    assert payload["controlPrefix"] == cloud.control_prefix
    assert "/cases/" in cloud.control_prefix
    assert item["native_requests"] == []
    refresh_entries(cloud, enter(item, "original", 1))
    assert cloud.case_context.attempts[0]["requestId"] == "original"


@pytest.mark.parametrize("coverage_profile", ["standard", "sdk-regression"])
@pytest.mark.parametrize("payload_profile", ["admitted", "substituted", "absent"])
def test_direct_launch_uses_the_admitted_coverage_profile(cloud, coverage_profile, payload_profile):
    cloud.case_context.scenario = replace(
        cloud.case_context.scenario,
        coverage_profile=coverage_profile,
        behavior_budgets={"settleMs": 10},
    )
    payload = cloud.case_context.dispatcher("victim")
    if payload_profile == "substituted":
        payload["coverageProfile"] = "another-profile"
    elif payload_profile == "absent":
        payload.pop("coverageProfile", None)
    item = cloud.start(
        "replay",
        role="victim",
        resume_roles=("resume",),
        dispatcher_payload=payload,
    )
    invoked = json.loads(cloud.lam.invoke.call_args.kwargs["Payload"])
    retained = json.loads((cloud.artifacts_dir / f"invocations/{item['marker']}.json").read_text())
    for observed in (invoked, item["payload"], retained["payload"]):
        assert observed["coverageProfile"] == coverage_profile
        assert observed["behaviorBudgets"] == {"settleMs": 10}
    assert cloud.case_context.scenario.coverage_profile == coverage_profile


def test_service_replays_are_journaled_without_another_invoke(cloud):
    item = cloud.start("replay", role="victim", resume_roles=("resume",))
    refresh_entries(
        cloud,
        enter(item, "first", 1),
        enter(item, "second", 2),
        enter(item, "third", 3),
    )
    assert cloud.lam.invoke.call_count == 1
    assert [(attempt["role"], attempt["attempt"], attempt["requestId"]) for attempt in cloud.case_context.attempts] == [
        ("victim", 1, "first"),
        ("resume", 1, "second"),
        ("resume", 2, "third"),
    ]
    assert cloud.request_roles == {
        "first": "victim",
        "second": "resume",
        "third": "resume",
    }
    cloud.refresh()
    assert len(cloud.case_context.attempts) == 3


def test_lost_invoke_response_retains_launch_and_recovers_identity(cloud):
    cloud.lam.invoke.side_effect = TimeoutError("response lost")
    with pytest.raises(CollectionError, match="API failed"):
        cloud.start("success", role="victim")
    item = cloud.invocations[0]
    assert item["arn"] is None
    record = json.loads((cloud.artifacts_dir / f"invocations/{item['marker']}.json").read_text())
    assert "response lost" in record["error"]
    event = enter(item, "original", 1)
    with pytest.raises(CollectionError, match="journaled launch"):
        refresh_entries(cloud, event)
    assert item["arn"] is None
    arn = FUNCTION + f"/durable-execution/{record['durableExecutionName']}/id"
    refresh_entries(cloud, {**event, "execution": arn})
    assert item["arn"] == arn
    assert cloud.case_context.attempts[0]["requestId"] == "original"
    assert cloud.lam.invoke.call_count == 1


def test_replacement_execution_cannot_rebind_a_launched_marker(cloud):
    item = cloud.start("success", role="victim")
    event = enter(item, "replacement", 1)
    event["execution"] = EXECUTION + "-other"
    with pytest.raises(CollectionError, match="original durable execution"):
        refresh_entries(cloud, event)


def test_case_controls_and_collection_cannot_touch_another_case(cloud, tmp_path):
    cloud.hold("own-gate")
    with pytest.raises(CollectionError, match="not owned"):
        cloud.release("another-gate")
    with pytest.raises(CollectionError, match="another case"):
        cloud.refresh(markers=["another-marker"])
    with pytest.raises(CollectionError, match="another case"):
        cloud.refresh(markers=[])
    with pytest.raises(CollectionError, match="only its own"):
        cloud.collect(full_run=True)
    cloud.s3.get_object.side_effect = lambda **_: {"Body": io.BytesIO(b"hold"), "ETag": '"hold"'}
    cloud.release_all()
    keys = [call.kwargs["Key"] for call in cloud.s3.put_object.call_args_list]
    assert all(key.startswith(cloud.control_prefix) for key in keys)
    assert not (tmp_path / "events.json").exists()


def test_unlaunched_case_does_not_scan_shared_event_prefix(cloud):
    assert cloud.refresh() == []
    cloud.s3.get_paginator.assert_not_called()


def _carryover_ledger_and_history(*, wakeup_at, waking_event_type, name=None):
    """Three sequential invocations: A (0-1, PENDING at 0.9), B (2-3, PENDING at
    2.9, another operation's retry), C (starts at 6). A wait or callback started
    during A can only be recognized as waking C if it survives, as outstanding,
    past B's own PENDING decision.
    """
    ledger = [
        {"phase": "WRAPPER_ENTER", "request": "A"},
        {"phase": "WRAPPER_ENTER", "request": "B"},
        {"phase": "WRAPPER_ENTER", "request": "C"},
        {"phase": "WRAPPER_RETURN", "status": "PENDING", "time": 0.9, "request": "A"},
        {"phase": "WRAPPER_RETURN", "status": "PENDING", "time": 2.9, "request": "B"},
    ]
    history: list[dict[str, Any]] = [
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {"RequestId": "A", "StartTimestamp": 0, "EndTimestamp": 1},
        },
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {"RequestId": "B", "StartTimestamp": 2, "EndTimestamp": 3},
        },
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {"RequestId": "C", "StartTimestamp": 6, "EndTimestamp": 7},
        },
        {
            "EventType": waking_event_type + "Started",
            "Id": "timer",
            "EventTimestamp": 0.5,
            **({"Name": name} if name else {}),
        },
        {"EventType": waking_event_type + "Succeeded", "Id": "timer", "EventTimestamp": wakeup_at},
    ]
    return ledger, history


def test_suspension_resumptions_carries_outstanding_wait_across_intermediate_decision():
    from aws_durable_execution_conformance_tests.lmi.observations import suspension_resumptions

    ledger, history = _carryover_ledger_and_history(wakeup_at=5, waking_event_type="Wait")
    assert suspension_resumptions(ledger, history, "wait") == {"C": {"timer"}}


def test_suspension_resumptions_carries_outstanding_callback_across_intermediate_decision():
    from aws_durable_execution_conformance_tests.lmi.observations import suspension_resumptions

    ledger, history = _carryover_ledger_and_history(wakeup_at=5, waking_event_type="Callback", name="completion")
    assert suspension_resumptions(ledger, history, "callback") == {"C": {"timer"}}


def test_suspension_resumptions_rejects_wait_from_a_request_that_never_went_pending():
    from aws_durable_execution_conformance_tests.lmi.observations import suspension_resumptions

    ledger: list[dict[str, Any]] = [
        {"phase": "WRAPPER_ENTER", "request": "X"},
        {"phase": "WRAPPER_ENTER", "request": "Y"},
        {"phase": "WRAPPER_ENTER", "request": "Z"},
        # X never returns PENDING: it is not a decision window at all.
        {"phase": "WRAPPER_RETURN", "status": "PENDING", "time": 2.9, "request": "Y"},
    ]
    history: list[dict[str, Any]] = [
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {"RequestId": "X", "StartTimestamp": 0, "EndTimestamp": 1},
        },
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {"RequestId": "Y", "StartTimestamp": 2, "EndTimestamp": 3},
        },
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {"RequestId": "Z", "StartTimestamp": 6, "EndTimestamp": 7},
        },
        # Started during X's run (0-1), which never went PENDING.
        {"EventType": "WaitStarted", "Id": "timer", "EventTimestamp": 0.5},
        # Completes between Y's PENDING decision (2.9) and Z's start (6), which
        # would otherwise satisfy the completion window for Y's decision.
        {"EventType": "WaitSucceeded", "Id": "timer", "EventTimestamp": 5},
    ]
    assert suspension_resumptions(ledger, history, "wait") == {}


def test_suspension_resumptions_rejects_wakeup_completing_after_resume_started():
    from aws_durable_execution_conformance_tests.lmi.observations import suspension_resumptions

    # Completes at 6.5, after C (the resume) already started at 6.
    ledger, history = _carryover_ledger_and_history(wakeup_at=6.5, waking_event_type="Wait")
    assert suspension_resumptions(ledger, history, "wait") == {}


def _teardown_ledger_and_history(*, decision_observed=True):
    """first returns PENDING at 1s and its invocation ends at 1.1s; second resumes at 2s.

    With ``decision_observed`` the SDK reports its PENDING decision at 900ms,
    so work completing during wrapper teardown follows the decision.
    """
    ledger: list[dict[str, Any]] = [
        {"phase": "WRAPPER_RETURN", "status": "PENDING", "time": 1, "request": "first"},
        {"phase": "WRAPPER_ENTER", "request": "first"},
        {"phase": "WRAPPER_ENTER", "request": "second"},
    ]
    if decision_observed:
        ledger.append(
            {
                "phase": "CONFORMANCE",
                "request": "first",
                "observation": {"kind": "invocation_end", "outcome": "PENDING", "atMs": 900},
            }
        )
    history: list[dict[str, Any]] = [
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {"RequestId": "first", "StartTimestamp": 0, "EndTimestamp": 1.1},
        },
        {
            "EventType": "InvocationCompleted",
            "InvocationCompletedDetails": {"RequestId": "second", "StartTimestamp": 2, "EndTimestamp": 3},
        },
        {
            "EventType": "StepFailed",
            "Id": "retry",
            "StepFailedDetails": {"RetryDetails": {"NextAttemptDelaySeconds": 1}},
        },
    ]
    return ledger, history


@pytest.mark.parametrize(
    ("durable_wait", "decision_observed", "crossed"),
    [
        # The timer elapses during wrapper teardown, after the SDK's PENDING decision.
        (True, True, True),
        # Only a backoff suspended the execution.
        (False, True, False),
        # Without the SDK's decision the wrapper return bounds it, and the timer elapsed first.
        (True, False, False),
    ],
)
def test_wait_crossed_suspension_uses_the_sdk_pending_decision(durable_wait, decision_observed, crossed):
    from aws_durable_execution_conformance_tests.lmi.observations import wait_crossed_suspension

    ledger, history = _teardown_ledger_and_history(decision_observed=decision_observed)
    if durable_wait:
        history += [
            {"EventType": "WaitStarted", "Id": "timer", "EventTimestamp": 0.8},
            {"EventType": "WaitSucceeded", "Id": "timer", "EventTimestamp": 0.95},
        ]
    assert wait_crossed_suspension(ledger, history) is crossed


@pytest.mark.parametrize(
    ("started_at", "decision_observed", "selected"), [(0.85, True, True), (0.95, True, False), (0.95, False, True)]
)
def test_pending_callback_must_start_by_the_sdk_pending_decision(started_at, decision_observed, selected):
    from aws_durable_execution_conformance_tests.lmi.observations import pending_callback

    ledger, history = _teardown_ledger_and_history(decision_observed=decision_observed)
    history.append(
        {
            "EventType": "CallbackStarted",
            "Name": "completion",
            "Id": "callback",
            "EventTimestamp": started_at,
            "CallbackStartedDetails": {"CallbackId": "token"},
        }
    )
    assert pending_callback(ledger, history, "completion") == ("token" if selected else None)


def test_wrong_function_response_is_never_adopted_for_case_cleanup(cloud):
    cloud.lam.invoke.side_effect = None
    cloud.lam.invoke.return_value = {
        "StatusCode": 202,
        "DurableExecutionArn": "arn:another-function/durable-execution/name/id",
    }
    with pytest.raises(CollectionError, match="API failed"):
        cloud.start("success", role="victim")
    assert cloud.invocations[0]["arn"] is None


def test_late_original_stream_is_rejected_across_refreshes(cloud):
    item = cloud.start("replay", role="victim", resume_roles=("resume",))
    refresh_entries(cloud, enter(item, "retry", 2))
    with pytest.raises(CollectionError, match="chronology"):
        refresh_entries(cloud, enter(item, "original", 1))
    assert cloud.case_context.attempts[0]["requestId"] == "retry"
    assert "original" not in cloud.request_roles
    with pytest.raises(CollectionError, match="chronology"):
        cloud.refresh()


def test_later_retry_remains_bindable_across_refreshes(cloud):
    item = cloud.start("replay", role="victim", resume_roles=("resume",))
    refresh_entries(cloud, enter(item, "original", 1))
    refresh_entries(cloud, enter(item, "retry", 2))
    assert [(item["role"], item["requestId"]) for item in cloud.case_context.attempts] == [
        ("victim", "original"),
        ("resume", "retry"),
    ]


def test_quiescent_finish_cannot_clear_a_foreign_run_fence(tmp_path):
    from aws_durable_execution_conformance_tests.lmi.quarantine import release_fence

    storage = Mock()
    storage.get_object.return_value = {
        "Body": io.BytesIO(
            json.dumps(
                {"resourceId": FUNCTION, "runId": "foreign", "commit": "sdk-sha", "token": "foreign-token"}
            ).encode()
        ),
        "ETag": '"fence-etag"',
    }
    with pytest.raises(ProvisioningError, match="unverified persistent retirement fence"):
        release_fence(storage, deployment(), FUNCTION, "owned-token")
    storage.delete_object.assert_not_called()


def test_attempt_audit_failure_does_not_mark_invocation_submitted(cloud):
    def reject(_):
        (intent,) = (cloud.artifacts_dir / "invocations").glob("*.json")
        assert json.loads(intent.read_text())["dispatchState"] == "PREPARED"
        raise OSError("audit failed")

    cloud.case_context._on_attempt = reject
    with pytest.raises(CollectionError, match="invocation API failed"):
        cloud.start("fixture", role="victim")
    (intent,) = (cloud.artifacts_dir / "invocations").glob("*.json")
    assert json.loads(intent.read_text())["dispatchState"] == "PREPARED"
    assert cloud.case_context.attempts == []
    assert cloud.invocations == []
    cloud.lam.invoke.assert_not_called()
