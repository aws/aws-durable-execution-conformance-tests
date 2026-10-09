# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Exercise callback delivery through the real polling validator and sender."""

from __future__ import annotations

from itertools import count
from types import SimpleNamespace
from typing import Any

import pytest
from botocore.exceptions import ClientError

from aws_durable_execution_conformance_tests import validate
from aws_durable_execution_conformance_tests.callback import CallbackAction

ARN = "arn:test:owned-execution"
CALLBACK = {
    "EventId": 3,
    "EventType": "CallbackStarted",
    "Name": "target",
    "CallbackStartedDetails": {"CallbackId": "target-capability"},
}


class Service:
    def __init__(self, frames: list[list[dict[str, Any]]]):
        self.frames = frames
        self.reads: list[str] = []
        self.requests: list[tuple[int, dict[str, Any]]] = []

    def history(self, arn: str, client: Any) -> dict[str, Any]:
        assert client is self
        self.reads.append(arn)
        if self.requests:
            return {"Events": [*self.frames[-1], {"EventId": 10, "EventType": "ExecutionFailed"}]}
        return {"Events": self.frames[min(len(self.reads) - 1, len(self.frames) - 1)]}

    def send_durable_execution_callback_failure(self, **kwargs: Any) -> dict:
        self.requests.append((len(self.reads), kwargs))
        return {}

    def get_durable_execution(self, **kwargs: Any) -> dict:
        assert kwargs["DurableExecutionArn"] == ARN
        return {"Status": "FAILED"}


def run_validator(monkeypatch: pytest.MonkeyPatch, service: Service, raw: dict, *, history_only=False):
    ticks = count()
    monkeypatch.setattr(validate, "time", SimpleNamespace(time=lambda: next(ticks), sleep=lambda _seconds: None))
    monkeypatch.setattr(validate, "get_execution_history", service.history)
    action = CallbackAction.from_dict({"CallbackName": "target", "Operation": "failure", **raw})
    return validate.PollingValidator(
        service, validate.AsyncValidationConfig(poll_interval_seconds=0, no_progress_timeout_seconds=2)
    ).validate(
        ARN,
        [{"EventId": 3, "EventType": "CallbackStarted"}],
        None if history_only else {"ExecutionStatus": "FAILED"},
        [action],
    )


@pytest.mark.parametrize("value", [None, "true", "false", 0, 1, [], {}])
def test_phase_option_rejects_non_boolean(value: Any) -> None:
    with pytest.raises(ValueError, match="AfterInvocationCompleted must be a boolean"):
        CallbackAction.from_dict({"CallbackName": "target", "Operation": "failure", "AfterInvocationCompleted": value})


@pytest.mark.parametrize("raw", [{}, {"AfterInvocationCompleted": False}])
def test_missing_or_false_preserves_delivery_before_completion(monkeypatch: pytest.MonkeyPatch, raw: dict) -> None:
    service = Service([[CALLBACK]])
    result = run_validator(monkeypatch, service, raw)
    assert result.passed and result.callbacks_sent == 1
    assert service.requests == [(1, {"CallbackId": "target-capability"})]


@pytest.mark.parametrize("history_only", [False, True])
def test_gate_waits_for_later_completion_without_consuming_action(
    monkeypatch: pytest.MonkeyPatch, history_only: bool
) -> None:
    old_completion = {"EventId": 2, "EventType": "InvocationCompleted"}
    completion = {"EventId": 6, "EventType": "InvocationCompleted"}
    service = Service([[old_completion, CALLBACK], [old_completion, CALLBACK, completion]])
    result = run_validator(monkeypatch, service, {"AfterInvocationCompleted": True}, history_only=history_only)
    assert result.passed and result.callbacks_sent == 1
    assert service.requests == [(2, {"CallbackId": "target-capability"})]
    assert service.reads == [ARN] * (2 if history_only else 3)


@pytest.mark.parametrize("completion_id", [None, 2, 3, "6", True])
def test_missing_older_equal_or_malformed_completion_does_not_deliver(
    monkeypatch: pytest.MonkeyPatch, completion_id: Any
) -> None:
    completion = [] if completion_id is None else [{"EventId": completion_id, "EventType": "InvocationCompleted"}]
    service = Service([[*completion, CALLBACK]])
    result = run_validator(monkeypatch, service, {"AfterInvocationCompleted": True}, history_only=True)
    assert not result.passed
    assert any("No new events" in error for error in result.errors)
    assert result.callbacks_sent == 0 and service.requests == []


def test_completed_unrelated_callback_is_not_target_phase_proof(monkeypatch: pytest.MonkeyPatch) -> None:
    service = Service([[CALLBACK, {"EventId": 6, "EventType": "CallbackSucceeded"}]])
    result = run_validator(monkeypatch, service, {"AfterInvocationCompleted": True}, history_only=True)
    assert not result.passed and not service.requests


@pytest.mark.parametrize("terminal_on_first_poll", [False, True])
def test_terminal_execution_cannot_hide_undelivered_phase_action(
    monkeypatch: pytest.MonkeyPatch, terminal_on_first_poll: bool
) -> None:
    terminal = [CALLBACK, {"EventId": 5, "EventType": "ExecutionFailed"}]
    service = Service([terminal] if terminal_on_first_poll else [[CALLBACK], terminal])
    result = run_validator(monkeypatch, service, {"AfterInvocationCompleted": True}, history_only=True)
    assert not result.passed and not service.requests
    assert "Callback actions awaiting InvocationCompleted were not delivered" in result.errors


@pytest.mark.parametrize("terminal", [False, True])
def test_unmatched_phase_action_preserves_unused_action_behavior(
    monkeypatch: pytest.MonkeyPatch, terminal: bool
) -> None:
    events = [{"EventId": 1, "EventType": "ExecutionStarted"}]
    if terminal:
        events.append({"EventId": 2, "EventType": "ExecutionFailed"})
    service = Service([events])
    ticks = count()
    monkeypatch.setattr(validate, "time", SimpleNamespace(time=lambda: next(ticks), sleep=lambda _seconds: None))
    monkeypatch.setattr(validate, "get_execution_history", service.history)
    action = CallbackAction.from_dict(
        {"CallbackName": "unused", "Operation": "failure", "AfterInvocationCompleted": True}
    )
    result = validate.PollingValidator(service).validate(
        ARN,
        [{"EventId": 1, "EventType": "ExecutionStarted"}],
        {"ExecutionStatus": "FAILED"} if terminal else None,
        [action],
    )
    assert result.passed and result.errors == []
    assert service.reads == [ARN] and service.requests == []


def test_later_target_still_waits_for_phase_and_unused_action_does_not_block(monkeypatch: pytest.MonkeyPatch) -> None:
    service = Service(
        [
            [{"EventId": 1, "EventType": "ExecutionStarted"}],
            [CALLBACK],
            [CALLBACK, {"EventId": 6, "EventType": "InvocationCompleted"}],
        ]
    )
    ticks = count()
    monkeypatch.setattr(validate, "time", SimpleNamespace(time=lambda: next(ticks), sleep=lambda _seconds: None))
    monkeypatch.setattr(validate, "get_execution_history", service.history)
    actions = [
        CallbackAction.from_dict({"CallbackName": name, "Operation": "failure", "AfterInvocationCompleted": True})
        for name in ["unused", "target"]
    ]
    result = validate.PollingValidator(service).validate(
        ARN, [{"EventId": 3, "EventType": "CallbackStarted"}], None, actions
    )
    assert result.passed and result.callbacks_sent == 1
    assert service.requests == [(3, {"CallbackId": "target-capability"})]


def test_multiple_matching_callbacks_use_delivery_time_action_indices(monkeypatch: pytest.MonkeyPatch) -> None:
    other = {**CALLBACK, "EventId": 4, "CallbackStartedDetails": {"CallbackId": "second-capability"}}
    service = Service([[CALLBACK, other], [CALLBACK, other, {"EventId": 6, "EventType": "InvocationCompleted"}]])
    ticks = count()
    monkeypatch.setattr(validate, "time", SimpleNamespace(time=lambda: next(ticks), sleep=lambda _seconds: None))
    monkeypatch.setattr(validate, "get_execution_history", service.history)
    actions = [
        CallbackAction.from_dict({"CallbackName": "target", "Operation": "failure", "AfterInvocationCompleted": True})
        for _ in range(2)
    ]
    result = validate.PollingValidator(service).validate(ARN, [{"EventId": 3}, {"EventId": 4}], None, actions)
    assert result.passed and result.callbacks_sent == 2
    assert service.requests == [(2, {"CallbackId": "target-capability"}), (2, {"CallbackId": "second-capability"})]


@pytest.mark.parametrize("history_only", [False, True])
def test_one_action_two_matching_callbacks_preserve_delivery_time_rules(
    monkeypatch: pytest.MonkeyPatch, history_only: bool
) -> None:
    other = {**CALLBACK, "EventId": 4, "CallbackStartedDetails": {"CallbackId": "second-capability"}}

    def run(gated: bool):
        service = Service([[CALLBACK, other], [CALLBACK, other, {"EventId": 6, "EventType": "InvocationCompleted"}]])
        ticks = count()
        monkeypatch.setattr(validate, "time", SimpleNamespace(time=lambda: next(ticks), sleep=lambda _seconds: None))
        monkeypatch.setattr(validate, "get_execution_history", service.history)
        action = CallbackAction.from_dict(
            {"CallbackName": "target", "Operation": "failure", "AfterInvocationCompleted": gated}
        )
        result = validate.PollingValidator(service).validate(
            ARN, [{"EventId": 3}, {"EventId": 4}], None if history_only else {"ExecutionStatus": "FAILED"}, [action]
        )
        assert result.callbacks_sent == 1
        assert service.requests == [(2 if gated else 1, {"CallbackId": "target-capability"})]
        return result

    ordinary = run(False)
    gated = run(True)
    # Preserve the existing unmatched-callback outcome; only delivery timing changes.
    assert gated.passed == ordinary.passed
    assert gated.errors == ordinary.errors


@pytest.mark.parametrize("operation", ["success", "failure", "heartbeat"])
@pytest.mark.parametrize("gated", [False, True])
@pytest.mark.parametrize("history_only", [False, True])
def test_callback_delivery_error_is_not_a_pending_phase_or_history_success(
    monkeypatch: pytest.MonkeyPatch, operation: str, gated: bool, history_only: bool
) -> None:
    completed = [CALLBACK, {"EventId": 6, "EventType": "InvocationCompleted"}]
    service = Service([[CALLBACK], completed, [*completed, {"EventId": 10, "EventType": "ExecutionFailed"}]])
    attempts: list[tuple[int, dict[str, Any]]] = []

    def reject_callback(**kwargs: Any) -> dict:
        attempts.append((len(service.reads), kwargs))
        raise ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "callback delivery denied"}},
            f"SendDurableExecutionCallback{operation.title()}",
        )

    monkeypatch.setattr(service, f"send_durable_execution_callback_{operation}", reject_callback, raising=False)
    result = run_validator(
        monkeypatch, service, {"Operation": operation, "AfterInvocationCompleted": gated}, history_only=history_only
    )
    assert not result.passed
    assert result.callbacks_sent == 0
    assert attempts == [(2 if gated else 1, {"CallbackId": "target-capability"})]
    # An ordinary heartbeat remains matchable for a follow-up action. With
    # none configured, preserve its existing unmatched-action diagnostic.
    assert len(result.errors) == (2 if operation == "heartbeat" and not gated and not history_only else 1)
    assert result.errors[0].startswith(f"Callback {operation} failed")
    assert "callback delivery denied" in result.errors[0]
    assert len(service.reads) == ((2 if gated else 1) if history_only else 3)
