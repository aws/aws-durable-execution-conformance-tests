# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Exercise callback scheduling through the polling validator and sender."""

from __future__ import annotations

import json
from typing import Any

import pytest
from botocore.exceptions import ClientError

import aws_durable_execution_conformance_tests.validate as validation
from aws_durable_execution_conformance_tests.callback import CallbackAction


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class _CallbackService:
    """Model service deadlines while a history request spends time retrying."""

    def __init__(
        self,
        clock: _Clock,
        *,
        history_retry_delay: float = 0,
        names: tuple[str, ...] = ("callback",),
        reject_heartbeat: bool = False,
        heartbeat_timeouts: dict[str, float] | None = None,
    ) -> None:
        self.clock = clock
        self.history_retry_delay = history_retry_delay
        self.reject_heartbeat = reject_heartbeat
        self.history_reads = 0
        self.attempts: list[tuple[str, str, float, Any]] = []
        self.heartbeat_timeouts = heartbeat_timeouts or dict.fromkeys(names, 10.0)
        self.deadlines = self.heartbeat_timeouts.copy()
        self.outcomes: dict[str, str] = {}

    def _expire(self) -> None:
        for name, deadline in self.deadlines.items():
            if name not in self.outcomes and self.clock.now >= deadline:
                self.outcomes[name] = "CallbackTimedOut"

    def _status(self) -> str:
        self._expire()
        if len(self.outcomes) < len(self.deadlines):
            return "RUNNING"
        if all(outcome == "CallbackSucceeded" for outcome in self.outcomes.values()):
            return "SUCCEEDED"
        return "FAILED"

    def get_durable_execution_history(self, **_kwargs: Any) -> dict[str, Any]:
        self.history_reads += 1
        retries = 0
        if self.history_reads == 2:
            self.clock.sleep(self.history_retry_delay)
            retries = 3 if self.history_retry_delay else 0
        status = self._status()
        events: list[dict[str, Any]] = [
            {
                "EventType": "CallbackStarted",
                "Id": f"operation-{name}",
                "Name": name,
                "CallbackStartedDetails": {"CallbackId": name, "HeartbeatTimeout": self.heartbeat_timeouts[name]},
            }
            for name in self.deadlines
        ]
        events.extend(
            {"EventType": outcome, "Id": f"operation-{name}", "Name": name} for name, outcome in self.outcomes.items()
        )
        return {
            "Events": events,
            "ExecutionStatus": status,
            "ResponseMetadata": {"RetryAttempts": retries},
        }

    def get_durable_execution(self, **_kwargs: Any) -> dict[str, Any]:
        return {"Status": self._status()}

    def _send(self, operation: str, callback_id: str, payload: Any = None) -> dict[str, Any]:
        self.attempts.append((operation, callback_id, self.clock.now, payload))
        self._expire()
        if callback_id in self.outcomes or (operation == "heartbeat" and self.reject_heartbeat):
            raise ClientError(
                {
                    "Error": {
                        "Code": "CallbackTimeoutException"
                        if callback_id in self.outcomes
                        else "InvalidParameterValueException",
                        "Message": "callback unavailable",
                    }
                },
                "SendDurableExecutionCallback" + operation.title(),
            )
        if operation == "heartbeat":
            self.deadlines[callback_id] = self.clock.now + self.heartbeat_timeouts[callback_id]
        else:
            self.outcomes[callback_id] = "CallbackSucceeded" if operation == "success" else "CallbackFailed"
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def send_durable_execution_callback_heartbeat(self, *, CallbackId: str) -> dict[str, Any]:
        return self._send("heartbeat", CallbackId)

    def send_durable_execution_callback_success(self, *, CallbackId: str, Result: bytes) -> dict[str, Any]:
        return self._send("success", CallbackId, json.loads(Result))

    def send_durable_execution_callback_failure(self, *, CallbackId: str, Error: dict[str, Any]) -> dict[str, Any]:
        return self._send("failure", CallbackId, Error)


def _validate(
    monkeypatch: pytest.MonkeyPatch,
    service: _CallbackService,
    actions: list[CallbackAction],
    expected_status: str = "SUCCEEDED",
) -> validation.AsyncValidationResult:
    monkeypatch.setattr(validation, "time", service.clock)
    return validation.PollingValidator(service).validate(
        execution_arn="arn:test:execution",
        expected_events=[],
        expected_result={"ExecutionStatus": expected_status},
        callback_actions=actions,
    )


@pytest.mark.parametrize("history_retry_delay", [10, 25])
def test_heartbeat_followup_precedes_slow_history_poll(
    monkeypatch: pytest.MonkeyPatch, history_retry_delay: float
) -> None:
    clock = _Clock()
    service = _CallbackService(clock, history_retry_delay=history_retry_delay)

    result = _validate(
        monkeypatch,
        service,
        [CallbackAction("*", "heartbeat"), CallbackAction("*", "success", "done", delay_seconds=1)],
    )

    assert result.passed, result.errors
    assert result.final_status == "SUCCEEDED"
    assert result.callbacks_sent == 2
    assert service.attempts == [("heartbeat", "callback", 2, None), ("success", "callback", 3, "done")]
    assert service.history_reads == 2
    assert clock.now == 5 + history_retry_delay
    assert service.outcomes == {"callback": "CallbackSucceeded"}


@pytest.mark.parametrize("operation", ["success", "failure"])
def test_heartbeat_chain_keeps_delays_and_stops_after_terminal_action(
    monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    service = _CallbackService(_Clock(), history_retry_delay=10)
    payload: Any = "done" if operation == "success" else {"ErrorType": "ExternalFailure"}

    result = _validate(
        monkeypatch,
        service,
        [
            CallbackAction("*", "heartbeat"),
            CallbackAction("*", "heartbeat", delay_seconds=1),
            CallbackAction("*", operation, payload, delay_seconds=2),
            CallbackAction("*", "success", "must not send"),
        ],
        "SUCCEEDED" if operation == "success" else "FAILED",
    )

    assert result.passed, result.errors
    assert result.callbacks_sent == 3
    assert service.attempts == [
        ("heartbeat", "callback", 2, None),
        ("heartbeat", "callback", 3, None),
        (operation, "callback", 5, payload),
    ]


def test_followup_uses_the_same_callback_and_leaves_other_actions_available(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _CallbackService(_Clock(), names=("first", "second"))

    result = _validate(
        monkeypatch,
        service,
        [
            CallbackAction("first", "heartbeat"),
            CallbackAction("first", "success", "first result", delay_seconds=1),
            CallbackAction("second", "success", "second result"),
        ],
    )

    assert result.passed, result.errors
    assert result.callbacks_sent == 3
    assert service.attempts == [
        ("heartbeat", "first", 2, None),
        ("success", "second", 2, "second result"),
        ("success", "first", 3, "first result"),
    ]


@pytest.mark.parametrize("second_timeout", [5.0, 10.0])
def test_delayed_followup_does_not_starve_another_known_callback(
    monkeypatch: pytest.MonkeyPatch, second_timeout: float
) -> None:
    service = _CallbackService(_Clock(), heartbeat_timeouts={"first": 10, "second": second_timeout})

    result = _validate(
        monkeypatch,
        service,
        [
            CallbackAction("first", "heartbeat"),
            CallbackAction("first", "success", "first result", delay_seconds=4),
            CallbackAction("second", "success", "second result"),
        ],
    )

    assert result.passed, result.errors
    assert result.final_status == "SUCCEEDED"
    assert result.callbacks_sent == 3
    assert service.attempts == [
        ("heartbeat", "first", 2, None),
        ("success", "second", 2, "second result"),
        ("success", "first", 6, "first result"),
    ]
    assert service.outcomes == {"first": "CallbackSucceeded", "second": "CallbackSucceeded"}
    assert service.history_reads == 2


def test_heartbeat_followup_keeps_first_round_wildcard_action_matching(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _CallbackService(_Clock(), names=("first", "second"))

    result = _validate(
        monkeypatch,
        service,
        [
            CallbackAction("*", "heartbeat"),
            CallbackAction("*", "success", "second result"),
            CallbackAction("*", "success", "first result", delay_seconds=1),
        ],
    )

    assert result.passed, result.errors
    assert result.callbacks_sent == 3
    assert service.attempts == [
        ("heartbeat", "first", 2, None),
        ("success", "second", 2, "second result"),
        ("success", "first", 3, "first result"),
    ]


def test_configured_delay_that_exceeds_heartbeat_timeout_still_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _CallbackService(_Clock())

    result = _validate(
        monkeypatch,
        service,
        [CallbackAction("*", "heartbeat"), CallbackAction("*", "success", "too late", delay_seconds=11)],
    )

    assert not result.passed
    assert result.final_status == "FAILED"
    assert result.callbacks_sent == 1
    assert any("Callback success failed" in error for error in result.errors)
    assert service.attempts == [("heartbeat", "callback", 2, None), ("success", "callback", 13, "too late")]
    assert service.outcomes == {"callback": "CallbackTimedOut"}


def test_failed_heartbeat_does_not_retry_or_dispatch_followup_before_next_poll(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _CallbackService(_Clock(), reject_heartbeat=True)

    result = _validate(
        monkeypatch,
        service,
        [CallbackAction("*", "heartbeat"), CallbackAction("*", "success", "done", delay_seconds=1)],
    )

    assert not result.passed
    assert result.final_status == "SUCCEEDED"
    assert result.callbacks_sent == 1
    assert any("Callback heartbeat failed" in error for error in result.errors)
    assert service.attempts == [("heartbeat", "callback", 2, None), ("success", "callback", 5, "done")]


def test_terminal_history_does_not_dispatch_callback_actions(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _CallbackService(_Clock())
    service.outcomes["callback"] = "CallbackSucceeded"

    result = _validate(monkeypatch, service, [CallbackAction("*", "heartbeat")])

    assert result.passed, result.errors
    assert result.callbacks_sent == 0
    assert service.attempts == []


def test_heartbeat_without_followup_keeps_existing_unmatched_action_error(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _CallbackService(_Clock())

    result = _validate(monkeypatch, service, [CallbackAction("*", "heartbeat")], "FAILED")

    assert not result.passed
    assert any("No matching CallbackAction" in error for error in result.errors)
    assert result.callbacks_sent == 1
    assert service.attempts == [("heartbeat", "callback", 2, None)]
    assert service.outcomes == {"callback": "CallbackTimedOut"}


class _StopConditionService(_CallbackService):
    def __init__(
        self,
        clock: _Clock,
        *,
        names: tuple[str, ...] = ("first", "second"),
        finish_on_second: str | None = None,
        stale_history_reads: int = 0,
        fail_status: bool = False,
        history_retry_delay: float = 0,
        status_delay: float = 0,
        fail_running_status: bool = False,
        closed_error_code: str = "CallbackTimeoutException",
        omit_operation_ids: bool = False,
        incomplete_history: bool = False,
        finish_delay: float = 0,
    ) -> None:
        super().__init__(clock, names=names, history_retry_delay=history_retry_delay)
        self.finish_on_second = finish_on_second
        self.finished: str | None = None
        self.stale_history_reads = stale_history_reads
        self.fail_status = fail_status
        self.status_reads = 0
        self.status_delay = status_delay
        self.fail_running_status = fail_running_status
        self.closed_error_code = closed_error_code
        self.omit_operation_ids = omit_operation_ids
        self.incomplete_history = incomplete_history
        self.finish_delay = finish_delay
        self.finish_at: float | None = None

    def _status(self) -> str:
        if self.finish_at is not None and self.clock.now >= self.finish_at:
            self.finished = self.finish_on_second
        return self.finished or super()._status()

    def _send(self, operation: str, callback_id: str, payload: Any = None) -> dict[str, Any]:
        self._status()
        if self.finished:
            self.attempts.append((operation, callback_id, self.clock.now, payload))
            raise ClientError(
                {"Error": {"Code": self.closed_error_code, "Message": "execution already finished"}},
                "SendDurableExecutionCallback" + operation.title(),
            )
        response = super()._send(operation, callback_id, payload)
        if callback_id == "second" and operation != "heartbeat" and self.finish_on_second:
            self.finish_at = self.clock.now + self.finish_delay
        return response

    def get_durable_execution(self, **_kwargs: Any) -> dict[str, Any]:
        self.status_reads += 1
        if self._status() == "RUNNING":
            self.clock.sleep(self.status_delay)
        if self.fail_status or (self.fail_running_status and self._status() == "RUNNING"):
            self.fail_status = False
            raise ClientError(
                {"Error": {"Code": "ServiceException", "Message": "status unavailable"}},
                "GetDurableExecution",
            )
        return super().get_durable_execution()

    def get_durable_execution_history(self, **_kwargs: Any) -> dict[str, Any]:
        response = super().get_durable_execution_history()
        if self.finished and self.stale_history_reads:
            self.stale_history_reads -= 1
            response["ExecutionStatus"] = "RUNNING"
            response["Events"] = [event for event in response["Events"] if event["EventType"] == "CallbackStarted"]
        elif self._status() in {"SUCCEEDED", "FAILED"}:
            response["Events"].append(
                {"EventType": "ExecutionSucceeded" if self._status() == "SUCCEEDED" else "ExecutionFailed"}
            )
        for i, event in enumerate(response["Events"], 1):
            event["EventId"] = i
            if self.omit_operation_ids:
                event.pop("Id", None)
        if self.incomplete_history:
            response["NextMarker"] = "more-history"
        return response


def test_history_only_completion_stops_before_heartbeat_followup(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _StopConditionService(_Clock(), names=("callback",))
    monkeypatch.setattr(validation, "time", service.clock)

    result = validation.PollingValidator(service).validate(
        execution_arn="arn:test:execution",
        expected_events=[{"EventType": "CallbackStarted", "EventId": 1, "Name": "callback"}],
        expected_result=None,
        callback_actions=[CallbackAction("*", "heartbeat"), CallbackAction("*", "success", "must not send", 1)],
    )

    assert result.passed, result.errors
    assert result.final_status == "RUNNING"
    assert result.callbacks_sent == 1
    assert service.attempts == [("heartbeat", "callback", 2, None)]
    assert service.history_reads == 1
    assert service.status_reads == 0


@pytest.mark.parametrize("terminal_status", ["SUCCEEDED", "FAILED"])
@pytest.mark.parametrize("stale_history_reads", [0, 1])
def test_early_terminal_execution_stops_followups_and_still_validates_final_history(
    monkeypatch: pytest.MonkeyPatch, terminal_status: str, stale_history_reads: int
) -> None:
    service = _StopConditionService(_Clock(), finish_on_second=terminal_status, stale_history_reads=stale_history_reads)
    monkeypatch.setattr(validation, "time", service.clock)
    success = terminal_status == "SUCCEEDED"
    operation = "success" if success else "failure"
    payload: Any = "second result" if success else {"ErrorType": "ExternalFailure"}
    result = validation.PollingValidator(service).validate(
        execution_arn="arn:test:execution",
        expected_events=[
            {"EventType": "CallbackStarted", "EventId": 1, "Name": "first"},
            {"EventType": "CallbackStarted", "EventId": 2, "Name": "second"},
            {"EventType": "CallbackSucceeded" if success else "CallbackFailed", "EventId": 3, "Name": "second"},
            {"EventType": "ExecutionSucceeded" if success else "ExecutionFailed", "EventId": 4},
        ],
        expected_result={"ExecutionStatus": terminal_status},
        callback_actions=[
            CallbackAction("first", "heartbeat"),
            CallbackAction("first", "success", "must not send", 1),
            CallbackAction("second", operation, payload),
        ],
    )

    assert result.passed, result.errors
    assert result.final_status == terminal_status
    assert result.callbacks_sent == 2
    assert service.attempts == [
        ("heartbeat", "first", 2, None),
        (operation, "second", 2, payload),
        ("success", "first", 3, "must not send"),
    ]
    assert service.history_reads == 2 + stale_history_reads
    assert service.status_reads == 2  # Reconcile rejection, then final result assertion.
    assert result.event_count == 4


@pytest.mark.parametrize("history_retry_delay", [10, 25])
def test_nonterminal_status_probe_does_not_wait_for_history_before_followup(
    monkeypatch: pytest.MonkeyPatch, history_retry_delay: float
) -> None:
    service = _StopConditionService(_Clock(), history_retry_delay=history_retry_delay)

    result = _validate(
        monkeypatch,
        service,
        [
            CallbackAction("first", "heartbeat"),
            CallbackAction("first", "success", "first result", 1),
            CallbackAction("second", "success", "second result"),
        ],
    )

    assert result.passed, result.errors
    assert service.attempts == [
        ("heartbeat", "first", 2, None),
        ("success", "second", 2, "second result"),
        ("success", "first", 3, "first result"),
    ]
    assert service.history_reads == 2
    assert service.status_reads == 1  # Healthy sends need only the final result lookup.


def test_status_probe_failure_retains_original_callback_error(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _StopConditionService(_Clock(), finish_on_second="SUCCEEDED", fail_status=True)

    result = _validate(
        monkeypatch,
        service,
        [
            CallbackAction("first", "heartbeat"),
            CallbackAction("first", "success", "must not send", 1),
            CallbackAction("second", "success", "second result"),
        ],
    )

    assert not result.passed
    assert any("Callback success failed" in error and "CallbackTimeoutException" in error for error in result.errors)
    assert result.callbacks_sent == 2
    assert service.attempts == [
        ("heartbeat", "first", 2, None),
        ("success", "second", 2, "second result"),
        ("success", "first", 3, "must not send"),
    ]
    assert service.history_reads == 2
    assert service.status_reads == 2


def test_history_only_completion_is_checked_without_callback_events(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _StopConditionService(_Clock())
    event = {"EventType": "StepStarted", "EventId": 1, "Name": "ordinary-step"}
    monkeypatch.setattr(validation, "time", service.clock)
    monkeypatch.setattr(
        service, "get_durable_execution_history", lambda **_kwargs: {"ExecutionStatus": "RUNNING", "Events": [event]}
    )

    result = validation.PollingValidator(service).validate(
        execution_arn="arn:test:execution",
        expected_events=[event],
        expected_result=None,
    )

    assert result.passed, result.errors
    assert result.final_status == "RUNNING"
    assert result.callbacks_sent == 0
    assert service.attempts == []
    assert service.status_reads == 0


@pytest.mark.parametrize("status_delay,fail_running_status", [(10, False), (25, False), (0, True)])
def test_healthy_followups_do_not_depend_on_status_lookup(
    monkeypatch: pytest.MonkeyPatch, status_delay: float, fail_running_status: bool
) -> None:
    service = _StopConditionService(_Clock(), status_delay=status_delay, fail_running_status=fail_running_status)

    result = _validate(
        monkeypatch,
        service,
        [
            CallbackAction("first", "heartbeat"),
            CallbackAction("first", "success", "first result", 1),
            CallbackAction("second", "success", "second result"),
        ],
    )

    assert result.passed, result.errors
    assert service.attempts[-1] == ("success", "first", 3, "first result")
    assert service.status_reads == 1  # Only the existing final result assertion.


@pytest.mark.parametrize("names", [("first", "second"), ("second", "first")])
@pytest.mark.parametrize("finish_delay", [0, 0.5])
def test_closed_race_loser_is_reconciled_in_either_callback_order(
    monkeypatch: pytest.MonkeyPatch, names: tuple[str, ...], finish_delay: float
) -> None:
    service = _StopConditionService(_Clock(), names=names, finish_on_second="SUCCEEDED", finish_delay=finish_delay)

    result = _validate(
        monkeypatch,
        service,
        [
            CallbackAction("first", "heartbeat"),
            CallbackAction("first", "success", "first result", 1),
            CallbackAction("second", "success", "second result"),
        ],
    )

    assert result.passed, result.errors
    assert result.final_status == "SUCCEEDED"
    assert service.outcomes == {"second": "CallbackSucceeded"}
    assert service.status_reads == 2
    first_actions = [operation for operation, name, _at, _payload in service.attempts if name == "first"]
    assert first_actions == (["heartbeat"] if names[0] == "second" and finish_delay == 0 else ["heartbeat", "success"])


@pytest.mark.parametrize(
    "error_code",
    [
        "InvalidParameterValueException",
        "ResourceNotFoundException",
        "AccessDeniedException",
        "KMSAccessDeniedException",
        "TooManyRequestsException",
        "ServiceException",
    ],
)
def test_other_service_errors_are_not_reconciled_away(monkeypatch: pytest.MonkeyPatch, error_code: str) -> None:
    service = _StopConditionService(_Clock(), finish_on_second="SUCCEEDED", closed_error_code=error_code)

    result = _validate(
        monkeypatch,
        service,
        [
            CallbackAction("first", "heartbeat"),
            CallbackAction("first", "success", "first result", 1),
            CallbackAction("second", "success", "second result"),
        ],
    )

    assert not result.passed
    assert result.final_status == "SUCCEEDED"
    assert any(error_code in error for error in result.errors)
    assert service.status_reads == 1


@pytest.mark.parametrize("missing_id,incomplete_history", [(True, False), (False, True)])
def test_closed_error_requires_stable_identity_and_complete_history(
    monkeypatch: pytest.MonkeyPatch, missing_id: bool, incomplete_history: bool
) -> None:
    service = _StopConditionService(
        _Clock(), finish_on_second="SUCCEEDED", omit_operation_ids=missing_id, incomplete_history=incomplete_history
    )

    result = _validate(
        monkeypatch,
        service,
        [
            CallbackAction("first", "heartbeat"),
            CallbackAction("first", "success", "first result", 1),
            CallbackAction("second", "success", "second result"),
        ],
    )

    assert not result.passed
    assert result.final_status == "SUCCEEDED"
    assert any("CallbackTimeoutException" in error for error in result.errors)


def test_genuine_expiry_is_retained_even_when_failed_execution_is_expected(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _StopConditionService(_Clock())

    result = _validate(
        monkeypatch,
        service,
        [
            CallbackAction("first", "heartbeat"),
            CallbackAction("first", "success", "too late", 11),
            CallbackAction("second", "success", "second result"),
        ],
        "FAILED",
    )

    assert not result.passed
    assert result.final_status == "FAILED"
    assert service.outcomes["first"] == "CallbackTimedOut"
    assert any("CallbackTimeoutException" in error for error in result.errors)


def test_repeated_send_to_already_completed_callback_remains_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _StopConditionService(_Clock())
    service.outcomes["first"] = "CallbackSucceeded"

    result = _validate(
        monkeypatch,
        service,
        [CallbackAction("first", "heartbeat"), CallbackAction("second", "success", "second result")],
    )

    assert not result.passed
    assert result.final_status == "SUCCEEDED"
    assert any("CallbackTimeoutException" in error for error in result.errors)
    assert service.attempts == [("heartbeat", "first", 2, None), ("success", "second", 2, "second result")]


def test_nonterminal_reconciliation_keeps_original_error(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _StopConditionService(_Clock())
    service.outcomes["first"] = "CallbackTimedOut"

    result = _validate(
        monkeypatch,
        service,
        [CallbackAction("first", "heartbeat"), CallbackAction("second", "heartbeat")],
        "FAILED",
    )

    assert not result.passed
    assert any("Callback heartbeat failed" in error and "CallbackTimeoutException" in error for error in result.errors)
    assert service.attempts == [("heartbeat", "first", 2, None), ("heartbeat", "second", 2, None)]
    assert service.status_reads == 2


def test_reconciliation_waits_until_healthy_followups_have_been_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _StopConditionService(_Clock(), names=("first", "second", "third"), status_delay=25)
    service.outcomes["first"] = "CallbackTimedOut"

    result = _validate(
        monkeypatch,
        service,
        [
            CallbackAction("first", "heartbeat"),
            CallbackAction("second", "heartbeat"),
            CallbackAction("second", "success", "second result", 1),
            CallbackAction("third", "success", "third result"),
        ],
        "FAILED",
    )

    assert not result.passed  # The genuine expiry must still be reported.
    assert any("CallbackTimeoutException" in error for error in result.errors)
    assert service.attempts[-1] == ("success", "second", 3, "second result")
    assert service.outcomes["second"] == "CallbackSucceeded"
    assert result.callbacks_sent == 3


def test_local_failure_payload_validation_is_not_reconciled_away(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _StopConditionService(_Clock(), finish_on_second="SUCCEEDED")

    result = _validate(
        monkeypatch,
        service,
        [
            CallbackAction("first", "heartbeat"),
            CallbackAction("first", "failure", "invalid failure payload", 1),
            CallbackAction("second", "success", "second result"),
        ],
    )

    assert not result.passed
    assert any("Failure callback payload must be a dict" in error for error in result.errors)
    assert len(service.attempts) == 2  # Invalid payload never reaches the service.
    assert service.status_reads == 1
