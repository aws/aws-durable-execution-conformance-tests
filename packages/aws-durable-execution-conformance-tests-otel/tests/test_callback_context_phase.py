# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The case-23 callback must resume an invocation that has already ended."""

import json
from types import SimpleNamespace
from typing import Any

import pytest

from aws_durable_execution_conformance_tests import validate
from aws_durable_execution_conformance_tests.callback import CallbackAction
from aws_durable_execution_conformance_tests_otel.extension import OtelExtension


@pytest.mark.parametrize("view", ["execution", "invocation"])
def test_case23_waits_for_creating_invocation_that_outlasts_callback_delay(
    monkeypatch: pytest.MonkeyPatch, view: str
) -> None:
    suite = next(item for item in OtelExtension().requirement_suites() if item.name == f"otel-{view}")
    requirement = validate.load_yaml_file(validate.discover_test_files(suite.root, suite="all")[f"otel-{view}-23"])
    actions = [CallbackAction.from_dict(item) for item in requirement["CallbackActions"]]
    now = 0.0
    completion_observed = False
    deliveries: list[tuple[float, bool, dict[str, Any]]] = []

    def sleep(seconds: float) -> None:
        nonlocal now
        now += seconds

    class Service:
        def history(self, _arn: str, _client: object) -> dict:
            nonlocal completion_observed
            events = [
                {"EventId": 1, "EventType": "ExecutionStarted"},
                {
                    "EventId": 2,
                    "EventType": "CallbackStarted",
                    "Name": "otel-context-callback",
                    "CallbackStartedDetails": {"CallbackId": "case23-capability"},
                },
            ]
            # Closing the creating invocation takes longer than the catalog's
            # one-second callback delay, without changing the SDK contract.
            if now >= 3:
                completion_observed = True
                events.append({"EventId": 3, "EventType": "InvocationCompleted"})
            if deliveries:
                events.append({"EventId": 4, "EventType": "ExecutionSucceeded"})
            return {"Events": events}

        def send_durable_execution_callback_success(self, **kwargs: Any) -> dict:
            deliveries.append((now, completion_observed, kwargs))
            return {}

        def get_durable_execution(self, **_kwargs: Any) -> dict:
            return {"Status": "SUCCEEDED", "Result": json.dumps("callback-context-complete")}

    service = Service()
    monkeypatch.setattr(validate, "get_execution_history", service.history)
    monkeypatch.setattr(validate, "time", SimpleNamespace(time=lambda: now, sleep=sleep))
    result = validate.PollingValidator(
        service, validate.AsyncValidationConfig(poll_interval_seconds=1, no_progress_timeout_seconds=10)
    ).validate("arn:case23", requirement["ExpectedExecutionHistory"], requirement["ExpectedResult"], actions)

    assert result.passed, result.errors
    assert result.callbacks_sent == 1
    assert len(deliveries) == 1
    delivered_at, after_completion, payload = deliveries[0]
    assert after_completion, "The callback was sent before InvocationCompleted was observed"
    assert delivered_at >= 3
    assert payload == {"CallbackId": "case23-capability", "Result": b'"callback-complete"'}
