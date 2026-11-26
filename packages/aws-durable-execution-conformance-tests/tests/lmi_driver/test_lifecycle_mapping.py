# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Persisted service payloads and singular assertion boundaries preserve audit facts."""

from copy import deepcopy

import pytest

from aws_durable_execution_conformance_tests.lmi.source_evidence import _decoded, assertion_events


def test_scalar_and_wrapped_service_payloads_have_the_same_persisted_meaning():
    assert _decoded({"Truncated": False, "Payload": '"value"'}) == _decoded('"value"') == "value"
    assert _decoded({"Truncated": False, "Payload": "callback bytes"}) == "callback bytes"
    assert _decoded('{"Payload":"business field"}') == {"Payload": "business field"}
    with pytest.raises(ValueError, match="Truncated"):
        _decoded({"Truncated": True, "Payload": '"partial"'})


@pytest.mark.parametrize(
    "scenario,target",
    [
        ("normal-return", "root"),
        ("normal-failure", "root"),
        ("warm-reuse", "late-callback"),
    ],
)
def test_singular_assertion_boundary_does_not_discard_other_task_lifetimes(scenario, target):
    original = [
        {"kind": kind, "taskId": task, "requestId": "victim", "atMs": time}
        for task in ("root", "user-task-1", "late-callback")
        for kind, time in (("task_start", 1), ("task_exit", 2))
    ]
    retained = deepcopy(original)
    events = assertion_events(original, scenario, {"victim": "victim"})
    assert [e["taskId"] for e in events if e["kind"] == "task_exit"] == [target]
    assert len(events) == len(original)
    assert original == retained
    assert [event for event in events if event["kind"].startswith("observed_task_")] == [
        {**original_event, "kind": f"observed_{original_event['kind']}", "sourceKind": original_event["kind"]}
        for original_event in original
        if original_event["taskId"] != target
    ]
