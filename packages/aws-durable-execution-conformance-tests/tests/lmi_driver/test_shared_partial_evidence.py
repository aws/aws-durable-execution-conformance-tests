# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Shared deadline-step case fixture; a final read bound alone cannot prove quiescence."""

from types import SimpleNamespace
from unittest.mock import patch

from aws_durable_execution_conformance_tests.lmi.coordinator import (
    Profile,
    Scenario,
    ScenarioContext,
)

from .support import build_evidence


def context():
    profile = Profile("c1", "function", 1, 1, "sdk", "configuration")
    return ScenarioContext(
        "run",
        Scenario(
            "case",
            "lmi-lifecycle-3",
            "c1",
            "deadline-during-step",
            roles=("victim", "retry"),
            coverage_profile="standard",
        ),
        profile,
    )


def test_final_read_bound_alone_does_not_invent_quiescence(tmp_path):
    ctx = context()
    ctx.scenario = Scenario(
        "case",
        "lmi-lifecycle-1",
        "c1",
        "native-overlap",
        roles=("victim",),
        coverage_profile="standard",
    )
    cloud = SimpleNamespace(
        manifest={"invocationTimeout": 60, "conformanceRevision": "conformance"},
        observation_ended_at=2,
        retired_at=2,
        source_uris={"memory://source"},
        events={},
        raw_histories={},
        terminal_histories=set(),
        control_records=[],
        invocations=[],
        artifacts_dir=tmp_path,
        _save=lambda *args: None,
    )
    event = {
        "kind": "task_start",
        "taskId": "root",
        "requestId": "request",
        "ownerRequestId": "request",
        "executionArn": "execution",
        "atMs": 1000,
    }
    sources = SimpleNamespace(
        observations=lambda **_: ([event], []),
        histories=lambda: ([], {}, []),
        attempts=lambda _, **kwargs: (
            [{"requestId": "request", "executionArn": "execution"}],
            {"victim": "request"},
            [],
        ),
    )
    cloud.execution_results = {"execution": {"Status": "STOPPED"}}
    with patch("aws_durable_execution_conformance_tests.lmi.source_evidence.CaseSources.capture", return_value=sources):
        evidence = build_evidence(ctx, cloud, phase="final")
    assert evidence["observationEndedAtMs"] == 2000
    assert evidence["collection"]["watermarks"] == {}
    assert evidence["collectionErrors"]
    invocation = next(item for item in evidence["retirement"]["resources"] if item["kind"] == "invocation")
    assert invocation["outcome"] == "UNKNOWN"
