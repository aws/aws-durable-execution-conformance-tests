# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the SAM cleanup helper."""

from __future__ import annotations

from typing import TYPE_CHECKING

from botocore.exceptions import ClientError

from aws_durable_execution_conformance_tests import sam
from aws_durable_execution_conformance_tests.sam import delete_stack

if TYPE_CHECKING:
    import pytest


def test_delete_stack_returns_true_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, object] = {}

    class _FakeClient:
        def delete_stack(self, **kwargs: object) -> None:
            calls.update(kwargs)

    def _fake_boto3_client(service: str, region_name: str | None = None) -> _FakeClient:
        calls["service"] = service
        calls["region_name"] = region_name
        return _FakeClient()

    monkeypatch.setattr(sam.boto3, "client", _fake_boto3_client)

    assert delete_stack("my-stack", "us-west-2") is True
    assert calls["service"] == "cloudformation"
    assert calls["region_name"] == "us-west-2"
    assert calls["StackName"] == "my-stack"


def test_delete_stack_is_best_effort_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeClient:
        def delete_stack(self, **_kwargs: object) -> None:
            raise ClientError({"Error": {"Code": "AccessDenied", "Message": "no"}}, "DeleteStack")

    monkeypatch.setattr(sam.boto3, "client", lambda *_a, **_k: _FakeClient())

    # Never raises; returns False so the caller can note the best-effort miss.
    assert delete_stack("my-stack", "us-west-2") is False
